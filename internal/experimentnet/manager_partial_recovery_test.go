package experimentnet

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestManagerNewAttemptRecoversPartialPreparationAfterRestart(t *testing.T) {
	stages := []struct {
		name  string
		match func([]string) bool
	}{
		{"overlay-remove", func(args []string) bool { return len(args) > 1 && args[0] == "network" && args[1] == "rm" }},
		{"overlay-create", func(args []string) bool { return len(args) > 1 && args[0] == "network" && args[1] == "create" }},
		{"gateway-create", func(args []string) bool { return args[0] == "create" }},
		{"gateway-copy", func(args []string) bool { return args[0] == "cp" }},
		{"gateway-connect", func(args []string) bool { return len(args) > 1 && args[0] == "network" && args[1] == "connect" }},
		{"gateway-start", func(args []string) bool { return args[0] == "start" }},
		{"gateway-inspect", func(args []string) bool {
			return len(args) > 4 && args[0] == "inspect" && args[4] == gatewayInspectionFormat
		}},
	}
	for _, stage := range stages {
		for _, after := range []bool{false, true} {
			timing := "before"
			if after {
				timing = "after"
			}
			t.Run(stage.name+"/"+timing, func(t *testing.T) {
				m, fake, request := managerFixture(t)
				original, injected := m.command, false
				m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
					if injected || !stage.match(args) {
						return original(ctx, input, args...)
					}
					injected = true
					if after {
						if _, err := original(ctx, input, args...); err != nil {
							return nil, err
						}
					}
					return nil, errors.New("injected preparation response failure")
				}
				first, err := m.prepare(t.Context(), request)
				if !injected {
					t.Fatal("preparation did not reach the injection stage")
				}
				if after && stage.name == "overlay-create" {
					if err != nil || first.NetworkID == "" || fake.createCalls != 1 {
						t.Fatalf("ambiguous creation did not reconcile: result=%+v err=%v", first, err)
					}
				} else if err == nil || first.NetworkID != "" {
					t.Fatalf("partial preparation was acknowledged: result=%+v err=%v", first, err)
				}
				// Reconstruct the Manager using only its persisted recovery state;
				// the independent fake Engine keeps the partially changed objects.
				restarted, err := NewManager(m.config)
				if err != nil {
					t.Fatal(err)
				}
				restarted.client.CloseIdleConnections()
				restarted.command, restarted.client = fake.command, m.client
				if first.NetworkID == "" && fake.network != nil && fake.network.Labels["io.kpl.execution"] == request.Epoch {
					mutations := len(fake.mutations)
					if _, err := restarted.prepare(t.Context(), request); err == nil || !strings.Contains(err.Error(), "previous preparation is incomplete") {
						t.Fatalf("incomplete attempt was silently resumed: %v", err)
					}
					if len(fake.mutations) != mutations {
						t.Fatal("incomplete acknowledgment retry mutated Docker")
					}
				}
				oldID := ""
				if fake.network != nil {
					oldID = fake.network.ID
				}
				request.Epoch = "restarted-attempt"
				request.RequestedAt = request.RequestedAt.Add(time.Second)
				result, err := restarted.prepare(t.Context(), request)
				if err != nil || result.NetworkID == "" || result.NetworkID == oldID || !fake.gateway {
					t.Fatalf("new attempt could not recover partial preparation: oldID=%q result=%+v err=%v", oldID, result, err)
				}
			})
		}
	}
}

func TestManagerNewAttemptRecoversUnrecordedReadyGateway(t *testing.T) {
	m, fake, request := managerFixture(t)
	path := filepath.Join(m.config.DataDir, "active.json.tmp")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prepare(t.Context(), request); err == nil || !fake.gateway {
		t.Fatalf("ready gateway persistence failure was not preserved: gateway=%t err=%v", fake.gateway, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewManager(m.config)
	if err != nil {
		t.Fatal(err)
	}
	restarted.client.CloseIdleConnections()
	restarted.command, restarted.client = fake.command, m.client
	mutations := len(fake.mutations)
	if _, err := restarted.prepare(t.Context(), request); err == nil || !strings.Contains(err.Error(), "previous preparation is incomplete") {
		t.Fatalf("missing durable acknowledgment was accepted: %v", err)
	}
	if len(fake.mutations) != mutations {
		t.Fatal("missing acknowledgment retry mutated Docker")
	}
	request.Epoch = "recovery-attempt"
	request.RequestedAt = request.RequestedAt.Add(time.Second)
	if result, err := restarted.prepare(t.Context(), request); err != nil || result.NetworkID == "" {
		t.Fatalf("new attempt did not recover the unrecorded gateway: result=%+v err=%v", result, err)
	}
}

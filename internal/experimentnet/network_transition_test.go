package experimentnet

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestManagerWaitsForNetworkRemovalAndCreationVisibility(t *testing.T) {
	m, fake, request := managerFixture(t)
	fake.removeLag, fake.createLag = 2, 2
	result, err := m.prepare(t.Context(), request)
	if err != nil || result.NetworkID == "" {
		t.Fatalf("transition failed: %+v %v", result, err)
	}
	if fake.createCalls != 1 {
		t.Fatalf("created before the old network disappeared: %d calls", fake.createCalls)
	}
}

func TestManagerRetriesOnlyNameConflictsAfterConfirmingAbsence(t *testing.T) {
	m, fake, request := managerFixture(t)
	fake.createConflicts = 2
	result, err := m.prepare(t.Context(), request)
	if err != nil || result.NetworkID == "" || fake.createCalls != 3 {
		t.Fatalf("name conflict recovery: %+v %v creates=%d", result, err, fake.createCalls)
	}
	removals := 0
	for _, call := range fake.mutations {
		if strings.HasPrefix(call, "network rm ") {
			removals++
		}
	}
	if removals != 1 {
		t.Fatalf("retried deletion: %d", removals)
	}
}

func TestManagerReconcilesLostCreationResponseWithoutCreatingTwice(t *testing.T) {
	m, fake, request := managerFixture(t)
	original := m.command
	m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
		data, err := original(ctx, input, args...)
		if err == nil && len(args) > 1 && args[0] == "network" && args[1] == "create" {
			return nil, errors.New("creation response lost")
		}
		return data, err
	}
	result, err := m.prepare(t.Context(), request)
	if err != nil || result.NetworkID == "" || fake.createCalls != 1 {
		t.Fatalf("lost response reconciliation: %+v %v creates=%d", result, err, fake.createCalls)
	}
}

func TestManagerDoesNotRetryUncertainCreationWithoutConfirmedNetwork(t *testing.T) {
	m, fake, request := managerFixture(t)
	fake.failCreate = true
	_, err := m.prepare(t.Context(), request)
	if err == nil || !strings.Contains(err.Error(), "response may be ambiguous") || fake.createCalls != 1 || fake.gateway {
		t.Fatalf("uncertain create retried: %v creates=%d gateway=%t", err, fake.createCalls, fake.gateway)
	}
}

func TestManagerNetworkTransitionCancellationStopsMutations(t *testing.T) {
	for _, kind := range []string{"removal", "name-conflict", "visibility"} {
		t.Run(kind, func(t *testing.T) {
			m, fake, request := managerFixture(t)
			switch kind {
			case "removal":
				fake.removeLag = 1000
			case "name-conflict":
				fake.createConflicts = 1000
			case "visibility":
				fake.createLag = 1000
			}
			ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
			defer cancel()
			_, err := m.prepare(ctx, request)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline not retained: %v", err)
			}
			expected := 1
			if kind == "removal" {
				expected = 0
			}
			if fake.createCalls != expected || fake.gateway {
				t.Fatalf("work continued after cancellation: creates=%d gateway=%t", fake.createCalls, fake.gateway)
			}
			if !m.mu.TryLock() {
				t.Fatal("preparation lock leaked")
			}
			m.mu.Unlock()
		})
	}
}

func TestManagerDoesNotRemoveConcurrentReplacement(t *testing.T) {
	m, fake, request := managerFixture(t)
	original := m.command
	m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
		data, err := original(ctx, input, args...)
		if err == nil && len(args) > 1 && args[0] == "network" && args[1] == "rm" {
			fake.network = fixtureNetwork(t)
			fake.network.ID = "concurrent-network"
		}
		return data, err
	}
	_, err := m.prepare(t.Context(), request)
	if err == nil || !strings.Contains(err.Error(), "concurrent-network") || fake.createCalls != 0 || fake.network == nil || fake.network.ID != "concurrent-network" {
		t.Fatalf("unexpected replacement changed: %v creates=%d", err, fake.createCalls)
	}
	if len(fake.mutations) != 1 {
		t.Fatalf("changed replacement: %v", fake.mutations)
	}
}

func TestManagerRejectsMismatchedOrOccupiedNetworkAfterAmbiguousCreate(t *testing.T) {
	for _, kind := range []string{"epoch", "subnet", "owner", "endpoint", "service"} {
		t.Run(kind, func(t *testing.T) {
			m, fake, request := managerFixture(t)
			original := m.command
			m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
				data, err := original(ctx, input, args...)
				if err == nil && len(args) > 1 && args[0] == "network" && args[1] == "create" {
					switch kind {
					case "epoch":
						fake.network.Labels["io.kpl.execution"] = "other-attempt"
					case "subnet":
						fake.network.IPAM.Config[0].Subnet = "10.12.0.0/16"
					case "owner":
						fake.network.Labels["io.kpl.stack"] = "other-stack"
					case "endpoint":
						fake.network.Containers = fixtureNetwork(t).Containers
						fake.gateway = true
						_, _ = original(ctx, nil, "network", "connect", fake.network.ID, "gateway-container")
					case "service":
						fake.legacyService = true
					}
					return nil, errors.New("creation response lost")
				}
				return data, err
			}
			result, err := m.prepare(t.Context(), request)
			if err == nil || result.NetworkID != "" || fake.createCalls != 1 {
				t.Fatalf("unverified replacement accepted: %+v %v", result, err)
			}
			for _, call := range fake.mutations {
				if strings.HasPrefix(call, "create ") {
					t.Fatalf("gateway started on an unverified network: %s", call)
				}
			}
		})
	}
}

func TestManagerFailedRemovalCheckDoesNotCreate(t *testing.T) {
	m, fake, request := managerFixture(t)
	original := m.command
	removed := false
	m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
		if removed && len(args) > 1 && args[0] == "network" && args[1] == "ls" {
			return nil, errors.New("network list unavailable")
		}
		data, err := original(ctx, input, args...)
		if err == nil && len(args) > 1 && args[0] == "network" && args[1] == "rm" {
			removed = true
		}
		return data, err
	}
	_, err := m.prepare(t.Context(), request)
	if err == nil || !strings.Contains(err.Error(), "network list unavailable") || fake.createCalls != 0 {
		t.Fatalf("unknown absence accepted: %v creates=%d", err, fake.createCalls)
	}
}

func TestManagerNameConflictDoesNotReuseAnotherExecution(t *testing.T) {
	m, fake, request := managerFixture(t)
	original := m.command
	m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
		if len(args) > 1 && args[0] == "network" && args[1] == "create" {
			fake.network = fixtureNetwork(t)
			fake.network.ID = "another-execution-network"
			fake.network.Labels["io.kpl.execution"] = "another-execution"
		}
		return original(ctx, input, args...)
	}
	_, err := m.prepare(t.Context(), request)
	if err == nil || !strings.Contains(err.Error(), "another-execution") || fake.createCalls != 1 || fake.gateway {
		t.Fatalf("conflicting execution accepted: %v creates=%d", err, fake.createCalls)
	}
	if len(fake.mutations) != 1 || fake.network == nil || fake.network.ID != "another-execution-network" {
		t.Fatalf("conflicting network changed: %v", fake.mutations)
	}
}

func TestManagerNameConflictWaitsForRetiringIDWithoutInspectingIt(t *testing.T) {
	m, fake, request := managerFixture(t)
	retiring := *fake.network
	original := m.command
	m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
		if len(args) > 1 && args[0] == "network" {
			if args[1] == "create" && fake.createCalls == 0 {
				// The daemon's name view briefly exposes the retired ID again.
				fake.network = &retiring
				fake.removing = true
				fake.removeLag = 1
			}
			if args[1] == "inspect" && args[2] == retiring.ID && fake.createCalls > 0 {
				return nil, errors.New("No such network: " + retiring.ID)
			}
		}
		return original(ctx, input, args...)
	}
	result, err := m.prepare(t.Context(), request)
	if err != nil || result.NetworkID == "" || fake.createCalls != 2 {
		t.Fatalf("retiring ID race: %+v %v creates=%d", result, err, fake.createCalls)
	}
}

package experimentnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPreparationLogsDistinguishGatewayStartFailureFromReady(t *testing.T) {
	for _, failStart := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready", true: "start-failed"}[failStart], func(t *testing.T) {
			m, fake, request := managerFixture(t)
			fake.failStart = failStart
			var output bytes.Buffer
			m.logger = slog.New(slog.NewJSONHandler(&output, nil))
			_, err := m.prepare(t.Context(), request)
			if (err != nil) != failStart {
				t.Fatalf("preparation error=%v, failStart=%v", err, failStart)
			}
			for _, secret := range []string{m.config.Token, m.config.ControllerURL, request.Agents["agent"]} {
				if strings.Contains(output.String(), secret) {
					t.Fatal("preparation trace exposed credentials or upstream configuration")
				}
			}
			var gatewayEnd, networkEnd map[string]any
			decoder := json.NewDecoder(&output)
			for decoder.More() {
				var entry map[string]any
				if err := decoder.Decode(&entry); err != nil {
					t.Fatal(err)
				}
				if entry["run"] != request.RunID || entry["epoch"] != request.Epoch {
					t.Fatalf("trace lost preparation identity: %v", entry)
				}
				switch entry["msg"] {
				case "experiment gateway preparation finished":
					gatewayEnd = entry
				case "experiment network preparation finished":
					networkEnd = entry
				}
			}
			if gatewayEnd == nil || networkEnd == nil {
				t.Fatal("preparation trace is missing completion records")
			}
			if failStart {
				if gatewayEnd["outcome"] != "failed" || gatewayEnd["stage"] != "start gateway container" || networkEnd["outcome"] != "failed" || networkEnd["stage"] != "prepare gateway" {
					t.Fatalf("failed gateway was reported ready or lost its stage: gateway=%v network=%v", gatewayEnd, networkEnd)
				}
			} else if gatewayEnd["outcome"] != "ready" || gatewayEnd["stage"] != "wait for gateway listener" || networkEnd["outcome"] != "ready" || networkEnd["stage"] != "record active network" {
				t.Fatalf("successful preparation did not reach its final proof: gateway=%v network=%v", gatewayEnd, networkEnd)
			}
		})
	}
}

type listenLogHandler struct{ records chan slog.Record }

func (h listenLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h listenLogHandler) Handle(_ context.Context, record slog.Record) error {
	h.records <- record.Clone()
	return nil
}
func (h listenLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h listenLogHandler) WithGroup(string) slog.Handler      { return h }

func TestNetworkListeningLogRequiresBoundHTTPListener(t *testing.T) {
	records := make(chan slog.Record, 2)
	logger := slog.New(listenLogHandler{records: records})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, "127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }), logger)
	}()
	var address string
	select {
	case record := <-records:
		if record.Message != "network service listening" {
			t.Fatalf("unexpected startup log: %s", record.Message)
		}
		record.Attrs(func(a slog.Attr) bool {
			if a.Key == "listen" {
				address = a.Value.String()
			}
			return true
		})
	case <-time.After(3 * time.Second):
		t.Fatal("network server did not bind")
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + address)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	client.CloseIdleConnections()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("bound server returned %d", response.StatusCode)
	}
	// A second bind must fail without claiming that the server is listening.
	if err := serve(t.Context(), address, http.NotFoundHandler(), logger); err == nil {
		t.Fatal("duplicate listener unexpectedly bound")
	}
	if len(records) != 0 {
		t.Fatal("failed bind reported a listening server")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("network server did not stop after cancellation")
	}
}

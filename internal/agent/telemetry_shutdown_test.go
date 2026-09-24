package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAgentRunDrainsTelemetryWhoseBodyCompletesDuringShutdown(t *testing.T) {
	forwarded := make(chan model.TraceEvent, 4)
	registered := make(chan struct{}, 1)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/agents/register" {
			select {
			case registered <- struct{}{}:
			default:
			}
		}
		if r.URL.Path == "/api/v1/events/batch" {
			var batch model.EventBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			for _, event := range batch.Events {
				forwarded <- event
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer controller.Close()
	port, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := port.Addr().String()
	_ = port.Close()
	server, _ := newContainerTestServer(t, nil)
	server.config.Listen = address
	server.config.ControllerURL = controller.URL
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); done <- server.Run(ctx) }()
	var connection net.Conn
	defer func() {
		cancel()
		if connection != nil {
			_ = connection.Close()
		}
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("Agent did not finish after test cleanup")
		}
	}()
	// Run binds its port before Docker checks/cleanup, so a successful dial
	// does not mean HTTP is serving yet. Wait for registration before starting
	// the request deadline; race-instrumented Docker helpers exit more slowly.
	select {
	case <-registered:
	case err := <-done:
		t.Fatalf("Agent failed before registration: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Agent did not finish startup")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		connection, err = net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Agent did not listen: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = connection.SetDeadline(time.Now().Add(4 * time.Second))
	body := `{"events":[{"eventId":"late-body","runId":"run","nodeId":"peer","sessionId":"session","sequence":7,"type":"measurement_stop","timestamp":"2026-09-05T01:02:03Z"}]}`
	if _, err := fmt.Fprintf(connection, "POST /api/v1/telemetry HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n", address, len(body)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(connection)
	interim, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = interim.Body.Close()
	if interim.StatusCode != http.StatusContinue {
		t.Fatalf("handler did not start reading telemetry: %s", interim.Status)
	}
	// 100 Continue proves the real handler is active and blocked in Decode.
	// Keep its JSON body incomplete until shutdown has had time to begin.
	split := len(body) / 2
	if _, err := io.WriteString(connection, body[:split]); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("Agent returned while telemetry handler was still reading: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := io.WriteString(connection, body[split:]); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("active telemetry request was not acknowledged: %s", response.Status)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Agent did not finish after the telemetry body completed")
	}
	select {
	case event := <-forwarded:
		if event.EventID != "late-body" || event.SessionID != "session" || event.Sequence != 7 || event.AgentID != "agent" || event.Timestamp.Format(time.RFC3339) != "2026-09-05T01:02:03Z" {
			t.Fatalf("forwarding changed source identity or timestamp: %+v", event)
		}
	default:
		t.Fatal("Agent acknowledged the late telemetry body but exited without forwarding it")
	}
}

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestHeartbeatRecoveryReplaysHistoryOnlyAfterRegistrationLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(Config{ID: "agent", DockerImage: "kpl:test", DockerNetwork: "peers", AdvertiseURL: "http://agent:8090", ControllerURL: "http://controller:8080", DataDir: t.TempDir()}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		s.processes["stopped"] = &process{node: model.Node{ID: "stopped", State: model.NodeStopped}, exited: true}
		s.processes["live"] = &process{node: model.Node{ID: "live", State: model.NodeReady}}
		registrations := 0
		var historyCounts []int
		s.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			status := http.StatusNoContent
			switch r.URL.Path {
			case "/api/v1/agents/register":
				registrations++
			case "/api/v1/agents/heartbeat":
				var h model.AgentHeartbeat
				if err := json.NewDecoder(r.Body).Decode(&h); err != nil {
					t.Fatal(err)
				}
				stopped := 0
				for _, node := range h.Nodes {
					if node.ID == "stopped" {
						stopped++
					}
				}
				historyCounts = append(historyCounts, stopped)
				switch len(historyCounts) {
				case 2:
					return nil, errors.New("temporary connection failure")
				case 3:
					status = http.StatusServiceUnavailable
				case 5:
					// A restarted Controller has forgotten registration/inventory.
					status = http.StatusConflict
				}
			default:
				t.Fatalf("unexpected request %s", r.URL.Path)
			}
			return &http.Response{StatusCode: status, Status: http.StatusText(status), Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header)}, nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); s.heartbeatLoop(ctx) }()
		defer func() { cancel(); <-done }()
		synctest.Wait()
		time.Sleep(14 * time.Second)
		synctest.Wait()
		if registrations != 2 {
			t.Fatalf("registered %d times, want initial registration and Controller restart only", registrations)
		}
		if want := []int{1, 0, 0, 0, 0, 1, 0}; !slices.Equal(historyCounts, want) {
			t.Fatalf("stopped Peer history sent per heartbeat = %v, want %v", historyCounts, want)
		}
	})
}

func TestRegistrationResponseAllowsConnectionReuse(t *testing.T) {
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":"agent","capacity":200}`)
	}))
	defer controller.Close()
	s := &Server{config: Config{ControllerURL: controller.URL}, client: controller.Client()}
	var reused []bool
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		reused = append(reused, info.Reused)
	}})
	for range 2 {
		if err := s.postJSON(ctx, "/api/v1/agents/register", model.Agent{ID: "agent"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(reused, []bool{false, true}) {
		t.Fatalf("Controller connection reuse = %v, want [false true]", reused)
	}
}

func TestAcknowledgedControllerRequestIgnoresResponseDrainFailure(t *testing.T) {
	s := &Server{
		config: Config{ControllerURL: "http://controller"},
		client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body, writer := io.Pipe()
			_ = writer.CloseWithError(io.ErrUnexpectedEOF)
			return &http.Response{StatusCode: http.StatusOK, Body: body, Header: make(http.Header)}, nil
		})},
	}
	if err := s.postJSON(context.Background(), "/api/v1/events/batch", model.EventBatch{}, nil); err != nil {
		t.Fatalf("accepted telemetry would be retried after response draining failed: %v", err)
	}
}

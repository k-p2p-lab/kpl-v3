package peer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func scheduleTestConfig(reference string) model.PeerProcessConfig {
	return model.PeerProcessConfig{NodeConfig: model.NodeConfig{Network: model.NetworkConfig{Delay: "10ms", Schedule: &model.NetworkSchedule{Reference: reference, Changes: []model.NetworkChange{
		{After: "1m", Set: model.NetworkConfig{Delay: "100ms"}},
		{After: "3m", Set: model.NetworkConfig{Delay: "200ms"}},
	}}}}}
}

func TestNetworkScheduleClocksLateJoinAndRequiredSynchronization(t *testing.T) {
	now := time.Now()
	peerConfig := scheduleTestConfig("peer-join")
	noClock := func(context.Context, string, *http.Client) (controllerClockEstimate, error) {
		t.Fatal("peer-join queried Controller clock")
		return controllerClockEstimate{}, nil
	}
	p, err := newPeerNetworkSchedule(context.Background(), peerConfig, now, noClock)
	if err != nil || p.next != 0 || p.initial.Delay != "10ms" || !p.base.Equal(now) {
		t.Fatalf("relative clock: %+v %v", p, err)
	}
	// The Controller clock is an hour ahead. A Peer arriving two minutes after
	// experiment start must begin at 100ms, not wait another minute at 10ms.
	config := scheduleTestConfig("experiment-start")
	config.ExperimentStartedAt = now.Add(time.Hour - 2*time.Minute)
	p, err = newPeerNetworkSchedule(context.Background(), config, now, func(context.Context, string, *http.Client) (controllerClockEstimate, error) {
		return controllerClockEstimate{offset: time.Hour}, nil
	})
	if err != nil || p.next != 1 || p.initial.Delay != "100ms" || time.Until(p.base.Add(3*time.Minute)) < 59*time.Second {
		t.Fatalf("absolute clock/late join: %+v %v", p, err)
	}
	config.ExperimentStartedAt = now.Add(time.Hour - 4*time.Minute)
	p, err = newPeerNetworkSchedule(context.Background(), config, now, func(context.Context, string, *http.Client) (controllerClockEstimate, error) {
		return controllerClockEstimate{offset: time.Hour}, nil
	})
	if err != nil || p.next != 2 || p.initial.Delay != "200ms" {
		t.Fatalf("all changes already due: %+v %v", p, err)
	}
	if _, err = newPeerNetworkSchedule(context.Background(), config, now, func(context.Context, string, *http.Client) (controllerClockEstimate, error) {
		return controllerClockEstimate{}, errors.New("offline")
	}); err == nil || !strings.Contains(err.Error(), "synchronized") {
		t.Fatal("unsynchronized absolute schedule silently started")
	}
	config.ExperimentStartedAt = time.Time{}
	if _, err = newPeerNetworkSchedule(context.Background(), config, now, noClock); err == nil {
		t.Fatal("missing run start accepted")
	}
}

func networkScheduleTestServer(p *peerNetworkSchedule) *Server {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &Server{networkSchedule: p, logger: logger, telemetry: newTelemetry(model.Node{ID: "peer", RunID: "run"}, "", "", logger)}
}

func TestNetworkScheduleTimersCancellationCatchupAndFailure(t *testing.T) {
	t.Run("wait and apply", func(t *testing.T) {
		base := time.Now()
		p := &peerNetworkSchedule{reference: "peer-join", base: base, initial: model.NetworkConfig{Delay: "10ms"}, steps: []peerNetworkStep{{after: 40 * time.Millisecond, config: model.NetworkConfig{Delay: "100ms"}}}}
		s := networkScheduleTestServer(p)
		calls := 0
		err := s.runNetworkSchedule(context.Background(), func(ctx context.Context, previous, next model.NetworkConfig, port int) error {
			calls++
			if time.Since(base) < 40*time.Millisecond || previous.Delay != "10ms" || next.Delay != "100ms" || port != 20000 {
				t.Fatal("wrong transition/deadline")
			}
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 5*time.Second {
				t.Fatal("unbounded tc command")
			}
			return nil
		})
		if err != nil || calls != 1 || s.networkApplied.Load().revision != 1 {
			t.Fatalf("apply: %v", err)
		}
	})
	t.Run("cancel before deadline", func(t *testing.T) {
		s := networkScheduleTestServer(&peerNetworkSchedule{base: time.Now(), steps: []peerNetworkStep{{after: time.Hour}}})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := s.runNetworkSchedule(ctx, func(context.Context, model.NetworkConfig, model.NetworkConfig, int) error {
			t.Fatal("ran canceled transition")
			return nil
		}); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
	})
	t.Run("cancel active tc", func(t *testing.T) {
		s := networkScheduleTestServer(&peerNetworkSchedule{base: time.Now(), steps: []peerNetworkStep{{}}})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		entered := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- s.runNetworkSchedule(ctx, func(ctx context.Context, _, _ model.NetworkConfig, _ int) error {
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			})
		}()
		<-entered
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("tc cancellation blocked")
		}
		if s.networkApplied.Load() != nil {
			t.Fatal("canceled change reported as applied")
		}
	})
	t.Run("catch up without obsolete transitions", func(t *testing.T) {
		s := networkScheduleTestServer(&peerNetworkSchedule{base: time.Now().Add(-time.Hour), steps: []peerNetworkStep{{after: time.Second, config: model.NetworkConfig{Delay: "100ms"}}, {after: 2 * time.Second, config: model.NetworkConfig{Delay: "200ms"}}}})
		calls := 0
		if err := s.runNetworkSchedule(context.Background(), func(_ context.Context, _, next model.NetworkConfig, _ int) error {
			calls++
			if next.Delay != "200ms" {
				t.Fatal("obsolete state installed")
			}
			return nil
		}); err != nil || calls != 1 {
			t.Fatalf("catchup: %v calls=%d", err, calls)
		}
		if s.networkApplied.Load().revision != 2 {
			t.Fatal("catchup revision lost")
		}
	})
	t.Run("failure keeps last known state", func(t *testing.T) {
		s := networkScheduleTestServer(&peerNetworkSchedule{base: time.Now(), steps: []peerNetworkStep{{config: model.NetworkConfig{Delay: "100ms"}}}})
		sentinel := errors.New("tc refused")
		err := s.runNetworkSchedule(context.Background(), func(context.Context, model.NetworkConfig, model.NetworkConfig, int) error { return sentinel })
		if !errors.Is(err, sentinel) || s.networkApplied.Load() != nil {
			t.Fatalf("failure hidden: %v", err)
		}
	})
}

func TestNetworkStatusDoesNotMutateSharedMetadata(t *testing.T) {
	s := networkScheduleTestServer(&peerNetworkSchedule{reference: "peer-join"})
	s.recordNetworkApplied(model.NetworkConfig{Delay: "100ms"}, 1, time.Now(), "3m", false)
	original := map[string]string{"network": "initial", "networkPending": "true", "keep": "value"}
	node := model.Node{Metadata: original}
	s.addNetworkStatus(&node)
	if original["network"] != "initial" || node.Metadata["network"] != `{"delay":"100ms"}` || node.Metadata["networkRevision"] != "1" || node.Metadata["networkPending"] != "false" || node.Metadata["keep"] != "value" {
		t.Fatalf("metadata snapshot: %v original=%v", node.Metadata, original)
	}
}

func TestNetworkScheduleFailurePropagatesThroughPeerRun(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/health" {
			json.NewEncoder(w).Encode(map[string]any{"time": time.Now().UTC()})
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer endpoint.Close()
	s := newRunTestServer(t, model.PeerProcessConfig{Node: model.Node{ID: "peer", RunID: "run", Role: "boot"}, ControllerURL: endpoint.URL, AgentURL: endpoint.URL})
	// Invalid injected tc input fails before any real interface can be touched.
	s.networkSchedule = &peerNetworkSchedule{base: time.Now(), steps: []peerNetworkStep{{config: model.NetworkConfig{Delay: "invalid"}}}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := s.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "network schedule change 1") {
		t.Fatalf("scheduler failure lost on shutdown: %v", err)
	}
}

func TestNetworkScheduleReusesStartupClock(t *testing.T) {
	var probes atomic.Int64
	ready := make(chan struct{}, 1)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/health" {
			probes.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"time": time.Now().UTC()})
			return
		}
		if r.URL.Path == "/api/v1/bootstrap" {
			_ = json.NewEncoder(w).Encode([]bootstrapNode{})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/status") {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer endpoint.Close()
	s := newRunTestServer(t, model.PeerProcessConfig{Node: model.Node{ID: "peer", RunID: "run", Role: "boot"}, ControllerURL: endpoint.URL, AgentURL: endpoint.URL, APListen: "127.0.0.1:0"})
	s.networkSchedule = &peerNetworkSchedule{reference: "experiment-start", base: time.Now()}
	s.telemetry.acceptClockEstimate(controllerClockEstimate{offset: time.Second}, time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("Peer did not become ready")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Peer did not stop")
	}
	if probes.Load() != 0 {
		t.Fatalf("repeated %d clock probes after initial shaping", probes.Load())
	}
}

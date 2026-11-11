package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAnalysisCapabilityAndScopedPrivateRoutes(t *testing.T) {
	legacy := &Server{config: Config{Token: "peer-token"}, processes: map[string]*process{}}
	if legacy.snapshotAgent().AnalysisProcesses {
		t.Fatal("Agent without a calculation handler advertised analysis support")
	}
	encoded, err := json.Marshal(legacy.snapshotAgent())
	if err != nil || strings.Contains(string(encoded), "analysisProcesses") {
		t.Fatalf("legacy status gained a capability field: %s %v", encoded, err)
	}
	missing := httptest.NewRecorder()
	legacy.Handler().ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/internal/result-analysis", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("unsupported analysis endpoint: %d", missing.Code)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer analysis-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	s := &Server{config: Config{Token: "peer-token", AnalysisHandler: handler}, startupReconciled: true, processes: map[string]*process{}}
	if !s.snapshotAgent().AnalysisProcesses {
		t.Fatal("Agent calculation capability was not advertised")
	}
	for _, path := range []string{"/internal/result-analysis", "/internal/analysis-idle"} {
		for _, token := range []string{"", "peer-token", "analysis-token"} {
			request := httptest.NewRequest(http.MethodGet, path, nil)
			request.Header.Set("Authorization", "Bearer "+token)
			response := httptest.NewRecorder()
			s.Handler().ServeHTTP(response, request)
			want := http.StatusUnauthorized
			if token == "analysis-token" {
				want = http.StatusNoContent
			}
			if response.Code != want {
				t.Fatalf("analysis private route %s returned %d, want %d", path, response.Code, want)
			}
		}
	}
}

func TestAnalysisAdmissionReservesHostAgainstPeerCreation(t *testing.T) {
	s, _ := newContainerTestServer(t, nil)
	s.startupReconciled = true
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	s.config.AnalysisHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
	})
	go func() {
		defer close(done)
		s.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/internal/result-analysis", nil))
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("analysis did not start")
	}
	defer func() { close(release); <-done }()
	if _, err := s.createNode(t.Context(), model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "workers"}); err == nil || !strings.Contains(err.Error(), "result analysis") {
		t.Fatalf("Peer creation overlapped analysis: %v", err)
	}
	if _, err := s.ensureRunWorker(t.Context(), "run"); err == nil || !strings.Contains(err.Error(), "result analysis") {
		t.Fatalf("Run worker creation overlapped analysis: %v", err)
	}
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/internal/result-analysis", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("a second analysis entered the same host: %d", response.Code)
	}
	idle := httptest.NewRecorder()
	s.Handler().ServeHTTP(idle, httptest.NewRequest(http.MethodGet, "/internal/analysis-idle", nil))
	if idle.Code != http.StatusTooManyRequests {
		t.Fatalf("analysis cleanup probe ignored the host reservation: %d", idle.Code)
	}
	if !s.snapshotAgent().AnalysisProcesses {
		t.Fatal("long analysis blocked Agent status")
	}
}

func TestAnalysisAdmissionWaitsForStartupAndPeerCleanup(t *testing.T) {
	for _, state := range []string{"starting", "stopping", "fencing", "reboot", "peer", "cleanup", "run-worker"} {
		t.Run(state, func(t *testing.T) {
			s := &Server{startupReconciled: true, processes: map[string]*process{}, config: Config{AnalysisHandler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("analysis admitted before the host was idle")
			})}}
			switch state {
			case "starting":
				s.startupReconciled = false
			case "stopping":
				s.shuttingDown = true
			case "fencing":
				s.fencingAll = true
			case "reboot":
				s.host = &model.AgentHost{RebootPending: true}
			case "peer":
				s.processes["peer"] = &process{node: model.Node{ID: "peer", State: model.NodeStarting}}
			case "cleanup":
				s.processes["peer"] = &process{node: model.Node{ID: "peer", State: model.NodeFailed}, exited: true, cleanupErr: errors.New("remove pending")}
			case "run-worker":
				s.runWorkers = map[string]*agentRunWorker{"run": {}}
			}
			response := httptest.NewRecorder()
			s.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/internal/result-analysis", nil))
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("host readiness guard returned %d", response.Code)
			}
		})
	}
}

func TestAgentCloseWaitsForAnalysisCleanupAndPreservesFailure(t *testing.T) {
	s, _ := newContainerTestServer(t, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	failure := errors.New("analysis child cleanup failed")
	s.config.StopAnalysis = func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return failure
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	<-entered
	select {
	case err := <-done:
		t.Fatalf("Agent released ownership before analysis stopped: %v", err)
	default:
	}
	close(release)
	if err := <-done; !errors.Is(err, failure) {
		t.Fatalf("analysis cleanup error was lost: %v", err)
	}
	if err := s.Close(); !errors.Is(err, failure) {
		t.Fatalf("repeat Close lost cleanup outcome: %v", err)
	}
}

func TestAnalysisHostReservationBlocksReboot(t *testing.T) {
	s, fixture := newHostRebootServer(t, rebootTestBoot)
	s.analysisActive = true
	if result, status := s.rebootHost(t.Context(), hostRebootRequest(s)); status != http.StatusConflict {
		t.Fatalf("host reboot overlapped analysis: %d %+v", status, result)
	}
	if fixture.count("create") != 0 || fixture.count("start") != 0 || s.host.RebootPending {
		t.Fatal("analysis reservation allowed reboot dispatch or retained maintenance admission")
	}
}

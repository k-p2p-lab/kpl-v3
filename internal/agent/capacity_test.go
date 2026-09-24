package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestDockerConcurrentAdmissionCannotExceedCapacity(t *testing.T) {
	s, _ := newContainerTestServer(t, map[string]string{"HANG": "wait"})
	s.config.Capacity = 3
	const requests = 20
	results := make(chan error, requests)
	var workers sync.WaitGroup
	for i := 0; i < requests; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			_, err := s.createNode(context.Background(), model.CreateNodeRequest{ID: fmt.Sprintf("peer-%d", i), RunID: "run", Group: "workers"})
			results <- err
		}(i)
	}
	workers.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		} else if !strings.Contains(err.Error(), "capacity reached") {
			t.Fatal(err)
		}
	}
	if accepted != s.config.Capacity || s.snapshot().Agent.ActiveNodes != accepted {
		t.Fatalf("accepted %d peers with capacity %d; snapshot %+v", accepted, s.config.Capacity, s.snapshot().Agent)
	}
}

func TestDockerCapacityReleasedOnlyAfterConfirmedCleanup(t *testing.T) {
	for _, failedCleanup := range []bool{false, true} {
		t.Run(fmt.Sprint(failedCleanup), func(t *testing.T) {
			settings := map[string]string{"HANG": "wait", "DELAY": "rm", "DELAY_MS": "300"}
			if failedCleanup {
				settings["FAIL"] = "rm"
			}
			s, path := newContainerTestServer(t, settings)
			s.config.Capacity = 1
			_, err := s.createNode(context.Background(), model.CreateNodeRequest{ID: "old", RunID: "run", Group: "workers"})
			if err != nil {
				t.Fatal(err)
			}
			waitDockerCall(t, path, "wait")
			if err := s.stopNode("old"); err != nil {
				t.Fatal(err)
			}
			waitDockerCall(t, path, "rm")
			if failedCleanup {
				waitForNodeState(t, s, "old", model.NodeFailed)
			}
			if snapshot := s.snapshot(); snapshot.Agent.ActiveNodes != 1 {
				t.Fatalf("cleanup released capacity prematurely: %+v", snapshot)
			}
			replacement := model.CreateNodeRequest{ID: "replacement", RunID: "run", Group: "workers"}
			if _, err := s.createNode(context.Background(), replacement); err == nil || !strings.Contains(err.Error(), "capacity reached") {
				t.Fatalf("admitted a replacement while old container still occupies the host: %v", err)
			}
			if failedCleanup {
				delete(settings, "FAIL")
				if err := s.stopNode("old"); err != nil {
					t.Fatal(err)
				}
			}
			waitForNodeState(t, s, "old", model.NodeStopped)
			if used := s.snapshot().Agent.ActiveNodes; used != 0 {
				t.Fatalf("confirmed removal still occupies %d slots", used)
			}
			if _, err := s.createNode(context.Background(), replacement); err != nil {
				t.Fatalf("did not release capacity after cleanup: %v", err)
			}
		})
	}
}

func TestAgentHeartbeatAndStatusHaveConsistentCapacityAndObservationTime(t *testing.T) {
	s, _ := newContainerTestServer(t, nil)
	s.mu.Lock()
	s.processes["removed"] = &process{node: model.Node{ID: "removed", State: model.NodeStopped}, exited: true}
	s.mu.Unlock()
	reports := make(chan model.AgentHeartbeat, 2)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var report model.AgentHeartbeat
		if strings.HasSuffix(r.URL.Path, "/register") {
			_ = json.NewDecoder(r.Body).Decode(&report.Agent)
		} else {
			_ = json.NewDecoder(r.Body).Decode(&report)
		}
		reports <- report
		w.WriteHeader(http.StatusNoContent)
	}))
	defer controller.Close()
	s.config.ControllerURL = controller.URL
	before := time.Now().UTC()
	if err := s.register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	registered, heartbeat := (<-reports).Agent, <-reports
	recorder := httptest.NewRecorder()
	s.handleStatus(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
	var status model.AgentHeartbeat
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	for _, observed := range []model.Agent{registered, heartbeat.Agent, status.Agent} {
		if !observed.StartedAt.Equal(s.startedAt) || observed.LastSeen.Before(before) || observed.ActiveNodes != 0 {
			t.Fatalf("inconsistent process generation, timestamp or capacity: %+v", observed)
		}
	}
	if len(heartbeat.Nodes) != 1 || len(status.Nodes) != 1 || status.Agent.LastSeen.Before(heartbeat.Agent.LastSeen) {
		t.Fatalf("heartbeat/status snapshots disagree: heartbeat=%+v status=%+v", heartbeat, status)
	}
}

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAllPeerCleanupWaitsForFinalTelemetryAndPreservesFutureRuns(t *testing.T) {
	received := make(chan model.EventBatch, 10)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch model.EventBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		received <- batch
		w.WriteHeader(http.StatusNoContent)
	}))
	defer controller.Close()
	s, _ := newContainerTestServer(t, map[string]string{"HANG": "wait"})
	s.config.ControllerURL = controller.URL
	if _, err := s.createNode(context.Background(), model.CreateNodeRequest{ID: "peer", RunID: "completed", Group: "workers"}); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/api/v1/nodes", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("cleanup failed: %d %s", response.Code, response.Body)
	}
	found := false
	for len(received) > 0 {
		for _, e := range (<-received).Events {
			if e.Type == "measurement_terminated" && e.NodeID == "peer" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("acknowledged cleanup without forwarding terminal event")
	}
	if len(s.events) != 0 || len(s.terminations) != 0 || s.capacityUsedLocked() != 0 {
		t.Fatal("resources still pending after cleanup acknowledgment")
	}
	if _, err := s.createNode(context.Background(), model.CreateNodeRequest{ID: "late", RunID: "completed", Group: "workers", Generation: 99}); err == nil {
		t.Fatal("late create escaped old run fence")
	}
	if _, err := s.createNode(context.Background(), model.CreateNodeRequest{ID: "next", RunID: "new-run", Group: "workers"}); err != nil {
		t.Fatalf("Controller restart cannot start a fresh run: %v", err)
	}
}

func TestFinishProcessReleasesSuccessfulPeerTopology(t *testing.T) {
	node := topologyStatusAt(time.Now())
	node.ID = "peer"
	node.State = model.NodeStopping
	node.Metadata = map[string]string{"containerCreatedAt": "admitted", "lifetimeBasis": "container-created", "resolvedConfig": "large"}
	proc := &process{node: node, done: make(chan struct{}), containerID: "container"}
	s := &Server{processes: map[string]*process{"peer": proc}}
	s.finishProcess("peer", proc, nil, nil)
	if len(proc.node.ConnectedPeers)+len(proc.node.RoutingPeers)+len(proc.node.MeshPeers)+len(proc.node.PeerScores) != 0 {
		t.Fatal("successful tombstone retained full topology")
	}
	if proc.node.Metadata["containerCreatedAt"] != "admitted" || proc.node.Metadata["lifetimeBasis"] != "container-created" {
		t.Fatal("lifecycle evidence lost")
	}
	if proc.containerID != "container" || !proc.exited {
		t.Fatal("cleanup identity lost")
	}
}

func TestAllPeerCleanupNeverAcknowledgesFailedRemoval(t *testing.T) {
	s := &Server{processes: map[string]*process{"peer": {node: model.Node{ID: "peer", RunID: "run", State: model.NodeFailed}, exited: true, cleanupErr: errors.New("Docker unavailable")}}, events: []model.TraceEvent{{EventID: "pending"}}}
	response := httptest.NewRecorder()
	s.handleNodes(response, httptest.NewRequest(http.MethodDelete, "/api/v1/nodes", nil))
	if response.Code != http.StatusInternalServerError || len(s.events) != 1 {
		t.Fatalf("cleanup falsely acknowledged or lost queued data: %d", response.Code)
	}
}

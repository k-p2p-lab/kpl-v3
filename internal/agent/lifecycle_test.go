package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestCreateContainerPreservesLosslessTopicMetadata(t *testing.T) {
	server, _ := newContainerTestServer(t, map[string]string{"HANG": "wait"})
	request := model.CreateNodeRequest{
		ID: "peer", RunID: "run", Group: "workers", Type: "full",
		Config: model.NodeConfig{GossipSub: model.GossipSubConfig{Topics: []string{"topic,with,comma"}}},
	}
	node, err := server.createNode(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	var topics []string
	if err := json.Unmarshal([]byte(node.Metadata["topicsJSON"]), &topics); err != nil {
		t.Fatalf("decode lossless topic metadata: %v", err)
	}
	if want := []string{"topic,with,comma"}; !reflect.DeepEqual(topics, want) {
		t.Fatalf("topicsJSON = %v, want %v", topics, want)
	}
}

func TestRunGenerationFenceRejectsLateCreate(t *testing.T) {
	server, _ := newContainerTestServer(t, map[string]string{"HANG": "wait"})
	server.stopRunGeneration("run", 2)

	for index, generation := range []uint64{0, 2} {
		_, err := server.createNode(context.Background(), model.CreateNodeRequest{
			ID: "late-" + string(rune('a'+index)), RunID: "run", Generation: generation, Group: "workers", Type: "full",
		})
		if err == nil || !strings.Contains(err.Error(), "is fenced") {
			t.Fatalf("generation %d create error = %v, want fence rejection", generation, err)
		}
	}

	node, err := server.createNode(context.Background(), model.CreateNodeRequest{
		ID: "current", RunID: "run", Generation: 3, Group: "workers", Type: "full",
	})
	if err != nil {
		t.Fatalf("create newer generation: %v", err)
	}
	if node.Generation != 3 {
		t.Fatalf("created node generation = %d, want 3", node.Generation)
	}
	server.stopRunGeneration("run", 3)
	waitForNodeState(t, server, node.ID, model.NodeStopped)
}

func TestStopRunGenerationOnlyStopsFencedProcesses(t *testing.T) {
	canceledOld := make(chan struct{}, 2)
	canceledNew := make(chan struct{}, 1)
	canceledOther := make(chan struct{}, 1)
	server := &Server{processes: map[string]*process{
		"legacy": {
			node:   model.Node{ID: "legacy", RunID: "run", Generation: 0, State: model.NodeReady},
			cancel: func() { canceledOld <- struct{}{} },
		},
		"old": {
			node:   model.Node{ID: "old", RunID: "run", Generation: 2, State: model.NodeReady},
			cancel: func() { canceledOld <- struct{}{} },
		},
		"new": {
			node:   model.Node{ID: "new", RunID: "run", Generation: 3, State: model.NodeReady},
			cancel: func() { canceledNew <- struct{}{} },
		},
		"other": {
			node:   model.Node{ID: "other", RunID: "other-run", Generation: 1, State: model.NodeReady},
			cancel: func() { canceledOther <- struct{}{} },
		},
	}}

	server.stopRunGeneration("run", 2)
	if len(canceledOld) != 2 {
		t.Fatalf("canceled old process count = %d, want 2", len(canceledOld))
	}
	if len(canceledNew) != 0 || len(canceledOther) != 0 {
		t.Fatal("newer or unrelated process was canceled")
	}
	server.mu.RLock()
	fence, fenced := server.runFences["run"]
	legacyState := server.processes["legacy"].node.State
	oldState := server.processes["old"].node.State
	newState := server.processes["new"].node.State
	otherState := server.processes["other"].node.State
	server.mu.RUnlock()
	if !fenced || fence != 2 {
		t.Fatalf("run fence = %d/%t, want 2/true", fence, fenced)
	}
	if legacyState != model.NodeStopping || oldState != model.NodeStopping {
		t.Fatalf("old states = %q/%q, want stopping", legacyState, oldState)
	}
	if newState != model.NodeReady || otherState != model.NodeReady {
		t.Fatalf("unfenced states = %q/%q, want ready", newState, otherState)
	}

	server.stopRunGeneration("run", 1)
	server.mu.RLock()
	fence = server.runFences["run"]
	server.mu.RUnlock()
	if fence != 2 {
		t.Fatalf("older stop lowered run fence to %d", fence)
	}
}

func TestRunGenerationStopAPIWaitsForContainerCleanup(t *testing.T) {
	for _, failCleanup := range []bool{false, true} {
		t.Run(map[bool]string{false: "removed", true: "cleanup-failed"}[failCleanup], func(t *testing.T) {
			settings := map[string]string{"HANG": "wait"}
			if failCleanup {
				settings["FAIL"] = "rm"
			}
			server, path := newContainerTestServer(t, settings)
			if _, err := server.createNode(context.Background(), model.CreateNodeRequest{
				ID: "node", RunID: "run-a", Generation: 4, Group: "workers",
			}); err != nil {
				t.Fatal(err)
			}
			waitDockerCall(t, path, "wait")
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodDelete, "/api/v1/runs/run-a/nodes?generation=4", nil)
			server.Handler().ServeHTTP(recorder, request)
			wantStatus, wantState := http.StatusAccepted, model.NodeStopped
			if failCleanup {
				wantStatus, wantState = http.StatusInternalServerError, model.NodeFailed
			}
			if recorder.Code != wantStatus {
				t.Fatalf("stop run response = %d, want %d: %s", recorder.Code, wantStatus, recorder.Body.String())
			}
			if node := server.nodes()[0]; node.State != wantState {
				t.Fatalf("stop run API returned before cleanup completed: %+v", node)
			}
			waitDockerCall(t, path, "rm")
			if _, err := server.createNode(context.Background(), model.CreateNodeRequest{
				ID: "late", RunID: "run-a", Generation: 4, Group: "workers",
			}); err == nil || !strings.Contains(err.Error(), "fenced") {
				t.Fatalf("stop run API failed to fence late creates: %v", err)
			}
			recorder = httptest.NewRecorder()
			request = httptest.NewRequest(http.MethodDelete, "/api/v1/runs/run-a/nodes?generation=invalid", nil)
			server.Handler().ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("invalid generation response = %d, want %d", recorder.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestInFlightPublishRetainsPeerIdentityAndCannotReviveStoppedPeer(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-KPL-Node-ID") != "node" {
			t.Error("publish omitted the peer identity needed to reject reused container IPs")
		}
		close(started)
		<-release
		w.WriteHeader(http.StatusAccepted)
	}))
	defer peer.Close()
	defer unblock()

	server := &Server{client: peer.Client(), processes: make(map[string]*process)}
	proc := &process{node: model.Node{ID: "node", State: model.NodeReady}, apiURL: peer.URL}
	server.processes[proc.node.ID] = proc

	published := make(chan error, 1)
	go func() {
		published <- server.proxyPublish(context.Background(), proc.node.ID, model.PublishRequest{PayloadSize: 1})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("publish did not reach peer")
	}

	server.mu.Lock()
	proc.exited = true
	proc.node.State = model.NodeStopped
	server.mu.Unlock()
	if err := server.proxyPublish(context.Background(), proc.node.ID, model.PublishRequest{PayloadSize: 1}); err == nil {
		t.Fatal("publish accepted a stopped container")
	}

	if err := server.updateNode(model.Node{ID: proc.node.ID, State: model.NodeReady}); err != nil {
		t.Fatalf("apply late status: %v", err)
	}
	server.mu.RLock()
	state := proc.node.State
	server.mu.RUnlock()
	if state != model.NodeStopped {
		t.Fatalf("late status resurrected exited process as %s", state)
	}

	unblock()
	select {
	case err := <-published:
		if err != nil {
			t.Fatalf("publish failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("publish did not finish")
	}
}

func TestProxyPublishConcurrentStateTransitions(t *testing.T) {
	server := &Server{
		client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusAccepted, Status: "202 Accepted", Body: http.NoBody}, nil
		})},
		processes: map[string]*process{
			"node": {node: model.Node{ID: "node", State: model.NodeReady}, apiURL: "http://peer.invalid"},
		},
	}
	start := make(chan struct{})
	var group sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		group.Add(2)
		go func() {
			defer group.Done()
			<-start
			for i := 0; i < 1000; i++ {
				state := model.NodeReady
				if i%2 == 0 {
					state = model.NodeStarting
				}
				if err := server.updateNode(model.Node{ID: "node", State: state}); err != nil {
					t.Errorf("update node: %v", err)
					return
				}
			}
		}()
		go func() {
			defer group.Done()
			<-start
			for i := 0; i < 1000; i++ {
				_ = server.proxyPublish(context.Background(), "node", model.PublishRequest{PayloadSize: 1})
			}
		}()
	}
	close(start)
	group.Wait()
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func waitForNodeState(t *testing.T, server *Server, nodeID, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		server.mu.RLock()
		proc := server.processes[nodeID]
		state := ""
		if proc != nil {
			state = proc.node.State
		}
		server.mu.RUnlock()
		if state == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node %q did not reach state %q", nodeID, want)
}

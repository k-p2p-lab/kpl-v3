package controller

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAttackTargetRegistryScopesIdentityAndReadiness(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	now := time.Now().UTC()
	s.state.mu.Lock()
	s.state.agents["agent"] = model.Agent{ID: "agent", State: "online", LastSeen: now}
	add := func(id, run string, generation uint64, state, role, author, mode string) {
		s.state.setNodeLocked(model.Node{ID: id, RunID: run, Generation: generation, Group: "workers", Role: role, Type: "full", State: state, AgentID: "agent", PeerID: "transport-" + id, Addresses: []string{"/ip4/127.0.0.1/tcp/20000"}, Metadata: map[string]string{"pubsubEnabled": "true", "pubsubAuthorId": author, "pubsubAuthorMode": mode}})
	}
	add("requester", "run", 2, model.NodeReady, "worker", "", "")
	add("publisher", "run", 2, model.NodeReady, "worker", "virtual-signing-id", "explicit")
	add("retired", "run", 2, model.NodeStopped, "worker", "old-signing-id", "explicit")
	add("anonymous", "run", 2, model.NodeReady, "worker", "", "anonymous")
	add("other-run", "other", 2, model.NodeReady, "worker", "", "")
	add("old-generation", "run", 1, model.NodeReady, "worker", "", "")
	add("bootstrap", "run", 2, model.NodeReady, "boot", "", "")
	s.state.mu.Unlock()
	request := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		s.handleAttackTargets(w, httptest.NewRequest(http.MethodGet, path, nil))
		return w
	}
	prefix := "/api/v1/attack-targets?runId=run&generation=2&requesterNodeId="
	for _, id := range []string{"absent", "bootstrap", "retired"} {
		if got := request(prefix + id); got.Code != http.StatusConflict {
			t.Fatalf("requester %s accepted: %d", id, got.Code)
		}
	}
	got := request(prefix + "requester")
	if got.Code != http.StatusOK {
		t.Fatal(got.Code, got.Body.String())
	}
	var targets []model.AttackTarget
	if err := json.Unmarshal(got.Body.Bytes(), &targets); err != nil {
		t.Fatal(err)
	}
	found := map[string]model.AttackTarget{}
	for _, target := range targets {
		found[target.NodeID] = target
	}
	if _, ok := found["other-run"]; ok {
		t.Fatal("cross-run target exposed")
	}
	if _, ok := found["old-generation"]; ok {
		t.Fatal("cross-generation target exposed")
	}
	if !found["publisher"].Ready || found["publisher"].AuthorPeerID != "virtual-signing-id" || found["retired"].Ready || found["anonymous"].AuthorPeerID != "" {
		t.Fatalf("bad signing/readiness projection: %+v", found)
	}
	if found["requester"].AuthorPeerID != "transport-requester" {
		t.Fatal("host author fallback missing")
	}
	// Cached bytes are immutable and the caller is validated on every request.
	if repeated := request(prefix + "requester"); repeated.Body.String() != got.Body.String() {
		t.Fatal("registry cache changed response")
	}
}
func TestAttackRegistryRequiresInternalAuthentication(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/attack-targets", nil)
	if !internalAuthPath(request) {
		t.Fatal("Peer token is not accepted for the internal target registry")
	}
	s := New(ServerConfig{DataDir: t.TempDir(), Token: "secret"}, nil)
	recorder := httptest.NewRecorder()
	s.withAuthentication(http.HandlerFunc(s.handleAttackTargets)).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous registry request accepted: %d", recorder.Code)
	}
}

func TestAttackRegistryRejectsStaleCallerGeneration(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.state.mu.Lock()
	s.state.setNodeLocked(model.Node{ID: "requester", RunID: "run", Generation: 3, Role: "worker", State: model.NodeReady, PeerID: "current-transport", Type: "full", Metadata: map[string]string{"pubsubEnabled": "true"}})
	s.state.mu.Unlock()
	for _, item := range []struct {
		value  string
		status int
	}{{"3", http.StatusOK}, {"2", http.StatusConflict}, {"", http.StatusBadRequest}, {"-1", http.StatusBadRequest}, {"18446744073709551616", http.StatusBadRequest}, {"bad", http.StatusBadRequest}} {
		w := httptest.NewRecorder()
		s.handleAttackTargets(w, httptest.NewRequest(http.MethodGet, "/api/v1/attack-targets?runId=run&requesterNodeId=requester&generation="+item.value, nil))
		if w.Code != item.status {
			t.Fatalf("generation=%q status=%d want=%d body=%s", item.value, w.Code, item.status, w.Body.String())
		}
	}
}

type attackBlockedWriter struct {
	header           http.Header
	entered, release chan struct{}
}

func (w *attackBlockedWriter) Header() http.Header { return w.header }
func (*attackBlockedWriter) WriteHeader(int)       {}
func (w *attackBlockedWriter) Write(data []byte) (int, error) {
	close(w.entered)
	<-w.release
	return len(data), nil
}
func TestAttackRegistrySlowClientDoesNotHoldCacheLock(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.state.mu.Lock()
	s.state.setNodeLocked(model.Node{ID: "requester", RunID: "run", Role: "worker", State: model.NodeReady})
	s.state.mu.Unlock()
	path := "/api/v1/attack-targets?runId=run&requesterNodeId=requester&generation=0"
	slow := &attackBlockedWriter{header: http.Header{}, entered: make(chan struct{}), release: make(chan struct{})}
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		s.handleAttackTargets(slow, httptest.NewRequest(http.MethodGet, path, nil))
	}()
	<-slow.entered
	defer func() { close(slow.release); <-firstDone }()
	done := make(chan int, 1)
	go func() {
		w := httptest.NewRecorder()
		s.handleAttackTargets(w, httptest.NewRequest(http.MethodGet, path, nil))
		done <- w.Code
	}()
	select {
	case code := <-done:
		if code != http.StatusOK {
			t.Fatal(code)
		}
	case <-time.After(time.Second):
		t.Fatal("slow client held registry cache lock")
	}
}

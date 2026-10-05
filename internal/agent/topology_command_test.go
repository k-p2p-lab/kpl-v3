package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

func topologyProxyTestID(t *testing.T) string {
	t.Helper()
	hash, err := multihash.Sum([]byte("proxy-peer"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return peer.ID(hash).String()
}

func topologyProxyRequest(stage string) model.TopologyRequest {
	return model.TopologyRequest{RunID: "run", Generation: 2, TopologyID: "plan", Topic: "topic", Stage: stage}
}

func topologyProxyAck(id, stage string) model.TopologyResponse {
	return model.TopologyResponse{NodeID: "node", PeerID: id, TopologyID: "plan", Topic: "topic", Stage: stage, Frozen: stage == "apply", Neighbors: []string{}}
}

func TestTopologyProxyForwardsIdentityAndUsesCommandDeadline(t *testing.T) {
	id := topologyProxyTestID(t)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/topology" || r.Header.Get("X-KPL-Node-ID") != "node" || r.Header.Get("Authorization") != "Bearer secret-test-token" {
			t.Error("topology request lost route, identity or authentication")
		}
		var request model.TopologyRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		time.Sleep(15 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(topologyProxyAck(id, request.Stage))
	}))
	defer endpoint.Close()
	proc := meshFreezeTestProcess(endpoint.URL)
	proc.node.PeerID = id
	s := &Server{client: endpoint.Client(), processes: map[string]*process{"node": proc}}
	s.client.Timeout = time.Millisecond
	s.config.Token = "secret-test-token"
	for _, stage := range []string{"prepare", "apply"} {
		body, _ := json.Marshal(topologyProxyRequest(stage))
		r := httptest.NewRequest(http.MethodPost, "/api/v1/nodes/node/topology", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer secret-test-token")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			t.Fatalf("stage %s: status=%d body=%s", stage, w.Code, w.Body)
		}
		if (proc.node.Metadata["meshFrozen"] == "true") != (stage == "apply") {
			t.Fatalf("stage %s changed freeze state incorrectly", stage)
		}
	}
	if s.client.Timeout != time.Millisecond {
		t.Fatal("topology changed the shared HTTP client timeout")
	}
}

func TestTopologyProxyRejectsScopeChangesBeforeNetwork(t *testing.T) {
	id := topologyProxyTestID(t)
	var calls atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer endpoint.Close()
	for _, name := range []string{"stopped", "old run", "old generation", "fenced", "global fence", "disabled", "invalid stage"} {
		t.Run(name, func(t *testing.T) {
			proc := meshFreezeTestProcess(endpoint.URL)
			proc.node.PeerID = id
			s := &Server{client: endpoint.Client(), processes: map[string]*process{"node": proc}}
			request := topologyProxyRequest("prepare")
			switch name {
			case "stopped":
				proc.exited = true
			case "old run":
				request.RunID = "old"
			case "old generation":
				request.Generation--
			case "fenced":
				s.runFences = map[string]uint64{"run": 2}
			case "global fence":
				s.fencingAll = true
			case "disabled":
				proc.node.Metadata["meshFreezeEnabled"] = "false"
			case "invalid stage":
				request.Stage = "wrong"
			}
			if _, err := s.proxyTopology(t.Context(), "node", request); err == nil {
				t.Fatal("invalid target accepted")
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid commands dispatched: %d", calls.Load())
	}
}

func TestTopologyProxyRejectsWrongOrOversizedAcknowledgement(t *testing.T) {
	id := topologyProxyTestID(t)
	for _, name := range []string{"different plan", "different neighbors", "not frozen", "trailing JSON", "unknown field", "oversized"} {
		t.Run(name, func(t *testing.T) {
			ack := topologyProxyAck(id, "apply")
			switch name {
			case "different plan":
				ack.TopologyID = "other"
			case "different neighbors":
				ack.Neighbors = []string{id}
			case "not frozen":
				ack.Frozen = false
			}
			body, _ := json.Marshal(ack)
			switch name {
			case "trailing JSON":
				body = append(body, []byte("{}")...)
			case "unknown field":
				body = append(body[:len(body)-1], []byte(",\"unknown\":true}")...)
			case "oversized":
				body = append(body, []byte(strings.Repeat(" ", model.MaxTopologyRequestBytes))...)
			}
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(body) }))
			defer endpoint.Close()
			proc := meshFreezeTestProcess(endpoint.URL)
			proc.node.PeerID = id
			s := &Server{client: endpoint.Client(), processes: map[string]*process{"node": proc}}
			if _, err := s.proxyTopology(t.Context(), "node", topologyProxyRequest("apply")); err == nil {
				t.Fatal("bad acknowledgement accepted")
			}
			if proc.node.Metadata["meshFrozen"] == "true" {
				t.Fatal("invalid acknowledgement changed Agent metadata")
			}
		})
	}
}

func TestTopologyProxyRechecksGenerationAfterNetworkWithoutHoldingLock(t *testing.T) {
	id := topologyProxyTestID(t)
	started, release := make(chan struct{}), make(chan struct{})
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_ = json.NewEncoder(w).Encode(topologyProxyAck(id, "apply"))
	}))
	defer endpoint.Close()
	proc := meshFreezeTestProcess(endpoint.URL)
	proc.node.PeerID = id
	s := &Server{client: endpoint.Client(), processes: map[string]*process{"node": proc}}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.proxyTopology(ctx, "node", topologyProxyRequest("apply")); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("request not sent")
	}
	changed := make(chan struct{})
	go func() { s.mu.Lock(); proc.node.Generation++; s.mu.Unlock(); close(changed) }()
	select {
	case <-changed:
	case <-ctx.Done():
		t.Fatal("network request held Agent lock")
	}
	close(release)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("late acknowledgement accepted after generation changed")
		}
	case <-ctx.Done():
		t.Fatal("proxy did not finish")
	}
	if proc.node.Metadata["meshFrozen"] == "true" {
		t.Fatal("late acknowledgement changed replacement metadata")
	}
}

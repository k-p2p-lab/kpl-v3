package peer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/k-p2p-lab/v3/internal/model"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestFetchBootstrapSendsRunNamespace(t *testing.T) {
	const runID = "run with/+reserved?characters"
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer internal-test-key" {
			t.Error("bootstrap request omitted internal authentication")
		}
		if r.URL.Path != "/api/v1/bootstrap" {
			t.Errorf("bootstrap path = %q", r.URL.Path)
		}
		if got := r.URL.Query().Get("runId"); got != runID {
			t.Errorf("runId = %q, want %q", got, runID)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]bootstrapNode{{
			NodeID: "boot-a", PeerID: "peer-a", Addresses: []string{"/ip4/127.0.0.1/tcp/10001"},
		}})
	}))
	t.Cleanup(registry.Close)

	server := &Server{config: model.PeerProcessConfig{
		Token:         "internal-test-key",
		Node:          model.Node{RunID: runID},
		ControllerURL: registry.URL,
	}}
	nodes, err := server.fetchBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].NodeID != "boot-a" {
		t.Fatalf("bootstrap nodes = %+v", nodes)
	}
}

func TestDeterministicIdentityIsNamespacedByRun(t *testing.T) {
	peerID := func(runID string, seed int64) corepeer.ID {
		t.Helper()
		privateKey, err := deterministicIdentity(runID, seed)
		if err != nil {
			t.Fatal(err)
		}
		id, err := corepeer.IDFromPrivateKey(privateKey)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	first := peerID("run-a", 42)
	if again := peerID("run-a", 42); again != first {
		t.Fatalf("same run and seed produced %q then %q", first, again)
	}
	if otherRun := peerID("run-b", 42); otherRun == first {
		t.Fatalf("different runs produced the same peer ID %q", first)
	}
	if otherSeed := peerID("run-a", 43); otherSeed == first {
		t.Fatalf("different seeds produced the same peer ID %q", first)
	}
}

func TestDeterministicIdentityRejectsEmptyRunNamespace(t *testing.T) {
	if _, err := deterministicIdentity("", 42); err == nil {
		t.Fatal("empty run namespace was accepted")
	}
}

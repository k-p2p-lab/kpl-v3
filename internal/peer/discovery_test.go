package peer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	corenetwork "github.com/libp2p/go-libp2p/core/network"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestSelectDiscoveryPeersIsStableAndPrefersSubscribers(t *testing.T) {
	nodes := bootstrapTestNodes(t, 10)
	for index := range nodes {
		nodes[index].NodeID = fmt.Sprintf("candidate-%02d", index)
		nodes[index].Subscribed = index < 2
	}
	candidates := append([]bootstrapNode(nil), nodes[:9]...)
	candidates = append(candidates,
		bootstrapNode{NodeID: "requester", PeerID: nodes[9].PeerID, Addresses: nodes[9].Addresses, Subscribed: true},
		bootstrapNode{NodeID: "invalid-peer", PeerID: "not-a-peer-id", Addresses: []string{"/ip4/127.0.0.1/tcp/20000"}, Subscribed: true},
		bootstrapNode{NodeID: "invalid-address", PeerID: nodes[9].PeerID, Addresses: []string{"not-a-multiaddr"}, Subscribed: true},
		bootstrapNode{NodeID: "duplicate-fallback", PeerID: nodes[0].PeerID, Addresses: nodes[0].Addresses},
	)

	selected := selectDiscoveryPeers("requester", "topic-a", candidates, 4)
	if len(selected) != 4 {
		t.Fatalf("selected %d peers, want 4: %+v", len(selected), selected)
	}
	if !selected[0].Subscribed || !selected[1].Subscribed || selected[2].Subscribed || selected[3].Subscribed {
		t.Fatalf("subscriber preference not preserved: %+v", selected)
	}
	selectedIDs := make(map[string]struct{}, len(selected))
	for _, candidate := range selected {
		if _, duplicate := selectedIDs[candidate.PeerID]; duplicate {
			t.Fatalf("duplicate peer selected: %+v", selected)
		}
		selectedIDs[candidate.PeerID] = struct{}{}
	}

	reversed := append([]bootstrapNode(nil), candidates...)
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	if reordered := selectDiscoveryPeers("requester", "topic-a", reversed, 4); !reflect.DeepEqual(selected, reordered) {
		t.Fatalf("registry order changed rendezvous selection:\nfirst=%+v\nsecond=%+v", selected, reordered)
	}

	newSubscriber := nodes[9]
	newSubscriber.NodeID = "new-subscriber"
	newSubscriber.Subscribed = true
	changed := selectDiscoveryPeers("requester", "topic-a", append(candidates, newSubscriber), 4)
	intersection := 0
	for _, candidate := range changed {
		if _, exists := selectedIDs[candidate.PeerID]; exists {
			intersection++
		}
	}
	if intersection < 3 {
		t.Fatalf("one added candidate displaced more than one rendezvous selection: before=%+v after=%+v", selected, changed)
	}
	if !changed[0].Subscribed || !changed[1].Subscribed || !changed[2].Subscribed {
		t.Fatalf("new subscriber did not take priority over fallback: %+v", changed)
	}
}

func TestFetchDiscoveryPeersEncodesRegistryQuery(t *testing.T) {
	nodes := bootstrapTestNodes(t, 1)
	nodes[0].NodeID = "candidate"
	nodes[0].Subscribed = true
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer internal-test-key" {
			t.Error("discovery request omitted internal authentication")
		}
		if r.URL.Path != "/api/v1/discovery" || r.URL.Query().Get("runId") != "run with spaces" ||
			r.URL.Query().Get("topic") != "topic/a b" || r.URL.Query().Get("requesterNodeId") != "requester" {
			t.Errorf("unexpected discovery request: %s", r.URL.String())
		}
		_ = json.NewEncoder(w).Encode(nodes)
	}))
	defer registry.Close()

	server := &Server{config: model.PeerProcessConfig{
		ControllerURL: registry.URL,
		Token:         "internal-test-key",
		Node:          model.Node{ID: "requester", RunID: "run with spaces"},
	}}
	got, err := server.fetchDiscoveryPeers(context.Background(), "topic/a b")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, nodes) {
		t.Fatalf("discovery response=%+v, want %+v", got, nodes)
	}
}

func TestConnectDiscoveryRoundDialsSelectedPeer(t *testing.T) {
	target, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.DisableMetrics())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Close() })
	addresses := make([]string, 0, len(target.Addrs()))
	for _, address := range target.Addrs() {
		addresses = append(addresses, address.String())
	}
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]bootstrapNode{{
			NodeID: "target", PeerID: target.ID().String(), Addresses: addresses, Subscribed: true,
		}})
	}))
	defer registry.Close()

	config := model.NodeConfig{GossipSub: model.GossipSubConfig{Topics: []string{"topic-a"}}}.WithDefaults()
	targetCount := 1
	config.GossipSub.Params.DHigh = &targetCount
	server := &Server{
		config: model.PeerProcessConfig{
			ControllerURL: registry.URL,
			Node:          model.Node{ID: "requester", RunID: "run-a"},
			NodeConfig:    config,
		},
		host: newConfigTestHost(t),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	server.connectDiscoveryRound(ctx)
	if connected := server.host.Network().Connectedness(target.ID()); connected != corenetwork.Connected {
		t.Fatalf("discovered peer connectedness=%s, want connected", connected)
	}
}

func TestStartPubSubRunsDiscoveryImmediately(t *testing.T) {
	requested := make(chan struct{}, 1)
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/discovery" {
			select {
			case requested <- struct{}{}:
			default:
			}
			_, _ = w.Write([]byte("[]"))
			return
		}
		http.NotFound(w, r)
	}))
	defer registry.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := model.NodeConfig{GossipSub: model.GossipSubConfig{Topics: []string{"topic-a"}}}.WithDefaults()
	node := model.Node{ID: "requester", RunID: "run-a"}
	server := &Server{
		config: model.PeerProcessConfig{ControllerURL: registry.URL, Node: node, NodeConfig: config},
		host:   newConfigTestHost(t), topics: make(map[string]*pubsub.Topic),
		telemetry: newTelemetry(node, "", "", nil),
	}
	defer server.Close()
	if err := server.startPubSub(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requested:
	case <-time.After(2 * time.Second):
		t.Fatal("startPubSub did not run the discovery registry request immediately")
	}
}

func TestConnectDiscoveryCandidatesAdvancesAfterFailure(t *testing.T) {
	nodes := bootstrapTestNodes(t, 2)
	nodes[0].NodeID = "first"
	nodes[1].NodeID = "fallback"
	plans := []discoveryTopicPlan{{topic: "topic-a", needed: 1, candidates: nodes}}
	var attempts []string
	connectDiscoveryCandidates(context.Background(), plans, 1, nil, func(_ context.Context, info corepeer.AddrInfo) error {
		attempts = append(attempts, info.ID.String())
		if info.ID.String() == nodes[0].PeerID {
			return errors.New("unreachable")
		}
		return nil
	})
	if !reflect.DeepEqual(attempts, []string{nodes[0].PeerID, nodes[1].PeerID}) {
		t.Fatalf("dial attempts=%v, want failed candidate followed by fallback", attempts)
	}
	if plans[0].needed != 0 {
		t.Fatalf("successful fallback did not fill topic deficit: %+v", plans[0])
	}
}

func TestConnectDiscoveryCandidatesSharesCapFairlyAcrossTopics(t *testing.T) {
	nodes := bootstrapTestNodes(t, 4)
	peerTopic := make(map[string]string, len(nodes))
	for index := range nodes {
		nodes[index].NodeID = fmt.Sprintf("candidate-%d", index)
		if index < 2 {
			peerTopic[nodes[index].PeerID] = "topic-a"
		} else {
			peerTopic[nodes[index].PeerID] = "topic-b"
		}
	}
	plans := []discoveryTopicPlan{
		{topic: "topic-a", needed: 2, candidates: nodes[:2]},
		{topic: "topic-b", needed: 2, candidates: nodes[2:]},
	}
	connectedByTopic := map[string]int{}
	var mu sync.Mutex
	connectDiscoveryCandidates(context.Background(), plans, 2, nil, func(_ context.Context, info corepeer.AddrInfo) error {
		mu.Lock()
		connectedByTopic[peerTopic[info.ID.String()]]++
		mu.Unlock()
		return nil
	})
	if connectedByTopic["topic-a"] != 1 || connectedByTopic["topic-b"] != 1 {
		t.Fatalf("cap was not shared round-robin across topics: %v", connectedByTopic)
	}
	if plans[0].needed != 1 || plans[1].needed != 1 {
		t.Fatalf("topic deficits after capped round=%+v, want one remaining each", plans)
	}
}

func TestDiscoveryConnectionBudgetMatchesHostLimitSemantics(t *testing.T) {
	zero, ten := 0, 10
	for _, test := range []struct {
		name     string
		limit    *int
		current  int
		required int
		want     int
	}{
		{name: "nil is unlimited", required: 4, limit: nil, want: 4},
		{name: "zero is unlimited", required: 4, limit: &zero, want: 4},
		{name: "positive leaves remaining slots", current: 8, required: 4, limit: &ten, want: 2},
		{name: "positive exhausted", current: 10, required: 4, limit: &ten, want: 0},
		{name: "no deficit", current: 2, required: 0, limit: &ten, want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := discoveryConnectionBudget(test.limit, test.current, test.required); got != test.want {
				t.Fatalf("budget=%d, want %d", got, test.want)
			}
		})
	}
}

func TestDiscoveryFailureBackoffExpires(t *testing.T) {
	nodes := bootstrapTestNodes(t, 1)
	peerID, err := corepeer.Decode(nodes[0].PeerID)
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{}
	now := time.Now()
	server.recordDiscoveryFailure(peerID, now.Add(time.Second))
	if !server.discoveryRetryPending(peerID, now) {
		t.Fatal("new discovery failure did not enter retry backoff")
	}
	server.pruneDiscoveryFailures(now.Add(time.Second))
	if server.discoveryRetryPending(peerID, now.Add(time.Second)) {
		t.Fatal("expired discovery failure remained in retry backoff")
	}
}

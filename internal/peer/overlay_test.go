package peer

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestMeshSnapshotFollowsRouterCallbacksDespiteTelemetryLoss(t *testing.T) {
	s := &Server{telemetry: &telemetry{node: model.Node{ID: "node", RunID: "run"}, events: make(chan model.TraceEvent)}}
	s.mesh.configure(true)
	tracer := &messageTracer{server: s}
	first, second := corepeer.ID("first"), corepeer.ID("second")
	tracer.Join("a")
	tracer.Join("b")
	tracer.Graft(second, "a")
	tracer.Graft(first, "a")
	tracer.Graft(first, "a")
	tracer.Graft(first, "b")
	// The separate event sink cannot accept any events. Losing more than the
	// Controller's recent-event limit does not change the authoritative set.
	for i := 0; i < 350; i++ {
		topic := "a"
		(&gossipTracer{telemetry: s.telemetry}).Trace(&pubsubpb.TraceEvent{Type: pubsubpb.TraceEvent_GRAFT.Enum(), Graft: &pubsubpb.TraceEvent_Graft{PeerID: []byte(first), Topic: &topic}})
	}
	if s.telemetry.dropped.Load() != 350 {
		t.Fatal("test did not saturate the event queue")
	}
	before := time.Now().UTC()
	mesh, observed := s.mesh.snapshot()
	expected := []string{first.String(), second.String()}
	sort.Strings(expected)
	if observed.Before(before) || !reflect.DeepEqual(mesh["a"], expected) || !reflect.DeepEqual(mesh["b"], []string{first.String()}) {
		t.Fatalf("mesh=%v observed=%s", mesh, observed)
	}
	mesh["a"][0] = "mutated-copy"
	delete(mesh, "b")
	if copy, _ := s.mesh.snapshot(); !reflect.DeepEqual(copy["a"], expected) || len(copy["b"]) != 1 {
		t.Fatalf("snapshot aliases tracker state: %v", copy)
	}
	tracer.Prune(second, "a")
	tracer.Prune(second, "a")
	tracer.RemovePeer(first)
	mesh, _ = s.mesh.snapshot()
	if len(mesh["a"]) != 0 || len(mesh["b"]) != 0 {
		t.Fatalf("disconnect did not clear every topic: %v", mesh)
	}
	tracer.Leave("a")
	tracer.Prune(first, "a")
	mesh, _ = s.mesh.snapshot()
	if _, exists := mesh["a"]; exists {
		t.Fatal("trailing PRUNE recreated a departed topic")
	}
	tracer.Join("a")
	tracer.Graft(second, "a")
	mesh, _ = s.mesh.snapshot()
	if !reflect.DeepEqual(mesh["a"], []string{second.String()}) {
		t.Fatalf("rejoined topic retained stale peers: %v", mesh)
	}
}

func TestMeshSnapshotConcurrentCallbacksAndCopies(t *testing.T) {
	var tracker meshTracker
	tracker.configure(true)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := corepeer.ID(string(rune('a' + i)))
			for j := 0; j < 100; j++ {
				tracker.graft(id, "topic")
				mesh, _ := tracker.snapshot()
				if peers := mesh["topic"]; len(peers) > 0 {
					peers[0] = "local-copy"
				}
				tracker.prune(id, "topic")
			}
		}(i)
	}
	wg.Wait()
	mesh, _ := tracker.snapshot()
	if len(mesh["topic"]) != 0 {
		t.Fatalf("concurrent tracker retained departed peers: %v", mesh)
	}
}

func TestPeerStatusReportsActualRoutingTableAndClearsOldOverlays(t *testing.T) {
	reports := make(chan model.Node, 2)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var node model.Node
		if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		reports <- node
		w.WriteHeader(http.StatusAccepted)
	}))
	defer endpoint.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host := newConfigTestHost(t)
	instance, err := dht.New(ctx, host, dht.Mode(dht.ModeServer), dht.DisableAutoRefresh())
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	var ids []corepeer.ID
	for _, node := range bootstrapTestNodes(t, 2) {
		id, err := corepeer.Decode(node.PeerID)
		if err != nil {
			t.Fatal(err)
		}
		if added, err := instance.RoutingTable().TryAddPeer(id, true, false); err != nil || !added {
			t.Fatalf("add actual routing-table peer: added=%v error=%v", added, err)
		}
		ids = append(ids, id)
	}
	node := model.Node{ID: "node", RunID: "run", RoutingPeers: []string{"stale-routing"}, ConnectedPeers: []string{"stale-connection"}, MeshPeers: map[string][]string{"stale-topic": {"stale-peer"}}}
	s := &Server{config: model.PeerProcessConfig{Node: node}, host: host, dht: instance, telemetry: newTelemetry(node, endpoint.URL, "", slog.New(slog.NewTextHandler(io.Discard, nil)))}
	s.mesh.configure(true)
	s.mesh.graft(ids[0], "actual-topic")
	if err := s.reportStatus(ctx, model.NodeReady, ""); err != nil {
		t.Fatal(err)
	}
	report := <-reports
	want := []string{ids[0].String(), ids[1].String()}
	sort.Strings(want)
	if !reflect.DeepEqual(report.RoutingPeers, want) || !reflect.DeepEqual(report.MeshPeers, map[string][]string{"actual-topic": {ids[0].String()}}) || len(report.ConnectedPeers) != 0 || report.OverlayObservedAt.IsZero() || report.OverlayObservedAt.Before(report.LastSeen) {
		t.Fatalf("overlay report=%+v", report)
	}
	for _, id := range ids {
		instance.RoutingTable().RemovePeer(id)
	}
	s.mesh.leave("actual-topic")
	if err := s.reportStatus(ctx, model.NodeReady, ""); err != nil {
		t.Fatal(err)
	}
	empty := <-reports
	if len(empty.RoutingPeers) != 0 || len(empty.MeshPeers) != 0 || !empty.OverlayObservedAt.After(report.OverlayObservedAt) {
		t.Fatalf("empty overlay did not clear old state with a new observation: %+v", empty)
	}
}

func TestNonGossipAndDisabledPubSubHaveNoMesh(t *testing.T) {
	for _, router := range []string{"disabled", "floodsub", "randomsub"} {
		t.Run(router, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			config := model.NodeConfig{}.WithDefaults()
			if router == "disabled" {
				value := false
				config.GossipSub.Enabled = &value
			} else {
				config.GossipSub.Router = router
				config = config.WithDefaults()
				if router == "randomsub" {
					degree, size := 3, 10
					config.GossipSub.RandomDegree = &degree
					config.GossipSub.RandomNetworkSize = &size
				}
			}
			s := &Server{config: model.PeerProcessConfig{NodeConfig: config}, host: newConfigTestHost(t), topics: make(map[string]*pubsub.Topic), telemetry: &telemetry{events: make(chan model.TraceEvent, 20)}}
			defer s.Close()
			if router != "disabled" {
				if err := s.startPubSub(ctx); err != nil {
					t.Fatal(err)
				}
			}
			tracer := &messageTracer{server: s}
			tracer.Join("topic")
			tracer.Graft(corepeer.ID("remote"), "topic")
			routing, mesh, observed := s.overlaySnapshot()
			if len(routing) != 0 || len(mesh) != 0 || observed.IsZero() {
				t.Fatalf("inactive overlay reported peers: routing=%v mesh=%v observed=%s", routing, mesh, observed)
			}
		})
	}
}

func TestActualGossipMeshTracksLeaveAndDisconnectWithoutPublishing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	create := func(id string) *Server {
		t.Helper()
		host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.DisableMetrics())
		if err != nil {
			t.Fatal(err)
		}
		config := model.NodeConfig{GossipSub: model.GossipSubConfig{Topics: []string{"a", "b"}}}.WithDefaults()
		disabled := false
		config.GossipSub.AllowPublish = &disabled
		s := &Server{config: model.PeerProcessConfig{Node: model.Node{ID: id, RunID: "run"}, NodeConfig: config}, host: host, topics: make(map[string]*pubsub.Topic), telemetry: &telemetry{node: model.Node{ID: id, RunID: "run"}, events: make(chan model.TraceEvent)}}
		t.Cleanup(s.Close)
		if err := s.startPubSub(ctx); err != nil {
			t.Fatal(err)
		}
		return s
	}
	first, second := create("first"), create("second")
	if err := first.host.Connect(ctx, corepeer.AddrInfo{ID: second.host.ID(), Addrs: second.host.Addrs()}); err != nil {
		t.Fatal(err)
	}
	waitFor := func(description string, ready func(map[string][]string, map[string][]string) bool) {
		t.Helper()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			a, _ := first.mesh.snapshot()
			b, _ := second.mesh.snapshot()
			if ready(a, b) {
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatalf("%s: first=%v second=%v", description, a, b)
			}
		}
	}
	waitFor("two subscribed topics did not form real meshes", func(a, b map[string][]string) bool {
		return reflect.DeepEqual(a["a"], []string{second.host.ID().String()}) && reflect.DeepEqual(a["b"], a["a"]) && reflect.DeepEqual(b["a"], []string{first.host.ID().String()}) && reflect.DeepEqual(b["b"], b["a"])
	})
	first.subs[1].Cancel()
	waitFor("topic leave/prune was not reflected", func(a, b map[string][]string) bool {
		_, exists := a["b"]
		return !exists && len(b["b"]) == 0 && len(a["a"]) == 1 && len(b["a"]) == 1
	})
	if err := first.host.Network().ClosePeer(second.host.ID()); err != nil {
		t.Fatal(err)
	}
	waitFor("disconnected peer remained in the mesh", func(a, b map[string][]string) bool {
		return len(a["a"]) == 0 && len(b["a"]) == 0
	})
	if first.telemetry.dropped.Load() == 0 || second.telemetry.dropped.Load() == 0 {
		t.Fatal("test did not exercise loss of PubSub trace telemetry")
	}
}

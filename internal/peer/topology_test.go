package peer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	corehost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func newTopologyTestServer(t *testing.T, id string) *Server {
	t.Helper()
	on := true
	config := model.PeerProcessConfig{Node: model.Node{ID: id, RunID: "run", Generation: 2}, Token: "test-token",
		NodeConfig: model.NodeConfig{GossipSub: model.GossipSubConfig{MeshFreeze: &on}}.WithDefaults()}
	options, err := hostOptions(config.NodeConfig)
	if err != nil {
		t.Fatal(err)
	}
	h, err := libp2p.New(append(options, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.DisableMetrics())...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	params := pubsub.DefaultGossipSubParams()
	params.HeartbeatInitialDelay = time.Hour
	p, err := pubsub.NewGossipSub(ctx, h, pubsub.WithMeshFreeze(), pubsub.WithGossipSubParams(params))
	if err != nil {
		t.Fatal(err)
	}
	topic, err := p.Join("topology-topic")
	if err != nil {
		t.Fatal(err)
	}
	sub, err := topic.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &Server{config: config, host: h, pubsub: p, topology: topologyCommandState{lifetime: ctx}, topics: map[string]*pubsub.Topic{"topology-topic": topic}, subs: []*pubsub.Subscription{sub},
		telemetry: newTelemetry(config.Node, "", config.Token, logger), logger: logger}
}

func topologyTestNeighbor(s *Server) model.TopologyPeer {
	neighbor := model.TopologyPeer{PeerID: s.host.ID().String()}
	for _, address := range s.host.Addrs() {
		neighbor.Addresses = append(neighbor.Addresses, address.String())
	}
	return neighbor
}

func topologyTestRequest(s *Server, stage string, neighbors ...*Server) model.TopologyRequest {
	r := model.TopologyRequest{RunID: s.config.Node.RunID, Generation: s.config.Node.Generation, TopologyID: "line-plan", Stage: stage, Topic: "topology-topic"}
	for _, neighbor := range neighbors {
		r.Neighbors = append(r.Neighbors, topologyTestNeighbor(neighbor))
	}
	return r
}

func topologyTestHTTP(t *testing.T, ctx context.Context, s *Server, request model.TopologyRequest) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/topology", bytes.NewReader(body)).WithContext(ctx)
	r.Header.Set("X-KPL-Node-ID", s.config.Node.ID)
	r.Header.Set("Authorization", "Bearer "+s.config.Token)
	w := httptest.NewRecorder()
	s.handler().ServeHTTP(w, r)
	return w
}

func TestTopologyHTTPPreparesAppliesAndRetriesDisconnectedPlan(t *testing.T) {
	servers := []*Server{newTopologyTestServer(t, "a"), newTopologyTestServer(t, "b"), newTopologyTestServer(t, "c")}
	servers[0].telemetry.acceptClockEstimate(controllerClockEstimate{offset: 2 * time.Second, uncertainty: time.Millisecond}, time.Now())
	plans := []model.TopologyRequest{
		topologyTestRequest(servers[0], "prepare", servers[1]),
		topologyTestRequest(servers[1], "prepare", servers[2], servers[0]),
		topologyTestRequest(servers[2], "prepare", servers[1]),
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	for i, s := range servers {
		w := topologyTestHTTP(t, ctx, s, plans[i])
		var ack model.TopologyResponse
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &ack) != nil || ack.Frozen || ack.Validate(plans[i], s.config.Node.ID, s.host.ID().String()) != nil {
			t.Fatalf("prepare %d status=%d body=%s", i, w.Code, w.Body)
		}
		if s.meshFrozen.Load() {
			t.Fatal("prepare froze the mesh")
		}
	}
	for i, s := range servers {
		plans[i].Stage = "apply"
		w := topologyTestHTTP(t, ctx, s, plans[i])
		var ack model.TopologyResponse
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &ack) != nil || ack.Validate(plans[i], s.config.Node.ID, s.host.ID().String()) != nil {
			t.Fatalf("apply %d status=%d body=%s", i, w.Code, w.Body)
		}
		snapshot, err := s.pubsub.MeshFreezeSnapshot(ctx)
		if err != nil || !snapshot.Frozen || len(snapshot.Mesh[plans[i].Topic]) != len(plans[i].Neighbors) {
			t.Fatalf("wrong applied mesh: %+v %v", snapshot, err)
		}
	}
	if err := servers[0].topics["topology-topic"].Publish(ctx, []byte("through-fixed-line")); err != nil {
		t.Fatal(err)
	}
	message, err := servers[2].subs[0].Next(ctx)
	if err != nil || string(message.GetData()) != "through-fixed-line" {
		t.Fatalf("fixed line did not forward: %v %v", message, err)
	}
	if err := servers[0].host.Network().ClosePeer(servers[1].host.ID()); err != nil {
		t.Fatal(err)
	}
	for {
		snapshot, err := servers[0].pubsub.MeshFreezeSnapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Active[plans[0].Topic]) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, stage := range []string{"prepare", "apply", "apply"} {
		request := plans[0]
		request.Stage = stage
		w := topologyTestHTTP(t, ctx, servers[0], request)
		if w.Code != http.StatusOK {
			t.Fatalf("disconnected retry %s: %d %s", stage, w.Code, w.Body)
		}
	}
	if servers[0].host.Network().Connectedness(servers[1].host.ID()) == network.Connected {
		t.Fatal("idempotent frozen retry redialed a lost link")
	}
	counts := map[string]int{}
	for len(servers[0].telemetry.events) > 0 {
		event := <-servers[0].telemetry.events
		counts[event.Type]++
		if event.Type == "topology_applied" && (event.Topic != "topology-topic" || event.Fields["clockBasis"] != controllerClockBasis || event.Fields["clockUncertaintyMs"] == nil) {
			t.Fatalf("topology observation lost its topic or synchronized clock evidence: %+v", event)
		}
	}
	if counts["topology_applied"] != 1 || counts["mesh_freeze"] != 1 {
		t.Fatalf("duplicate application observations: %v", counts)
	}
	for _, stage := range []string{"prepare", "apply"} {
		changed := topologyTestRequest(servers[0], stage, servers[2])
		w := topologyTestHTTP(t, ctx, servers[0], changed)
		if w.Code != http.StatusConflict {
			t.Fatalf("same plan ID changed meaning: %d %s", w.Code, w.Body)
		}
	}
}

type topologyNoConnectHost struct {
	corehost.Host
	calls atomic.Int32
}

type topologyDelayedConnectHost struct {
	corehost.Host
}

func (h *topologyDelayedConnectHost) Connect(ctx context.Context, info corepeer.AddrInfo) error {
	timer := time.NewTimer(30 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return h.Host.Connect(ctx, info)
	}
}

func TestTopologyResponseUsesCommandBudgetBeyondDefaultWriteTimeout(t *testing.T) {
	s, neighbor := newTopologyTestServer(t, "target"), newTopologyTestServer(t, "neighbor")
	s.host = &topologyDelayedConnectHost{Host: s.host}
	endpoint := httptest.NewUnstartedServer(s.handler())
	endpoint.Config.WriteTimeout = time.Millisecond
	endpoint.Start()
	defer endpoint.Close()
	body, _ := json.Marshal(topologyTestRequest(s, "prepare", neighbor))
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.URL+"/topology", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-KPL-Node-ID", s.config.Node.ID)
	r.Header.Set("Authorization", "Bearer "+s.config.Token)
	response, err := endpoint.Client().Do(r)
	if err != nil {
		t.Fatalf("ordinary write timeout cut off topology preparation: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("status=%d body=%s", response.StatusCode, body)
	}
}

func (h *topologyNoConnectHost) Connect(context.Context, corepeer.AddrInfo) error {
	h.calls.Add(1)
	return errors.New("unexpected connection")
}

func TestTopologyHTTPRejectsInvalidInputBeforeAnyDial(t *testing.T) {
	s, neighbor := newTopologyTestServer(t, "target"), newTopologyTestServer(t, "neighbor")
	h := &topologyNoConnectHost{Host: s.host}
	s.host = h
	for _, name := range []string{"self", "duplicate", "later bad address", "old generation", "empty topic", "apply before prepare"} {
		t.Run(name, func(t *testing.T) {
			request := topologyTestRequest(s, "prepare", neighbor)
			want := http.StatusBadRequest
			switch name {
			case "self":
				request.Neighbors = []model.TopologyPeer{topologyTestNeighbor(s)}
			case "duplicate":
				request.Neighbors = append(request.Neighbors, request.Neighbors[0])
			case "later bad address":
				bad := topologyTestNeighbor(s)
				bad.Addresses = []string{"broken"}
				request.Neighbors = append(request.Neighbors, bad)
			case "old generation":
				request.Generation--
				want = http.StatusConflict
			case "empty topic":
				request.Topic = ""
			case "apply before prepare":
				request.Stage = "apply"
				want = http.StatusConflict
			}
			w := topologyTestHTTP(t, t.Context(), s, request)
			if w.Code != want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
		})
	}
	if h.calls.Load() != 0 || s.meshFrozen.Load() {
		t.Fatal("invalid input changed network or mesh state")
	}
}

func TestTopologyCanceledPreparationCannotBeApplied(t *testing.T) {
	s, neighbor := newTopologyTestServer(t, "target"), newTopologyTestServer(t, "neighbor")
	request := topologyTestRequest(s, "prepare", neighbor)
	// A request waiting behind another command must be cancelable.
	if err := s.topology.gate.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	w := topologyTestHTTP(t, ctx, s, request)
	s.topology.gate.release()
	if w.Code != http.StatusServiceUnavailable || s.topology.prepared != nil {
		t.Fatalf("canceled prepare accepted: %d %s", w.Code, w.Body)
	}
	// The peer is connected, but never joins the requested topic. Its readiness
	// wait must obey the caller deadline and must not leave an applicable plan.
	request.Topic = "not-subscribed"
	ctx, cancel = context.WithTimeout(t.Context(), 60*time.Millisecond)
	defer cancel()
	w = topologyTestHTTP(t, ctx, s, request)
	if w.Code != http.StatusGatewayTimeout || s.topology.prepared != nil {
		t.Fatalf("failed preparation persisted: %d %s", w.Code, w.Body)
	}
	request.Stage = "apply"
	w = topologyTestHTTP(t, t.Context(), s, request)
	if w.Code != http.StatusConflict || s.meshFrozen.Load() {
		t.Fatalf("failed prepare was applied: %d %s", w.Code, w.Body)
	}
}

func TestTopologyPreparedPlanCannotChangeUnderSameID(t *testing.T) {
	s := newTopologyTestServer(t, "target")
	request := topologyTestRequest(s, "prepare")
	w := topologyTestHTTP(t, t.Context(), s, request)
	if w.Code != http.StatusOK {
		t.Fatalf("empty plan prepare: %d %s", w.Code, w.Body)
	}
	before := slices.Clone(s.topology.prepared.neighbors)
	request.Topic = "changed-topic"
	w = topologyTestHTTP(t, t.Context(), s, request)
	if w.Code != http.StatusConflict || !slices.Equal(before, s.topology.prepared.neighbors) {
		t.Fatalf("same plan ID was replaced: %d %s", w.Code, w.Body)
	}
}

func TestTopologyCanceledAcknowledgementRecoversOnlyConfirmedApplication(t *testing.T) {
	for _, state := range []string{"not applied", "applied", "peer stopped"} {
		t.Run(state, func(t *testing.T) {
			s := newTopologyTestServer(t, "target")
			request := topologyTestRequest(s, "prepare")
			w := topologyTestHTTP(t, t.Context(), s, request)
			if w.Code != http.StatusOK {
				t.Fatalf("prepare: %d %s", w.Code, w.Body)
			}
			request.Stage = "apply"
			if state != "not applied" {
				// Model a PubSub commit whose request acknowledgment was lost.
				if err := s.pubsub.SetMeshAndFreeze(t.Context(), request.Topic, nil); err != nil {
					t.Fatal(err)
				}
			}
			if state == "peer stopped" {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				s.topology.lifetime = ctx
			}
			for range 2 {
				s.recoverTopologyApplication(request, s.topology.prepared, nil)
			}
			count := 0
			for len(s.telemetry.events) > 0 {
				event := <-s.telemetry.events
				if event.Type == "topology_applied" {
					count++
					if event.Fields["recoveredAfterCancellation"] != true || event.Topic != request.Topic {
						t.Fatalf("recovery not identified: %+v", event)
					}
				}
			}
			if state == "applied" {
				if count != 1 || !s.topology.prepared.applied || !s.meshFrozen.Load() {
					t.Fatal("confirmed lost acknowledgement was not recovered exactly once")
				}
			} else if count != 0 || s.topology.prepared.applied {
				t.Fatal("unconfirmed or post-shutdown application was reported")
			}
		})
	}
}

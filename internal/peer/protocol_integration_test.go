package peer

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/discovery"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestConfiguredMessageIDMatchesPublishAndUncachedDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := model.NodeConfig{GossipSub: model.GossipSubConfig{
		Topics: []string{"global", "override"}, MessageID: "sha256",
		TopicOptions: map[string]model.PubSubTopicConfig{"override": {MessageID: "topic-sha256"}},
	}}.WithDefaults()
	node := model.Node{ID: "publisher", RunID: "policy-test"}
	server := &Server{config: model.PeerProcessConfig{Node: node, NodeConfig: config}, host: newConfigTestHost(t),
		topics: make(map[string]*pubsub.Topic), telemetry: &telemetry{node: node, events: make(chan model.TraceEvent, 100)}}
	defer server.Close()
	if err := server.startPubSub(ctx); err != nil {
		t.Fatal(err)
	}
	for _, topic := range config.GossipSub.Topics {
		wire := []byte("same raw bytes")
		publication, err := server.publishMessage(ctx, topic, publication{wire: wire, encoding: "raw"})
		if err != nil {
			t.Fatal(err)
		}
		remote := corepeer.ID("remote")
		message := &pubsub.Message{Message: &pubsubpb.Message{Data: wire, Topic: &topic}, ReceivedFrom: remote}
		expected := hex.EncodeToString([]byte(gossipMessageID(config.GossipSub, topic)(message.Message)))
		event, ok := server.deliveryEvent(message, topic, time.Now())
		duplicate, dupOK := server.duplicateEvent(message, time.Now())
		if !ok || !dupOK || publication.pubsubID != expected || event.MessageID != publication.id || duplicate.MessageID != publication.id {
			t.Fatalf("ID policy correlation differs: publication=%+v delivery=%+v duplicate=%+v", publication, event, duplicate)
		}
	}
}

func TestLocalPublicationEmitsExplicitScopeAndEmptyRemoteCohort(t *testing.T) {
	server, _ := publicationTestServer(t)
	enabled := true
	server.config.NodeConfig.GossipSub.Publish = &model.PubSubPublishConfig{Local: &enabled}
	payload := []byte(`{"topic":"topic-a","payloadSize":32,"targetNodeIds":["remote"],"targetNodes":2}`)
	recorder := httptest.NewRecorder()
	server.handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader(payload)))
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("publish: %d %s", recorder.Code, recorder.Body.String())
	}
	for len(server.telemetry.events) > 0 {
		event := <-server.telemetry.events
		if event.Type != "publish" {
			continue
		}
		targets, ok := event.Fields["targetNodeIds"].([]string)
		if event.Fields["localPublication"] != true || !ok || targets == nil || len(targets) != 0 || event.Fields["targetNodes"] != 0 {
			t.Fatalf("local publication retained remote cohort: %+v", event)
		}
		return
	}
	t.Fatal("missing successful local publication event")
}

func TestReadinessTimeoutDoesNotCreatePublication(t *testing.T) {
	server, _ := publicationTestServer(t)
	one := 1
	server.config.NodeConfig.GossipSub.Publish = &model.PubSubPublishConfig{ReadinessMinPeers: &one, ReadinessTimeout: "20ms"}
	recorder := httptest.NewRecorder()
	server.handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/publish", bytes.NewBufferString(`{"topic":"topic-a"}`)))
	if recorder.Code != http.StatusGatewayTimeout || server.publishSeq.Load() != 0 {
		t.Fatalf("readiness created publication: %d seq=%d", recorder.Code, server.publishSeq.Load())
	}
	for len(server.telemetry.events) > 0 {
		if event := <-server.telemetry.events; event.Type == "publish" {
			t.Fatal("timeout emitted publish event")
		}
	}
}

type recordingProtocolDiscovery struct {
	namespace string
	options   discovery.Options
}

func (d *recordingProtocolDiscovery) Advertise(_ context.Context, ns string, opts ...discovery.Option) (time.Duration, error) {
	d.namespace = ns
	err := d.options.Apply(opts...)
	return d.options.Ttl, err
}
func (d *recordingProtocolDiscovery) FindPeers(_ context.Context, ns string, opts ...discovery.Option) (<-chan corepeer.AddrInfo, error) {
	d.namespace = ns
	err := d.options.Apply(opts...)
	peers := make(chan corepeer.AddrInfo)
	close(peers)
	return peers, err
}
func TestRoutingDiscoveryScopesTopicsByRunAndPreservesOptions(t *testing.T) {
	backend := &recordingProtocolDiscovery{}
	scoped := runScopedDiscovery{Discovery: backend, runID: "run-a"}
	ttl, err := scoped.Advertise(context.Background(), "floodsub:topic", discovery.TTL(2*time.Minute))
	if err != nil || ttl != 2*time.Minute || backend.namespace != "kpl/run-a/floodsub:topic" {
		t.Fatalf("advertise: %s %s %v", backend.namespace, ttl, err)
	}
	if _, err := scoped.FindPeers(context.Background(), "floodsub:topic", discovery.Limit(4)); err != nil || backend.options.Limit != 4 || backend.namespace != "kpl/run-a/floodsub:topic" {
		t.Fatalf("find: %+v %v", backend, err)
	}
}

func TestControllerPubSubDiscoveryUsesExactTopicAndSameRun(t *testing.T) {
	h := newConfigTestHost(t)
	remote := newConfigTestHost(t)
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/discovery" || r.URL.Query().Get("runId") != "run-a" || r.URL.Query().Get("topic") != "topic" || r.URL.Query().Get("requesterNodeId") != "local" {
			t.Errorf("discovery request escaped scope: %s", r.URL)
		}
		json.NewEncoder(w).Encode([]bootstrapNode{{NodeID: "remote", PeerID: remote.ID().String(), Addresses: []string{"/ip4/10.0.0.2/tcp/4001"}, Subscribed: true}})
	}))
	defer registry.Close()
	server := &Server{host: h, config: model.PeerProcessConfig{Node: model.Node{ID: "local", RunID: "run-a"}, ControllerURL: registry.URL}}
	peers, err := (controllerPubSubDiscovery{server: server}).FindPeers(context.Background(), "floodsub:topic", discovery.Limit(1))
	if err != nil {
		t.Fatal(err)
	}
	got, ok := <-peers
	if !ok || got.ID != remote.ID() {
		t.Fatalf("registry adapter returned %+v", got)
	}
	if _, ok := <-peers; ok {
		t.Fatal("discovery exceeded configured limit")
	}
}

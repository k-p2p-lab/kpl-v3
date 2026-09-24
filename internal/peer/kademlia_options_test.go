package peer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-msgio/pbio"
	ma "github.com/multiformats/go-multiaddr"
)

// Option's argument is an upstream internal type. Reflection here only exposes
// configured public callbacks, which are exercised with real addresses/hosts.
func configuredDHTOptions(t *testing.T, config model.KademliaConfig) reflect.Value {
	t.Helper()
	config = (model.NodeConfig{Kademlia: config}).WithDefaults().Kademlia
	options, err := dhtOptions(config)
	if err != nil {
		t.Fatal(err)
	}
	value := reflect.New(reflect.TypeOf(options[0]).In(0).Elem())
	for _, option := range options {
		result := reflect.ValueOf(option).Call([]reflect.Value{value})
		if !result[0].IsNil() {
			t.Fatal(result[0].Interface())
		}
	}
	return value.Elem()
}

func TestKademliaQueryAndAddressPolicies(t *testing.T) {
	address := func(raw string) ma.Multiaddr { return ma.StringCast(raw) }
	allowed := address("/ip4/10.42.1.7/tcp/20000")
	denied := address("/ip4/10.42.9.7/tcp/20000")
	outside := address("/ip4/10.43.1.7/tcp/20000")
	config := model.KademliaConfig{
		QueryFilter:   &model.KademliaFilterConfig{Policy: "private", AllowCIDRs: []string{"10.42.0.0/16"}, DenyCIDRs: []string{"10.42.9.0/24"}},
		AddressFilter: &model.KademliaFilterConfig{Policy: "non-loopback", AllowCIDRs: []string{"10.42.0.0/16"}, DenyCIDRs: []string{"10.42.9.0/24"}},
	}
	fields := configuredDHTOptions(t, config)
	query := fields.FieldByName("QueryPeerFilter").Interface().(dht.QueryFilterFunc)
	for name, test := range map[string]struct {
		addrs []ma.Multiaddr
		want  bool
	}{
		"allowed": {[]ma.Multiaddr{allowed}, true}, "denied": {[]ma.Multiaddr{denied}, false},
		"mixed denied": {[]ma.Multiaddr{allowed, denied}, false}, "outside": {[]ma.Multiaddr{outside}, false}, "empty": {nil, false},
		"unknown hostname": {[]ma.Multiaddr{allowed, address("/dns4/unknown.example/tcp/20000")}, false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := query(nil, corepeer.AddrInfo{Addrs: test.addrs}); got != test.want {
				t.Fatalf("query accepted=%t want=%t", got, test.want)
			}
		})
	}
	filter := fields.FieldByName("AddressFilter").Interface().(func([]ma.Multiaddr) []ma.Multiaddr)
	input := []ma.Multiaddr{denied, allowed, outside, address("/ip4/127.0.0.1/tcp/20000")}
	result := filter(input)
	if len(result) != 1 || !result[0].Equal(allowed) || !input[0].Equal(denied) {
		t.Fatalf("filtered addresses or input changed: %v / %v", result, input)
	}
	for policy, want := range map[string]bool{"public": false, "private": true, "all": true, "non-loopback": true} {
		fields := configuredDHTOptions(t, model.KademliaConfig{QueryFilter: &model.KademliaFilterConfig{Policy: policy}})
		query := fields.FieldByName("QueryPeerFilter").Interface().(dht.QueryFilterFunc)
		if query(nil, corepeer.AddrInfo{Addrs: []ma.Multiaddr{allowed}}) != want {
			t.Fatalf("policy %s mismatched overlay address", policy)
		}
	}
}

func newListeningKademliaHost(t *testing.T) host.Host {
	t.Helper()
	value, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.DisableMetrics())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = value.Close() })
	return value
}

func TestKademliaRoutingFilterUsesConnectedAddresses(t *testing.T) {
	source, target := newConfigTestHost(t), newListeningKademliaHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := source.Connect(ctx, corepeer.AddrInfo{ID: target.ID(), Addrs: target.Addrs()}); err != nil {
		t.Fatal(err)
	}
	fields := configuredDHTOptions(t, model.KademliaConfig{RoutingTableFilter: &model.KademliaFilterConfig{AllowCIDRs: []string{"127.0.0.0/8"}}})
	filter := fields.FieldByName("RoutingTable").FieldByName("PeerFilter").Interface().(dht.RouteTableFilterFunc)
	if !filter(kademliaTestHostOwner{source}, target.ID()) {
		t.Fatal("connected peer in allowed CIDR was rejected")
	}
	fields = configuredDHTOptions(t, model.KademliaConfig{RoutingTableFilter: &model.KademliaFilterConfig{DenyCIDRs: []string{"127.0.0.0/8"}}})
	filter = fields.FieldByName("RoutingTable").FieldByName("PeerFilter").Interface().(dht.RouteTableFilterFunc)
	if filter(kademliaTestHostOwner{source}, target.ID()) {
		t.Fatal("connected peer in denied CIDR was accepted")
	}
}

type kademliaTestHostOwner struct{ value host.Host }

func (owner kademliaTestHostOwner) Host() host.Host { return owner.value }

func TestKademliaStaticBootstrapKeepsOnlyExplicitPeers(t *testing.T) {
	candidate := newConfigTestHost(t)
	first := "/ip4/10.42.1.7/tcp/20000/p2p/" + candidate.ID().String()
	second := "/ip4/10.42.1.8/tcp/20000/p2p/" + candidate.ID().String()
	fields := configuredDHTOptions(t, model.KademliaConfig{BootstrapSource: "static", BootstrapPeers: []string{first, second, first}})
	peers := fields.FieldByName("BootstrapPeers").Interface().(func() []corepeer.AddrInfo)()
	if len(peers) != 1 || peers[0].ID != candidate.ID() || len(peers[0].Addrs) != 2 {
		t.Fatalf("static bootstrap not preserved/deduplicated: %v", peers)
	}
	fields = configuredDHTOptions(t, model.KademliaConfig{BootstrapSource: "none"})
	if got := fields.FieldByName("BootstrapPeers").Interface().(func() []corepeer.AddrInfo)(); len(got) != 0 {
		t.Fatalf("none unexpectedly adds public bootstraps: %v", got)
	}
}

func kademliaTestServer(t *testing.T, config model.KademliaConfig) *Server {
	t.Helper()
	config.DisableAutoRefresh = func() *bool { value := true; return &value }()
	config = (model.NodeConfig{Kademlia: config}).WithDefaults().Kademlia
	return &Server{host: newConfigTestHost(t), config: model.PeerProcessConfig{Node: model.Node{ID: "node", RunID: "run /?"}, NodeConfig: model.NodeConfig{Kademlia: config}}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestKademliaControllerBootstrapRetainsRunNamespace(t *testing.T) {
	requests := make(chan string, 4)
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requests <- r.URL.Query().Get("runId"):
		default:
		}
		if r.URL.Path != "/api/v1/bootstrap" {
			t.Errorf("path=%s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode([]bootstrapNode{})
	}))
	defer registry.Close()
	server := kademliaTestServer(t, model.KademliaConfig{BootstrapSource: "controller"})
	server.config.ControllerURL = registry.URL
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	instance, err := server.newDHT(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	select {
	case got := <-requests:
		if got != server.config.Node.RunID {
			t.Fatalf("bootstrap run=%q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DHT recovery did not query run-scoped controller")
	}
}

func TestKademliaDiversityEnforcesConfiguredTableLimit(t *testing.T) {
	one, two := 1, 2
	server := kademliaTestServer(t, model.KademliaConfig{RoutingTableDiversity: &model.KademliaDiversityConfig{MaxPerCpl: &two, MaxForTable: &one}})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	instance, err := server.newDHT(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	for index := 0; index < 2; index++ {
		target := newListeningKademliaHost(t)
		// Identify must see a real DHT server. A plain host can asynchronously
		// remove the first manual RT entry before the second admission check.
		remote, err := dht.New(ctx, target, dht.Mode(dht.ModeServer), dht.ProtocolPrefix(protocol.ID(server.config.NodeConfig.Kademlia.ProtocolPrefix)), dht.DisableAutoRefresh())
		if err != nil {
			t.Fatal(err)
		}
		defer remote.Close()
		if err := server.host.Connect(ctx, corepeer.AddrInfo{ID: target.ID(), Addrs: target.Addrs()}); err != nil {
			t.Fatal(err)
		}
		added, err := instance.RoutingTable().TryAddPeer(target.ID(), true, false)
		if index == 0 && (err != nil || instance.RoutingTable().Find(target.ID()) != target.ID()) {
			t.Fatalf("first peer rejected: %t %v", added, err)
		}
		if index == 1 && (added || err == nil || !strings.Contains(err.Error(), "diversity")) {
			t.Fatalf("diversity limit not enforced: %t %v", added, err)
		}
	}
}

func TestKademliaDatastoreAndValidatorPoliciesRun(t *testing.T) {
	for _, kind := range []string{"memory", "null"} {
		t.Run(kind, func(t *testing.T) {
			server := kademliaTestServer(t, model.KademliaConfig{Datastore: kind, Validator: &model.KademliaValidatorConfig{Mode: "namespaced", Namespaces: map[string]string{"lab": "bytes", "blocked": "reject"}}})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			instance, err := server.newDHT(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer instance.Close()
			if err := instance.Validator.Validate("/lab/key", []byte("record")); err != nil {
				t.Fatal(err)
			}
			if err := instance.Validator.Validate("/blocked/key", []byte("record")); err == nil {
				t.Fatal("reject namespace accepted a record")
			}
			if err := instance.Validator.Validate("/pk/key", []byte("record")); err == nil {
				t.Fatal("namespaced replacement unexpectedly kept defaults")
			}
			// With no connected peers, PutValue can fail to announce after its
			// successful local write; GetValue still checks the selected store.
			_ = instance.PutValue(ctx, "/lab/key", []byte("record"))
			value, err := instance.GetValue(ctx, "/lab/key", routing.Offline)
			if kind == "memory" && (err != nil || string(value) != "record") {
				t.Fatalf("memory value=%q error=%v", value, err)
			}
			if kind == "null" && err == nil {
				t.Fatalf("null datastore retained %q", value)
			}
		})
	}
	for _, values := range [][][]byte{{[]byte("a"), []byte("z")}, {[]byte("z"), []byte("a")}} {
		index, err := (kademliaBytesValidator{}).Select("/lab/key", values)
		if err != nil || string(values[index]) != "z" {
			t.Fatalf("selection depends on order: %v %v", index, err)
		}
	}
}

func TestKademliaProviderStorePoliciesRun(t *testing.T) {
	size := 2
	for _, kind := range []string{"manager", "null"} {
		t.Run(kind, func(t *testing.T) {
			settings := &model.KademliaProviderStoreConfig{Mode: kind}
			if kind == "manager" {
				settings.CacheSize = &size
				settings.CleanupInterval = "17s"
			}
			server := kademliaTestServer(t, model.KademliaConfig{ProviderStore: settings})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			instance, err := server.newDHT(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer instance.Close()
			store := instance.ProviderStore()
			if err := store.AddProvider(ctx, []byte("key"), corepeer.AddrInfo{ID: server.host.ID()}); err != nil {
				t.Fatal(err)
			}
			records, err := store.GetProviders(ctx, []byte("key"))
			if err != nil {
				t.Fatal(err)
			}
			if kind == "null" && len(records) != 0 {
				t.Fatalf("null store returned providers: %v", records)
			}
			if kind == "manager" {
				if len(records) != 1 || records[0].ID != server.host.ID() {
					t.Fatalf("provider manager lost record: %v", records)
				}
				interval := reflect.ValueOf(store).Elem().FieldByName("cleanupInterval").Int()
				if time.Duration(interval) != 17*time.Second {
					t.Fatalf("provider cleanup interval=%s", time.Duration(interval))
				}
			}
		})
	}
}

func TestKademliaDefaultValidatorKeepsStandardNamespaces(t *testing.T) {
	server := kademliaTestServer(t, model.KademliaConfig{Validator: &model.KademliaValidatorConfig{Namespaces: map[string]string{"lab": "bytes"}}})
	instance, err := server.newDHT(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	validators, ok := instance.Validator.(record.NamespacedValidator)
	if !ok || len(validators) != 3 || validators["ipns"] == nil || validators["lab"] == nil {
		t.Fatalf("namespaces=%v", validators)
	}
	if _, ok := validators["pk"].(record.PublicKeyValidator); !ok {
		t.Fatal("default public-key validator was replaced")
	}
}

type kademliaLogWriter struct{ lines chan string }

func (writer kademliaLogWriter) Write(data []byte) (int, error) {
	select {
	case writer.lines <- string(data):
	default:
	}
	return len(data), nil
}

func TestKademliaRequestLoggingHookObservesIncomingRPC(t *testing.T) {
	server := kademliaTestServer(t, model.KademliaConfig{RequestHook: "log"})
	server.host = newListeningKademliaHost(t)
	logs := make(chan string, 4)
	server.logger = slog.New(slog.NewTextHandler(kademliaLogWriter{logs}, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	instance, err := server.newDHT(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	caller := newConfigTestHost(t)
	if err := caller.Connect(ctx, corepeer.AddrInfo{ID: server.host.ID(), Addrs: server.host.Addrs()}); err != nil {
		t.Fatal(err)
	}
	stream, err := caller.NewStream(ctx, server.host.ID(), protocol.ID(server.config.NodeConfig.Kademlia.ProtocolPrefix+"/kad/1.0.0"))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(2 * time.Second))
	if err := pbio.NewDelimitedWriter(stream).WriteMsg(pb.NewMessage(pb.Message_PING, nil, 0)); err != nil {
		t.Fatal(err)
	}
	var response pb.Message
	if err := pbio.NewDelimitedReader(stream, network.MessageSizeMax).ReadMsg(&response); err != nil {
		t.Fatal(err)
	}
	select {
	case line := <-logs:
		if !strings.Contains(line, "DHT request") || !strings.Contains(line, "type=PING") {
			t.Fatalf("hook log=%s", line)
		}
	case <-ctx.Done():
		t.Fatal("incoming DHT request hook did not log")
	}
}

func TestKademliaExactProtocolAllowsPrivateTuning(t *testing.T) {
	twentyFour, disabled := 24, true
	for name, tuning := range map[string]model.KademliaConfig{
		"bucket size":        {BucketSize: &twentyFour},
		"disabled values":    {DisableValues: &disabled},
		"disabled providers": {DisableProviders: &disabled},
		"custom validator":   {Validator: &model.KademliaValidatorConfig{Mode: "namespaced", Namespaces: map[string]string{"lab": "bytes"}}},
	} {
		t.Run(name, func(t *testing.T) {
			tuning.ProtocolID = "/lab/exact/9.9.9"
			server := kademliaTestServer(t, tuning)
			if err := server.config.NodeConfig.Validate(); err != nil {
				t.Fatal(err)
			}
			instance, err := server.newDHT(context.Background())
			if err != nil {
				t.Fatalf("private exact protocol rejected tuning: %v", err)
			}
			defer instance.Close()
			protocols := server.host.Mux().Protocols()
			if !hasProtocol(protocols, protocol.ID(tuning.ProtocolID)) || hasProtocol(protocols, "/ipfs/kad/1.0.0") || hasProtocol(protocols, protocol.ID(tuning.ProtocolID+"/kad/1.0.0")) {
				t.Fatalf("exact wire protocol changed: %v", protocols)
			}
			if tuning.DisableValues != nil {
				if err := instance.PutValue(context.Background(), "/lab/key", []byte("value")); !errors.Is(err, routing.ErrNotSupported) {
					t.Fatalf("disableValues ineffective: %v", err)
				}
			}
			if tuning.Validator != nil {
				if err := instance.Validator.Validate("/lab/key", []byte("value")); err != nil {
					t.Fatalf("custom validator ineffective: %v", err)
				}
				if err := instance.Validator.Validate("/pk/key", []byte("value")); err == nil {
					t.Fatal("namespaced validator unexpectedly retained pk")
				}
			}
			tuning.ProtocolID = "/ipfs/kad/1.0.0"
			if _, err := kademliaTestServer(t, tuning).newDHT(context.Background()); err == nil {
				t.Fatal("standard exact protocol accepted incompatible tuning")
			}
		})
	}
	standard := kademliaTestServer(t, model.KademliaConfig{ProtocolID: "/ipfs/kad/1.0.0"})
	instance, err := standard.newDHT(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	if !hasProtocol(standard.host.Mux().Protocols(), "/ipfs/kad/1.0.0") {
		t.Fatal("standard exact protocol missing")
	}
}

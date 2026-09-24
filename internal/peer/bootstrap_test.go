package peer

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func bootstrapTestNodes(t *testing.T, count int) []bootstrapNode {
	t.Helper()
	nodes := make([]bootstrapNode, count)
	for i := range nodes {
		key, err := deterministicIdentity("bootstrap-test", int64(i))
		if err != nil {
			t.Fatal(err)
		}
		id, err := corepeer.IDFromPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = bootstrapNode{PeerID: id.String(), Addresses: []string{"/ip4/127.0.0.1/tcp/20000"}}
	}
	return nodes
}

func TestWorkerBootstrapUsesSeededOrderAndStopsAtFirstSuccess(t *testing.T) {
	nodes := bootstrapTestNodes(t, 12)
	original := append([]bootstrapNode(nil), nodes...)
	attempt := func(seed int64) []string {
		var connected []string
		config := model.PeerProcessConfig{Seed: seed, Node: model.Node{Role: "worker"}}
		err := connectBootstrapPeers(context.Background(), config, "", func(context.Context) ([]bootstrapNode, error) { return nodes, nil }, func(_ context.Context, info corepeer.AddrInfo) error {
			connected = append(connected, info.ID.String())
			if len(connected) == 1 {
				return errors.New("first candidate is unreachable")
			}
			if len(connected) > 2 {
				t.Fatal("worker continued after its first successful bootstrap")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(connected) != 2 {
			t.Fatalf("dial attempts=%v", connected)
		}
		return connected
	}
	first := attempt(42)
	if again := attempt(42); !reflect.DeepEqual(first, again) {
		t.Fatalf("same seed produced %v then %v", first, again)
	}
	if other := attempt(99); reflect.DeepEqual(first, other) {
		t.Fatalf("different seeds did not change bootstrap choice: %v", first)
	}
	if !reflect.DeepEqual(nodes, original) {
		t.Fatal("bootstrap shuffled the caller's registry slice")
	}
}

func TestBootBootstrapRetainsAllConnectionsAndSkipsSelf(t *testing.T) {
	nodes := bootstrapTestNodes(t, 4)
	self, err := corepeer.Decode(nodes[0].PeerID)
	if err != nil {
		t.Fatal(err)
	}
	var connected []string
	err = connectBootstrapPeers(context.Background(), model.PeerProcessConfig{Node: model.Node{Role: "boot"}}, self, func(context.Context) ([]bootstrapNode, error) { return nodes, nil }, func(_ context.Context, info corepeer.AddrInfo) error {
		connected = append(connected, info.ID.String())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{nodes[1].PeerID, nodes[2].PeerID, nodes[3].PeerID}
	if !reflect.DeepEqual(connected, want) {
		t.Fatalf("boot connections=%v, want %v", connected, want)
	}
}

func TestInitialBootstrapCanStartEmptyNetwork(t *testing.T) {
	err := connectBootstrapPeers(context.Background(), model.PeerProcessConfig{Node: model.Node{Role: "boot"}}, "", func(context.Context) ([]bootstrapNode, error) { return nil, nil }, func(context.Context, corepeer.AddrInfo) error {
		t.Fatal("empty registry must not be dialed")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBootstrapTimeoutBoundsRegistryFetchAndDial(t *testing.T) {
	nodes := bootstrapTestNodes(t, 2)
	for _, phase := range []string{"fetch", "dial"} {
		t.Run(phase, func(t *testing.T) {
			config := model.PeerProcessConfig{Node: model.Node{Role: "worker"}, NodeConfig: model.NodeConfig{Kademlia: model.KademliaConfig{BootstrapTimeout: "40ms", BootstrapRetryInterval: "1ms"}, Libp2p: model.Libp2pConfig{DialTimeout: "5s"}}}
			parent, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			start := time.Now()
			dials := 0
			err := connectBootstrapPeers(parent, config, "", func(ctx context.Context) ([]bootstrapNode, error) {
				if phase == "fetch" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return nodes, nil
			}, func(ctx context.Context, _ corepeer.AddrInfo) error {
				dials++
				<-ctx.Done()
				return ctx.Err()
			})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error=%v", err)
			}
			if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
				t.Fatalf("overall bootstrap timeout did not bound %s: %s", phase, elapsed)
			}
			if phase == "fetch" && dials != 0 || phase == "dial" && dials != 1 {
				t.Fatalf("%s: unexpected dial count %d", phase, dials)
			}
		})
	}
}

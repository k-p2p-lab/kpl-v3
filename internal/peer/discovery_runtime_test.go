package peer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
)

func TestDiscoveryUsesAppliedProfileDegree(t *testing.T) {
	var candidates []bootstrapNode
	for index := 0; index < 3; index++ {
		h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.DisableMetrics())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Close() })
		node := bootstrapNode{NodeID: fmt.Sprintf("candidate-%d", index), PeerID: h.ID().String(), Subscribed: true}
		for _, addr := range h.Addrs() {
			node.Addresses = append(node.Addresses, addr.String())
		}
		candidates = append(candidates, node)
	}
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(candidates)
	}))
	defer registry.Close()
	for _, stage := range []string{"prepare", "apply"} {
		t.Run(stage, func(t *testing.T) {
			s := newMeshFreezeTestServer(t, false)
			s.config.ControllerURL = registry.URL
			s.config.NodeConfig.GossipSub.Topics = []string{"topic"}
			request := profileTestRequest(t, `{"gossipsub":{"params":{"d":1,"dLow":1,"dHigh":1,"dScore":1,"dOut":0}}}`)
			if _, err := s.executeProfileUpdate(t.Context(), request, nil); err != nil {
				t.Fatal(err)
			}
			want := len(candidates)
			if stage == "apply" {
				request.Stage = "apply"
				if _, err := s.executeProfileUpdate(t.Context(), request, nil); err != nil {
					t.Fatal(err)
				}
				params, err := s.pubsub.GossipSubRuntimeParamsSnapshot(t.Context())
				if err != nil || params.Dhi != 1 {
					t.Fatalf("router did not apply degree: %+v, %v", params, err)
				}
				want = 1
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			s.connectDiscoveryRound(ctx)
			if got := len(s.host.Network().Peers()); got != want {
				t.Fatalf("%s: discovery connected %d peers, want %d", stage, got, want)
			}
		})
	}
}

package scenario

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestProtocolOptionsExampleSurvivesResolutionAndPeerJSON(t *testing.T) {
	data, err := os.ReadFile("../../examples/protocol-options.yaml")
	if err != nil {
		t.Fatal(err)
	}
	scenario, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	node := scenario.Phases[2].Node
	wire, err := json.Marshal(model.PeerProcessConfig{NodeConfig: node})
	if err != nil {
		t.Fatal(err)
	}
	var process model.PeerProcessConfig
	if err := json.Unmarshal(wire, &process); err != nil {
		t.Fatal(err)
	}
	got := process.NodeConfig.WithDefaults()
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
	if got.GossipSub.Score == nil || !got.GossipSub.Score.IsEnabled() || got.GossipSub.Score.AppSpecificScore.Default != 1 || got.GossipSub.Score.AppSpecificWeight != .5 {
		t.Fatalf("scoring was lost between scenario profile and Peer: %+v", got.GossipSub.Score)
	}
	if got.Kademlia.BootstrapSource != "controller" || got.Kademlia.AddressFilter.Policy != "non-loopback" || got.Kademlia.ProviderStore.CacheSize == nil || *got.Kademlia.ProviderStore.CacheSize != 256 {
		t.Fatalf("Kademlia policies lost: %+v", got.Kademlia)
	}
	if got.GossipSub.MessageID != "topic-sha256" || got.GossipSub.Discovery.Connector.Base == nil || *got.GossipSub.Discovery.Connector.Base != 2 || got.GossipSub.TopicOptions["kpl/options"].Validator.Format != "envelope" {
		t.Fatalf("PubSub policies lost: %+v", got.GossipSub)
	}
	if score := scenario.Phases[0].Node.GossipSub.Score; score != nil && score.IsEnabled() {
		t.Fatal("scoring enabled without opt-in")
	}
}

func TestProtocolOptionsRejectUnknownFields(t *testing.T) {
	_, err := Parse([]byte(`version: 2
name: typo
phases:
  - action: join
    group: workers
    role: worker
    count: 1
    node:
      gossipsub:
        peerGater:
          sourceDecya: 0.9
`))
	if err == nil {
		t.Fatal("misspelled policy was silently accepted")
	}
}

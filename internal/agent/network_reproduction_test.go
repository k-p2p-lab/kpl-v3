package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/distribution"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestDirectAgentNetworkDistributionIsSeededAndPersisted(t *testing.T) {
	server, _ := newContainerTestServer(t, map[string]string{"DELAY": "create", "DELAY_MS": "300"})
	requested := model.NetworkConfig{
		Scope: "all", Jitter: "1ms", JitterDistribution: "uniform",
		DelayDistribution: &distribution.Distribution{Model: "normal", Mean: "50ms", Sigma: "10ms", Min: "1ms"},
		TBF:               &model.TBFConfig{RateMbps: 12.5, BurstKbit: 128, Latency: "20ms"},
	}
	var delays []string
	for i, seed := range []int64{42, 42, 43} {
		request := model.CreateNodeRequest{ID: fmt.Sprintf("peer-%d", i), RunID: "run", Group: "workers", Seed: seed, Config: model.NodeConfig{Network: requested}}
		node, err := server.createNode(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		server.mu.RLock()
		configPath := server.processes[node.ID].configPath
		server.mu.RUnlock()
		data, err := os.ReadFile(configPath)
		if err != nil {
			t.Fatal(err)
		}
		var stored model.PeerProcessConfig
		if err := json.Unmarshal(data, &stored); err != nil {
			t.Fatal(err)
		}
		var effective, original model.NetworkConfig
		if err := json.Unmarshal([]byte(node.Metadata["network"]), &effective); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(node.Metadata["networkRequested"]), &original); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(original, requested) {
			t.Fatalf("requested network provenance changed: %+v", original)
		}
		if !reflect.DeepEqual(stored.NodeConfig.Network, effective) || effective.DelayDistribution != nil || effective.Delay == "" {
			t.Fatalf("stored peer config must contain the recorded concrete network: stored=%+v effective=%+v", stored.NodeConfig.Network, effective)
		}
		if err := effective.Validate(); err != nil {
			t.Fatalf("resolved network invalid: %v", err)
		}
		if stored.Seed != seed || node.Metadata["seed"] != strconv.FormatInt(seed, 10) {
			t.Fatal("seed was not retained")
		}
		for _, key := range []string{"network", "networkRequested", "seed"} {
			if stored.Node.Metadata[key] != node.Metadata[key] {
				t.Fatalf("metadata %s differs between returned node and peer.json", key)
			}
		}
		delays = append(delays, effective.Delay)
	}
	if delays[0] != delays[1] {
		t.Fatalf("same seed with different node IDs changed delay: %v", delays)
	}
	if delays[0] == delays[2] {
		t.Fatalf("different seeds produced the same sampled delay: %v", delays)
	}
	if requested.Delay != "" || requested.DelayDistribution == nil {
		t.Fatal("creating peers changed the original profile")
	}
}

func TestDirectAgentZeroDelaySampleRetainsProvenanceWithoutNetAdmin(t *testing.T) {
	server, path := newContainerTestServer(t, map[string]string{"DELAY": "create", "DELAY_MS": "300"})
	node, err := server.createNode(context.Background(), model.CreateNodeRequest{
		ID: "zero-delay", RunID: "run", Group: "workers", Seed: 7,
		Config: model.NodeConfig{Network: model.NetworkConfig{DelayDistribution: &distribution.Distribution{Model: "normal", Mean: "-1s", Sigma: "0s"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var effective, requested model.NetworkConfig
	if err := json.Unmarshal([]byte(node.Metadata["network"]), &effective); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(node.Metadata["networkRequested"]), &requested); err != nil {
		t.Fatal(err)
	}
	if effective.Delay != "0s" || effective.DelayDistribution != nil || effective.Enabled() {
		t.Fatalf("zero sample should disable impairment: %+v", effective)
	}
	if requested.DelayDistribution == nil || requested.DelayDistribution.Mean != "-1s" {
		t.Fatalf("original distribution was lost: %+v", requested)
	}
	waitDockerCall(t, path, "create")
	for _, call := range dockerCalls(t, path) {
		if call.Args[0] == "create" && strings.Contains(strings.Join(call.Args, " "), "NET_ADMIN") {
			t.Fatal("zero effective impairment unnecessarily requested NET_ADMIN")
		}
	}
}

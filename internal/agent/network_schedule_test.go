package agent

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestScheduledNetworkReservesCapabilityAndPersistsClock(t *testing.T) {
	server, path := newContainerTestServer(t, map[string]string{"DELAY": "create", "DELAY_MS": "300"})
	startedAt := time.Now().UTC().Add(-time.Minute)
	network := model.NetworkConfig{Schedule: &model.NetworkSchedule{Reference: "experiment-start", Changes: []model.NetworkChange{{After: "10m", Set: model.NetworkConfig{Delay: "100ms"}}}}}
	node, err := server.createNode(context.Background(), model.CreateNodeRequest{ID: "scheduled", RunID: "run", Group: "workers", ExperimentStartedAt: startedAt, Config: model.NodeConfig{Network: network}})
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
	if !stored.ExperimentStartedAt.Equal(startedAt) || stored.Node.Metadata["networkPending"] != "true" || stored.Node.Metadata["networkSchedule"] == "" || stored.Node.Metadata["network"] != "{}" {
		t.Fatalf("stored schedule: %+v", stored)
	}
	waitDockerCall(t, path, "create")
	found := false
	for _, call := range dockerCalls(t, path) {
		if call.Args[0] == "create" {
			found = true
			if !strings.Contains(strings.Join(call.Args, " "), "--cap-add NET_ADMIN") {
				t.Fatal("delayed-only impairment missing NET_ADMIN")
			}
		}
	}
	if !found {
		t.Fatal("missing Docker create")
	}
	if _, err := server.createNode(context.Background(), model.CreateNodeRequest{ID: "missing-anchor", RunID: "run", Group: "workers", Config: model.NodeConfig{Network: network}}); err == nil {
		t.Fatal("missing experiment clock accepted")
	}
}

func TestNetworkStatusRevisionPreventsRegressingEffectiveConfiguration(t *testing.T) {
	s := topologyStatusServer()
	s.processes["node"].node.Metadata["networkSchedule"] = "plan"
	s.processes["node"].node.Metadata["networkPending"] = "true"
	update := topologyStatusAt(time.Now())
	update.Metadata = map[string]string{"network": `{"delay":"100ms"}`, "networkPending": "false", "networkRevision": "2", "networkAppliedAt": time.Now().UTC().Format(time.RFC3339Nano)}
	if err := s.updateNode(update); err != nil {
		t.Fatal(err)
	}
	if got := s.nodes()[0].Metadata; got["network"] != `{"delay":"100ms"}` || got["networkPending"] != "false" {
		t.Fatalf("effective config not published: %v", got)
	}
	update.LastSeen = update.LastSeen.Add(time.Second)
	update.Metadata["networkRevision"] = "1"
	update.Metadata["network"] = `{"delay":"10ms"}`
	if err := s.updateNode(update); err != nil {
		t.Fatal(err)
	}
	if s.nodes()[0].Metadata["network"] != `{"delay":"100ms"}` {
		t.Fatal("delayed report restored an old tc state")
	}
	for _, raw := range []string{`{"lossPercent":101}`, `null`, `{"unknown":1}`, `{"delay":"100ms"} {}`, `{"schedule":{"changes":[]}}`} {
		update.LastSeen = update.LastSeen.Add(time.Second)
		update.Metadata["networkRevision"] = "3"
		update.Metadata["network"] = raw
		if err := s.updateNode(update); err != nil {
			t.Fatal(err)
		}
		if s.nodes()[0].Metadata["networkRevision"] != "2" {
			t.Fatalf("invalid config accepted: %s", raw)
		}
	}
	update.LastSeen = update.LastSeen.Add(time.Second)
	update.Metadata["networkRevision"] = "2"
	update.Metadata["network"] = `{"delay":"10ms"}`
	if err := s.updateNode(update); err != nil {
		t.Fatal(err)
	}
	if s.nodes()[0].Metadata["network"] != `{"delay":"100ms"}` {
		t.Fatal("same revision rewrote the applied state")
	}
}

package controller

import (
	"encoding/json"
	"maps"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func replayTerminalInventoryHeartbeat(t *testing.T, server *Server, node model.Node) {
	t.Helper()
	// HTTP decoding gives repeated histories distinct maps and slices. Do not
	// accidentally prove only the cheap shared-pointer equality case.
	raw, err := json.Marshal(node)
	if err != nil {
		t.Fatal(err)
	}
	var decoded model.Node
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	agent := server.state.agents["agent"]
	agent.LastSeen = time.Now().UTC()
	if err := server.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{decoded}, Partial: true}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerInventoryRepeatedTerminalHistoryIsNotRetransmitted(t *testing.T) {
	for _, terminal := range []string{model.NodeStopped, model.NodeFailed} {
		t.Run(terminal, func(t *testing.T) {
			server := inventoryTestServer(t, 1)
			node := server.state.nodes["dense-00"]
			node.State = terminal
			node.Metadata = map[string]string{"cleanupComplete": "true"}
			replayTerminalInventoryHeartbeat(t, server, node)
			poll, broker := inventoryDeltaProcess(t, server, nil)
			first, _ := poll()
			if first.Error != "" {
				t.Fatal(first.Error)
			}
			observed := server.state.nodeReportTimes[node.ID]
			for range 3 {
				replayTerminalInventoryHeartbeat(t, server, node)
				proof, stats := poll()
				if proof.Error != "" || proof.Digest != expectedInventoryDigest(broker) || proof.Revision != first.Revision || stats.full || stats.nodes != 0 || stats.bytes > 64<<10 || !server.state.nodeReportTimes[node.ID].Equal(observed) {
					t.Fatalf("identical confirmed terminal history was retransmitted or changed its observation: proof=%+v stats=%+v", proof, stats)
				}
			}
			// No-op inventory publication must still execute admission cleanup
			// and preserve the independently reported physical occupancy.
			server.state.reservations[node.ID] = "agent"
			server.state.uncertainCreates = map[string]uncertainCreate{node.ID: {runID: node.RunID}}
			agent := server.state.agents["agent"]
			agent.ActiveNodes, agent.LastSeen = 2, time.Now().UTC()
			if err := server.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{node}, Partial: true}); err != nil {
				t.Fatal(err)
			}
			if len(server.state.reservations) != 0 || len(server.state.uncertainCreates) != 0 || server.state.agents["agent"].ActiveNodes != 2 {
				t.Fatal("terminal replay optimization skipped real admission or occupancy handling")
			}
		})
	}
}

func TestWorkerInventoryChangedStoppedHistoryStillPublishesCompleteFields(t *testing.T) {
	server := inventoryTestServer(t, 1)
	node := server.state.nodes["dense-00"]
	node.State = model.NodeStopped
	node.Metadata = map[string]string{"cleanupComplete": "true"}
	replayTerminalInventoryHeartbeat(t, server, node)
	poll, broker := inventoryDeltaProcess(t, server, nil)
	first, _ := poll()
	if first.Error != "" {
		t.Fatal(first.Error)
	}
	node.Metadata = maps.Clone(node.Metadata)
	node.Metadata["late-detail"] = "original terminal correction"
	node.PeerScores = maps.Clone(node.PeerScores)
	node.PeerScores["late-final-score"] = -3.25
	node.Profile, node.Error = "corrected-final-profile", "original exit detail"
	node.LastSeen = node.LastSeen.Add(time.Second)
	replayTerminalInventoryHeartbeat(t, server, node)
	proof, stats := poll()
	if proof.Error != "" || proof.Digest != expectedInventoryDigest(broker) || proof.Revision <= first.Revision || stats.nodes != 1 || stats.full {
		t.Fatalf("real terminal metadata/score/clock correction was suppressed: proof=%+v stats=%+v", proof, stats)
	}
}

func TestWorkerInventoryDoesNotSkipUnconfirmedOrUnobservedHeartbeat(t *testing.T) {
	for _, mode := range []string{"active", "unconfirmed", "missing-report-time", "zero-report-time"} {
		t.Run(mode, func(t *testing.T) {
			server := inventoryTestServer(t, 1)
			node := server.state.nodes["dense-00"]
			if mode != "active" {
				node.State, node.Metadata = model.NodeStopped, map[string]string{"cleanupComplete": "true"}
				if mode == "unconfirmed" {
					node.Metadata["cleanupComplete"] = "false"
				}
			}
			replayTerminalInventoryHeartbeat(t, server, node)
			poll, broker := inventoryDeltaProcess(t, server, nil)
			first, _ := poll()
			if first.Error != "" {
				t.Fatal(first.Error)
			}
			switch mode {
			case "missing-report-time":
				delete(server.state.nodeReportTimes, node.ID)
			case "zero-report-time":
				server.state.nodeReportTimes[node.ID] = time.Time{}
			}
			replayTerminalInventoryHeartbeat(t, server, node)
			proof, stats := poll()
			if proof.Error != "" || proof.Digest != expectedInventoryDigest(broker) || proof.Revision <= first.Revision || stats.nodes != 1 || server.state.nodeReportTimes[node.ID].IsZero() {
				t.Fatalf("heartbeat without confirmed stable terminal observation was suppressed: mode=%s proof=%+v stats=%+v", mode, proof, stats)
			}
		})
	}
}

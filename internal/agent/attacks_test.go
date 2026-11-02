package agent

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAttackRuntimeMetadataSurvivesOtherProfileChanges(t *testing.T) {
	enabled := true
	probability := 0.3
	proc := &process{node: model.Node{Role: "worker", Metadata: map[string]string{"pubsubEnabled": "true", "pubsubRouter": "gossipsub"}}}
	now := time.Now().UTC()
	storeProfileAcknowledgement(proc, model.ProfileUpdateResponse{Revision: 1, Effective: model.RuntimeProfilePatch{Sink: &model.ProbabilityAttack{Enabled: &enabled, Probability: &probability}, Forwarding: &model.ProbabilityAttack{Enabled: &enabled, Probability: &probability}}}, now)
	storeProfileAcknowledgement(proc, model.ProfileUpdateResponse{Revision: 2, Effective: model.RuntimeProfilePatch{Network: &model.NetworkConfig{Delay: "1ms"}}}, now)
	var state model.RuntimeProfilePatch
	if err := json.Unmarshal([]byte(proc.node.Metadata["runtimeProfile"]), &state); err != nil {
		t.Fatal(err)
	}
	if state.Sink == nil || !state.Sink.Active() || *state.Sink.Probability != 0.3 || state.Forwarding == nil || !state.Forwarding.Active() || *state.Forwarding.Probability != 0.3 || state.Network == nil {
		t.Fatal("network ACK erased attack state")
	}
}

func TestForwardingStatusRecoveryChecksWorkerAndRouter(t *testing.T) {
	enabled, probability := true, 0.5
	raw, err := json.Marshal(model.RuntimeProfilePatch{Forwarding: &model.ProbabilityAttack{Enabled: &enabled, Probability: &probability}})
	if err != nil {
		t.Fatal(err)
	}
	status := map[string]string{"profileRevision": "1", "runtimeProfile": string(raw), "profileAppliedAt": time.Now().UTC().Format(time.RFC3339Nano)}
	for _, invalid := range []string{"", "role", "router", "disabled"} {
		proc := &process{node: model.Node{Role: "worker", Metadata: map[string]string{"pubsubEnabled": "true", "pubsubRouter": "gossipsub"}}}
		switch invalid {
		case "role":
			proc.node.Role = "boot"
		case "router":
			proc.node.Metadata["pubsubRouter"] = "floodsub"
		case "disabled":
			proc.node.Metadata["pubsubEnabled"] = "false"
		}
		updateProcessProfile(proc, status)
		if (proc.node.Metadata["profileRevision"] == "1") != (invalid == "") {
			t.Fatalf("invalid=%s wrong recovery: %v", invalid, proc.node.Metadata)
		}
	}
}
func TestTerminalHeartbeatRetainsAttackAndAuthorEvidence(t *testing.T) {
	proc := &process{node: model.Node{Metadata: map[string]string{"cleanupComplete": "true", "attackOptions": "options", "attackStats": "counters", "attackRevision": "3", "pubsubAuthorId": "virtual-author", "pubsubAuthorMode": "explicit"}}}
	result := heartbeatNodeStatus(proc)
	for _, key := range []string{"attackOptions", "attackStats", "attackRevision", "pubsubAuthorId", "pubsubAuthorMode"} {
		if result.Metadata[key] != proc.node.Metadata[key] {
			t.Fatalf("terminal projection lost %s", key)
		}
	}
}

func TestAttackPeerStatusReachesHeartbeatWithoutLosingAuthorOrCounters(t *testing.T) {
	proc := &process{node: model.Node{ID: "node", State: model.NodeReady, Metadata: map[string]string{"pubsubEnabled": "true", "pubsubRouter": "gossipsub"}}, apiURL: "http://peer"}
	s := &Server{processes: map[string]*process{"node": proc}}
	at := time.Now().UTC()
	fields := map[string]string{"attackOptions": `{"forwarding":{"enabled":true,"probability":1}}`, "attackStats": `{"forwarding":{"publishWithheld":4}}`, "attackRevision": "2", "attackStatsObservedAt": at.Format(time.RFC3339Nano), "attackRegistryObservedAt": at.Format(time.RFC3339Nano), "pubsubAuthorId": "alternate-public-author", "pubsubAuthorMode": "explicit"}
	if err := s.updateNode(model.Node{ID: "node", State: model.NodeReady, PeerID: "transport", LastSeen: at, Metadata: fields}); err != nil {
		t.Fatal(err)
	}
	for key, value := range fields {
		if proc.node.Metadata[key] != value {
			t.Fatalf("Peer status lost %s before heartbeat: got=%q want=%q", key, proc.node.Metadata[key], value)
		}
	}
	older := model.Node{ID: "node", State: model.NodeReady, LastSeen: at.Add(-time.Second), Metadata: map[string]string{"attackStats": "older", "pubsubAuthorMode": "anonymous", "pubsubAuthorId": ""}}
	if err := s.updateNode(older); err != nil {
		t.Fatal(err)
	}
	if proc.node.Metadata["attackStats"] != fields["attackStats"] {
		t.Fatal("stale Peer status rewound counters")
	}
	if err := s.updateNode(model.Node{ID: "node", State: model.NodeReady, LastSeen: at.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if proc.node.Metadata["pubsubAuthorId"] != fields["pubsubAuthorId"] {
		t.Fatal("absent legacy identity erased observed author")
	}
	proc.node.State = model.NodeStopped
	proc.node.Metadata["cleanupComplete"] = "true"
	status := heartbeatNodeStatus(proc)
	for key, value := range fields {
		if status.Metadata[key] != value {
			t.Fatalf("terminal heartbeat lost %s", key)
		}
	}
}

func TestAttackStatusRejectsRegressedPolicyAndObservationTime(t *testing.T) {
	at := time.Now().UTC()
	for _, incoming := range []map[string]string{
		{"attackOptions": "{}", "attackRevision": "2", "attackStatsObservedAt": at.Add(time.Second).Format(time.RFC3339Nano), "attackStats": "old-revision"},
		{"attackOptions": "{}", "attackRevision": "3", "attackStatsObservedAt": at.Add(-time.Second).Format(time.RFC3339Nano), "attackStats": "older-observation"},
		{"attackStats": "missing-revision"},
	} {
		proc := &process{node: model.Node{ID: "node", State: model.NodeReady, Metadata: map[string]string{"attackRevision": "3", "attackStatsObservedAt": at.Format(time.RFC3339Nano), "attackStats": "current"}}, apiURL: "http://peer"}
		s := &Server{processes: map[string]*process{"node": proc}}
		if err := s.updateNode(model.Node{ID: "node", State: model.NodeReady, LastSeen: at.Add(2 * time.Second), Metadata: incoming}); err != nil {
			t.Fatal(err)
		}
		if proc.node.Metadata["attackStats"] != "current" || proc.node.Metadata["attackRevision"] != "3" {
			t.Fatalf("newer Node time rewound policy/counter observation: %v", proc.node.Metadata)
		}
	}
}

func TestAttackStatusRequiresCompleteObservationAndKeepsAnonymousAuthor(t *testing.T) {
	at := time.Now().UTC()
	proc := &process{node: model.Node{ID: "node", State: model.NodeReady, Metadata: map[string]string{"attackRevision": "0", "attackStatsObservedAt": at.Format(time.RFC3339Nano), "attackStats": "current", "pubsubAuthorId": "author", "pubsubAuthorMode": "explicit"}}, apiURL: "http://peer"}
	s := &Server{processes: map[string]*process{"node": proc}}
	partial := map[string]string{"attackOptions": "{}", "attackRevision": "1", "attackStatsObservedAt": at.Add(time.Second).Format(time.RFC3339Nano), "pubsubAuthorId": "", "pubsubAuthorMode": "anonymous"}
	if err := s.updateNode(model.Node{ID: "node", State: model.NodeReady, LastSeen: at.Add(2 * time.Second), Metadata: partial}); err != nil {
		t.Fatal(err)
	}
	if proc.node.Metadata["attackRevision"] != "0" || proc.node.Metadata["attackStats"] != "current" {
		t.Fatal("partial observation relabeled existing counters")
	}
	if proc.node.Metadata["pubsubAuthorMode"] != "anonymous" || proc.node.Metadata["pubsubAuthorId"] != "" {
		t.Fatal("explicit anonymous identity lost")
	}
	complete := map[string]string{"attackOptions": "{}", "attackStats": "new", "attackRevision": "1", "attackStatsObservedAt": at.Add(time.Second).Format(time.RFC3339Nano)}
	if err := s.updateNode(model.Node{ID: "node", State: model.NodeReady, LastSeen: at.Add(3 * time.Second), Metadata: complete}); err != nil {
		t.Fatal(err)
	}
	if proc.node.Metadata["attackRevision"] != "1" || proc.node.Metadata["attackStats"] != "new" || proc.node.Metadata["pubsubAuthorMode"] != "anonymous" {
		t.Fatal("complete observation or prior identity lost")
	}
}

package controller

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestOriginMetadataInferenceUsesExistingEventsOnly(t *testing.T) {
	epoch := time.Unix(100, 0)
	ev := func(kind, from, to, topic string, seconds float64) model.TraceEvent {
		return model.TraceEvent{Type: kind, PeerID: from, NodeID: from, RemotePeerID: to, Topic: topic, Timestamp: epoch.Add(time.Duration(seconds * float64(time.Second))), Fields: map[string]any{"messageIdCount": 3}}
	}
	graft := ev("graft", "sender", "receiver", "t", 0)
	ihave := ev("send_ihave", "sender", "receiver", "t", 1)
	iwant := ev("send_iwant", "receiver", "sender", "", 2)
	tests := []struct {
		name   string
		events []model.TraceEvent
		at     float64
		want   string
	}{
		{"grafted peer", []model.TraceEvent{graft}, 3, "eager"},
		{"same pair pull sequence", []model.TraceEvent{ihave, iwant}, 3, "lazy"},
		{"receive-side pull metadata", []model.TraceEvent{ev("recv_ihave", "receiver", "sender", "t", 1), ev("recv_iwant", "sender", "receiver", "", 2)}, 3, "lazy"},
		{"IHAVE alone", []model.TraceEvent{ihave}, 3, "unknown"},
		{"IWANT alone", []model.TraceEvent{iwant}, 3, "unknown"},
		{"conflicting push and pull", []model.TraceEvent{graft, ihave, iwant}, 3, "unknown"},
		{"pruned link", []model.TraceEvent{graft, ev("prune", "receiver", "sender", "t", 1)}, 3, "unknown"},
		{"pruned then pull", []model.TraceEvent{graft, ev("prune", "receiver", "sender", "t", .5), ihave, iwant}, 3, "lazy"},
		{"future metadata", []model.TraceEvent{ev("graft", "sender", "receiver", "t", 4), ev("send_ihave", "sender", "receiver", "t", 4), ev("send_iwant", "receiver", "sender", "", 5)}, 3, "unknown"},
		{"wrong topic", []model.TraceEvent{ev("send_ihave", "sender", "receiver", "other", 1), iwant}, 3, "unknown"},
		{"wrong peer", []model.TraceEvent{ihave, ev("send_iwant", "receiver", "other", "", 2)}, 3, "unknown"},
		{"stale advertisement", []model.TraceEvent{ihave, iwant}, 10, "unknown"},
		{"request before advertisement", []model.TraceEvent{ev("send_iwant", "receiver", "sender", "", .5), ihave}, 3, "unknown"},
		{"equal timestamp ordering", []model.TraceEvent{ihave, ev("send_iwant", "receiver", "sender", "", 1)}, 3, "unknown"},
		{"equal timestamp mesh conflict", []model.TraceEvent{graft, ev("prune", "receiver", "sender", "t", 0)}, 3, "unknown"},
		{"leave resets graft", []model.TraceEvent{graft, ev("leave", "receiver", "", "t", 1)}, 3, "unknown"},
		{"stop resets all topics", []model.TraceEvent{graft, ev("measurement_stop", "receiver", "", "", 1)}, 3, "unknown"},
		{"removed connection resets graft", []model.TraceEvent{graft, ev("remove_peer", "receiver", "sender", "", 1)}, 3, "unknown"},
		{"another removed connection preserves graft", []model.TraceEvent{graft, ev("remove_peer", "receiver", "other", "", 1)}, 3, "eager"},
		{"regraft after connection reset", []model.TraceEvent{graft, ev("remove_peer", "receiver", "sender", "", 1), ev("graft", "receiver", "sender", "t", 2)}, 3, "eager"},
		{"removed connection resets pull", []model.TraceEvent{ihave, iwant, ev("remove_peer", "receiver", "sender", "", 2.5)}, 3, "unknown"},
		{"drops are not successful requests", []model.TraceEvent{ihave, ev("drop_iwant", "receiver", "sender", "", 2)}, 3, "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var first []string
			for _, reverse := range []bool{false, true} {
				a := newResearchAccumulator()
				for i := range test.events {
					j := i
					if reverse {
						j = len(test.events) - 1 - i
					}
					a.observe(test.events[j])
				}
				index := newOriginMetadataIndex(a.inference, a.peers)
				got, evidence := index.estimate("sender", "receiver", "t", "", epoch, epoch.Add(time.Duration(test.at*float64(time.Second))))
				if got != test.want || len(evidence) == 0 {
					t.Fatalf("got %s (%v), want %s", got, evidence, test.want)
				}
				if reverse && !reflect.DeepEqual(first, evidence) {
					t.Fatal("log order changed inference evidence")
				}
				first = evidence
			}
		})
	}
}
func TestOriginMetadataDoesNotAcceptUnavailableDirectSource(t *testing.T) {
	a := newResearchAccumulator()
	a.observe(model.TraceEvent{Type: "forward", PeerID: "sender", RemotePeerID: "receiver", Topic: "t", MessageID: "m", Timestamp: time.Unix(100, 0), Fields: map[string]any{"forwardKind": "lazy", "forwardEvidence": "sender-queue-origin-v1"}})
	if len(a.inference) != 0 {
		t.Fatal("obsolete direct-source evidence used")
	}
}

func TestOriginMetadataMatchesSpecificMessageIDs(t *testing.T) {
	epoch := time.Unix(100, 0)
	base := func(kind string, at int, fields map[string]any) model.TraceEvent {
		return model.TraceEvent{Type: kind, PeerID: "sender", RemotePeerID: "receiver", NodeID: "sender", Topic: "t", Timestamp: epoch.Add(time.Duration(at) * time.Second), Fields: fields}
	}
	ihave := base("send_ihave", 1, map[string]any{"messageIdCount": 1, "messageIdsComplete": true, "topicMessageIds": map[string][]string{"t": {"aa"}}})
	request := base("recv_iwant", 2, map[string]any{"messageIdCount": 1, "messageIdsComplete": true, "messageIds": []string{"aa"}})
	rpc := base("rpc_metadata", 3, map[string]any{"direction": "send", "messages": []map[string]any{{"topic": "t", "pubsubMessageId": "aa"}}})
	for _, decoded := range []bool{false, true} {
		a := newResearchAccumulator()
		for _, event := range []model.TraceEvent{ihave, request, rpc} {
			if decoded {
				bytes, _ := json.Marshal(event)
				if err := json.Unmarshal(bytes, &event); err != nil {
					t.Fatal(err)
				}
			}
			a.observe(event)
		}
		index := newOriginMetadataIndex(a.inference, a.peers)
		kind, evidence := index.estimate("sender", "receiver", "t", "aa", epoch, epoch.Add(4*time.Second))
		if kind != "lazy" || !strings.Contains(strings.Join(evidence, " "), "IHAVE and IWANT match") || !strings.Contains(strings.Join(evidence, " "), "data RPC") {
			t.Fatalf("matching evidence lost: %s %v", kind, evidence)
		}
		if kind, _ := index.estimate("sender", "receiver", "t", "bb", epoch, epoch.Add(4*time.Second)); kind != "unknown" {
			t.Fatal("other message's requests classified this delivery")
		}
	}
	request.Fields["messageIds"] = []string{"bb"}
	a := newResearchAccumulator()
	a.observe(ihave)
	a.observe(request)
	if kind, _ := newOriginMetadataIndex(a.inference, a.peers).estimate("sender", "receiver", "t", "aa", epoch, epoch.Add(4*time.Second)); kind != "unknown" {
		t.Fatal("IHAVE and IWANT with different IDs joined")
	}
}
func TestOriginIncompleteMetadataDoesNotProveEager(t *testing.T) {
	epoch := time.Unix(100, 0)
	a := newResearchAccumulator()
	a.observe(model.TraceEvent{Type: "graft", PeerID: "s", RemotePeerID: "r", Topic: "t", Timestamp: epoch})
	a.observe(model.TraceEvent{Type: "recv_iwant", PeerID: "s", RemotePeerID: "r", Topic: "t", Timestamp: epoch.Add(time.Second), Fields: map[string]any{"messageIdCount": 2, "messageIdsComplete": false, "omittedMessageIds": 1, "messageIds": []string{"other"}}})
	kind, evidence := newOriginMetadataIndex(a.inference, a.peers).estimate("s", "r", "t", "missing", epoch, epoch.Add(2*time.Second))
	if kind != "unknown" || !strings.Contains(strings.Join(evidence, " "), "truncated") {
		t.Fatal("missing IDs incorrectly treated as no pull request")
	}
}

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

func TestOriginTopologyApplicationResetsOnlyObservedTopic(t *testing.T) {
	epoch := time.Unix(100, 0)
	event := func(kind, from, to, topic string, seconds float64) model.TraceEvent {
		return model.TraceEvent{Type: kind, PeerID: from, NodeID: from, RemotePeerID: to, Topic: topic,
			Timestamp: epoch.Add(time.Duration(seconds * float64(time.Second))), Fields: map[string]any{"messageIdCount": 1}}
	}
	graft := event("graft", "sender", "receiver", "t", 0)
	otherGraft := event("graft", "sender", "receiver", "other", 0)
	applied := event("topology_applied", "receiver", "", "t", 2)
	applied.Fields = map[string]any{"frozen": true, "neighbors": []string{"sender"}, "topologyId": "applied-plan"}
	legacyApplied := applied
	legacyApplied.Topic = ""
	legacyApplied.Fields = map[string]any{"frozen": true, "topic": "t", "neighbors": []string{"sender"}}
	planned := event("topology_assignment", "receiver", "", "t", 2)
	planned.Fields = map[string]any{"evidence": "planned", "neighbors": []string{}}
	cases := []struct {
		name   string
		events []model.TraceEvent
		topic  string
		at     float64
		want   string
	}{
		{"applied discards old graft", []model.TraceEvent{graft, applied}, "t", 3, "unknown"},
		{"applied preserves other topics", []model.TraceEvent{graft, otherGraft, applied}, "other", 3, "eager"},
		{"planned graph does not invalidate observed graft", []model.TraceEvent{graft, planned}, "t", 3, "eager"},
		{"planned graph does not manufacture graft", []model.TraceEvent{planned}, "t", 3, "unknown"},
		{"applied neighbors do not manufacture graft", []model.TraceEvent{applied}, "t", 3, "unknown"},
		{"legacy topic field reset", []model.TraceEvent{graft, legacyApplied}, "t", 3, "unknown"},
		{"legacy topic field preserves other topics", []model.TraceEvent{otherGraft, legacyApplied}, "other", 3, "eager"},
		{"future application preserves earlier observation", []model.TraceEvent{graft, applied}, "t", 1, "eager"},
		{"equal-time graft is discarded", []model.TraceEvent{event("graft", "sender", "receiver", "t", 2), applied}, "t", 3, "unknown"},
		{"old pull sequence is discarded", []model.TraceEvent{event("send_ihave", "sender", "receiver", "t", 1), event("recv_iwant", "sender", "receiver", "t", 1.5), applied}, "t", 3, "unknown"},
		{"pull split across application is discarded", []model.TraceEvent{event("send_ihave", "sender", "receiver", "t", 1), applied, event("recv_iwant", "sender", "receiver", "t", 2.5)}, "t", 3, "unknown"},
		{"equal-time advertisement is discarded", []model.TraceEvent{applied, event("send_ihave", "sender", "receiver", "t", 2), event("recv_iwant", "sender", "receiver", "t", 2.5)}, "t", 3, "unknown"},
		{"new pull sequence is retained", []model.TraceEvent{graft, applied, event("send_ihave", "sender", "receiver", "t", 2.25), event("recv_iwant", "sender", "receiver", "t", 2.5)}, "t", 3, "lazy"},
		{"other topic pull sequence is retained", []model.TraceEvent{event("send_ihave", "sender", "receiver", "other", 1), event("recv_iwant", "sender", "receiver", "other", 1.5), applied}, "other", 3, "lazy"},
		{"unrelated peer application preserves graft", []model.TraceEvent{graft, event("topology_applied", "unrelated", "", "t", 2)}, "t", 3, "eager"},
		{"later actual graft can establish evidence", []model.TraceEvent{applied, event("graft", "sender", "receiver", "t", 2.5)}, "t", 3, "eager"},
		{"missing topic does not invent a global reset", []model.TraceEvent{graft, event("topology_applied", "receiver", "", "", 2)}, "t", 3, "eager"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var wantEvidence []string
			for _, reverse := range []bool{false, true} {
				for _, decoded := range []bool{false, true} {
					accumulator := newResearchAccumulator()
					for i := range tc.events {
						j := i
						if reverse {
							j = len(tc.events) - 1 - i
						}
						e := tc.events[j]
						if decoded {
							data, err := json.Marshal(e)
							if err != nil || json.Unmarshal(data, &e) != nil {
								t.Fatal("event failed JSON archive round trip")
							}
						}
						accumulator.observe(e)
					}
					index := newOriginMetadataIndex(accumulator.inference, accumulator.peers)
					kind, evidence := index.estimate("sender", "receiver", tc.topic, "", epoch, epoch.Add(time.Duration(tc.at*float64(time.Second))))
					if kind != tc.want {
						t.Fatalf("kind=%s evidence=%v, want %s", kind, evidence, tc.want)
					}
					if wantEvidence == nil {
						wantEvidence = evidence
					} else if !reflect.DeepEqual(wantEvidence, evidence) {
						t.Fatal("archive decoding or event order changed reset inference")
					}
				}
			}
		})
	}
}

func TestOriginResetCannotUseEqualTimeControlEvidence(t *testing.T) {
	epoch := time.Unix(100, 0)
	for _, resetType := range []string{"leave", "measurement_stop", "measurement_terminated", "remove_peer", "topology_applied"} {
		t.Run(resetType, func(t *testing.T) {
			a := newResearchAccumulator()
			resetTopic := "t"
			if resetType == "remove_peer" {
				resetTopic = ""
			}
			a.observe(model.TraceEvent{Type: resetType, PeerID: "sender", RemotePeerID: "receiver", NodeID: "sender", Topic: resetTopic, Timestamp: epoch.Add(time.Second)})
			a.observe(model.TraceEvent{Type: "send_ihave", PeerID: "sender", RemotePeerID: "receiver", Topic: "t", Timestamp: epoch.Add(time.Second), Fields: map[string]any{"messageIdCount": 1}})
			a.observe(model.TraceEvent{Type: "recv_iwant", PeerID: "sender", RemotePeerID: "receiver", Topic: "t", Timestamp: epoch.Add(2 * time.Second), Fields: map[string]any{"messageIdCount": 1}})
			index := newOriginMetadataIndex(a.inference, a.peers)
			if kind, evidence := index.estimate("sender", "receiver", "t", "", epoch, epoch.Add(3*time.Second)); kind != "unknown" {
				t.Fatalf("same-time control crossed reset: %s %v", kind, evidence)
			}
		})
	}
}

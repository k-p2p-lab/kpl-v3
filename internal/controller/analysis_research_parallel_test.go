package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func researchParallelFixture(messages, nodes, churn int) (*runMetricAccumulator, []analysisObservation, time.Time) {
	metrics := newRunMetricAccumulator()
	metrics.research = newResearchAccumulator()
	a := metrics.research
	epoch := time.Unix(1000, 0).UTC()
	sequence := 0
	observe := func(event model.TraceEvent) {
		sequence++
		event.RunID = "research-parallel"
		event.EventID = fmt.Sprint(sequence)
		if event.PeerID == "" && event.NodeID != "" {
			event.PeerID = "peer-" + event.NodeID
		}
		if event.Fields == nil {
			event.Fields = map[string]any{}
		}
		event.Fields["clockBasis"] = "controller-offset-v1"
		metrics.observe(event)
	}
	for topic := range 3 {
		topicName := fmt.Sprintf("topic-%d", topic)
		observe(model.TraceEvent{Type: "join", NodeID: "publisher", Group: "publishers", Topic: topicName, Timestamp: epoch})
		for node := range nodes {
			name := fmt.Sprintf("receiver-%03d", node)
			group := fmt.Sprintf("group-%d", node%4)
			observe(model.TraceEvent{Type: "join", NodeID: name, Group: group, Topic: topicName, Timestamp: epoch})
			observe(model.TraceEvent{Type: "graft", NodeID: "publisher", RemotePeerID: "peer-" + name, Group: "publishers", Topic: topicName, Timestamp: epoch.Add(time.Millisecond)})
			for cycle := range churn {
				at := epoch.Add(time.Duration(cycle+1) * time.Second)
				observe(model.TraceEvent{Type: "leave", NodeID: name, Group: group, Topic: topicName, Timestamp: at})
				observe(model.TraceEvent{Type: "join", NodeID: name, Group: group, Topic: topicName, Timestamp: at.Add(200 * time.Millisecond)})
			}
		}
	}
	for index := range messages {
		topic := fmt.Sprintf("topic-%d", index%3)
		id := fmt.Sprintf("message-%05d", index)
		at := epoch.Add(500*time.Millisecond + time.Duration(index)*time.Millisecond)
		publication := model.TraceEvent{Type: "publish", NodeID: "publisher", Group: "publishers", Topic: topic, MessageID: id, Timestamp: at, Fields: map[string]any{"targetNodeIds": []string{"outside"}}}
		observe(publication)
		for ordinal := range min(6, nodes) {
			node := (index + ordinal) % nodes
			name := fmt.Sprintf("receiver-%03d", node)
			delivery := model.TraceEvent{Type: "deliver", NodeID: name, RemotePeerID: "peer-publisher", Group: fmt.Sprintf("group-%d", node%4), Topic: topic, MessageID: id, Timestamp: at.Add(time.Duration(ordinal+1) * 10 * time.Millisecond), LatencyMS: float64(ordinal+1) * 10, Fields: map[string]any{"payloadEncoding": "envelope", "latencyAvailable": true, "latencyClockSynchronized": true, "pubsubMessageId": id}}
			if ordinal%3 == 1 {
				from := "peer-publisher"
				to := "peer-" + name
				a.inference = append(a.inference, originMetadataEvent{at: at.Add(time.Millisecond), from: from, to: to, topic: topic, kind: "ihave", ids: []string{id}, detailed: true, complete: true}, originMetadataEvent{at: at.Add(2 * time.Millisecond), from: from, to: to, topic: topic, kind: "iwant", ids: []string{id}, detailed: true, complete: true})
				// The simultaneous push and pull evidence intentionally remains unknown.
			}
			observe(delivery)
			if ordinal%2 == 0 {
				observe(model.TraceEvent{Type: "duplicate", NodeID: name, Group: fmt.Sprintf("group-%d", node%4), Topic: topic, MessageID: id, Timestamp: at.Add(80 * time.Millisecond)})
			}
		}
	}
	observe(model.TraceEvent{Type: "deliver", NodeID: "orphan", Topic: "topic-0", MessageID: "unpublished", Timestamp: epoch.Add(time.Second)})
	if nodes > 0 {
		a.observeNodeGroup("receiver-000", "conflicting-group")
	}
	observations := make([]analysisObservation, 4)
	for i := range observations {
		observations[i] = analysisObservation{RunID: "research-parallel", At: epoch.Add(time.Duration(i) * time.Second), Groups: []analysisGroup{{Layers: []analysisLayer{{Protocol: "gossipsub", Nodes: nodes + 1}}}}}
		for group := range 4 {
			observations[i].Groups = append(observations[i].Groups, analysisGroup{Group: fmt.Sprintf("group-%d", group), Layers: []analysisLayer{{Protocol: "gossipsub", Nodes: (nodes + 3 - group) / 4}}})
		}
	}
	return metrics, observations, epoch
}

func TestResearchMembershipIndexMatchesHalfOpenLifetimeScan(t *testing.T) {
	epoch := time.Unix(100, 0).UTC()
	random := rand.New(rand.NewSource(31))
	lives := []researchMembershipLife{}
	for topic := range 3 {
		for node := range 12 {
			for span := range 30 {
				start := epoch.Add(time.Duration(span*10+random.Intn(4)) * time.Millisecond)
				end := start.Add(time.Duration(random.Intn(18)) * time.Millisecond)
				if span == 29 && node%2 == 0 {
					end = time.Time{}
				}
				lives = append(lives, researchMembershipLife{node: fmt.Sprint(node), topic: fmt.Sprint(topic), start: start, end: end})
			}
		}
	}
	// Include adjacent half-open intervals and a zero-width lifetime.
	lives = append(lives, researchMembershipLife{"boundary", "0", epoch, epoch.Add(time.Second)}, researchMembershipLife{"boundary", "0", epoch.Add(time.Second), epoch.Add(2 * time.Second)}, researchMembershipLife{"empty", "0", epoch.Add(3 * time.Second), epoch.Add(3 * time.Second)})
	random.Shuffle(len(lives), func(i, j int) { lives[i], lives[j] = lives[j], lives[i] })
	index := researchMembershipIndex(lives)
	queries := [][]time.Time{nil, {epoch}, {epoch.Add(time.Second)}, {epoch.Add(2 * time.Second)}, {epoch.Add(3 * time.Second)}, {time.Time{}, epoch.Add(150 * time.Millisecond)}}
	for range 100 {
		queries = append(queries, []time.Time{epoch.Add(time.Duration(random.Intn(400)) * time.Millisecond), epoch.Add(time.Duration(random.Intn(400)) * time.Millisecond)})
	}
	for _, receipts := range queries {
		sort.Slice(receipts, func(i, j int) bool { return receipts[i].Before(receipts[j]) })
		for topic, nodes := range index {
			for _, node := range nodes {
				want := false
				for _, life := range lives {
					if life.node != node.node || life.topic != topic {
						continue
					}
					for _, at := range receipts {
						if !at.Before(life.start) && (life.end.IsZero() || at.Before(life.end)) {
							want = true
						}
					}
				}
				if got := node.receivedDuring(receipts); got != want {
					t.Fatalf("topic=%s node=%s receipts=%v: indexed membership=%t want=%t", topic, node.node, receipts, got, want)
				}
			}
		}
	}
}

func TestResearchParallelMessagesAndReceiverGroupsPreserveSerialArtifact(t *testing.T) {
	var expected researchAnalysis
	var expectedProgress []string
	for _, workers := range []int{1, 2, 8, 16} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			metrics, observations, epoch := researchParallelFixture(257, 24, 8)
			var progress []string
			ctx := context.WithValue(t.Context(), analysisProgressKey{}, analysisProgress(func(phase string, _ int64) { progress = append(progress, phase) }))
			research, err := metrics.research.finishWithWorkers(ctx, metrics, observations, workers)
			if err != nil {
				t.Fatal(err)
			}
			_, samples, err := metrics.summarizeContext(ctx, "research-parallel", epoch.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			research.ReceiverGroups, err = metrics.research.receiverGroupsWithWorkers(ctx, metrics, research.Messages, observations, samples, epoch, workers)
			if err != nil {
				t.Fatal(err)
			}
			if workers == 1 {
				expected = research
				expectedProgress = progress
			} else if !reflect.DeepEqual(expected, research) || !reflect.DeepEqual(expectedProgress, progress) {
				left, _ := json.Marshal(expected)
				right, _ := json.Marshal(research)
				t.Fatalf("%d workers changed scientific output/progress (equal JSON=%t): %s / %s", workers, string(left) == string(right), left, right)
			}
			if research.MessageCount != 257 || research.OrphanReceipts != 1 || len(research.ReceiverGroups) != 5 || research.UnknownOrigins == 0 {
				t.Fatalf("fixture lost important populations: messages=%d orphans=%d groups=%d unknown=%d", research.MessageCount, research.OrphanReceipts, len(research.ReceiverGroups), research.UnknownOrigins)
			}
		})
	}
}

func TestResearchParallelCancellationKeepsProgressOrdered(t *testing.T) {
	for _, workers := range []int{1, 16} {
		metrics, observations, _ := researchParallelFixture(257, 24, 2)
		ctx, cancel := context.WithCancel(t.Context())
		count := 0
		ctx = context.WithValue(ctx, analysisProgressKey{}, analysisProgress(func(phase string, _ int64) {
			if strings.HasPrefix(phase, "propagation ") {
				count++
				if count == 2 {
					cancel()
				}
			}
		}))
		_, err := metrics.research.finishWithWorkers(ctx, metrics, observations, workers)
		cancel()
		if err != context.Canceled || count != 2 {
			t.Fatalf("%d workers cancellation=%v reports=%d", workers, err, count)
		}
	}
}

func TestResearchFinishDoesNotAccumulateDerivedControlsAcrossRetries(t *testing.T) {
	metrics, observations, _ := researchParallelFixture(257, 24, 4)
	before, err := json.Marshal(metrics.research.controls)
	if err != nil {
		t.Fatal(err)
	}
	first, err := metrics.research.finishWithWorkers(t.Context(), metrics, observations, 1)
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, _ := json.Marshal(first)
	ctx, cancel := context.WithCancel(t.Context())
	ctx = context.WithValue(ctx, analysisProgressKey{}, analysisProgress(func(phase string, _ int64) {
		if strings.HasPrefix(phase, "propagation ") {
			cancel()
		}
	}))
	if _, err := metrics.research.finishWithWorkers(ctx, metrics, observations, 16); err != context.Canceled {
		t.Fatalf("calculation did not stop after its graph-derived counts: %v", err)
	}
	cancel()
	for _, workers := range []int{16, 1} {
		again, err := metrics.research.finishWithWorkers(t.Context(), metrics, observations, workers)
		if err != nil {
			t.Fatal(err)
		}
		againBytes, _ := json.Marshal(again)
		if string(firstBytes) != string(againBytes) {
			t.Fatalf("%d-worker retry changed derived counts", workers)
		}
		if !reflect.DeepEqual(first, again) {
			t.Fatal("repeated finish changed the saved scientific artifact")
		}
	}
	after, _ := json.Marshal(metrics.research.controls)
	if string(before) != string(after) {
		t.Fatal("finish changed raw control evidence")
	}
}

func TestResearchIncidentIndexPreservesMissingAndSharedPeerOwnership(t *testing.T) {
	for _, check := range []struct {
		name  string
		peers map[string]string
		graft float64
		prune float64
	}{
		{"both-known", map[string]string{"A": "x", "B": "y"}, 2, 0},
		{"one-known", map[string]string{"A": "x"}, 2, 0},
		{"shared-node", map[string]string{"A": "x", "B": "x"}, 2, 0},
		{"neither-known", map[string]string{}, 1, 1},
	} {
		t.Run(check.name, func(t *testing.T) {
			metrics := newRunMetricAccumulator()
			a := newResearchAccumulator()
			a.peers = check.peers
			epoch := time.Unix(100, 0).UTC()
			for index, event := range []researchGraphEvent{
				{node: "x", peer: "A", remote: "B", topic: "t", kind: "graft"},
				{node: "y", peer: "B", remote: "A", topic: "t", kind: "graft"},
				{node: "x", topic: "t", kind: "leave"},
				{node: "x", peer: "A", remote: "B", topic: "t", kind: "graft"},
				{node: "y", peer: "B", remote: "A", topic: "t", kind: "graft"},
				{node: "x", topic: "other", kind: "measurement_stop"},
				{node: "x", peer: "A", remote: "B", topic: "t", kind: "graft"},
				{node: "y", peer: "B", remote: "A", topic: "t", kind: "prune"},
				{node: "y", peer: "B", remote: "A", topic: "t", kind: "graft"},
			} {
				event.at = epoch.Add(time.Duration(index) * time.Second)
				a.graphEvents = append(a.graphEvents, event)
			}
			result, err := a.finishWithWorkers(t.Context(), metrics, nil, 16)
			if err != nil {
				t.Fatal(err)
			}
			researchClose(t, result.Summary["logical_send_graft"].Average, check.graft)
			prune := 0.
			if value := result.Summary["logical_send_prune"].Average; value != nil {
				prune = *value
			}
			if prune != check.prune {
				t.Fatalf("logical PRUNE=%g, want %g", prune, check.prune)
			}
		})
	}
}

func TestResearchLeaveIsTopicScopedAndStopClosesAllTopics(t *testing.T) {
	metrics := newRunMetricAccumulator()
	a := newResearchAccumulator()
	a.peers = map[string]string{"A": "x", "B": "y"}
	epoch := time.Unix(100, 0).UTC()
	index := 0
	appendEvent := func(kind, node, from, to, topic string) {
		a.graphEvents = append(a.graphEvents, researchGraphEvent{at: epoch.Add(time.Duration(index) * time.Second), node: node, peer: from, remote: to, topic: topic, kind: kind})
		index++
	}
	for _, topic := range []string{"first", "second"} {
		appendEvent("join", "x", "A", "", topic)
		appendEvent("join", "y", "B", "", topic)
		appendEvent("graft", "x", "A", "B", topic)
		appendEvent("graft", "y", "B", "A", topic)
	}
	appendEvent("leave", "x", "A", "", "first")
	appendEvent("graft", "x", "A", "B", "first")
	appendEvent("graft", "x", "A", "B", "second")
	appendEvent("graft", "y", "B", "A", "second")
	result, err := a.finishWithWorkers(t.Context(), metrics, nil, 16)
	if err != nil {
		t.Fatal(err)
	}
	researchClose(t, result.Summary["logical_send_graft"].Average, 2)
	appendEvent("measurement_terminated", "x", "A", "", "first")
	appendEvent("graft", "x", "A", "B", "second")
	appendEvent("graft", "y", "B", "A", "second")
	result, err = a.finishWithWorkers(t.Context(), metrics, nil, 16)
	if err != nil {
		t.Fatal(err)
	}
	researchClose(t, result.Summary["logical_send_graft"].Average, 3)
}

var researchParallelBenchmarkSink researchAnalysis

func BenchmarkResearchMessageProjection(b *testing.B) {
	for _, workers := range []int{1, 4, 16} {
		b.Run(fmt.Sprint(workers), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				metrics, observations, _ := researchParallelFixture(4096, 48, 32)
				b.StartTimer()
				result, err := metrics.research.finishWithWorkers(b.Context(), metrics, observations, workers)
				if err != nil {
					b.Fatal(err)
				}
				researchParallelBenchmarkSink = result
			}
		})
	}
}

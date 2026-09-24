package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

var windowTestEpoch = time.Unix(1_700_000_000, 0).UTC()

func windowEvent(node, session string, sequence uint64, kind string, seconds int) model.TraceEvent {
	return model.TraceEvent{RunID: "window-run", AgentID: "agent-" + node, NodeID: node, SessionID: session, Sequence: sequence,
		EventID: fmt.Sprintf("%s/%s/%d", node, session, sequence), Type: kind, Timestamp: windowTestEpoch.Add(time.Duration(seconds) * time.Second)}
}

func windowStart(node, session string, seconds int, topics ...string) model.TraceEvent {
	event := windowEvent(node, session, 1, "measurement_start", seconds)
	event.Fields = map[string]any{"subscribedTopics": append([]string{}, topics...)}
	return event
}

func windowPublish() model.TraceEvent {
	event := windowEvent("publisher", "publisher-session", 2, "publish", 10)
	event.Topic, event.MessageID = "topic", "message"
	event.Fields = map[string]any{"measurementDefinition": sessionWindowDefinition, "deliveryWindow": "10s"}
	return event
}

func windowDeliveredEvent(node, session string, sequence uint64, seconds int) model.TraceEvent {
	event := windowEvent(node, session, sequence, "deliver", seconds)
	event.Topic, event.MessageID, event.LatencyMS = "topic", "message", float64((seconds-10)*1000)
	event.Fields = map[string]any{"payloadEncoding": "envelope", "latencyAvailable": true}
	return event
}

func windowEvents(extra ...model.TraceEvent) []model.TraceEvent {
	events := []model.TraceEvent{windowStart("publisher", "publisher-session", 0), windowPublish(), windowEvent("publisher", "publisher-session", 3, "measurement_checkpoint", 22)}
	return append(events, extra...)
}

func windowSummary(events []model.TraceEvent, seconds int) model.Metrics {
	accumulator := newRunMetricAccumulator()
	for _, event := range events {
		accumulator.observe(event)
	}
	result, _ := accumulator.summarize("window-run", windowTestEpoch.Add(time.Duration(seconds)*time.Second))
	return result
}

func TestSessionWindowDispatchTargetsOnlyAuditMissingStartEvidence(t *testing.T) {
	for _, test := range []struct {
		name       string
		evidence   []model.TraceEvent
		incomplete bool
	}{
		{name: "completely silent target", incomplete: true},
		{name: "different topic", evidence: []model.TraceEvent{windowStart("hint-only", "other", 0, "other-topic")}},
		{name: "already stopped", evidence: []model.TraceEvent{windowStart("hint-only", "stopped", 0, "topic"), windowEvent("hint-only", "stopped", 2, "measurement_stop", 5)}},
		{name: "late join", evidence: []model.TraceEvent{windowStart("hint-only", "later", 23, "topic")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			events := windowEvents(windowStart("receiver", "session", 0, "topic"),
				windowDeliveredEvent("receiver", "session", 2, 12), windowEvent("receiver", "session", 3, "measurement_checkpoint", 22))
			events[1].Fields["targetNodeIds"] = []string{"receiver", "hint-only"}
			result := windowSummary(append(events, test.evidence...), 30)
			if result.MeasurementIncomplete != test.incomplete || result.ExpectedDeliveries != 1 || result.EligibleDeliveries != 1 || result.Reachability != 1 ||
				result.InitialExpectedDeliveries != 1 || result.AvailabilityUnknownPairs != 0 || !result.StableCoverageAvailable || !result.InitialDeliveryRatioAvailable {
				t.Fatalf("dispatch audit changed session membership or missed silent evidence: %+v", result)
			}
		})
	}
}

func TestSessionWindowDepartureDoesNotConditionMembershipOnDelivery(t *testing.T) {
	for _, success := range []bool{false, true} {
		extra := []model.TraceEvent{windowStart("receiver", "session", 0, "topic")}
		sequence := uint64(2)
		if success {
			extra = append(extra, windowDeliveredEvent("receiver", "session", sequence, 12))
			sequence++
		}
		extra = append(extra, windowEvent("receiver", "session", sequence, "measurement_stop", 15))
		result := windowSummary(windowEvents(extra...), 30)
		if result.ExpectedDeliveries != 0 || result.EligibleDeliveries != 0 || result.DepartedPairs != 1 || result.InitialExpectedDeliveries != 1 || !result.StableCoverageAvailable || result.StableCoverage != 0 {
			t.Fatalf("departure membership depends on outcome (%v): %+v", success, result)
		}
		want := 0
		if success {
			want = 1
		}
		if result.InitialEligibleDeliveries != want || result.InitialUnknownDeliveries != 0 || !result.InitialDeliveryRatioAvailable {
			t.Fatalf("complete stop did not settle the initial cohort: %+v", result)
		}
	}
}

func TestSessionWindowDisconnectRemainsEligibleAndDeadlineBoundaryIsInclusive(t *testing.T) {
	for _, seconds := range []int{0, 20, 21} {
		extra := []model.TraceEvent{windowStart("receiver", "session", 0, "topic")}
		sequence := uint64(2)
		if seconds > 0 {
			extra = append(extra, windowDeliveredEvent("receiver", "session", sequence, seconds))
			sequence++
		}
		extra = append(extra, windowEvent("receiver", "session", sequence, "remove_peer", 21))
		sequence++
		extra = append(extra, windowEvent("receiver", "session", sequence, "measurement_checkpoint", 22))
		result := windowSummary(windowEvents(extra...), 30)
		if result.ExpectedDeliveries != 1 || result.AvailabilityUnknownPairs != 0 || result.UnknownDeliveries != 0 || result.MeasurementIncomplete {
			t.Fatalf("transport disconnect rewrote session membership: %+v", result)
		}
		if seconds == 20 {
			if result.EligibleDeliveries != 1 || result.MissedDeliveries != 0 {
				t.Fatalf("receipt at deadline was excluded: %+v", result)
			}
		} else if result.EligibleDeliveries != 0 || result.MissedDeliveries != 1 {
			t.Fatalf("missing/late receipt was credited: %+v", result)
		}
		if seconds == 21 && result.LateDeliveries != 1 {
			t.Fatalf("late receipt was not distinguished: %+v", result)
		}
	}
	result := windowSummary(windowEvents(windowStart("receiver", "session", 0, "topic"), windowEvent("receiver", "session", 2, "measurement_stop", 20)), 30)
	if result.ExpectedDeliveries != 1 || result.DepartedPairs != 0 || result.MissedDeliveries != 1 {
		t.Fatalf("stop at deadline violated the half-open session window: %+v", result)
	}
}

func TestSessionWindowLateJoinAndRestartCannotSatisfyOldSession(t *testing.T) {
	events := windowEvents(
		windowStart("receiver", "old", 0, "topic"), windowEvent("receiver", "old", 2, "measurement_stop", 15),
		windowStart("receiver", "new", 16, "topic"), windowDeliveredEvent("receiver", "new", 2, 17), windowEvent("receiver", "new", 3, "measurement_checkpoint", 22),
		windowStart("late-join", "late", 11, "topic"), windowDeliveredEvent("late-join", "late", 2, 12), windowEvent("late-join", "late", 3, "measurement_checkpoint", 22),
	)
	result := windowSummary(events, 30)
	if result.Delivered != 2 || result.InitialExpectedDeliveries != 1 || result.InitialEligibleDeliveries != 0 || result.ExpectedDeliveries != 0 || result.DepartedPairs != 1 {
		t.Fatalf("later session/late join satisfied a previous cohort: %+v", result)
	}
}

func TestSessionWindowPendingAndUnknownAvailabilityAreNotFailures(t *testing.T) {
	events := windowEvents(windowStart("receiver", "session", 0, "topic"), windowDeliveredEvent("receiver", "session", 2, 12))
	pending := windowSummary(events, 19)
	if pending.PendingPublications != 1 || pending.FinalizedPublications != 0 || pending.ExpectedDeliveries != 0 || pending.InitialExpectedDeliveries != 0 {
		t.Fatalf("unmatured publication was classified: %+v", pending)
	}
	unknown := windowSummary(events, 30)
	if unknown.AvailabilityUnknownPairs != 1 || unknown.PublicationAvailabilityUnknownPairs != 1 || unknown.ContinuityUnknownPairs != 0 || unknown.InitialExpectedDeliveries != 0 || unknown.DeliveryRatioAvailable || unknown.InitialDeliveryRatioAvailable || unknown.MissedDeliveries != 0 {
		t.Fatalf("delivery was incorrectly used as availability proof: %+v", unknown)
	}
	events = append(events, windowEvent("receiver", "session", 3, "measurement_checkpoint", 13))
	unknown = windowSummary(events, 30)
	if unknown.InitialExpectedDeliveries != 1 || unknown.InitialEligibleDeliveries != 1 || unknown.AvailabilityUnknownPairs != 1 || unknown.PublicationAvailabilityUnknownPairs != 0 || unknown.ContinuityUnknownPairs != 1 ||
		unknown.ExpectedDeliveries != 0 || !unknown.InitialDeliveryRatioAvailable || unknown.InitialDeliveryRatio != 1 || unknown.InitialDeliveryRatioUpperBound != 1 ||
		!unknown.StableCoverageAvailable || unknown.StableCoverage != 0 || unknown.StableCoverageUpperBound != 1 {
		t.Fatalf("pre-deadline checkpoint proved full-window survival: %+v", unknown)
	}
}

func TestSessionWindowCoveragePartitionsKnownStartingCohort(t *testing.T) {
	events := windowEvents(
		windowStart("stable", "stable-session", 0, "topic"), windowEvent("stable", "stable-session", 2, "measurement_checkpoint", 22),
		windowStart("departed", "departed-session", 0, "topic"), windowEvent("departed", "departed-session", 2, "measurement_stop", 15),
		windowStart("continuity", "continuity-session", 0, "topic"), windowEvent("continuity", "continuity-session", 2, "measurement_checkpoint", 12),
		windowStart("publication-unknown", "publication-session", 0, "topic"),
	)
	result := windowSummary(events, 30)
	if result.InitialExpectedDeliveries != 3 || result.ExpectedDeliveries != 1 || result.DepartedPairs != 1 || result.ContinuityUnknownPairs != 1 ||
		result.PublicationAvailabilityUnknownPairs != 1 || result.AvailabilityUnknownPairs != 2 {
		t.Fatalf("known and candidate availability classes overlap or disappeared: %+v", result)
	}
	if result.InitialExpectedDeliveries != result.ExpectedDeliveries+result.DepartedPairs+result.ContinuityUnknownPairs {
		t.Fatalf("K != S + D + C: %+v", result)
	}
	if !result.InitialDeliveryRatioAvailable || !result.StableCoverageAvailable || result.StableCoverage != 1.0/3 || result.StableCoverageUpperBound != 2.0/3 {
		t.Fatalf("known-cohort bounds were hidden or miscomputed: %+v", result)
	}
}

func TestSessionWindowSequenceGapsAndOutOfOrderRetries(t *testing.T) {
	events := windowEvents(windowStart("receiver", "session", 0, "topic"), windowEvent("receiver", "session", 3, "measurement_checkpoint", 22))
	unknown := windowSummary(events, 30)
	if unknown.ExpectedDeliveries != 1 || unknown.UnknownDeliveries != 1 || unknown.MissedDeliveries != 0 || unknown.Reachability != 0 || unknown.DeliveryRatioUpperBound != 1 || unknown.MeasurementIncomplete || !unknown.StableCoverageAvailable || unknown.InitialUnknownDeliveries != 1 {
		t.Fatalf("gap silently certified a miss or unknown membership: %+v", unknown)
	}
	accumulator := newRunMetricAccumulator()
	for i := len(events) - 1; i >= 0; i-- {
		accumulator.observe(events[i])
		accumulator.observe(events[i])
	}
	before, _ := accumulator.summarize("window-run", windowTestEpoch.Add(30*time.Second))
	if !reflect.DeepEqual(before, unknown) {
		t.Fatalf("out-of-order retry changed metrics: %+v versus %+v", before, unknown)
	}
	delivery := windowDeliveredEvent("receiver", "session", 2, 12)
	accumulator.observe(delivery)
	delivery.EventID = "a transport retry must still share the source sequence"
	if accumulator.observe(delivery) {
		t.Fatal("same session sequence was counted twice")
	}
	after, _ := accumulator.summarize("window-run", windowTestEpoch.Add(30*time.Second))
	if after.EligibleDeliveries != 1 || after.UnknownDeliveries != 0 || after.Delivered != 1 || after.DeliveryRatioUpperBound != 1 {
		t.Fatalf("late gap fill did not repair receipt evidence: %+v", after)
	}
	// Gaps elsewhere do not erase a positively observed on-time receipt.
	events = windowEvents(windowStart("receiver", "session", 0, "topic"), delivery, windowEvent("receiver", "session", 5, "measurement_checkpoint", 22))
	positive := windowSummary(events, 30)
	if positive.EligibleDeliveries != 1 || positive.UnknownDeliveries != 0 {
		t.Fatalf("a known receipt was made unknown by another gap: %+v", positive)
	}
}

func TestSessionWindowNegativeOrderingRawLatencyAndDuplicates(t *testing.T) {
	negative := windowDeliveredEvent("receiver", "session", 2, 9)
	result := windowSummary(windowEvents(windowStart("receiver", "session", 0, "topic"), negative, windowEvent("receiver", "session", 3, "measurement_checkpoint", 22)), 30)
	if result.EligibleDeliveries != 0 || result.UnknownDeliveries != 1 || result.MissedDeliveries != 0 || result.InvalidLatencySamples != 1 {
		t.Fatalf("negative clock ordering became a success or definite miss: %+v", result)
	}
	// A Controller-synchronized estimate whose uncertainty overlaps zero is a
	// causally valid on-time receipt. It contributes to delivery, while its
	// negative point estimate remains outside the latency distribution.
	negative.Fields["latencyClockSynchronized"] = true
	negative.Fields["latencyUncertaintyMs"] = 1001.0
	result = windowSummary(windowEvents(windowStart("receiver", "session", 0, "topic"), negative, windowEvent("receiver", "session", 3, "measurement_checkpoint", 22)), 30)
	if result.EligibleDeliveries != 1 || result.UnknownDeliveries != 0 || result.InvalidLatencySamples != 1 || result.LatencySamples != 0 {
		t.Fatalf("bounded clock uncertainty discarded a proven on-time receipt or fabricated latency: %+v", result)
	}
	raw := windowDeliveredEvent("receiver", "session", 2, 12)
	raw.Fields = map[string]any{"payloadEncoding": "raw", "latencyAvailable": false}
	raw.LatencyMS = -1
	result = windowSummary(windowEvents(windowStart("receiver", "session", 0, "topic"), raw, windowEvent("receiver", "session", 3, "measurement_checkpoint", 22)), 30)
	if result.EligibleDeliveries != 1 || result.LatencySamples != 0 || result.InvalidLatencySamples != 0 {
		t.Fatalf("raw on-time delivery invented latency or lost reachability: %+v", result)
	}
	duplicate := windowEvent("receiver", "session", 3, "duplicate", 14)
	duplicate.Topic, duplicate.MessageID = "topic", "message"
	lateDuplicate := duplicate
	lateDuplicate.Sequence, lateDuplicate.EventID, lateDuplicate.Timestamp = 4, "late-duplicate", windowTestEpoch.Add(21*time.Second)
	result = windowSummary(windowEvents(windowStart("receiver", "session", 0, "topic"), windowDeliveredEvent("receiver", "session", 2, 12), duplicate, lateDuplicate, windowEvent("receiver", "session", 5, "measurement_checkpoint", 22)), 30)
	if result.EligibleDuplicates != 1 || result.AverageDuplicates != 1 || result.Duplicates != 2 || result.LatencySamples != 1 || result.AverageLatencyMS != 2000 {
		t.Fatalf("duplicate/latency samples did not use stable successful window: %+v", result)
	}
}

func TestSessionWindowMissingLifecycleAndExactTopics(t *testing.T) {
	missing := windowSummary(windowEvents(windowEvent("receiver", "session", 2, "measurement_checkpoint", 22)), 30)
	if !missing.MeasurementIncomplete || missing.InitialDeliveryRatioAvailable || missing.StableCoverageAvailable {
		t.Fatalf("missing start was presented as complete membership: %+v", missing)
	}
	start := windowStart("receiver", "session", 0, "topic")
	start.Fields = nil
	missing = windowSummary(windowEvents(start, windowEvent("receiver", "session", 2, "measurement_checkpoint", 22)), 30)
	if !missing.MeasurementIncomplete {
		t.Fatal("unknown subscribed topics were treated as no subscriptions")
	}
	result := windowSummary(windowEvents(windowStart("receiver", "session", 0, " topic "), windowEvent("receiver", "session", 2, "measurement_checkpoint", 22)), 30)
	if result.ExpectedDeliveries != 0 || result.InitialExpectedDeliveries != 0 || result.MeasurementIncomplete {
		t.Fatalf("distinct exact topic was merged or marked unknown: %+v", result)
	}
	// Bootstrap tracer sequence gaps before measurement_start are irrelevant.
	start = windowStart("receiver", "session", 0, "topic")
	start.Sequence = 10
	result = windowSummary(windowEvents(start, windowEvent("receiver", "session", 11, "measurement_checkpoint", 22)), 30)
	if result.MissedDeliveries != 1 || result.UnknownDeliveries != 0 {
		t.Fatalf("pre-start tracer gaps poisoned the measurement interval: %+v", result)
	}
}

func TestSessionTerminationUpperBoundNeverProvesLivenessOrReceipts(t *testing.T) {
	for _, fixture := range []struct {
		termination, checkpoint                                           int
		initial, departed, publicationUnknown, continuity, initialUnknown int
	}{
		{9, 8, 0, 0, 0, 0, 0},
		{15, 12, 1, 1, 0, 0, 1},
		{15, 0, 0, 0, 1, 0, 0},
		{20, 12, 1, 0, 0, 1, 1},
	} {
		extra := []model.TraceEvent{windowStart("receiver", "session", 0, "topic")}
		if fixture.checkpoint != 0 {
			extra = append(extra, windowEvent("receiver", "session", 2, "measurement_checkpoint", fixture.checkpoint))
		}
		terminated := windowEvent("receiver", "", 0, "measurement_terminated", fixture.termination)
		extra = append(extra, terminated)
		result := windowSummary(windowEvents(extra...), 30)
		if result.InitialExpectedDeliveries != fixture.initial || result.DepartedPairs != fixture.departed ||
			result.PublicationAvailabilityUnknownPairs != fixture.publicationUnknown || result.ContinuityUnknownPairs != fixture.continuity ||
			result.AvailabilityUnknownPairs != fixture.publicationUnknown+fixture.continuity || result.InitialUnknownDeliveries != fixture.initialUnknown || result.ExpectedDeliveries != 0 {
			t.Fatalf("termination %+v was mistaken for a Peer checkpoint: %+v", fixture, result)
		}
	}
}

func TestSessionWindowInitialMissRequiresCompleteStopAndRejectsPostRemovalReceipt(t *testing.T) {
	start := windowStart("receiver", "session", 0, "topic")
	result := windowSummary(windowEvents(start, windowEvent("receiver", "session", 3, "measurement_stop", 15)), 30)
	if result.DepartedPairs != 1 || result.InitialUnknownDeliveries != 1 || result.InitialDeliveryRatioUpperBound != 1 || !result.InitialDeliveryRatioAvailable {
		t.Fatalf("missing stop prefix became a definite initial miss: %+v", result)
	}
	result = windowSummary(windowEvents(start,
		windowEvent("receiver", "session", 2, "measurement_checkpoint", 12),
		windowEvent("receiver", "", 0, "measurement_terminated", 15),
		windowDeliveredEvent("receiver", "session", 3, 16)), 30)
	if result.DepartedPairs != 1 || result.InitialEligibleDeliveries != 0 || result.InitialUnknownDeliveries != 1 {
		t.Fatalf("receipt after confirmed removal was credited: %+v", result)
	}
}

func TestSessionWindowReportsCanonicalDurationsAndRejectsInvalidArchiveWindows(t *testing.T) {
	second := windowPublish()
	second.Sequence, second.EventID, second.MessageID = 4, "second-publication", "second"
	second.Fields = map[string]any{"measurementDefinition": sessionWindowDefinition, "deliveryWindow": "5000ms"}
	result := windowSummary(windowEvents(second), 30)
	if !reflect.DeepEqual(result.DeliveryWindows, []string{"10s", "5s"}) || result.FinalizedPublications != 2 {
		t.Fatalf("mixed delivery windows were not reported deterministically: %+v", result)
	}
	for _, invalid := range []any{"0s", "-1s", "2h", "invalid", 10} {
		publication := windowPublish()
		publication.Fields["deliveryWindow"] = invalid
		events := []model.TraceEvent{windowStart("publisher", "publisher-session", 0), publication, windowEvent("publisher", "publisher-session", 3, "measurement_checkpoint", 22)}
		result := windowSummary(events, 30)
		if !result.MeasurementIncomplete || result.FinalizedPublications != 0 || result.ExpectedDeliveries != 0 {
			t.Fatalf("invalid window %v produced fabricated metrics: %+v", invalid, result)
		}
	}
}

func TestSessionWindowPrometheusKeepsNewBoundsSeparateFromLegacyMetrics(t *testing.T) {
	s := newState(t.TempDir())
	events := windowEvents(
		windowStart("receiver", "session", 0, "topic"), windowEvent("receiver", "session", 3, "measurement_checkpoint", 22),
		windowStart("continuity", "continuity-session", 0, "topic"), windowEvent("continuity", "continuity-session", 2, "measurement_checkpoint", 13),
		windowStart("publication-unknown", "publication-session", 0, "topic"),
	)
	if err := s.appendEvents(model.EventBatch{Events: events}); err != nil {
		t.Fatal(err)
	}
	metrics := gatherTestMetrics(t, s)
	labels := map[string]string{"run_id": "window-run"}
	requireMetricValue(t, metrics, "kpl_window_stable_pairs", labels, 1)
	requireMetricValue(t, metrics, "kpl_window_unknown_pairs", labels, 1)
	requireMetricValue(t, metrics, "kpl_window_publication_availability_unknown_pairs", labels, 1)
	requireMetricValue(t, metrics, "kpl_window_continuity_unknown_pairs", labels, 1)
	requireMetricValue(t, metrics, "kpl_window_availability_unknown_pairs", labels, 2)
	requireMetricValue(t, metrics, "kpl_window_stable_coverage", labels, 0.5)
	requireMetricValue(t, metrics, "kpl_window_stable_coverage_upper_bound", labels, 1)
	requireMetricValue(t, metrics, "kpl_window_measurement_incomplete", labels, 0)
	requireMetricValue(t, metrics, "kpl_window_delivery_ratio", map[string]string{"run_id": "window-run", "bound": "lower"}, 0)
	requireMetricValue(t, metrics, "kpl_window_delivery_ratio", map[string]string{"run_id": "window-run", "bound": "upper"}, 1)
	if _, exists := metrics[metricTestKey("kpl_delivery_ratio", labels)]; exists {
		t.Fatal("session-window run emitted the legacy delivery ratio")
	}
	if err := s.appendEvents(model.EventBatch{Events: []model.TraceEvent{windowDeliveredEvent("receiver", "session", 2, 12)}}); err != nil {
		t.Fatal(err)
	}
	metrics = gatherTestMetrics(t, s)
	latency := metrics[metricTestKey("kpl_window_propagation_latency_seconds", map[string]string{"run_id": "window-run", "agent_id": "agent-receiver", "topic": "topic"})]
	if latency.count != 1 || latency.sum != 2 {
		t.Fatalf("new latency histogram omitted stable receipt: %+v", latency)
	}
	if err := s.appendEvents(model.EventBatch{Events: []model.TraceEvent{cohortPublish("legacy", "topic", []string{"receiver"})}}); err != nil {
		t.Fatal(err)
	}
	metrics = gatherTestMetrics(t, s)
	requireMetricValue(t, metrics, "kpl_window_legacy_publications", map[string]string{"run_id": "run"}, 1)
	if _, exists := metrics[metricTestKey("kpl_window_stable_pairs", map[string]string{"run_id": "run"})]; exists {
		t.Fatal("legacy run fabricated stable session pairs")
	}
}

func TestSessionWindowArchivePinsMaturityAndRebuildsSameMetrics(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, server, "window-run", "completed", windowTestEpoch)
	events := windowEvents(windowStart("receiver", "session", 0, "topic"), windowDeliveredEvent("receiver", "session", 2, 12), windowEvent("receiver", "session", 3, "measurement_checkpoint", 22))
	if err := server.state.appendEvents(model.EventBatch{Events: events}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := server.captureResult("window-run")
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	snapshot.exportedAt = windowTestEpoch.Add(19 * time.Second)
	var output bytes.Buffer
	if err := snapshot.writeZIP(context.Background(), &output); err != nil {
		t.Fatal(err)
	}
	files := decodeResultZIP(t, output.Bytes())
	var exported model.Metrics
	if err := json.Unmarshal(files["metrics.json"], &exported); err != nil {
		t.Fatal(err)
	}
	if exported.PendingPublications != 1 || exported.FinalizedPublications != 0 {
		t.Fatalf("export used current wall clock instead of pinned exportedAt: %+v", exported)
	}
	rebuilt, err := summarizeRunEvents("window-run", bytes.NewReader(files["events.jsonl"]), snapshot.exportedAt)
	if err != nil || !reflect.DeepEqual(rebuilt, exported) {
		t.Fatalf("archive reproduction diverged: %+v / %+v / %v", rebuilt, exported, err)
	}
	legacy := cohortPublish("legacy", "topic", []string{"receiver"})
	legacy.RunID = "window-run"
	mixed := windowSummary(append(events, legacy), 30)
	if mixed.LegacyPublications != 1 || mixed.ExpectedDeliveries != 1 || mixed.EligibleDeliveries != 1 || mixed.Definition != sessionWindowDefinition {
		t.Fatalf("legacy publication contaminated new cohort: %+v", mixed)
	}
}

func TestSequenceRangesMergeOutOfOrderWithoutEnumeratingMissingNumbers(t *testing.T) {
	var ranges sequenceRanges
	for _, number := range []uint64{10, 15, 12, 11, 14, 13, 1_000_000_000, 11} {
		ranges.add(number)
	}
	if !reflect.DeepEqual(ranges, sequenceRanges{{10, 15}, {1_000_000_000, 1_000_000_000}}) || ranges.through(10) != 15 || ranges.through(9) != 0 {
		t.Fatalf("incorrect compact sequence intervals: %+v", ranges)
	}
}

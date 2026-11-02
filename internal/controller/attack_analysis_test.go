package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// A selected attack is evidence of a local decision, not an application receipt,
// an independently observed wire transfer, or proof of receiver availability.
func TestAttackTelemetryPreservesSessionWindowEvidence(t *testing.T) {
	for _, kind := range []string{"attack_stats", "attack_decision", "pubsub_reject"} {
		t.Run(kind, func(t *testing.T) {
			events := windowEvents(
				windowStart("honest", "h", 0, "topic"),
				windowDeliveredEvent("honest", "h", 2, 12),
				windowEvent("honest", "h", 3, "measurement_stop", 22),
				windowStart("attacker", "a", 0, "topic"),
				windowEvent("attacker", "a", 2, "placeholder", 12),
				windowEvent("attacker", "a", 3, "measurement_stop", 22),
			)
			baseline := windowSummary(events, 30)
			event := &events[len(events)-2]
			event.Type, event.Topic = kind, "topic"
			event.Fields = map[string]any{
				"attack": "sink", "decision": "drop", "pubsubMessageId": "wire-message",
				"revision": 3, "final": false, "sampled": true,
				"counters": map[string]any{"sink": map[string]any{"considered": 1, "dropped": 1}},
			}
			got := windowSummary(events, 30)
			if !reflect.DeepEqual(got, baseline) || got.ExpectedDeliveries != 2 || got.EligibleDeliveries != 1 || got.MissedDeliveries != 1 || got.Reachability != .5 {
				t.Fatalf("attack observation changed delivery evidence: got=%+v baseline=%+v", got, baseline)
			}
			// Missing telemetry remains unknown even when the policy reports a
			// drop. A late copy fills continuity; retries cannot add deliveries.
			missing := append([]model.TraceEvent(nil), events[:len(events)-2]...)
			missing = append(missing, events[len(events)-1])
			unknown := windowSummary(missing, 30)
			if unknown.UnknownDeliveries != 1 || unknown.MissedDeliveries != 0 || unknown.DeliveryRatioUpperBound != 1 {
				t.Fatalf("missing attack event certified a miss: %+v", unknown)
			}
			missing = append(missing, *event, *event)
			if repaired := windowSummary(missing, 30); !reflect.DeepEqual(repaired, baseline) {
				t.Fatalf("late attack evidence/retry changed metrics: %+v", repaired)
			}
		})
	}
}

func TestAttackFinalEvidencePersistsAcrossRestartAndSnapshotBoundary(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, server, "window-run", "completed", windowTestEpoch)
	initial := windowEvents(windowStart("attacker", "a", 0, "topic"), windowEvent("attacker", "a", 2, "measurement_stop", 22))
	if err := server.state.appendEvents(model.EventBatch{Events: initial}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := server.captureResult("window-run")
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	final := windowEvent("attacker", "a", 3, "attack_stats", 23)
	final.Fields = map[string]any{
		"final": true, "cumulative": true, "revision": 7,
		"options":  map[string]any{"forwarding": map[string]any{"enabled": false, "probability": 0}},
		"counters": map[string]any{"forwarding": map[string]any{"publishWithheld": 17, "unknown": 2}},
	}
	for range 2 {
		if err := server.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{final}}); err != nil {
			t.Fatal(err)
		}
	}
	var oldZIP bytes.Buffer
	if err := snapshot.writeZIP(context.Background(), &oldZIP); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(decodeResultZIP(t, oldZIP.Bytes())["events.jsonl"], []byte("attack_stats")) {
		t.Fatal("late final attack evidence crossed an existing export boundary")
	}
	restarted := New(server.config, nil)
	latest, err := restarted.captureResult("window-run")
	if err != nil {
		t.Fatal(err)
	}
	defer latest.close()
	var latestZIP bytes.Buffer
	if err := latest.writeZIP(context.Background(), &latestZIP); err != nil {
		t.Fatal(err)
	}
	files := decodeResultZIP(t, latestZIP.Bytes())
	decoder := json.NewDecoder(bytes.NewReader(files["events.jsonl"]))
	count := 0
	want, err := json.Marshal(final)
	if err != nil {
		t.Fatal(err)
	}
	for {
		var event model.TraceEvent
		if err := decoder.Decode(&event); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if event.Type != "attack_stats" {
			continue
		}
		count++
		got, err := json.Marshal(event)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("final observation changed across storage: %s, %v", got, err)
		}
	}
	if count != 1 {
		t.Fatalf("final attack observation count=%d, want exactly one after retry", count)
	}
	latest.exportedAt = windowTestEpoch.Add(30 * time.Second)
	analysis, err := analyzeResult(context.Background(), latest)
	if err != nil {
		t.Fatal(err)
	}
	if baseline := windowSummary(initial, 30); !reflect.DeepEqual(analysis.Metrics, baseline) {
		t.Fatalf("final attack counters changed reloaded metrics: got=%+v baseline=%+v", analysis.Metrics, baseline)
	}
}

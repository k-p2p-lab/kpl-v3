package peer

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

func TestAdaptiveHopwaveMeasurementBoundaries(t *testing.T) {
	tel := newTelemetry(model.Node{}, "", "", nil)
	tel.enableAdaptiveHopwave()
	d := pubsub.HopwaveDecision{
		Topic: "t", MessageID: "\x00\xff", RangeMatched: true, PayloadBytes: 0,
		ConfiguredFactor: .9, BaseFactor: .5, EffectiveFactor: .5, EligiblePeers: 1, SelectedPeers: 1,
	}
	tel.observeAdaptiveHopwave(d) // Before measurement.
	tel.startMeasurement([]string{"t"})
	start := <-tel.events
	if _, ok := start.Fields["hopwaveAdaptive"]; ok {
		t.Fatal("unexpected adaptive snapshot on measurement_start")
	}
	tel.observeAdaptiveHopwave(d) // Payload only, including a measured zero-byte payload.
	d.RetentionValid, d.Retention, d.EffectiveFactor = true, 0, .9
	tel.observeAdaptiveHopwave(d) // Both changes, net F unchanged.
	d.PayloadBytes, d.RangeMatched = -1, false
	d.ConfiguredFactor, d.BaseFactor, d.Retention = 1, 1, 1
	tel.observeAdaptiveHopwave(d) // Retention cap lowers the factor, R = 1.
	d.ConfiguredFactor, d.BaseFactor = .9, .9
	tel.observeAdaptiveHopwave(d) // Valid R with no change.
	tel.checkpoint()
	event := <-tel.events
	stats := event.Fields["hopwaveAdaptive"].(hopwaveAdaptiveSnapshot)
	if stats.TroughDecisions != 4 || stats.Adjustments != 3 || stats.PayloadAdjustments != 2 || stats.RetentionAdjustments != 2 || stats.OmittedRecords != 0 || len(stats.Records) != 3 {
		t.Fatalf("unexpected checkpoint: %+v", stats)
	}
	first, second, third := stats.Records[0], stats.Records[1], stats.Records[2]
	if first.Adjustment != 1 || first.At.IsZero() || first.PubsubMessageID != "00ff" || first.PayloadBytes == nil || *first.PayloadBytes != 0 || first.Retention != nil {
		t.Fatalf("unexpected payload record: %+v", first)
	}
	if second.Adjustment != 2 || second.Retention == nil || *second.Retention != 0 || third.Adjustment != 3 || third.PayloadBytes != nil {
		t.Fatalf("unexpected retention records: %+v, %+v", second, third)
	}
	tel.stopMeasurement()
	tel.observeAdaptiveHopwave(d)
	tel.checkpoint()
	stop := <-tel.events
	final := stop.Fields["hopwaveAdaptive"].(hopwaveAdaptiveSnapshot)
	if stop.Sequence != start.Sequence+2 || stop.Type != "measurement_stop" || final.TroughDecisions != 4 || final.Adjustments != 3 || len(final.Records) != 0 || len(tel.events) != 0 {
		t.Fatalf("unexpected stop snapshot: %+v", stop)
	}
	if len(stats.Records) != 3 || tel.hopwaveAdaptive.TroughDecisions != 4 {
		t.Fatal("closing the session mutated a prior checkpoint or recorded a late decision")
	}
}

func TestAdaptiveHopwaveCheckpointAdmissionAndOmissions(t *testing.T) {
	tel := &telemetry{events: make(chan model.TraceEvent, 4)}
	tel.enableAdaptiveHopwave()
	tel.startMeasurement(nil) // Occupies the only ordinary queue slot.
	d := pubsub.HopwaveDecision{
		Topic: strings.Repeat("\x00", hopwaveIdentifierLimit), MessageID: strings.Repeat("x", hopwaveIdentifierLimit),
		ConfiguredFactor: 1, BaseFactor: .5, EffectiveFactor: .5,
	}
	oversized := d
	oversized.Topic += "x"
	tel.observeAdaptiveHopwave(oversized)
	oversized = d
	oversized.MessageID += "x"
	tel.observeAdaptiveHopwave(oversized)
	for range hopwaveRecordLimit + 1 {
		tel.observeAdaptiveHopwave(d)
	}
	tel.checkpoint() // Rejected. The final priority snapshot must carry the records.
	<-tel.events
	tel.stopMeasurement()
	if drop := <-tel.events; drop.Type != "telemetry_drop" || drop.Fields["count"] != uint64(1) {
		t.Fatalf("unexpected drop notice: %+v", drop)
	}
	stop := <-tel.events
	s := stop.Fields["hopwaveAdaptive"].(hopwaveAdaptiveSnapshot)
	if s.Adjustments != hopwaveRecordLimit+3 || s.OmittedRecords != 3 || len(s.Records) != hopwaveRecordLimit || s.Records[0].Adjustment != 3 || s.Records[hopwaveRecordLimit-1].Adjustment != hopwaveRecordLimit+2 {
		t.Fatalf("lost or duplicated pending records: %+v", s)
	}
	raw, err := json.Marshal(stop)
	if err != nil || len(raw) >= model.MaxEventBatchBytes {
		t.Fatalf("bounded snapshot encoded to %d bytes: %v", len(raw), err)
	}
}

func TestAdaptiveHopwaveZeroAndUnavailable(t *testing.T) {
	for _, adaptive := range []bool{true, false} {
		tel := newTelemetry(model.Node{}, "", "", nil)
		if adaptive {
			tel.enableAdaptiveHopwave()
		}
		tel.startMeasurement(nil)
		<-tel.events
		tel.checkpoint()
		tel.stopMeasurement()
		for range 2 {
			event := <-tel.events
			value, present := event.Fields["hopwaveAdaptive"]
			if present != adaptive {
				t.Fatalf("adaptive=%v: unexpected event fields: %+v", adaptive, event.Fields)
			}
			if present {
				raw, err := json.Marshal(value)
				const want = `{"troughDecisions":0,"adjustments":0,"payloadAdjustments":0,"retentionAdjustments":0,"omittedRecords":0}`
				if err != nil || string(raw) != want {
					t.Fatalf("unexpected idle snapshot: %s, %v", raw, err)
				}
			}
		}
	}
}

func TestAdaptiveHopwaveConcurrentCheckpointsOwnTheirRecords(t *testing.T) {
	tel := newTelemetry(model.Node{}, "", "", nil)
	tel.enableAdaptiveHopwave()
	tel.startMeasurement(nil)
	<-tel.events
	d := pubsub.HopwaveDecision{ConfiguredFactor: .5, BaseFactor: .7, EffectiveFactor: .7}
	const decisions = 256
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			for range decisions / 2 {
				tel.observeAdaptiveHopwave(d)
				tel.checkpoint()
			}
		})
	}
	wg.Wait()
	tel.stopMeasurement()
	var seen uint64
	for len(tel.events) != 0 {
		snapshot := (<-tel.events).Fields["hopwaveAdaptive"].(hopwaveAdaptiveSnapshot)
		for _, record := range snapshot.Records {
			seen++
			if record.Adjustment != seen {
				t.Fatalf("queued snapshots contain record %d, want %d", record.Adjustment, seen)
			}
		}
		if snapshot.Adjustments != seen || snapshot.TroughDecisions != seen || snapshot.OmittedRecords != 0 {
			t.Fatalf("counters disagree with %d records: %+v", seen, snapshot)
		}
	}
	if seen != decisions {
		t.Fatalf("got %d adjustments, want %d", seen, decisions)
	}
}

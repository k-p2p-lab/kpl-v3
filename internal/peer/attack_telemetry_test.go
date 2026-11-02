package peer

import (
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

func TestAttackEventsPreserveClockQuality(t *testing.T) {
	for _, mode := range []string{"synchronized", "expired", "unsynchronized"} {
		t.Run(mode, func(t *testing.T) {
			tel := &telemetry{node: model.Node{ID: "node", RunID: "run"}, events: make(chan model.TraceEvent, 16)}
			offset := time.Duration(0)
			if mode != "unsynchronized" {
				sampledAt := time.Now()
				if mode == "expired" {
					sampledAt = sampledAt.Add(-controllerClockMaxAge - time.Second)
				}
				offset = 2 * time.Second
				tel.acceptClockEstimate(controllerClockEstimate{offset: offset, uncertainty: 4 * time.Millisecond}, sampledAt)
			}
			runtime := newAttackRuntime(&Server{telemetry: tel, config: model.PeerProcessConfig{NodeConfig: model.NodeConfig{Sink: &model.ProbabilityAttack{Enabled: attackBool(true)}}}})
			topic := "blocks"
			before := time.Now().UTC().Add(offset)
			if !runtime.decide(&pubsub.Message{ID: "wire-id", Message: &pb.Message{Topic: &topic}}).Drop {
				t.Fatal("fixture did not exercise an actual attack decision")
			}
			runtime.flush(false)
			runtime.flush(true)
			after := time.Now().UTC().Add(offset)
			if len(tel.events) != 3 {
				t.Fatalf("missing decision or periodic/final statistics: queued=%d", len(tel.events))
			}
			for len(tel.events) > 0 {
				event := <-tel.events
				if event.Timestamp.Before(before) || event.Timestamp.After(after) {
					t.Errorf("%s timestamp %s outside source observation window", event.Type, event.Timestamp)
				}
				if mode == "synchronized" {
					if event.Fields["clockBasis"] != controllerClockBasis || event.Fields["clockOffsetMs"] != float64(2000) || event.Fields["clockUncertaintyMs"] != float64(4) {
						t.Errorf("%s omitted the synchronized observation quality: %+v", event.Type, event.Fields)
					}
				} else {
					for _, key := range []string{"clockBasis", "clockOffsetMs", "clockUncertaintyMs"} {
						if _, exists := event.Fields[key]; exists {
							t.Errorf("%s claimed %s clock quality in %s mode", event.Type, key, mode)
						}
					}
				}
			}
		})
	}
}

func TestFinalAttackStatsPreserveStopDropAndBandwidthWhenSaturated(t *testing.T) {
	for _, mode := range []string{"enabled", "inactive", "runtime-activation"} {
		t.Run(mode, func(t *testing.T) { testFinalAttackTelemetrySaturation(t, mode) })
	}
}

func TestProfileUpdatesPreserveFinalAttackTelemetryWhenSaturated(t *testing.T) {
	s := newRunTestServer(t, model.PeerProcessConfig{Node: model.Node{ID: "node", RunID: "run", Role: "worker"}})
	s.bandwidth = newBandwidthReporter()
	s.telemetry.events = make(chan model.TraceEvent, 12)
	s.attacks = newAttackRuntime(s)
	s.telemetry.startMeasurement([]string{"blocks"})
	for i := 0; i < 20; i++ {
		s.telemetry.emit(model.TraceEvent{Type: "deliver"})
	}
	// Apply through the real prepare/apply path while the telemetry transport is
	// backpressured. Configuration events must not steal shutdown reservations.
	for revision := uint64(1); revision <= 4; revision++ {
		request := model.ProfileUpdateRequest{RunID: "run", Revision: revision, Stage: "prepare", Set: model.RuntimeProfilePatch{Sink: &model.ProbabilityAttack{Enabled: attackBool(revision%2 == 0)}}}
		prepared, err := s.executeProfileUpdate(t.Context(), request, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := model.ValidateProfileAcknowledgement(request, prepared); err != nil {
			t.Fatal(err)
		}
		request.Stage = "apply"
		applied, err := s.executeProfileUpdate(t.Context(), request, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := model.ValidateProfileAcknowledgement(request, applied); err != nil {
			t.Fatal(err)
		}
	}
	s.telemetry.stopMeasurement()
	s.attacks.flush(true)
	s.bandwidth.LogSentMessage(321)
	s.emitBandwidth(true)
	found := map[string]model.TraceEvent{}
	for len(s.telemetry.events) > 0 {
		event := <-s.telemetry.events
		found[event.Type] = event
	}
	for _, kind := range []string{"telemetry_drop", "measurement_stop", "attack_stats", "bandwidth"} {
		if found[kind].Sequence == 0 {
			t.Errorf("final %s lost after successful profile updates", kind)
		}
	}
	if found["attack_stats"].Fields["revision"] != uint64(4) {
		t.Errorf("final attack snapshot lost the applied revision: %+v", found["attack_stats"])
	}
	if found["telemetry_drop"].Fields["count"] != uint64(17) || found["telemetry_drop"].Sequence != 26 {
		t.Errorf("dropped normal/configuration observations lost quality evidence: %+v", found["telemetry_drop"])
	}
	if found["measurement_stop"].Sequence >= found["attack_stats"].Sequence || found["attack_stats"].Sequence >= found["bandwidth"].Sequence {
		t.Error("shutdown evidence source order changed")
	}
	if found["bandwidth"].Bandwidth == nil || found["bandwidth"].Bandwidth.SentBytes != 321 || !found["bandwidth"].Bandwidth.Final {
		t.Error("final bandwidth value or flag lost")
	}
}

func testFinalAttackTelemetrySaturation(t *testing.T, mode string) {
	t.Helper()
	s := newRunTestServer(t, model.PeerProcessConfig{Node: model.Node{ID: "node", RunID: "run"}})
	s.bandwidth = newBandwidthReporter()
	s.telemetry.events = make(chan model.TraceEvent, 8)
	if mode != "runtime-activation" {
		s.config.NodeConfig.Sink = &model.ProbabilityAttack{Enabled: attackBool(mode == "enabled"), Probability: attackFloat(1)}
	}
	s.attacks = newAttackRuntime(s)
	s.telemetry.startMeasurement([]string{"blocks"})
	for i := 0; i < 20; i++ {
		s.telemetry.emit(model.TraceEvent{Type: "deliver"})
	}
	if mode == "runtime-activation" {
		config := s.config.NodeConfig.Merge(model.NodeConfig{Sink: &model.ProbabilityAttack{Enabled: attackBool(true)}}).WithDefaults()
		s.attacks.update(config, 1)
	}
	s.telemetry.stopMeasurement()
	s.attacks.flush(true)
	s.bandwidth.LogSentMessage(321)
	s.emitBandwidth(true)
	found := map[string]model.TraceEvent{}
	for len(s.telemetry.events) > 0 {
		event := <-s.telemetry.events
		found[event.Type] = event
	}
	for _, kind := range []string{"telemetry_drop", "measurement_stop", "attack_stats", "bandwidth"} {
		if found[kind].Sequence == 0 {
			t.Fatalf("final %s lost with configured attack: %v", kind, found)
		}
	}
	if found["attack_stats"].Fields["final"] != true || found["bandwidth"].Bandwidth == nil || !found["bandwidth"].Bandwidth.Final || found["bandwidth"].Bandwidth.SentBytes != 321 {
		t.Fatal("final observation content lost")
	}
	if found["bandwidth"].Sequence <= found["attack_stats"].Sequence || found["attack_stats"].Sequence <= found["measurement_stop"].Sequence {
		t.Fatal("final source order changed")
	}
}

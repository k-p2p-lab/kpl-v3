package peer

import (
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestFinalAttackStatsPreserveStopDropAndBandwidthWhenSaturated(t *testing.T) {
	for _, mode := range []string{"enabled", "inactive", "runtime-activation"} {
		t.Run(mode, func(t *testing.T) { testFinalAttackTelemetrySaturation(t, mode) })
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

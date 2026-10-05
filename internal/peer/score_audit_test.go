package peer

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestScoreAuditSamplesRemainOrderedAfterClockCorrection(t *testing.T) {
	for _, empty := range []bool{false, true} {
		name := "updated-score"
		if empty {
			name = "empty-snapshot"
		}
		t.Run(name, func(t *testing.T) {
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			enabled := true
			s := &Server{
				config: model.PeerProcessConfig{NodeConfig: model.NodeConfig{GossipSub: model.GossipSubConfig{
					Score: &model.PeerScoreConfig{Enabled: &enabled, AppSpecificWeight: 1}, ScoreInspectInterval: "1s",
				}}},
				logger: logger, telemetry: newTelemetry(model.Node{ID: "observer", RunID: "run"}, "", "", logger),
			}
			now := time.Now()
			sampledAt := now.Add(-2 * time.Second)
			first := map[corepeer.ID]*pubsub.PeerScoreSnapshot{"target": {Score: 1, AppSpecificScore: 1}}
			s.telemetry.acceptClockEstimate(controllerClockEstimate{offset: 30 * time.Second, uncertainty: time.Millisecond}, now)
			s.recordDetailedPeerScores(sampledAt, first)
			previous := s.scoreSample.Clone()
			s.telemetry.acceptClockEstimate(controllerClockEstimate{offset: 0, uncertainty: time.Millisecond}, now)
			latest := map[corepeer.ID]*pubsub.PeerScoreSnapshot{"target": {Score: 2, AppSpecificScore: 2}}
			if empty {
				latest = nil
			}
			s.recordDetailedPeerScores(sampledAt.Add(time.Second), latest)
			if previous.Sequence == 0 || s.scoreSample.Sequence != previous.Sequence+1 {
				t.Fatalf("newer local observation lost sequence ordering after clock correction: previous=%d current=%d", previous.Sequence, s.scoreSample.Sequence)
			}
			if want := sampledAt.Add(time.Second).UTC(); s.scoreSample.ObservedAt != want {
				t.Fatalf("sample time was shifted away from the corrected clock: got=%s want=%s", s.scoreSample.ObservedAt, want)
			}
			if !s.scoreSample.ObservedAt.Before(previous.ObservedAt) {
				t.Fatal("test did not exercise a backwards clock correction")
			}
			if s.scoreSample.Components["total"].Count != len(latest) {
				t.Fatalf("latest score sample lost: %+v", s.scoreSample)
			}
			accepted := s.scoreSample.Clone()
			s.recordDetailedPeerScores(sampledAt, first)
			if s.scoreSample.Sequence != accepted.Sequence || s.scoreSample.ObservedAt != accepted.ObservedAt || len(s.telemetry.events) != 2 {
				t.Fatal("late callback replaced newer sample or emitted another event")
			}
		})
	}
}

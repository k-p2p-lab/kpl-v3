package agent

import (
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func auditScoreSample(at time.Time, sequence uint64, count int) *model.PeerScoreSample {
	sample := &model.PeerScoreSample{ObservedAt: at, Sequence: sequence, IntervalSeconds: 1, Components: make(map[string]model.ScoreStatistic)}
	for _, key := range model.ScoreComponentKeys {
		sample.Components[key] = model.ScoreStatistic{Count: count}
	}
	return sample
}

func TestScoreStatusAcceptsClockCorrectionsAndRejectsReorderedSamples(t *testing.T) {
	for _, empty := range []bool{false, true} {
		server := topologyStatusServer()
		at := time.Now().UTC()
		report := topologyStatusAt(at)
		report.ScoreSample = auditScoreSample(at.Add(30*time.Second), 1, 1)
		if err := server.updateNode(report); err != nil {
			t.Fatal(err)
		}
		count := 2
		if empty {
			count = 0
		}
		report.LastSeen = report.LastSeen.Add(time.Second)
		report.ScoreSample = auditScoreSample(at, 2, count)
		if err := server.updateNode(report); err != nil {
			t.Fatal(err)
		}
		accepted := server.nodes()[0].ScoreSample
		if accepted.Sequence != 2 || !accepted.ObservedAt.Equal(at) || accepted.Components["total"].Count != count || !accepted.Fresh(at) {
			t.Fatalf("clock correction retained an old score or lost an empty snapshot: %+v", accepted)
		}
		// A newer status may still contain an old, repeated, or legacy sample.
		for _, sequence := range []uint64{1, 2, 0} {
			report.LastSeen = report.LastSeen.Add(time.Second)
			report.ScoreSample = auditScoreSample(at.Add(time.Minute), sequence, 4)
			if err := server.updateNode(report); err != nil {
				t.Fatal(err)
			}
			if got := server.nodes()[0].ScoreSample; !reflect.DeepEqual(got, accepted) {
				t.Fatalf("reordered sequence %d replaced the latest score: %+v", sequence, got)
			}
		}
		// Both the accepted report and status responses must own their maps.
		report.LastSeen = report.LastSeen.Add(time.Second)
		report.ScoreSample = auditScoreSample(at, 3, count)
		if err := server.updateNode(report); err != nil {
			t.Fatal(err)
		}
		report.ScoreSample.Components["total"] = model.ScoreStatistic{Count: 9}
		out := server.nodes()[0].ScoreSample
		out.Components["total"] = model.ScoreStatistic{Count: 8}
		if server.nodes()[0].ScoreSample.Components["total"].Count != count {
			t.Fatal("external mutation changed the accepted score")
		}
	}
}

func TestScoreStatusLegacyOrderingAndSequenceUpgrade(t *testing.T) {
	server := topologyStatusServer()
	at := time.Now().UTC()
	report := topologyStatusAt(at)
	for i, sample := range []*model.PeerScoreSample{
		auditScoreSample(at, 0, 1),
		auditScoreSample(at.Add(time.Second), 0, 2),
		auditScoreSample(at, 0, 3),                   // stale legacy sample
		auditScoreSample(at.Add(-time.Second), 1, 4), // sequence takes precedence
	} {
		report.LastSeen = report.LastSeen.Add(time.Second)
		report.ScoreSample = sample
		if err := server.updateNode(report); err != nil {
			t.Fatal(err)
		}
		want := []int{1, 2, 2, 4}[i]
		if got := server.nodes()[0].ScoreSample.Components["total"].Count; got != want {
			t.Fatalf("step %d: count=%d, want=%d", i, got, want)
		}
	}
}

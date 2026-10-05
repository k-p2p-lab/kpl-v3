package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func scoreSeriesByGroup(t *testing.T, series []analysisScoreSeries, group string) []analysisScorePoint {
	t.Helper()
	for _, item := range series {
		if item.Group == group {
			return item.Points
		}
	}
	t.Fatalf("score group %q missing", group)
	return nil
}

func TestAnalysisScoreTimelineRetainsMeasurementsDiscardedByObservationThinning(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	at := time.Unix(1000, 0).UTC()
	resultFixture(t, s, "run", "completed", at)
	observations := make([]analysisObservation, 1441)
	for i := range observations {
		observations[i] = analysisObservation{RunID: "run", At: at.Add(time.Duration(i) * 5 * time.Second), Groups: []analysisGroup{{Group: "workers", Layers: []analysisLayer{}}}}
	}
	zero := scoreComponentFixture(at, 2, 0).Components
	zero["p1"] = model.ScoreStatistic{Count: 2, Mean: 0, Min: -1, Max: 1}
	penalty := scoreComponentFixture(at, 3, -3).Components
	penalty["p3b"] = model.ScoreStatistic{Count: 3, Mean: -3, Min: -7, Max: 0}
	observations[1].Groups[0].ScoreComponents = zero
	observations[3].Groups = append(observations[3].Groups, analysisGroup{Group: "boot", ScoreComponents: penalty})
	observations[5].Groups[0].ScoreComponents = penalty
	analysisWriteLines(t, s, "run", "observations.jsonl", observations)
	analysis := analysisRequest(t, s, "run")
	if analysis.ObservationCount != 1441 || len(analysis.Observations) > 1440 {
		t.Fatalf("general observation budget changed: %d/%d", analysis.ObservationCount, len(analysis.Observations))
	}
	for _, observation := range analysis.Observations {
		for _, group := range observation.Groups {
			if len(group.ScoreComponents) > 0 {
				t.Fatal("fixture no longer reproduces discarded original score observations")
			}
		}
	}
	workers := scoreSeriesByGroup(t, analysis.ScoreTimeline, "workers")
	if len(workers) != 2 || !workers[0].At.Equal(observations[1].At) || !reflect.DeepEqual(workers[0].Components, zero) || workers[0].BreakBefore || !workers[1].At.Equal(observations[5].At) || !reflect.DeepEqual(workers[1].Components, penalty) || !workers[1].BreakBefore {
		t.Fatalf("measured zero, penalty, time or missing interval changed: %+v", workers)
	}
	boot := scoreSeriesByGroup(t, analysis.ScoreTimeline, "boot")
	if len(boot) != 1 || !boot[0].At.Equal(observations[3].At) || !reflect.DeepEqual(boot[0].Components, penalty) {
		t.Fatalf("sparse second group lost: %+v", boot)
	}
}

func TestAnalysisScoreTimelineBoundsEachGroupWithoutBridgingDiscardedGaps(t *testing.T) {
	var timeline scoreTimeline
	at := time.Unix(1000, 0).UTC()
	const samples = 20000
	observed := make(map[int]map[string]model.ScoreStatistic)
	for i := 0; i < samples; i++ {
		observation := analysisObservation{At: at.Add(time.Duration(i) * time.Second)}
		if i%37 != 1 {
			components := scoreComponentFixture(at, 2, float64(i%11-5)).Components
			observed[i] = components
			observation.Groups = append(observation.Groups, analysisGroup{Group: "frequent", ScoreComponents: components})
		}
		if i == 1 || i == samples-1 {
			observation.Groups = append(observation.Groups, analysisGroup{Group: "rare", ScoreComponents: scoreComponentFixture(at, 1, 9).Components})
		}
		timeline.observe(observation)
	}
	points := scoreSeriesByGroup(t, timeline.result(), "frequent")
	if len(points) > analysisScorePointLimit || !points[0].At.Equal(at) || !points[len(points)-1].At.Equal(at.Add((samples-1)*time.Second)) {
		t.Fatalf("unbounded series or endpoints lost: n=%d first=%v last=%v", len(points), points[0].At, points[len(points)-1].At)
	}
	previous := -1
	for _, point := range points {
		index := int(point.At.Sub(at) / time.Second)
		if !reflect.DeepEqual(point.Components, observed[index]) {
			t.Fatalf("sample at %d was averaged or fabricated", index)
		}
		gap := false
		if previous >= 0 {
			for i := previous + 1; i < index; i++ {
				gap = gap || observed[i] == nil
			}
		}
		if point.BreakBefore != gap {
			t.Fatalf("gap across retained samples %d..%d: got %t, want %t", previous, index, point.BreakBefore, gap)
		}
		previous = index
	}
	rare := scoreSeriesByGroup(t, timeline.result(), "rare")
	if len(rare) != 2 || !rare[0].At.Equal(at.Add(time.Second)) || !rare[1].BreakBefore {
		t.Fatalf("frequent group displaced rare evidence: %+v", rare)
	}
}

func TestAnalysisScoreTimelinePartialAndUnorderedEvidenceHasNoAssumedContinuity(t *testing.T) {
	at := time.Unix(1000, 0).UTC()
	stat := model.ScoreStatistic{Count: 1, Mean: -2, Min: -2, Max: -2}
	var partial scoreTimeline
	for i, components := range []map[string]model.ScoreStatistic{{"p1": stat}, {"p1": stat, "p2": stat}, {"p1": stat}} {
		partial.observe(analysisObservation{At: at.Add(time.Duration(i) * time.Second), Groups: []analysisGroup{{Group: "", ScoreComponents: components}}})
	}
	points := scoreSeriesByGroup(t, partial.result(), "")
	if len(points) != 3 || points[0].BreakBefore || !points[1].BreakBefore || !points[2].BreakBefore {
		t.Fatalf("partial components were connected over absent values: %+v", points)
	}
	var unordered scoreTimeline
	for _, offset := range []int{3, 1, 2, 2} {
		unordered.observe(analysisObservation{At: at.Add(time.Duration(offset) * time.Second), Groups: []analysisGroup{{Group: "", ScoreComponents: map[string]model.ScoreStatistic{"p1": stat}}}})
	}
	points = scoreSeriesByGroup(t, unordered.result(), "")
	if len(points) != 4 {
		t.Fatal("clock reversal discarded recorded measurements")
	}
	for i, point := range points {
		if point.Components["p1"] != stat || i > 0 && (point.At.Before(points[i-1].At) || !point.BreakBefore) {
			t.Fatalf("clock reversal inferred continuity or changed a measurement: %+v", points)
		}
	}
}

func TestAnalysisScoreTimelineRetainsRarePartialComponentEndpoints(t *testing.T) {
	var timeline scoreTimeline
	at := time.Unix(1000, 0).UTC()
	for i := 0; i < 10000; i++ {
		components := map[string]model.ScoreStatistic{"p1": {Count: 1, Mean: 2, Min: 2, Max: 2}}
		if i == 1 {
			components["p7"] = model.ScoreStatistic{Count: 1, Mean: -3, Min: -3, Max: -3}
		}
		if i == 1999 {
			components["p7"] = model.ScoreStatistic{Count: 1}
		}
		timeline.observe(analysisObservation{At: at.Add(time.Duration(i) * time.Second), Groups: []analysisGroup{{Group: "workers", ScoreComponents: components}}})
	}
	points := scoreSeriesByGroup(t, timeline.result(), "workers")
	if len(points) > analysisScorePointLimit {
		t.Fatalf("component endpoints exceeded the group budget: %d", len(points))
	}
	var measured []analysisScorePoint
	for _, point := range points {
		if point.Components["p7"].Count > 0 {
			measured = append(measured, point)
		}
	}
	if len(measured) != 2 || !measured[0].At.Equal(at.Add(time.Second)) || measured[0].Components["p7"].Mean != -3 || !measured[1].At.Equal(at.Add(1999*time.Second)) || measured[1].Components["p7"].Mean != 0 || !measured[1].BreakBefore {
		t.Fatalf("rare component endpoints disappeared or missing evidence was bridged: %+v", measured)
	}
}

func TestAnalysisScoreTimelinePersistsInRunAndBatchButNotSummary(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	components := scoreComponentFixture(time.Unix(1100, 0), 2, -3).Components
	for i, id := range []string{"first", "second"} {
		batchFixture(t, s, "batch", id, "completed", i+1, 2, 1)
		analysisWriteLines(t, s, id, "observations.jsonl", []analysisObservation{{RunID: id, At: time.Unix(1100+int64(i)*100, 0).UTC(), Groups: []analysisGroup{{Group: "workers", ScoreComponents: components}}}})
	}
	if _, err := s.startAnalysisJob(context.Background(), "first", false); err != nil {
		t.Fatal(err)
	}
	individual := awaitAnalysisJob(t, s, "first")
	s.analysisWorkers.Wait()
	if individual.State != "completed" {
		t.Fatalf("individual analysis: %+v", individual)
	}
	if _, err := s.startBatchAnalysis(context.Background(), "batch", false); err != nil {
		t.Fatal(err)
	}
	batch := awaitBatch(t, s, "batch")
	s.analysisWorkers.Wait()
	if batch.State != "completed" {
		t.Fatalf("batch analysis: %+v", batch)
	}
	restarted := New(s.config, nil)
	for _, server := range []*Server{s, restarted} {
		response := resultRequest(server, http.MethodGet, individual.ResultURL)
		var run resultAnalysis
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &run) != nil {
			t.Fatalf("run result: %d %s", response.Code, response.Body)
		}
		if got := scoreSeriesByGroup(t, run.ScoreTimeline, "workers"); len(got) != 1 || !reflect.DeepEqual(got[0].Components, components) {
			t.Fatal("individual score timeline lost")
		}
		summary := resultRequest(server, http.MethodGet, fmt.Sprintf("/api/v1/analysis-jobs/first/summary?jobId=%s", individual.ID))
		if summary.Code != http.StatusOK || !strings.Contains(summary.Body.String(), `"scoreTimeline":[]`) {
			t.Fatalf("summary retained detailed timeline or omitted explicit empty array: %d %s", summary.Code, summary.Body)
		}
		response = resultRequest(server, http.MethodGet, batch.ResultURL)
		var result batchAnalysisResult
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil || len(result.Runs) != 2 {
			t.Fatalf("batch result: %d %s", response.Code, response.Body)
		}
		for _, run := range result.Runs {
			if got := scoreSeriesByGroup(t, run.ScoreTimeline, "workers"); len(got) != 1 || !reflect.DeepEqual(got[0].Components, components) {
				t.Fatal("batch compaction removed component measurements")
			}
		}
	}
	// The previous analysis version may already have discarded score evidence.
	// Its completed artifact must request regeneration even with the same input.
	individual.AnalysisVersion = currentAnalysisVersion - 1
	if err := s.persistAnalysisJob(individual); err != nil {
		t.Fatal(err)
	}
	legacy := New(s.config, nil)
	status, err := legacy.analysisJobStatus("first")
	if err != nil || !status.Stale {
		t.Fatalf("old analysis version reused: %+v %v", status, err)
	}
	if _, err := legacy.startAnalysisJob(context.Background(), "first", false); err != nil {
		t.Fatal(err)
	}
	regenerated := awaitAnalysisJob(t, legacy, "first")
	legacy.analysisWorkers.Wait()
	if regenerated.State != "completed" || regenerated.ID == individual.ID || regenerated.AnalysisVersion != currentAnalysisVersion {
		t.Fatalf("old artifact was not regenerated: %+v", regenerated)
	}
}

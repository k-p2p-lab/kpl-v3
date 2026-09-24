package controller

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type analysisPoint struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type analysisBin struct {
	At        time.Time `json:"at"`
	Publish   int       `json:"publish"`
	Deliver   int       `json:"deliver"`
	Duplicate int       `json:"duplicate"`
	Start     int       `json:"start"`
	Stop      int       `json:"stop"`
	Send      int       `json:"send"`
	Recv      int       `json:"recv"`
	Drop      int       `json:"drop"`
}

type resultAnalysis struct {
	AnalysisVersion     int                   `json:"analysisVersion"`
	Research            *researchAnalysis     `json:"research,omitempty"`
	AnalysisID          string                `json:"analysisId,omitempty"`
	BandwidthTimeline   []bandwidthBin        `json:"bandwidthTimeline"`
	BandwidthBinSeconds int64                 `json:"bandwidthBinSeconds"`
	Version             int                   `json:"version"`
	Result              savedResult           `json:"result"`
	AsOf                time.Time             `json:"asOf"`
	EventBytes          int64                 `json:"eventBytes"`
	EventCount          int                   `json:"eventCount"`
	UntimedEvents       int                   `json:"untimedEvents"`
	Metrics             model.Metrics         `json:"metrics"`
	LatencyCDF          []analysisPoint       `json:"latencyCDF"`
	LatencyHistogram    []analysisPoint       `json:"latencyHistogram"`
	Timeline            []analysisBin         `json:"timeline"`
	BinSeconds          int64                 `json:"binSeconds"`
	Observations        []analysisObservation `json:"observations"`
	ObservationCount    int                   `json:"observationCount"`
}

func (s *Server) handleResultAnalysis(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	if !validResultID(id) {
		http.NotFound(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	// User-initiated analysis runs independently of archive size inspection.
	// Bound expensive event indexes without holding persistence/deletion locks.
	select {
	case s.analysisSlots <- struct{}{}:
		defer func() { <-s.analysisSlots }()
	case <-ctx.Done():
		writeError(w, http.StatusGatewayTimeout, "analysis timed out waiting for another analysis; try one result at a time")
		return
	}
	snapshot, err := s.captureResultFiles(id, false)
	if err != nil {
		if errors.Is(err, errResultNotFound) {
			http.NotFound(w, r)
		} else {
			writeError(w, http.StatusUnprocessableEntity, err.Error())
		}
		return
	}
	defer snapshot.close()
	analysis, err := analyzeResult(ctx, snapshot)
	if err != nil {
		status := http.StatusUnprocessableEntity
		if ctx.Err() != nil {
			status = http.StatusGatewayTimeout
		}
		writeError(w, status, err.Error())
		return
	}
	s.state.persistMu.Lock()
	deleted, err := s.state.resultDeletedLocked(id)
	s.state.persistMu.Unlock()
	if deleted {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot check saved result")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, analysis)
}

type analysisProgress func(phase string, bytes int64)
type analysisProgressKey struct{}
type analysisProgressReader struct {
	io.Reader
	report func(int64)
}

func (r analysisProgressReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.report(int64(n))
	}
	return n, err
}
func reportAnalysisProgress(ctx context.Context, phase string, bytes int64) {
	if report, ok := ctx.Value(analysisProgressKey{}).(analysisProgress); ok {
		report(phase, bytes)
	}
}

func scanAnalysisFile(ctx context.Context, file resultFile, consume func([]byte) error) error {
	if file.file == nil {
		return nil
	}
	reportAnalysisProgress(ctx, file.name, 0)
	reader := analysisProgressReader{Reader: io.NewSectionReader(file.file, 0, file.size), report: func(n int64) { reportAnalysisProgress(ctx, file.name, n) }}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := consume(scanner.Bytes()); err != nil {
			return fmt.Errorf("%s line %d: %w", file.name, line, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read %s: %w", file.name, err)
	}
	return ctx.Err()
}

func analyzeResult(ctx context.Context, snapshot *resultSnapshot) (resultAnalysis, error) {
	result := resultAnalysis{Version: 1, Result: snapshot.result, AsOf: snapshot.exportedAt,
		LatencyCDF: []analysisPoint{}, LatencyHistogram: []analysisPoint{}, Timeline: []analysisBin{}, Observations: []analysisObservation{}}
	accumulator := newRunMetricAccumulator()
	accumulator.research = newResearchAccumulator()
	var bandwidth bandwidthTimeline
	accumulator.onBandwidth = bandwidth.add
	bins := make(map[int64]analysisBin)
	width := int64(1)
	stride := 1
	var latestObservation analysisObservation
	for _, file := range snapshot.files {
		switch file.name {
		case "events.jsonl":
			result.EventBytes = file.size
			err := scanAnalysisFile(ctx, file, func(data []byte) error {
				var event model.TraceEvent
				if err := json.Unmarshal(data, &event); err != nil {
					return err
				}
				if event.RunID != snapshot.result.ID || !accumulator.observe(event) {
					return nil
				}
				result.EventCount++
				if event.Timestamp.IsZero() {
					result.UntimedEvents++
					return nil
				}
				key := analysisBucket(event.Timestamp.Unix(), width)
				bin := bins[key]
				switch event.Type {
				case "publish":
					bin.Publish++
				case "deliver":
					bin.Deliver++
				case "duplicate":
					bin.Duplicate++
				case "measurement_start":
					bin.Start++
				case "measurement_stop":
					bin.Stop++
				default:
					control, ok := gossipSubControlEvent(event.Type)
					if !ok {
						return nil
					}
					switch control.direction {
					case "send":
						bin.Send++
					case "recv":
						bin.Recv++
					case "drop":
						bin.Drop++
					}
				}
				bins[key] = bin
				for len(bins) > 360 {
					width *= 2
					merged := make(map[int64]analysisBin)
					for at, value := range bins {
						key := analysisBucket(at, width)
						sum := merged[key]
						sum.Publish += value.Publish
						sum.Deliver += value.Deliver
						sum.Duplicate += value.Duplicate
						sum.Start += value.Start
						sum.Stop += value.Stop
						sum.Send += value.Send
						sum.Recv += value.Recv
						sum.Drop += value.Drop
						merged[key] = sum
					}
					bins = merged
				}
				return nil
			})
			if err != nil {
				return result, err
			}
		case "observations.jsonl":
			err := scanAnalysisFile(ctx, file, func(data []byte) error {
				var observation analysisObservation
				if err := json.Unmarshal(data, &observation); err != nil {
					return err
				}
				if observation.RunID != result.Result.ID {
					return nil
				}
				if observation.At.IsZero() {
					return errors.New("observation timestamp is missing")
				}
				if latestObservation.At.IsZero() || observation.At.After(latestObservation.At) {
					latestObservation = observation
				}
				accumulator.research.observeGroups(observation)
				result.ObservationCount++
				if (result.ObservationCount-1)%stride != 0 {
					return nil
				}
				result.Observations = append(result.Observations, observation)
				if len(result.Observations) > 1440 {
					for i := 0; i < len(result.Observations); i += 2 {
						result.Observations[i/2] = result.Observations[i]
					}
					result.Observations = result.Observations[:(len(result.Observations)+1)/2]
					stride *= 2
				}
				return nil
			})
			if err != nil {
				return result, err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.BandwidthTimeline, result.BandwidthBinSeconds = bandwidth.result()
	// Preserve the latest sample even when older observations are downsampled.
	if !latestObservation.At.IsZero() {
		found := false
		for _, observation := range result.Observations {
			if observation.At.Equal(latestObservation.At) {
				found = true
				break
			}
		}
		if !found {
			if len(result.Observations) >= 1440 {
				result.Observations = result.Observations[:1439]
			}
			result.Observations = append(result.Observations, latestObservation)
		}
	}
	reportAnalysisProgress(ctx, "aggregating", 0)
	metrics, samples, err := accumulator.summarizeContext(ctx, result.Result.ID, result.AsOf)
	if err != nil {
		return result, err
	}
	result.Metrics = metrics
	values := make([]float64, 0, len(samples))
	for _, sample := range samples {
		values = append(values, sample.seconds*1000)
	}
	result.LatencyCDF, result.LatencyHistogram = analysisDistribution(values)
	result.BinSeconds = width
	for at, bin := range bins {
		bin.At = time.Unix(at, 0).UTC()
		result.Timeline = append(result.Timeline, bin)
	}
	sort.Slice(result.Timeline, func(i, j int) bool { return result.Timeline[i].At.Before(result.Timeline[j].At) })
	sort.Slice(result.Observations, func(i, j int) bool { return result.Observations[i].At.Before(result.Observations[j].At) })
	if err := enrichAnalysisGraphs(ctx, result.Observations); err != nil {
		return result, err
	}
	research, err := accumulator.research.finish(ctx, accumulator, result.Observations)
	if err != nil {
		return result, err
	}
	research.ReceiverGroups, err = accumulator.research.receiverGroups(ctx, accumulator, research.Messages, result.Observations, samples, result.Result.StartedAt)
	if err != nil {
		return result, err
	}
	result.Research = &research
	result.AnalysisVersion = currentAnalysisVersion
	return result, ctx.Err()
}

func analysisBucket(seconds, width int64) int64 {
	quotient := seconds / width
	if seconds < 0 && seconds%width != 0 {
		quotient--
	}
	return quotient * width
}

func analysisDistribution(values []float64) ([]analysisPoint, []analysisPoint) {
	cdf, histogram := []analysisPoint{}, []analysisPoint{}
	if len(values) == 0 {
		return cdf, histogram
	}
	sort.Float64s(values)
	step := max(1, (len(values)+199)/200)
	for i := 0; i < len(values); {
		end := i + 1
		for end < len(values) && values[end] == values[i] {
			end++
		}
		if len(cdf) == 0 || end == len(values) || end/step > i/step {
			cdf = append(cdf, analysisPoint{values[i], float64(end) / float64(len(values))})
		}
		i = end
	}
	count := min(30, max(1, int(math.Ceil(math.Sqrt(float64(len(values)))))))
	width := (values[len(values)-1] - values[0]) / float64(count)
	if width == 0 {
		return cdf, []analysisPoint{{values[0], float64(len(values))}}
	}
	histogram = make([]analysisPoint, count)
	for i := range histogram {
		histogram[i].X = values[0] + (float64(i)+0.5)*width
	}
	for _, value := range values {
		histogram[min(count-1, int((value-values[0])/width))].Y++
	}
	return cdf, histogram
}

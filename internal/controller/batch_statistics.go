package controller

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"
)

type batchSeriesPoint struct {
	X     float64  `json:"x"`
	Y     *float64 `json:"y"`
	Error *float64 `json:"error"`
}
type researchOverview struct {
	MessageSeries map[string][]batchSeriesPoint `json:"messageSeries"`
	OriginCounts  map[string]int                `json:"originCounts"`
	ReceiversTime []analysisPoint               `json:"receiversTime"`
	ReceiversHop  []analysisPoint               `json:"receiversHop"`
}

// Keep all overview chart inputs, without retaining per-node identities or
// message payloads for every repetition in a potentially large batch.
func compactBatchAnalysis(a *resultAnalysis) {
	a.Result.Analysis, a.Result.BatchAnalysis = nil, nil
	for i := range a.Observations {
		a.Observations[i].Graphs = nil
		for j := range a.Observations[i].Groups {
			for k := range a.Observations[i].Groups[j].Layers {
				a.Observations[i].Groups[j].Layers[k].Degrees = nil
			}
		}
	}
	r := a.Research
	if r == nil {
		return
	}
	overview := &researchOverview{MessageSeries: map[string][]batchSeriesPoint{}, OriginCounts: map[string]int{"eager": 0, "lazy": 0, "unknown": 0}}
	times, hops := map[float64]float64{}, map[float64]float64{}
	for _, message := range r.Messages {
		for _, node := range message.Nodes {
			source := node.Source
			if source != "eager" && source != "lazy" {
				source = "unknown"
			}
			overview.OriginCounts[source]++
			if node.Seconds != nil && *node.Seconds >= 0 {
				times[*node.Seconds]++
			}
			if node.Hop != nil && *node.Hop >= 0 {
				hops[float64(*node.Hop)]++
			}
		}
	}
	overview.ReceiversTime = cumulativePoints(times, float64(len(r.Messages)))
	overview.ReceiversHop = cumulativePoints(hops, float64(len(r.Messages)))
	// A published run with zero receipts contributes a measured zero count.
	// Receipts lacking time/hop evidence instead retain an unavailable curve.
	if len(r.Messages) > 0 && overview.OriginCounts["eager"]+overview.OriginCounts["lazy"]+overview.OriginCounts["unknown"] == 0 {
		overview.ReceiversTime = []analysisPoint{{X: 0, Y: 0}}
		overview.ReceiversHop = []analysisPoint{{X: 0, Y: 0}}
		if r.EligiblePopulation > 0 {
			r.PropagationCDF = []analysisPoint{{X: 0, Y: 0}}
		}
	}
	if r.EligiblePopulation > 0 && a.Metrics.Duplicates == 0 && len(r.DuplicateCDF) == 0 {
		r.DuplicateCDF = []analysisPoint{{X: 0, Y: 0}}
	}
	overview.MessageSeries = researchMessageSeries(r.Messages, a.Result.StartedAt)
	r.MessageCount = len(r.Messages)
	r.Messages = []researchMessage{}
	r.Overview = overview
}

func researchMessageSeries(messages []researchMessage, origin time.Time) map[string][]batchSeriesPoint {
	series := map[string][]batchSeriesPoint{}
	for _, key := range []string{"frt", "eager_frt", "reachability", "eager_reachability", "drc", "drc_per_node_count", "eager_count", "lazy_count", "unknown_count"} {
		rows := []researchMessage{}
		for _, message := range messages {
			if message.Metrics[key].Average != nil && !message.At.IsZero() {
				rows = append(rows, message)
			}
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].At.Before(rows[j].At) })
		points := []batchSeriesPoint{}
		chunk := max(1, (len(rows)+359)/360)
		for i := 0; i < len(rows); i += chunk {
			values := []float64{}
			for _, row := range rows[i:min(i+chunk, len(rows))] {
				values = append(values, *row.Metrics[key].Average)
			}
			stat := batchStats(values)
			deviation := stat.Deviation
			if chunk == 1 {
				deviation = rows[i].Metrics[key].Deviation
			}
			pointOrigin := origin
			if pointOrigin.IsZero() {
				pointOrigin = rows[0].At
			}
			points = append(points, batchSeriesPoint{X: rows[i].At.Sub(pointOrigin).Seconds(), Y: stat.Average, Error: deviation})
		}
		series[key] = points
	}
	return series
}

// Sample SD describes variation between runs. One run has no sample SD.
func batchStats(values []float64) analysisStatistic {
	stat := analysisStats(values)
	if stat.Count < 2 {
		stat.Deviation = nil
	} else if stat.Deviation != nil {
		stat.Deviation = numberPointer(*stat.Deviation * math.Sqrt(float64(stat.Count)/float64(stat.Count-1)))
	}
	return stat
}
func batchSummary(runs []resultAnalysis) map[string]analysisStatistic {
	values := map[string][]float64{}
	add := func(key string, value *float64) {
		if _, ok := values[key]; !ok {
			values[key] = nil
		}
		if value != nil {
			values[key] = append(values[key], *value)
		}
	}
	for _, run := range runs {
		data, _ := json.Marshal(run.Metrics)
		metrics := map[string]any{}
		_ = json.Unmarshal(data, &metrics)
		for key, raw := range metrics {
			value, ok := raw.(float64)
			if !ok {
				continue
			}
			available := true
			switch key {
			case "averageLatencyMs", "p95LatencyMs":
				available = run.Metrics.LatencySamples > 0
			case "averageDuplicates":
				available = run.Metrics.DuplicateSamples > 0
			case "reachability", "deliveryRatioUpperBound":
				available = run.Metrics.DeliveryRatioAvailable
			case "initialDeliveryRatio", "initialDeliveryRatioUpperBound":
				available = run.Metrics.InitialDeliveryRatioAvailable
			case "stableCoverage", "stableCoverageUpperBound":
				available = run.Metrics.StableCoverageAvailable
			}
			if key == "deliveryRatioUpperBound" && run.Metrics.Definition != "session-window-v1" {
				available = false
			}
			if available {
				add("metrics."+key, numberPointer(value))
			} else {
				add("metrics."+key, nil)
			}
		}
		if run.Research != nil {
			for key, stat := range run.Research.Summary {
				add("research."+key, stat.Average)
			}
		}
		for _, key := range []string{"sentBytes", "receivedBytes"} {
			var value *float64
			if bw := run.Metrics.Bandwidth; bw != nil && bw.Samples > 0 {
				if key == "sentBytes" {
					value = numberPointer(float64(bw.SentBytes))
				} else {
					value = numberPointer(float64(bw.ReceivedBytes))
				}
			}
			add("bandwidth."+key, value)
		}
	}
	out := map[string]analysisStatistic{}
	for key, vs := range values {
		out[key] = batchStats(vs)
	}
	return out
}

func validateBatchDefinitions(runs []resultAnalysis) error {
	definitions := map[string]bool{}
	for _, run := range runs {
		m := run.Metrics
		if m.LatencySamples > 0 || m.DuplicateSamples > 0 || m.DeliveryRatioAvailable || m.InitialDeliveryRatioAvailable || m.StableCoverageAvailable {
			definitions[m.Definition] = true
		}
	}
	if len(definitions) > 1 {
		return fmt.Errorf("batch runs use different measurement definitions; analyze these runs individually instead of averaging incompatible populations")
	}
	return nil
}

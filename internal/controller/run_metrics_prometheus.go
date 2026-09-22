package controller

import (
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

// Reconstructing the histogram from first deliveries also handles out-of-order
// batches: a late publish cohort or an earlier delivery can correct a sample.
// No transport retry, self-delivery, or late join enters the histogram.
type runMetricsCollector struct {
	state                                                              *state
	propagation, expected, delivered, ratio, duplicates, duplicateMean *prometheus.Desc
	windowPropagation, windowRatio, windowInitialRatio                 *prometheus.Desc
	windowGauges                                                       []windowGauge
}

func newRunMetricsCollector(s *state) *runMetricsCollector {
	collector := &runMetricsCollector{
		state:              s,
		propagation:        prometheus.NewDesc("kpl_propagation_latency_seconds", "Whole-run first remote cohort application latency; excludes raw, self, late join, unknown cohorts and negative clock-skew samples. Late events may correct buckets/sum: query directly, not with rate/increase.", []string{"run_id", "agent_id", "topic"}, nil),
		expected:           prometheus.NewDesc("kpl_delivery_expected_pairs", "Remote subscriber pairs frozen at publish dispatch, including subsequent departures.", []string{"run_id"}, nil),
		delivered:          prometheus.NewDesc("kpl_delivery_reached_pairs", "Distinct first deliveries among the frozen remote subscriber pairs.", []string{"run_id"}, nil),
		ratio:              prometheus.NewDesc("kpl_delivery_ratio", "Reached pairs divided by expected pairs over the observed run; absent when denominator is zero.", []string{"run_id"}, nil),
		duplicates:         prometheus.NewDesc("kpl_delivery_duplicate_copies", "Additional PubSub copies at successfully delivered remote cohort pairs.", []string{"run_id"}, nil),
		duplicateMean:      prometheus.NewDesc("kpl_delivery_duplicates_per_reached_pair", "Additional copies divided by successful remote cohort pairs; absent when denominator is zero.", []string{"run_id"}, nil),
		windowPropagation:  prometheus.NewDesc("kpl_window_propagation_latency_seconds", "First on-time application latency at stable successful receiver sessions. Excludes raw timestamps and invalid clock ordering. Late evidence can correct this snapshot histogram: query directly, not with rate/increase.", []string{"run_id", "agent_id", "topic"}, nil),
		windowRatio:        prometheus.NewDesc("kpl_window_delivery_ratio", "On-time delivery lower/upper bounds among receiver sessions proved alive through the delivery window. Unknown receipt evidence widens bounds; absent when no stable pairs are known.", []string{"run_id", "bound"}, nil),
		windowInitialRatio: prometheus.NewDesc("kpl_window_initial_delivery_ratio", "On-time delivery lower/upper bounds for the initial living receiver cohort, including departures. Absent when initial membership or availability is unknown.", []string{"run_id", "bound"}, nil),
	}
	collector.windowGauges = newWindowGauges()
	return collector
}

func (c *runMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{c.propagation, c.expected, c.delivered, c.ratio, c.duplicates, c.duplicateMean} {
		ch <- desc
	}
	for _, desc := range []*prometheus.Desc{c.windowPropagation, c.windowRatio, c.windowInitialRatio} {
		ch <- desc
	}
	for _, gauge := range c.windowGauges {
		ch <- gauge.desc
	}
}

var propagationBounds = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

type runPropagationKey struct{ runID, agentID, topic string }
type propagationHistogram struct {
	count   uint64
	sum     float64
	buckets map[float64]uint64
}

// Immutable histograms share the summary's revision/deadline cache. Scraping
// settled experiments must not replay every historical delivery into buckets.
// New events (including late gap fills) still invalidate the exact summary.
func (a *runMetricAccumulator) livePrometheusSummary(runID string, now time.Time) (model.Metrics, map[propagationSeriesKey]*propagationHistogram) {
	a.summaryMu.Lock()
	defer a.summaryMu.Unlock()
	result, samples := a.liveSummaryLocked(runID, now)
	if a.cachedHistograms == nil {
		a.cachedHistograms = make(map[propagationSeriesKey]*propagationHistogram)
		for _, sample := range samples {
			histogram := a.cachedHistograms[sample.key]
			if histogram == nil {
				histogram = &propagationHistogram{buckets: make(map[float64]uint64, len(propagationBounds))}
				for _, bound := range propagationBounds {
					histogram.buckets[bound] = 0
				}
				a.cachedHistograms[sample.key] = histogram
			}
			histogram.count++
			histogram.sum += sample.seconds
			for _, bound := range propagationBounds {
				if sample.seconds <= bound {
					histogram.buckets[bound]++
				}
			}
		}
	}
	return result, a.cachedHistograms
}

func (c *runMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	metrics := make([]model.Metrics, 0)
	definitions := make(map[string]string)
	asOf := time.Now().UTC()
	histograms := make(map[runPropagationKey]*propagationHistogram)
	initHistogram := func(key runPropagationKey) *propagationHistogram {
		if current := histograms[key]; current != nil {
			return current
		}
		current := &propagationHistogram{buckets: make(map[float64]uint64, len(propagationBounds))}
		for _, bound := range propagationBounds {
			current.buckets[bound] = 0
		}
		histograms[key] = current
		return current
	}
	c.state.mu.RLock()
	for runID, accumulator := range c.state.runMetrics {
		if runID == "" {
			continue
		}
		result, cached := accumulator.livePrometheusSummary(runID, asOf)
		definitions[runID] = result.Definition
		metrics = append(metrics, result)
		for key, histogram := range cached {
			histograms[runPropagationKey{runID, key.agentID, key.topic}] = histogram
		}
	}
	for _, node := range c.state.nodes {
		if node.RunID == "" || node.AgentID == "" {
			continue
		}
		if _, known := c.state.experiments[node.RunID]; !known && c.state.runMetrics[node.RunID] == nil {
			continue
		}
		topics := nodeTopics(node)
		if len(topics) == 0 {
			topics = []string{"kpl/default"}
		}
		for _, topic := range topics {
			initHistogram(runPropagationKey{node.RunID, node.AgentID, topic})
		}
	}
	c.state.mu.RUnlock()
	for _, result := range metrics {
		if result.Definition == sessionWindowDefinition {
			c.collectWindow(ch, result)
			continue
		}
		for _, gauge := range c.windowGauges {
			if gauge.name == "legacy_publications" {
				ch <- prometheus.MustNewConstMetric(gauge.desc, prometheus.GaugeValue, float64(result.Published), result.RunID)
			}
		}
		ch <- prometheus.MustNewConstMetric(c.expected, prometheus.GaugeValue, float64(result.ExpectedDeliveries), result.RunID)
		ch <- prometheus.MustNewConstMetric(c.delivered, prometheus.GaugeValue, float64(result.EligibleDeliveries), result.RunID)
		ch <- prometheus.MustNewConstMetric(c.duplicates, prometheus.GaugeValue, float64(result.EligibleDuplicates), result.RunID)
		if result.DeliveryRatioAvailable {
			ch <- prometheus.MustNewConstMetric(c.ratio, prometheus.GaugeValue, result.Reachability, result.RunID)
		}
		if result.DuplicateSamples > 0 {
			ch <- prometheus.MustNewConstMetric(c.duplicateMean, prometheus.GaugeValue, result.AverageDuplicates, result.RunID)
		}
	}
	for key, histogram := range histograms {
		desc := c.propagation
		if definitions[key.runID] == sessionWindowDefinition {
			desc = c.windowPropagation
		}
		ch <- prometheus.MustNewConstHistogram(desc, histogram.count, histogram.sum, histogram.buckets, key.runID, key.agentID, key.topic)
	}
}

type windowGauge struct {
	name      string
	desc      *prometheus.Desc
	value     func(model.Metrics) float64
	available func(model.Metrics) bool
}

func newWindowGauges() []windowGauge {
	var gauges []windowGauge
	add := func(name, help string, value func(model.Metrics) float64, available func(model.Metrics) bool) {
		gauges = append(gauges, windowGauge{name, prometheus.NewDesc("kpl_window_"+name, help, []string{"run_id"}, nil), value, available})
	}
	add("stable_pairs", "Receiver pairs proved alive from actual publication through its delivery deadline, excluding early departures.", func(m model.Metrics) float64 { return float64(m.ExpectedDeliveries) }, nil)
	add("reached_pairs", "Stable receiver pairs with a same-session on-time application receipt.", func(m model.Metrics) float64 { return float64(m.EligibleDeliveries) }, nil)
	add("unknown_pairs", "Stable receiver pairs whose missing on-time receipt cannot be certified because of sequence gaps or invalid clock ordering.", func(m model.Metrics) float64 { return float64(m.UnknownDeliveries) }, nil)
	add("missed_pairs", "Stable receiver pairs with no on-time receipt and a complete source sequence prefix through the delivery deadline.", func(m model.Metrics) float64 { return float64(m.MissedDeliveries) }, nil)
	add("late_pairs", "Stable receiver pairs with an observed receipt after the deadline and no valid on-time receipt.", func(m model.Metrics) float64 { return float64(m.LateDeliveries) }, nil)
	add("initial_pairs", "Receiver pairs proved alive at actual publication time, including subsequent departures.", func(m model.Metrics) float64 { return float64(m.InitialExpectedDeliveries) }, nil)
	add("initial_reached_pairs", "Initial cohort receiver pairs with a same-session on-time receipt, including early departures.", func(m model.Metrics) float64 { return float64(m.InitialEligibleDeliveries) }, nil)
	add("initial_unknown_pairs", "Initial cohort receiver pairs with no on-time receipt and incomplete observation through their stop or delivery deadline.", func(m model.Metrics) float64 { return float64(m.InitialUnknownDeliveries) }, nil)
	add("departed_pairs", "Initial cohort receiver pairs that ended before the delivery deadline, regardless of receipt outcome.", func(m model.Metrics) float64 { return float64(m.DepartedPairs) }, nil)
	add("publication_availability_unknown_pairs", "Candidate receiver pairs lacking proof of availability at publication; excluded from the known starting cohort.", func(m model.Metrics) float64 { return float64(m.PublicationAvailabilityUnknownPairs) }, nil)
	add("continuity_unknown_pairs", "Known starting-cohort receiver pairs with neither a confirmed departure nor proof of survival through the delivery deadline.", func(m model.Metrics) float64 { return float64(m.ContinuityUnknownPairs) }, nil)
	add("availability_unknown_pairs", "Compatibility total of publication-availability-unknown and continuity-unknown receiver pairs.", func(m model.Metrics) float64 { return float64(m.AvailabilityUnknownPairs) }, nil)
	add("stable_coverage", "Lower bound of proven stable receiver pairs divided by known starting-cohort pairs; absent only when that cohort is empty.", func(m model.Metrics) float64 { return m.StableCoverage }, func(m model.Metrics) bool { return m.StableCoverageAvailable })
	add("stable_coverage_upper_bound", "Upper bound of stable receiver coverage when continuity-unknown known starting pairs are treated as stable; absent only when that cohort is empty.", func(m model.Metrics) float64 { return m.StableCoverageUpperBound }, func(m model.Metrics) bool { return m.StableCoverageAvailable })
	add("pending_publications", "Observed session-window publications whose delivery deadline has not matured.", func(m model.Metrics) float64 { return float64(m.PendingPublications) }, nil)
	add("finalized_publications", "Observed session-window publications whose delivery deadline has matured; telemetry evidence may still arrive later.", func(m model.Metrics) float64 { return float64(m.FinalizedPublications) }, nil)
	add("legacy_publications", "Legacy publications excluded from session-window metrics; legacy-only runs expose their publication count here.", func(m model.Metrics) float64 { return float64(m.LegacyPublications) }, nil)
	add("measurement_incomplete", "One when a known source lacks valid session-start, exact subscription, or lifecycle evidence. Fully silent sessions cannot be detected.", func(m model.Metrics) float64 {
		if m.MeasurementIncomplete {
			return 1
		}
		return 0
	}, nil)
	add("duplicate_copies", "Additional on-time PubSub copies at stable successful receiver pairs.", func(m model.Metrics) float64 { return float64(m.EligibleDuplicates) }, nil)
	add("duplicates_per_reached_pair", "Additional on-time copies divided by stable successful receiver pairs; absent when none succeeded.", func(m model.Metrics) float64 { return m.AverageDuplicates }, func(m model.Metrics) bool { return m.DuplicateSamples > 0 })
	return gauges
}

func (c *runMetricsCollector) collectWindow(ch chan<- prometheus.Metric, result model.Metrics) {
	for _, gauge := range c.windowGauges {
		if gauge.available == nil || gauge.available(result) {
			ch <- prometheus.MustNewConstMetric(gauge.desc, prometheus.GaugeValue, gauge.value(result), result.RunID)
		}
	}
	if result.DeliveryRatioAvailable {
		ch <- prometheus.MustNewConstMetric(c.windowRatio, prometheus.GaugeValue, result.Reachability, result.RunID, "lower")
		ch <- prometheus.MustNewConstMetric(c.windowRatio, prometheus.GaugeValue, result.DeliveryRatioUpperBound, result.RunID, "upper")
	}
	if result.InitialDeliveryRatioAvailable {
		ch <- prometheus.MustNewConstMetric(c.windowInitialRatio, prometheus.GaugeValue, result.InitialDeliveryRatio, result.RunID, "lower")
		ch <- prometheus.MustNewConstMetric(c.windowInitialRatio, prometheus.GaugeValue, result.InitialDeliveryRatioUpperBound, result.RunID, "upper")
	}
}

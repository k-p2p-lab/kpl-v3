package controller

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type messageMetricKey struct{ topic, id string }
type deliveryMetric struct {
	research             *deliveryResearch
	timestamp            time.Time
	agentID              string
	latencyMS            float64
	latencyAvailable     bool
	clockSynchronized    bool
	latencyUncertaintyMS float64
}
type messageMetric struct {
	publishedAt time.Time
	published   bool
	publisher   string
	targets     map[string]struct{} // nil: unknown legacy cohort, empty: explicitly none
	deliveries  map[string]deliveryMetric
	duplicates  map[string]int
}

// This compact per-message index outlives the 300-row recent-events feed.
// All access is protected by state.mu; deleting a result releases its index.
type runMetricAccumulator struct {
	research                         *researchAccumulator
	seen                             map[string]struct{} // Legacy, unsequenced event IDs only.
	sequences                        map[measurementSessionKey]sequenceRanges
	messages                         map[messageMetricKey]*messageMetric
	control                          map[gossipSubControlKey]*model.GossipSubControlMetric
	published, delivered, duplicates int
	window                           sessionWindowAccumulator
	bandwidth                        bandwidthAccumulator
	onBandwidth                      func(bandwidthInterval)
	// The state read lock permits concurrent dashboard/Prometheus readers.
	summaryMu        sync.Mutex
	revision         uint64
	cachedRevision   uint64
	cachedAt         time.Time
	cachedRun        string
	cachedMetrics    model.Metrics
	cachedSamples    []propagationSample
	cachedHistograms map[propagationSeriesKey]*propagationHistogram
}

func newRunMetricAccumulator() *runMetricAccumulator {
	return &runMetricAccumulator{
		seen: make(map[string]struct{}), messages: make(map[messageMetricKey]*messageMetric),
		control:   make(map[gossipSubControlKey]*model.GossipSubControlMetric),
		sequences: make(map[measurementSessionKey]sequenceRanges),
	}
}

type gossipSubControlKey struct{ agentID, direction, controlType string }

func eventIdentity(event model.TraceEvent) string {
	if event.SessionID != "" && event.Sequence > 0 {
		return fmt.Sprintf("%s\x00session:%s:%d", event.NodeID, event.SessionID, event.Sequence)
	}
	if event.EventID == "" {
		return ""
	}
	return event.NodeID + "\x00" + event.EventID
}

func (a *runMetricAccumulator) hasEvent(event model.TraceEvent) bool {
	if event.SessionID != "" && event.Sequence > 0 {
		return a.sequences[measurementSessionKey{event.NodeID, event.SessionID}].through(event.Sequence) != 0
	}
	id := eventIdentity(event)
	_, exists := a.seen[id]
	return id != "" && exists
}

// observe tolerates delivery/duplicate telemetry arriving before publication.
// Event IDs distinguish actual network duplicates from HTTP batch retries.
func (a *runMetricAccumulator) observe(event model.TraceEvent) bool {
	if a.hasEvent(event) {
		return false
	}
	if event.SessionID != "" && event.Sequence > 0 {
		key := measurementSessionKey{event.NodeID, event.SessionID}
		ranges := a.sequences[key]
		ranges.add(event.Sequence)
		a.sequences[key] = ranges
	} else if id := eventIdentity(event); id != "" {
		a.seen[id] = struct{}{}
	}
	a.revision++
	if a.research != nil {
		a.research.observe(event)
	}
	a.window.observe(event)
	a.observeGossipSubControl(event)
	if interval := a.bandwidth.observe(event); interval != nil && a.onBandwidth != nil {
		a.onBandwidth(*interval)
	}
	if event.Type != "publish" && event.Type != "deliver" && event.Type != "duplicate" {
		return true
	}
	var message *messageMetric
	if event.MessageID != "" {
		key := messageMetricKey{event.Topic, event.MessageID}
		message = a.messages[key]
		if message == nil {
			message = &messageMetric{deliveries: make(map[string]deliveryMetric), duplicates: make(map[string]int)}
			a.messages[key] = message
		}
	}
	switch event.Type {
	case "publish":
		if message == nil {
			a.published++
			return true
		}
		if message.published {
			return true
		}
		a.published++
		message.published = true
		message.publisher = event.NodeID
		message.publishedAt = event.Timestamp
		if targets, known := targetNodeIDs(event.Fields["targetNodeIds"]); known {
			message.targets = make(map[string]struct{}, len(targets))
			for _, id := range targets {
				if id != "" && id != event.NodeID {
					message.targets[id] = struct{}{}
				}
			}
		}
	case "deliver":
		a.delivered++
		if message == nil || event.NodeID == "" {
			return true
		}
		if local, _ := event.Fields["localDelivery"].(bool); local {
			return true
		}
		previous, exists := message.deliveries[event.NodeID]
		if exists && !event.Timestamp.Before(previous.timestamp) {
			return true
		}
		encoding, _ := event.Fields["payloadEncoding"].(string)
		available, _ := event.Fields["latencyAvailable"].(bool)
		clockSynchronized, _ := event.Fields["latencyClockSynchronized"].(bool)
		latencyUncertaintyMS, _ := numericEventField(event.Fields, "latencyUncertaintyMs")
		var detail *deliveryResearch
		if a.research != nil {
			basis, _ := event.Fields["clockBasis"].(string)
			wireID, _ := event.Fields["pubsubMessageId"].(string)
			detail = &deliveryResearch{wireID: wireID, peer: event.PeerID, from: event.RemotePeerID, synchronized: basis == "controller-offset-v1"}
		}
		message.deliveries[event.NodeID] = deliveryMetric{
			research:  detail,
			timestamp: event.Timestamp, agentID: event.AgentID, latencyMS: event.LatencyMS,
			latencyAvailable: encoding == "envelope" && available, clockSynchronized: clockSynchronized,
			latencyUncertaintyMS: latencyUncertaintyMS,
		}
	case "duplicate":
		a.duplicates++
		if message != nil && event.NodeID != "" {
			message.duplicates[event.NodeID]++
		}
	}
	return true
}

func numericEventField(fields map[string]any, key string) (float64, bool) {
	value, exists := fields[key]
	if !exists {
		return 0, false
	}
	switch value := value.(type) {
	case float64:
		return value, true
	case float32:
		return float64(value), true
	case int:
		return float64(value), true
	case int64:
		return float64(value), true
	case json.Number:
		parsed, err := value.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}

func targetNodeIDs(value any) ([]string, bool) {
	switch value := value.(type) {
	case []string:
		return value, value != nil
	case []any:
		if value == nil {
			return nil, false
		}
		ids := make([]string, 0, len(value))
		for _, item := range value {
			id, ok := item.(string)
			if !ok {
				return nil, false
			}
			ids = append(ids, id)
		}
		return ids, true
	default:
		return nil, false
	}
}

type propagationSeriesKey struct{ agentID, topic string }
type propagationSample struct {
	key     propagationSeriesKey
	seconds float64
	nodeID  string
}

func (a *runMetricAccumulator) summarize(runID string, asOf ...time.Time) (model.Metrics, []propagationSample) {
	metrics, samples, _ := a.summarizeContext(context.Background(), runID, asOf...)
	return metrics, samples
}

// liveSummary shares expensive whole-run reconstruction between monitoring
// readers. Offline exports still call summarizeContext for exact, uncached data.
// A settled, unchanged run can reuse its result indefinitely; pending delivery
// deadlines must be reevaluated as wall time advances, even with no new events.
func (a *runMetricAccumulator) liveSummary(runID string, now time.Time) (model.Metrics, []propagationSample) {
	a.summaryMu.Lock()
	defer a.summaryMu.Unlock()
	return a.liveSummaryLocked(runID, now)
}

func (a *runMetricAccumulator) liveSummaryLocked(runID string, now time.Time) (model.Metrics, []propagationSample) {
	if !a.cachedAt.IsZero() && a.cachedRun == runID && !now.Before(a.cachedAt) &&
		a.cachedRevision == a.revision && (now.Sub(a.cachedAt) < snapshotInterval || a.cachedMetrics.PendingPublications == 0) {
		return a.cachedMetrics, a.cachedSamples
	}
	a.cachedMetrics, a.cachedSamples = a.summarize(runID, now)
	a.cachedHistograms = nil
	a.cachedRevision, a.cachedAt, a.cachedRun = a.revision, now, runID
	return a.cachedMetrics, a.cachedSamples
}

func (a *runMetricAccumulator) summarizeContext(ctx context.Context, runID string, asOf ...time.Time) (model.Metrics, []propagationSample, error) {
	if err := ctx.Err(); err != nil {
		return model.Metrics{}, nil, err
	}
	var err error
	var result model.Metrics
	var samples []propagationSample
	if a.window.enabled {
		boundary := time.Now().UTC()
		if len(asOf) > 0 {
			boundary = asOf[0]
		}
		result, samples, err = a.window.summarizeContext(ctx, runID, boundary, a.published, a.delivered, a.duplicates)
	} else {
		result, samples, err = a.summarizeLegacyContext(ctx, runID)
	}
	if err != nil {
		return model.Metrics{}, nil, err
	}
	result.GossipSubControl = a.summarizeGossipSubControl()
	result.Bandwidth = a.bandwidth.summarize()
	return result, samples, ctx.Err()
}

func gossipSubControlEvent(eventType string) (gossipSubControlKey, bool) {
	for _, direction := range []string{"send", "recv", "drop"} {
		for _, controlType := range []string{"ihave", "iwant", "idontwant", "graft", "prune"} {
			if eventType == direction+"_"+controlType {
				return gossipSubControlKey{direction: direction, controlType: controlType}, true
			}
		}
	}
	return gossipSubControlKey{}, false
}

func (a *runMetricAccumulator) observeGossipSubControl(event model.TraceEvent) {
	key, ok := gossipSubControlEvent(event.Type)
	if !ok {
		return
	}
	key.agentID = event.AgentID
	metric := a.control[key]
	if metric == nil {
		metric = &model.GossipSubControlMetric{AgentID: key.agentID, Direction: key.direction, ControlType: key.controlType}
		a.control[key] = metric
	}
	metric.RPCs++
	metric.Entries += nonnegativeEventCount(event.Fields, "controlEntries")
	metric.MessageIDs += nonnegativeEventCount(event.Fields, "messageIdCount")
	metric.PeerExchangeRecords += nonnegativeEventCount(event.Fields, "peerExchangeCount")
}

func nonnegativeEventCount(fields map[string]any, key string) int {
	number, ok := nonnegativeMetricCount(fields[key])
	if !ok || number > float64(1<<31-1) {
		return 0
	}
	return int(number)
}

func (a *runMetricAccumulator) summarizeGossipSubControl() []model.GossipSubControlMetric {
	if len(a.control) == 0 {
		return nil
	}
	result := make([]model.GossipSubControlMetric, 0, len(a.control))
	for _, metric := range a.control {
		result = append(result, *metric)
	}
	directionOrder := map[string]int{"send": 0, "recv": 1, "drop": 2}
	typeOrder := map[string]int{"ihave": 0, "iwant": 1, "idontwant": 2, "graft": 3, "prune": 4}
	sort.Slice(result, func(i, j int) bool {
		if result[i].AgentID != result[j].AgentID {
			return result[i].AgentID < result[j].AgentID
		}
		if directionOrder[result[i].Direction] != directionOrder[result[j].Direction] {
			return directionOrder[result[i].Direction] < directionOrder[result[j].Direction]
		}
		return typeOrder[result[i].ControlType] < typeOrder[result[j].ControlType]
	})
	return result
}

func (a *runMetricAccumulator) summarizeLegacy(runID string) (model.Metrics, []propagationSample) {
	metrics, samples, _ := a.summarizeLegacyContext(context.Background(), runID)
	return metrics, samples
}
func (a *runMetricAccumulator) summarizeLegacyContext(ctx context.Context, runID string) (model.Metrics, []propagationSample, error) {
	result := model.Metrics{Definition: "dispatch-cohort-v1", RunID: runID, Published: a.published, Delivered: a.delivered, Duplicates: a.duplicates}
	latencies := make([]float64, 0)
	samples := make([]propagationSample, 0)
	knownPublications := 0
	for key, message := range a.messages {
		if err := ctx.Err(); err != nil {
			return model.Metrics{}, nil, err
		}
		if !message.published || message.targets == nil {
			continue
		}
		knownPublications++
		result.ExpectedDeliveries += len(message.targets)
		for receiver := range message.targets {
			if err := ctx.Err(); err != nil {
				return model.Metrics{}, nil, err
			}
			delivery, exists := message.deliveries[receiver]
			if !exists || receiver == message.publisher {
				continue
			}
			result.EligibleDeliveries++
			result.EligibleDuplicates += message.duplicates[receiver]
			if delivery.latencyAvailable {
				latency := delivery.latencyMS
				if latency < 0 || math.IsNaN(latency) || math.IsInf(latency, 0) {
					result.InvalidLatencySamples++
					continue
				}
				latencies = append(latencies, latency)
				samples = append(samples, propagationSample{key: propagationSeriesKey{delivery.agentID, key.topic}, seconds: latency / 1000, nodeID: receiver})
			}
		}
	}
	result.UnscopedPublications = result.Published - knownPublications
	result.DeliveryRatioAvailable = result.ExpectedDeliveries > 0
	if result.DeliveryRatioAvailable {
		result.Reachability = float64(result.EligibleDeliveries) / float64(result.ExpectedDeliveries)
	}
	result.DuplicateSamples = result.EligibleDeliveries
	if result.DuplicateSamples > 0 {
		result.AverageDuplicates = float64(result.EligibleDuplicates) / float64(result.DuplicateSamples)
	}
	result.LatencySamples = len(latencies)
	if len(latencies) > 0 {
		sort.Float64s(latencies)
		// Samples are finite and nonnegative. Update the mean directly so
		// their sum cannot overflow an otherwise JSON-encodable result.
		for i, latency := range latencies {
			result.AverageLatencyMS += (latency - result.AverageLatencyMS) / float64(i+1)
		}
		result.P95LatencyMS = latencies[int(math.Ceil(0.95*float64(len(latencies))))-1]
	}
	return result, samples, ctx.Err()
}

// Rebuild the identical metrics from a pinned events.jsonl prefix in a result
// archive. No current topology, Controller uptime, or event arrival order is
// required. Legacy events without a cohort retain counts but have no ratio.
func summarizeRunEvents(runID string, reader io.Reader, asOf ...time.Time) (model.Metrics, error) {
	return summarizeRunEventsContext(context.Background(), runID, reader, asOf...)
}

func summarizeRunEventsContext(ctx context.Context, runID string, reader io.Reader, asOf ...time.Time) (model.Metrics, error) {
	if err := ctx.Err(); err != nil {
		return model.Metrics{}, err
	}
	a := newRunMetricAccumulator()
	scanner := bufio.NewScanner(resultContextReader{ctx: ctx, reader: reader})
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	line := 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return model.Metrics{}, err
		}
		line++
		var event model.TraceEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return model.Metrics{}, fmt.Errorf("decode event line %d: %w", line, err)
		}
		if event.RunID == runID {
			a.observe(event)
		}
	}
	if err := scanner.Err(); err != nil {
		return model.Metrics{}, fmt.Errorf("read event log: %w", err)
	}
	metrics, _, err := a.summarizeContext(ctx, runID, asOf...)
	return metrics, err
}

func calculateMetrics(_ []model.Node, experiments []model.Experiment, events []model.TraceEvent) model.Metrics {
	runID := ""
	if len(experiments) > 0 {
		runID = experiments[0].ID
	}
	a := newRunMetricAccumulator()
	for _, event := range events {
		if runID == "" || event.RunID == runID {
			a.observe(event)
		}
	}
	metrics, _ := a.summarize(runID)
	return metrics
}

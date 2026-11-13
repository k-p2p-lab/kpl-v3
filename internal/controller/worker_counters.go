package controller

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type workerCounterSeries struct {
	Family string   `json:"family"`
	Labels []string `json:"labels"`
	Value  float64  `json:"value"`
}

type eventCounterFamily struct {
	name   string
	vector *prometheus.CounterVec
	labels []string
}

func (m *controllerMetrics) counterFamilies() []eventCounterFamily {
	return []eventCounterFamily{
		{"events", m.events, []string{"run_id", "agent_id", "event_type", "topic"}},
		{"messageBytes", m.messageBytes, []string{"run_id", "agent_id", "topic", "direction", "encoding"}},
		{"controlRPCs", m.controlRPCs, []string{"run_id", "agent_id", "direction", "control_type"}},
		{"controlEntries", m.controlEntries, []string{"run_id", "agent_id", "direction", "control_type"}},
		{"controlMessageIDs", m.controlMessageIDs, []string{"run_id", "agent_id", "direction", "control_type"}},
		{"controlPXRecords", m.controlPXRecords, []string{"run_id", "agent_id", "direction", "control_type"}},
		{"operationFailures", m.operationFailures, []string{"run_id", "agent_id", "action"}},
		{"droppedEvents", m.droppedEvents, []string{"run_id", "agent_id"}},
	}
}

type controllerEventCollector struct{ metrics *controllerMetrics }

func newControllerEventCollector(m *controllerMetrics) *controllerEventCollector {
	m.counterDescriptions = make(map[string]*prometheus.Desc)
	m.counterVectors = make(map[string]*prometheus.CounterVec)
	for _, family := range m.counterFamilies() {
		ch := make(chan *prometheus.Desc, 1)
		family.vector.Describe(ch)
		m.counterDescriptions[family.name] = <-ch
		m.counterVectors[family.name] = family.vector
	}
	return &controllerEventCollector{metrics: m}
}
func (c *controllerEventCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, family := range c.metrics.counterFamilies() {
		ch <- c.metrics.counterDescriptions[family.name]
	}
}
func (c *controllerEventCollector) Collect(ch chan<- prometheus.Metric) {
	skip, err := c.collectCurrent(ch)
	if err == nil && c.metrics.state.journalOnly {
		err = c.metrics.state.walkWorkerSummaries(skip, func(summary runWorkerSummary) error {
			if summary.Retired {
				return c.emitCounters(ch, summary.Counters)
			}
			return nil
		})
	}
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.metrics.counterDescriptions["events"], err)
	}
}

func (c *controllerEventCollector) collectCurrent(ch chan<- prometheus.Metric) (map[string]bool, error) {
	m := c.metrics
	// Snapshot the active/cached label owners while serializing with sealing.
	// No filesystem access or historical decode holds the ingestion lock.
	m.lifecycleMu.Lock()
	skip := make(map[string]bool, len(m.counterRunIDs)+len(m.sealed))
	for id := range m.counterRunIDs {
		skip[id] = true
	}
	for _, family := range m.counterFamilies() {
		family.vector.Collect(ch)
	}
	cached := make([][]workerCounterSeries, 0, len(m.sealed))
	for id, rows := range m.sealed {
		skip[id] = true
		cached = append(cached, cloneWorkerCounters(rows))
	}
	m.lifecycleMu.Unlock()
	for _, rows := range cached {
		if err := c.emitCounters(ch, rows); err != nil {
			return skip, err
		}
	}
	return skip, nil
}

func (c *controllerEventCollector) emitCounters(ch chan<- prometheus.Metric, rows []workerCounterSeries) error {
	for _, row := range rows {
		desc := c.metrics.counterDescriptions[row.Family]
		if desc == nil || math.IsNaN(row.Value) || math.IsInf(row.Value, 0) || row.Value < 0 {
			return fmt.Errorf("invalid persisted event counter")
		}
		metric, err := prometheus.NewConstMetric(desc, prometheus.CounterValue, row.Value, row.Labels...)
		if err != nil {
			return err
		}
		ch <- metric
	}
	return nil
}

// Caller holds lifecycleMu. Closed Runs update compact scalar records rather
// than recreating CounterVec objects and Agent/topic initialization caches.
func (m *controllerMetrics) addEventCounter(family string, labels []string, value float64) {
	if len(labels) > 0 {
		if rows, closed := m.sealed[labels[0]]; closed {
			for i := range rows {
				if rows[i].Family == family && slices.Equal(rows[i].Labels, labels) {
					rows[i].Value += value
					return
				}
			}
			m.sealed[labels[0]] = append(rows, workerCounterSeries{Family: family, Labels: slices.Clone(labels), Value: value})
			return
		}
	}
	if len(labels) > 0 {
		m.counterRunIDs[labels[0]] = true
	}
	if vector := m.counterVectors[family]; vector != nil {
		vector.WithLabelValues(labels...).Add(value)
	}
}

func (m *controllerMetrics) sealRunCounters(id string) ([]workerCounterSeries, error) {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if existing, closed := m.sealed[id]; closed {
		return cloneWorkerCounters(existing), nil
	}
	var rows []workerCounterSeries
	for _, family := range m.counterFamilies() {
		ch := make(chan prometheus.Metric)
		go func() { family.vector.Collect(ch); close(ch) }()
		var firstErr error
		for metric := range ch {
			data := new(dto.Metric)
			if err := metric.Write(data); err != nil {
				firstErr = errors.Join(firstErr, err)
				continue
			}
			labels := make(map[string]string, len(data.Label))
			for _, label := range data.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["run_id"] != id {
				continue
			}
			row := workerCounterSeries{Family: family.name, Value: data.GetCounter().GetValue()}
			for _, name := range family.labels {
				row.Labels = append(row.Labels, labels[name])
			}
			rows = append(rows, row)
		}
		if firstErr != nil {
			return nil, firstErr
		}
	}
	for _, family := range m.counterFamilies() {
		family.vector.DeletePartialMatch(prometheus.Labels{"run_id": id})
	}
	m.initializedAgents.Range(func(key, _ any) bool {
		if key.([2]string)[0] == id {
			m.initializedAgents.Delete(key)
		}
		return true
	})
	m.initializedTopics.Range(func(key, _ any) bool {
		if key.([3]string)[0] == id {
			m.initializedTopics.Delete(key)
		}
		return true
	})
	if m.sealed == nil {
		m.sealed = make(map[string][]workerCounterSeries)
	}
	m.sealed[id] = rows
	delete(m.counterRunIDs, id)
	return cloneWorkerCounters(rows), nil
}

func cloneWorkerCounters(rows []workerCounterSeries) []workerCounterSeries {
	result := slices.Clone(rows)
	for i := range result {
		result[i].Labels = slices.Clone(result[i].Labels)
	}
	return result
}

// Source replay supplies complete positive totals. Keep the pre-traffic zero
// label baseline, replacing matching totals rather than adding a second copy.
func mergeWorkerCounterProjection(baseline, computed []workerCounterSeries) []workerCounterSeries {
	result := cloneWorkerCounters(computed)
	for _, old := range baseline {
		found := false
		for _, row := range computed {
			if row.Family == old.Family && slices.Equal(row.Labels, old.Labels) {
				found = true
				break
			}
		}
		if !found {
			old.Value = 0
			old.Labels = slices.Clone(old.Labels)
			result = append(result, old)
		}
	}
	return result
}

func (m *controllerMetrics) restoreSealedCounters(id string, rows []workerCounterSeries) {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.sealed == nil {
		m.sealed = make(map[string][]workerCounterSeries)
	}
	m.sealed[id] = cloneWorkerCounters(rows)
}
func (m *controllerMetrics) sealedRunCounters(id string) []workerCounterSeries {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	return cloneWorkerCounters(m.sealed[id])
}

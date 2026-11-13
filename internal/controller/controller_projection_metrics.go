package controller

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// These projections share one historical source. Collecting them together
// decodes one bounded summary at a time without a process-lifetime cache.
type controllerProjectionCollector struct {
	events *controllerEventCollector
	runs   *runMetricsCollector
	state  *controllerStateCollector
}

func newControllerProjectionCollector(m *controllerMetrics) *controllerProjectionCollector {
	return &controllerProjectionCollector{events: newControllerEventCollector(m), runs: newRunMetricsCollector(m.state), state: newControllerStateCollector(m.state)}
}

func (c *controllerProjectionCollector) Describe(ch chan<- *prometheus.Desc) {
	c.events.Describe(ch)
	c.runs.Describe(ch)
	c.state.Describe(ch)
}

func (c *controllerProjectionCollector) Collect(ch chan<- prometheus.Metric) {
	if !c.runs.state.journalOnly {
		c.events.Collect(ch)
		c.runs.Collect(ch)
		c.state.Collect(ch)
		return
	}
	skipCounters, counterErr := c.events.collectCurrent(ch)
	nodes, nodeErr := c.state.collectStateSnapshot(ch)
	err := c.runs.collectWorkerSummariesWith(ch, func(summary runWorkerSummary) error {
		if !summary.Retired {
			return nil
		}
		if counterErr == nil && !skipCounters[summary.RunID] {
			if err := c.events.emitCounters(ch, summary.Counters); err != nil {
				counterErr = err
			}
		}
		// A scrape that captured pre-retirement live Nodes already owns those
		// counts. The later durable summary contains the same terminal cohort.
		if nodeErr == nil && !nodes.hotRuns[summary.RunID] {
			nodeErr = c.runs.state.mergeRetiredNodeMetricCounts(nodes, summary)
		}
		return nil
	})
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.runs.expected, err)
		counterErr = errors.Join(counterErr, err)
		nodeErr = errors.Join(nodeErr, err)
	}
	if counterErr != nil {
		ch <- prometheus.NewInvalidMetric(c.events.metrics.counterDescriptions["events"], counterErr)
	}
	if nodeErr != nil {
		ch <- prometheus.NewInvalidMetric(c.state.nodes, nodeErr)
	} else {
		c.state.collectNodeCounts(ch, nodes.counts)
	}
}

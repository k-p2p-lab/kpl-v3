package controller

import (
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

type networkCollector struct {
	state                      *state
	peers, delay, jitter, loss *prometheus.Desc
}

func newNetworkCollector(s *state) prometheus.Collector {
	labels := []string{"run_id", "agent_id", "group", "profile", "scope"}
	delayLabels := append(append([]string(nil), labels...), "stat")
	return &networkCollector{
		state:  s,
		peers:  prometheus.NewDesc("kpl_network_configured_peers", "Starting or ready peers with a valid concrete network configuration; configured values are not observed network measurements.", labels, nil),
		delay:  prometheus.NewDesc("kpl_network_configured_delay_seconds", "Configured per-peer outbound base delay in seconds, aggregated by stat (mean/min/max); not observed RTT or message delivery latency.", delayLabels, nil),
		jitter: prometheus.NewDesc("kpl_network_configured_jitter_seconds", "Mean configured per-peer outbound jitter parameter in seconds; not observed jitter.", labels, nil),
		loss:   prometheus.NewDesc("kpl_network_configured_loss_ratio", "Mean configured per-peer outbound packet loss probability as a ratio from 0 to 1; not observed packet or message loss.", labels, nil),
	}
}

func (c *networkCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.peers
	ch <- c.delay
	ch <- c.jitter
	ch <- c.loss
}

type networkMetricKey struct{ runID, agentID, group, profile, scope string }

type networkMetricRecord struct {
	key        networkMetricKey
	configJSON string
}

type networkMetricAggregate struct {
	count                                            int
	delaySum, delayMin, delayMax, jitterSum, lossSum float64
}

func (c *networkCollector) Collect(ch chan<- prometheus.Metric) {
	// Copy only the immutable values needed for this scrape while holding the
	// state lock; JSON validation and aggregation happen after releasing it.
	c.state.mu.RLock()
	records := make([]networkMetricRecord, 0, len(c.state.nodes))
	for _, node := range c.state.nodes {
		if node.State != model.NodeReady && node.State != model.NodeStarting {
			continue
		}
		raw := node.Metadata["network"]
		if strings.TrimSpace(raw) == "" {
			continue
		}
		records = append(records, networkMetricRecord{key: networkMetricKey{runID: node.RunID, agentID: node.AgentID, group: node.Group, profile: node.Profile}, configJSON: raw})
	}
	c.state.mu.RUnlock()
	aggregates := make(map[networkMetricKey]*networkMetricAggregate)
	for _, record := range records {
		var config *model.NetworkConfig
		decoder := json.NewDecoder(strings.NewReader(record.configJSON))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&config); err != nil || config == nil || config.DelayDistribution != nil {
			continue
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			continue
		}
		if err := config.Validate(); err != nil {
			continue
		}
		record.key.scope = config.Scope
		if record.key.scope == "" {
			record.key.scope = "p2p"
		}
		delay, _ := time.ParseDuration(config.Delay)
		jitter, _ := time.ParseDuration(config.Jitter)
		delaySeconds := delay.Seconds()
		aggregate := aggregates[record.key]
		if aggregate == nil {
			aggregate = &networkMetricAggregate{delayMin: delaySeconds, delayMax: delaySeconds}
			aggregates[record.key] = aggregate
		}
		aggregate.count++
		aggregate.delaySum += delaySeconds
		aggregate.delayMin = min(aggregate.delayMin, delaySeconds)
		aggregate.delayMax = max(aggregate.delayMax, delaySeconds)
		aggregate.jitterSum += jitter.Seconds()
		if config.LossPercent != nil {
			aggregate.lossSum += *config.LossPercent / 100
		}
	}
	for key, aggregate := range aggregates {
		labels := []string{key.runID, key.agentID, key.group, key.profile, key.scope}
		count := float64(aggregate.count)
		ch <- prometheus.MustNewConstMetric(c.peers, prometheus.GaugeValue, count, labels...)
		for _, stat := range []struct {
			name  string
			value float64
		}{
			{"mean", aggregate.delaySum / count}, {"min", aggregate.delayMin}, {"max", aggregate.delayMax},
		} {
			ch <- prometheus.MustNewConstMetric(c.delay, prometheus.GaugeValue, stat.value, append(labels, stat.name)...)
		}
		ch <- prometheus.MustNewConstMetric(c.jitter, prometheus.GaugeValue, aggregate.jitterSum/count, labels...)
		ch <- prometheus.MustNewConstMetric(c.loss, prometheus.GaugeValue, aggregate.lossSum/count, labels...)
	}
}

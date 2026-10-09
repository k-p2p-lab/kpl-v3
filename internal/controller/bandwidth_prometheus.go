package controller

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type bandwidthCollector struct {
	state                                                         *state
	total, protocolTotal, rate, protocolRate, timestamp, sessions *prometheus.Desc
}

func newBandwidthCollector(s *state) *bandwidthCollector {
	labels := []string{"run_id", "agent_id"}
	direction := append(append([]string{}, labels...), "direction")
	protocols := append(append([]string{}, direction...), "protocol")
	return &bandwidthCollector{state: s,
		total:         prometheus.NewDesc("kpl_p2p_stream_bytes_total", "Cumulative libp2p stream bytes summed across process sessions per run/Agent; excludes transport overhead and HTTP management traffic.", direction, nil),
		protocolTotal: prometheus.NewDesc("kpl_p2p_protocol_stream_bytes_total", "Cumulative protocol stream bytes summed across process sessions per run/Agent (empty protocol during negotiation).", protocols, nil),
		rate:          prometheus.NewDesc("kpl_p2p_stream_bits_per_second", "Sum of fresh source interval-average stream rates per run/Agent; stale non-final sessions are excluded.", direction, nil),
		protocolRate:  prometheus.NewDesc("kpl_p2p_protocol_stream_bits_per_second", "Sum of fresh source interval-average protocol rates per run/Agent.", protocols, nil),
		timestamp:     prometheus.NewDesc("kpl_p2p_bandwidth_sample_timestamp_seconds", "Latest accepted source bandwidth timestamp across this run/Agent's sessions.", labels, nil),
		sessions:      prometheus.NewDesc("kpl_p2p_bandwidth_sessions", "Observed bandwidth sessions per run/Agent, with or without a normal post-close sample.", append(append([]string{}, labels...), "state"), nil),
	}
}
func (c *bandwidthCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.total, c.protocolTotal, c.rate, c.protocolRate, c.timestamp, c.sessions} {
		ch <- d
	}
}
func (c *bandwidthCollector) Collect(ch chan<- prometheus.Metric) {
	// Keep session detail in the persisted logs/analysis, not in TSDB labels:
	// churn would otherwise create new time series for every departed Peer.
	type key struct{ run, agent string }
	type values struct {
		total, rate [2]float64
		fresh       bool
	}
	type row struct {
		values
		at          time.Time
		final, open int
		protocols   map[string]*values
	}
	rows := make(map[key]*row)
	now := time.Now()
	for run, a := range c.state.metricAccumulators() {
		a.dataMu.RLock()
		for _, s := range a.bandwidth.sessions {
			k := key{run, s.agentID}
			r := rows[k]
			if r == nil {
				r = &row{protocols: make(map[string]*values)}
				rows[k] = r
			}
			if s.at.After(r.at) {
				r.at = s.at
			}
			if s.sample.Final {
				r.final++
			} else {
				r.open++
			}
			r.total[0] += float64(s.sample.ReceivedBytes)
			r.total[1] += float64(s.sample.SentBytes)
			factor, fresh := s.rateFactor(now)
			if fresh {
				r.fresh = true
				r.rate[0] += float64(s.interval.receivedBytes) * factor
				r.rate[1] += float64(s.interval.sentBytes) * factor
			}
			for _, p := range s.sample.Protocols {
				v := r.protocols[p.Protocol]
				if v == nil {
					v = &values{}
					r.protocols[p.Protocol] = v
				}
				v.total[0] += float64(p.ReceivedBytes)
				v.total[1] += float64(p.SentBytes)
			}
			if fresh {
				for _, p := range s.interval.protocols {
					v := r.protocols[p.Protocol]
					v.fresh = true
					v.rate[0] += float64(p.ReceivedBytes) * factor
					v.rate[1] += float64(p.SentBytes) * factor
				}
			}
		}
		a.dataMu.RUnlock()
	}
	for k, r := range rows {
		ch <- prometheus.MustNewConstMetric(c.timestamp, prometheus.GaugeValue, float64(r.at.Unix())+float64(r.at.Nanosecond())/1e9, k.run, k.agent)
		ch <- prometheus.MustNewConstMetric(c.sessions, prometheus.GaugeValue, float64(r.final), k.run, k.agent, "final")
		ch <- prometheus.MustNewConstMetric(c.sessions, prometheus.GaugeValue, float64(r.open), k.run, k.agent, "open")
		for i, direction := range []string{"receive", "send"} {
			ch <- prometheus.MustNewConstMetric(c.total, prometheus.CounterValue, r.total[i], k.run, k.agent, direction)
			if r.fresh {
				ch <- prometheus.MustNewConstMetric(c.rate, prometheus.GaugeValue, r.rate[i], k.run, k.agent, direction)
			}
			for protocol, v := range r.protocols {
				ch <- prometheus.MustNewConstMetric(c.protocolTotal, prometheus.CounterValue, v.total[i], k.run, k.agent, direction, protocol)
				if v.fresh {
					ch <- prometheus.MustNewConstMetric(c.protocolRate, prometheus.GaugeValue, v.rate[i], k.run, k.agent, direction, protocol)
				}
			}
		}
	}
}

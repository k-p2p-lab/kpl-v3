package controller

import (
	"math"
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type bandwidthSessionKey struct{ nodeID, sessionID string }
type bandwidthInterval struct {
	at                       time.Time
	durationNS               int64
	receivedBytes, sentBytes int64
	protocols                []model.BandwidthProtocol
}
type bandwidthSession struct {
	agentID  string
	at       time.Time
	sample   model.BandwidthSample
	interval bandwidthInterval
}
type bandwidthAccumulator struct {
	sessions                      map[bandwidthSessionKey]bandwidthSession
	receivedBytes, sentBytes      int64
	samples, rejected, superseded int
}

func validBandwidthSample(e model.TraceEvent) bool {
	s := e.Bandwidth
	if s == nil || e.NodeID == "" || e.SessionID == "" || e.Timestamp.IsZero() || s.ElapsedNS <= 0 || s.ReceivedBytes < 0 || s.SentBytes < 0 {
		return false
	}
	// Subtract to avoid overflow when validating untrusted counters. During a
	// read/write the global hook can precede attribution, so equality is not required.
	remainingIn, remainingOut := s.ReceivedBytes, s.SentBytes
	seen := make(map[string]bool, len(s.Protocols))
	for _, p := range s.Protocols {
		if seen[p.Protocol] || p.ReceivedBytes < 0 || p.SentBytes < 0 || p.ReceivedBytes > remainingIn || p.SentBytes > remainingOut {
			return false
		}
		seen[p.Protocol] = true
		remainingIn -= p.ReceivedBytes
		remainingOut -= p.SentBytes
	}
	return true
}

// Cumulative snapshots recover bytes across missing batches. An older snapshot
// cannot undo a newer one; the accepted interval then represents a longer average.
// A reset must have a new source session, never a negative throughput spike.
func (a *bandwidthAccumulator) observe(e model.TraceEvent) *bandwidthInterval {
	if e.Type != "bandwidth" {
		return nil
	}
	if !validBandwidthSample(e) {
		a.rejected++
		return nil
	}
	key := bandwidthSessionKey{e.NodeID, e.SessionID}
	prev, exists := a.sessions[key]
	s := *e.Bandwidth
	if exists && s.ElapsedNS <= prev.sample.ElapsedNS {
		a.superseded++
		return nil
	}
	if exists && (prev.sample.Final || s.ReceivedBytes < prev.sample.ReceivedBytes || s.SentBytes < prev.sample.SentBytes) {
		a.rejected++
		return nil
	}
	old := make(map[string]model.BandwidthProtocol, len(prev.sample.Protocols))
	for _, p := range prev.sample.Protocols {
		old[p.Protocol] = p
	}
	interval := bandwidthInterval{at: e.Timestamp, durationNS: s.ElapsedNS - prev.sample.ElapsedNS, receivedBytes: s.ReceivedBytes - prev.sample.ReceivedBytes, sentBytes: s.SentBytes - prev.sample.SentBytes}
	// Bound the whole-run representation before installing any session delta.
	if interval.receivedBytes > math.MaxInt64-a.receivedBytes || interval.sentBytes > math.MaxInt64-a.sentBytes {
		a.rejected++
		return nil
	}
	for _, p := range s.Protocols {
		before := old[p.Protocol]
		if p.ReceivedBytes < before.ReceivedBytes || p.SentBytes < before.SentBytes {
			a.rejected++
			return nil
		}
		interval.protocols = append(interval.protocols, model.BandwidthProtocol{Protocol: p.Protocol, ReceivedBytes: p.ReceivedBytes - before.ReceivedBytes, SentBytes: p.SentBytes - before.SentBytes})
		delete(old, p.Protocol)
	}
	if len(old) > 0 {
		a.rejected++
		return nil
	}
	s.Protocols = append([]model.BandwidthProtocol(nil), s.Protocols...)
	if a.sessions == nil {
		a.sessions = make(map[bandwidthSessionKey]bandwidthSession)
	}
	a.sessions[key] = bandwidthSession{agentID: e.AgentID, at: e.Timestamp, sample: s, interval: interval}
	a.receivedBytes += interval.receivedBytes
	a.sentBytes += interval.sentBytes
	a.samples++
	return &interval
}

const bandwidthSampleMaxAge = 15 * time.Second

// Share the same source interval and freshness rules with Prometheus.
func (s bandwidthSession) rateFactor(now time.Time) (float64, bool) {
	if s.sample.Final {
		return 0, true
	}
	age := now.Sub(s.at)
	if age < -bandwidthSampleMaxAge || age > bandwidthSampleMaxAge || s.interval.durationNS <= 0 {
		return 0, false
	}
	return 8e9 / float64(s.interval.durationNS), true
}

func (a *bandwidthAccumulator) currentRates(now time.Time) *model.BandwidthRates {
	out := &model.BandwidthRates{}
	for _, session := range a.sessions {
		factor, fresh := session.rateFactor(now)
		if !fresh {
			out.StaleSessions++
			continue
		}
		if !session.sample.Final {
			out.ReportingSessions++
		}
		out.SentBitsPerSecond += float64(session.interval.sentBytes) * factor
		out.ReceivedBitsPerSecond += float64(session.interval.receivedBytes) * factor
	}
	// All finalized sessions are a measured zero; entirely stale active
	// sessions have an unknown rate even if other sessions already finalized.
	out.Available = out.ReportingSessions > 0 || (len(a.sessions) > 0 && out.StaleSessions == 0)
	return out
}

func (a *bandwidthAccumulator) summarize() *model.BandwidthSummary {
	if a.samples == 0 && a.rejected == 0 {
		return nil
	}
	out := &model.BandwidthSummary{ReceivedBytes: a.receivedBytes, SentBytes: a.sentBytes, Scope: model.BandwidthScope, Sessions: len(a.sessions), Samples: a.samples, RejectedSamples: a.rejected, SupersededSamples: a.superseded, Protocols: []model.BandwidthProtocol{}}
	protocols := make(map[string]model.BandwidthProtocol)
	for _, session := range a.sessions {
		s := session.sample
		if s.Final {
			out.FinalizedSessions++
		}
		if session.at.After(out.LatestAt) {
			out.LatestAt = session.at
		}
		for _, p := range s.Protocols {
			total := protocols[p.Protocol]
			total.Protocol = p.Protocol
			total.ReceivedBytes += p.ReceivedBytes
			total.SentBytes += p.SentBytes
			protocols[p.Protocol] = total
		}
	}
	for _, p := range protocols {
		out.Protocols = append(out.Protocols, p)
	}
	sort.Slice(out.Protocols, func(i, j int) bool { return out.Protocols[i].Protocol < out.Protocols[j].Protocol })
	return out
}

// Bins integrate interval-average bytes/s over their overlapping time. The area
// under the throughput curve preserves all accepted bytes, including the first
// interval (implicit zero at host creation). Empty intervals remain unknown.
// Space and per-sample work stay bounded even after a very long reporting gap.
type bandwidthBinProtocol struct {
	Protocol      string  `json:"protocol"`
	ReceivedBytes float64 `json:"receivedBytes"`
	SentBytes     float64 `json:"sentBytes"`
}
type bandwidthBin struct {
	At            time.Time              `json:"at"`
	ReceivedBytes float64                `json:"receivedBytes"`
	SentBytes     float64                `json:"sentBytes"`
	PeerSeconds   float64                `json:"peerSeconds"`
	Protocols     []bandwidthBinProtocol `json:"protocols"`
}
type bandwidthBucket struct {
	received, sent, peerSeconds float64
	protocols                   map[string]bandwidthBinProtocol
}
type bandwidthTimeline struct {
	width       int64
	first, last int64
	bins        map[int64]*bandwidthBucket
}

func (t *bandwidthTimeline) add(interval bandwidthInterval) {
	if interval.durationNS <= 0 {
		return
	}
	// Absolute nanoseconds overflow outside 1678–2262 and when subtracting a
	// wide span. Bucket in Unix seconds, preserving subsecond overlap with Time.
	end := interval.at
	start := end.Add(-time.Duration(interval.durationNS))
	firstSecond, lastSecond := start.Unix(), end.Add(-time.Nanosecond).Unix()
	if t.bins == nil {
		t.width = 5
		t.first = firstSecond
		t.last = lastSecond
		t.bins = make(map[int64]*bandwidthBucket)
	}
	if firstSecond < t.first {
		t.first = firstSecond
	}
	if lastSecond > t.last {
		t.last = lastSecond
	}
	for (analysisBucket(t.last, t.width)-analysisBucket(t.first, t.width))/t.width >= 360 {
		t.width *= 2
		merged := make(map[int64]*bandwidthBucket)
		for at, b := range t.bins {
			key := analysisBucket(at, t.width)
			target := merged[key]
			if target == nil {
				target = &bandwidthBucket{protocols: make(map[string]bandwidthBinProtocol)}
				merged[key] = target
			}
			target.received += b.received
			target.sent += b.sent
			target.peerSeconds += b.peerSeconds
			for name, p := range b.protocols {
				q := target.protocols[name]
				q.Protocol = name
				q.ReceivedBytes += p.ReceivedBytes
				q.SentBytes += p.SentBytes
				target.protocols[name] = q
			}
		}
		t.bins = merged
	}
	for at := analysisBucket(firstSecond, t.width); at <= lastSecond; at += t.width {
		begin, finish := time.Unix(at, 0), time.Unix(at+t.width, 0)
		if start.After(begin) {
			begin = start
		}
		if end.Before(finish) {
			finish = end
		}
		overlap := finish.Sub(begin).Nanoseconds()
		fraction := float64(overlap) / float64(interval.durationNS)
		b := t.bins[at]
		if b == nil {
			b = &bandwidthBucket{protocols: make(map[string]bandwidthBinProtocol)}
			t.bins[at] = b
		}
		b.received += float64(interval.receivedBytes) * fraction
		b.sent += float64(interval.sentBytes) * fraction
		b.peerSeconds += float64(overlap) / 1e9
		for _, p := range interval.protocols {
			q := b.protocols[p.Protocol]
			q.Protocol = p.Protocol
			q.ReceivedBytes += float64(p.ReceivedBytes) * fraction
			q.SentBytes += float64(p.SentBytes) * fraction
			b.protocols[p.Protocol] = q
		}
	}
}
func (t *bandwidthTimeline) result() ([]bandwidthBin, int64) {
	out := []bandwidthBin{}
	for at, b := range t.bins {
		bin := bandwidthBin{At: time.Unix(at, 0).UTC(), ReceivedBytes: b.received, SentBytes: b.sent, PeerSeconds: b.peerSeconds, Protocols: []bandwidthBinProtocol{}}
		for _, p := range b.protocols {
			bin.Protocols = append(bin.Protocols, p)
		}
		sort.Slice(bin.Protocols, func(i, j int) bool { return bin.Protocols[i].Protocol < bin.Protocols[j].Protocol })
		out = append(out, bin)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out, t.width
}

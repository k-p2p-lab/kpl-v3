package peer

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// The upstream flow-meter reporter publishes counters on an asynchronous sweep.
// Keep synchronous integer counters so even sub-second runs and the last bytes
// before host.Close are included. Global and stream hooks are called separately:
// stream hooks update attribution only and must not double-count global bytes.
type bandwidthReporter struct {
	mu        sync.Mutex
	started   time.Time
	totals    metrics.Stats
	protocols map[protocol.ID]metrics.Stats
	peers     map[peer.ID]metrics.Stats
}

var _ metrics.Reporter = (*bandwidthReporter)(nil)

func newBandwidthReporter() *bandwidthReporter {
	return &bandwidthReporter{started: time.Now(), protocols: make(map[protocol.ID]metrics.Stats), peers: make(map[peer.ID]metrics.Stats)}
}
func (b *bandwidthReporter) LogSentMessage(n int64) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.totals.TotalOut += n
}
func (b *bandwidthReporter) LogRecvMessage(n int64) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.totals.TotalIn += n
}
func (b *bandwidthReporter) logStream(n int64, p protocol.ID, remote peer.ID, sent bool) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	ps, rs := b.protocols[p], b.peers[remote]
	if sent {
		ps.TotalOut += n
		rs.TotalOut += n
	} else {
		ps.TotalIn += n
		rs.TotalIn += n
	}
	b.protocols[p], b.peers[remote] = ps, rs
}
func (b *bandwidthReporter) LogSentMessageStream(n int64, p protocol.ID, remote peer.ID) {
	b.logStream(n, p, remote, true)
}
func (b *bandwidthReporter) LogRecvMessageStream(n int64, p protocol.ID, remote peer.ID) {
	b.logStream(n, p, remote, false)
}

// Reporter getters expose lifetime mean bytes/s. Chart rates are calculated
// separately from successive samples, using monotonic elapsed time.
func (b *bandwidthReporter) withRate(s metrics.Stats) metrics.Stats {
	seconds := time.Since(b.started).Seconds()
	if seconds > 0 {
		s.RateIn = float64(s.TotalIn) / seconds
		s.RateOut = float64(s.TotalOut) / seconds
	}
	return s
}
func (b *bandwidthReporter) GetBandwidthTotals() metrics.Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.withRate(b.totals)
}
func (b *bandwidthReporter) GetBandwidthForPeer(p peer.ID) metrics.Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.withRate(b.peers[p])
}
func (b *bandwidthReporter) GetBandwidthForProtocol(p protocol.ID) metrics.Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.withRate(b.protocols[p])
}
func (b *bandwidthReporter) GetBandwidthByPeer() map[peer.ID]metrics.Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[peer.ID]metrics.Stats, len(b.peers))
	for p, s := range b.peers {
		out[p] = b.withRate(s)
	}
	return out
}
func (b *bandwidthReporter) GetBandwidthByProtocol() map[protocol.ID]metrics.Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[protocol.ID]metrics.Stats, len(b.protocols))
	for p, s := range b.protocols {
		out[p] = b.withRate(s)
	}
	return out
}
func (b *bandwidthReporter) snapshot(final bool) *model.BandwidthSample {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := &model.BandwidthSample{ElapsedNS: time.Since(b.started).Nanoseconds(), ReceivedBytes: b.totals.TotalIn, SentBytes: b.totals.TotalOut, Final: final, Protocols: []model.BandwidthProtocol{}}
	for p, v := range b.protocols {
		s.Protocols = append(s.Protocols, model.BandwidthProtocol{Protocol: string(p), ReceivedBytes: v.TotalIn, SentBytes: v.TotalOut})
	}
	sort.Slice(s.Protocols, func(i, j int) bool { return s.Protocols[i].Protocol < s.Protocols[j].Protocol })
	return s
}
func (s *Server) emitBandwidth(final bool) {
	if s.bandwidth == nil {
		return
	}
	s.telemetry.emitObservedPriority(func(reading controllerClockReading) (model.TraceEvent, bool) {
		return model.TraceEvent{Type: "bandwidth", Timestamp: reading.timestamp, PeerID: s.host.ID().String(), Bandwidth: s.bandwidth.snapshot(final)}, true
	}, final)
}
func (s *Server) bandwidthLoop(ctx context.Context) {
	if s.bandwidth == nil {
		return
	}
	s.emitBandwidth(false)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.emitBandwidth(false)
		}
	}
}

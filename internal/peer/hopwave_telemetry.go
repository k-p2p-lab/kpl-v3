package peer

import (
	"encoding/hex"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// Bound both the in-memory history and the size of the existing checkpoint event.
const (
	hopwaveRecordLimit     = 64
	hopwaveIdentifierLimit = 1024 // bytes before JSON or hex encoding
)

type hopwaveAdaptiveSnapshot struct {
	TroughDecisions      uint64                    `json:"troughDecisions"`
	Adjustments          uint64                    `json:"adjustments"`
	PayloadAdjustments   uint64                    `json:"payloadAdjustments"`
	RetentionAdjustments uint64                    `json:"retentionAdjustments"`
	OmittedRecords       uint64                    `json:"omittedRecords"`
	Records              []hopwaveAdjustmentRecord `json:"records,omitempty"`
}

type hopwaveAdjustmentRecord struct {
	Adjustment       uint64    `json:"adjustment"`
	At               time.Time `json:"at"`
	Topic            string    `json:"topic"`
	PubsubMessageID  string    `json:"pubsubMessageId"`
	HopCount         int32     `json:"hopCount"`
	ConfiguredFactor float64   `json:"configuredFactor"`
	BaseFactor       float64   `json:"baseFactor"`
	EffectiveFactor  float64   `json:"effectiveFactor"`
	RangeMatched     bool      `json:"rangeMatched"`
	PayloadBytes     *int      `json:"payloadBytes,omitempty"`
	Retention        *float64  `json:"retention,omitempty"`
	EligiblePeers    int       `json:"eligiblePeers"`
	SelectedPeers    int       `json:"selectedPeers"`
}

func (t *telemetry) enableAdaptiveHopwave() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.hopwaveAdaptive == nil {
		t.hopwaveAdaptive = &hopwaveAdaptiveSnapshot{}
	}
}

// observeAdaptiveHopwave runs after recipient selection on the router event loop.
// It performs no I/O and enqueues no events. Checkpoint admission and the
// measurement boundaries use the same lock so each decision belongs to one interval.
func (t *telemetry) observeAdaptiveHopwave(d pubsub.HopwaveDecision) {
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := t.hopwaveAdaptive
	if stats == nil || !t.measurementStarted || t.measurementStopped || t.sealed {
		return
	}
	stats.TroughDecisions++
	payloadChanged := d.BaseFactor != d.ConfiguredFactor
	retentionChanged := d.EffectiveFactor != d.BaseFactor
	if !payloadChanged && !retentionChanged {
		return
	}
	stats.Adjustments++
	if payloadChanged {
		stats.PayloadAdjustments++
	}
	if retentionChanged {
		stats.RetentionAdjustments++
	}
	if len(stats.Records) >= hopwaveRecordLimit || len(d.Topic) > hopwaveIdentifierLimit || len(d.MessageID) > hopwaveIdentifierLimit {
		stats.OmittedRecords++
		return
	}
	record := hopwaveAdjustmentRecord{
		Adjustment:       stats.Adjustments,
		At:               t.now(),
		Topic:            d.Topic,
		PubsubMessageID:  hex.EncodeToString([]byte(d.MessageID)),
		HopCount:         d.HopCount,
		ConfiguredFactor: d.ConfiguredFactor,
		BaseFactor:       d.BaseFactor,
		EffectiveFactor:  d.EffectiveFactor,
		RangeMatched:     d.RangeMatched,
		EligiblePeers:    d.EligiblePeers,
		SelectedPeers:    d.SelectedPeers,
	}
	if d.PayloadBytes >= 0 {
		size := d.PayloadBytes
		record.PayloadBytes = &size
	}
	if d.RetentionValid {
		retention := d.Retention
		record.Retention = &retention
	}
	stats.Records = append(stats.Records, record)
}

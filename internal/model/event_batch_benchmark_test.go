package model

import (
	"strings"
	"testing"
)

func BenchmarkValidateTelemetryBatch(b *testing.B) {
	batch := EventBatch{AgentID: "agent", Events: make([]TraceEvent, 250)}
	for i := range batch.Events {
		batch.Events[i] = TraceEvent{Type: "recv_ihave", NodeID: "peer", Fields: map[string]any{"messageIds": strings.Repeat("m", 4096)}}
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := batch.ValidateEventSizes(); err != nil {
			b.Fatal(err)
		}
	}
}

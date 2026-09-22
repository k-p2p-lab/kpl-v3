package model

import (
	"encoding/json"
	"fmt"
)

const (
	MaxEventBatchBytes  = 10 << 20
	MaxEventBatchEvents = 5000
)

// MarshalEventBatchPrefix encodes the longest source-ordered prefix that fits
// both receiver limits, including JSON escaping and the batch envelope. The
// caller must retain any suffix and only remove this prefix after acceptance.
func MarshalEventBatchPrefix(agentID string, events []TraceEvent) ([]byte, int, error) {
	agent, err := json.Marshal(agentID)
	if err != nil {
		return nil, 0, err
	}
	data := append([]byte(`{"agentId":`), agent...)
	data = append(data, `,"events":[`...)
	if len(data)+2 > MaxEventBatchBytes {
		return nil, 0, fmt.Errorf("event batch envelope exceeds %d bytes", MaxEventBatchBytes)
	}
	count := 0
	for _, event := range events[:min(len(events), MaxEventBatchEvents)] {
		encoded, err := json.Marshal(event)
		if err != nil {
			if count > 0 {
				break
			}
			return nil, 0, fmt.Errorf("encode telemetry event: %w", err)
		}
		separator := 0
		if count > 0 {
			separator = 1
		}
		if len(data)+separator+len(encoded)+2 > MaxEventBatchBytes {
			if count > 0 {
				break
			}
			return nil, 0, fmt.Errorf("encoded telemetry event exceeds %d-byte batch limit", MaxEventBatchBytes)
		}
		if count > 0 {
			data = append(data, ',')
		}
		data = append(data, encoded...)
		count++
	}
	return append(data, ']', '}'), count, nil
}

// ValidateEventSizes checks every event before admitting any part of a batch.
// A body below the input limit can still expand past it when JSON is re-encoded.
func (batch EventBatch) ValidateEventSizes() error {
	if len(batch.Events) == 0 {
		return nil
	}
	agent, err := json.Marshal(batch.AgentID)
	if err != nil {
		return err
	}
	overhead := len(`{"agentId":`) + len(agent) + len(`,"events":[]}`)
	if overhead > MaxEventBatchBytes {
		return fmt.Errorf("event batch envelope exceeds %d bytes", MaxEventBatchBytes)
	}
	// Validate each event against a single-event envelope. Encoder reuses its
	// scratch space and the writer discards bytes: validation must not allocate
	// a second full wire batch on both the Agent and Controller ingest paths.
	encoder := json.NewEncoder(eventSizeLimit(MaxEventBatchBytes - overhead))
	for i := range batch.Events {
		if err := encoder.Encode(&batch.Events[i]); err != nil {
			return fmt.Errorf("encode telemetry event: %w", err)
		}
	}
	return nil
}

type eventSizeLimit int

func (limit eventSizeLimit) Write(data []byte) (int, error) {
	// Encoder appends one newline that is not part of an event in a batch.
	if len(data)-1 > int(limit) {
		return 0, fmt.Errorf("encoded telemetry event exceeds %d-byte batch limit", MaxEventBatchBytes)
	}
	return len(data), nil
}

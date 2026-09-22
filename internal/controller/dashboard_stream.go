package controller

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
)

const dashboardEventLimit = 40

// Dashboard frames never carry raw trace metadata (message-ID lists, cohorts,
// bandwidth samples, etc.). Full records remain in REST snapshots and exports.
func dashboardSnapshot(snapshot model.Snapshot) model.Snapshot {
	nodes := make([]model.Node, 0, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		if node.State != model.NodeStopping && node.State != model.NodeStopped && node.State != model.NodeFailed {
			nodes = append(nodes, node)
		}
	}
	snapshot.Nodes = nodes
	events := snapshot.Events
	if len(events) > dashboardEventLimit {
		events = events[len(events)-dashboardEventLimit:]
	}
	snapshot.Events = make([]model.TraceEvent, 0, len(events))
	for _, event := range events {
		summary := model.TraceEvent{
			RunID: event.RunID, NodeID: event.NodeID, Type: event.Type,
			RemotePeerID: event.RemotePeerID, Timestamp: event.Timestamp, LatencyMS: event.LatencyMS,
		}
		for _, key := range []string{"latencyAvailable", "direction", "controlType", "controlEntries", "messageIdCount", "peerExchangeCount"} {
			value, ok := event.Fields[key]
			if !ok {
				continue
			}
			// Keep only bounded scalar display fields, even for malformed input.
			switch v := value.(type) {
			case bool, int, int64, uint64, float64:
			case string:
				if len(v) > 96 {
					continue
				}
			default:
				continue
			}
			if summary.Fields == nil {
				summary.Fields = make(map[string]any)
			}
			summary.Fields[key] = value
		}
		snapshot.Events = append(snapshot.Events, summary)
	}
	return snapshot
}

type dashboardEntities struct {
	items map[string]json.RawMessage
	order []string
}

type dashboardFrame struct {
	at          time.Time
	data        []byte
	fields      map[string]json.RawMessage
	collections map[string]dashboardEntities
}

func newDashboardFrame(snapshot model.Snapshot, at time.Time) (*dashboardFrame, error) {
	data, err := json.Marshal(dashboardSnapshot(snapshot))
	if err != nil {
		return nil, err
	}
	frame := &dashboardFrame{at: at, data: data, collections: make(map[string]dashboardEntities)}
	if err := json.Unmarshal(data, &frame.fields); err != nil {
		return nil, err
	}
	for _, key := range []string{"agents", "nodes", "experiments"} {
		var rows []json.RawMessage
		if err := json.Unmarshal(frame.fields[key], &rows); err != nil {
			return nil, err
		}
		collection := dashboardEntities{items: make(map[string]json.RawMessage, len(rows)), order: make([]string, 0, len(rows))}
		for _, row := range rows {
			var identity struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(row, &identity); err != nil {
				return nil, err
			}
			collection.items[identity.ID] = row
			collection.order = append(collection.order, identity.ID)
		}
		frame.collections[key] = collection
	}
	return frame, nil
}

func (s *Server) dashboardStreamSnapshot() (*dashboardFrame, error) {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	if s.dashboardFrame != nil && time.Since(s.dashboardFrame.at) < snapshotInterval {
		return s.dashboardFrame, nil
	}
	// This is the start of the read, so a concurrent notification stays pending.
	generatedAt := time.Now()
	frame, err := newDashboardFrame(s.state.dashboardSnapshot(), generatedAt)
	if err == nil {
		s.dashboardFrame = frame
	}
	return frame, err
}

// Each connection diffs against its own last frame, so skipped/coalesced frames
// need no replay log. Reconnects always receive a complete dashboard snapshot.
func (frame *dashboardFrame) delta(previous *dashboardFrame) ([]byte, error) {
	patch := make(map[string]any)
	for _, key := range []string{"agents", "nodes", "experiments"} {
		if bytes.Equal(frame.fields[key], previous.fields[key]) {
			continue
		}
		current, old := frame.collections[key], previous.collections[key]
		changes := make(map[string]any)
		var upsert []json.RawMessage
		var removed []string
		for _, id := range current.order {
			if !bytes.Equal(current.items[id], old.items[id]) {
				upsert = append(upsert, current.items[id])
			}
		}
		for _, id := range old.order {
			if _, ok := current.items[id]; !ok {
				removed = append(removed, id)
			}
		}
		if len(upsert) > 0 {
			changes["upsert"] = upsert
		}
		if len(removed) > 0 {
			changes["remove"] = removed
		}
		sameOrder := len(current.order) == len(old.order)
		if sameOrder {
			for i, id := range current.order {
				if id != old.order[i] {
					sameOrder = false
					break
				}
			}
		}
		if !sameOrder {
			changes["order"] = current.order
		}
		if len(changes) > 0 {
			patch[key] = changes
		}
	}
	for _, key := range []string{"edges", "events", "metrics"} {
		if !bytes.Equal(frame.fields[key], previous.fields[key]) {
			patch[key] = frame.fields[key]
		}
	}
	if len(patch) == 0 {
		return nil, nil
	}
	patch["generatedAt"] = frame.fields["generatedAt"]
	return json.Marshal(patch)
}

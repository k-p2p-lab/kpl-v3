package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// The memo is scoped to one read/scrape, never to the process lifetime.
func (s *state) historyVisibility() func(string) (bool, error) {
	known, deleted := make(map[string]bool), make(map[string]bool)
	return func(id string) (bool, error) {
		if !validResultID(id) {
			return true, nil
		}
		if !known[id] {
			value, err := s.resultDeletedLocked(id)
			if err != nil {
				return false, err
			}
			known[id], deleted[id] = true, value
		}
		return !deleted[id], nil
	}
}

func (s *state) historicalMetricCounts(hotIDs map[string]bool, visit func(nodeMetricLabels, int) error) error {
	_, err := s.historicalMetricCountsSnapshot(hotIDs, visit)
	return err
}

func (s *state) historicalMetricCountsSnapshot(hotIDs map[string]bool, visit func(nodeMetricLabels, int) error) (terminalHistoryMetricRevision, error) {
	if s.terminalHistory == nil {
		return terminalHistoryMetricRevision{}, nil
	}
	visible := s.historyVisibility()
	return s.terminalHistory.countsSnapshot(hotIDs, func(labels nodeMetricLabels, count int) error {
		ok, err := visible(labels.runID)
		if err != nil || !ok {
			return err
		}
		return visit(labels, count)
	})
}

func (s *state) historicalPropagation(visit func(string, string, string) error) error {
	if s.terminalHistory == nil {
		return nil
	}
	visible := s.historyVisibility()
	return s.terminalHistory.propagation(func(runID, agentID, topic string) error {
		ok, err := visible(runID)
		if err != nil || !ok {
			return err
		}
		return visit(runID, agentID, topic)
	})
}

// An explicit inventory can contain arbitrarily many historical Peers. Keep
// one bounded disk page while streaming it, and release every transaction and
// file descriptor before writing to a possibly slow HTTP client.
func (s *state) walkHistoricalInventory(ctx context.Context, hot []model.Node, asOf time.Time, visit func(model.Node) error) error {
	sort.Slice(hot, func(i, j int) bool { return hot[i].ID < hot[j].ID })
	if s.terminalHistory == nil {
		for _, node := range hot {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(node); err != nil {
				return err
			}
		}
		return nil
	}
	visible := s.historyVisibility()
	after, index, done := "", 0, false
	for !done {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, last, end, err := s.terminalHistory.page(ctx, after, asOf, 128)
		if err != nil {
			return err
		}
		if !end && last <= after {
			return errors.New("historical Peer inventory did not advance")
		}
		after, done = last, end
		for _, node := range page {
			for index < len(hot) && hot[index].ID < node.ID {
				if err := visit(hot[index]); err != nil {
					return err
				}
				index++
			}
			if index < len(hot) && hot[index].ID == node.ID {
				// The current control projection wins an overlapping disk record.
				continue
			}
			ok, err := visible(node.RunID)
			if err != nil {
				return err
			}
			if ok {
				if err := visit(node); err != nil {
					return err
				}
			}
		}
	}
	for ; index < len(hot); index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(hot[index]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) writeHistoricalInventory(w http.ResponseWriter, r *http.Request, nodes []model.Node, asOf time.Time, metadata any) {
	prefix, suffix := "[", "]\n"
	if metadata != nil {
		encoded, err := json.Marshal(metadata)
		if err != nil || len(encoded) == 0 || encoded[len(encoded)-1] != '}' {
			writeError(w, http.StatusInternalServerError, "cannot encode inventory metadata")
			return
		}
		prefix, suffix = string(encoded[:len(encoded)-1])+",\"nodes\":[", "]}\n"
	}
	started := false
	first := true
	encoder := json.NewEncoder(w)
	err := s.state.walkHistoricalInventory(r.Context(), nodes, asOf, func(node model.Node) error {
		if !started {
			w.Header().Set("Content-Type", "application/json")
			if _, err := io.WriteString(w, prefix); err != nil {
				return err
			}
			started = true
		}
		if !first {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		first = false
		return encoder.Encode(node)
	})
	if err != nil {
		if !started {
			writeError(w, http.StatusInternalServerError, "cannot read historical Peer inventory")
			return
		}
		if r.Context().Err() == nil {
			s.logger.Warn("stream historical Peer inventory", "error", err)
		}
		// A truncated inventory must never look like a complete successful list.
		panic(http.ErrAbortHandler)
	}
	if !started {
		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, prefix); err != nil {
			return
		}
	}
	_, _ = io.WriteString(w, suffix)
}

func snapshotWithoutHistoricalNodes(snapshot model.Snapshot) any {
	type snapshotFields model.Snapshot
	return struct {
		snapshotFields
		Nodes json.RawMessage `json:"nodes,omitempty"`
	}{snapshotFields: snapshotFields(snapshot)}
}

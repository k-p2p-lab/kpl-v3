package controller

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"
	"unicode/utf8"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	bolt "go.etcd.io/bbolt"
)

type terminalHistoryMetricRevision struct {
	transaction int
	info        os.FileInfo
}

// The projection needs identity and labels, never historical topology/score
// fields. Large legacy Node metadata is skipped rather than retained per page.
type terminalMetricNode struct {
	ID       string `json:"id"`
	RunID    string `json:"runId"`
	AgentID  string `json:"agentId"`
	Group    string `json:"group"`
	Role     string `json:"role"`
	Type     string `json:"type"`
	State    string `json:"state"`
	Metadata struct {
		CleanupComplete string `json:"cleanupComplete"`
	} `json:"metadata"`
}

func (node terminalMetricNode) labels() nodeMetricLabels {
	return nodeMetricLabels{node.RunID, node.AgentID, node.Group, node.Role, node.Type, node.State}
}

func (node terminalMetricNode) valid() bool {
	return node.ID != "" && len(node.ID) <= bolt.MaxKeySize && utf8.ValidString(node.ID) && node.AgentID != "" && utf8.ValidString(node.AgentID) && utf8.ValidString(node.RunID) &&
		(node.State == model.NodeStopped || node.State == model.NodeFailed) && node.Metadata.CleanupComplete == "true"
}

func (node terminalMetricNode) bytes() int {
	return len(node.ID) + len(node.RunID) + len(node.AgentID) + len(node.Group) + len(node.Role) + len(node.Type) + len(node.State)
}

// Each bounded lookup closes its database before the next filesystem read or
// metric send. A different DB/transaction cannot be subtracted from an earlier
// aggregate; fail the collection rather than emitting an invented Node count.
func (h *terminalNodeHistory) metricOverlap(revision terminalHistoryMetricRevision, nodes []terminalMetricNode, visit func(terminalMetricNode, nodeMetricLabels) error) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	root, db, info, err := h.openWithIdentity(true)
	if err != nil {
		return err
	}
	if db == nil {
		return errors.New("terminal history changed during metric collection")
	}
	err = db.View(func(tx *bolt.Tx) error {
		if revision.info == nil || tx.ID() != revision.transaction || !os.SameFile(revision.info, info) || sourceFileIdentity(revision.info.Sys()) != sourceFileIdentity(info.Sys()) {
			return errors.New("terminal history changed during metric collection")
		}
		stored, _, _, err := terminalHistoryBuckets(tx, false)
		if err != nil || stored == nil {
			return errors.Join(err, errors.New("terminal history metric evidence is unavailable"))
		}
		for _, node := range nodes {
			encoded := stored.Get([]byte(node.ID))
			if encoded == nil {
				continue
			}
			if len(encoded) > terminalHistoryRecordLimit {
				return errors.New("terminal history record exceeds its size limit")
			}
			var record struct {
				Version    int                `json:"version"`
				Node       terminalMetricNode `json:"node"`
				RecordedAt time.Time          `json:"recordedAt"`
			}
			if json.Unmarshal(encoded, &record) != nil || record.Version != 1 || record.RecordedAt.IsZero() || !record.Node.valid() || record.Node.ID != node.ID || record.Node.RunID != node.RunID || record.Node.AgentID != node.AgentID {
				return errors.New("invalid terminal history metric identity")
			}
			if err := visit(node, record.Node.labels()); err != nil {
				return err
			}
		}
		return nil
	})
	return errors.Join(err, db.Close(), root.Close())
}

func (s *state) mergeRetiredNodeMetricCounts(snapshot *controllerNodeMetricSnapshot, summary runWorkerSummary) error {
	rows := make(map[nodeMetricLabels]int, len(summary.Nodes))
	for _, row := range summary.Nodes {
		labels := nodeMetricLabels{summary.RunID, row.AgentID, row.Group, row.Role, row.Type, row.State}
		if row.Count < 0 || rows[labels] > math.MaxInt-row.Count {
			return errors.New("invalid persisted Node count")
		}
		rows[labels] += row.Count
	}
	if len(rows) > 0 && snapshot.historyRuns[summary.RunID] {
		if err := s.subtractRetiredMetricOverlap(snapshot, summary.RunID, rows); err != nil {
			return err
		}
	}
	for labels, count := range rows {
		if snapshot.counts[labels] > math.MaxInt-count {
			return errors.New("persisted Node count exceeds integer range")
		}
		snapshot.counts[labels] += count
	}
	return nil
}

// Only mixed legacy-history/summary Runs need the existing retirement identity
// evidence. Stream this potentially large file; keep at most one Node decode
// and a 128-identity / 4 MiB page, rather than loading all retired records.
func (s *state) subtractRetiredMetricOverlap(snapshot *controllerNodeMetricSnapshot, id string, expected map[nodeMetricLabels]int) error {
	root, err := os.OpenRoot(filepath.Join(s.dataDir, retiredRuntimeDirectory))
	if err != nil {
		return fmt.Errorf("read retired Node metric identity: %w", err)
	}
	defer root.Close()
	file, err := openResultFile(root, id+".json")
	if err != nil {
		return fmt.Errorf("read retired Node metric identity: %w", err)
	}
	defer file.close()
	remaining := make(map[nodeMetricLabels]int, len(expected))
	for labels, count := range expected {
		remaining[labels] = count
	}
	page := make([]terminalMetricNode, 0, 128)
	pageBytes := 0
	// Digest membership is scoped to this one Run, proportional to its Nodes.
	// It rejects duplicate proof identities across pages without retaining
	// their potentially large original IDs or decoded Node records.
	seenNodes := make(map[[sha256.Size]byte]bool)
	overlapCounts := make(map[nodeMetricLabels]int)
	flush := func() error {
		if len(page) == 0 {
			return nil
		}
		err := s.terminalHistory.metricOverlap(snapshot.historyRevision, page, func(node terminalMetricNode, labels nodeMetricLabels) error {
			if snapshot.counts[labels] <= overlapCounts[labels] || expected[node.labels()] < 1 {
				return errors.New("retired Node metric identity does not match history aggregate")
			}
			overlapCounts[labels]++
			// Cold history freezes the original terminal evidence. Keep its
			// labels if retirement captured another status before that prepared
			// heartbeat was applied; omit that identity from the summary cohort.
			expected[node.labels()]--
			return nil
		})
		clear(page)
		page, pageBytes = page[:0], 0
		return err
	}
	reader := &terminalMetricProofReader{source: file.reader()}
	decoder := json.NewDecoder(reader)
	if token, err := terminalMetricProofToken(decoder, reader); err != nil || token != json.Delim('{') {
		return errors.New("invalid retired Node metric evidence")
	}
	seen := make(map[string]bool, 3)
	runID := ""
	for terminalMetricProofMore(decoder, reader) {
		token, err := terminalMetricProofToken(decoder, reader)
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return errors.New("invalid retired Node metric evidence fields")
		}
		seen[key] = true
		switch key {
		case "runId":
			if err := decodeTerminalMetricProofValue(decoder, reader, &runID); err != nil {
				return err
			}
		case "at":
			var at time.Time
			if err := decodeTerminalMetricProofValue(decoder, reader, &at); err != nil || at.IsZero() {
				return errors.New("invalid retired Node metric evidence time")
			}
		case "nodes":
			if token, err := terminalMetricProofToken(decoder, reader); err != nil || token != json.Delim('[') {
				return errors.New("invalid retired Node metric evidence cohort")
			}
			for terminalMetricProofMore(decoder, reader) {
				var node terminalMetricNode
				if err := decodeTerminalMetricProofValue(decoder, reader, &node); err != nil {
					return err
				}
				labels := node.labels()
				key := sha256.Sum256([]byte(node.ID))
				if !node.valid() || node.RunID != id || remaining[labels] < 1 || seenNodes[key] {
					return errors.New("retired Node metric identity does not match summary")
				}
				seenNodes[key] = true
				remaining[labels]--
				if len(page) > 0 && (len(page) == cap(page) || pageBytes+node.bytes() > terminalHistoryPageBytes) {
					if err := flush(); err != nil {
						return err
					}
				}
				page, pageBytes = append(page, node), pageBytes+node.bytes()
			}
			if token, err := terminalMetricProofToken(decoder, reader); err != nil || token != json.Delim(']') {
				return errors.New("invalid retired Node metric evidence cohort end")
			}
		default:
			return errors.New("unknown retired Node metric evidence field")
		}
	}
	if token, err := terminalMetricProofToken(decoder, reader); err != nil || token != json.Delim('}') || runID != id || !seen["nodes"] || !seen["at"] {
		return errors.New("invalid retired Node metric evidence identity")
	}
	if _, err := terminalMetricProofToken(decoder, reader); !errors.Is(err, io.EOF) {
		return errors.New("trailing retired Node metric evidence")
	}
	for _, count := range remaining {
		if count != 0 {
			return errors.New("retired Node metric cohort is incomplete")
		}
	}
	info, err := file.file.Stat()
	if err != nil || file.info == nil || info.Size() != file.size || !info.ModTime().Equal(file.info.ModTime()) || sourceFileIdentity(info.Sys()) != sourceFileIdentity(file.info.Sys()) {
		return errors.Join(err, errors.New("retired Node metric identity changed during collection"))
	}
	return flush()
}

// Bound decoder read-ahead for each JSON value, including ignored legacy
// fields. There is no whole-file cap: a large valid Run streams many values.
type terminalMetricProofReader struct {
	source    io.Reader
	read      int64
	remaining int64
	limited   bool
}

func (reader *terminalMetricProofReader) Read(buffer []byte) (int, error) {
	if reader.limited {
		if reader.remaining <= 0 {
			return 0, errors.New("retired Node metric value exceeds its size limit")
		}
		if int64(len(buffer)) > reader.remaining {
			buffer = buffer[:reader.remaining]
		}
	}
	n, err := reader.source.Read(buffer)
	reader.read += int64(n)
	if reader.limited {
		reader.remaining -= int64(n)
	}
	return n, err
}

func (reader *terminalMetricProofReader) bound(decoder *json.Decoder) {
	reader.remaining = terminalHistoryRecordLimit + 1 - (reader.read - decoder.InputOffset())
	reader.limited = true
}

func decodeTerminalMetricProofValue(decoder *json.Decoder, reader *terminalMetricProofReader, value any) error {
	reader.bound(decoder)
	start := decoder.InputOffset()
	err := decoder.Decode(value)
	if err == nil && decoder.InputOffset()-start > terminalHistoryRecordLimit {
		return errors.New("retired Node metric value exceeds its size limit")
	}
	return err
}

func terminalMetricProofToken(decoder *json.Decoder, reader *terminalMetricProofReader) (json.Token, error) {
	reader.bound(decoder)
	start := decoder.InputOffset()
	token, err := decoder.Token()
	if err == nil && decoder.InputOffset()-start > terminalHistoryRecordLimit {
		return nil, errors.New("retired Node metric token exceeds its size limit")
	}
	return token, err
}

func terminalMetricProofMore(decoder *json.Decoder, reader *terminalMetricProofReader) bool {
	// More also performs reads while looking past whitespace. Leave every
	// decoder operation bounded, including this otherwise implicit refill.
	reader.bound(decoder)
	return decoder.More()
}

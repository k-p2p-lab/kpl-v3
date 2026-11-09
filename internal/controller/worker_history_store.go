package controller

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	bolt "go.etcd.io/bbolt"
)

const (
	terminalHistoryFile = "node-history.db"
	// The existing heartbeat body can contain a 10 MiB original Node. Keep
	// room for its durable envelope without rejecting supported old Agents.
	terminalHistoryRecordLimit = 16 << 20
	terminalHistoryPageNodes   = 256
	terminalHistoryPageBytes   = 4 << 20
)

type terminalNodeHistory struct {
	directory string
	mu        sync.Mutex
}

type terminalHistoryRecord struct {
	Version    int        `json:"version"`
	Node       model.Node `json:"node"`
	RecordedAt time.Time  `json:"recordedAt"`
}

func newTerminalNodeHistory(dataDir string) *terminalNodeHistory {
	return &terminalNodeHistory{directory: dataDir}
}

func (h *terminalNodeHistory) open(readOnly bool) (*os.Root, *bolt.DB, error) {
	if h.directory == "" {
		if readOnly {
			return nil, nil, nil
		}
		return nil, nil, errors.New("terminal history data directory is unavailable")
	}
	if !readOnly {
		if err := os.MkdirAll(h.directory, 0755); err != nil {
			return nil, nil, err
		}
	}
	root, err := os.OpenRoot(h.directory)
	if readOnly && errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	info, err := root.Lstat(terminalHistoryFile)
	if readOnly && errors.Is(err, os.ErrNotExist) {
		root.Close()
		return nil, nil, nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		root.Close()
		return nil, nil, err
	}
	if err == nil && !info.Mode().IsRegular() {
		root.Close()
		return nil, nil, errors.New("terminal node history is not a regular file")
	}
	db, err := bolt.Open(filepath.Join(h.directory, terminalHistoryFile), 0600, &bolt.Options{Timeout: time.Second, ReadOnly: readOnly, OpenFile: func(_ string, flag int, mode os.FileMode) (*os.File, error) {
		return root.OpenFile(terminalHistoryFile, flag, mode)
	}})
	if err != nil {
		root.Close()
		return nil, nil, err
	}
	if !readOnly {
		if err := syncRunDirectory(root); err != nil {
			db.Close()
			root.Close()
			return nil, nil, err
		}
	}
	return root, db, nil
}

func terminalHistoryBuckets(tx *bolt.Tx, create bool) (*bolt.Bucket, *bolt.Bucket, *bolt.Bucket, error) {
	meta := tx.Bucket([]byte("meta"))
	if meta == nil {
		empty := true
		if err := tx.ForEach(func(_ []byte, _ *bolt.Bucket) error { empty = false; return nil }); err != nil {
			return nil, nil, nil, err
		}
		if !empty {
			return nil, nil, nil, errors.New("terminal history schema is missing")
		}
		if !create {
			return nil, nil, nil, nil
		}
		var err error
		meta, err = tx.CreateBucket([]byte("meta"))
		if err != nil {
			return nil, nil, nil, err
		}
		if err := meta.Put([]byte("version"), []byte{1}); err != nil {
			return nil, nil, nil, err
		}
		for _, name := range []string{"nodes", "counts", "propagation"} {
			if _, err := tx.CreateBucket([]byte(name)); err != nil {
				return nil, nil, nil, err
			}
		}
	}
	if !bytes.Equal(meta.Get([]byte("version")), []byte{1}) {
		return nil, nil, nil, errors.New("unsupported terminal history schema")
	}
	nodes, counts, propagation := tx.Bucket([]byte("nodes")), tx.Bucket([]byte("counts")), tx.Bucket([]byte("propagation"))
	if nodes == nil || counts == nil || propagation == nil {
		return nil, nil, nil, errors.New("terminal history schema is incomplete")
	}
	return nodes, counts, propagation, nil
}

func terminalHistoryIdentity(node model.Node) error {
	if node.ID == "" || len(node.ID) > bolt.MaxKeySize || !utf8.ValidString(node.ID) || node.AgentID == "" || !utf8.ValidString(node.AgentID) || !utf8.ValidString(node.RunID) {
		return errors.New("invalid terminal node identity or key size")
	}
	return nil
}

func terminalHistoryConfirmed(node model.Node) bool {
	return (node.State == model.NodeStopped || node.State == model.NodeFailed) && node.Metadata["cleanupComplete"] == "true"
}

func decodeTerminalHistory(key, encoded []byte) (terminalHistoryRecord, error) {
	var record terminalHistoryRecord
	if len(encoded) == 0 || len(encoded) > terminalHistoryRecordLimit {
		return record, errors.New("terminal history record exceeds its size limit")
	}
	if err := json.Unmarshal(encoded, &record); err != nil {
		return record, err
	}
	if record.Version != 1 || record.Node.ID != string(key) || record.RecordedAt.IsZero() || terminalHistoryIdentity(record.Node) != nil || !terminalHistoryConfirmed(record.Node) {
		return record, errors.New("invalid terminal history record identity or cleanup evidence")
	}
	return record, nil
}

func terminalHistoryOwner(old, incoming model.Node) error {
	if old.ID != incoming.ID || old.AgentID != incoming.AgentID || old.RunID != incoming.RunID {
		return fmt.Errorf("terminal node %q changed its Agent or Run ownership", incoming.ID)
	}
	return nil
}

func terminalHistoryLabels(node model.Node) nodeMetricLabels {
	return nodeMetricLabels{node.RunID, node.AgentID, node.Group, node.Role, node.Type, node.State}
}

func terminalHistoryLabelKey(labels nodeMetricLabels) ([]byte, error) {
	return terminalHistoryKey([6]string{labels.runID, labels.agentID, labels.group, labels.role, labels.nodeType, labels.state})
}

func terminalHistoryKey(value any) ([]byte, error) {
	key, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(key) > bolt.MaxKeySize {
		return nil, errors.New("terminal history aggregate key exceeds its size limit")
	}
	return key, nil
}

// No state/persistence lock may be held by the caller. Existing cold records
// freeze the original terminal evidence, including against a later live replay.
// Validation precedes all writes; the one write transaction also commits the
// metric aggregates, so retries cannot duplicate counts or topic baselines.
func (h *terminalNodeHistory) reconcile(nodes []model.Node, cold map[string]bool) (map[string]model.Node, error) {
	result := map[string]model.Node{}
	incoming := make(map[string]model.Node, len(nodes))
	confirmed := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		if err := terminalHistoryIdentity(node); err != nil {
			return nil, err
		}
		if old, exists := incoming[node.ID]; exists {
			if err := terminalHistoryOwner(old, node); err != nil {
				return nil, err
			}
			confirmed[node.ID] = confirmed[node.ID] && terminalHistoryConfirmed(node)
		} else {
			incoming[node.ID] = node
			confirmed[node.ID] = terminalHistoryConfirmed(node)
		}
	}
	if len(incoming) == 0 {
		return result, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	root, db, err := h.open(true)
	if err != nil {
		return nil, err
	}
	if db != nil {
		err = db.View(func(tx *bolt.Tx) error {
			stored, _, _, err := terminalHistoryBuckets(tx, false)
			if err != nil || stored == nil {
				return err
			}
			for id, node := range incoming {
				if encoded := stored.Get([]byte(id)); encoded != nil {
					record, err := decodeTerminalHistory([]byte(id), encoded)
					if err != nil {
						return err
					}
					if err := terminalHistoryOwner(record.Node, node); err != nil {
						return err
					}
					result[id] = record.Node
				}
			}
			return nil
		})
		err = errors.Join(err, db.Close(), root.Close())
		if err != nil {
			return nil, err
		}
	}
	type prepared struct {
		node             model.Node
		record, countKey []byte
		topics           [][]byte
	}
	pending := map[string]prepared{}
	recordedAt := time.Now().UTC()
	for id, node := range incoming {
		if _, exists := result[id]; exists || !cold[id] {
			continue
		}
		if !confirmed[id] {
			return nil, fmt.Errorf("terminal node %q lacks confirmed cleanup evidence", id)
		}
		var buffer bytes.Buffer
		encoder := json.NewEncoder(&buffer)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(terminalHistoryRecord{Version: 1, Node: node, RecordedAt: recordedAt}); err != nil {
			return nil, err
		}
		encoded := buffer.Bytes()
		if len(encoded) > terminalHistoryRecordLimit {
			return nil, errors.New("terminal history record exceeds its size limit")
		}
		countKey, err := terminalHistoryLabelKey(terminalHistoryLabels(node))
		if err != nil {
			return nil, err
		}
		item := prepared{node: node, record: encoded, countKey: countKey}
		if node.RunID != "" {
			topics := nodeTopics(node)
			if len(topics) == 0 {
				topics = []string{"kpl/default"}
			}
			for _, topic := range topics {
				key, err := terminalHistoryKey([3]string{node.RunID, node.AgentID, topic})
				if err != nil {
					return nil, err
				}
				item.topics = append(item.topics, key)
			}
		}
		pending[id] = item
	}
	if len(pending) == 0 {
		return result, nil
	}
	root, db, err = h.open(false)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		stored, counts, propagation, err := terminalHistoryBuckets(tx, true)
		if err != nil {
			return err
		}
		// Recheck every incoming owner under the write transaction, including
		// hot entries and records inserted by another store instance.
		for id, node := range incoming {
			if encoded := stored.Get([]byte(id)); encoded != nil {
				record, err := decodeTerminalHistory([]byte(id), encoded)
				if err != nil {
					return err
				}
				if err := terminalHistoryOwner(record.Node, node); err != nil {
					return err
				}
				result[id] = record.Node
			}
		}
		for id, item := range pending {
			if stored.Get([]byte(id)) != nil {
				continue
			}
			count := uint64(0)
			if encoded := counts.Get(item.countKey); encoded != nil {
				if len(encoded) != 8 {
					return errors.New("invalid terminal history count")
				}
				count = binary.BigEndian.Uint64(encoded)
			}
			if count >= uint64(math.MaxInt) {
				return errors.New("terminal history count exceeds integer range")
			}
			var encodedCount [8]byte
			binary.BigEndian.PutUint64(encodedCount[:], count+1)
			if err := stored.Put([]byte(id), item.record); err != nil {
				return err
			}
			if err := counts.Put(item.countKey, encodedCount[:]); err != nil {
				return err
			}
			for _, key := range item.topics {
				if err := propagation.Put(key, []byte{1}); err != nil {
					return err
				}
			}
			record, err := decodeTerminalHistory([]byte(id), item.record)
			if err != nil {
				return err
			}
			result[id] = record.Node
		}
		return nil
	})
	err = errors.Join(err, db.Close(), root.Close())
	if err != nil {
		return nil, err
	}
	return result, nil
}

// The returned values own their bytes. A page closes its DB before the caller
// writes to HTTP; both records scanned and raw bytes decoded are bounded.
func (h *terminalNodeHistory) page(ctx context.Context, after string, asOf time.Time, limit int) ([]model.Node, string, bool, error) {
	result := []model.Node{}
	if err := ctx.Err(); err != nil {
		return nil, after, false, err
	}
	if len(after) > bolt.MaxKeySize || !utf8.ValidString(after) || limit < 1 {
		return nil, after, false, errors.New("invalid terminal history page cursor or limit")
	}
	limit = min(limit, terminalHistoryPageNodes)
	h.mu.Lock()
	defer h.mu.Unlock()
	root, db, err := h.open(true)
	if err != nil {
		return nil, after, false, err
	}
	if db == nil {
		return result, after, true, nil
	}
	last, done := after, false
	err = db.View(func(tx *bolt.Tx) error {
		stored, _, _, err := terminalHistoryBuckets(tx, false)
		if err != nil {
			return err
		}
		if stored == nil {
			done = true
			return nil
		}
		cursor := stored.Cursor()
		key, value := cursor.First()
		if after != "" {
			key, value = cursor.Seek([]byte(after))
			if bytes.Equal(key, []byte(after)) {
				key, value = cursor.Next()
			}
		}
		scanned, bytesRead := 0, 0
		for key != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			if scanned >= limit || scanned > 0 && bytesRead+len(value) > terminalHistoryPageBytes {
				break
			}
			record, err := decodeTerminalHistory(key, value)
			if err != nil {
				return err
			}
			last = string(key)
			scanned++
			bytesRead += len(value)
			if asOf.IsZero() || !record.RecordedAt.After(asOf) {
				result = append(result, record.Node)
			}
			key, value = cursor.Next()
		}
		done = key == nil
		return nil
	})
	err = errors.Join(err, db.Close(), root.Close())
	if err != nil {
		return nil, after, false, err
	}
	return result, last, done, nil
}

// Aggregate keys avoid decoding the node history on every scrape. Only the
// explicitly excluded hot IDs need point lookups; callbacks hold no DB handle.
func (h *terminalNodeHistory) counts(excludeIDs map[string]bool, visit func(nodeMetricLabels, int) error) error {
	counts := map[nodeMetricLabels]int{}
	err := func() error {
		h.mu.Lock()
		defer h.mu.Unlock()
		root, db, err := h.open(true)
		if err != nil {
			return err
		}
		if db == nil {
			return nil
		}
		err = db.View(func(tx *bolt.Tx) error {
			nodes, stored, _, err := terminalHistoryBuckets(tx, false)
			if err != nil || stored == nil {
				return err
			}
			if err := stored.ForEach(func(key, value []byte) error {
				var labels [6]string
				if len(key) > bolt.MaxKeySize || json.Unmarshal(key, &labels) != nil || len(value) != 8 {
					return errors.New("invalid terminal history aggregate")
				}
				canonical, err := terminalHistoryKey(labels)
				if err != nil || !bytes.Equal(canonical, key) {
					return errors.New("invalid terminal history aggregate key")
				}
				count := binary.BigEndian.Uint64(value)
				if count == 0 || count > uint64(math.MaxInt) {
					return errors.New("invalid terminal history aggregate count")
				}
				counts[nodeMetricLabels{labels[0], labels[1], labels[2], labels[3], labels[4], labels[5]}] = int(count)
				return nil
			}); err != nil {
				return err
			}
			for id, exclude := range excludeIDs {
				if !exclude {
					continue
				}
				if len(id) > bolt.MaxKeySize {
					return errors.New("invalid excluded terminal history ID")
				}
				if encoded := nodes.Get([]byte(id)); encoded != nil {
					record, err := decodeTerminalHistory([]byte(id), encoded)
					if err != nil {
						return err
					}
					labels := terminalHistoryLabels(record.Node)
					if counts[labels] < 1 {
						return errors.New("terminal history aggregate does not match its node")
					}
					counts[labels]--
				}
			}
			return nil
		})
		return errors.Join(err, db.Close(), root.Close())
	}()
	if err != nil {
		return err
	}
	for labels, count := range counts {
		if count > 0 {
			if err := visit(labels, count); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *terminalNodeHistory) propagation(visit func(runID, agentID, topic string) error) error {
	var keys [][3]string
	err := func() error {
		h.mu.Lock()
		defer h.mu.Unlock()
		root, db, err := h.open(true)
		if err != nil {
			return err
		}
		if db == nil {
			return nil
		}
		err = db.View(func(tx *bolt.Tx) error {
			_, _, stored, err := terminalHistoryBuckets(tx, false)
			if err != nil || stored == nil {
				return err
			}
			return stored.ForEach(func(key, value []byte) error {
				var labels [3]string
				if json.Unmarshal(key, &labels) != nil || !bytes.Equal(value, []byte{1}) {
					return errors.New("invalid terminal history propagation baseline")
				}
				canonical, err := terminalHistoryKey(labels)
				if err != nil || !bytes.Equal(canonical, key) {
					return errors.New("invalid terminal history propagation key")
				}
				keys = append(keys, labels)
				return nil
			})
		})
		return errors.Join(err, db.Close(), root.Close())
	}()
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := visit(key[0], key[1], key[2]); err != nil {
			return err
		}
	}
	return nil
}

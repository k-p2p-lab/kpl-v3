package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	bolt "go.etcd.io/bbolt"
)

var historyNodesBucket = []byte("nodes-v1")
var historyMetaBucket = []byte("kpl-history")

// Open lazily after the Agent has bound its unique ports. A duplicate process
// must not migrate storage used by an older Agent before detecting that conflict.
func (h *agentHistory) store() (*bolt.DB, error) {
	h.storeMu.Lock()
	defer h.storeMu.Unlock()
	if h.closed {
		return nil, errors.New("Peer history store is closed")
	}
	if h.db == nil {
		db, err := openHistoryStore(h.directory)
		if err != nil {
			return nil, err
		}
		h.db = db
	}
	return h.db, nil
}

func (h *agentHistory) close() error {
	if h == nil {
		return nil
	}
	h.storeMu.Lock()
	defer h.storeMu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	if h.db != nil {
		return h.db.Close()
	}
	return nil
}

// The database deliberately occupies the former nodes directory's path. Older
// Agents fail their MkdirAll instead of silently ignoring retired identities.
// Conversion exchanges the complete database and legacy directory atomically.
func openHistoryStore(directory string) (*bolt.DB, error) {
	path := filepath.Join(directory, "nodes")
	info, err := os.Lstat(path)
	fresh := errors.Is(err, os.ErrNotExist)
	if err != nil && !fresh {
		return nil, err
	}
	if fresh {
		if err := createHistoryStore(directory); err != nil {
			return nil, err
		}
	} else if info.IsDir() {
		if err := migrateHistoryStore(directory); err != nil {
			return nil, err
		}
	} else if !fresh && !info.Mode().IsRegular() {
		return nil, errors.New("invalid Peer history store")
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open Peer history store: %w", err)
	}
	err = db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(historyMetaBucket)
		if meta == nil || string(meta.Get([]byte("format"))) != "1" || tx.Bucket(historyNodesBucket) == nil {
			return errors.New("unsupported or incomplete Peer history store")
		}
		return nil
	})
	if err == nil {
		err = syncTelemetryDirectory(directory)
	}
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func createHistoryStore(directory string) error {
	path := filepath.Join(directory, "nodes.initializing")
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return errors.New("invalid unfinished Peer history initialization")
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return err
	}
	err = initializeHistoryStore(db)
	err = errors.Join(err, db.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(path, filepath.Join(directory, "nodes")); err != nil {
		return err
	}
	return syncTelemetryDirectory(directory)
}

func initializeHistoryStore(db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(historyMetaBucket)
		if err != nil {
			return err
		}
		if err := meta.Put([]byte("format"), []byte("1")); err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(historyNodesBucket)
		return err
	})
}

func validateHistoryNode(data []byte) (model.Node, error) {
	var node model.Node
	if len(data) > heartbeatBodyLimit {
		return node, errors.New("retired Peer record exceeds size limit")
	}
	if err := json.Unmarshal(data, &node); err != nil {
		return node, fmt.Errorf("read retired Peer: %w", err)
	}
	if node.ID == "" || node.RunID == "" || (node.State != model.NodeStopped && node.State != model.NodeFailed) {
		return node, errors.New("invalid retired Peer identity or state")
	}
	return node, nil
}

func readLegacyHistory(path string) (model.Node, []byte, error) {
	var node model.Node
	info, err := os.Lstat(path)
	if err != nil {
		return node, nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > heartbeatBodyLimit {
		return node, nil, errors.New("invalid retired Peer record")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return node, nil, err
	}
	node, err = validateHistoryNode(data)
	if err != nil {
		return node, nil, err
	}
	hash := sha256.Sum256([]byte(node.ID))
	if filepath.Base(path) != hex.EncodeToString(hash[:])+".json" {
		return node, nil, errors.New("retired Peer filename does not match identity")
	}
	return node, data, nil
}

func migrateHistoryStore(directory string) error {
	path, staging := filepath.Join(directory, "nodes"), filepath.Join(directory, "nodes.migrating")
	if info, err := os.Lstat(staging); err == nil {
		// An interrupted pre-install copy is disposable only while the complete
		// legacy source still occupies nodes. Never overwrite a saved directory.
		if !info.Mode().IsRegular() {
			return errors.New("unexpected Peer history migration backup")
		}
		if err := os.Remove(staging); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	db, err := bolt.Open(staging, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	if err := initializeHistoryStore(db); err != nil {
		return err
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	type record struct {
		key  [32]byte
		data []byte
	}
	var batch []record
	batchBytes := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(historyNodesBucket)
			for _, record := range batch {
				if err := bucket.Put(record.key[:], record.data); err != nil {
					return err
				}
			}
			return nil
		})
		batch, batchBytes = nil, 0
		return err
	}
	for {
		entries, readErr := dir.ReadDir(historyPruneBatch)
		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), ".json") {
				return fmt.Errorf("unexpected file in Peer history: %s", entry.Name())
			}
			node, data, err := readLegacyHistory(filepath.Join(path, entry.Name()))
			if err != nil {
				return err
			}
			if len(batch) > 0 && (len(batch) >= historyPruneBatch || batchBytes+len(data) > 1<<20) {
				if err := flush(); err != nil {
					return err
				}
			}
			batch = append(batch, record{sha256.Sum256([]byte(node.ID)), data})
			batchBytes += len(data)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	if err := syncTelemetryDirectory(directory); err != nil {
		return err
	}
	if err := exchangeHistoryPaths(path, staging); err != nil {
		return fmt.Errorf("install Peer history store (legacy files preserved): %w", err)
	}
	return syncTelemetryDirectory(directory)
}

func (h *agentHistory) containsStored(id string) (bool, error) {
	db, err := h.store()
	if err != nil {
		return false, err
	}
	hash := sha256.Sum256([]byte(id))
	found := false
	err = db.View(func(tx *bolt.Tx) error {
		found = tx.Bucket(historyNodesBucket).Get(hash[:]) != nil
		return nil
	})
	return found, err
}

func (h *agentHistory) saveNodes(nodes []model.Node) error {
	if len(nodes) == 0 {
		return nil
	}
	// Run indexes retain stop-all fencing for histories evicted from RAM.
	seenRuns := make(map[string]bool)
	for _, node := range nodes {
		if seenRuns[node.RunID] {
			continue
		}
		seenRuns[node.RunID] = true
		runPath := h.path("runs", node.RunID)
		if _, err := os.Stat(runPath); errors.Is(err, os.ErrNotExist) {
			data, _ := json.Marshal(node.RunID)
			if err := writeTelemetryFile(runPath, data); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	db, err := h.store()
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(historyNodesBucket)
		for _, node := range nodes {
			data, err := json.Marshal(node)
			if err != nil {
				return err
			}
			if _, err := validateHistoryNode(data); err != nil {
				return err
			}
			hash := sha256.Sum256([]byte(node.ID))
			if err := bucket.Put(hash[:], data); err != nil {
				return err
			}
		}
		return nil
	})
}

// No database transaction or mmap slice survives a heartbeat. A failed send
// retains this bounded page; future reads resume strictly after its last key.
func (h *agentHistory) replayPage(ctx context.Context, after []byte) (nodes []model.Node, last []byte, eof bool, err error) {
	db, err := h.store()
	if err != nil {
		return nil, nil, false, err
	}
	err = db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(historyNodesBucket).Cursor()
		key, value := cursor.First()
		if len(after) != 0 {
			key, value = cursor.Seek(after)
			if bytes.Equal(key, after) {
				key, value = cursor.Next()
			}
		}
		size := 0
		for key != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(nodes) > 0 && (len(nodes) >= historyPruneBatch || size+len(value) > 1<<20) {
				break
			}
			node, err := validateHistoryNode(value)
			if err != nil {
				return err
			}
			nodes = append(nodes, node)
			size += len(value)
			last = append(last[:0], key...)
			key, value = cursor.Next()
		}
		eof = key == nil
		return nil
	})
	return
}

// Reclaim old inodes in bounded batches only after checking the installed store
// still contains their exact bytes. A restart after conversion resumes cleanup.
func (h *agentHistory) cleanLegacyNodes() error {
	db, err := h.store()
	if err != nil {
		return err
	}
	path := filepath.Join(h.directory, "nodes.migrating")
	dir, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer dir.Close()
	entries, err := dir.ReadDir(historyPruneBatch)
	if err != nil && err != io.EOF {
		return err
	}
	for _, entry := range entries {
		file := filepath.Join(path, entry.Name())
		node, data, err := readLegacyHistory(file)
		if err != nil {
			return err
		}
		hash := sha256.Sum256([]byte(node.ID))
		if err := db.View(func(tx *bolt.Tx) error {
			if !bytes.Equal(tx.Bucket(historyNodesBucket).Get(hash[:]), data) {
				return errors.New("Peer history backup differs from installed store; retaining it")
			}
			return nil
		}); err != nil {
			return err
		}
		if err := os.Remove(file); err != nil {
			return err
		}
	}
	if len(entries) == 0 {
		if err := dir.Close(); err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return err
		}
		return syncTelemetryDirectory(h.directory)
	}
	return syncTelemetryDirectory(path)
}

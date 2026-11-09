package controller

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	bolt "go.etcd.io/bbolt"
)

const journalIndexFile = ".journal-identities.db"
const journalPendingFile = ".journal-pending.json"
const journalPendingLimit = 32 << 20

var errJournalIndexRequired = errors.New("Run telemetry identity index needs queued reconstruction; retry this batch")

type pendingJournalAppend struct {
	Offset int64    `json:"offset"`
	Data   []byte   `json:"data"`
	Keys   [][]byte `json:"keys"`
	SHA256 string   `json:"sha256,omitempty"`
}

func journalDataChecksum(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// Every accepted identity is disk backed. The pending transaction is a bounded
// write-ahead record for one ingress batch, bridging raw-log fsync and index
// commit without replaying the complete journal in the persistent service.
func openJournalIndex(directory string) (*os.Root, *bolt.DB, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, nil, err
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, nil, err
	}
	if info, err := root.Lstat(journalIndexFile); err == nil && !info.Mode().IsRegular() {
		root.Close()
		return nil, nil, errors.New("unsafe telemetry identity index")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		root.Close()
		return nil, nil, err
	}
	db, err := bolt.Open(filepath.Join(directory, journalIndexFile), 0600, &bolt.Options{Timeout: time.Second, OpenFile: func(_ string, flag int, mode os.FileMode) (*os.File, error) {
		return root.OpenFile(journalIndexFile, flag, mode)
	}})
	if err != nil {
		root.Close()
		return nil, nil, err
	}
	if err := syncRunDirectory(root); err != nil {
		db.Close()
		root.Close()
		return nil, nil, err
	}
	return root, db, nil
}

func journalActiveBucket(tx *bolt.Tx) *bolt.Bucket {
	meta := tx.Bucket([]byte("meta"))
	if meta == nil || !bytes.Equal(meta.Get([]byte("ready")), []byte{1}) {
		return nil
	}
	return tx.Bucket(meta.Get([]byte("active")))
}

func journalIndexReady(db *bolt.DB) bool {
	ready := false
	_ = db.View(func(tx *bolt.Tx) error { ready = journalActiveBucket(tx) != nil; return nil })
	return ready
}

func journalHasSources(root *os.Root, id string) (bool, error) {
	manifest, err := readRunArchive(root, id)
	if err != nil {
		return false, err
	}
	if len(manifest.Logs["events.jsonl"]) != 0 {
		return true, nil
	}
	entries, err := localRunEntries(root)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.Name() != "events.jsonl" && !logSegment(entry.Name(), "events.jsonl") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return false, err
		}
		if !info.Mode().IsRegular() {
			return false, errors.New("unsafe telemetry source")
		}
		if info.Size() != 0 {
			return true, nil
		}
	}
	return false, nil
}

func initializeEmptyJournalIndex(root *os.Root, db *bolt.DB, id string) error {
	if journalIndexReady(db) {
		return nil
	}
	hasSources, err := journalHasSources(root, id)
	if err != nil {
		return err
	}
	if hasSources {
		return errJournalIndexRequired
	}
	return db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("events")); err != nil {
			return err
		}
		meta, err := tx.CreateBucketIfNotExists([]byte("meta"))
		if err != nil {
			return err
		}
		if err := meta.Put([]byte("active"), []byte("events")); err != nil {
			return err
		}
		return meta.Put([]byte("ready"), []byte{1})
	})
}

func journalEventKey(event model.TraceEvent) []byte {
	identity := eventIdentity(event)
	if identity == "" {
		return nil
	}
	key := sha256.Sum256([]byte(identity))
	return key[:]
}

func recoverJournalAppend(root *os.Root, db *bolt.DB) ([]model.TraceEvent, error) {
	encoded, err := readResumeFile(root, journalPendingFile, journalPendingLimit)
	fileIntent := err == nil
	if errors.Is(err, os.ErrNotExist) {
		// Read an interrupted intent from the initial implementation too. New
		// batches use a separate file so deleting a large intent does not leave
		// that blob's free pages in every Run's persistent identity database.
		err = db.View(func(tx *bolt.Tx) error {
			if meta := tx.Bucket([]byte("meta")); meta != nil {
				encoded = bytes.Clone(meta.Get([]byte("pending")))
			}
			return nil
		})
	}
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 {
		if fileIntent {
			return nil, errors.New("pending telemetry append is empty")
		}
		return nil, nil
	}
	var pending pendingJournalAppend
	if err := json.Unmarshal(encoded, &pending); err != nil || pending.Offset < 0 || len(pending.Data) == 0 {
		return nil, errors.New("invalid pending telemetry append")
	}
	if (fileIntent || pending.SHA256 != "") && pending.SHA256 != journalDataChecksum(pending.Data) {
		return nil, errors.New("pending telemetry append checksum differs from its durable intent")
	}
	var events []model.TraceEvent
	decoder := json.NewDecoder(bytes.NewReader(pending.Data))
	for {
		var event model.TraceEvent
		if err := decoder.Decode(&event); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	file, err := root.OpenFile("events.jsonl", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < pending.Offset || info.Size() > pending.Offset+int64(len(pending.Data)) {
		return nil, errors.New("pending telemetry source boundary changed")
	}
	length := info.Size() - pending.Offset
	if length != 0 {
		present := make([]byte, int(length))
		if _, err := file.ReadAt(present, pending.Offset); err != nil || !bytes.Equal(present, pending.Data[:length]) {
			return nil, errors.New("pending telemetry prefix differs from its durable intent")
		}
	}
	if length < int64(len(pending.Data)) {
		if n, err := file.WriteAt(pending.Data[length:], info.Size()); err != nil {
			return nil, err
		} else if n != len(pending.Data)-int(length) {
			return nil, io.ErrShortWrite
		}
	}
	if err := errors.Join(file.Sync(), syncRunDirectory(root)); err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket := journalActiveBucket(tx)
		if bucket == nil {
			return errors.New("pending append has no committed telemetry index")
		}
		for _, key := range pending.Keys {
			if len(key) != sha256.Size {
				return errors.New("invalid pending telemetry identity")
			}
			if err := bucket.Put(key, []byte{1}); err != nil {
				return err
			}
		}
		return tx.Bucket([]byte("meta")).Delete([]byte("pending"))
	}); err != nil {
		return nil, err
	}
	if fileIntent {
		if err := root.Remove(journalPendingFile); err != nil {
			return nil, err
		}
		if err := syncRunDirectory(root); err != nil {
			return nil, err
		}
	}
	return events, nil
}

func (s *state) appendIndexedJournalLocked(id string, events []model.TraceEvent) ([]model.TraceEvent, error) {
	root, db, err := openJournalIndex(filepath.Join(s.dataDir, currentRunsDirectory, id))
	if err != nil {
		return nil, err
	}
	defer root.Close()
	defer db.Close()
	if err := initializeEmptyJournalIndex(root, db, id); err != nil {
		return nil, err
	}
	recovered, err := recoverJournalAppend(root, db)
	if err != nil {
		return nil, err
	}
	accepted := make([]model.TraceEvent, 0, len(events))
	keys := make([][]byte, 0, len(events))
	seen := make(map[string]bool, len(events))
	err = db.View(func(tx *bolt.Tx) error {
		bucket := journalActiveBucket(tx)
		for _, event := range events {
			key := journalEventKey(event)
			if len(key) != 0 {
				if seen[string(key)] || bucket.Get(key) != nil {
					continue
				}
				seen[string(key)] = true
				keys = append(keys, key)
			}
			accepted = append(accepted, event)
		}
		return nil
	})
	if err != nil || len(accepted) == 0 {
		return recovered, err
	}
	var buffer bytes.Buffer
	for _, event := range accepted {
		if err := json.NewEncoder(&buffer).Encode(event); err != nil {
			return recovered, err
		}
	}
	offset := int64(0)
	if info, err := root.Stat("events.jsonl"); err == nil {
		offset = info.Size()
	} else if !errors.Is(err, os.ErrNotExist) {
		return recovered, err
	}
	pending := pendingJournalAppend{Offset: offset, Data: buffer.Bytes(), Keys: keys, SHA256: journalDataChecksum(buffer.Bytes())}
	encoded, err := json.Marshal(pending)
	if err != nil {
		return recovered, err
	}
	if len(encoded) >= journalPendingLimit {
		return recovered, errors.New("telemetry append intent exceeds its bounded batch limit")
	}
	if err := writeAnalysisJSON(root, journalPendingFile, pending); err != nil {
		return recovered, fmt.Errorf("persist telemetry append intent: %w", err)
	}
	committed, err := recoverJournalAppend(root, db)
	if err == nil {
		s.markRunArchiveDirty(id)
	}
	return append(recovered, committed...), err
}

// Caller holds persistMu. Archive rotation and worker cursors call this before
// observing bytes so a crash-recovered append cannot be rotated mid-commit.
func (s *state) recoverPendingJournalLocked(id string) error {
	path := filepath.Join(s.dataDir, currentRunsDirectory, id)
	if _, err := os.Stat(filepath.Join(path, journalIndexFile)); errors.Is(err, os.ErrNotExist) {
		if _, pendingErr := os.Lstat(filepath.Join(path, journalPendingFile)); pendingErr == nil {
			return errors.New("pending telemetry append has no identity database")
		} else if !errors.Is(pendingErr, os.ErrNotExist) {
			return pendingErr
		}
		return nil
	} else if err != nil {
		return err
	}
	root, db, err := openJournalIndex(path)
	if err != nil {
		return err
	}
	defer root.Close()
	defer db.Close()
	events, err := recoverJournalAppend(root, db)
	if len(events) != 0 {
		s.publishJournalEvents(id, events)
		s.markRunArchiveDirty(id)
	}
	return err
}

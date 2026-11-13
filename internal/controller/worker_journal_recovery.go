package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
	bolt "go.etcd.io/bbolt"
)

func journalIndexWorkerFactory(_ context.Context, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
	var start workerSummaryStart
	if err := json.Unmarshal(raw, &start); err != nil {
		return nil, err
	}
	if start.RunID != peer.RunID() || start.Snapshot.Result.ID != start.RunID {
		return nil, errors.New("telemetry index worker identity mismatch")
	}
	return func(ctx context.Context, method string, _ json.RawMessage) (any, error) {
		if method != "execute" {
			return nil, errors.New("unknown telemetry index operation")
		}
		snapshot := workerSnapshotFromDescriptor(ctx, peer, 0, start.Snapshot)
		keys := make([][]byte, 0, 2048)
		flush := func() error {
			if len(keys) == 0 {
				return nil
			}
			err := peer.Call(ctx, "journal.keys", keys, nil)
			keys = keys[:0]
			return err
		}
		for _, file := range snapshot.files {
			if file.name != "events.jsonl" {
				continue
			}
			if err := scanAnalysisFile(ctx, file, func(data []byte) error {
				var event model.TraceEvent
				if err := json.Unmarshal(data, &event); err != nil {
					return err
				}
				if event.RunID != start.RunID {
					return errors.New("foreign event in telemetry index source")
				}
				if key := journalEventKey(event); key != nil {
					keys = append(keys, key)
				}
				if len(keys) == cap(keys) {
					return flush()
				}
				return nil
			}); err != nil {
				return nil, err
			}
		}
		return nil, flush()
	}, nil
}

func (s *Server) requestJournalIndexRecovery(id string) {
	if !validResultID(id) {
		return
	}
	if _, busy := s.journalRecovery.LoadOrStore(id, true); busy {
		return
	}
	s.cancelMu.Lock()
	if s.shuttingDown || len(s.experimentQueue) >= analysisJobLimit {
		s.cancelMu.Unlock()
		s.journalRecovery.Delete(id)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	slot := s.enqueueWorkLocked(ctx, model.WorkQueueItem{Kind: "analysis", Name: "Rebuild telemetry identity index", RunID: id}, cancel)
	s.cancelMu.Unlock()
	go func() {
		defer cancel()
		defer s.finishWork(slot)
		defer s.journalRecovery.Delete(id)
		if err := s.waitWork(ctx, slot); err != nil {
			return
		}
		// Cleanup itself repairs an old index before waiting for final ACKs.
		// It must never wait for another recovery slot queued behind this one.
		if err := s.waitQueueCleanup(ctx, id, scenario.Scenario{JobShutdownTimeout: "3m"}); err != nil {
			return
		}
		if err := s.rebuildJournalIndex(ctx, id); err != nil && ctx.Err() == nil {
			s.logger.Warn("rebuild telemetry identity index", "run", id, "error", err)
		}
	}()
}

// Called by the shared queue head, including its prior-Run cleanup subtask.
// No application/persistence lock is held during source/NAS reads or child Wait.
func (s *Server) rebuildJournalIndex(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.journalBuildMu.Lock()
	defer s.journalBuildMu.Unlock()
	path := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
	s.state.persistMu.Lock()
	if err := ctx.Err(); err != nil {
		s.state.persistMu.Unlock()
		return err
	}
	if deleted, err := s.state.resultWriteFencedLocked(id); err != nil || deleted {
		s.state.persistMu.Unlock()
		return err
	}
	// Most cleanup attempts revisit a committed index. A writable open loads
	// its freelist (or scans an old unsynced database) and syncs the directory
	// even when no recovery work exists. Keep that path read-only, checking
	// both intent formats under the same persistence lock as ingress.
	ready, err := journalIndexCleanAndReady(path)
	if err != nil || ready {
		s.state.persistMu.Unlock()
		return errors.Join(err, ctx.Err())
	}
	root, db, err := openJournalIndex(path)
	if err == nil {
		err = initializeEmptyJournalIndex(root, db, id)
	}
	if err == nil {
		var recovered []model.TraceEvent
		recovered, err = recoverJournalAppend(root, db)
		if len(recovered) != 0 {
			s.state.publishJournalEvents(id, recovered)
			s.state.markRunArchiveDirty(id)
		}
	}
	if db != nil {
		err = errors.Join(err, db.Close())
	}
	if root != nil {
		root.Close()
	}
	s.state.persistMu.Unlock()
	if !errors.Is(err, errJournalIndexRequired) {
		return err
	}
	snapshot, err := s.captureResultFilesContext(ctx, id, true)
	if err != nil {
		return err
	}
	defer snapshot.close()
	source := s.workerSnapshotSource([]*resultSnapshot{snapshot})
	stage := []byte("rebuild-" + rand.Text())
	// A staging bucket is never used for admission until the captured source
	// revision is revalidated and the worker has been completely reaped.
	s.state.persistMu.Lock()
	if deleted, err := s.state.resultWriteFencedLocked(id); err != nil || deleted {
		s.state.persistMu.Unlock()
		return errors.Join(err, errResultNotFound)
	}
	root, db, err = openJournalIndex(path)
	if err == nil {
		err = db.Update(func(tx *bolt.Tx) error {
			var active []byte
			if meta := tx.Bucket([]byte("meta")); meta != nil {
				active = meta.Get([]byte("active"))
			}
			var abandoned [][]byte
			if err := tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
				if bytes.HasPrefix(name, []byte("rebuild-")) && !bytes.Equal(name, active) {
					abandoned = append(abandoned, bytes.Clone(name))
				}
				return nil
			}); err != nil {
				return err
			}
			for _, name := range abandoned {
				if err := tx.DeleteBucket(name); err != nil {
					return err
				}
			}
			_, err := tx.CreateBucket(stage)
			return err
		})
	}
	if db != nil {
		err = errors.Join(err, db.Close())
	}
	if root != nil {
		root.Close()
	}
	s.state.persistMu.Unlock()
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		s.state.persistMu.Lock()
		defer s.state.persistMu.Unlock()
		if deleted, err := s.state.resultWriteFencedLocked(id); err != nil || deleted {
			return
		}
		root, db, err := openJournalIndex(path)
		if err == nil {
			_ = db.Update(func(tx *bolt.Tx) error { return tx.DeleteBucket(stage) })
			db.Close()
			root.Close()
		}
	}()
	broker := func(callCtx context.Context, method string, raw json.RawMessage) (any, error) {
		if method == "source.read" {
			return workerBinarySourceResult(source(callCtx, method, raw))
		}
		if method != "journal.keys" {
			return nil, errors.New("unknown telemetry index broker operation")
		}
		var keys [][]byte
		if err := json.Unmarshal(raw, &keys); err != nil || len(keys) > 2048 {
			return nil, errors.New("invalid telemetry identity page")
		}
		for _, key := range keys {
			if len(key) != sha256.Size {
				return nil, errors.New("invalid telemetry identity key")
			}
		}
		s.state.persistMu.Lock()
		defer s.state.persistMu.Unlock()
		if deleted, err := s.state.resultWriteFencedLocked(id); err != nil || deleted {
			return nil, errors.Join(err, errResultNotFound)
		}
		root, db, err := openJournalIndex(path)
		if err != nil {
			return nil, err
		}
		defer root.Close()
		defer db.Close()
		return nil, db.Update(func(tx *bolt.Tx) error {
			if err := callCtx.Err(); err != nil {
				return err
			}
			bucket := tx.Bucket(stage)
			if bucket == nil {
				return errors.New("telemetry reconstruction lease expired")
			}
			for _, key := range keys {
				if err := bucket.Put(key, []byte{1}); err != nil {
					return err
				}
			}
			return nil
		})
	}
	process, err := workerproc.Start(ctx, "controller-journal", workerSummaryStart{RunID: id, Snapshot: describeWorkerSnapshot(snapshot)}, broker, workerproc.Options{Command: s.workerCommand})
	if err != nil {
		if process != nil {
			s.rememberUnreapedWorker(id, process)
		}
		return err
	}
	executeErr := process.Call(ctx, "execute", nil, nil)
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	closeErr := process.CloseContext(stopCtx)
	cancel()
	if closeErr != nil {
		s.rememberUnreapedWorker(id, process)
	}
	if err := errors.Join(executeErr, closeErr); err != nil {
		return err
	}
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	if deleted, err := s.state.resultWriteFencedLocked(id); err != nil || deleted {
		return err
	}
	root, db, err = openJournalIndex(path)
	if err != nil {
		return err
	}
	defer root.Close()
	defer db.Close()
	revision, err := sourceRevisionAt(root, id)
	if err != nil || revision != snapshot.result.SourceRevision {
		return errors.Join(err, errors.New("telemetry index source changed during reconstruction"))
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		meta, err := tx.CreateBucketIfNotExists([]byte("meta"))
		if err != nil {
			return err
		}
		old := append([]byte(nil), meta.Get([]byte("active"))...)
		if err := meta.Put([]byte("active"), stage); err != nil {
			return err
		}
		if err := meta.Put([]byte("ready"), []byte{1}); err != nil {
			return err
		}
		if len(old) != 0 && !bytes.Equal(old, stage) {
			return tx.DeleteBucket(old)
		}
		return nil
	})
	committed = err == nil
	return err
}

func journalIndexCleanAndReady(path string) (bool, error) {
	root, db, err := openJournalIndexMode(path, true)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	ready := false
	err = db.View(func(tx *bolt.Tx) error {
		if meta := tx.Bucket([]byte("meta")); meta != nil {
			ready = journalActiveBucket(tx) != nil && len(meta.Get([]byte("pending"))) == 0
		}
		return nil
	})
	if err == nil && ready {
		if _, pendingErr := root.Lstat(journalPendingFile); pendingErr == nil {
			ready = false
		} else if !errors.Is(pendingErr, os.ErrNotExist) {
			err = pendingErr
		}
	}
	return ready, errors.Join(err, db.Close(), root.Close())
}

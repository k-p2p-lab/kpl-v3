package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

const defaultAnalysisTransferMiBPerSecond uint64 = 32

// Remote analysis reads a captured source in bounded blocks. Pace the whole
// job, releasing the shared storage read slot before waiting for the next
// block's budget. A slow transfer must not keep a storage slot occupied.
func (s *Server) remoteAnalysisSnapshotSource(snapshots []*resultSnapshot) workerproc.Handler {
	source := s.workerSnapshotSource(snapshots)
	rate := s.config.AnalysisTransferMiBPerSecond
	if rate == 0 {
		rate = defaultAnalysisTransferMiBPerSecond
	}
	gate := make(chan struct{}, 1)
	return func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		select {
		case gate <- struct{}{}:
			defer func() { <-gate }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		started := time.Now()
		value, err := source(ctx, method, raw)
		if err != nil {
			return nil, err
		}
		chunk := value.(workerSourceBytes)
		var request workerSourceRead
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		// The source handler has validated the indices, offset and length.
		file := snapshots[request.Snapshot].files[request.File]
		length := min(int64(request.Length), max(0, file.size-request.Offset))
		if int64(len(chunk.Data)) != length {
			return nil, io.ErrUnexpectedEOF
		}
		if err := sleepContext(ctx, archiveTransferDelay(len(chunk.Data), rate, time.Since(started))); err != nil {
			return nil, err
		}
		return chunk, nil
	}
}

// The independent queue can read committed results during another Run. Recovery
// of an unfinished journal append can involve a large write under persistMu,
// so leave that operation to the existing journal recovery paths.
func checkRemoteAnalysisJournal(root *os.Root) error {
	if _, err := root.Lstat(journalPendingFile); err == nil {
		return fmt.Errorf("%w: telemetry append awaits recovery", errResultBusy)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := root.Lstat(journalIndexFile); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	pending, err := hasLegacyJournalIntent(root.Name())
	if err != nil {
		return err
	}
	if pending {
		return fmt.Errorf("%w: telemetry append awaits recovery", errResultBusy)
	}
	return nil
}

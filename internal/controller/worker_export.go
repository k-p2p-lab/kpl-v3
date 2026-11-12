package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

type snapshotMetricsCache struct {
	mu        sync.Mutex
	flight    chan struct{}
	ready     bool
	metrics   model.Metrics
	at        time.Time
	boundary  func() time.Time
	calculate func(context.Context) (model.Metrics, error)
}

func (c *snapshotMetricsCache) get(ctx context.Context) (model.Metrics, error) {
	for {
		if err := ctx.Err(); err != nil {
			return model.Metrics{}, err
		}
		boundary := time.Time{}
		if c.boundary != nil {
			boundary = c.boundary()
		}
		c.mu.Lock()
		if c.ready && c.at.Equal(boundary) {
			metrics := c.metrics
			c.mu.Unlock()
			return metrics, nil
		}
		if pending := c.flight; pending != nil {
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return model.Metrics{}, ctx.Err()
			case <-pending:
				continue
			}
		}
		c.flight = make(chan struct{})
		pending := c.flight
		c.mu.Unlock()
		metrics, err := c.calculate(ctx)
		c.mu.Lock()
		if err == nil {
			c.metrics, c.ready, c.at = metrics, true, boundary
		}
		c.flight = nil
		close(pending)
		c.mu.Unlock()
		return metrics, err
	}
}

func exportWorkerFactory(_ context.Context, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
	var start workerSummaryStart
	if err := json.Unmarshal(raw, &start); err != nil {
		return nil, err
	}
	if start.RunID != peer.RunID() || start.Snapshot.Result.ID != start.RunID || start.Snapshot.ExportedAt.IsZero() {
		return nil, errors.New("export worker snapshot identity is invalid")
	}
	return func(ctx context.Context, method string, _ json.RawMessage) (any, error) {
		if method != "execute" {
			return nil, errors.New("unknown export operation")
		}
		snapshot := workerSnapshotFromDescriptor(ctx, peer, 0, start.Snapshot)
		var events io.Reader = strings.NewReader("")
		for _, file := range snapshot.files {
			if file.name == "events.jsonl" {
				events = file.reader()
				break
			}
		}
		return summarizeRunEventsContext(ctx, snapshot.result.ID, events, snapshot.exportedAt)
	}, nil
}

func (s *Server) exportMetricsWithWorker(ctx context.Context, snapshot *resultSnapshot) (model.Metrics, error) {
	snapshot.releaseArchiveReader()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.cancelMu.Lock()
	if s.shuttingDown || ctx.Err() != nil {
		s.cancelMu.Unlock()
		return model.Metrics{}, context.Canceled
	}
	if len(s.experimentQueue) >= analysisJobLimit {
		s.cancelMu.Unlock()
		return model.Metrics{}, errAnalysisQueueFull
	}
	slot := s.enqueueWorkLocked(ctx, model.WorkQueueItem{Kind: "analysis", Name: "Prepare download metrics", RunID: snapshot.result.ID}, cancel)
	s.cancelMu.Unlock()
	defer s.finishWork(slot)
	if err := s.waitWork(ctx, slot); err != nil {
		return model.Metrics{}, err
	}
	if err := s.waitQueueCleanup(ctx, snapshot.result.ID, scenario.Scenario{JobShutdownTimeout: "3m"}); err != nil {
		return model.Metrics{}, err
	}
	s.updateWorkStatus(slot.id, "Computing snapshot metrics", 0)
	broker := workerBinarySourceHandler(s.workerSnapshotSource([]*resultSnapshot{snapshot}))
	process, err := workerproc.Start(ctx, "controller-export", workerSummaryStart{RunID: snapshot.result.ID, Snapshot: describeWorkerSnapshot(snapshot)}, broker, workerproc.Options{Command: s.workerCommand})
	if err != nil {
		if process != nil {
			s.rememberUnreapedWorker(snapshot.result.ID, process)
		}
		return model.Metrics{}, err
	}
	var metrics model.Metrics
	workErr := process.Call(ctx, "execute", nil, &metrics)
	stopCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	closeErr := process.CloseContext(stopCtx)
	stop()
	if closeErr != nil {
		s.rememberUnreapedWorker(snapshot.result.ID, process)
	}
	if err := errors.Join(workErr, closeErr); err != nil {
		return model.Metrics{}, err
	}
	if metrics.RunID != snapshot.result.ID {
		return model.Metrics{}, errors.New("export worker returned metrics for a different Run")
	}
	s.updateWorkStatus(slot.id, "Snapshot metrics ready", 100)
	return metrics, nil
}

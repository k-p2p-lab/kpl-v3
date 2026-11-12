package controller

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)

// A local calculation reads pinned disk/NAS handles through the same bounded
// queue as an Agent. Its producer also owns a cancelable source context: local
// reads have no network reply to drain, but an in-flight ReadAt must still join
// before snapshots, handles or the work-queue slot can be released.
type controllerAnalysisSource struct {
	*analysisPrefetch
	cancel context.CancelFunc
	once   sync.Once
}

func (s *Server) controllerAnalysisPrefetch(ctx context.Context, input analysisProcessInput, snapshots []*resultSnapshot) (*controllerAnalysisSource, error) {
	budget := s.config.AnalysisPrefetchMiB
	if budget == 0 {
		budget = defaultAnalysisPrefetchMiB
	}
	if budget > maxAnalysisPrefetchMiB {
		return nil, errors.New("analysis prefetch must be between 1 and 1024 MiB")
	}
	sourceCtx, cancel := context.WithCancel(ctx)
	// A storage read slot belongs to one I/O block, not to queued bytes or a
	// waiting decoder. The local path retains its unthrottled I/O policy.
	source := s.workerSnapshotSource(snapshots)
	prefetch := newAnalysisPrefetch(sourceCtx, input, int64(budget)<<20, func(ctx context.Context, read workerSourceRead) (workerSourceBytes, error) {
		raw, err := json.Marshal(read)
		if err != nil {
			return workerSourceBytes{}, err
		}
		value, err := source(ctx, "source.read", raw)
		if err != nil {
			return workerSourceBytes{}, err
		}
		bytes, ok := value.(workerSourceBytes)
		if !ok {
			return workerSourceBytes{}, errors.New("invalid analysis source response")
		}
		return bytes, nil
	})
	return &controllerAnalysisSource{analysisPrefetch: prefetch, cancel: cancel}, nil
}

func (source *controllerAnalysisSource) close() {
	source.once.Do(func() {
		source.cancel()
		source.analysisPrefetch.close()
	})
}

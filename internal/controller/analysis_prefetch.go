package controller

import (
	"context"
	"errors"
	"io"
	"sync"
)

const (
	defaultAnalysisPrefetchMiB uint64 = 64
	maxAnalysisPrefetchMiB     uint64 = 1024
)

type analysisPrefetchFetch func(context.Context, workerSourceRead) (workerSourceBytes, error)

// One calculation owns this queue. Foreground callbacks are serialized;
// only the current sequential source is prefetched. Sampled hashes remain small
// exact reads, and no source is fetched before the analysis asks for it.
type analysisPrefetch struct {
	mu     sync.Mutex
	ctx    context.Context
	input  analysisProcessInput
	budget int64
	fetch  analysisPrefetchFetch
	stream *analysisPrefetchStream
	closed bool
}

type analysisPrefetchBlock struct {
	offset int64
	data   []byte
}

type analysisPrefetchStream struct {
	mu       sync.Mutex
	ctx      context.Context
	fetch    analysisPrefetchFetch
	source   workerSourceRead
	end      int64
	budget   int64
	position int64
	next     int64
	used     int64 // Retained blocks plus the in-flight reservation.
	blocks   []analysisPrefetchBlock
	err      error
	stopped  bool
	changed  chan struct{}
	done     chan struct{}
}

func newAnalysisPrefetch(ctx context.Context, input analysisProcessInput, budgetBytes int64, fetch func(context.Context, workerSourceRead) (workerSourceBytes, error)) *analysisPrefetch {
	if budgetBytes <= 0 {
		budgetBytes = int64(defaultAnalysisPrefetchMiB) << 20
	}
	return &analysisPrefetch{ctx: ctx, input: input, budget: budgetBytes, fetch: fetch}
}

func (p *analysisPrefetch) fetchBlock(ctx context.Context, read workerSourceRead) (workerSourceBytes, error) {
	length, eof, err := remoteAnalysisReadBoundary(p.input, read)
	if err != nil {
		return workerSourceBytes{}, err
	}
	block, err := p.fetch(ctx, read)
	if err != nil {
		return workerSourceBytes{}, err
	}
	if len(block.Data) != length || block.EOF != eof {
		return workerSourceBytes{}, errors.New("invalid remote analysis source block")
	}
	return block, nil
}

func (p *analysisPrefetch) read(ctx context.Context, read workerSourceRead) (workerSourceBytes, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return workerSourceBytes{}, err
	}
	if err := p.ctx.Err(); err != nil {
		return workerSourceBytes{}, context.Cause(p.ctx)
	}
	if p.closed {
		return workerSourceBytes{}, errors.New("analysis source prefetch is closed")
	}
	length, eof, err := remoteAnalysisReadBoundary(p.input, read)
	if err != nil || length == 0 {
		return workerSourceBytes{EOF: eof}, err
	}
	if p.stream != nil && (p.stream.source.Snapshot != read.Snapshot || p.stream.source.File != read.File || p.stream.position != read.Offset) {
		p.stream.close()
		p.stream = nil
	}
	if p.stream == nil {
		if read.Length < 64<<10 {
			// Use the job context for an exchange: canceling just a request
			// must not abandon a reply on the shared ordered connection.
			block, err := p.fetchBlock(p.ctx, read)
			if err == nil {
				err = ctx.Err()
			}
			return block, err
		}
		p.stream = &analysisPrefetchStream{ctx: p.ctx, fetch: p.fetchBlock, source: read,
			end: p.input.Snapshots[read.Snapshot].Files[read.File].Size, budget: p.budget,
			position: read.Offset, next: read.Offset, changed: make(chan struct{}), done: make(chan struct{})}
		go p.stream.run()
	}
	data, err := p.stream.read(ctx, length)
	return workerSourceBytes{Data: data, EOF: eof}, err
}

func (p *analysisPrefetch) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.stream != nil {
		p.stream.close()
		p.stream = nil
	}
}

func (s *analysisPrefetchStream) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *analysisPrefetchStream) run() {
	defer close(s.done)
	for {
		s.mu.Lock()
		if s.stopped || s.ctx.Err() != nil || s.next >= s.end {
			s.notifyLocked()
			s.mu.Unlock()
			return
		}
		length := min(int64(workerSourceChunk), s.end-s.next, s.budget)
		if s.used+length > s.budget {
			changed := s.changed
			s.mu.Unlock()
			select {
			case <-s.ctx.Done():
			case <-changed:
			}
			continue
		}
		read := s.source
		read.Offset, read.Length = s.next, int(length)
		s.used += length
		s.mu.Unlock()

		// Normal file switches/close let this exchange finish. Only whole
		// job cancellation interrupts it, preserving request/reply ordering.
		block, err := s.fetch(s.ctx, read)
		s.mu.Lock()
		if s.stopped || err != nil {
			s.used -= length
			s.err = err
			s.notifyLocked()
			s.mu.Unlock()
			return
		}
		s.blocks = append(s.blocks, analysisPrefetchBlock{offset: read.Offset, data: block.Data})
		s.next += length
		s.notifyLocked()
		s.mu.Unlock()
	}
}

func (s *analysisPrefetchStream) read(ctx context.Context, length int) ([]byte, error) {
	// Responses own their bytes. The private RPC marshals a returned response
	// after the callback unlocks; queue removal must never reuse that memory.
	data := make([]byte, length)
	read := 0
	for read < length {
		s.mu.Lock()
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		if err := s.ctx.Err(); err != nil {
			s.mu.Unlock()
			return nil, context.Cause(s.ctx)
		}
		if len(s.blocks) != 0 {
			block := s.blocks[0]
			start := s.position - block.offset
			n := copy(data[read:], block.data[start:])
			s.position += int64(n)
			read += n
			if s.position == block.offset+int64(len(block.data)) {
				s.used -= int64(len(block.data))
				copy(s.blocks, s.blocks[1:])
				s.blocks[len(s.blocks)-1] = analysisPrefetchBlock{}
				s.blocks = s.blocks[:len(s.blocks)-1]
				s.notifyLocked()
			}
			s.mu.Unlock()
			continue
		}
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return nil, err
		}
		if s.stopped || s.position >= s.end {
			s.mu.Unlock()
			return nil, io.ErrUnexpectedEOF
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.ctx.Done():
			return nil, context.Cause(s.ctx)
		case <-changed:
		}
	}
	return data, nil
}

func (s *analysisPrefetchStream) close() {
	s.mu.Lock()
	s.stopped = true
	s.notifyLocked()
	s.mu.Unlock()
	<-s.done
	s.mu.Lock()
	s.blocks = nil
	s.used = 0
	s.mu.Unlock()
}

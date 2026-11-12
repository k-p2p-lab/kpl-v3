package controller

import (
	"context"
	"errors"
	"math"
	"sync"
)

const remoteAnalysisProgressQueueLimit = 1024

// Child progress must not wait for a speculative source exchange: the next
// source block may be slow while already-buffered bytes are ready to parse.
// This sender retains byte counts and phase boundaries, with bounded storage.
type remoteAnalysisProgress struct {
	ctx    context.Context
	send   func(context.Context, analysisProcessProgress) error
	fail   func()
	mu     sync.Mutex
	queue  []analysisProcessProgress
	closed bool
	err    error
	wake   chan struct{}
	done   chan struct{}
}

func newRemoteAnalysisProgress(ctx context.Context, send func(context.Context, analysisProcessProgress) error, fail func()) *remoteAnalysisProgress {
	p := &remoteAnalysisProgress{ctx: ctx, send: send, fail: fail, wake: make(chan struct{}, 1), done: make(chan struct{})}
	go p.run()
	return p
}

func (p *remoteAnalysisProgress) notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *remoteAnalysisProgress) enqueue(ctx context.Context, update analysisProcessProgress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	if err := p.ctx.Err(); err != nil {
		return err
	}
	if p.closed {
		return errors.New("analysis progress source is closed")
	}
	if update.Bytes < 0 {
		return errors.New("invalid analysis progress byte count")
	}
	if count := len(p.queue); count > 0 {
		last := &p.queue[count-1]
		if last.Snapshot == update.Snapshot && last.Phase == update.Phase {
			if update.Bytes > math.MaxInt64-last.Bytes {
				return errors.New("analysis progress byte count exceeds the supported range")
			}
			last.Bytes += update.Bytes
			p.notify()
			return nil
		}
		// Intermediate graph-status labels carry no input bytes. Retain the
		// most recent label for this Run without moving source bytes into it.
		if last.Snapshot == update.Snapshot && last.Bytes == 0 && update.Bytes == 0 {
			*last = update
			p.notify()
			return nil
		}
	}
	if len(p.queue) >= remoteAnalysisProgressQueueLimit {
		return errors.New("analysis progress queue limit exceeded")
	}
	p.queue = append(p.queue, update)
	p.notify()
	return nil
}

func (p *remoteAnalysisProgress) run() {
	defer close(p.done)
	defer func() {
		p.mu.Lock()
		p.queue = nil
		p.mu.Unlock()
	}()
	for {
		if p.ctx.Err() != nil {
			return
		}
		p.mu.Lock()
		if len(p.queue) == 0 {
			closed := p.closed
			p.mu.Unlock()
			if closed {
				return
			}
			select {
			case <-p.ctx.Done():
				return
			case <-p.wake:
				continue
			}
		}
		update := p.queue[0]
		copy(p.queue, p.queue[1:])
		p.queue[len(p.queue)-1] = analysisProcessProgress{}
		p.queue = p.queue[:len(p.queue)-1]
		p.mu.Unlock()
		if err := p.send(p.ctx, update); err != nil {
			// A terminal calculation failure deliberately cancels this sender;
			// it must not replace that original failure with its own cleanup.
			if p.ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				return
			}
			p.mu.Lock()
			p.err, p.closed = err, true
			p.mu.Unlock()
			if p.fail != nil {
				p.fail()
			}
			return
		}
	}
}

// On normal source completion this drains all bytes before metadata/artifacts
// close source admission. On cancellation it joins the stopped sender instead.
func (p *remoteAnalysisProgress) close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.notify()
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

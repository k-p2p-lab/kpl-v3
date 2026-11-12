package workerproc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// Uncooperative application handlers cannot be killed as Go goroutines. Bound
// them across peer replacement too, rather than leaking a fresh set every Run.
var activeHandlers = func() [maxCallDepth + 1]chan struct{} {
	var limits [maxCallDepth + 1]chan struct{}
	for depth := range limits {
		// Deeper callbacks retain their own capacity even when every outer
		// handler is waiting for a nested call on another connection.
		limits[depth] = make(chan struct{}, callsPerDepth)
	}
	return limits
}()

type depthKey struct{}
type result struct {
	body json.RawMessage
	data []byte
	err  error
}
type packet struct {
	data    []byte
	written chan error
	charged int64
}
type request struct {
	frame  frame
	ctx    context.Context
	cancel context.CancelFunc
}

type Peer struct {
	id       identity
	ctx      context.Context
	cancel   context.CancelFunc
	reader   io.ReadCloser
	writer   io.WriteCloser
	done     chan struct{}
	stopped  chan struct{}
	workers  sync.WaitGroup
	calls    sync.WaitGroup
	closeOne sync.Once
	mu       sync.Mutex
	err      error
	pending  map[uint64]chan result
	active   map[uint64]context.CancelFunc
	handler  Handler
	sequence atomic.Uint64
	ready    chan struct{}
	readyOne sync.Once
	data     chan packet
	control  chan packet
	bytes    *semaphore.Weighted
	requests [maxCallDepth + 1]chan request
	credits  [maxCallDepth + 1]chan struct{}
}

func newPeer(parent context.Context, id identity, r io.ReadCloser, w io.WriteCloser, handler Handler) *Peer {
	ctx, cancel := context.WithCancel(parent)
	p := &Peer{id: id, ctx: ctx, cancel: cancel, reader: r, writer: w,
		done: make(chan struct{}), stopped: make(chan struct{}), pending: make(map[uint64]chan result), active: make(map[uint64]context.CancelFunc), handler: handler,
		ready: make(chan struct{}), data: make(chan packet, callsPerDepth), control: make(chan packet, 64), bytes: semaphore.NewWeighted(32 << 20)}
	for depth := range p.requests {
		p.requests[depth] = make(chan request, callsPerDepth)
		p.credits[depth] = make(chan struct{}, callsPerDepth)
		for range workersPerDepth {
			p.workers.Add(1)
			go func(queue <-chan request) { defer p.workers.Done(); p.worker(queue) }(p.requests[depth])
		}
	}
	p.workers.Add(3)
	go func() { defer p.workers.Done(); p.readLoop() }()
	go func() { defer p.workers.Done(); p.writeLoop() }()
	go func() {
		defer p.workers.Done()
		select {
		case <-ctx.Done():
			p.shutdown(ctx.Err())
		case <-p.done:
		}
	}()
	go func() {
		<-p.done
		p.workers.Wait()
		p.calls.Wait()
		p.mu.Lock()
		clear(p.pending)
		clear(p.active)
		p.handler = nil
		p.mu.Unlock()
		for _, queue := range p.requests {
			for {
				select {
				case r := <-queue:
					r.cancel()
				default:
					goto requestsDrained
				}
			}
		requestsDrained:
		}
		for _, queue := range []chan packet{p.data, p.control} {
			for {
				select {
				case q := <-queue:
					if q.charged != 0 {
						p.bytes.Release(q.charged)
					}
				default:
					goto packetsDrained
				}
			}
		packetsDrained:
		}
		close(p.stopped)
	}()
	return p
}

func (p *Peer) RunID() string         { return p.id.RunID }
func (p *Peer) Epoch() string         { return p.id.Epoch }
func (p *Peer) Kind() string          { return p.id.Kind }
func (p *Peer) Done() <-chan struct{} { return p.done }
func (p *Peer) connectionError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	return ErrClosed
}

func (p *Peer) shutdown(err error) {
	p.closeOne.Do(func() {
		if err == nil {
			err = ErrClosed
		}
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		p.cancel()
		p.reader.Close()
		p.writer.Close()
		close(p.done)
		// Drop queued request bodies even if an application handler is stuck.
		for _, queue := range p.requests {
			for {
				select {
				case r := <-queue:
					r.cancel()
				default:
					goto drained
				}
			}
		drained:
		}
	})
}

func (p *Peer) Call(ctx context.Context, method string, in, out any) error {
	_, err := p.call(ctx, method, in, nil, out, false)
	return err
}

// CallBinary sends and receives a bounded raw payload alongside JSON metadata.
// Payloads are copied into an immutable outbound packet before admission.
func (p *Peer) CallBinary(ctx context.Context, method string, in any, data []byte, out any) ([]byte, error) {
	return p.call(ctx, method, in, data, out, true)
}

func (p *Peer) call(ctx context.Context, method string, in any, data []byte, out any, binary bool) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("private worker Call requires a context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(data) > MaxBinaryBytes {
		return nil, ErrTooLarge
	}
	p.mu.Lock()
	if p.err != nil {
		err := p.err
		p.mu.Unlock()
		return nil, err
	}
	p.calls.Add(1)
	p.mu.Unlock()
	defer p.calls.Done()
	if method == "" || len(method) > 128 {
		return nil, fmt.Errorf("%w: invalid method", ErrProtocol)
	}
	depth := 0
	if parentDepth, ok := ctx.Value(depthKey{}).(int); ok {
		depth = parentDepth + 1
	}
	if depth > maxCallDepth {
		return nil, ErrDepth
	}
	select {
	case p.credits[depth] <- struct{}{}:
		defer func() { <-p.credits[depth] }()
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.done:
		return nil, p.connectionError()
	}
	body, err := marshalBody(in)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id := p.sequence.Add(1)
	answer := make(chan result, 1)
	p.mu.Lock()
	if p.err != nil {
		err = p.err
		p.mu.Unlock()
		return nil, err
	}
	p.pending[id] = answer
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.pending, id)
		p.mu.Unlock()
	}()
	f := frame{identity: p.id, Type: "request", ID: id, Method: method, Body: body, Depth: depth, data: data}
	if deadline, ok := ctx.Deadline(); ok {
		f.Deadline = deadline.UnixNano()
	}
	if err := p.send(ctx, f, false); err != nil {
		if ctx.Err() == nil && p.ctx.Err() != nil {
			return nil, p.connectionError()
		}
		return nil, err
	}
	select {
	case r := <-answer:
		if r.err != nil {
			return nil, r.err
		}
		if !binary && len(r.data) != 0 {
			return nil, fmt.Errorf("%w: binary response requires CallBinary", ErrProtocol)
		}
		if out != nil {
			if err := json.Unmarshal(r.body, out); err != nil {
				return nil, err
			}
		}
		return r.data, nil
	case <-ctx.Done():
		p.sendControl(frame{identity: p.id, Type: "cancel", ID: id})
		return nil, ctx.Err()
	case <-p.done:
		return nil, p.connectionError()
	}
}

func (p *Peer) send(ctx context.Context, f frame, written bool) error {
	data, err := encodeFrame(f)
	if err != nil {
		return err
	}
	return p.sendPrepared(ctx, data, written)
}

func (p *Peer) sendPrepared(ctx context.Context, data []byte, written bool) error {
	ctx, cancel := context.WithCancel(ctx)
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(p.ctx, func() { cancel(); close(callbackDone) })
	defer func() {
		if !stop() {
			<-callbackDone
		}
	}()
	defer cancel()
	if err := p.bytes.Acquire(ctx, int64(len(data))); err != nil {
		return err
	}
	q := packet{data: data, charged: int64(len(data))}
	if written {
		q.written = make(chan error, 1)
	}
	select {
	case p.data <- q:
	case <-ctx.Done():
		p.bytes.Release(q.charged)
		return ctx.Err()
	}
	if !written {
		return nil
	}
	select {
	case err := <-q.written:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Peer) sendControl(f frame) {
	data, err := encodeFrame(f)
	if err != nil || len(data) > 4096 {
		p.shutdown(ErrProtocol)
		return
	}
	select {
	case p.control <- packet{data: data}:
	case <-p.done:
	default:
		p.shutdown(ErrBusy)
	}
}

func (p *Peer) writeLoop() {
	defer func() {
		for {
			select {
			case q := <-p.data:
				p.bytes.Release(q.charged)
			default:
				return
			}
		}
	}()
	for {
		var q packet
		select {
		case <-p.done:
			return
		case q = <-p.control:
		default:
			select {
			case <-p.done:
				return
			case q = <-p.control:
			case q = <-p.data:
			}
		}
		err := writeFrame(p.writer, q.data)
		q.data = nil
		if q.charged != 0 {
			p.bytes.Release(q.charged)
		}
		if q.written != nil {
			q.written <- err
		}
		if err != nil {
			p.shutdown(err)
			return
		}
	}
}

func (p *Peer) readLoop() {
	seen := make(map[uint64]struct{})
	var recent [1024]uint64
	nextRecent := 0
	canceled := make(map[uint64]struct{})
	for {
		f, err := readFrame(p.reader)
		if err != nil {
			p.shutdown(err)
			return
		}
		if f.identity != p.id {
			p.shutdown(fmt.Errorf("%w: worker identity mismatch", ErrProtocol))
			return
		}
		switch f.Type {
		case "ready":
			p.readyOne.Do(func() { close(p.ready) })
		case "abort":
			p.shutdown(&RemoteError{Code: "startup", Message: "private worker initialization failed"})
			return
		case "response":
			p.mu.Lock()
			answer := p.pending[f.ID]
			p.mu.Unlock()
			if answer != nil {
				r := result{body: f.Body, data: f.data}
				if f.Error != nil {
					r.err = &RemoteError{Code: f.Error.Code, Message: f.Error.Message}
				}
				select {
				case answer <- r:
				default:
				}
			}
		case "cancel":
			if f.ID == 0 {
				p.shutdown(ErrProtocol)
				return
			}
			p.mu.Lock()
			cancel := p.active[f.ID]
			p.mu.Unlock()
			if cancel != nil {
				cancel()
			} else if _, received := seen[f.ID]; !received {
				// Priority cancellation can overtake a queued request frame.
				if len(canceled) >= callsPerDepth*(maxCallDepth+1) {
					p.shutdown(ErrBusy)
					return
				}
				canceled[f.ID] = struct{}{}
			}
		case "request":
			if f.ID == 0 || f.Method == "" || len(f.Method) > 128 || f.Depth < 0 || f.Depth > maxCallDepth {
				p.shutdown(ErrProtocol)
				return
			}
			ctx := context.WithValue(p.ctx, identityKey{}, p.id)
			ctx = context.WithValue(ctx, depthKey{}, f.Depth)
			if len(f.data) != 0 {
				ctx = context.WithValue(ctx, binaryKey{}, f.data)
			}
			var cancel context.CancelFunc
			if f.Deadline != 0 {
				ctx, cancel = context.WithDeadline(ctx, time.Unix(0, f.Deadline))
			} else {
				ctx, cancel = context.WithCancel(ctx)
			}
			p.mu.Lock()
			_, duplicate := p.active[f.ID]
			if !duplicate {
				p.active[f.ID] = cancel
			}
			p.mu.Unlock()
			if duplicate {
				cancel()
				p.shutdown(ErrProtocol)
				return
			}
			if _, received := seen[f.ID]; received {
				cancel()
				p.shutdown(ErrProtocol)
				return
			}
			delete(seen, recent[nextRecent])
			recent[nextRecent] = f.ID
			nextRecent = (nextRecent + 1) % len(recent)
			seen[f.ID] = struct{}{}
			if _, earlier := canceled[f.ID]; earlier {
				delete(canceled, f.ID)
				cancel()
			}
			select {
			case p.requests[f.Depth] <- request{frame: f, ctx: ctx, cancel: cancel}:
			case <-p.done:
				cancel()
				return
			default:
				cancel()
				p.shutdown(ErrBusy)
				return
			}
		default:
			p.shutdown(ErrProtocol)
			return
		}
		f.Body = nil
		f.data = nil
	}
}

func (p *Peer) worker(queue <-chan request) {
	for {
		select {
		case <-p.done:
			return
		case r := <-queue:
			p.handle(r)
		}
	}
}

func (p *Peer) handle(r request) {
	defer r.cancel()
	defer func() {
		p.mu.Lock()
		delete(p.active, r.frame.ID)
		p.mu.Unlock()
	}()
	var value any
	var err error
	select {
	case activeHandlers[r.frame.Depth] <- struct{}{}:
		defer func() { <-activeHandlers[r.frame.Depth] }()
		p.mu.Lock()
		handler := p.handler
		p.mu.Unlock()
		if err = r.ctx.Err(); err == nil {
			if handler == nil {
				err = errors.New("private worker handler is not ready")
			} else {
				value, err = invokeHandler(r.ctx, handler, r.frame.Method, r.frame.Body)
			}
		}
	case <-r.ctx.Done():
		err = r.ctx.Err()
	}
	var payload []byte
	if binary, ok := value.(Binary); ok {
		value, payload = binary.Body, binary.Data
	}
	if err != nil {
		value, payload = nil, nil
	}
	body, marshalErr := marshalBody(value)
	if err == nil {
		err = marshalErr
	}
	if err != nil {
		payload = nil
	}
	f := frame{identity: p.id, Type: "response", ID: r.frame.ID, Body: body, Error: remoteFailure(err), data: payload}
	encoded, encodeErr := encodeFrame(f)
	if errors.Is(encodeErr, ErrTooLarge) {
		f.Body, f.Error, f.data = nil, remoteFailure(encodeErr), nil
		encoded, encodeErr = encodeFrame(f)
	}
	if encodeErr != nil {
		p.shutdown(encodeErr)
		return
	}
	// A canceled request still gets a cancellation response when the peer lives.
	if sendErr := p.sendPrepared(p.ctx, encoded, false); sendErr != nil && !errors.Is(sendErr, context.Canceled) {
		p.shutdown(sendErr)
	}
}

func invokeHandler(ctx context.Context, handler Handler, method string, body json.RawMessage) (value any, err error) {
	defer func() {
		if recover() != nil {
			value, err = nil, errors.New("private worker RPC handler panicked")
		}
	}()
	return handler(ctx, method, body)
}

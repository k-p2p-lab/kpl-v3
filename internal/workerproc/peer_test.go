package workerproc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func peerPair(t *testing.T, left, right func(*Peer) Handler) (*Peer, *Peer) {
	t.Helper()
	lr, rw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	rr, lw, err := os.Pipe()
	if err != nil {
		lr.Close()
		rw.Close()
		t.Fatal(err)
	}
	id := identity{Version: ProtocolVersion, Kind: "fixture", RunID: "run", Epoch: strings.Repeat("a", 48), Build: "fixture"}
	l := newPeer(context.Background(), id, lr, lw, nil)
	r := newPeer(context.Background(), id, rr, rw, nil)
	if left != nil {
		l.mu.Lock()
		l.handler = left(l)
		l.mu.Unlock()
	}
	if right != nil {
		r.mu.Lock()
		r.handler = right(r)
		r.mu.Unlock()
	}
	t.Cleanup(func() {
		l.shutdown(ErrClosed)
		r.shutdown(ErrClosed)
		for _, p := range []*Peer{l, r} {
			select {
			case <-p.stopped:
			case <-time.After(time.Second):
				t.Error("fixture retained RPC workers after close")
			}
		}
	})
	return l, r
}

func TestNestedBidirectionalRPCAndDepthLimit(t *testing.T) {
	factory := func(p *Peer) Handler {
		return func(ctx context.Context, method string, body json.RawMessage) (any, error) {
			var n int
			if err := json.Unmarshal(body, &n); err != nil {
				return nil, err
			}
			if method == "echo" || n == 0 {
				return n, nil
			}
			var result int
			if err := p.Call(ctx, "nested", n-1, &result); err != nil {
				return nil, err
			}
			return result + 1, nil
		}
	}
	l, _ := peerPair(t, factory, factory)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range callsPerDepth {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var result int
			if err := l.Call(ctx, "nested", maxCallDepth, &result); err != nil || result != maxCallDepth {
				t.Errorf("nested RPC deadlocked or changed result: %d %v", result, err)
			}
		}()
	}
	wg.Wait()
	if err := l.Call(ctx, "nested", maxCallDepth+1, nil); !errors.Is(err, ErrDepth) {
		t.Fatalf("unbounded recursive RPC: %v", err)
	}
	var result int
	if err := l.Call(ctx, "echo", 11, &result); err != nil || result != 11 {
		t.Fatalf("depth failure poisoned RPC: %d %v", result, err)
	}
}

func TestLongExecuteDoesNotBlockCancellationRPC(t *testing.T) {
	started := make(chan struct{})
	var cancelRun context.CancelFunc
	var mu sync.Mutex
	l, _ := peerPair(t, nil, func(*Peer) Handler {
		return func(ctx context.Context, method string, _ json.RawMessage) (any, error) {
			if method == "execute" {
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				mu.Lock()
				cancelRun = cancel
				mu.Unlock()
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			mu.Lock()
			cancel := cancelRun
			mu.Unlock()
			if cancel != nil {
				cancel()
			}
			return true, nil
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- l.Call(ctx, "execute", nil, nil) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("execute did not start")
	}
	if err := l.Call(ctx, "cancel", nil, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancel semantics: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("long execute serialized cancellation")
	}
}

func TestNestedCapacityAcrossConcurrentPeers(t *testing.T) {
	gate := make(chan struct{})
	var arrived atomic.Int32
	factory := func(p *Peer) Handler {
		return func(ctx context.Context, _ string, body json.RawMessage) (any, error) {
			var n int
			if err := json.Unmarshal(body, &n); err != nil {
				return nil, err
			}
			if n == 0 {
				return 0, nil
			}
			if n == 1 {
				if arrived.Add(1) == 4*workersPerDepth {
					close(gate)
				}
				select {
				case <-gate:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			var result int
			if err := p.Call(ctx, "nested", n-1, &result); err != nil {
				return nil, err
			}
			return result + 1, nil
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 4 {
		p, _ := peerPair(t, factory, factory)
		for range workersPerDepth {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var result int
				if err := p.Call(ctx, "nested", 4, &result); err != nil || result != 4 {
					t.Errorf("cross-peer nested capacity deadlock: result=%d err=%v", result, err)
				}
			}()
		}
	}
	wg.Wait()
}

func TestCancellationCanArriveBeforeRequestFrame(t *testing.T) {
	r, remoteWrite, _ := os.Pipe()
	remoteRead, w, _ := os.Pipe()
	defer remoteWrite.Close()
	defer remoteRead.Close()
	id := identity{Version: ProtocolVersion, Kind: "fixture", RunID: "run", Epoch: "epoch", Build: "build"}
	var invoked atomic.Int32
	p := newPeer(t.Context(), id, r, w, func(context.Context, string, json.RawMessage) (any, error) { invoked.Add(1); return nil, nil })
	defer p.shutdown(ErrClosed)
	for _, f := range []frame{{identity: id, Type: "cancel", ID: 7}, {identity: id, Type: "request", ID: 7, Method: "execute", Body: json.RawMessage(`null`)}} {
		data, err := encodeFrame(f)
		if err != nil {
			t.Fatal(err)
		}
		if err := writeFrame(remoteWrite, data); err != nil {
			t.Fatal(err)
		}
	}
	remoteRead.SetReadDeadline(time.Now().Add(time.Second))
	response, err := readFrame(remoteRead)
	if err != nil || response.Error == nil || response.Error.Code != "canceled" || invoked.Load() != 0 {
		t.Fatalf("early cancellation lost: response=%+v invoked=%d err=%v", response, invoked.Load(), err)
	}
}

type forbiddenMarshal struct{ called *atomic.Bool }

func (v forbiddenMarshal) MarshalJSON() ([]byte, error) {
	v.called.Store(true)
	return []byte(`null`), nil
}

func TestCanceledCallDoesNotSerializeAndIdentityIsVerified(t *testing.T) {
	l, _ := peerPair(t, nil, func(*Peer) Handler {
		return func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
			id, ok := IdentityFromContext(ctx)
			if !ok {
				return nil, errors.New("missing verified identity")
			}
			return id, nil
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var called atomic.Bool
	if err := l.Call(ctx, "identity", forbiddenMarshal{&called}, nil); !errors.Is(err, context.Canceled) || called.Load() {
		t.Fatalf("canceled Call serialized its input: called=%v err=%v", called.Load(), err)
	}
	var id Identity
	if err := l.Call(t.Context(), "identity", nil, &id); err != nil || id.RunID != "run" || id.Epoch != strings.Repeat("a", 48) {
		t.Fatalf("unverified handler identity: %+v %v", id, err)
	}
}

func TestFrameSizeLimitsAndIdentityMismatch(t *testing.T) {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], MaxMessageBytes+1)
	if _, err := readFrame(strings.NewReader(string(header[:]))); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize header accepted: %v", err)
	}
	l, _ := peerPair(t, nil, func(*Peer) Handler {
		return func(_ context.Context, method string, body json.RawMessage) (any, error) {
			if method == "large" {
				return strings.Repeat("x", MaxMessageBytes), nil
			}
			return body, nil
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := l.Call(ctx, "echo", strings.Repeat("x", MaxMessageBytes), nil); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize outbound accepted: %v", err)
	}
	if err := l.Call(ctx, "large", nil, nil); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize response accepted: %v", err)
	}
	var value int
	if err := l.Call(ctx, "echo", 9, &value); err != nil || value != 9 {
		t.Fatalf("oversize message broke transport: %d %v", value, err)
	}
	r, remoteWrite, _ := os.Pipe()
	remoteRead, w, _ := os.Pipe()
	defer remoteWrite.Close()
	defer remoteRead.Close()
	p := newPeer(t.Context(), identity{Version: 1, Epoch: "expected"}, r, w, nil)
	defer p.shutdown(ErrClosed)
	data, _ := encodeFrame(frame{identity: identity{Version: 1, Epoch: "stale"}, Type: "ready"})
	if err := writeFrame(remoteWrite, data); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.Done():
	case <-ctx.Done():
		t.Fatal("stale worker identity accepted")
	}
	if !errors.Is(p.connectionError(), ErrProtocol) {
		t.Fatal(p.connectionError())
	}
}

func TestSlowHandlersStayBoundedAndCallsCancelOnClose(t *testing.T) {
	release := make(chan struct{})
	var active, peak atomic.Int32
	l, r := peerPair(t, nil, func(*Peer) Handler {
		return func(context.Context, string, json.RawMessage) (any, error) {
			n := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
			}
			<-release
			return nil, nil
		}
	})
	defer close(release)
	var wg sync.WaitGroup
	for range 2 * callsPerDepth {
		wg.Add(1)
		go func() { defer wg.Done(); _ = l.Call(t.Context(), "slow", nil, nil) }()
	}
	deadline := time.Now().Add(time.Second)
	for active.Load() < workersPerDepth && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if peak.Load() != workersPerDepth {
		t.Fatalf("unexpected handler concurrency %d", peak.Load())
	}
	l.shutdown(ErrClosed)
	r.shutdown(ErrClosed)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("closed peer retained pending calls")
	}
}

func TestPrivateEnvironmentAndOperatorGuard(t *testing.T) {
	env := privateEnvironment([]string{"PATH=/fixture", "LANG=C", "GOMAXPROCS=2", "KPL_PASSWORD=fake", "AWS_SECRET_ACCESS_KEY=fake", "DOCKER_AUTH_CONFIG=fake", "SSH_AUTH_SOCK=/fake", "HOME=/fake", "UNRELATED=fake"})
	for _, item := range env {
		key, _, _ := strings.Cut(item, "=")
		if key != "PATH" && key != "LANG" && key != "GOMAXPROCS" && key != privateMarker {
			t.Fatalf("unexpected inherited key %s", key)
		}
	}
	t.Setenv(privateMarker, "")
	if err := Serve(t.Context(), "fixture", func(context.Context, string, json.RawMessage, *Peer) (Handler, error) {
		t.Fatal("operator invocation called factory")
		return nil, nil
	}); !errors.Is(err, ErrPrivateRole) {
		t.Fatal(err)
	}
	if _, err := readFrame(io.LimitReader(strings.NewReader("x"), 1)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated header accepted: %v", err)
	}
}

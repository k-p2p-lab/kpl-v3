package peer

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	corehost "github.com/libp2p/go-libp2p/core/host"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
)

type topologyConnectHookHost struct {
	corehost.Host
	connect func(context.Context, corepeer.AddrInfo) error
}

func (h *topologyConnectHookHost) Connect(ctx context.Context, info corepeer.AddrInfo) error {
	return h.connect(ctx, info)
}

func TestTopologyPrepareRecoversInitialDialBackoffWithoutFreezing(t *testing.T) {
	s := newTopologyTestServer(t, "target")
	neighbor := newTopologyTestServer(t, "neighbor")
	unrelated := newTopologyTestServer(t, "unrelated")
	network := s.host.Network().(*swarm.Swarm)
	for _, target := range []*Server{neighbor, unrelated} {
		for _, address := range target.host.Addrs() {
			network.Backoff().AddBackoff(target.host.ID(), address)
		}
	}
	originalHost := s.host
	var calls atomic.Int32
	s.host = &topologyConnectHookHost{Host: originalHost, connect: func(ctx context.Context, info corepeer.AddrInfo) error {
		attempt := calls.Add(1)
		err := originalHost.Connect(ctx, info)
		if attempt == 1 && !errors.Is(err, swarm.ErrDialBackoff) {
			t.Errorf("first attempt did not encounter the existing dial backoff: %v", err)
		}
		return err
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	request := topologyTestRequest(s, "prepare", neighbor)
	w := topologyTestHTTP(t, ctx, s, request)
	var response model.TopologyResponse
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.Validate(request, s.config.Node.ID, s.host.ID().String()) != nil {
		t.Fatalf("prepare did not recover from existing backoff: %d %s", w.Code, w.Body)
	}
	if calls.Load() != 2 {
		t.Fatalf("prepare needed %d connection attempts, want two", calls.Load())
	}
	snapshot, err := s.pubsub.MeshFreezeSnapshot(ctx)
	if err != nil || snapshot.Frozen || response.Frozen || s.meshFrozen.Load() {
		t.Fatalf("prepare froze the mesh: snapshot=%+v response=%+v error=%v", snapshot, response, err)
	}
	for _, address := range unrelated.host.Addrs() {
		if !network.Backoff().Backoff(unrelated.host.ID(), address) {
			t.Fatal("topology preparation cleared an unrelated peer's backoff")
		}
	}
}

func TestTopologyPrepareRetriesTransientTransportFailure(t *testing.T) {
	s, neighbor := newTopologyTestServer(t, "target"), newTopologyTestServer(t, "neighbor")
	originalHost := s.host
	var calls atomic.Int32
	s.host = &topologyConnectHookHost{Host: originalHost, connect: func(ctx context.Context, info corepeer.AddrInfo) error {
		if calls.Add(1) == 1 {
			return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		}
		return originalHost.Connect(ctx, info)
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	w := topologyTestHTTP(t, ctx, s, topologyTestRequest(s, "prepare", neighbor))
	if w.Code != http.StatusOK || calls.Load() != 2 || s.meshFrozen.Load() {
		t.Fatalf("transient failure did not recover: status=%d calls=%d body=%s", w.Code, calls.Load(), w.Body)
	}
}

func TestTopologyConnectCancellationDuringBackoffStopsRetry(t *testing.T) {
	for _, reason := range []string{"cancel", "deadline"} {
		t.Run(reason, func(t *testing.T) {
			s, neighbor := newTopologyTestServer(t, "target"), newTopologyTestServer(t, "neighbor")
			info := corepeer.AddrInfo{ID: neighbor.host.ID(), Addrs: neighbor.host.Addrs()}
			network := s.host.Network().(*swarm.Swarm)
			network.Backoff().AddBackoff(info.ID, info.Addrs[0])
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Millisecond)
			defer cancel()
			wantErr := context.DeadlineExceeded
			var cancelTimer *time.Timer
			var calls atomic.Int32
			s.host = &topologyConnectHookHost{Host: s.host, connect: func(context.Context, corepeer.AddrInfo) error {
				if calls.Add(1) == 1 && reason == "cancel" {
					cancelTimer = time.AfterFunc(10*time.Millisecond, cancel)
				}
				return swarm.ErrDialBackoff
			}}
			if reason == "cancel" {
				wantErr = context.Canceled
			}
			err := s.connectTopologyPeer(ctx, info)
			if cancelTimer != nil {
				cancelTimer.Stop()
			}
			if !errors.Is(err, wantErr) || calls.Load() != 1 {
				t.Fatalf("cancellation allowed another dial or lost its cause: calls=%d error=%v", calls.Load(), err)
			}
			if !network.Backoff().Backoff(info.ID, info.Addrs[0]) {
				t.Fatal("cancellation during the retry wait still cleared backoff")
			}
		})
	}
}

func TestTopologyConnectKeepsTransportFailureAndDeadline(t *testing.T) {
	s, neighbor := newTopologyTestServer(t, "target"), newTopologyTestServer(t, "neighbor")
	var calls atomic.Int32
	s.host = &topologyConnectHookHost{Host: s.host, connect: func(context.Context, corepeer.AddrInfo) error {
		if calls.Add(1) == 1 {
			return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		}
		return swarm.ErrDialBackoff
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 450*time.Millisecond)
	defer cancel()
	err := s.connectTopologyPeer(ctx, corepeer.AddrInfo{ID: neighbor.host.ID(), Addrs: neighbor.host.Addrs()})
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, syscall.ECONNREFUSED) || calls.Load() < 2 {
		t.Fatalf("later backoff hid the transport failure or command deadline: calls=%d error=%v", calls.Load(), err)
	}
}

func TestTopologyConnectDoesNotClearBackoffAfterFreshDialFails(t *testing.T) {
	s, neighbor := newTopologyTestServer(t, "target"), newTopologyTestServer(t, "neighbor")
	info := corepeer.AddrInfo{ID: neighbor.host.ID(), Addrs: neighbor.host.Addrs()}
	network := s.host.Network().(*swarm.Swarm)
	network.Backoff().AddBackoff(info.ID, info.Addrs[0])
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	var calls atomic.Int32
	s.host = &topologyConnectHookHost{Host: s.host, connect: func(context.Context, corepeer.AddrInfo) error {
		switch calls.Add(1) {
		case 1:
			return swarm.ErrDialBackoff
		case 2:
			if network.Backoff().Backoff(info.ID, info.Addrs[0]) {
				t.Error("the initial backoff was not cleared before the fresh attempt")
			}
			network.Backoff().AddBackoff(info.ID, info.Addrs[0])
			return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		default:
			if !network.Backoff().Backoff(info.ID, info.Addrs[0]) {
				t.Error("the fresh failure's backoff was cleared")
			}
			cancel()
			return swarm.ErrDialBackoff
		}
	}}
	err := s.connectTopologyPeer(ctx, info)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, syscall.ECONNREFUSED) || calls.Load() != 3 {
		t.Fatalf("fresh failure did not retain its backoff and cause: calls=%d error=%v", calls.Load(), err)
	}
}

func TestTopologyConnectPeersLimitsConcurrentDials(t *testing.T) {
	s, neighbor := newTopologyTestServer(t, "target"), newTopologyTestServer(t, "neighbor")
	infos := make([]corepeer.AddrInfo, 9)
	for i := range infos {
		infos[i] = corepeer.AddrInfo{ID: neighbor.host.ID(), Addrs: neighbor.host.Addrs()}
	}
	entered := make(chan struct{}, len(infos))
	release := make(chan struct{})
	var calls, active, peak atomic.Int32
	s.host = &topologyConnectHookHost{Host: s.host, connect: func(ctx context.Context, _ corepeer.AddrInfo) error {
		calls.Add(1)
		current := active.Add(1)
		defer active.Add(-1)
		for before := peak.Load(); current > before && !peak.CompareAndSwap(before, current); before = peak.Load() {
		}
		entered <- struct{}{}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.connectTopologyPeers(ctx, infos) }()
	for range 4 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("the first four connection workers did not start")
		}
	}
	select {
	case <-entered:
		t.Error("more than four peers dialed concurrently")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil || calls.Load() != int32(len(infos)) || peak.Load() != 4 || active.Load() != 0 {
			t.Fatalf("unbounded or incomplete preparation: calls=%d active=%d peak=%d error=%v", calls.Load(), active.Load(), peak.Load(), err)
		}
	case <-ctx.Done():
		t.Fatal("connection workers did not finish")
	}
}

func TestTopologyConnectPeersCancellationDoesNotDialQueuedNeighbors(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	entered := make(chan struct{}, 8)
	var calls, active atomic.Int32
	s := &Server{host: &topologyConnectHookHost{connect: func(ctx context.Context, _ corepeer.AddrInfo) error {
		calls.Add(1)
		active.Add(1)
		defer active.Add(-1)
		entered <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}}}
	done := make(chan error, 1)
	go func() { done <- s.connectTopologyPeers(ctx, make([]corepeer.AddrInfo, 8)) }()
	for range 4 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("the first four connection workers did not start")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || calls.Load() != 4 || active.Load() != 0 {
			t.Fatalf("cancellation dialed queued neighbors or retained workers: calls=%d active=%d error=%v", calls.Load(), active.Load(), err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not stop the connection workers")
	}
}

func TestTopologyConnectDoesNotRetryPermanentDialErrors(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{"self", swarm.ErrDialToSelf},
		{"no addresses", swarm.ErrNoAddresses},
		{"connection gater", &swarm.DialError{Cause: swarm.ErrGaterDisallowedConnection}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var calls atomic.Int32
			s := &Server{host: &topologyConnectHookHost{connect: func(context.Context, corepeer.AddrInfo) error {
				calls.Add(1)
				return testCase.err
			}}}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err := s.connectTopologyPeer(ctx, corepeer.AddrInfo{})
			if !errors.Is(err, testCase.err) || errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
				t.Fatalf("permanent connection error was retried: calls=%d error=%v", calls.Load(), err)
			}
		})
	}
}

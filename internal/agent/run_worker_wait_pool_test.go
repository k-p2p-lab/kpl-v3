package agent

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRunProcessResetClosesIdleDockerWaitSockets(t *testing.T) {
	const count = 8
	entered, release := make(chan struct{}, count), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	changed := make(chan struct{}, 1)
	var mu sync.Mutex
	connections := make(map[net.Conn]http.ConnState)
	endpoint := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte(`{"StatusCode":0}`))
	}))
	endpoint.Config.ConnState = func(connection net.Conn, state http.ConnState) {
		mu.Lock()
		if state == http.StateClosed {
			delete(connections, connection)
		} else {
			connections[connection] = state
		}
		mu.Unlock()
		select {
		case changed <- struct{}{}:
		default:
		}
	}
	endpoint.Start()
	t.Cleanup(endpoint.Close)
	s, _ := newContainerTestServer(t, nil)
	s.config.RunWorkerFactory = agentRunProcessFactory()
	workerControllerFixture(t, s)
	transport := &http.Transport{MaxIdleConns: count, MaxIdleConnsPerHost: count, IdleConnTimeout: time.Minute}
	t.Cleanup(transport.CloseIdleConnections)
	s.docker.waitClient = &http.Client{Transport: transport}
	s.docker.waitURL = endpoint.URL
	worker, err := s.ensureRunWorker(t.Context(), "wait-socket-run")
	if err != nil {
		t.Fatal(err)
	}
	errors := make(chan error, count)
	for range count {
		go func() { errors <- s.docker.wait(t.Context(), strings.Repeat("a", 64)) }()
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for range count {
		select {
		case <-entered:
		case <-timer.C:
			t.Fatal("wait calls did not establish all real HTTP connections")
		}
	}
	releaseOnce.Do(func() { close(release) })
	for range count {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	waitConnections := func(expected int) {
		t.Helper()
		deadline := time.NewTimer(3 * time.Second)
		defer deadline.Stop()
		for {
			mu.Lock()
			idle, open := 0, len(connections)
			for _, state := range connections {
				if state == http.StateIdle {
					idle++
				}
			}
			mu.Unlock()
			if open == expected && idle == expected {
				return
			}
			select {
			case <-changed:
			case <-deadline.C:
				t.Fatalf("Docker wait sockets did not reach baseline: open=%d idle=%d expected=%d", open, idle, expected)
			}
		}
	}
	waitConnections(count)
	if active := s.docker.activeWaits.Load(); active != 0 {
		t.Fatalf("idle sockets test still has active wait calls: %d", active)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := s.finishRunWorker(ctx, "wait-socket-run"); err != nil {
		t.Fatal(err)
	}
	waitConnections(0)
	select {
	case <-worker.process.Done():
	default:
		t.Fatal("Run reset returned before its OS worker was reaped")
	}
	if proof, err := s.proveRunReset("wait-socket-run"); err != nil || proof.PeerWaitConnections != 0 {
		t.Fatalf("wait pool reset did not preserve the normal resource proof: %+v %v", proof, err)
	}
}

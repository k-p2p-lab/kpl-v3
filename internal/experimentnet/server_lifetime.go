package experimentnet

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// Join within the existing preparation budget and the stack's four-minute
// stop grace. A failed join must not be reported as a clean role exit.
const networkServiceShutdownTimeout = 3 * time.Minute

type networkHandlerLifetime struct {
	handler  http.Handler
	mu       sync.Mutex
	active   int
	stopping bool
	done     chan struct{}
}

func (l *networkHandlerLifetime) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.mu.Lock()
	if l.stopping {
		l.mu.Unlock()
		http.Error(w, "network service is stopping", http.StatusServiceUnavailable)
		return
	}
	l.active++
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.active--
		if l.stopping && l.active == 0 {
			close(l.done)
		}
		l.mu.Unlock()
	}()
	l.handler.ServeHTTP(w, r)
}

func (l *networkHandlerLifetime) stop() {
	l.mu.Lock()
	if !l.stopping {
		l.stopping = true
		if l.active == 0 {
			close(l.done)
		}
	}
	l.mu.Unlock()
}

func (l *networkHandlerLifetime) wait(ctx context.Context) error {
	select {
	case <-l.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

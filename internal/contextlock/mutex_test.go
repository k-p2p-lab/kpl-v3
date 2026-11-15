package contextlock

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

type waitingContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *waitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

type testMutex interface {
	mutex
	Lock()
}

func TestLockCancellationDoesNotLeaveLateOwner(t *testing.T) {
	for name, newMutex := range map[string]func() testMutex{"standard": func() testMutex { return &sync.Mutex{} }, "queued": func() testMutex { return &Mutex{} }} {
		t.Run(name, func(t *testing.T) {
			mu := newMutex()
			mu.Lock()
			locked := true
			defer func() {
				if locked {
					mu.Unlock()
				}
			}()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wait := &waitingContext{Context: ctx, waiting: make(chan struct{})}
			result := make(chan error, 1)
			go func() { result <- Lock(wait, mu) }()
			<-wait.waiting
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation lost: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("canceled waiter still owns a pending acquisition")
			}
			mu.Unlock()
			locked = false
			if !mu.TryLock() {
				t.Fatal("canceled acquisition stole a later owner's permit")
			}
			mu.Unlock()
		})
	}
}

func TestLockNativeQueueKeepsOrdinaryAndContextWaiterOrder(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	var mu Mutex
	mu.Lock()
	locked := true
	defer func() {
		if locked {
			mu.Unlock()
		}
	}()
	canceled, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := &waitingContext{Context: canceled, waiting: make(chan struct{})}
	canceledResult := make(chan error, 1)
	go func() { canceledResult <- Lock(first, &mu) }()
	<-first.waiting
	runtime.Gosched()
	order := make(chan int, 3)
	started := make(chan struct{})
	go func() { close(started); mu.Lock(); order <- 2; mu.Unlock() }()
	<-started
	runtime.Gosched()
	third := &waitingContext{Context: t.Context(), waiting: make(chan struct{})}
	go func() {
		if err := Lock(third, &mu); err != nil {
			order <- -3
			return
		}
		order <- 3
		mu.Unlock()
	}()
	<-third.waiting
	runtime.Gosched()
	started = make(chan struct{})
	go func() { close(started); mu.Lock(); order <- 4; mu.Unlock() }()
	<-started
	runtime.Gosched()
	cancel()
	if err := <-canceledResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued cancellation lost: %v", err)
	}
	mu.Unlock()
	locked = false
	for _, expected := range []int{2, 3, 4} {
		select {
		case got := <-order:
			if got != expected {
				t.Fatalf("mixed queue order=%d, want=%d", got, expected)
			}
		case <-time.After(time.Second):
			t.Fatal("canceled queue head lost a remaining permit")
		}
	}
	if !mu.TryLock() {
		t.Fatal("FIFO completion leaked a permit")
	}
	mu.Unlock()
}

type cancelAfterNativeAcquire struct {
	sync.Mutex
	cancel context.CancelFunc
}

func (m *cancelAfterNativeAcquire) LockContext(context.Context) error {
	m.Mutex.Lock()
	m.cancel()
	return nil
}
func TestLockRechecksCancellationAfterNativeAcquire(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	mu := &cancelAfterNativeAcquire{cancel: cancel}
	if err := Lock(ctx, mu); !errors.Is(err, context.Canceled) {
		t.Fatalf("native acquisition hid cancellation: %v", err)
	}
	if !mu.TryLock() {
		t.Fatal("native post-acquire cancellation retained a permit")
	}
	mu.Unlock()
}

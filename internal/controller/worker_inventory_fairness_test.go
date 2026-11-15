package controller

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

type inventoryWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *inventoryWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

// A periodic inventory arrives before a later wave of mandatory created-node
// updates. Cancellation must not turn that reader into a polling barger that
// loses every handoff to writers arriving after it and lets Agent leases age.
func TestWorkerInventoryWaiterIsNotStarvedByLaterWriters(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	client := &runWorkerClient{}
	client.syncMu.Lock()
	var writers sync.WaitGroup
	firstWaiting := make(chan struct{})
	writers.Add(1)
	go func() {
		defer writers.Done()
		close(firstWaiting)
		client.syncMu.Lock()
		time.Sleep(10 * time.Millisecond)
		client.syncMu.Unlock()
	}()
	<-firstWaiting
	time.Sleep(5 * time.Millisecond)
	client.syncMu.Unlock()
	// A standard mutex permits this owner to barge ahead of the waiting
	// writer. Yield while holding it so that writer enters fair handoff mode.
	// A queued mutex instead lets the earlier writer finish before this Lock.
	client.syncMu.Lock()
	runtime.Gosched()
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	wait := &inventoryWaitContext{Context: ctx, waiting: make(chan struct{})}
	observed := make(chan error, 1)
	go func() {
		err := client.lockInventory(wait)
		if err == nil {
			client.syncMu.Unlock()
		}
		observed <- err
	}()
	<-wait.waiting
	runtime.Gosched()
	started := make(chan struct{}, 64)
	for range 64 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			started <- struct{}{}
			client.syncMu.Lock()
			time.Sleep(10 * time.Millisecond)
			client.syncMu.Unlock()
		}()
	}
	for range 64 {
		<-started
	}
	client.syncMu.Unlock()
	err := <-observed
	writers.Wait()
	if err != nil {
		t.Fatalf("earlier inventory waiter was starved by later created-node writers: %v", err)
	}
}

package contextlock

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

// Mutex serializes cancelable and ordinary callers through one FIFO queue.
// Its zero value is ready for use. It must not be copied after first use.
// Unlike polling a standard mutex, a queued reader is not repeatedly passed
// over by a continuous stream of ordinary Lock callers.
type Mutex struct {
	once sync.Once
	sem  *semaphore.Weighted
}

func (m *Mutex) queue() *semaphore.Weighted {
	m.once.Do(func() { m.sem = semaphore.NewWeighted(1) })
	return m.sem
}

func (m *Mutex) LockContext(ctx context.Context) error {
	queue := m.queue()
	if err := queue.Acquire(ctx, 1); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		queue.Release(1)
		return err
	}
	return nil
}

func (m *Mutex) Lock() {
	if err := m.LockContext(context.Background()); err != nil {
		panic(err)
	}
}

func (m *Mutex) TryLock() bool { return m.queue().TryAcquire(1) }
func (m *Mutex) Unlock()       { m.queue().Release(1) }

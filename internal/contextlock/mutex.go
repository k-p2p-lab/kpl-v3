// Package contextlock preserves request cancellation while waiting for a
// mutex that serializes longer lifecycle or I/O operations.
package contextlock

import (
	"context"
	"time"
)

type mutex interface {
	TryLock() bool
	Unlock()
}

// Lock acquires mu only while ctx remains active. One ticker bounds contention
// without a busy loop or a goroutine that could acquire mu after the caller
// has returned. Other owners can continue using the mutex's Lock and Unlock.
// Native context-aware mutexes retain their own wait queue rather than polling.
func Lock(ctx context.Context, mu mutex) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if native, ok := mu.(interface{ LockContext(context.Context) error }); ok {
		if err := native.LockContext(ctx); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			mu.Unlock()
			return err
		}
		return nil
	}
	try := func() (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !mu.TryLock() {
			return false, nil
		}
		if err := ctx.Err(); err != nil {
			mu.Unlock()
			return false, err
		}
		return true, nil
	}
	if locked, err := try(); locked || err != nil {
		return err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if locked, err := try(); locked || err != nil {
				return err
			}
		}
	}
}

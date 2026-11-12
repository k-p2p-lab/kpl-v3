//go:build linux

package master

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"
)

// Retain the lock inode after release. Kernel ownership disappears on process
// exit, so a crash cannot leave a stale directory lock that blocks a retry.
func lockAnalysisRetirementJournal(ctx context.Context, path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*os.File, error) { _ = file.Close(); return nil, err }
	info, err := file.Stat()
	if err != nil {
		return fail(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return fail(errors.New("analysis retirement lock must be a private regular file"))
	}
	for {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) && !errors.Is(err, syscall.EINTR) {
			return fail(err)
		}
		if err := swarmSleep(ctx, 10*time.Millisecond); err != nil {
			return fail(err)
		}
	}
}

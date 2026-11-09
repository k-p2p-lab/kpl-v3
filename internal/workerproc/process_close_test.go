package workerproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"testing"
)

func TestNormalCloseAcceptsPipeDisconnectAndPreservesFaults(t *testing.T) {
	for _, err := range []error{io.EOF, io.ErrClosedPipe, syscall.EPIPE, &os.PathError{Op: "write", Path: "private-worker-out", Err: syscall.EPIPE}, fmt.Errorf("callback ended: %w", io.ErrClosedPipe), context.Canceled, ErrClosed, os.ErrClosed} {
		if got := normalClose(err); got != nil {
			t.Fatalf("ordinary private pipe disconnect became a worker failure: %v", got)
		}
	}
	for _, err := range []error{ErrProtocol, ErrTooLarge, ErrBusy, ErrDepth, io.ErrUnexpectedEOF, context.DeadlineExceeded, os.ErrPermission, errors.New("worker application failed")} {
		wrapped := fmt.Errorf("worker failed: %w", err)
		if got := normalClose(wrapped); got != wrapped {
			t.Fatalf("worker/protocol failure was hidden: %v -> %v", wrapped, got)
		}
	}
}

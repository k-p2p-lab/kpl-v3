package controller

import (
	"context"
	"io"
	"net/http"
	"time"
)

// Bound an individual write, then clear the deadline before waiting for the
// next update. HTTP/2 otherwise resets an idle stream after the write timeout.
func writeStreamEvent(ctx context.Context, w http.ResponseWriter, event string, data []byte) error {
	response := http.NewResponseController(w)
	_ = response.SetWriteDeadline(time.Now().Add(10 * time.Second))
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = response.SetWriteDeadline(time.Now())
		close(canceled)
	})
	defer func() {
		// Don't let an already-running cancellation callback reapply an expired
		// deadline after cleanup (especially on a reused HTTP/1 connection).
		if !stop() {
			<-canceled
		}
		_ = response.SetWriteDeadline(time.Time{})
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := io.WriteString(w, "event: "+event+"\ndata: "); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	if _, err := io.WriteString(w, "\n\n"); err != nil {
		return err
	}
	return response.Flush()
}

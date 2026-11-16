package controller

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func waitStorageCancellationBoundary(t *testing.T, symbol string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		stacks := make([]byte, 2<<20)
		n := runtime.Stack(stacks, true)
		if bytes.Contains(stacks[:n], []byte(symbol)) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("storage operation did not enter %s", symbol)
}

func TestArchivedReadAdmissionHonorsCanceledMetadataWait(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "archived-read-admission-cancel"
	resultFixture(t, s, id, "completed", time.Now())
	var nasReads atomic.Int32
	s.archiveIOCheck = func() { nasReads.Add(1) }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s.state.persistMu.Lock()
	locked := true
	done := make(chan error, 1)
	returned := false
	defer func() {
		cancel()
		if locked {
			s.state.persistMu.Unlock()
		}
		if !returned {
			<-done
		}
	}()
	go func() {
		release, err := s.resolveArchivedFiles(ctx, id, []resultFile{{name: "events.jsonl", size: 1, remote: &storedRunFile{Object: "events.jsonl", Size: 1}}})
		if release != nil {
			release()
		}
		done <- err
	}()
	waitStorageCancellationBoundary(t, "(*Server).resolveArchivedRead")
	cancel()
	select {
	case err := <-done:
		returned = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("archived read lost admission cancellation: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("canceled archived read waited for unrelated durable metadata")
	}
	if nasReads.Load() != 0 || len(s.archiveReadSlots) != 0 || len(s.resultReadPins) != 0 {
		t.Fatalf("canceled read began NAS I/O or retained ownership: reads=%d slots=%d pins=%v", nasReads.Load(), len(s.archiveReadSlots), s.resultReadPins)
	}
	s.state.persistMu.Unlock()
	locked = false
}

func TestResultArchiveCachePublicationHonorsCanceledHTTPHead(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "archive-head-publication-cancel"
	resultFixture(t, s, id, "completed", time.Now())
	for range cap(s.resultArchiveSlots) {
		s.resultArchiveSlots <- struct{}{}
	}
	gateHeld, metadataHeld := true, false
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan struct{}, 1)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { finished <- struct{}{} }()
		s.handleResultDownload(w, r, id)
	}))
	defer func() {
		cancel()
		if gateHeld {
			for range cap(s.resultArchiveSlots) {
				<-s.resultArchiveSlots
			}
		}
		if metadataHeld {
			s.state.persistMu.Unlock()
		}
		endpoint.Close()
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, endpoint.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	clientDone := make(chan error, 1)
	go func() {
		response, err := endpoint.Client().Do(req)
		if response != nil {
			response.Body.Close()
		}
		clientDone <- err
	}()
	waitStorageCancellationBoundary(t, "(*Server).acquireResultArchiveSlot")
	s.state.persistMu.Lock()
	metadataHeld = true
	for range cap(s.resultArchiveSlots) {
		<-s.resultArchiveSlots
	}
	gateHeld = false
	waitStorageCancellationBoundary(t, "(*Server).finishResultArchiveFlight")
	cancel()
	if err := <-clientDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("HTTP client lost cancellation: %v", err)
	}
	select {
	case <-finished:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("canceled HEAD retained its cache flight behind metadata persistence")
	}
	s.resultArchiveMu.Lock()
	flights, cached := len(s.resultArchiveFlights), len(s.resultArchives)
	s.resultArchiveMu.Unlock()
	if flights != 0 || cached != 0 || len(s.resultArchiveSlots) != 0 {
		t.Fatalf("canceled HEAD retained a flight, late cache or ZIP slot: flights=%d cached=%d slots=%d", flights, cached, len(s.resultArchiveSlots))
	}
	s.state.persistMu.Unlock()
	metadataHeld = false
	// Releasing the canceled flight must allow a new ordinary HEAD and GET.
	for _, method := range []string{http.MethodHead, http.MethodGet} {
		request, _ := http.NewRequestWithContext(t.Context(), method, endpoint.URL, nil)
		response, err := endpoint.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		<-finished
		if response.StatusCode != http.StatusOK || readErr != nil || response.ContentLength <= 0 || method == http.MethodGet && int64(len(data)) != response.ContentLength {
			t.Fatalf("new %s did not recover normal archive streaming: status=%d length=%d bytes=%d err=%v", method, response.StatusCode, response.ContentLength, len(data), readErr)
		}
	}
}

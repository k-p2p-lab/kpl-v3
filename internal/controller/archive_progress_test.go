package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func archiveOverviewForTest(t *testing.T, s *Server) resultStorageOverview {
	t.Helper()
	w := httptest.NewRecorder()
	s.handleResultStorage(w, httptest.NewRequest("GET", "/api/v1/result-storage", nil))
	if w.Code != 200 {
		t.Fatalf("storage status: %d %s", w.Code, w.Body)
	}
	var status resultStorageOverview
	if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestArchiveStallUsesProgressInsteadOfCycleDuration(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.archiveIOCheck = func() { t.Fatal("storage status probed NAS") }
	s.archiveChecking = true
	s.archiveCheckStartedAt = time.Now().Add(-time.Hour)
	s.archivePhase = "copying"
	s.archiveLastProgressAt = time.Now()
	if status := archiveOverviewForTest(t, s); status.Stalled || status.Phase != "copying" {
		t.Fatalf("long healthy transfer marked stalled: %+v", status)
	}
	for _, phase := range []string{"discovering", "copying", "verifying", "publishing", "deleting", "preparing", "paused", "idle"} {
		s.archivePhase = phase
		s.archiveLastProgressAt = time.Now().Add(-time.Minute)
		status := archiveOverviewForTest(t, s)
		want := phase != "preparing" && phase != "paused" && phase != "idle"
		if status.Stalled != want {
			t.Fatalf("phase %s: stalled=%v, want %v", phase, status.Stalled, want)
		}
	}
	s.archiveChecking = false
	s.archivePhase = "copying"
	if archiveOverviewForTest(t, s).Stalled {
		t.Fatal("inactive worker reported stalled")
	}
}

func TestArchiveLoopDistinguishesBlockedNASFromExperimentPause(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.state.experiments["active"] = model.Experiment{ID: "active", State: "running"}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	s.archiveIOCheck = func() { enteredOnce.Do(func() { close(entered); <-release }) }
	ctx, cancel := context.WithCancel(context.Background())
	go func() { defer close(done); s.runArchiveLoop(ctx) }()
	defer func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("archive loop did not exit")
		}
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("archive did not reach blocked NAS")
	}
	s.archiveStatusMu.Lock()
	s.archiveLastProgressAt = time.Now().Add(-time.Minute)
	s.archiveStatusMu.Unlock()
	status := archiveOverviewForTest(t, s)
	if status.Phase != "discovering" || !status.Checking || !status.Stalled {
		t.Fatalf("NAS stall hidden by active experiment: %+v", status)
	}
	releaseOnce.Do(func() { close(release) })
	deadline := time.Now().Add(2 * time.Second)
	for {
		status = archiveOverviewForTest(t, s)
		if !status.Checking && status.Phase == "paused" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker did not report deliberate pause: %+v", status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if status.Stalled || status.Error != "" {
		t.Fatalf("normal pause still warns: %+v", status)
	}
}

func TestArchiveCopyPauseResetsProgressAndResumesVerification(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.archiveChecking = true
	s.archiveError = "previous NAS failure"
	s.state.experiments["active"] = model.Experiment{ID: "active", State: "running"}
	local, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	payload := bytes.Repeat([]byte("record\n"), 20000)
	if err = local.WriteFile("events.jsonl", payload, 0600); err != nil {
		t.Fatal(err)
	}
	source, err := openResultFile(local, "events.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer source.close()
	remote, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.copyArchiveFile(ctx, remote, "events.jsonl", source); done <- err }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("copy did not exit")
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for archiveOverviewForTest(t, s).Phase != "paused" {
		if time.Now().After(deadline) {
			t.Fatal("copy did not pause")
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.archiveStatusMu.Lock()
	s.archiveLastProgressAt = time.Now().Add(-time.Hour)
	s.archiveStatusMu.Unlock()
	status := archiveOverviewForTest(t, s)
	if status.Stalled || status.Error != "" {
		t.Fatalf("paused copy retained an error: %+v", status)
	}
	resumedAt := time.Now()
	s.state.mu.Lock()
	delete(s.state.experiments, "active")
	s.state.mu.Unlock()
	select {
	case err = <-done:
		// Keep cleanup nonblocking after observing the result.
		done <- err
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("copy did not resume")
	}
	status = archiveOverviewForTest(t, s)
	if status.Stalled || status.Phase != "publishing" || status.LastProgressAt.Before(resumedAt) {
		t.Fatalf("resumed copy did not report progress: %+v", status)
	}
	data, err := remote.ReadFile("events.jsonl")
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("copy or verification changed content: %v", err)
	}
}

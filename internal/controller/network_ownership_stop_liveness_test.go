package controller

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestNetworkOwnershipPersistenceWaitDoesNotBlockStopAPI(t *testing.T) {
	f := newNetworkOwnershipFixture(t, "network-ownership-stop")
	s := f.server
	s.config.User, s.config.Password = "fixture-operator", "fixture-password"
	run := f.run
	run.State, run.ExecutionID = "running", "network-ownership-stop-execution"
	s.state.experiments[run.ID] = run
	if err := s.persistExperiment(run); err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()
	s.cancels[run.ID] = cancelRun
	cookie := loginCookie(t, s)
	api := httptest.NewServer(s.Handler(t.Context()))
	defer api.Close()
	s.state.persistMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.state.persistMu.Unlock()
		}
	}()
	prepared := make(chan error, 1)
	go func() { prepared <- s.prepareExperimentNetwork(runCtx, run) }()
	retirementReviewWait(t, "network preparation did not enter cleanup-ownership persistence", func() bool {
		stacks := make([]byte, 2<<20)
		n := runtime.Stack(stacks, true)
		return bytes.Contains(stacks[:n], []byte("(*Server).rememberRunNetworkAgents"))
	})
	requestCtx, cancelRequest := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancelRequest()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, api.URL+"/api/v1/experiments/"+run.ID+"/stop", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.AddCookie(cookie)
	request.Header.Set("X-KPL-Request", "dashboard")
	request.Header.Set("X-KPL-Execution", run.ExecutionID)
	response, stopErr := http.DefaultClient.Do(request)
	if stopErr != nil {
		uncanceled := runCtx.Err() == nil
		cancelRun() // Teardown only: the fault uses the real Stop API above.
		s.state.persistMu.Unlock()
		locked = false
		prepareErr := <-prepared
		t.Fatalf("Stop API could not cancel a network ownership persistence wait: request=%v runUncanceled=%t preparation=%v", stopErr, uncanceled, prepareErr)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("Stop API was not accepted while persistence was busy: HTTP %d", response.StatusCode)
	}
	select {
	case <-runCtx.Done():
	case <-time.After(300 * time.Millisecond):
		t.Fatal("accepted Stop API did not cancel the network preparation context")
	}
	select {
	case err := <-prepared:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("network ownership wait lost the accepted Stop cancellation: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		s.state.persistMu.Unlock()
		locked = false
		<-prepared
		t.Fatal("network ownership wait did not exit after Stop while persistence remained held")
	}
	for _, path := range []string{filepath.Join(ownershipDirectory, run.ID+".json"), filepath.Join(pendingWorkCleanupDirectory, run.ID)} {
		if _, err := os.Stat(filepath.Join(s.config.DataDir, path)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Stop before ownership admission published cleanup metadata %s: %v", path, err)
		}
	}
	if f.fences.Load() != 0 || f.managerCalls.Load() != 0 {
		t.Fatal("network RPC started while cleanup ownership was blocked and then canceled")
	}
}

func TestNetworkOwnershipPersistenceWaitHonorsConcurrentDeletion(t *testing.T) {
	f := newNetworkOwnershipFixture(t, "network-ownership-delete")
	s := f.server
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s.state.persistMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.state.persistMu.Unlock()
		}
	}()
	prepared := make(chan error, 1)
	go func() { prepared <- s.prepareExperimentNetwork(ctx, f.run) }()
	retirementReviewWait(t, "network preparation did not enter cleanup-ownership persistence", func() bool {
		stacks := make([]byte, 2<<20)
		n := runtime.Stack(stacks, true)
		return bytes.Contains(stacks[:n], []byte("(*Server).rememberRunNetworkAgents"))
	})
	deleted := make(chan error, 1)
	go func() { deleted <- s.deleteSavedResult(f.run.ID) }()
	// Deletion acquires cancelMu before analysisJobMu and persistence. Seeing
	// its analysis mutex held proves ownership preparation yielded cancelMu.
	deadline := time.NewTimer(500 * time.Millisecond)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !s.analysisJobMu.TryLock() {
			break
		}
		s.analysisJobMu.Unlock()
		select {
		case <-ticker.C:
		case <-deadline.C:
			cancel()
			s.state.persistMu.Unlock()
			locked = false
			<-prepared
			<-deleted
			t.Fatal("network ownership persistence wait prevented deletion from acquiring its fence locks")
		}
	}
	s.state.persistMu.Unlock()
	locked = false
	if err := <-deleted; err != nil {
		t.Fatalf("concurrent deletion could not commit its result fence: %v", err)
	}
	if err := <-prepared; err == nil || !strings.Contains(err.Error(), "no longer writable") {
		t.Fatalf("network ownership crossed the committed deletion fence: %v", err)
	}
	if f.fences.Load() != 0 || f.managerCalls.Load() != 0 {
		t.Fatal("late network ownership preparation sent RPCs after deletion won admission")
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, f.run.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late network ownership preparation restored the deleted result: %v", err)
	}
	for _, path := range []string{filepath.Join(ownershipDirectory, f.run.ID+".json"), filepath.Join(pendingWorkCleanupDirectory, f.run.ID)} {
		if _, err := os.Stat(filepath.Join(s.config.DataDir, path)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("late network ownership preparation published deleted metadata %s: %v", path, err)
		}
	}
}

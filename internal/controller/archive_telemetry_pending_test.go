package controller

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func pendingArchiveFixture(t *testing.T, s *Server, id string) model.Experiment {
	t.Helper()
	run, _ := resultFixture(t, s, id, "failed", time.Now())
	run.CleanupState = "complete"
	run.DataState = "incomplete"
	run.IntegrityError = "final telemetry delivery timed out"
	if err := s.persistExperiment(run); err != nil {
		t.Fatal(err)
	}
	if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "first")}); err != nil {
		t.Fatal(err)
	}
	return run
}

func TestArchiveKeepsPendingTelemetryLocal(t *testing.T) {
	for _, signals := range []struct {
		name      string
		live      bool
		persisted bool
	}{{"live-only", true, false}, {"persisted-only", false, true}, {"both", true, true}} {
		t.Run(signals.name, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			const id = "pending"
			run := pendingArchiveFixture(t, s, id)
			run.TelemetryPending = signals.persisted
			if err := s.persistExperiment(run); err != nil {
				t.Fatal(err)
			}
			if signals.live {
				run.TelemetryPending = true
				s.state.experiments[id] = run
			}
			before, err := s.runSourceRevision(id)
			if err != nil {
				t.Fatal(err)
			}
			s.archiveIOCheck = func() { t.Fatal("pending delivery probed archive storage") }
			if err := s.archiveRun(t.Context(), id, 0); !errors.Is(err, errResultBusy) {
				t.Fatalf("pending delivery was archived: %v", err)
			}
			after, err := s.runSourceRevision(id)
			if err != nil || before != after {
				t.Fatalf("deferred archive changed source revision: %q %q %v", before, after, err)
			}
			directory := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
			if _, err := os.Stat(filepath.Join(directory, "events.jsonl")); err != nil {
				t.Fatalf("pending tail was rotated or removed: %v", err)
			}
			segments, err := filepath.Glob(filepath.Join(directory, "events-segment-*.jsonl"))
			if err != nil || len(segments) != 0 {
				t.Fatalf("pending tail was sealed: %v %v", segments, err)
			}

			// Acknowledging the remaining tail releases this Run; its previous
			// incomplete-data warning remains part of the saved result.
			s.archiveIOCheck = nil
			run.TelemetryPending = false
			if err := s.persistExperiment(run); err != nil {
				t.Fatal(err)
			}
			s.state.experiments[id] = run
			if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "late")}); err != nil {
				t.Fatal(err)
			}
			archiveTestRun(t, s, id)
			if got := strings.Join(storedEventIDs(t, storageZIP(t, s, id)["events.jsonl"]), ","); got != "first,late" {
				t.Fatalf("recovered delivery lost evidence: %s", got)
			}
		})
	}
}

func TestArchivePendingTelemetryDoesNotBlockOtherSealedRuns(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	pending := pendingArchiveFixture(t, s, "pending")
	pending.TelemetryPending = true
	if err := s.persistExperiment(pending); err != nil {
		t.Fatal(err)
	}
	s.state.experiments[pending.ID] = pending
	pendingArchiveFixture(t, s, "legacy-incomplete")
	if !s.archiveIdle() {
		t.Fatal("one pending delivery blocked unrelated sealed results")
	}
	archiveTestRun(t, s, "legacy-incomplete")
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, "legacy-incomplete", "events.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy incomplete result could not release local space: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, pending.ID, "events.jsonl")); err != nil {
		t.Fatalf("unrelated archive changed pending delivery: %v", err)
	}
}

func TestArchiveRechecksPendingTelemetryBeforeLocalCleanup(t *testing.T) {
	for _, signals := range []struct {
		name      string
		live      bool
		persisted bool
	}{{"live-only", true, false}, {"persisted-only", false, true}, {"both", true, true}} {
		t.Run(signals.name, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			const id = "changed-during-copy"
			run := pendingArchiveFixture(t, s, id)
			var once sync.Once
			s.archiveIOCheck = func() {
				once.Do(func() {
					// NAS work starts after source capture releases persistMu.
					s.state.persistMu.Lock()
					defer s.state.persistMu.Unlock()
					if signals.live {
						s.state.mu.Lock()
						run.TelemetryPending = true
						s.state.experiments[id] = run
						s.state.mu.Unlock()
					}
					if signals.persisted {
						run.TelemetryPending = true
						if err := s.persistExperiment(run); err != nil {
							t.Fatal(err)
						}
					}
				})
			}
			if err := s.archiveRun(t.Context(), id, 0); !errors.Is(err, errResultBusy) {
				t.Fatalf("newly pending delivery allowed local cleanup: %v", err)
			}
			directory := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
			segments, err := filepath.Glob(filepath.Join(directory, "events-segment-*.jsonl"))
			if err != nil || len(segments) != 1 {
				t.Fatalf("in-flight archive discarded its local evidence: %v %v", segments, err)
			}
			// The remote commit may have completed. Clearing the delivery fence
			// must reuse it, retain late events, and release the local segments.
			s.archiveIOCheck = nil
			run.TelemetryPending = false
			if err := s.persistExperiment(run); err != nil {
				t.Fatal(err)
			}
			s.state.experiments[id] = run
			if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "late")}); err != nil {
				t.Fatal(err)
			}
			archiveTestRun(t, s, id)
			if got := strings.Join(storedEventIDs(t, storageZIP(t, s, id)["events.jsonl"]), ","); got != "first,late" {
				t.Fatalf("resumed archive duplicated or lost evidence: %s", got)
			}
			segments, err = filepath.Glob(filepath.Join(directory, "events-segment-*.jsonl"))
			if err != nil || len(segments) != 0 {
				t.Fatalf("acknowledged delivery retained local segments: %v %v", segments, err)
			}
		})
	}
}

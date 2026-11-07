package controller

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSubmissionReplayAfterRestartDoesNotClaimLiveExecution(t *testing.T) {
	for _, state := range []string{"queued", "running"} {
		t.Run(state, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			first, err := s.submitScenario(context.Background(), []byte(queueLongScenario), 1, "restart-receipt")
			if err != nil {
				t.Fatal(err)
			}
			if err := s.StopScenario(first.ID); err != nil {
				t.Fatal(err)
			}
			waitRepetitions(t, s)
			// Restore an interrupted process's last durable active manifest. No
			// scheduler or old process is running in the replacement Controller.
			saved := persistedExperiment(t, s, first.ID)
			saved.State, saved.StopRequested = state, false
			saved.FinishedAt = time.Time{}
			if state == "queued" {
				saved.StartedAt = time.Time{}
			}
			if err := s.persistExperiment(saved); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(s.config.DataDir, currentRunsDirectory, first.ID, "experiment.json")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			restarted := New(s.config, nil)
			replay, err := restarted.submitScenario(context.Background(), []byte(queueLongScenario), 1, "restart-receipt")
			if err != nil || replay.ID != first.ID || replay.State != "interrupted" || replay.Timing != nil || replay.QueuePosition != 0 {
				t.Fatalf("inactive receipt reported a live execution: %+v err=%v", replay, err)
			}
			if len(restarted.state.experiments) != 0 || len(restarted.experimentQueue) != 0 {
				t.Fatal("receipt replay restarted interrupted work")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("status projection changed saved evidence: %v", err)
			}
		})
	}
}

func TestSubmissionReplayValidatesSavedMetadata(t *testing.T) {
	for _, failure := range []string{"identity", "oversized"} {
		t.Run(failure, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			first, err := s.submitScenario(context.Background(), []byte(appendScenario), 1, "damaged-receipt")
			if err != nil {
				t.Fatal(err)
			}
			waitRepetitions(t, s)
			path := filepath.Join(s.config.DataDir, currentRunsDirectory, first.ID, "experiment.json")
			want := "invalid"
			if failure == "identity" {
				if err := os.WriteFile(path, []byte(`{"id":"another-run","state":"completed"}`), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				want = "exceeds"
				if err := os.Truncate(path, resultMetadataLimit+1); err != nil {
					t.Fatal(err)
				}
			}
			restarted := New(s.config, nil)
			if _, err := restarted.submitScenario(context.Background(), []byte(appendScenario), 1, "damaged-receipt"); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("damaged receipt metadata was not rejected: %v", err)
			}
		})
	}
}

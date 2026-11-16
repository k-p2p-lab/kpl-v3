//go:build linux

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestPreparedPeerIntentPublicationFailureHasLiveRecoveryOwner(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires directory permission enforcement without root bypass")
	}
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "live retry", true: "restart"}[restart], func(t *testing.T) {
			s := historyTestServer(t)
			s.config.RunWorkerFactory = agentRunProcessFactory()
			var dockerLog string
			s.docker, dockerLog = fakeDocker(t, nil)
			directory := filepath.Join(s.config.DataDir, "runtime-peer-intents")
			// Write/search permit temp creation and rename; reading the directory
			// for its fsync fails after the final intent name is already visible.
			if err := os.MkdirAll(directory, 0300); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(directory, 0700) })
			node := model.Node{ID: "partial-intent-peer", RunID: "partial-intent-run", AgentID: s.config.ID, State: model.NodeStarting, StartedAt: time.Now().UTC(), Metadata: map[string]string{"runtime": "docker"}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			proc := &process{node: node, cancel: cancel, done: make(chan struct{})}
			s.processes[node.ID] = proc
			data, _ := json.Marshal(model.PeerProcessConfig{Node: node})
			s.runDockerProcess(ctx, proc, data, "")
			s.mu.RLock()
			path, occupied, cleanupErr, originalErr := proc.intentPath, s.capacityUsedLocked(), proc.cleanupErr, proc.exitErr
			started := proc.node.Metadata["containerCreatedAt"]
			s.mu.RUnlock()
			if path == "" || occupied != 1 || !errors.Is(cleanupErr, os.ErrPermission) || !errors.Is(originalErr, os.ErrPermission) || started != "" {
				t.Fatalf("partially published unsent intent lost its live owner: path=%q occupied=%d cleanup=%v original=%v started=%q", path, occupied, cleanupErr, originalErr, started)
			}
			if len(dockerCalls(t, dockerLog)) != 0 {
				t.Fatal("initial intent sync failure reached a Docker phase")
			}
			if err := os.Chmod(directory, 0700); err != nil {
				t.Fatal(err)
			}
			intent, err := s.readPeerIntent(path)
			if err != nil || intent.Version != 2 || intent.CreateAttempted || intent.ContainerID != "" {
				t.Fatalf("partial intent did not retain definite unsent identity: %+v %v", intent, err)
			}
			if count, err := s.countPeerIntents(node.RunID); err != nil || count != 1 {
				t.Fatalf("fixture did not retain its visible pending file: %d %v", count, err)
			}
			if err := s.finishRunWorker(t.Context(), node.RunID); err == nil {
				t.Fatal("partial local durability was acknowledged as a verified Run reset")
			}
			owner := s
			if restart {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				owner, err = New(s.config, s.logger)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = owner.Close() })
				owner.client = s.client
				owner.docker, dockerLog = fakeDocker(t, nil)
				if err := owner.recoverPeerIntents(t.Context()); err != nil {
					t.Fatalf("restart did not reclaim the prepared unsent intent: %v", err)
				}
			} else {
				s.stopAll()
				if err := s.waitRunContainers(t.Context(), node.RunID, ^uint64(0)); err != nil {
					t.Fatalf("same Agent could not retry its local intent durability: %v", err)
				}
			}
			if len(dockerCalls(t, dockerLog)) != 0 {
				t.Fatal("definitely unsent recovery issued a Docker mutation")
			}
			stored, found, err := owner.history.storedNode(node.ID)
			if err != nil || !found || stored.State != model.NodeFailed {
				t.Fatalf("local recovery changed the failed Peer outcome: %+v %v %v", stored, found, err)
			}
			if !restart && stored.Error != originalErr.Error() {
				t.Fatalf("same Agent recovery erased the original sync failure: got=%q want=%q", stored.Error, originalErr.Error())
			}
			events, err := owner.spool.loadPrefix()
			if err != nil || len(events) != 1 || events[0].EventID != intent.TerminationID || !events[0].Timestamp.Equal(intent.TerminatedAt) {
				t.Fatalf("local recovery changed termination identity or time: %+v %v", events, err)
			}
			if err := owner.drainRunEvents(t.Context(), node.RunID); err != nil {
				t.Fatal(err)
			}
			if err := owner.finishRunWorker(t.Context(), node.RunID); err != nil {
				t.Fatalf("repaired local intent still blocked Run reset: %v", err)
			}
			if count, err := owner.countPeerIntents(node.RunID); err != nil || count != 0 || owner.snapshotAgent().ActiveNodes != 0 {
				t.Fatalf("repaired local intent retained false cleanup debt: intents=%d err=%v", count, err)
			}
		})
	}
}

func TestPreparedPeerIntentPrePublicationFailureDoesNotQuarantine(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires directory permission enforcement without root bypass")
	}
	s := historyTestServer(t)
	s.config.RunWorkerFactory = agentRunProcessFactory()
	var logPath string
	s.docker, logPath = fakeDocker(t, nil)
	directory := filepath.Join(s.config.DataDir, "runtime-peer-intents")
	if err := os.MkdirAll(directory, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(directory, 0700) })
	node := model.Node{ID: "unpublished-peer", RunID: "unpublished-run", AgentID: s.config.ID, State: model.NodeStarting, StartedAt: time.Now().UTC(), Metadata: map[string]string{"runtime": "docker"}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	proc := &process{node: node, cancel: cancel, done: make(chan struct{})}
	s.processes[node.ID] = proc
	data, _ := json.Marshal(model.PeerProcessConfig{Node: node})
	s.runDockerProcess(ctx, proc, data, "")
	if proc.intentPath != "" || proc.cleanupErr != nil || !errors.Is(proc.exitErr, os.ErrPermission) || s.snapshotAgent().ActiveNodes != 0 {
		t.Fatalf("definitely unpublished intent retained false ownership: path=%q cleanup=%v original=%v", proc.intentPath, proc.cleanupErr, proc.exitErr)
	}
	if count, err := s.countPeerIntents(node.RunID); err != nil || count != 0 || len(dockerCalls(t, logPath)) != 0 {
		t.Fatalf("pre-publication failure reached Docker or left an intent: %d %v", count, err)
	}
}

func TestTelemetryFilePublicationKeepsDirectorySyncFailureAnError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires directory permission enforcement without root bypass")
	}
	for _, mode := range []os.FileMode{0500, 0300, 0700} {
		t.Run(mode.String(), func(t *testing.T) {
			directory := t.TempDir()
			if err := os.Chmod(directory, mode); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(directory, 0700) })
			data := []byte(`{"proof":"prepared"}`)
			path := filepath.Join(directory, "intent.json")
			published, err := writeTelemetryFileWithPublication(path, data)
			if published != (mode != 0500) || (err == nil) != (mode == 0700) {
				t.Fatalf("publication was confused with durability: mode=%v published=%t err=%v", mode, published, err)
			}
			if err != nil && !errors.Is(err, os.ErrPermission) {
				t.Fatal(err)
			}
			wrapperErr := writeTelemetryFile(filepath.Join(directory, "wrapper.json"), data)
			if (wrapperErr == nil) != (mode == 0700) || wrapperErr != nil && !errors.Is(wrapperErr, os.ErrPermission) {
				t.Fatalf("legacy write wrapper changed its acknowledgement: mode=%v err=%v", mode, wrapperErr)
			}
			if mode != 0500 {
				contents, readErr := os.ReadFile(path)
				if readErr != nil || !bytes.Equal(contents, data) {
					t.Fatalf("published file did not contain its complete payload: %q %v", contents, readErr)
				}
			} else if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("pre-publication error left a final path: %v", statErr)
			}
			if wrapperErr != nil && !strings.Contains(wrapperErr.Error(), "permission denied") {
				t.Fatalf("wrapper hid its original sync error: %v", wrapperErr)
			}
		})
	}
}

//go:build linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Inject the filesystem permission change after intent/history writes but
// during real termination serialization. Unlink needs write/search permission;
// opening that directory for its durability sync additionally needs read.
type intentDirectorySyncFailure struct {
	directory  string
	intentPath string
	saved      []byte
	once       sync.Once
	err        error
}

func (f *intentDirectorySyncFailure) MarshalJSON() ([]byte, error) {
	f.once.Do(func() {
		if f.intentPath != "" {
			f.saved, f.err = os.ReadFile(f.intentPath)
			if f.err != nil {
				return
			}
		}
		f.err = os.Chmod(f.directory, 0300)
	})
	return []byte(`"directory-sync-fault"`), f.err
}

type intentReplacementDuringTermination struct {
	server  *Server
	process *process
	path    string
}

func (f intentReplacementDuringTermination) MarshalJSON() ([]byte, error) {
	f.server.mu.Lock()
	f.process.intentPath, f.process.intentSyncPending = f.path, false
	f.server.mu.Unlock()
	return []byte(`"replacement-owner"`), nil
}

func TestPeerIntentCompletionDoesNotRemoveReplacementOwner(t *testing.T) {
	s := historyTestServer(t)
	node := model.Node{ID: "replaced-intent-peer", RunID: "replaced-intent-run", AgentID: s.config.ID, State: model.NodeStopping, StartedAt: time.Now().UTC(), Metadata: map[string]string{}}
	proc := &process{node: node}
	if _, err := s.beginPeerIntent(proc, node); err != nil {
		t.Fatal(err)
	}
	oldPath := proc.intentPath
	replacement := filepath.Join(filepath.Dir(oldPath), "replacement-owner.json")
	if err := os.WriteFile(replacement, []byte("retain new owner evidence"), 0600); err != nil {
		t.Fatal(err)
	}
	event := model.TraceEvent{RunID: node.RunID, NodeID: node.ID, Type: "measurement_terminated", Timestamp: time.Now().UTC(), Fields: map[string]any{"replacement": intentReplacementDuringTermination{s, proc, replacement}}}
	if err := s.completePeerIntent(proc, event, model.NodeStopped, ""); err == nil {
		t.Fatal("completion did not reject a changed intent owner")
	}
	if data, err := os.ReadFile(replacement); err != nil || string(data) != "retain new owner evidence" {
		t.Fatalf("completion removed another intent owner's evidence: %q, %v", data, err)
	}
	if proc.intentPath != replacement || proc.intentSyncPending {
		t.Fatal("completion cleared or changed another owner's cleanup obligation")
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("changed ownership was not rejected before original intent removal: %v", err)
	}
}

func TestPeerIntentSyncFailureRestartKeepsDurableTerminationIdentity(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires directory permission enforcement without root bypass")
	}
	for _, reappears := range []bool{false, true} {
		t.Run(map[bool]string{false: "removed", true: "reappeared"}[reappears], func(t *testing.T) {
			s := historyTestServer(t)
			node := model.Node{ID: "restart-intent-peer", RunID: "restart-intent-run", AgentID: s.config.ID, State: model.NodeStopping, StartedAt: time.Now().UTC(), Metadata: map[string]string{}}
			proc := &process{node: node}
			if _, err := s.beginPeerIntent(proc, node); err != nil {
				t.Fatal(err)
			}
			if err := s.commitPeerIntent(proc, fakeContainerID); err != nil {
				t.Fatal(err)
			}
			path := proc.intentPath
			directory := filepath.Dir(path)
			t.Cleanup(func() { _ = os.Chmod(directory, 0700) })
			fault := &intentDirectorySyncFailure{directory: directory, intentPath: path}
			event := model.TraceEvent{RunID: node.RunID, NodeID: node.ID, Type: "measurement_terminated", Timestamp: time.Now().UTC(), Fields: map[string]any{"fault": fault}}
			if err := s.completePeerIntent(proc, event, model.NodeStopped, ""); !errors.Is(err, os.ErrPermission) {
				t.Fatalf("fixture did not leave an unconfirmed removal sync: %v", err)
			}
			if err := os.Chmod(directory, 0700); err != nil {
				t.Fatal(err)
			}
			var saved peerCreationIntent
			if err := json.Unmarshal(fault.saved, &saved); err != nil || saved.TerminationID == "" || saved.TerminatedAt.IsZero() {
				t.Fatalf("fixture did not retain the durable pre-unlink identity: %+v %v", saved, err)
			}
			if reappears {
				// An unlink not yet confirmed by directory sync can reappear
				// after a crash. Recreate exactly its last durable bytes.
				if err := writeTelemetryFile(path, fault.saved); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			restartedConfig := s.config
			restartedConfig.ControllerURL = "http://controller:8080"
			restarted, err := New(restartedConfig, s.logger)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := restarted.Close(); err != nil {
					t.Error(err)
				}
			})
			restarted.docker, _ = fakeDocker(t, nil)
			if err := restarted.recoverPeerIntents(t.Context()); err != nil {
				t.Fatalf("restart could not recover the on-disk intent state: %v", err)
			}
			events, err := restarted.spool.loadPrefix()
			if err != nil || len(events) == 0 {
				t.Fatalf("restart lost accepted termination evidence: %d %v", len(events), err)
			}
			for _, event := range events {
				if event.EventID != saved.TerminationID || !event.Timestamp.Equal(saved.TerminatedAt) {
					t.Fatalf("recovery changed the durable termination identity: %+v", event)
				}
			}
			if count, err := restarted.countPeerIntents(node.RunID); err != nil || count != 0 {
				t.Fatalf("restart retained a false pending intent: %d %v", count, err)
			}
		})
	}
}

func TestPeerIntentRetriesDirectorySyncAfterConfirmedUnlink(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires directory permission enforcement without root bypass")
	}
	s := historyTestServer(t)
	node := model.Node{ID: "removed-peer", RunID: "intent-sync-run", AgentID: s.config.ID, State: model.NodeStopping, StartedAt: time.Now().UTC(), Metadata: map[string]string{"runtime": "docker"}}
	proc := &process{node: node}
	if _, err := s.beginPeerIntent(proc, node); err != nil {
		t.Fatal(err)
	}
	path := proc.intentPath
	directory := filepath.Dir(path)
	t.Cleanup(func() { _ = os.Chmod(directory, 0700) })
	fault := &intentDirectorySyncFailure{directory: directory}
	event := model.TraceEvent{EventID: "termination-candidate", RunID: node.RunID, NodeID: node.ID, AgentID: s.config.ID, Type: "measurement_terminated", Timestamp: time.Now().UTC(), Fields: map[string]any{"fault": fault}}
	err := s.completePeerIntent(proc, event, model.NodeStopped, "")
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("fixture did not reach unlink-success/directory-sync-failure: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("intent was not actually unlinked before sync failed: %v", err)
	}
	if proc.intentPath == "" {
		t.Fatal("unverified directory sync was prematurely acknowledged")
	}
	if count := len(s.spool.records); count != 1 {
		t.Fatalf("termination was not durably queued before intent unlink: %d", count)
	}
	if stored, err := s.history.contains(node.ID); err != nil || !stored {
		t.Fatalf("terminal Peer history was not saved before intent unlink: %v, %v", stored, err)
	}
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.completePeerIntent(proc, event, model.NodeStopped, ""); err != nil {
		t.Fatalf("a transient directory sync failure permanently broke cleanup retry: %v", err)
	}
	if proc.intentPath != "" {
		t.Fatal("successful directory sync did not clear the cleanup obligation")
	}
	if count := len(s.spool.records); count != 1 {
		t.Fatalf("directory-sync retry duplicated the already accepted termination: %d", count)
	}
}

func TestPeerIntentMissingBeforeConfirmedUnlinkRemainsFailure(t *testing.T) {
	s := historyTestServer(t)
	node := model.Node{ID: "unknown-intent-peer", RunID: "missing-intent-run", AgentID: s.config.ID, State: model.NodeStopping, StartedAt: time.Now().UTC(), Metadata: map[string]string{}}
	proc := &process{node: node}
	if _, err := s.beginPeerIntent(proc, node); err != nil {
		t.Fatal(err)
	}
	path := proc.intentPath
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	event := model.TraceEvent{RunID: node.RunID, NodeID: node.ID, AgentID: s.config.ID, Type: "measurement_terminated", Timestamp: time.Now().UTC()}
	if err := s.completePeerIntent(proc, event, model.NodeStopped, ""); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexplained intent loss was incorrectly acknowledged: %v", err)
	}
	if proc.intentPath != path || len(s.spool.records) != 0 {
		t.Fatal("missing intent was cleared or invented a termination acknowledgment")
	}
	if stored, err := s.history.contains(node.ID); err != nil || stored {
		t.Fatalf("missing intent invented terminal history: %v, %v", stored, err)
	}
}

func TestPeerIntentSyncFailureBeforeUnlinkRetriesFullFinalization(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires directory permission enforcement without root bypass")
	}
	s := historyTestServer(t)
	node := model.Node{ID: "before-unlink-peer", RunID: "before-unlink-run", AgentID: s.config.ID, State: model.NodeStopping, StartedAt: time.Now().UTC(), Metadata: map[string]string{}}
	proc := &process{node: node}
	if _, err := s.beginPeerIntent(proc, node); err != nil {
		t.Fatal(err)
	}
	path := proc.intentPath
	directory := filepath.Dir(path)
	t.Cleanup(func() { _ = os.Chmod(directory, 0700) })
	if err := os.Chmod(directory, 0300); err != nil {
		t.Fatal(err)
	}
	event := model.TraceEvent{RunID: node.RunID, NodeID: node.ID, AgentID: s.config.ID, Type: "measurement_terminated", Timestamp: time.Now().UTC()}
	if err := s.completePeerIntent(proc, event, model.NodeStopped, ""); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("fixture did not fail the earlier intent write sync: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("an earlier write-sync failure removed the original intent: %v", err)
	}
	if len(s.spool.records) != 0 {
		t.Fatal("failed intent update skipped ahead to termination acknowledgment")
	}
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.completePeerIntent(proc, event, model.NodeStopped, ""); err != nil {
		t.Fatalf("earlier intent-write failure did not retry full finalization: %v", err)
	}
	if proc.intentPath != "" || len(s.spool.records) != 1 {
		t.Fatal("full retry lost intent cleanup or termination evidence")
	}
}

func TestKnownPeerIntentSyncPendingBlocksResetAndNextAdmission(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires directory permission enforcement without root bypass")
	}
	s := historyTestServer(t)
	node := model.Node{ID: "known-sync-peer", RunID: "known-sync-run", AgentID: s.config.ID, State: model.NodeStopped, StartedAt: time.Now().UTC(), Metadata: map[string]string{"cleanupComplete": "true"}}
	proc := &process{node: node, exited: true}
	s.processes[node.ID] = proc
	if _, err := s.beginPeerIntent(proc, node); err != nil {
		t.Fatal(err)
	}
	path := proc.intentPath
	directory := filepath.Dir(path)
	t.Cleanup(func() { _ = os.Chmod(directory, 0700) })
	fault := &intentDirectorySyncFailure{directory: directory, intentPath: path}
	event := model.TraceEvent{RunID: node.RunID, NodeID: node.ID, AgentID: s.config.ID, Type: "measurement_terminated", Timestamp: time.Now().UTC(), Fields: map[string]any{"fault": fault}}
	if err := s.completePeerIntent(proc, event, model.NodeStopped, ""); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("fixture did not retain a directory-sync obligation: %v", err)
	}
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.drainRunEvents(t.Context(), node.RunID); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(directory); err != nil {
		t.Fatal(err)
	}
	if count, err := s.countPeerIntents(node.RunID); err != nil || count != 1 {
		t.Fatalf("missing directory hid its known sync obligation: %d %v", count, err)
	}
	if err := s.completePeerIntent(proc, event, model.NodeStopped, ""); !errors.Is(err, os.ErrNotExist) || !proc.intentSyncPending {
		t.Fatalf("an unverified missing directory released cleanup: %v", err)
	}
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	// A reappeared file and the in-memory sync flag describe the same intent.
	if err := writeTelemetryFile(path, fault.saved); err != nil {
		t.Fatal(err)
	}
	if count, err := s.countPeerIntents(node.RunID); err != nil || count != 1 {
		t.Fatalf("reappeared intent was double-counted: %d %v", count, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	var pending *runWorkerPendingError
	proof, err := s.proveRunReset(node.RunID)
	if !errors.As(err, &pending) || proof.CreationIntents != 1 || proof.ActivePeers != 0 || proof.CleanupPending != 0 || proof.TelemetryEvents != 0 {
		t.Fatalf("a known sync debt did not independently block strict reset: %+v %v", proof, err)
	}
	if err := s.runWorkerAdmissionReady("next-run"); !errors.Is(err, errRunResetPending) {
		t.Fatalf("known sync debt admitted the next Run: %v", err)
	}
	counts := make(chan error, 1)
	go func() {
		for range 100 {
			if count, err := s.countPeerIntents(node.RunID); err != nil || count < 0 || count > 1 {
				counts <- fmt.Errorf("concurrent intent count=%d: %v", count, err)
				return
			}
		}
		counts <- nil
	}()
	if err := s.completePeerIntent(proc, event, model.NodeStopped, ""); err != nil {
		t.Fatal(err)
	}
	if err := <-counts; err != nil {
		t.Fatal(err)
	}
	if proof, err := s.proveRunReset(node.RunID); err != nil || proof.CreationIntents != 0 {
		t.Fatalf("verified directory sync kept a false reset barrier: %+v %v", proof, err)
	}
	if err := s.runWorkerAdmissionReady("next-run"); err != nil {
		t.Fatalf("confirmed sync did not reopen next-Run admission: %v", err)
	}
}

func TestRecoveredCompletedPeerIntentKeepsActualContainerOutcome(t *testing.T) {
	for _, exitCode := range []int{0, 7} {
		t.Run(fmt.Sprintf("exit=%d", exitCode), func(t *testing.T) {
			s, _ := newContainerTestServer(t, nil)
			s.config.RunWorkerFactory = agentRunProcessFactory()
			workerControllerFixture(t, s)
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				select {
				case <-release:
					_, _ = fmt.Fprintf(w, `{"StatusCode":%d}`, exitCode)
				case <-r.Context().Done():
				}
			}))
			t.Cleanup(engine.Close)
			s.docker.waitClient, s.docker.waitURL = engine.Client(), engine.URL
			created, err := s.createNode(t.Context(), model.CreateNodeRequest{ID: "completed-container-peer", RunID: "completed-container-run", Group: "workers", Generation: 3})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("real Peer creation did not reach its Docker wait")
			}
			s.mu.RLock()
			proc := s.processes[created.ID]
			path := proc.intentPath
			s.mu.RUnlock()
			intent, err := s.readPeerIntent(path)
			if err != nil || intent.ContainerID != fakeContainerID {
				t.Fatalf("actual creation did not persist the admitted container: %+v %v", intent, err)
			}
			// A Peer reports its own boot time. That report must not replace
			// the Agent's original incarnation timestamp used by its intent.
			if err := s.updateNode(model.Node{ID: created.ID, RunID: created.RunID, State: model.NodeReady, PeerID: "fixture-peer", StartedAt: created.StartedAt.Add(time.Hour), LastSeen: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			releaseOnce.Do(func() { close(release) })
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := s.waitRunContainers(ctx, created.RunID, created.Generation); err != nil {
				t.Fatal(err)
			}
			before, found, err := s.history.storedNode(created.ID)
			if err != nil || !found || !before.StartedAt.Equal(intent.StartedAt) || !before.StartedAt.Equal(created.StartedAt) || before.Metadata["cleanupComplete"] != "true" {
				t.Fatalf("actual lifecycle did not retain its confirmed original identity: %+v %v %v", before, found, err)
			}
			if exitCode == 0 && before.State != model.NodeStopped || exitCode == 7 && (before.State != model.NodeFailed || !strings.Contains(before.Error, "exited with code 7")) {
				t.Fatalf("actual container outcome was not recorded: %+v", before)
			}
			s.eventsMu.Lock()
			events, err := s.spool.loadPrefix()
			s.eventsMu.Unlock()
			if err != nil || len(events) != 1 || events[0].EventID != intent.TerminationID {
				t.Fatalf("actual termination did not retain its durable intent identity: %+v %v", events, err)
			}
			intent.TerminatedAt = events[0].Timestamp
			data, err := json.Marshal(intent)
			if err != nil {
				t.Fatal(err)
			}
			// Reappear with the exact previously persisted admission identity
			// and the termination time observed from its accepted spool event.
			if err := writeTelemetryFile(path, data); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			restartedConfig := s.config
			restartedConfig.ControllerURL = "http://controller:8080"
			restarted, err := New(restartedConfig, s.logger)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restarted.Close() })
			restarted.docker, _ = fakeDocker(t, nil)
			if err := restarted.recoverPeerIntents(ctx); err != nil {
				t.Fatalf("reappeared completed intent did not recover: %v", err)
			}
			after, found, err := restarted.history.storedNode(created.ID)
			if err != nil || !found || !reflect.DeepEqual(before, after) {
				t.Fatalf("recovery rewrote the actual scientific outcome: before=%+v after=%+v found=%v err=%v", before, after, found, err)
			}
			replayed, err := restarted.spool.loadPrefix()
			if err != nil || len(replayed) != 2 {
				t.Fatalf("recovery lost its at-least-once evidence: %+v %v", replayed, err)
			}
			for _, event := range replayed {
				if event.EventID != intent.TerminationID || !event.Timestamp.Equal(intent.TerminatedAt) {
					t.Fatalf("recovery changed the original termination identity or time: %+v", event)
				}
			}
		})
	}
}

func TestRecoveredPeerIntentRejectsUnmatchedTerminalHistory(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*model.Node)
	}{
		{"run", func(node *model.Node) { node.RunID = "different-run" }},
		{"agent", func(node *model.Node) { node.AgentID = "different-agent" }},
		{"generation", func(node *model.Node) { node.Generation++ }},
		{"started-at", func(node *model.Node) { node.StartedAt = node.StartedAt.Add(time.Second) }},
		{"legacy-zero-start", func(node *model.Node) { node.StartedAt = time.Time{} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, _ := newContainerTestServer(t, nil)
			node := model.Node{ID: "mismatched-history-peer", AgentID: s.config.ID, RunID: "mismatched-history-run", Generation: 5, State: model.NodeStopping, StartedAt: time.Now().UTC(), Metadata: map[string]string{}}
			proc := &process{node: node}
			if _, err := s.beginPeerIntent(proc, node); err != nil {
				t.Fatal(err)
			}
			if err := s.commitPeerIntent(proc, fakeContainerID); err != nil {
				t.Fatal(err)
			}
			stored := node
			stored.State, stored.Error = model.NodeFailed, "retain original failure"
			stored.Metadata = map[string]string{"cleanupComplete": "true"}
			test.change(&stored)
			if err := s.history.saveNodes([]model.Node{stored}); err != nil {
				t.Fatal(err)
			}
			if err := s.recoverPeerIntents(t.Context()); err == nil {
				t.Fatal("a mismatched terminal history was accepted as this Peer instance")
			}
			after, found, err := s.history.storedNode(node.ID)
			if err != nil || !found || !reflect.DeepEqual(stored, after) {
				t.Fatalf("recovery overwrote another history instance: %+v %v %v", after, found, err)
			}
			if _, err := os.Stat(proc.intentPath); err != nil || len(s.spool.records) != 0 {
				t.Fatalf("mismatched evidence was discarded or invented a termination: %v", err)
			}
		})
	}
}

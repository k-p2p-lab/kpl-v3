package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// The CLI is a real child process; the Engine remains in the parent and can
// commit an accepted create after that child and its HTTP socket have exited.
func TestUncertainDockerCLIHelper(t *testing.T) {
	base := os.Getenv("KPL_UNCERTAIN_ENGINE_TEST_URL")
	if base == "" {
		return
	}
	var args []string
	for i, arg := range os.Args {
		if arg == "--" {
			args = os.Args[i+1:]
			break
		}
	}
	if len(args) == 0 {
		os.Exit(2)
	}
	method, path := http.MethodGet, "/ps"
	var body io.Reader
	switch args[0] {
	case "create":
		identity := uncertainContainerIdentity{Labels: make(map[string]string)}
		for i := 1; i+1 < len(args); i++ {
			if args[i] == "--name" {
				identity.Name = "/" + args[i+1]
			}
			if args[i] == "--label" {
				key, value, _ := strings.Cut(args[i+1], "=")
				identity.Labels[key] = value
			}
		}
		data, _ := json.Marshal(identity)
		body, method, path = bytes.NewReader(data), http.MethodPost, "/create"
	case "inspect":
		path = "/inspect/" + args[len(args)-1]
	case "rm":
		method, path = http.MethodDelete, "/remove/"+args[len(args)-1]
	case "ps":
	default:
		os.Exit(2)
	}
	req, err := http.NewRequest(method, base+path, body)
	if err != nil {
		os.Exit(2)
	}
	response, err := (&http.Client{Transport: &http.Transport{}}).Do(req)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		os.Exit(1)
	}
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		os.Exit(1)
	}
	if response.StatusCode >= 400 {
		fmt.Fprint(os.Stderr, string(data))
		os.Exit(1)
	}
	fmt.Print(string(data))
	os.Exit(0)
}

type uncertainContainerIdentity struct {
	ID, Name string
	Labels   map[string]string
}

type uncertainEngine struct {
	t            *testing.T
	server       *Server
	endpoint     *httptest.Server
	mode         string
	accepted     chan struct{}
	release      chan struct{}
	committed    chan struct{}
	releaseOne   sync.Once
	mu           sync.Mutex
	identity     uncertainContainerIdentity
	present      bool
	removed      int
	listed       int
	beforeReply  func() error
	failRemovals int
}

func newUncertainEngine(t *testing.T, s *Server, mode string) *uncertainEngine {
	t.Helper()
	e := &uncertainEngine{t: t, server: s, mode: mode, accepted: make(chan struct{}), release: make(chan struct{}), committed: make(chan struct{})}
	e.endpoint = httptest.NewServer(http.HandlerFunc(e.handle))
	t.Cleanup(func() { e.allowCommit(); e.endpoint.Close() })
	return e
}

func (e *uncertainEngine) allowCommit() { e.releaseOne.Do(func() { close(e.release) }) }

func (e *uncertainEngine) runtime() *dockerRuntime {
	d := &dockerRuntime{binary: "uncertain-docker-test", image: "fake:image", network: "peers"}
	d.commandContext = func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, os.Args[0], append([]string{"-test.run=^TestUncertainDockerCLIHelper$", "--"}, args...)...)
		cmd.Env = append(os.Environ(), "KPL_UNCERTAIN_ENGINE_TEST_URL="+e.endpoint.URL)
		return cmd
	}
	return d
}

func (e *uncertainEngine) intentPath(identity uncertainContainerIdentity) string {
	hash := sha256.Sum256([]byte(identity.Labels["io.kpl.node"]))
	return filepath.Join(e.server.config.DataDir, "runtime-peer-intents", hex.EncodeToString(hash[:])+".json")
}

func (e *uncertainEngine) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/create":
		var identity uncertainContainerIdentity
		if err := json.NewDecoder(r.Body).Decode(&identity); err != nil {
			e.t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		identity.ID = fakeContainerID
		if e.server != nil {
			intent, err := e.server.readPeerIntent(e.intentPath(identity))
			if err != nil || intent.Version != 2 || !intent.CreateAttempted || intent.CleanupConfirmed {
				e.t.Errorf("daemon create preceded durable attempted state: %+v %v", intent, err)
			}
		}
		e.mu.Lock()
		e.identity = identity
		e.mu.Unlock()
		close(e.accepted)
		if e.mode == "eof" {
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				e.t.Error(err)
				return
			}
			_ = connection.Close()
		}
		<-e.release // Engine work survives the original CLI's disconnected socket.
		e.mu.Lock()
		e.present = true
		e.mu.Unlock()
		close(e.committed)
		if e.mode != "eof" {
			e.mu.Lock()
			beforeReply := e.beforeReply
			e.mu.Unlock()
			if beforeReply != nil {
				if err := beforeReply(); err != nil {
					e.t.Error(err)
				}
			}
			fmt.Fprintln(w, fakeContainerID)
		}
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/inspect/"):
		e.mu.Lock()
		identity, present := e.identity, e.present
		e.mu.Unlock()
		if !present {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "Error: No such object: "+strings.TrimPrefix(r.URL.Path, "/inspect/"))
			return
		}
		_ = json.NewEncoder(w).Encode(identity)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/remove/"):
		e.mu.Lock()
		identity, present := e.identity, e.present
		e.mu.Unlock()
		if strings.TrimPrefix(r.URL.Path, "/remove/") != fakeContainerID {
			e.t.Error("uncertain create removal did not bind the observed immutable ID")
			w.WriteHeader(http.StatusConflict)
			return
		}
		if !present {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "Error response from daemon: No such container: "+fakeContainerID)
			return
		}
		if e.server != nil {
			intent, err := e.server.readPeerIntent(e.intentPath(identity))
			if err != nil || intent.ContainerID != fakeContainerID {
				e.t.Errorf("positive identity was removed before its durable proof: %+v %v", intent, err)
			}
		}
		e.mu.Lock()
		if e.failRemovals > 0 {
			e.failRemovals--
			e.mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "daemon unavailable after positive identity persistence")
			return
		}
		e.present = false
		e.removed++
		e.mu.Unlock()
		fmt.Fprintln(w, fakeContainerID)
	case r.Method == http.MethodGet && r.URL.Path == "/ps":
		e.mu.Lock()
		e.listed++
		present := e.present
		e.mu.Unlock()
		if present {
			fmt.Fprintln(w, fakeContainerID)
		}
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func uncertainPeerProcess(t *testing.T, s *Server, e *uncertainEngine) *process {
	t.Helper()
	s.config.RunWorkerFactory, s.config.AutoResetNetwork, s.config.ControlNetwork = agentRunProcessFactory(), true, "control"
	s.startupReconciled, s.docker = true, e.runtime()
	node := model.Node{ID: "uncertain-peer", RunID: "uncertain-run", AgentID: s.config.ID, Generation: 1, State: model.NodeStarting, StartedAt: time.Now().UTC(), Metadata: map[string]string{"runtime": "docker"}}
	data, err := json.Marshal(model.PeerProcessConfig{Node: node})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	if e.mode == "deadline" {
		cancel()
		ctx, cancel = context.WithTimeout(t.Context(), 750*time.Millisecond)
	}
	t.Cleanup(cancel)
	proc := &process{node: node, cancel: cancel, done: make(chan struct{})}
	s.processes[node.ID] = proc
	done := make(chan struct{})
	go func() { defer close(done); s.runDockerProcess(ctx, proc, data, "") }()
	select {
	case <-e.accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("CLI did not reach the mock Engine")
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("interrupted create did not return with retained cleanup ownership")
	}
	return proc
}

func assertUncertainPeerOwned(t *testing.T, s *Server, proc *process) {
	t.Helper()
	s.mu.RLock()
	occupied, uncertain, pending, path := s.capacityUsedLocked(), proc.createUncertain, proc.cleanupErr, proc.intentPath
	s.mu.RUnlock()
	if occupied != 1 || !uncertain || !errors.Is(pending, errDockerCreateUncertain) {
		t.Fatalf("interrupted create lost its capacity or unknown result: occupied=%d uncertain=%t pending=%v", occupied, uncertain, pending)
	}
	intent, err := s.readPeerIntent(path)
	if err != nil || intent.Version != 2 || !intent.CreateAttempted || intent.CleanupConfirmed || intent.ContainerID != "" {
		t.Fatalf("interrupted create lost its durable unknown state: %+v %v", intent, err)
	}
	if err := s.finishRunWorker(t.Context(), proc.node.RunID); err == nil {
		t.Fatal("unconfirmed daemon create produced a successful Run reset")
	}
}

func TestDockerUncertainCreateLateCommitKeepsOwnershipUntilLiveRetry(t *testing.T) {
	for _, mode := range []string{"deadline", "eof"} {
		t.Run(mode, func(t *testing.T) {
			s := historyTestServer(t)
			e := newUncertainEngine(t, s, mode)
			proc := uncertainPeerProcess(t, s, e)
			assertUncertainPeerOwned(t, s, proc)
			s.stopRunGeneration(proc.node.RunID, ^uint64(0))
			if err := s.waitRunContainers(t.Context(), proc.node.RunID, ^uint64(0)); !errors.Is(err, errDockerCreateUncertain) {
				t.Fatalf("pre-commit 404 retry released unknown create: %v", err)
			}
			assertUncertainPeerOwned(t, s, proc)
			e.allowCommit()
			<-e.committed
			s.stopRunGeneration(proc.node.RunID, ^uint64(0))
			if err := s.waitRunContainers(t.Context(), proc.node.RunID, ^uint64(0)); err != nil {
				t.Fatal(err)
			}
			if err := s.drainRunEvents(t.Context(), proc.node.RunID); err != nil {
				t.Fatal(err)
			}
			if err := s.finishRunWorker(t.Context(), proc.node.RunID); err != nil {
				t.Fatalf("observed late create could not complete its normal reset: %v", err)
			}
			e.mu.Lock()
			present, removed := e.present, e.removed
			e.mu.Unlock()
			if present || removed != 1 || s.snapshotAgent().ActiveNodes != 0 {
				t.Fatalf("late create cleanup did not reclaim the exact container: present=%t removed=%d", present, removed)
			}
			if count, err := s.countPeerIntents(proc.node.RunID); err != nil || count != 0 {
				t.Fatalf("confirmed late create retained a false obligation: %d %v", count, err)
			}
		})
	}
}

func TestDockerUncertainCreateRecoversAfterSupervisorRestart(t *testing.T) {
	s := historyTestServer(t)
	e := newUncertainEngine(t, s, "eof")
	proc := uncertainPeerProcess(t, s, e)
	assertUncertainPeerOwned(t, s, proc)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(s.config, s.logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	restarted.client, restarted.docker = s.client, e.runtime()
	if err := restarted.recoverPeerIntents(t.Context()); !errors.Is(err, errDockerCreateUncertain) {
		t.Fatalf("restart accepted pre-commit absence as cleanup proof: %v", err)
	}
	if count, err := restarted.countPeerIntents(proc.node.RunID); err != nil || count != 1 {
		t.Fatalf("restart lost the unknown creation intent: %d %v", count, err)
	}
	e.allowCommit()
	<-e.committed
	if err := restarted.recoverPeerIntents(t.Context()); err != nil {
		t.Fatalf("restart could not reclaim its now-observed exact attempt: %v", err)
	}
	if err := restarted.drainRunEvents(t.Context(), proc.node.RunID); err != nil {
		t.Fatal(err)
	}
	if err := restarted.finishRunWorker(t.Context(), proc.node.RunID); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	present, removed := e.present, e.removed
	e.mu.Unlock()
	if present || removed != 1 {
		t.Fatalf("restarted Agent did not remove the exact late container: present=%t removed=%d", present, removed)
	}
}

func TestDockerUncertainCreatePersistenceFailureRetainsPositiveEvidence(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires directory permission enforcement without root bypass")
	}
	s := historyTestServer(t)
	e := newUncertainEngine(t, s, "normal")
	directory := filepath.Join(s.config.DataDir, "runtime-peer-intents")
	e.mu.Lock()
	e.beforeReply = func() error { return os.Chmod(directory, 0500) }
	e.mu.Unlock()
	t.Cleanup(func() { _ = os.Chmod(directory, 0700) })
	e.allowCommit()
	proc := uncertainPeerProcess(t, s, e)
	assertUncertainPeerOwned(t, s, proc)
	// A live retry must not switch to ID-only removal when persisting that
	// positive ID failed again. The exact container remains observable.
	s.stopRunGeneration(proc.node.RunID, ^uint64(0))
	if err := s.waitRunContainers(t.Context(), proc.node.RunID, ^uint64(0)); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("retry did not preserve the identity persistence failure: %v", err)
	}
	assertUncertainPeerOwned(t, s, proc)
	e.mu.Lock()
	present, removed := e.present, e.removed
	e.mu.Unlock()
	if !present || removed != 0 {
		t.Fatal("failed identity persistence removed the only positive evidence")
	}
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(s.config, s.logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	restarted.client, restarted.docker = s.client, e.runtime()
	e.mu.Lock()
	e.failRemovals = 1
	e.mu.Unlock()
	if err := restarted.recoverPeerIntents(t.Context()); err == nil {
		t.Fatal("fixture did not interrupt removal after durable positive identity")
	}
	intent, err := restarted.readPeerIntent(proc.intentPath)
	if err != nil || intent.ContainerID != fakeContainerID || intent.CleanupConfirmed {
		t.Fatalf("interrupted positive removal lost its durable proof: %+v %v", intent, err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	last, err := New(s.config, s.logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = last.Close() })
	last.client, last.docker = s.client, e.runtime()
	if err := last.recoverPeerIntents(t.Context()); err != nil {
		t.Fatalf("restart lost positive identity persisted before failed removal: %v", err)
	}
	e.mu.Lock()
	present, removed = e.present, e.removed
	e.mu.Unlock()
	if present || removed != 1 {
		t.Fatalf("positive evidence recovery did not remove the exact container: present=%t removed=%d", present, removed)
	}
}

func TestPeerIntentDefinitePreAdmissionFailuresDoNotQuarantine(t *testing.T) {
	for _, stage := range []string{"prepared restart", "canceled before create", "executable not started"} {
		t.Run(stage, func(t *testing.T) {
			s := historyTestServer(t)
			s.config.RunWorkerFactory = agentRunProcessFactory()
			calls := 0
			s.docker.commandContext = func(ctx context.Context, _ string, args ...string) *exec.Cmd {
				calls++
				if args[0] != "create" {
					t.Errorf("definite pre-admission failure issued cleanup: %v", args)
				}
				return exec.CommandContext(ctx, filepath.Join(t.TempDir(), "missing-docker-cli"))
			}
			node := model.Node{ID: "unsent-peer", RunID: "unsent-run", AgentID: s.config.ID, State: model.NodeStarting, StartedAt: time.Now().UTC(), Metadata: map[string]string{"runtime": "docker"}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			proc := &process{node: node, cancel: cancel, done: make(chan struct{})}
			if stage == "prepared restart" {
				if _, err := s.beginPeerIntent(proc, node); err != nil {
					t.Fatal(err)
				}
				if err := s.recoverPeerIntents(t.Context()); err != nil {
					t.Fatalf("unsent prepared intent did not recover without Docker: %v", err)
				}
			} else {
				s.processes[node.ID] = proc
				if stage == "canceled before create" {
					cancel()
				}
				data, _ := json.Marshal(model.PeerProcessConfig{Node: node})
				s.runDockerProcess(ctx, proc, data, "")
				if proc.cleanupErr != nil || s.snapshotAgent().ActiveNodes != 0 {
					t.Fatalf("definitely unsent create retained a false quarantine: %v", proc.cleanupErr)
				}
			}
			wantCalls := 0
			if stage == "executable not started" {
				wantCalls = 1
			}
			if calls != wantCalls {
				t.Fatalf("Docker calls=%d, want=%d", calls, wantCalls)
			}
			if count, err := s.countPeerIntents(node.RunID); err != nil || count != 0 {
				t.Fatalf("definitely unsent intent remained pending: %d %v", count, err)
			}
		})
	}
}

func TestPeerIntentPersistsContainerIDBeforeCopyOrStartup(t *testing.T) {
	s := historyTestServer(t)
	d, _ := fakeDocker(t, nil)
	node, config := dockerTestConfig(t, false)
	node.AgentID = s.config.ID
	node.Metadata = make(map[string]string)
	proc := &process{node: node}
	node, err := s.beginPeerIntent(proc, node)
	if err != nil {
		t.Fatal(err)
	}
	original := d.commandContext
	checked := false
	d.commandContext = func(ctx context.Context, binary string, args ...string) *exec.Cmd {
		if args[0] == "cp" {
			intent, err := s.readPeerIntent(proc.intentPath)
			if err != nil || intent.ContainerID != fakeContainerID || !intent.CreateAttempted || intent.CleanupConfirmed {
				t.Fatalf("startup preceded durable create identity: %+v %v", intent, err)
			}
			checked = true
		}
		return original(ctx, binary, args...)
	}
	_, _, err = d.createWithAdmission(t.Context(), node, config, nil, dockerAdmissionHooks{before: func() error { return s.markPeerIntentAttempted(proc) }, created: func(id string) error { return s.commitPeerIntent(proc, id) }})
	if err != nil || !checked {
		t.Fatalf("normal create did not pass its durable identity boundary: checked=%t err=%v", checked, err)
	}
}

func TestPeerLifetimeIncludesDelayedContainerIdentityPersistence(t *testing.T) {
	s := historyTestServer(t)
	d, logPath := fakeDocker(t, nil)
	node, config := dockerTestConfig(t, false)
	node.AgentID, node.Metadata = s.config.ID, make(map[string]string)
	proc := &process{node: node}
	node, err := s.beginPeerIntent(proc, node)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	recording, release, expired := make(chan time.Time, 1), make(chan struct{}), make(chan time.Time, 1)
	var once sync.Once
	defer once.Do(func() { close(release) })
	done := make(chan error, 1)
	go func() {
		_, _, err := d.createWithAdmission(ctx, node, config, func(createdAt time.Time) {
			scheduleLifetimeStop(ctx, "500ms", createdAt, func() { expired <- createdAt; cancel() })
		}, dockerAdmissionHooks{before: func() error { return s.markPeerIntentAttempted(proc) }, created: func(id string) error {
			recording <- time.Now()
			<-release
			return s.commitPeerIntent(proc, id)
		}})
		done <- err
	}()
	var recordStarted time.Time
	select {
	case recordStarted = <-recording:
	case <-time.After(3 * time.Second):
		t.Fatal("create did not reach durable identity recording")
	}
	// The filesystem boundary remains blocked longer than the whole lifetime.
	// It must consume that lifetime instead of moving the timer's origin.
	time.Sleep(800 * time.Millisecond)
	once.Do(func() { close(release) })
	select {
	case createdAt := <-expired:
		if createdAt.After(recordStarted) {
			t.Errorf("identity persistence moved the container-created clock: created=%s writeStarted=%s", createdAt, recordStarted)
		}
	case <-time.After(250 * time.Millisecond):
		t.Error("identity persistence extended the Peer lifetime beyond its original expiry")
		cancel()
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expired startup did not cancel after identity persistence: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expired startup did not finish its exact-container cleanup")
	}
	intent, err := s.readPeerIntent(proc.intentPath)
	if err != nil || intent.ContainerID != fakeContainerID {
		t.Fatalf("lifetime cancellation preceded durable container identity: %+v %v", intent, err)
	}
	calls := dockerCalls(t, logPath)
	if last := calls[len(calls)-1].Args; last[0] != "rm" || last[len(last)-1] != fakeContainerID {
		t.Fatalf("expired startup did not remove its known exact container: %v", last)
	}
}

func TestLegacyPeerIntentTimestampCannotProveUnknownCreateRemoval(t *testing.T) {
	s := historyTestServer(t)
	e := newUncertainEngine(t, s, "normal")
	s.docker = e.runtime()
	at := time.Now().UTC().Add(-time.Minute)
	node := model.Node{ID: "legacy-peer", RunID: "legacy-run", AgentID: s.config.ID, Generation: 7, State: model.NodeStopped, StartedAt: at.Add(-time.Minute), LastSeen: at, Metadata: map[string]string{"cleanupComplete": "true", "stoppedAt": at.Format(time.RFC3339Nano)}}
	proc := &process{node: node}
	if _, err := s.beginPeerIntent(proc, node); err != nil {
		t.Fatal(err)
	}
	intent, err := s.readPeerIntent(proc.intentPath)
	if err != nil {
		t.Fatal(err)
	}
	intent.Version, intent.CreateAttempted, intent.CleanupConfirmed, intent.TerminatedAt = 1, false, false, at
	data, _ := json.Marshal(intent)
	if err := writeTelemetryFile(proc.intentPath, data); err != nil {
		t.Fatal(err)
	}
	if err := s.history.saveNodes([]model.Node{node}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.identity = uncertainContainerIdentity{ID: fakeContainerID, Name: "/" + intent.AttemptName, Labels: map[string]string{"io.kpl.managed": "true", "io.kpl.network": "peers", "io.kpl.agent": node.AgentID, "io.kpl.run": node.RunID, "io.kpl.node": node.ID, "io.kpl.generation": "7"}}
	e.mu.Unlock()
	if err := s.recoverPeerIntents(t.Context()); !errors.Is(err, errDockerCreateUncertain) {
		t.Fatalf("legacy timestamp and terminal history invented physical cleanup proof: %v", err)
	}
	if current, err := os.ReadFile(proc.intentPath); err != nil || !bytes.Equal(current, data) {
		t.Fatalf("unknown legacy proof was rewritten or removed: %v", err)
	}
	e.mu.Lock()
	e.present = true
	e.mu.Unlock()
	if err := s.recoverPeerIntents(t.Context()); err != nil {
		t.Fatal(err)
	}
	stored, found, err := s.history.storedNode(node.ID)
	if err != nil || !found || stored.State != node.State || !stored.LastSeen.Equal(at) {
		t.Fatalf("physical legacy recovery changed the established scientific outcome: %+v %v %v", stored, found, err)
	}
	events, err := s.spool.loadPrefix()
	if err != nil || len(events) != 1 || events[0].EventID != intent.TerminationID || !events[0].Timestamp.Equal(at) {
		t.Fatalf("legacy recovery changed termination identity or time: %+v %v", events, err)
	}
}

func TestPeerIntentRecoveryContinuesPastUnknownAttempts(t *testing.T) {
	s := historyTestServer(t)
	s.docker, _ = fakeDocker(t, nil)
	var unknownPaths []string
	for _, id := range []string{"unknown-one", "unknown-two"} {
		proc := &process{node: model.Node{ID: id, RunID: "recovery-run", AgentID: s.config.ID, StartedAt: time.Now().UTC(), Metadata: map[string]string{}}}
		if _, err := s.beginPeerIntent(proc, proc.node); err != nil {
			t.Fatal(err)
		}
		if err := s.markPeerIntentAttempted(proc); err != nil {
			t.Fatal(err)
		}
		unknownPaths = append(unknownPaths, proc.intentPath)
	}
	known := &process{node: model.Node{ID: "known-peer", RunID: "recovery-run", AgentID: s.config.ID, StartedAt: time.Now().UTC(), Metadata: map[string]string{}}}
	if _, err := s.beginPeerIntent(known, known.node); err != nil {
		t.Fatal(err)
	}
	if err := s.commitPeerIntent(known, fakeContainerID); err != nil {
		t.Fatal(err)
	}
	prepared := &process{node: model.Node{ID: "prepared-peer", RunID: "recovery-run", AgentID: s.config.ID, StartedAt: time.Now().UTC(), Metadata: map[string]string{}}}
	if _, err := s.beginPeerIntent(prepared, prepared.node); err != nil {
		t.Fatal(err)
	}
	err := s.recoverPeerIntents(t.Context())
	if !errors.Is(err, errDockerCreateUncertain) {
		t.Fatalf("unknown attempts did not preserve their recovery error: %v", err)
	}
	for _, path := range unknownPaths {
		if !strings.Contains(err.Error(), filepath.Base(path)) {
			t.Fatalf("recovery stopped before collecting every unknown attempt: %v", err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("unknown attempt was discarded: %v", err)
		}
	}
	for _, proc := range []*process{known, prepared} {
		if _, err := os.Stat(proc.intentPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("another unknown attempt prevented confirmed recovery: %s %v", proc.node.ID, err)
		}
		if found, err := s.history.contains(proc.node.ID); err != nil || !found {
			t.Fatalf("confirmed recovery lost its terminal identity: %s %v %v", proc.node.ID, found, err)
		}
	}
}

func TestAgentStartupDoesNotBulkRemoveUnresolvedAttemptEvidence(t *testing.T) {
	s := historyTestServer(t)
	s.config.RunWorkerFactory, s.config.AutoResetNetwork, s.config.ControlNetwork = agentRunProcessFactory(), true, "control"
	var logPath string
	s.docker, logPath = fakeDocker(t, nil)
	proc := &process{node: model.Node{ID: "startup-unknown", RunID: "startup-unknown-run", AgentID: s.config.ID, StartedAt: time.Now().UTC(), Metadata: map[string]string{}}}
	if _, err := s.beginPeerIntent(proc, proc.node); err != nil {
		t.Fatal(err)
	}
	if err := s.markPeerIntentAttempted(proc); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := s.serve(t.Context(), listener, nil); !errors.Is(err, errDockerCreateUncertain) {
		t.Fatalf("startup accepted or concealed unresolved create evidence: %v", err)
	}
	for _, call := range dockerCalls(t, logPath) {
		if call.Args[0] == "ps" || call.Args[0] == "rm" {
			t.Fatalf("bulk cleanup could erase a late attempt's only positive evidence: %v", call.Args)
		}
	}
	if s.startupReconciled {
		t.Fatal("unresolved create admitted the Agent as reconciled")
	}
}

func TestDockerUncertainCreateRejectsDifferentObservedIdentity(t *testing.T) {
	for _, field := range []string{"ID", "Name", "managed", "network", "agent", "run", "node", "generation"} {
		t.Run(field, func(t *testing.T) {
			e := newUncertainEngine(t, nil, "normal")
			node := model.Node{ID: "expected-node", RunID: "expected-run", AgentID: "expected-agent", Generation: 3}
			name, err := peerContainerName(node)
			if err != nil {
				t.Fatal(err)
			}
			identity := uncertainContainerIdentity{ID: fakeContainerID, Name: "/" + name, Labels: map[string]string{"io.kpl.managed": "true", "io.kpl.network": "peers", "io.kpl.agent": node.AgentID, "io.kpl.run": node.RunID, "io.kpl.node": node.ID, "io.kpl.generation": "3"}}
			switch field {
			case "ID":
				identity.ID = "invalid"
			case "Name":
				identity.Name += "-other"
			default:
				identity.Labels["io.kpl."+field] = "different"
			}
			e.mu.Lock()
			e.identity, e.present = identity, true
			e.mu.Unlock()
			callback := false
			id, err := e.runtime().removeUncertainCreate(t.Context(), name, node, func(string) error { callback = true; return nil })
			if !errors.Is(err, errDockerCreateUncertain) || id != "" || callback {
				t.Fatalf("different %s was accepted as the original create: id=%s err=%v callback=%t", field, id, err, callback)
			}
			e.mu.Lock()
			present, removed := e.present, e.removed
			e.mu.Unlock()
			if !present || removed != 0 {
				t.Fatal("identity mismatch removed another container")
			}
		})
	}
}

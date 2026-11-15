package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLostReserveReplyCleanupRetiresOnlyNeverAttemptedReservation(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		t.Run(map[bool]string{false: "generation-fence", true: "worker-revoke"}[revoke], func(t *testing.T) { exerciseLostReservationReplyCleanup(t, revoke) })
	}
}
func exerciseLostReservationReplyCleanup(t *testing.T, revoke bool) {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	const id = "lost-reserve-response"
	run, _ := resultFixture(t, s, id, "running", time.Now().UTC())
	s.state.experiments[id] = run
	var owner model.Agent
	var creates, fences, drains atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/status":
			a := owner
			a.LastSeen = time.Now().UTC()
			writeJSON(w, 200, model.AgentHeartbeat{Agent: a})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/runs/"):
			fences.Add(1)
			w.WriteHeader(202)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/drain"):
			drains.Add(1)
			w.WriteHeader(204)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/nodes":
			creates.Add(1)
			http.Error(w, "must not create", 500)
		default:
			http.NotFound(w, r)
		}
	}))
	defer endpoint.Close()
	owner = model.Agent{ID: "agent", URL: endpoint.URL, StartedAt: time.Now().Add(-time.Minute), LastSeen: time.Now(), State: model.AgentOnline, Capacity: 100, RunProcesses: true, RunDrain: true, CreateReplay: true, StartupReconciled: true}
	if _, err := s.state.registerAgent(owner); err != nil {
		t.Fatal(err)
	}
	b := &runWorkerBroker{server: s, runID: id, executionID: run.ExecutionID, generation: 1}
	defer b.close()
	held, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	process, err := workerproc.Start(t.Context(), "controller-placement-deadline", struct{}{}, func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		if method == "draw-held" {
			return nil, nil
		}
		value, err := b.handle(ctx, method, raw)
		if method == "reserve" && err == nil {
			close(held)
			<-release
		}
		return value, err
	}, workerproc.Options{RunID: id, GracePeriod: 100 * time.Millisecond, KillWait: time.Second, Command: func(string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestPlacementDeadlineWorkerHelper$", "--", "placement-deadline-helper")
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		return cmd
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	b.process = process
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- process.Call(ctx, "execute", nil, nil) }()
	select {
	case <-held:
	case <-time.After(2 * time.Second):
		t.Fatal("actual parent reserve did not commit before reply gate")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("child did not lose its reserve acknowledgement: %v", err)
	}
	unblock()
	closeCtx, closeCancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer closeCancel()
	if err := process.CloseContext(closeCtx); err != nil {
		t.Fatal(err)
	}
	if revoke {
		b.revoke()
		if err := s.stopRunGeneration(t.Context(), id, ^uint64(0)); err != nil {
			t.Fatal(err)
		}
		if err := s.drainRunTelemetry(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	} else {
		request, _ := json.Marshal(workerGeneration{RunID: id, Generation: ^uint64(0)})
		if _, err := b.dispatch(t.Context(), "stop-generation", request); err != nil {
			t.Fatal(err)
		}
		if _, err := b.dispatch(t.Context(), "drain", nil); err != nil {
			t.Fatal(err)
		}
	}
	proofCtx, proofCancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer proofCancel()
	proofErr := s.waitRunTerminalEvidence(proofCtx, id)
	s.state.mu.RLock()
	reservation := s.state.reservations[id+"-peer"]
	uncertain := len(s.state.uncertainCreates)
	nodes := len(s.state.nodes)
	occupied := s.state.agents[owner.ID].ActiveNodes
	s.state.mu.RUnlock()
	b.mu.Lock()
	leases := len(b.createLeases)
	b.mu.Unlock()
	stale := model.CreateNodeRequest{ID: id + "-peer", RunID: id, Generation: 1, Group: "stale", Type: "full", AgentStartedAt: owner.StartedAt}
	staleRaw, _ := json.Marshal(stale)
	if _, err := b.dispatchAgent(t.Context(), workerAgentCall{BaseURL: owner.URL, Method: http.MethodPost, Path: "/api/v1/nodes", Input: staleRaw, Output: true}); err == nil {
		t.Fatal("retired reserve capability authorized a late HTTP create")
	}
	if creates.Load() != 0 || fences.Load() != 1 || drains.Load() != 1 {
		t.Fatalf("fixture did not prove no-create/fence/drain: creates=%d fence=%d drain=%d", creates.Load(), fences.Load(), drains.Load())
	}
	if proofErr != nil || reservation != "" || uncertain != 0 || nodes != 0 || occupied != 0 {
		t.Fatalf("confirmed empty Agent/fence/drain cannot resolve never-attempted lost-reply reservation: proof=%v reserved=%q uncertain=%d hotNodes=%d occupied=%d brokerLeases=%d", proofErr, reservation, uncertain, nodes, occupied, leases)
	}
}

func TestWorkerRevokePreservesIndependentAdmissionObligations(t *testing.T) {
	for _, boundary := range []string{"unattempted", "http-lost-ack", "uncertain", "http-inflight", "possible-evidence", "restarted-owner", "changed-endpoint", "other-owner", "live-peer", "missing-capability"} {
		t.Run(boundary, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			f := newBrokerCreateFixture(t, func(_ *brokerCreateFixture, w http.ResponseWriter, _ *http.Request, _ model.CreateNodeRequest) {
				if boundary == "http-inflight" {
					close(entered)
					<-release
				}
				dropCreateResponse(t, w)
			})
			defer f.b.close()
			keep := boundary != "unattempted"
			var inflight chan error
			switch boundary {
			case "http-lost-ack", "uncertain":
				_, err := f.b.dispatchAgent(t.Context(), f.call)
				var unknown *uncertainNodeCreateError
				if !errors.As(err, &unknown) {
					t.Fatalf("fixture did not lose HTTP acknowledgement: %v", err)
				}
				if boundary == "uncertain" {
					f.b.server.retainUncertainCreate(f.request, f.owner.ID)
				}
			case "http-inflight":
				inflight = make(chan error, 1)
				go func() { _, err := f.b.dispatchAgent(t.Context(), f.call); inflight <- err }()
				select {
				case <-entered:
				case <-time.After(2 * time.Second):
					t.Fatal("actual HTTP did not enter")
				}
			case "possible-evidence":
				f.b.createLeases[f.request.ID].possibleAdmission = true
			case "restarted-owner":
				f.mutateAgent(func(owner *model.Agent) { owner.StartedAt = owner.StartedAt.Add(time.Minute) })
			case "changed-endpoint":
				f.mutateAgent(func(owner *model.Agent) { owner.URL += "/changed" })
			case "other-owner":
				other := model.Agent{ID: "agent-two", URL: "http://other.invalid", StartedAt: time.Now(), LastSeen: time.Now(), State: model.AgentOnline, RunProcesses: true, RunDrain: true, ActiveNodes: 1}
				if _, err := f.b.server.state.registerAgent(other); err != nil {
					t.Fatal(err)
				}
				f.b.server.state.mu.Lock()
				f.b.server.state.reservations[f.request.ID] = other.ID
				original := f.b.server.state.agents[f.owner.ID]
				original.ActiveNodes = 0
				f.b.server.state.agents[f.owner.ID] = original
				f.b.server.state.mu.Unlock()
			case "live-peer":
				f.b.server.state.mu.Lock()
				f.b.server.state.setNodeLocked(model.Node{ID: f.request.ID, RunID: f.request.RunID, AgentID: f.owner.ID, Generation: 1, State: model.NodeReady})
				f.b.server.state.mu.Unlock()
			case "missing-capability":
				f.b.releaseCreateLease(f.request.ID)
			}
			f.b.revoke()
			unblock()
			if inflight != nil {
				var unknown *uncertainNodeCreateError
				if err := <-inflight; !errors.As(err, &unknown) {
					t.Fatalf("late HTTP lost its uncertain classification: %v", err)
				}
			}
			f.b.server.state.mu.RLock()
			reserved := f.b.server.state.reservations[f.request.ID]
			unknown := len(f.b.server.state.uncertainCreates)
			occupied := f.b.server.state.agents[f.owner.ID].ActiveNodes
			otherOccupied := f.b.server.state.agents["agent-two"].ActiveNodes
			f.b.server.state.mu.RUnlock()
			if keep != (reserved != "") {
				t.Fatalf("revocation changed an independent admission obligation: reserved=%q boundary=%s", reserved, boundary)
			}
			if boundary == "unattempted" && occupied != 0 {
				t.Fatalf("never-attempted slot stayed occupied: %d", occupied)
			}
			if boundary == "other-owner" && (reserved != "agent-two" || otherOccupied != 1 || occupied != 0) {
				t.Fatalf("old capability changed another owner's reservation: reserved=%s old=%d other=%d", reserved, occupied, otherOccupied)
			}
			if boundary == "uncertain" && unknown != 1 {
				t.Fatal("revocation erased actual uncertain admission evidence")
			}
			if len(f.b.createLeases) != 0 {
				t.Fatal("closed broker retained old create profiles/capabilities")
			}
			before := f.calls.Load()
			if _, err := f.b.dispatchAgent(t.Context(), f.call); err == nil || f.calls.Load() != before {
				t.Fatal("revoked capability reached a late HTTP dispatch")
			}
		})
	}
}

func TestGenerationFencePreservesLaterReservationAndRevokesOnlyOlderCapability(t *testing.T) {
	f := newBrokerCreateFixture(t, nil)
	defer f.b.close()
	future := f.request
	future.ID += "-future"
	future.Generation = 2
	f.b.generation = 2
	raw, _ := json.Marshal(workerReservation{NodeID: future.ID, AgentID: f.owner.ID})
	if _, err := f.b.dispatch(t.Context(), "reserve", raw); err != nil {
		t.Fatal(err)
	}
	stop, _ := json.Marshal(workerGeneration{RunID: f.b.runID, Generation: 1})
	if _, err := f.b.dispatch(t.Context(), "stop-generation", stop); err != nil {
		t.Fatal(err)
	}
	f.b.server.state.mu.RLock()
	old := f.b.server.state.reservations[f.request.ID]
	next := f.b.server.state.reservations[future.ID]
	occupied := f.b.server.state.agents[f.owner.ID].ActiveNodes
	f.b.server.state.mu.RUnlock()
	if old != "" || next != f.owner.ID || occupied != 1 || f.b.createLeases[future.ID] == nil || f.b.createLeases[f.request.ID] != nil || f.b.fenced {
		t.Fatalf("older fence changed future reservation: old=%s next=%s occupied=%d fenced=%t", old, next, occupied, f.b.fenced)
	}
	if _, err := f.b.dispatchAgent(t.Context(), f.call); err == nil || f.calls.Load() != 0 {
		t.Fatal("old generation still reached HTTP")
	}
	f.setRequest(t, future)
	if _, err := f.b.dispatchAgent(t.Context(), f.call); err != nil || f.calls.Load() != 1 {
		t.Fatalf("future generation lost its original capability: %v calls=%d", err, f.calls.Load())
	}
}

func TestReservationBindingAndSlotPublicationAreAtomicWithRetirement(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		for _, canceled := range []bool{false, true} {
			t.Run(fmt.Sprintf("revoke=%t/canceled=%t", revoke, canceled), func(t *testing.T) {
				f := newBrokerCreateFixture(t, nil)
				defer f.b.close()
				f.b.server.releaseReservation(f.request.ID)
				f.b.releaseCreateLease(f.request.ID)
				held, release := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				defer unblock()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				publishedBeforeBind := false
				go func() {
					_, err := f.b.server.acquireAgentWithPlacementBound(ctx, f.request.ID, f.owner.ID, nil, func(agent model.Agent) error {
						publishedBeforeBind = f.b.server.state.reservations[f.request.ID] != ""
						close(held)
						<-release
						return f.b.bindCreateReservation(f.request.ID, agent)
					}, func() error { return f.b.checkCreateReservation(f.request.ID) })
					done <- err
				}()
				select {
				case <-held:
				case <-time.After(2 * time.Second):
					t.Fatal("reservation did not reach its pure-memory bind boundary")
				}
				retired := make(chan error, 1)
				started := make(chan struct{})
				go func() {
					close(started)
					if revoke {
						f.b.revoke()
						retired <- nil
						return
					}
					raw, _ := json.Marshal(workerGeneration{RunID: f.b.runID, Generation: ^uint64(0)})
					_, err := f.b.dispatch(t.Context(), "stop-generation", raw)
					retired <- err
				}()
				<-started
				time.Sleep(10 * time.Millisecond)
				crossed := false
				select {
				case err := <-retired:
					if err != nil {
						t.Fatal(err)
					}
					crossed = true
				default:
				}
				if canceled {
					cancel()
				}
				unblock()
				err := <-done
				if canceled && !errors.Is(err, context.Canceled) || !canceled && err != nil {
					t.Fatalf("bound acquisition changed cancellation: %v", err)
				}
				if !crossed {
					if err := <-retired; err != nil {
						t.Fatal(err)
					}
				}
				f.b.server.state.mu.RLock()
				reserved := f.b.server.state.reservations[f.request.ID]
				occupied := f.b.server.state.agents[f.owner.ID].ActiveNodes
				f.b.server.state.mu.RUnlock()
				if crossed || publishedBeforeBind || reserved != "" || occupied != 0 {
					t.Fatalf("retirement crossed an unbound slot: crossed=%t publishedBeforeBind=%t reserved=%q occupied=%d", crossed, publishedBeforeBind, reserved, occupied)
				}
				if _, err := f.b.dispatchAgent(t.Context(), f.call); err == nil || f.calls.Load() != 0 {
					t.Fatal("retired transaction reached a late HTTP admission")
				}
			})
		}
	}
}

func TestReservationBindingFailureNeverPublishesOrReleasesAnotherSlot(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%t", existing), func(t *testing.T) {
			f := newBrokerCreateFixture(t, nil)
			defer f.b.close()
			if !existing {
				f.b.server.releaseReservation(f.request.ID)
				f.b.releaseCreateLease(f.request.ID)
			}
			original := f.b.createLeases[f.request.ID]
			failure := errors.New("fixture capability binding failed")
			_, err := f.b.server.acquireAgentWithPlacementBound(t.Context(), f.request.ID, f.owner.ID, nil, func(model.Agent) error { return failure })
			if !errors.Is(err, failure) {
				t.Fatalf("binding error was lost: %v", err)
			}
			f.b.server.state.mu.RLock()
			reserved := f.b.server.state.reservations[f.request.ID]
			occupied := f.b.server.state.agents[f.owner.ID].ActiveNodes
			f.b.server.state.mu.RUnlock()
			if existing && (reserved != f.owner.ID || occupied != 1 || f.b.createLeases[f.request.ID] != original) || !existing && (reserved != "" || occupied != 0 || len(f.b.createLeases) != 0) {
				t.Fatalf("failed bind changed reservation ownership: existing=%t reserved=%q occupied=%d", existing, reserved, occupied)
			}
		})
	}
}

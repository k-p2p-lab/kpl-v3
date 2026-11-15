package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

func TestPlacementDeadlineWorkerHelper(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-1] != "placement-deadline-helper" {
		return
	}
	err := workerproc.Serve(context.Background(), "controller-placement-deadline", func(_ context.Context, _ string, _ json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		rng := rand.New(rand.NewSource(42))
		return func(ctx context.Context, method string, _ json.RawMessage) (any, error) {
			switch method {
			case "execute":
				var reply workerReply
				if err := peer.Call(ctx, "reserve", workerReservation{NodeID: peer.RunID() + "-peer", Random: true}, &reply); err != nil {
					return nil, err
				}
				if reply.Error != nil {
					return nil, reply.Error.decode()
				}
				return nil, nil
			case "placement-draw":
				if err := peer.Call(ctx, "draw-held", nil, nil); err != nil {
					return nil, err
				}
				return rng.Int63(), nil
			default:
				return nil, fmt.Errorf("unknown placement operation %s", method)
			}
		}, nil
	})
	if err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

// Hold the actual child RNG callback while delivering a real Controller
// heartbeat. A slow application worker must not own the shared state mutex
// while the parent waits for that callback.
func TestWorkerPlacementDrawDoesNotBlockAgentHeartbeat(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	var heartbeat model.Agent
	for i := range 2 {
		agent := model.Agent{ID: fmt.Sprintf("placement-agent-%d", i), URL: fmt.Sprintf("http://placement-agent-%d", i), StartedAt: time.Now().Add(-time.Minute).UTC(), LastSeen: time.Now().UTC(), State: model.AgentOnline, Capacity: 10, RunProcesses: true, RunDrain: true}
		if _, err := s.state.registerAgent(agent); err != nil {
			t.Fatal(err)
		}
		heartbeat = agent
	}
	broker := &runWorkerBroker{server: s, runID: "placement-heartbeat", generation: 1}
	defer broker.close()
	held, release := make(chan struct{}), make(chan struct{})
	var heldOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	process, err := workerproc.Start(t.Context(), "controller-placement-deadline", struct{}{}, func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		if method == "draw-held" {
			heldOnce.Do(func() { close(held) })
			select {
			case <-release:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return broker.handle(ctx, method, raw)
	}, workerproc.Options{RunID: broker.runID, GracePeriod: 100 * time.Millisecond, KillWait: time.Second, Command: func(string) *exec.Cmd {
		command := exec.Command(os.Args[0], "-test.run=^TestPlacementDeadlineWorkerHelper$", "--", "placement-deadline-helper")
		command.Stdout, command.Stderr = io.Discard, io.Discard
		return command
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	broker.process = process
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- process.Call(ctx, "execute", nil, nil) }()
	select {
	case <-held:
	case err := <-done:
		t.Fatalf("worker did not reach its RNG callback: %v", err)
	case <-ctx.Done():
		t.Fatal("worker did not reach its RNG callback")
	}
	endpoint := httptest.NewServer(http.HandlerFunc(s.handleAgentHeartbeat))
	defer endpoint.Close()
	data, err := json.Marshal(model.AgentHeartbeat{Agent: heartbeat, Partial: true})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 200 * time.Millisecond}
	response, heartbeatErr := client.Post(endpoint.URL, "application/json", bytes.NewReader(data))
	status := 0
	if response != nil {
		status = response.StatusCode
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatalf("released placement failed: %v", err)
	}
	s.releaseReservation(broker.runID + "-peer")
	if heartbeatErr != nil || status != http.StatusNoContent {
		t.Fatalf("slow child placement RNG blocked Agent heartbeat: status=%d error=%v", status, heartbeatErr)
	}
}

type gatedPlacementRandomSource struct {
	rng     *rand.Rand
	held    chan struct{}
	release chan struct{}
	draws   int
	values  []int64
}

func (s *gatedPlacementRandomSource) Seed(seed int64) { s.rng.Seed(seed) }
func (s *gatedPlacementRandomSource) Int63() int64 {
	s.draws++
	if s.draws == 1 {
		close(s.held)
		<-s.release
	}
	if s.draws <= len(s.values) {
		return s.values[s.draws-1]
	}
	return s.rng.Int63()
}

func TestRandomPlacementRechecksStateAfterUnlockedDraw(t *testing.T) {
	for _, boundary := range []string{"capacity", "disabled", "instance", "cooldown", "duplicate", "canceled"} {
		t.Run(boundary, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
			for i := range 2 {
				id := fmt.Sprintf("agent-%d", i)
				s.state.agents[id] = model.Agent{ID: id, URL: "http://" + id, StartedAt: time.Now().Add(-time.Minute), LastSeen: time.Now(), State: model.AgentOnline, Capacity: 10, RunProcesses: true}
			}
			// Seed 42 chooses candidate 1 from these two sorted Agents.
			source := &gatedPlacementRandomSource{rng: rand.New(rand.NewSource(42)), held: make(chan struct{}), release: make(chan struct{})}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var selected model.Agent
			var reserved bool
			done := make(chan struct{})
			go func() {
				selected, reserved = s.tryReserveAgentWithPlacementContext(ctx, "peer", "", rand.New(source))
				close(done)
			}()
			select {
			case <-source.held:
			case <-time.After(time.Second):
				t.Fatal("selection did not reach the random draw")
			}
			if !s.state.mu.TryLock() {
				close(source.release)
				<-done
				t.Fatal("random callback retained Controller state during revalidation")
			}
			current := s.state.agents["agent-1"]
			switch boundary {
			case "capacity":
				current.Capacity, current.ActiveNodes = 1, 1
			case "disabled":
				current.Disabled = true
			case "instance":
				current.StartedAt = current.StartedAt.Add(time.Second)
			case "cooldown":
				s.state.agentAdmissionUntil = map[string]time.Time{current.ID: time.Now().Add(time.Minute)}
			case "duplicate":
				other := s.state.agents["agent-0"]
				other.ActiveNodes = 1
				s.state.agents[other.ID] = other
				s.state.reservations["peer"] = other.ID
			case "canceled":
				cancel()
			}
			s.state.agents[current.ID] = current
			s.state.mu.Unlock()
			close(source.release)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("random selection did not complete after its callback")
			}
			if source.draws != 1 {
				t.Fatalf("state revalidation consumed extra random draws: %d", source.draws)
			}
			if boundary == "duplicate" {
				if !reserved || selected.ID != "agent-0" || s.state.reservations["peer"] != "agent-0" || s.state.agents["agent-0"].ActiveNodes != 1 || s.state.agents["agent-1"].ActiveNodes != 0 {
					t.Fatal("concurrent reservation was replaced or counted twice")
				}
			} else if reserved || len(s.state.reservations) != 0 {
				t.Fatalf("late random selection committed after %s changed: %+v", boundary, selected)
			}
		})
	}
}

func TestUnlockedRandomPlacementPreservesIntnRejectionDraws(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	for i := range 7 {
		id := fmt.Sprintf("agent-%d", i)
		s.state.agents[id] = model.Agent{ID: id, URL: "http://" + id, LastSeen: time.Now(), State: model.AgentOnline, Capacity: 10, RunProcesses: true}
	}
	source := &gatedPlacementRandomSource{rng: rand.New(rand.NewSource(42)), held: make(chan struct{}), release: make(chan struct{}), values: []int64{1<<63 - 1, 0}}
	close(source.release)
	agent, ok := s.tryReserveAgentWithPlacementContext(t.Context(), "peer", "", rand.New(source))
	if !ok || agent.ID != "agent-0" || source.draws != 2 {
		t.Fatalf("unlocked draw changed Intn rejection sampling: agent=%s ok=%v draws=%d", agent.ID, ok, source.draws)
	}
}

func TestFailedRandomPlacementPreservesConcurrentUncertainAdmission(t *testing.T) {
	exerciseConcurrentUncertainPlacement(t, placementAdmissionBoundary{drawFails: true, duplicate: true})
}

func TestSuccessfulRandomPlacementPreservesUncertainDisabledOwner(t *testing.T) {
	exerciseConcurrentUncertainPlacement(t, placementAdmissionBoundary{duplicate: true, disableOwner: true})
}

func TestFailedRandomPlacementCreatesNoReservation(t *testing.T) {
	exerciseConcurrentUncertainPlacement(t, placementAdmissionBoundary{drawFails: true})
}

func TestCanceledRandomPlacementPreservesConcurrentUncertainAdmission(t *testing.T) {
	exerciseConcurrentUncertainPlacement(t, placementAdmissionBoundary{drawFails: true, duplicate: true, cancel: true})
}

func TestLateRandomDrawCannotCommitAfterCancellation(t *testing.T) {
	exerciseConcurrentUncertainPlacement(t, placementAdmissionBoundary{duplicate: true, cancel: true, lateSuccess: true})
}

type placementAdmissionBoundary struct {
	drawFails, duplicate, disableOwner, cancel, lateSuccess bool
}

func exerciseConcurrentUncertainPlacement(t *testing.T, boundary placementAdmissionBoundary) {
	t.Helper()
	f := newBrokerCreateFixture(t, func(_ *brokerCreateFixture, w http.ResponseWriter, _ *http.Request, _ model.CreateNodeRequest) {
		dropCreateResponse(t, w)
	})
	f.b.server.releaseReservation(f.request.ID)
	f.b.releaseCreateLease(f.request.ID)
	f.request.ID = f.b.runID + "-peer"
	f.setRequest(t, f.request)
	if boundary.disableOwner {
		if _, err := f.b.server.state.registerAgent(model.Agent{ID: "agent-two", URL: "http://agent-two", State: model.AgentOnline, Capacity: 10, StartedAt: time.Now().Add(-time.Minute), LastSeen: time.Now(), RunProcesses: true, RunDrain: true, CreateReplay: true}); err != nil {
			t.Fatal(err)
		}
	}
	defer f.b.close()
	held, release, callbackDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	process, err := workerproc.Start(t.Context(), "controller-placement-deadline", struct{}{}, func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		if method == "draw-held" {
			defer close(callbackDone)
			close(held)
			if boundary.lateSuccess {
				// Model a handler which has accepted work and returns success
				// after the requesting RPC has already been canceled.
				<-release
				return nil, nil
			}
			select {
			case <-release:
				if boundary.drawFails {
					return nil, errors.New("fixture random callback failed")
				}
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return f.b.handle(ctx, method, raw)
	}, workerproc.Options{RunID: f.b.runID, GracePeriod: 100 * time.Millisecond, KillWait: time.Second, Command: func(string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestPlacementDeadlineWorkerHelper$", "--", "placement-deadline-helper")
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		return cmd
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	f.b.process = process
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- process.Call(ctx, "execute", nil, nil) }()
	select {
	case <-held:
	case err := <-done:
		t.Fatalf("first placement did not reach its callback: %v", err)
	case <-ctx.Done():
		t.Fatal("first placement did not reach its callback")
	}
	// A second request reserves the same Node while the first callback is
	// unresolved, then crosses the real HTTP admission boundary with a lost ACK.
	if boundary.duplicate {
		reserve, _ := json.Marshal(workerReservation{NodeID: f.request.ID, AgentID: f.owner.ID})
		if _, err := f.b.dispatch(ctx, "reserve", reserve); err != nil {
			t.Fatal(err)
		}
		_, err = f.b.dispatchAgent(ctx, f.call)
		var uncertain *uncertainNodeCreateError
		if !errors.As(err, &uncertain) || f.calls.Load() != 1 {
			t.Fatalf("fixture did not create an uncertain HTTP admission: %v", err)
		}
		f.b.server.retainUncertainCreate(f.request, f.owner.ID)
		if boundary.disableOwner {
			f.mutateAgent(func(owner *model.Agent) { owner.Disabled = true })
		}
	}
	if boundary.cancel {
		cancel()
	}
	releaseOnce.Do(func() { close(release) })
	executeErr := <-done
	if executeErr == nil {
		t.Fatal("failed or canceled random callback was ignored")
	}
	if boundary.drawFails && !boundary.cancel && !strings.Contains(executeErr.Error(), "fixture random callback failed") {
		t.Fatalf("original callback error was replaced: %v", executeErr)
	}
	if boundary.drawFails && !boundary.cancel && errors.Is(executeErr, context.Canceled) {
		t.Fatalf("internal acquisition cancellation replaced a real callback failure: %v", executeErr)
	}
	closeCtx, closeCancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer closeCancel()
	if err := process.CloseContext(closeCtx); err != nil {
		t.Fatalf("worker did not reach OS/private RPC quiescence: %v", err)
	}
	select {
	case <-callbackDone:
	default:
		t.Fatal("worker close returned before the late callback completed")
	}
	f.b.server.state.mu.RLock()
	reserved := f.b.server.state.reservations[f.request.ID]
	occupied := f.b.server.state.agents[f.owner.ID].ActiveNodes
	unknown := f.b.server.state.uncertainCreates[f.request.ID]
	reservationCount, uncertainCount := len(f.b.server.state.reservations), len(f.b.server.state.uncertainCreates)
	f.b.server.state.mu.RUnlock()
	if !boundary.duplicate {
		if reservationCount != 0 || occupied != 0 || uncertainCount != 0 || f.calls.Load() != 0 {
			t.Fatalf("a failed draw created a reservation or HTTP admission: reserved=%q count=%d occupied=%d uncertain=%+v calls=%d", reserved, reservationCount, occupied, unknown, f.calls.Load())
		}
	} else if reserved != f.owner.ID || occupied != 1 || unknown.runID != f.request.RunID || reservationCount != 1 || uncertainCount != 1 || f.calls.Load() != 1 {
		t.Fatalf("a failed earlier draw released another admitted request's cleanup barrier: reserved=%q occupied=%d uncertain=%+v", reserved, occupied, unknown)
	}
}

func TestReservationRollbackKeepsConcurrentAdmissionEvidence(t *testing.T) {
	for _, boundary := range []string{"unattempted", "attempted", "uncertain", "revoked", "unregistered"} {
		t.Run(boundary, func(t *testing.T) {
			f := newBrokerCreateFixture(t, func(_ *brokerCreateFixture, w http.ResponseWriter, _ *http.Request, _ model.CreateNodeRequest) {
				dropCreateResponse(t, w)
			})
			if boundary == "attempted" || boundary == "uncertain" || boundary == "revoked" {
				_, err := f.b.dispatchAgent(t.Context(), f.call)
				var pending *uncertainNodeCreateError
				if !errors.As(err, &pending) {
					t.Fatalf("fixture did not cross possible HTTP admission: %v", err)
				}
			}
			switch boundary {
			case "uncertain":
				f.b.server.retainUncertainCreate(f.request, f.owner.ID)
				f.b.releaseCreateLease(f.request.ID)
			case "revoked":
				f.b.revoke()
			case "unregistered":
				f.b.server.state.mu.Lock()
				delete(f.b.server.state.agents, f.owner.ID)
				f.b.server.state.mu.Unlock()
			}
			f.b.releaseUnattemptedReservation(f.request.ID)
			f.b.server.state.mu.RLock()
			reserved := f.b.server.state.reservations[f.request.ID]
			owner, registered := f.b.server.state.agents[f.owner.ID]
			f.b.server.state.mu.RUnlock()
			if boundary == "unattempted" || boundary == "unregistered" {
				if reserved != "" || boundary == "unregistered" && registered || registered && owner.ActiveNodes != 0 {
					t.Fatalf("safe rollback retained a slot or invented an Agent: reserved=%q registered=%v owner=%+v", reserved, registered, owner)
				}
			} else if reserved != f.owner.ID || owner.ActiveNodes != 1 {
				t.Fatalf("rollback discarded concurrent possible admission at %s: reserved=%q owner=%+v", boundary, reserved, owner)
			}
		})
	}
}

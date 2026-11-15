package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
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

type attemptedDebtWorkerStart struct {
	Owner   model.Agent
	Request model.CreateNodeRequest
}

func TestAttemptedDebtWorkerHelper(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-1] != "attempted-debt-helper" {
		return
	}
	factory := func(ctx context.Context, kind string, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		var start attemptedDebtWorkerStart
		if err := json.Unmarshal(raw, &start); err != nil {
			return nil, err
		}
		child := New(ServerConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		c := &runWorkerClient{peer: peer, server: child, runID: peer.RunID(), ctx: ctx}
		child.worker, child.state.worker = c, c
		child.state.agents[start.Owner.ID] = start.Owner
		child.state.experiments[start.Request.RunID] = model.Experiment{ID: start.Request.RunID, State: "running"}
		return func(callCtx context.Context, method string, raw json.RawMessage) (any, error) {
			if method != "execute" {
				return nil, errors.New("wrong operation")
			}
			return nil, child.createReservedNode(callCtx, start.Request, start.Owner, "", nil)
		}, nil
	}
	if err := workerproc.Serve(context.Background(), "controller-attempted-debt-test", factory); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

// Run the production child create path through actual private IPC and HTTP. An
// entered HTTP request may create no Peer, so an empty fenced/drained inventory
// cannot rely on terminal history to consume its abandoned reservation.
func TestAttemptedReservationWorkerLossCleanup(t *testing.T) {
	for _, mode := range []string{"revoke-no-admission", "generation-fence-no-admission", "revoke-admitted-history", "generation-fence-admitted-history", "delivered-unknown", "revoke-positive503"} {
		t.Run(mode, func(t *testing.T) {
			parent := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			const id = "attempted-debt"
			run, _ := resultFixture(t, parent, id, "running", time.Now().UTC())
			parent.state.experiments[id] = run
			entered, release := make(chan struct{}), make(chan struct{})
			refusalKnown := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var mu sync.Mutex
			var owner model.Agent
			var physical, creates, fences, drains, statuses int
			fenced := false
			definiteRefusal := mode == "revoke-positive503"
			noAdmission := strings.Contains(mode, "no-admission") || definiteRefusal
			var terminal []model.Node
			var request model.CreateNodeRequest
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/api/v1/nodes":
					var req model.CreateNodeRequest
					if json.NewDecoder(r.Body).Decode(&req) != nil {
						http.Error(w, "decode", 400)
						return
					}
					mu.Lock()
					creates++
					request = req
					if !fenced && !noAdmission {
						physical++
					}
					mu.Unlock()
					close(entered)
					if definiteRefusal {
						w.Header().Set(model.AgentAdmissionRetryHeader, "not-created")
						w.Header().Set("Retry-After", "1")
						http.Error(w, "telemetry backlog prevented admission", http.StatusServiceUnavailable)
						return
					}
					<-release
					if noAdmission {
						select {
						case <-r.Context().Done():
						case <-time.After(time.Second):
							t.Error("NoAdmission control did not observe canceled HTTP before admission")
						}
						return
					}
					if mode == "delivered-unknown" {
						http.Error(w, "unknown after possible physical admission", 500)
						return
					}
					writeJSON(w, 201, model.Node{ID: req.ID, RunID: req.RunID, Generation: req.Generation, AgentID: owner.ID, Group: req.Group, State: model.NodeReady})
				case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/runs/"):
					mu.Lock()
					fences++
					fenced = true
					if physical > 0 {
						terminal = []model.Node{{ID: request.ID, RunID: request.RunID, Generation: request.Generation, Group: request.Group, AgentID: owner.ID, State: model.NodeStopped, Metadata: map[string]string{"cleanupComplete": "true"}}}
					}
					physical = 0
					mu.Unlock()
					w.WriteHeader(202)
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/drain"):
					mu.Lock()
					drains++
					empty := fenced && physical == 0
					mu.Unlock()
					if !empty {
						http.Error(w, "not empty", 503)
						return
					}
					w.WriteHeader(204)
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/status":
					mu.Lock()
					statuses++
					a := owner
					a.LastSeen = time.Now().UTC()
					a.ActiveNodes = physical
					history := append([]model.Node(nil), terminal...)
					mu.Unlock()
					writeJSON(w, 200, model.AgentHeartbeat{Agent: a, Nodes: history})
				default:
					http.NotFound(w, r)
				}
			}))
			defer endpoint.Close()
			defer unblock()
			owner = model.Agent{ID: "agent", URL: endpoint.URL, StartedAt: time.Now().Add(-time.Minute).UTC(), LastSeen: time.Now().UTC(), State: model.AgentOnline, Capacity: 100, RunProcesses: true, RunDrain: true, CreateReplay: true, StartupReconciled: true}
			if _, err := parent.state.registerAgent(owner); err != nil {
				t.Fatal(err)
			}
			b := &runWorkerBroker{server: parent, runID: id, executionID: run.ExecutionID, generation: 1}
			defer b.close()
			req := model.CreateNodeRequest{ID: id + "-peer", RunID: id, Generation: 1, Group: "boot", Type: "full", AgentStartedAt: owner.StartedAt}
			raw, _ := json.Marshal(workerReservation{NodeID: req.ID, AgentID: owner.ID})
			if _, err := b.dispatch(t.Context(), "reserve", raw); err != nil {
				t.Fatal(err)
			}
			handler := b.handle
			if definiteRefusal {
				handler = func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
					value, err := b.handle(ctx, method, raw)
					if method == "agent" {
						// Lose only the private reply, after the parent received the
						// Agent's definite pre-admission refusal.
						close(refusalKnown)
						select {
						case <-ctx.Done():
						case <-release:
						}
					}
					return value, err
				}
			}
			p, err := workerproc.Start(t.Context(), "controller-attempted-debt-test", attemptedDebtWorkerStart{owner, req}, handler, workerproc.Options{RunID: id, GracePeriod: 100 * time.Millisecond, KillWait: time.Second, Command: func(string) *exec.Cmd {
				cmd := exec.Command(os.Args[0], "-test.run=^TestAttemptedDebtWorkerHelper$", "--", "attempted-debt-helper")
				cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
				return cmd
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			b.process = p
			execute := make(chan error, 1)
			go func() { execute <- p.Call(t.Context(), "execute", nil, nil) }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("real child production create did not enter HTTP")
			}
			if definiteRefusal {
				select {
				case <-refusalKnown:
				case <-time.After(3 * time.Second):
					t.Fatal("parent did not receive the definite admission refusal")
				}
			}
			parent.state.mu.RLock()
			beforeUnknown := len(parent.state.uncertainCreates)
			beforeR := parent.state.reservations[req.ID]
			parent.state.mu.RUnlock()
			b.mu.Lock()
			l := b.createLeases[req.ID]
			attempted, inflight := l != nil && l.attempted, 0
			if l != nil {
				inflight = l.inFlight
			}
			b.mu.Unlock()
			wantInFlight := 1
			if definiteRefusal {
				wantInFlight = 0
			}
			if attempted == definiteRefusal || inflight != wantInFlight || beforeR != owner.ID {
				t.Fatalf("not at attempted boundary: attempted=%t inFlight=%d R=%q unknown=%d", attempted, inflight, beforeR, beforeUnknown)
			}
			executed := false
			if mode == "delivered-unknown" {
				unblock()
				if err := <-execute; err == nil {
					t.Fatal("unknown HTTP did not reach production child error path")
				}
				executed = true
				parent.state.mu.RLock()
				n := len(parent.state.uncertainCreates)
				parent.state.mu.RUnlock()
				if n != 1 {
					t.Fatalf("production child did not publish independent debt: %d", n)
				}
				b.revoke()
			} else if strings.HasPrefix(mode, "generation-fence") {
				f, _ := json.Marshal(workerGeneration{RunID: id, Generation: ^uint64(0)})
				if _, err := b.dispatch(t.Context(), "stop-generation", f); err != nil {
					t.Fatal(err)
				}
			} else {
				b.revoke()
			}
			if definiteRefusal {
				parent.state.mu.RLock()
				reserved, unknown := parent.state.reservations[req.ID], len(parent.state.uncertainCreates)
				parent.state.mu.RUnlock()
				if reserved != "" || unknown != 0 {
					t.Fatalf("definite no-create response gained cleanup debt: reserved=%q unknown=%d", reserved, unknown)
				}
			}
			closeCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			closeErr := p.CloseContext(closeCtx)
			cancel()
			if closeErr != nil {
				t.Fatal(closeErr)
			}
			select {
			case <-p.Done():
			default:
				t.Fatal("child callbacks not quiescent")
			}
			if !executed {
				<-execute
			}
			unblock()
			if !strings.HasPrefix(mode, "generation-fence") {
				if err := parent.stopRunGeneration(t.Context(), id, ^uint64(0)); err != nil {
					t.Fatal(err)
				}
			}
			if err := parent.drainRunTelemetry(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			proofCtx, proofCancel := context.WithTimeout(t.Context(), time.Second)
			proofErr := parent.waitRunTerminalEvidence(proofCtx, id)
			proofCancel()
			parent.state.mu.RLock()
			reserved := parent.state.reservations[req.ID]
			unknown := len(parent.state.uncertainCreates)
			nodes := len(parent.state.nodes)
			unconfirmed := 0
			for _, n := range parent.state.nodes {
				if !confirmedHistoricalNode(n) {
					unconfirmed++
				}
			}
			occupied := parent.state.agents[owner.ID].ActiveNodes
			parent.state.mu.RUnlock()
			mu.Lock()
			counts := [5]int{physical, creates, fences, drains, statuses}
			mu.Unlock()
			t.Logf("beforeUnknown=%d attempted=%t inFlight=%d afterR=%q unknown=%d hotNodes=%d occupied=%d physical/create/fence/drain/status=%v unconfirmed=%d proof=%v", beforeUnknown, attempted, inflight, reserved, unknown, nodes, occupied, counts, unconfirmed, proofErr)
			if proofErr != nil || reserved != "" || unknown != 0 || unconfirmed != 0 || occupied != 0 || counts[0] != 0 || counts[1] != 1 || counts[2] != 1 || counts[3] != 1 || counts[4] < 1 {
				t.Fatalf("actual child lost admission error report and confirmed fence/drain/empty status did not resolve reservation")
			}
			// A callback already admitted before revoke can arrive after proof.
			// It must not recreate an unknown entry after the slot was consumed.
			late, _ := json.Marshal(workerCreated{Request: req, AgentID: owner.ID})
			if _, err := b.dispatch(t.Context(), "uncertain", late); err != nil {
				t.Fatal(err)
			}
			parent.state.mu.RLock()
			lateReserved, lateUnknown := parent.state.reservations[req.ID], len(parent.state.uncertainCreates)
			parent.state.mu.RUnlock()
			if lateReserved != "" || lateUnknown != 0 {
				t.Fatalf("late worker error recreated resolved debt: reserved=%q unknown=%d", lateReserved, lateUnknown)
			}
		})
	}
}

func attemptedDebtIdentityFixture(t *testing.T, childReported bool) *brokerCreateFixture {
	t.Helper()
	f := newBrokerCreateFixture(t, func(_ *brokerCreateFixture, w http.ResponseWriter, _ *http.Request, _ model.CreateNodeRequest) {
		// An ordinary 5xx cannot prove whether admission occurred.
		http.Error(w, "interrupted admission", http.StatusInternalServerError)
	})
	t.Cleanup(func() { _ = f.b.close() })
	// Bind the capability after giving the Agent an original host identity.
	// Neither a later registration nor a child report may rewrite it.
	raw, _ := json.Marshal(f.request.ID)
	if _, err := f.b.dispatch(t.Context(), "release", raw); err != nil {
		t.Fatal(err)
	}
	f.owner.Hostname = "original-host"
	f.owner.Labels = map[string]string{"swarmNodeId": "original-node"}
	f.mutateAgent(func(agent *model.Agent) {
		agent.Hostname, agent.Labels = f.owner.Hostname, f.owner.Labels
	})
	raw, _ = json.Marshal(workerReservation{NodeID: f.request.ID, AgentID: f.owner.ID})
	if _, err := f.b.dispatch(t.Context(), "reserve", raw); err != nil {
		t.Fatal(err)
	}
	_, err := f.b.dispatchAgent(t.Context(), f.call)
	var unknown *uncertainNodeCreateError
	if !errors.As(err, &unknown) || f.calls.Load() != 1 {
		t.Fatalf("actual HTTP did not produce unresolved admission: calls=%d err=%v", f.calls.Load(), err)
	}
	if childReported {
		raw, _ = json.Marshal(workerCreated{Request: f.request, AgentID: f.owner.ID})
		if _, err := f.b.dispatch(t.Context(), "uncertain", raw); err != nil {
			t.Fatal(err)
		}
	}
	f.b.revoke()
	f.b.server.state.mu.RLock()
	debt, exists := f.b.server.state.uncertainCreates[f.request.ID]
	f.b.server.state.mu.RUnlock()
	if !exists || debt.owner != createAgentIdentityOf(f.owner) {
		t.Fatalf("lost the original HTTP target identity: exists=%t debt=%+v original=%+v", exists, debt.owner, createAgentIdentityOf(f.owner))
	}
	return f
}

func TestAttemptedDebtCleanupRequiresMatchingAgentFenceProof(t *testing.T) {
	for _, childReported := range []bool{false, true} {
		origin := "broker-transfer"
		if childReported {
			origin = "child-unknown-first"
		}
		t.Run(origin, func(t *testing.T) {
			for _, mode := range []string{"original-instance", "matching-reconciled-replacement", "replacement-not-reconciled", "different-host", "missing-swarm-node-id", "process-capability-downgrade", "drain-capability-downgrade", "registration-changed-during-fence", "missing-current-registration", "older-generation"} {
				t.Run(mode, func(t *testing.T) {
					f := attemptedDebtIdentityFixture(t, childReported)
					proof, current := f.owner, f.owner
					generation := ^uint64(0)
					accept := mode == "original-instance" || mode == "matching-reconciled-replacement"
					if mode == "matching-reconciled-replacement" || mode == "replacement-not-reconciled" || mode == "different-host" || mode == "missing-swarm-node-id" {
						proof.StartedAt = proof.StartedAt.Add(time.Minute)
						proof.StartupReconciled = mode != "replacement-not-reconciled"
					}
					switch mode {
					case "different-host":
						proof.Hostname, proof.Labels = "another-host", map[string]string{"swarmNodeId": "another-node"}
					case "missing-swarm-node-id":
						proof.Labels = nil // A matching hostname cannot downgrade a recorded node ID.
					case "process-capability-downgrade":
						proof.RunProcesses = false
					case "drain-capability-downgrade":
						proof.RunDrain = false
					case "older-generation":
						generation = 0
					}
					current = proof
					switch mode {
					case "registration-changed-during-fence":
						current.StartedAt = current.StartedAt.Add(time.Minute)
						current.StartupReconciled = true
					case "missing-current-registration":
						current = model.Agent{}
					}
					// Model the captured successful HTTP target and the registered
					// view at commit separately. Normal registerAgent restart policy
					// is tested elsewhere and can retire these entries beforehand.
					f.b.server.state.mu.Lock()
					if current.ID == "" {
						delete(f.b.server.state.agents, f.owner.ID)
					} else {
						current.ActiveNodes = 1
						f.b.server.state.agents[current.ID] = current
					}
					f.b.server.state.mu.Unlock()
					f.b.server.releaseUncertainCreates(f.request.RunID, generation, map[string]model.Agent{proof.ID: proof})
					f.b.server.state.mu.RLock()
					reserved := f.b.server.state.reservations[f.request.ID]
					_, unknown := f.b.server.state.uncertainCreates[f.request.ID]
					occupied := f.b.server.state.agents[f.owner.ID].ActiveNodes
					f.b.server.state.mu.RUnlock()
					if accept {
						if reserved != "" || unknown || occupied != 0 {
							t.Fatalf("matching fence proof did not release old debt: reserved=%q unknown=%t occupied=%d", reserved, unknown, occupied)
						}
					} else if reserved != f.owner.ID || !unknown || current.ID != "" && occupied != 1 {
						t.Fatalf("unrelated proof released admission debt: reserved=%q unknown=%t occupied=%d", reserved, unknown, occupied)
					}
				})
			}
		})
	}
}

func TestNeverAttemptedReservationAgentRestartBeforeWorkerRevoke(t *testing.T) {
	f := newBrokerCreateFixture(t, nil)
	defer f.b.close()
	var replacement model.Agent
	var mu sync.Mutex
	var creates, fences, drains, statuses int
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/status":
			statuses++
			a := replacement
			a.LastSeen = time.Now().UTC()
			writeJSON(w, http.StatusOK, model.AgentHeartbeat{Agent: a})
		case r.Method == http.MethodDelete:
			fences++
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/drain"):
			drains++
			w.WriteHeader(http.StatusNoContent)
		default:
			creates++
			http.Error(w, "no create may be dispatched", http.StatusInternalServerError)
		}
	}))
	defer endpoint.Close()
	// A lost reserve reply never reached the Agent's create endpoint. Let the
	// actual registration policy accept a newer reconciled instance on that host.
	f.b.server.state.mu.Lock()
	old := f.b.server.state.agents[f.owner.ID]
	old.LastSeen = time.Now().Add(-agentStaleAfter - time.Second)
	old.Hostname, old.Labels = "same-host", map[string]string{"swarmNodeId": "same-node"}
	f.b.server.state.agents[old.ID] = old
	f.b.server.state.mu.Unlock()
	replacement = old
	replacement.URL, replacement.StartedAt = endpoint.URL, old.StartedAt.Add(time.Minute)
	replacement.LastSeen, replacement.ActiveNodes, replacement.StartupReconciled = time.Now().UTC(), 0, true
	if _, err := f.b.server.state.registerAgent(replacement); err != nil {
		t.Fatal(err)
	}
	f.b.revoke()
	if err := f.b.server.stopRunGeneration(t.Context(), f.b.runID, ^uint64(0)); err != nil {
		t.Fatal(err)
	}
	if err := f.b.server.drainRunTelemetry(t.Context(), f.b.runID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := f.b.server.waitRunTerminalEvidence(ctx, f.b.runID); err != nil {
		t.Fatal(err)
	}
	f.b.server.state.mu.RLock()
	reserved, unknown, occupied := len(f.b.server.state.reservations), len(f.b.server.state.uncertainCreates), f.b.server.state.agents[replacement.ID].ActiveNodes
	f.b.server.state.mu.RUnlock()
	mu.Lock()
	counts := [4]int{creates, fences, drains, statuses}
	mu.Unlock()
	if reserved != 0 || unknown != 0 || occupied != 0 || f.calls.Load() != 0 || counts[0] != 0 || counts[1] != 1 || counts[2] != 1 || counts[3] == 0 {
		t.Fatalf("legitimate Agent restart retained an unused old slot: reserved=%d unknown=%d occupied=%d oldCreates=%d newCreate/fence/drain/status=%v", reserved, unknown, occupied, f.calls.Load(), counts)
	}
}

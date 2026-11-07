package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/auth"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type controllerHostRebootFixture struct {
	server     *Server
	api        *httptest.Server
	handler    http.Handler
	agent      model.Agent
	requests   atomic.Int32
	proofs     atomic.Int32
	cancel     context.CancelFunc
	physicalMu sync.RWMutex
	physical   model.Agent
}

func newControllerHostRebootFixture(t *testing.T, reply func(*controllerHostRebootFixture, http.ResponseWriter, *http.Request, model.HostRebootRequest)) *controllerHostRebootFixture {
	t.Helper()
	f := &controllerHostRebootFixture{server: New(ServerConfig{DataDir: t.TempDir(), Token: "fake-peer-service-key", HostRebootToken: "fake-host-management-key"}, nil)}
	f.api = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/host/reboot-status" {
			f.proofs.Add(1)
			var input model.HostRebootProofRequest
			if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&input) != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			actor := f.physicalAgent()
			proof := model.HostRebootProof{Nonce: input.Nonce, AgentID: actor.ID, NodeID: actor.Host.NodeID, BootID: actor.Host.BootID,
				AgentStartedAt: actor.StartedAt.UTC(), StartupReconciled: actor.StartupReconciled,
				RebootPending: actor.Host.RebootPending, RebootSupported: actor.Host.RebootSupported}
			canonical, _ := json.Marshal(proof)
			proof.Signature = auth.HostRebootStatusSignature(f.server.config.HostRebootToken, canonical)
			_ = json.NewEncoder(w).Encode(proof)
			return
		}
		if r.URL.Path == "/api/v1/host/reboot" {
			f.requests.Add(1)
			var input model.HostRebootRequest
			if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&input) != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if reply != nil {
				reply(f, w, r, input)
				return
			}
			controllerHostRebootWriteACK(w, f.server.config.HostRebootToken, input, "rejected", "simulated refusal")
			return
		}
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: f.agent})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	f.agent = controllerHostRebootAgent("agent-one", "node-one", f.api.URL)
	f.setPhysicalAgent(f.agent)
	if _, err := f.server.state.registerAgent(f.agent); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel, f.handler = cancel, f.server.apiTestHandler(ctx)
	t.Cleanup(func() {
		cancel()
		f.server.workWorkers.Wait()
		f.api.Close()
	})
	return f
}

// Signed physical evidence is intentionally separate from the service-key
// registry. An untrusted heartbeat must not become its own reboot proof.
func (f *controllerHostRebootFixture) setPhysicalAgent(agent model.Agent) {
	f.physicalMu.Lock()
	defer f.physicalMu.Unlock()
	f.physical = cloneAgentHost(agent)
}

func (f *controllerHostRebootFixture) physicalAgent() model.Agent {
	f.physicalMu.RLock()
	defer f.physicalMu.RUnlock()
	return cloneAgentHost(f.physical)
}

func controllerHostRebootWriteACK(w http.ResponseWriter, key string, input model.HostRebootRequest, state, message string) {
	input.AgentStartedAt = input.AgentStartedAt.UTC()
	requestBytes, _ := json.Marshal(input)
	result := model.HostRebootResult{OperationID: input.OperationID, NodeID: input.NodeID, BootID: input.BootID, State: state, Error: message}
	resultBytes, _ := json.Marshal(result)
	result.Signature = auth.HostRebootAckSignature(key, requestBytes, resultBytes)
	_ = json.NewEncoder(w).Encode(result)
}

func controllerHostRebootAgent(id, nodeID, endpoint string) model.Agent {
	return model.Agent{ID: id, Name: id, URL: endpoint, Hostname: "container-" + id, Capacity: 8,
		State: model.AgentOnline, StartedAt: time.Now().Add(-time.Hour).UTC(), LastSeen: time.Now().UTC(),
		StartupReconciled: true, RunDrain: true, Labels: map[string]string{"swarmNodeId": nodeID},
		Host: &model.AgentHost{NodeID: nodeID, Hostname: "linux-" + nodeID, BootID: "boot-original", RebootEnabled: true, RebootSupported: true}}
}

func controllerHostRebootTarget(agent model.Agent) hostRebootTarget {
	return hostRebootTarget{AgentID: agent.ID, NodeID: agent.Host.NodeID, BootID: agent.Host.BootID, AgentStartedAt: agent.StartedAt}
}

func controllerHostRebootPost(t *testing.T, handler http.Handler, id string, targets []hostRebootTarget, requestCtx context.Context) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(hostRebootSubmission{ID: id, Confirm: true, Targets: targets})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/agent-hosts/reboot", bytes.NewReader(data))
	if requestCtx != nil {
		request = request.WithContext(requestCtx)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-KPL-Request", "dashboard")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func controllerHostRebootStatus(t *testing.T, server *Server, id string) hostRebootStatus {
	t.Helper()
	server.hostRebootMu.Lock()
	defer server.hostRebootMu.Unlock()
	job, err := server.loadHostRebootLocked(id)
	if err != nil {
		t.Fatal(err)
	}
	return cloneHostRebootStatus(job.status)
}

func awaitControllerHostReboot(t *testing.T, server *Server, id string, ready func(hostRebootStatus) bool) hostRebootStatus {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		status := controllerHostRebootStatus(t, server, id)
		if ready(status) {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("host reboot did not reach expected state: %+v", status)
		}
		time.Sleep(time.Millisecond)
	}
}

func controllerHostRebootTerminal(status hostRebootStatus) bool {
	return status.State != "queued" && status.State != "running"
}

func controllerHostRebootWork(t *testing.T, server *Server, id string) model.WorkQueueItem {
	t.Helper()
	for _, item := range server.state.snapshot().WorkQueue {
		if item.OperationID == id {
			return item
		}
	}
	t.Fatal("host reboot is absent from shared execution queue")
	return model.WorkQueueItem{}
}

func TestHostRebootSharesFIFOWithExperimentsAndAnalysis(t *testing.T) {
	called := make(chan struct{})
	f := newControllerHostRebootFixture(t, func(f *controllerHostRebootFixture, w http.ResponseWriter, _ *http.Request, input model.HostRebootRequest) {
		close(called)
		controllerHostRebootWriteACK(w, f.server.config.HostRebootToken, input, "requested", "")
	})
	head := queueTestEnqueue(t, f.server, "experiment")
	queueTestAwait(t, head.entered, "preceding experiment")
	const operation = "reboot-fifo-1"
	if response := controllerHostRebootPost(t, f.handler, operation, []hostRebootTarget{controllerHostRebootTarget(f.agent)}, nil); response.Code != http.StatusAccepted {
		t.Fatalf("reboot reservation failed: %d %s", response.Code, response.Body)
	}
	analysis := queueTestEnqueue(t, f.server, "analysis")
	tail := queueTestEnqueue(t, f.server, "experiment")
	var kinds []string
	for _, item := range f.server.state.snapshot().WorkQueue {
		kinds = append(kinds, item.Kind)
	}
	if !slices.Equal(kinds, []string{"experiment", "host-reboot", "analysis", "experiment"}) || f.requests.Load() != 0 {
		t.Fatalf("host reboot escaped the FIFO: %v requests=%d", kinds, f.requests.Load())
	}
	f.server.state.mu.RLock()
	disabledEarly := f.server.state.agents[f.agent.ID].Disabled
	f.server.state.mu.RUnlock()
	if disabledEarly {
		t.Fatal("queued host reboot disabled an Agent during the preceding experiment")
	}
	head.once.Do(func() { close(head.release) })
	queueTestAwait(t, called, "host reboot dispatch")
	status := awaitControllerHostReboot(t, f.server, operation, func(status hostRebootStatus) bool { return status.Hosts[0].State == "requested" })
	select {
	case <-analysis.entered:
		t.Fatal("analysis overlapped the pending Linux host reboot")
	default:
	}
	if status.Hosts[0].RequestedAt == nil {
		t.Fatal("dispatch was not timestamped before the host command")
	}
	replacement := controllerHostRebootAgent("replacement-agent", f.agent.Host.NodeID, f.api.URL)
	replacement.Host.BootID, replacement.StartedAt = "boot-after-reboot", time.Now().UTC()
	f.setPhysicalAgent(replacement)
	if _, err := f.server.state.registerAgent(replacement); err != nil {
		t.Fatal(err)
	}
	queueTestAwait(t, analysis.entered, "analysis after reboot confirmation")
	status = controllerHostRebootStatus(t, f.server, operation)
	if status.State != "completed" || status.Hosts[0].State != "rebooted" || f.requests.Load() != 1 {
		t.Fatalf("reboot was not confirmed exactly once: %+v requests=%d", status, f.requests.Load())
	}
	select {
	case <-tail.entered:
		t.Fatal("later experiment overlapped analysis")
	default:
	}
	analysis.once.Do(func() { close(analysis.release) })
	queueTestAwait(t, tail.entered, "experiment after analysis")
	tail.once.Do(func() { close(tail.release) })
	f.server.workWorkers.Wait()
	if len(f.server.state.snapshot().WorkQueue) != 0 {
		t.Fatal("completed maintenance retained its execution slot")
	}
}

func TestHostRebootDeduplicatesOperationAndPhysicalHost(t *testing.T) {
	f := newControllerHostRebootFixture(t, nil)
	head := queueTestEnqueue(t, f.server, "experiment")
	queueTestAwait(t, head.entered, "held experiment")
	duplicate := controllerHostRebootAgent("agent-two", f.agent.Host.NodeID, f.api.URL)
	if _, err := f.server.state.registerAgent(duplicate); err != nil {
		t.Fatal(err)
	}
	targets := []hostRebootTarget{controllerHostRebootTarget(duplicate), controllerHostRebootTarget(f.agent)}
	const operation = "reboot-dedupe-1"
	for attempt := 0; attempt < 2; attempt++ {
		response := controllerHostRebootPost(t, f.handler, operation, targets, nil)
		if response.Code != http.StatusAccepted {
			t.Fatalf("attempt %d: %d %s", attempt, response.Code, response.Body)
		}
	}
	if status := controllerHostRebootStatus(t, f.server, operation); len(status.Targets) != 1 || len(status.Hosts) != 1 {
		t.Fatalf("same physical host was not deduplicated: %+v", status)
	}
	if len(f.server.state.snapshot().WorkQueue) != 2 || f.requests.Load() != 0 {
		t.Fatal("a repeated operation reserved a second slot or dispatched early")
	}
	changed := []hostRebootTarget{controllerHostRebootTarget(f.agent)}
	changed[0].BootID = "another-boot"
	if response := controllerHostRebootPost(t, f.handler, operation, changed, nil); response.Code != http.StatusConflict {
		t.Fatalf("reused operation identity accepted another target: %d %s", response.Code, response.Body)
	}
	head.once.Do(func() { close(head.release) })
	awaitControllerHostReboot(t, f.server, operation, controllerHostRebootTerminal)
	f.server.workWorkers.Wait()
	if f.requests.Load() != 1 {
		t.Fatalf("duplicate host or request rebooted more than once: %d", f.requests.Load())
	}
	if response := controllerHostRebootPost(t, f.handler, operation, targets, nil); response.Code != http.StatusOK || f.requests.Load() != 1 {
		t.Fatalf("terminal operation was resubmitted: %d %s", response.Code, response.Body)
	}
}

func TestHostRebootQueuedCancelDoesNotDisableOrDispatch(t *testing.T) {
	f := newControllerHostRebootFixture(t, nil)
	head := queueTestEnqueue(t, f.server, "experiment")
	queueTestAwait(t, head.entered, "held experiment")
	const operation = "reboot-cancel-1"
	response := controllerHostRebootPost(t, f.handler, operation, []hostRebootTarget{controllerHostRebootTarget(f.agent)}, nil)
	if response.Code != http.StatusAccepted {
		t.Fatalf("reservation: %d %s", response.Code, response.Body)
	}
	work := controllerHostRebootWork(t, f.server, operation)
	if err := f.server.cancelWork(work.ID); err != nil {
		t.Fatal(err)
	}
	status := awaitControllerHostReboot(t, f.server, operation, controllerHostRebootTerminal)
	if status.State != "interrupted" || f.requests.Load() != 0 {
		t.Fatalf("queued cancellation sent a host command: %+v", status)
	}
	f.server.state.mu.RLock()
	disabled := f.server.state.agents[f.agent.ID].Disabled
	f.server.state.mu.RUnlock()
	if disabled {
		t.Fatal("queued cancellation applied a host placement hold")
	}
	if _, err := os.Stat(filepath.Join(f.server.config.DataDir, hostAvailabilityFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("undispatched operation persisted host exclusion: %v", err)
	}
	head.once.Do(func() { close(head.release) })
	f.server.workWorkers.Wait()
}

func TestHostRebootHTTPDisconnectKeepsReservationButServerCancelStopsIt(t *testing.T) {
	for _, disconnect := range []bool{true, false} {
		t.Run(fmt.Sprint(disconnect), func(t *testing.T) {
			f := newControllerHostRebootFixture(t, nil)
			head := queueTestEnqueue(t, f.server, "experiment")
			queueTestAwait(t, head.entered, "held experiment")
			requestCtx, requestCancel := context.WithCancel(context.Background())
			t.Cleanup(requestCancel)
			const operation = "reboot-context-1"
			if response := controllerHostRebootPost(t, f.handler, operation, []hostRebootTarget{controllerHostRebootTarget(f.agent)}, requestCtx); response.Code != http.StatusAccepted {
				t.Fatalf("reservation: %d %s", response.Code, response.Body)
			}
			requestCancel()
			if disconnect {
				if got := controllerHostRebootStatus(t, f.server, operation); got.State != "queued" {
					t.Fatalf("HTTP disconnect canceled admitted work: %+v", got)
				}
				head.once.Do(func() { close(head.release) })
				awaitControllerHostReboot(t, f.server, operation, controllerHostRebootTerminal)
				if f.requests.Load() != 1 {
					t.Fatal("admitted work was lost with its observer")
				}
			} else {
				f.cancel()
				awaitControllerHostReboot(t, f.server, operation, controllerHostRebootTerminal)
				if f.requests.Load() != 0 {
					t.Fatal("server cancellation dispatched a queued reboot")
				}
				head.once.Do(func() { close(head.release) })
			}
			f.server.workWorkers.Wait()
		})
	}
}

func TestHostRebootRestartNeverReplaysInFlightOrUnknownOperations(t *testing.T) {
	for _, previous := range []struct{ job, host, wantJob, wantHost string }{
		{"queued", "queued", "interrupted", "rejected"},
		{"running", "checking", "interrupted", "unknown"},
		{"failed", "unknown", "failed", "unknown"},
	} {
		t.Run(previous.job+"-"+previous.host, func(t *testing.T) {
			f := newControllerHostRebootFixture(t, nil)
			targets, hash, err := normalizeHostRebootTargets([]hostRebootTarget{controllerHostRebootTarget(f.agent)})
			if err != nil {
				t.Fatal(err)
			}
			const operation = "reboot-restart-1"
			now := time.Now().UTC()
			status := hostRebootStatus{Version: 1, ID: operation, RequestHash: hash, State: previous.job, Targets: targets, CreatedAt: now,
				Hosts: []hostRebootHostStatus{{AgentID: f.agent.ID, NodeID: targets[0].NodeID, BootID: targets[0].BootID, State: previous.host, RequestedAt: &now}}}
			if err := f.server.persistHostReboot(status); err != nil {
				t.Fatal(err)
			}
			if err := f.server.state.setHostDisabled(targets[0].NodeID, true); err != nil {
				t.Fatal(err)
			}
			restarted := New(f.server.config, nil)
			handler := restarted.apiTestHandler(context.Background())
			request := httptest.NewRequest(http.MethodGet, "/api/v1/agent-hosts/reboot/"+operation, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			var restored hostRebootStatus
			if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &restored) != nil || restored.State != previous.wantJob || restored.Hosts[0].State != previous.wantHost {
				t.Fatalf("restart outcome was not conservative: %d %s", response.Code, response.Body)
			}
			if response := controllerHostRebootPost(t, handler, operation, targets, nil); response.Code != 200 {
				t.Fatalf("same ID was not returned after restart: %d %s", response.Code, response.Body)
			}
			if f.requests.Load() != 0 || len(restarted.state.snapshot().WorkQueue) != 0 {
				t.Fatal("reading/replaying saved metadata automatically rebooted a host")
			}
		})
	}
}

func TestHostRebootAcknowledgmentIsBoundedIdentityCheckedAndNotRetried(t *testing.T) {
	input := model.HostRebootRequest{OperationID: "reboot-ack-1", NodeID: "node-one", BootID: "boot-original", AgentStartedAt: time.Now().Add(-time.Hour).In(time.FixedZone("test-offset", 3600))}
	canonical := input
	canonical.AgentStartedAt = canonical.AgentStartedAt.UTC()
	canonicalRequest, err := json.Marshal(canonical)
	if err != nil {
		t.Fatal(err)
	}
	const hostKey = "fake-separate-host-key"
	expectedSignature := auth.HostRebootSignature(hostKey, canonicalRequest)
	valid := model.HostRebootResult{OperationID: input.OperationID, NodeID: input.NodeID, BootID: input.BootID, State: "requested"}
	for _, test := range []struct {
		name     string
		code     int
		edit     func(*model.HostRebootResult)
		raw      string
		want     string
		unsigned bool
		tampered bool
		wrongKey bool
	}{
		{name: "requested", code: 202, want: "requested"},
		{name: "unknown", code: 200, edit: func(v *model.HostRebootResult) { v.State = "unknown" }, want: "unknown"},
		{name: "rejected", code: 409, edit: func(v *model.HostRebootResult) { v.State = "rejected" }, want: "rejected"},
		{name: "wrong-operation", code: 200, edit: func(v *model.HostRebootResult) { v.OperationID = "other-operation" }},
		{name: "wrong-node", code: 200, edit: func(v *model.HostRebootResult) { v.NodeID = "other-node" }},
		{name: "wrong-boot", code: 200, edit: func(v *model.HostRebootResult) { v.BootID = "other-boot" }},
		{name: "invalid-state", code: 200, edit: func(v *model.HostRebootResult) { v.State = "rebooted" }},
		{name: "oversized", code: 200, edit: func(v *model.HostRebootResult) { v.Error = strings.Repeat("x", 4097) }},
		{name: "html-login", code: 200, raw: "<html>Log in</html>"},
		{name: "trailing-value", code: 200, raw: `{"operationId":"reboot-ack-1","nodeId":"node-one","bootId":"boot-original","state":"requested"} {}`},
		{name: "empty-ack", code: 204},
		{name: "transient-refusal", code: 503, edit: func(v *model.HostRebootResult) { v.State = "rejected" }, want: "rejected"},
		{name: "transient-requested", code: 503},
		{name: "transient-malformed", code: 503, raw: `{"error":"service unavailable"}`},
		{name: "unsigned-rejection", code: 409, edit: func(v *model.HostRebootResult) { v.State = "rejected" }, unsigned: true},
		{name: "tampered-rejection", code: 409, tampered: true},
		{name: "wrong-response-key", code: 200, wrongKey: true},
		{name: "redirect302", code: 302}, {name: "redirect303", code: 303},
		{name: "redirect307", code: 307}, {name: "redirect308", code: 308},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls, followed atomic.Int32
			login := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { followed.Add(1); w.WriteHeader(200) }))
			t.Cleanup(login.Close)
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodPost || r.URL.Path != "/api/v1/host/reboot" || r.Header.Get("Authorization") != "Bearer fake-internal" {
					t.Errorf("invalid internal reboot request: %s %s", r.Method, r.URL.Path)
				}
				body, _ := io.ReadAll(r.Body)
				if !bytes.Equal(body, canonicalRequest) {
					t.Error("Controller did not normalize the signed reboot request to UTC")
				}
				signature := r.Header.Get(model.HostRebootAuthHeader)
				if signature != expectedSignature || signature == hostKey || signature == "fake-internal" {
					t.Error("Controller did not send a request-bound signature without exposing its host-management key")
				}
				w.Header().Set("Location", login.URL)
				w.WriteHeader(test.code)
				if test.code == 204 {
					return
				}
				if test.raw != "" {
					_, _ = w.Write([]byte(test.raw))
					return
				}
				value := valid
				if test.edit != nil {
					test.edit(&value)
				}
				if !test.unsigned {
					resultBytes, _ := json.Marshal(value)
					key := hostKey
					if test.wrongKey {
						key = "another-fake-host-key"
					}
					value.Signature = auth.HostRebootAckSignature(key, canonicalRequest, resultBytes)
				}
				if test.tampered {
					value.State = "rejected"
				}
				_ = json.NewEncoder(w).Encode(value)
			}))
			t.Cleanup(endpoint.Close)
			server := New(ServerConfig{DataDir: t.TempDir(), Token: "fake-internal", HostRebootToken: hostKey}, nil)
			result, err := server.sendHostReboot(t.Context(), endpoint.URL, input)
			if test.want == "" && err == nil || test.want != "" && (err != nil || result.State != test.want) {
				t.Fatalf("unexpected acknowledgment: %+v %v", result, err)
			}
			if calls.Load() != 1 || followed.Load() != 0 {
				t.Fatalf("reboot command was replayed/redirected: requests=%d redirect=%d", calls.Load(), followed.Load())
			}
		})
	}
}

func TestHostRebootConfirmationNeedsNewBootFreshInstanceAndReceipt(t *testing.T) {
	requested := time.Now().Add(-time.Second).UTC()
	original := controllerHostRebootAgent("agent-old", "node-one", "http://agent.example")
	target := controllerHostRebootTarget(original)
	status := hostRebootStatus{Version: 1, ID: "reboot-evidence-1", State: "running", Targets: []hostRebootTarget{target},
		Hosts: []hostRebootHostStatus{{AgentID: target.AgentID, NodeID: target.NodeID, BootID: target.BootID, State: "requested", RequestedAt: &requested}}}
	for _, test := range []struct {
		name string
		edit func(*model.Agent, *hostRebootStatus)
		want bool
	}{
		{name: "new-boot-ready-new-agent-id", want: true},
		{name: "agent-restart-only", edit: func(a *model.Agent, _ *hostRebootStatus) { a.Host.BootID = target.BootID }},
		{name: "same-instance", edit: func(a *model.Agent, _ *hostRebootStatus) { a.StartedAt = target.AgentStartedAt }},
		{name: "old-duplicate-instance", edit: func(a *model.Agent, _ *hostRebootStatus) { a.StartedAt = target.AgentStartedAt.Add(-time.Hour) }},
		{name: "unreconciled", edit: func(a *model.Agent, _ *hostRebootStatus) { a.StartupReconciled = false }},
		{name: "reboot-still-pending", edit: func(a *model.Agent, _ *hostRebootStatus) { a.Host.RebootPending = true }},
		{name: "offline", edit: func(a *model.Agent, _ *hostRebootStatus) { a.State = model.AgentOffline }},
		{name: "received-before-dispatch", edit: func(_ *model.Agent, s *hostRebootStatus) {
			future := time.Now().Add(time.Hour)
			s.Hosts[0].RequestedAt = &future
		}},
		{name: "wrong-node", edit: func(a *model.Agent, _ *hostRebootStatus) { a.Host.NodeID = "different-node" }},
		{name: "no-dispatch-timestamp", edit: func(_ *model.Agent, s *hostRebootStatus) { s.Hosts[0].RequestedAt = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newControllerHostRebootFixture(t, nil)
			candidate := controllerHostRebootAgent("new-agent-id", target.NodeID, f.api.URL)
			candidate.Host.BootID, candidate.StartedAt = "boot-new", time.Now().UTC()
			candidate.LastSeen = time.Now().UTC()
			copy := cloneHostRebootStatus(status)
			if test.edit != nil {
				test.edit(&candidate, &copy)
			}
			f.setPhysicalAgent(candidate)
			f.server.state.agents[candidate.ID] = candidate
			if got := f.server.hostRebootEvidence(copy); got.Hosts[0].State == "rebooted" || f.proofs.Load() != 0 {
				t.Fatal("an ordinary Agent report or status read fabricated a host reboot")
			}
			job := &hostRebootJob{status: copy, slot: &workSlot{id: "evidence-test-slot"}}
			timeout := 10 * time.Millisecond
			if test.want {
				timeout = 250 * time.Millisecond
			}
			err := f.server.confirmHostReboots(t.Context(), job, timeout)
			if (err == nil) != test.want {
				t.Fatalf("nonce-bound confirmation accepted insufficient evidence: %v", err)
			}
			if f.requests.Load() != 0 {
				t.Fatal("read-only boot confirmation sent a reboot command")
			}
		})
	}
}

func TestHostRebootUsesControllerReceiptTimeInsteadOfAgentClock(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	original := controllerHostRebootAgent("old-instance", "node-one", "http://agent.example")
	target := controllerHostRebootTarget(original)
	requested := time.Now().Add(time.Hour).UTC()
	replacement := controllerHostRebootAgent("new-instance", target.NodeID, "http://agent.example")
	replacement.Host.BootID, replacement.StartedAt, replacement.LastSeen = "boot-new", time.Now().UTC(), requested.Add(time.Hour)
	stored, err := server.state.registerAgent(replacement)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.LastSeen.Before(requested) {
		t.Fatal("Agent-supplied clock replaced Controller receipt time")
	}
	status := hostRebootStatus{Targets: []hostRebootTarget{target}, Hosts: []hostRebootHostStatus{{NodeID: target.NodeID, BootID: target.BootID, State: "requested", RequestedAt: &requested}}}
	if got := server.hostRebootEvidence(status); got.Hosts[0].State == "rebooted" {
		t.Fatal("a future Agent timestamp falsely confirmed a host reboot")
	}
}

func TestHostRebootHoldSurvivesNewAgentIDAndKeepsIndividualDisable(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	individuallyDisabled := controllerHostRebootAgent("manual-disabled", "node-one", "http://agent.example")
	if _, err := server.state.registerAgent(individuallyDisabled); err != nil {
		t.Fatal(err)
	}
	setEnabledTest(t, server.state, individuallyDisabled.ID, false)
	if err := server.state.setHostDisabled("node-one", true); err != nil {
		t.Fatal(err)
	}
	restarted := New(server.config, nil)
	for _, agent := range []model.Agent{individuallyDisabled, controllerHostRebootAgent("replacement-agent", "node-one", "http://agent.example")} {
		stored, err := restarted.state.registerAgent(agent)
		if err != nil || !stored.Disabled {
			t.Fatalf("new Agent bypassed retained host exclusion: %+v %v", stored, err)
		}
	}
	restarted.cancelMu.Lock()
	restarted.hostRebootActiveNodes = map[string]string{"node-one": "reboot-hold-1"}
	restarted.cancelMu.Unlock()
	put := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPut, "/api/v1/agents/replacement-agent/enabled", strings.NewReader(`{"enabled":true}`))
		request.Header.Set("X-KPL-Request", "dashboard")
		response := httptest.NewRecorder()
		restarted.apiTestHandler(context.Background()).ServeHTTP(response, request)
		return response
	}
	if response := put(); response.Code != http.StatusConflict {
		t.Fatalf("active reboot host was reenabled: %d %s", response.Code, response.Body)
	}
	restarted.cancelMu.Lock()
	delete(restarted.hostRebootActiveNodes, "node-one")
	restarted.cancelMu.Unlock()
	if response := put(); response.Code != http.StatusOK {
		t.Fatalf("finished host hold could not be released: %d %s", response.Code, response.Body)
	}
	restarted.state.mu.RLock()
	manual, replacement := restarted.state.agents[individuallyDisabled.ID], restarted.state.agents["replacement-agent"]
	restarted.state.mu.RUnlock()
	if !manual.Disabled || replacement.Disabled {
		t.Fatal("releasing host hold changed an independent Agent disable setting")
	}
	again := New(server.config, nil)
	if stored, err := again.state.registerAgent(controllerHostRebootAgent("another-new-agent", "node-one", "http://agent.example")); err != nil || stored.Disabled {
		t.Fatalf("released host hold was not persisted: %+v %v", stored, err)
	}
}

func TestHostRebootAPIRequiresLoginCSRFAndBoundedConfirmedTargets(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "host-reboot-test-only"}, nil)
	handler := server.Handler(context.Background())
	send := func(body string, cookie *http.Cookie, csrf bool) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/agent-hosts/reboot", strings.NewReader(body))
		if cookie != nil {
			request.AddCookie(cookie)
		}
		if csrf {
			request.Header.Set("X-KPL-Request", "dashboard")
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	if response := send(`{}`, nil, true); response.Code != http.StatusUnauthorized {
		t.Fatal("unauthenticated host reboot request was accepted")
	}
	cookie := loginCookie(t, server)
	if response := send(`{}`, cookie, false); response.Code != http.StatusForbidden {
		t.Fatal("host reboot request bypassed CSRF")
	}
	for _, body := range []string{`{}`, `{"id":"valid-id-1","confirm":false}`, `{"id":"short","confirm":true}`, `{"id":"valid-id-1","confirm":true,"targets":[]}`, `{"id":"valid-id-1","confirm":true,"unknown":true}`} {
		if response := send(body, cookie, true); response.Code != http.StatusBadRequest {
			t.Fatalf("unconfirmed or malformed host request accepted: %d %s", response.Code, response.Body)
		}
	}
	target := controllerHostRebootTarget(controllerHostRebootAgent("agent-one", "node-one", "http://agent.example"))
	tooMany := make([]hostRebootTarget, hostRebootTargetLimit+1)
	for i := range tooMany {
		tooMany[i] = target
	}
	body, _ := json.Marshal(hostRebootSubmission{ID: "reboot-bounded-1", Confirm: true, Targets: tooMany})
	if response := send(string(body), cookie, true); response.Code != http.StatusBadRequest {
		t.Fatalf("oversized host selection accepted: %d %s", response.Code, response.Body)
	}
	if len(server.state.snapshot().WorkQueue) != 0 {
		t.Fatal("refused host request reserved work")
	}
}

func TestHostRebootActiveQueueBoundDoesNotDispatchAndCancelReleasesAdmission(t *testing.T) {
	f := newControllerHostRebootFixture(t, nil)
	head := queueTestEnqueue(t, f.server, "experiment")
	queueTestAwait(t, head.entered, "held experiment")
	target := []hostRebootTarget{controllerHostRebootTarget(f.agent)}
	for i := 0; i < hostRebootQueueLimit; i++ {
		id := fmt.Sprintf("reboot-limit-%03d", i)
		if response := controllerHostRebootPost(t, f.handler, id, target, nil); response.Code != http.StatusAccepted {
			t.Fatalf("valid queue admission %d: %d %s", i, response.Code, response.Body)
		}
	}
	if response := controllerHostRebootPost(t, f.handler, "reboot-limit-extra", target, nil); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unbounded maintenance queue: %d %s", response.Code, response.Body)
	}
	for _, item := range f.server.state.snapshot().WorkQueue {
		if item.Kind == "host-reboot" {
			if err := f.server.cancelWork(item.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := 0; i < hostRebootQueueLimit; i++ {
		awaitControllerHostReboot(t, f.server, fmt.Sprintf("reboot-limit-%03d", i), controllerHostRebootTerminal)
	}
	if response := controllerHostRebootPost(t, f.handler, "reboot-limit-again", target, nil); response.Code != http.StatusAccepted {
		t.Fatalf("canceled queued jobs retained admission capacity: %d %s", response.Code, response.Body)
	}
	work := controllerHostRebootWork(t, f.server, "reboot-limit-again")
	if err := f.server.cancelWork(work.ID); err != nil {
		t.Fatal(err)
	}
	awaitControllerHostReboot(t, f.server, "reboot-limit-again", controllerHostRebootTerminal)
	if f.requests.Load() != 0 {
		t.Fatal("canceling queued host jobs sent a reboot command")
	}
	head.once.Do(func() { close(head.release) })
	f.server.workWorkers.Wait()
}

func TestHostRebootRequestedContextCancellationNeverResends(t *testing.T) {
	var called sync.Once
	sent := make(chan struct{})
	f := newControllerHostRebootFixture(t, func(f *controllerHostRebootFixture, w http.ResponseWriter, _ *http.Request, input model.HostRebootRequest) {
		called.Do(func() { close(sent) })
		controllerHostRebootWriteACK(w, f.server.config.HostRebootToken, input, "requested", "")
	})
	const operation = "reboot-sent-cancel-1"
	targets := []hostRebootTarget{controllerHostRebootTarget(f.agent)}
	if response := controllerHostRebootPost(t, f.handler, operation, targets, nil); response.Code != http.StatusAccepted {
		t.Fatalf("admission: %d %s", response.Code, response.Body)
	}
	queueTestAwait(t, sent, "requested host command")
	awaitControllerHostReboot(t, f.server, operation, func(status hostRebootStatus) bool { return status.Hosts[0].State == "requested" })
	f.cancel()
	status := awaitControllerHostReboot(t, f.server, operation, controllerHostRebootTerminal)
	f.server.workWorkers.Wait()
	if status.State != "interrupted" || status.Hosts[0].State == "rebooted" || f.requests.Load() != 1 {
		t.Fatalf("shutdown fabricated confirmation or retransmitted a reboot: %+v", status)
	}
	if response := controllerHostRebootPost(t, f.handler, operation, targets, nil); response.Code != 200 || f.requests.Load() != 1 {
		t.Fatalf("retry after interrupted wait resent a host command: %d %s", response.Code, response.Body)
	}
}

func TestHostRebootTransportCancellationRemainsUnknownAndSingleAttempt(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var input model.HostRebootRequest
		_ = json.NewDecoder(r.Body).Decode(&input)
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(endpoint.Close)
	t.Cleanup(func() { close(release) })
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() {
		_, err := server.sendHostReboot(ctx, endpoint.URL, model.HostRebootRequest{OperationID: "reboot-timeout-1", NodeID: "node-one", BootID: "boot-original", AgentStartedAt: time.Now().Add(-time.Hour)})
		done <- err
	}()
	queueTestAwait(t, entered, "single reboot transport attempt")
	cancel()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "unknown") || !errors.Is(err, context.Canceled) {
			t.Fatalf("transport cancellation was treated as an acknowledged reboot: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reboot transport ignored the canceled operation")
	}
	if calls.Load() != 1 {
		t.Fatalf("ambiguous reboot dispatch was automatically retried: %d", calls.Load())
	}
}

func TestHostRebootRevalidatesQueuedHostBeforeAnyDispatch(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*model.Agent)
	}{
		{name: "new-boot", edit: func(a *model.Agent) { a.Host.BootID = "boot-new" }},
		{name: "new-instance", edit: func(a *model.Agent) { a.StartedAt = time.Now().UTC() }},
		{name: "offline", edit: func(a *model.Agent) { a.State = model.AgentOffline }},
		{name: "disabled-capability", edit: func(a *model.Agent) { a.Host.RebootEnabled = false }},
		{name: "unsupported", edit: func(a *model.Agent) { a.Host.RebootSupported = false }},
		{name: "already-pending", edit: func(a *model.Agent) { a.Host.RebootPending = true }},
		{name: "wrong-node", edit: func(a *model.Agent) { a.Host.NodeID = "different-node" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newControllerHostRebootFixture(t, nil)
			head := queueTestEnqueue(t, f.server, "experiment")
			queueTestAwait(t, head.entered, "preceding experiment")
			const operation = "reboot-stale-1"
			if response := controllerHostRebootPost(t, f.handler, operation, []hostRebootTarget{controllerHostRebootTarget(f.agent)}, nil); response.Code != http.StatusAccepted {
				t.Fatalf("reservation: %d %s", response.Code, response.Body)
			}
			f.server.state.mu.Lock()
			current := f.server.state.agents[f.agent.ID]
			host := *current.Host
			current.Host = &host
			test.edit(&current)
			f.server.state.agents[current.ID] = current
			f.server.state.mu.Unlock()
			head.once.Do(func() { close(head.release) })
			status := awaitControllerHostReboot(t, f.server, operation, controllerHostRebootTerminal)
			f.server.workWorkers.Wait()
			if status.State != "failed" || status.Hosts[0].State != "rejected" || f.requests.Load() != 0 {
				t.Fatalf("stale queued host selection was dispatched: %+v requests=%d", status, f.requests.Load())
			}
		})
	}
}

func TestHostRebootCanceledDispatchKeepsOldBootFencedAcrossRestart(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var released sync.Once
	f := newControllerHostRebootFixture(t, func(f *controllerHostRebootFixture, w http.ResponseWriter, _ *http.Request, input model.HostRebootRequest) {
		close(entered)
		// A real Agent finishes its durably accepted command independently of
		// a Controller HTTP disconnect. Model that lifetime explicitly here.
		<-release
		controllerHostRebootWriteACK(w, f.server.config.HostRebootToken, input, "requested", "")
	})
	t.Cleanup(func() { released.Do(func() { close(release) }) })
	const operation = "reboot-uncertain-cancel-1"
	if response := controllerHostRebootPost(t, f.handler, operation, []hostRebootTarget{controllerHostRebootTarget(f.agent)}, nil); response.Code != http.StatusAccepted {
		t.Fatalf("admission: %d %s", response.Code, response.Body)
	}
	queueTestAwait(t, entered, "durably accepted Agent reboot")
	work := controllerHostRebootWork(t, f.server, operation)
	if err := f.server.cancelWork(work.ID); err != nil {
		t.Fatal(err)
	}
	status := awaitControllerHostReboot(t, f.server, operation, controllerHostRebootTerminal)
	f.server.workWorkers.Wait()
	if status.State != "interrupted" || status.Hosts[0].State != "unknown" || status.Hosts[0].RequestedAt == nil {
		t.Fatalf("canceled dispatch lost its uncertainty evidence: %+v", status)
	}
	enable := func(server *Server, agentID string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPut, "/api/v1/agents/"+agentID+"/enabled", strings.NewReader(`{"enabled":true}`))
		request.Header.Set("X-KPL-Request", "dashboard")
		response := httptest.NewRecorder()
		server.apiTestHandler(context.Background()).ServeHTTP(response, request)
		return response
	}
	if response := enable(f.server, f.agent.ID); response.Code != http.StatusConflict {
		t.Fatalf("cancel released an accepted reboot against the cached old boot: %d %s", response.Code, response.Body)
	}
	restarted := New(f.server.config, nil)
	if _, err := restarted.state.registerAgent(f.agent); err != nil {
		t.Fatal(err)
	}
	if response := enable(restarted, f.agent.ID); response.Code != http.StatusConflict {
		t.Fatalf("Controller restart discarded the uncertain host fence: %d %s", response.Code, response.Body)
	}
	released.Do(func() { close(release) })
	replacement := controllerHostRebootAgent("replacement-after-reboot", f.agent.Host.NodeID, f.api.URL)
	replacement.Host.BootID, replacement.StartedAt = "boot-after-reboot", time.Now().UTC()
	f.setPhysicalAgent(replacement)
	if _, err := restarted.state.registerAgent(replacement); err != nil {
		t.Fatal(err)
	}
	if response := enable(restarted, replacement.ID); response.Code != http.StatusOK {
		t.Fatalf("observed new boot did not release its old fence: %d %s", response.Code, response.Body)
	}
	if f.requests.Load() != 1 {
		t.Fatalf("unknown host operation was retransmitted: %d", f.requests.Load())
	}
}

func TestHostRebootEnableClearsOnlyTheVerifiedHostIdentity(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	verified := controllerHostRebootAgent("same-agent-id", "node-alpha", "http://agent.example")
	for _, nodeID := range []string{"node-alpha", "node-bravo"} {
		if err := server.state.setHostDisabled(nodeID, true); err != nil {
			t.Fatal(err)
		}
	}
	for _, edit := range []func(*model.Agent){
		func(a *model.Agent) {
			a.Host.NodeID = "node-bravo"
			a.Labels = map[string]string{"swarmNodeId": "node-bravo"}
		},
		func(a *model.Agent) { a.ID = "different-agent" },
		func(a *model.Agent) { a.StartedAt = a.StartedAt.Add(time.Second) },
		func(a *model.Agent) { a.Host.RebootPending = true },
	} {
		current := verified
		host := *verified.Host
		current.Host = &host
		edit(&current)
		if err := server.completeHostEnable(verified, current); !errors.Is(err, errHostEnableIdentityChanged) {
			t.Fatalf("a changed Agent/host identity reused another host's Enable proof: %v", err)
		}
		stored, err := loadDisabledHosts(server.config.DataDir)
		if err != nil || !stored["node-alpha"] || !stored["node-bravo"] {
			t.Fatalf("stale Enable proof cleared a host hold: %+v %v", stored, err)
		}
	}
	if err := server.completeHostEnable(verified, verified); err != nil {
		t.Fatal(err)
	}
	stored, err := loadDisabledHosts(server.config.DataDir)
	if err != nil || stored["node-alpha"] || !stored["node-bravo"] {
		t.Fatalf("valid Enable did not release only its verified host: %+v %v", stored, err)
	}
	restarted := New(server.config, nil)
	for nodeID, wantDisabled := range map[string]bool{"node-alpha": false, "node-bravo": true} {
		agent := controllerHostRebootAgent("returned-"+nodeID, nodeID, "http://agent.example")
		got, err := restarted.state.registerAgent(agent)
		if err != nil || got.Disabled != wantDisabled {
			t.Fatalf("host hold identity did not survive restart for %s: %+v %v", nodeID, got, err)
		}
	}
}

func TestHostRebootProofRequiresFreshNonceActorAndSignature(t *testing.T) {
	const key = "fake-proof-management-key"
	for _, test := range []struct {
		name     string
		edit     func(*model.HostRebootProof)
		unsigned bool
		tampered bool
	}{
		{name: "valid"},
		{name: "missing-signature", unsigned: true},
		{name: "replayed-nonce", edit: func(p *model.HostRebootProof) { p.Nonce = "an-older-challenge" }},
		{name: "wrong-actor", edit: func(p *model.HostRebootProof) { p.AgentID = "another-agent" }},
		{name: "wrong-node", edit: func(p *model.HostRebootProof) { p.NodeID = "another-node" }},
		{name: "wrong-instance", edit: func(p *model.HostRebootProof) { p.AgentStartedAt = p.AgentStartedAt.Add(-time.Hour) }},
		{name: "tampered-boot", tampered: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			actor := controllerHostRebootAgent("actual-agent", "node-one", "")
			var calls atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var input model.HostRebootProofRequest
				if r.Method != http.MethodPost || r.URL.Path != "/api/v1/host/reboot-status" || json.NewDecoder(r.Body).Decode(&input) != nil {
					t.Error("invalid read-only host challenge request")
				}
				proof := model.HostRebootProof{Nonce: input.Nonce, AgentID: actor.ID, NodeID: actor.Host.NodeID, BootID: actor.Host.BootID,
					AgentStartedAt: actor.StartedAt.UTC(), StartupReconciled: true, RebootSupported: true}
				if test.edit != nil {
					test.edit(&proof)
				}
				if !test.unsigned {
					body, _ := json.Marshal(proof)
					proof.Signature = auth.HostRebootStatusSignature(key, body)
				}
				if test.tampered {
					proof.BootID = "a-forged-new-boot"
				}
				_ = json.NewEncoder(w).Encode(proof)
			}))
			t.Cleanup(endpoint.Close)
			actor.URL = endpoint.URL
			server := New(ServerConfig{DataDir: t.TempDir(), Token: "fake-service-key", HostRebootToken: key}, nil)
			proof, err := server.requestHostRebootProof(t.Context(), actor)
			if test.name == "valid" {
				if err != nil || proof == nil {
					t.Fatalf("fresh signed host proof rejected: %v", err)
				}
			} else if err == nil || proof != nil {
				t.Fatal("an unsigned, replayed or wrong-actor host proof was accepted")
			}
			if calls.Load() != 1 {
				t.Fatal("a read-only proof request was unexpectedly repeated")
			}
		})
	}
}

func TestHostRebootForgedACKAndHeartbeatCannotReleaseFence(t *testing.T) {
	for _, tampered := range []bool{false, true} {
		t.Run(fmt.Sprint(tampered), func(t *testing.T) {
			f := newControllerHostRebootFixture(t, func(f *controllerHostRebootFixture, w http.ResponseWriter, _ *http.Request, input model.HostRebootRequest) {
				result := model.HostRebootResult{OperationID: input.OperationID, NodeID: input.NodeID, BootID: input.BootID, State: "requested"}
				if tampered {
					requestBytes := dataForRebootRequest(input)
					resultBytes, _ := json.Marshal(result)
					result.Signature = auth.HostRebootAckSignature(f.server.config.HostRebootToken, requestBytes, resultBytes)
				}
				// A service-key holder can forge this JSON, but cannot authenticate
				// a definite refusal for the captured reboot authorization.
				result.State = "rejected"
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(result)
			})
			const operation = "reboot-forged-response-1"
			if response := controllerHostRebootPost(t, f.handler, operation, []hostRebootTarget{controllerHostRebootTarget(f.agent)}, nil); response.Code != http.StatusAccepted {
				t.Fatalf("admission: %d %s", response.Code, response.Body)
			}
			awaitControllerHostReboot(t, f.server, operation, func(s hostRebootStatus) bool { return s.Hosts[0].State == "unknown" })
			work := controllerHostRebootWork(t, f.server, operation)
			if err := f.server.cancelWork(work.ID); err != nil {
				t.Fatal(err)
			}
			awaitControllerHostReboot(t, f.server, operation, controllerHostRebootTerminal)
			f.server.workWorkers.Wait()
			forged := controllerHostRebootAgent("forged-returned-agent", f.agent.Host.NodeID, f.api.URL)
			forged.Host.BootID, forged.StartedAt = "forged-new-boot", time.Now().UTC()
			if _, err := f.server.state.registerAgent(forged); err != nil {
				t.Fatal(err)
			}
			// Registering a plausible heartbeat does not change the independent
			// physical host used by the signed, nonce-bound endpoint above.
			before := f.proofs.Load()
			request := httptest.NewRequest(http.MethodGet, "/api/v1/agent-hosts/reboot/"+operation, nil)
			response := httptest.NewRecorder()
			f.handler.ServeHTTP(response, request)
			var status hostRebootStatus
			if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &status) != nil || status.Hosts[0].State != "unknown" || f.proofs.Load() != before {
				t.Fatalf("job status trusted a forged heartbeat or performed network I/O: %d %s", response.Code, response.Body)
			}
			request = httptest.NewRequest(http.MethodPut, "/api/v1/agents/"+forged.ID+"/enabled", strings.NewReader(`{"enabled":true}`))
			request.Header.Set("X-KPL-Request", "dashboard")
			response = httptest.NewRecorder()
			f.handler.ServeHTTP(response, request)
			if response.Code != http.StatusConflict {
				t.Fatalf("forged ACK/heartbeat released an old-boot fence: %d %s", response.Code, response.Body)
			}
			if f.requests.Load() != 1 {
				t.Fatal("forged evidence caused another reboot dispatch")
			}
		})
	}
}

func TestHostRebootUntrustedSelectionBaselineNeverGetsCommandAuthorization(t *testing.T) {
	f := newControllerHostRebootFixture(t, nil)
	forged := cloneAgentHost(f.agent)
	forged.Host.BootID = "a-fabricated-old-boot"
	forged.StartedAt = forged.StartedAt.Add(-time.Hour)
	f.server.state.mu.Lock()
	f.server.state.agents[forged.ID] = forged
	f.server.state.mu.Unlock()
	const operation = "reboot-forged-baseline-1"
	if response := controllerHostRebootPost(t, f.handler, operation, []hostRebootTarget{controllerHostRebootTarget(forged)}, nil); response.Code != http.StatusAccepted {
		t.Fatalf("reservation: %d %s", response.Code, response.Body)
	}
	status := awaitControllerHostReboot(t, f.server, operation, controllerHostRebootTerminal)
	f.server.workWorkers.Wait()
	if status.Hosts[0].State != "rejected" || status.Hosts[0].RequestedAt != nil || f.requests.Load() != 0 || f.proofs.Load() == 0 {
		t.Fatalf("untrusted old-boot metadata obtained a reboot capability: %+v requests=%d proofs=%d", status, f.requests.Load(), f.proofs.Load())
	}
}

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/auth"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const rebootTestNode = "1111111111111111111111111"
const rebootTestControl = "2222222222222222222222222"
const rebootTestBoot = "11111111-1111-4111-8111-111111111111"
const rebootTestImage = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const rebootTestHelper = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// This fake records arguments only: no Docker daemon or host command is run.
type rebootDockerFixture struct {
	mu           sync.Mutex
	calls        [][]string
	boot         string
	manager      bool
	controlNode  bool
	security     []string
	peer         string
	selfImage    string
	fail         string
	beforeCreate func()
	beforeStart  func(context.Context)
}

func (f *rebootDockerFixture) command(ctx context.Context, args ...string) ([]byte, error) {
	f.mu.Lock()
	f.calls = append(f.calls, slices.Clone(args))
	f.mu.Unlock()
	if f.fail == args[0] {
		return nil, errors.New("simulated " + args[0] + " failure")
	}
	switch args[0] {
	case "info":
		node := rebootTestNode
		if f.controlNode {
			node = rebootTestControl
		}
		data, _ := json.Marshal(map[string]any{"OSType": "linux", "Name": "worker-host", "SecurityOptions": f.security, "Swarm": map[string]any{"NodeID": node, "LocalNodeState": "active", "ControlAvailable": f.manager}})
		return data, nil
	case "inspect":
		image := f.selfImage
		if image == "" {
			image = rebootTestImage
		}
		data, _ := json.Marshal(map[string]any{"Image": image, "NodeID": rebootTestNode, "Running": true})
		return data, nil
	case "run":
		return []byte(f.boot + "\n"), nil
	case "ps":
		return []byte(f.peer), nil
	case "create":
		if f.beforeCreate != nil {
			f.beforeCreate()
		}
		return []byte(rebootTestHelper + "\n"), nil
	case "start":
		if f.beforeStart != nil {
			f.beforeStart(ctx)
		}
		return nil, nil
	default:
		return nil, errors.New("unexpected Docker command " + args[0])
	}
}

func (f *rebootDockerFixture) count(command string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, call := range f.calls {
		if call[0] == command {
			count++
		}
	}
	return count
}

func newHostRebootServer(t *testing.T, boot string) (*Server, *rebootDockerFixture) {
	t.Helper()
	s, err := New(Config{ID: "agent", Name: "Agent", DockerImage: "kpl:test", DockerNetwork: "peers", ControllerURL: "http://controller:8080", AdvertiseURL: "http://agent:8090", Token: "fake-internal-test-token", HostRebootToken: "fake-host-control-key", DataDir: t.TempDir(), HostRebootEnabled: true, ControlNodeID: rebootTestControl, SelfContainer: "agent-task", Labels: map[string]string{"swarmNodeId": rebootTestNode}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.hostBootIDReader = func() (string, error) { return boot, nil }
	s.host.BootID = boot
	s.startupReconciled = true
	f := &rebootDockerFixture{boot: boot, security: []string{"name=seccomp,profile=builtin"}}
	s.hostDockerCommand = f.command
	return s, f
}

func hostRebootRequest(s *Server) model.HostRebootRequest {
	return model.HostRebootRequest{OperationID: "operation-test", NodeID: rebootTestNode, BootID: s.host.BootID, AgentStartedAt: s.startedAt}
}

func hostRebootTestSignature(s *Server, request model.HostRebootRequest) string {
	request.AgentStartedAt = request.AgentStartedAt.UTC()
	payload, _ := json.Marshal(request)
	return auth.HostRebootSignature(s.config.HostRebootToken, payload)
}

func TestHostRebootRejectsUnsupportedEnvironmentBeforeCommand(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*Server, *rebootDockerFixture)
	}{
		{"disabled", func(s *Server, f *rebootDockerFixture) { s.config.HostRebootEnabled = false }},
		{"missing-host-key", func(s *Server, f *rebootDockerFixture) { s.config.HostRebootToken = "" }},
		{"reused-peer-key", func(s *Server, f *rebootDockerFixture) { s.config.HostRebootToken = s.config.Token }},
		{"manager", func(s *Server, f *rebootDockerFixture) { f.manager = true }},
		{"control-host", func(s *Server, f *rebootDockerFixture) { s.config.ControlNodeID = rebootTestNode }},
		{"rootless", func(s *Server, f *rebootDockerFixture) { f.security = []string{"name=rootless"} }},
		{"user-namespace", func(s *Server, f *rebootDockerFixture) { f.security = []string{"name=userns"} }},
		{"missing-security-evidence", func(s *Server, f *rebootDockerFixture) { f.security = nil }},
		{"other-node", func(s *Server, f *rebootDockerFixture) { f.controlNode = true }},
		{"image-tag", func(s *Server, f *rebootDockerFixture) { f.selfImage = "kpl:latest" }},
		{"other-host-boot", func(s *Server, f *rebootDockerFixture) { f.boot = "22222222-2222-4222-8222-222222222222" }},
		{"no-systemd", func(s *Server, f *rebootDockerFixture) { f.fail = "run" }},
		{"daemon-peer", func(s *Server, f *rebootDockerFixture) { f.peer = "peer-from-another-agent\n" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, f := newHostRebootServer(t, rebootTestBoot)
			test.configure(s, f)
			result, status := s.rebootHost(context.Background(), hostRebootRequest(s))
			if status != 409 || result.State != "rejected" || f.count("create") != 0 || f.count("start") != 0 || s.host.RebootPending || s.telemetryHostRebootClosed {
				t.Fatalf("unsafe environment admitted: %+v status=%d calls=%v", result, status, f.calls)
			}
		})
	}
}

func TestHostRebootBindsBootAndAgentInstance(t *testing.T) {
	for _, test := range []struct {
		name   string
		update func(*model.HostRebootRequest)
	}{
		{"boot", func(r *model.HostRebootRequest) { r.BootID = "22222222-2222-4222-8222-222222222222" }},
		{"instance", func(r *model.HostRebootRequest) { r.AgentStartedAt = r.AgentStartedAt.Add(-time.Second) }},
		{"node", func(r *model.HostRebootRequest) { r.NodeID = rebootTestControl }},
		{"command-input", func(r *model.HostRebootRequest) { r.OperationID = "operation; reboot" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, f := newHostRebootServer(t, rebootTestBoot)
			r := hostRebootRequest(s)
			test.update(&r)
			result, status := s.rebootHost(context.Background(), r)
			if status != 409 || result.State != "rejected" || len(f.calls) != 0 {
				t.Fatalf("stale identity touched Docker: %+v %d %v", result, status, f.calls)
			}
		})
	}
}

func TestHostRebootRequiresPeerCleanupAndTelemetryDelivery(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*Server)
	}{
		{"starting", func(s *Server) { s.processes["peer"] = &process{node: model.Node{State: model.NodeStarting}} }},
		{"retained-ready", func(s *Server) { s.processes["peer"] = &process{node: model.Node{State: model.NodeReady}} }},
		{"cleanup-error", func(s *Server) {
			s.processes["peer"] = &process{node: model.Node{State: model.NodeFailed}, exited: true, cleanupErr: errors.New("remove failed")}
		}},
		{"telemetry", func(s *Server) { s.spool = nil; s.events = []model.TraceEvent{{RunID: "pending"}} }},
		{"spool-error", func(s *Server) { s.spoolError = errors.New("disk stalled") }},
		{"decoder", func(s *Server) { s.telemetryDecoders.Store(1) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, f := newHostRebootServer(t, rebootTestBoot)
			test.setup(s)
			result, status := s.rebootHost(context.Background(), hostRebootRequest(s))
			if status != 409 || result.State != "rejected" || len(f.calls) != 0 || s.host.RebootPending {
				t.Fatalf("unconfirmed evidence rebooted: %+v %d %v", result, status, f.calls)
			}
		})
	}
}

func TestHostRebootRecordsBeforeExecutionAndNeverReplays(t *testing.T) {
	s, f := newHostRebootServer(t, rebootTestBoot)
	r := hostRebootRequest(s)
	f.beforeCreate = func() {
		receipt, err := s.readHostRebootReceipt()
		if err != nil || receipt == nil || receipt.Result.State != "unknown" {
			t.Fatalf("operation was not durable before create: %+v %v", receipt, err)
		}
		if err := s.lockAdmission(context.Background(), model.CreateNodeRequest{ID: "new-peer", RunID: "new-run"}); err == nil {
			s.mu.Unlock()
			t.Error("reboot did not close Peer admission")
		} else if !strings.Contains(err.Error(), "host reboot") {
			t.Errorf("unexpected admission error: %v", err)
		}
	}
	result, status := s.rebootHost(context.Background(), r)
	if status != 202 || result.State != "requested" || !s.host.RebootPending || !s.telemetryHostRebootClosed {
		t.Fatalf("missing reboot acknowledgment/gate: %+v %d", result, status)
	}
	if repeat, status := s.rebootHost(context.Background(), r); status != 202 || repeat != result || f.count("create") != 1 || f.count("start") != 1 {
		t.Fatalf("duplicate operation replayed: %+v %d calls=%v", repeat, status, f.calls)
	}
	changed := r
	changed.AgentStartedAt = changed.AgentStartedAt.Add(time.Second)
	if altered, status := s.rebootHost(context.Background(), changed); status != 409 || altered.State != "rejected" || f.count("create") != 1 {
		t.Fatalf("changed operation payload was accepted: %+v %d", altered, status)
	}
	r.OperationID = "another-operation"
	if result, status := s.rebootHost(context.Background(), r); status != 409 || result.State != "rejected" || f.count("create") != 1 {
		t.Fatalf("another operation rebooted same boot: %+v %d", result, status)
	}
	for _, call := range f.calls {
		if call[0] != "create" {
			continue
		}
		for _, fixed := range []string{"--network=none", "--pid=host", "--privileged", "--user=0:0", "--pull=never", "--restart=no", "--entrypoint=/usr/bin/nsenter", rebootTestImage, hostSystemdReboot} {
			if !slices.Contains(call, fixed) {
				t.Errorf("missing fixed helper argument %q: %v", fixed, call)
			}
		}
		if slices.Contains(call, "--rm") || slices.Contains(call, "--network=host") || slices.Contains(call, "--force") || slices.Contains(call, "--volume") {
			t.Fatalf("helper has unexpected lifecycle/privileges: %v", call)
		}
	}
	statusCopy := s.snapshotAgent()
	statusCopy.Host.RebootPending = false
	if !s.host.RebootPending {
		t.Fatal("published host capability aliases mutable Agent state")
	}
}

func TestHostRebootUnknownSurvivesAgentRestart(t *testing.T) {
	boot, err := readHostBootID()
	if err != nil {
		t.Skip("Linux boot identity unavailable")
	}
	s, f := newHostRebootServer(t, boot)
	f.fail = "start"
	request := hostRebootRequest(s)
	result, status := s.rebootHost(context.Background(), request)
	if status != 202 || result.State != "unknown" || !s.host.RebootPending {
		t.Fatalf("lost uncertain outcome: %+v %d", result, status)
	}
	config := s.config
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restarted.hostDockerCommand = func(context.Context, ...string) ([]byte, error) {
		t.Fatal("recorded reboot was executed after Agent restart")
		return nil, nil
	}
	if !restarted.host.RebootPending || !restarted.telemetryHostRebootClosed {
		t.Fatal("restart forgot pending host maintenance")
	}
	if repeated, status := restarted.rebootHost(context.Background(), request); status != 202 || repeated.State != "unknown" {
		t.Fatalf("restart did not preserve unknown receipt: %+v %d", repeated, status)
	}
	request.OperationID = "new-operation"
	request.AgentStartedAt = restarted.startedAt
	if repeated, status := restarted.rebootHost(context.Background(), request); status != 409 || repeated.State != "rejected" {
		t.Fatalf("restart accepted another operation on same boot: %+v %d", repeated, status)
	}
}

func TestHostRebootReceiptFromPreviousBootDoesNotCloseAdmission(t *testing.T) {
	boot, err := readHostBootID()
	if err != nil {
		t.Skip("Linux boot identity unavailable")
	}
	s, _ := newHostRebootServer(t, boot)
	request := hostRebootRequest(s)
	request.BootID = rebootTestBoot
	if request.BootID == boot {
		request.BootID = "22222222-2222-4222-8222-222222222222"
	}
	if err := s.persistHostRebootReceipt(&hostRebootReceipt{Version: 1, Request: request, Result: model.HostRebootResult{OperationID: request.OperationID, NodeID: request.NodeID, BootID: request.BootID, State: "requested"}, UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	config := s.config
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if restarted.host.RebootPending || restarted.telemetryHostRebootClosed {
		t.Fatal("old boot's receipt blocked a new boot")
	}
}

func TestHostRebootReceiptRequiresKnownBootBeforeOpeningAdmission(t *testing.T) {
	for _, withReceipt := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-receipt", true: "pending-receipt"}[withReceipt], func(t *testing.T) {
			s, _ := newHostRebootServer(t, rebootTestBoot)
			if withReceipt {
				request := hostRebootRequest(s)
				if err := s.persistHostRebootReceipt(&hostRebootReceipt{Version: 1, Request: request, Result: model.HostRebootResult{OperationID: request.OperationID, NodeID: request.NodeID, BootID: request.BootID, State: "unknown"}, UpdatedAt: time.Now()}); err != nil {
					t.Fatal(err)
				}
			}
			s.hostBootIDReader = func() (string, error) { return "", errors.New("host boot identity unavailable") }
			err := s.initializeHostReboot()
			if withReceipt && err == nil {
				t.Fatal("unreadable boot identity silently reopened an existing reboot obligation")
			}
			if withReceipt && (!s.host.RebootPending || !s.telemetryHostRebootClosed) {
				t.Fatal("an unknown boot identity did not preserve the pending admission gates")
			}
			if !withReceipt && err != nil {
				t.Fatalf("missing boot identity unnecessarily stopped an ordinary Agent: %v", err)
			}
		})
	}
}

func TestHostRebootHTTPRequiresBearerAndStrictIdentity(t *testing.T) {
	s, f := newHostRebootServer(t, rebootTestBoot)
	body, _ := json.Marshal(hostRebootRequest(s))
	for _, test := range []struct {
		name, method, token, body string
		status                    int
	}{
		{"no-token", "POST", "", string(body), 401},
		{"wrong-token", "POST", "wrong", string(body), 401},
		{"get", "GET", "", string(body), 405},
		{"unknown-command", "POST", s.config.Token, strings.TrimSuffix(string(body), "}") + `,"command":"reboot"}`, 400},
		{"trailing", "POST", s.config.Token, string(body) + "{}", 400},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := httptest.NewRequest(test.method, "/api/v1/host/reboot", strings.NewReader(test.body))
			r.Header.Set(model.HostRebootAuthHeader, hostRebootTestSignature(s, hostRebootRequest(s)))
			if test.token != "" {
				r.Header.Set("Authorization", "Bearer "+test.token)
			}
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != test.status || len(f.calls) != 0 {
				t.Fatalf("HTTP policy failed: %d %s calls=%v", w.Code, w.Body, f.calls)
			}
		})
	}
}

func TestHostRebootPeerBearerCannotAuthorizeHostReboot(t *testing.T) {
	s, f := newHostRebootServer(t, rebootTestBoot)
	body, _ := json.Marshal(hostRebootRequest(s))
	request := httptest.NewRequest("POST", "/api/v1/host/reboot", strings.NewReader(string(body)))
	// Peers receive this base credential for telemetry and control forwarding.
	request.Header.Set("Authorization", "Bearer "+s.config.Token)
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != 401 || f.count("create") != 0 || f.count("start") != 0 {
		t.Fatalf("a Peer credential authorized host reboot: status=%d create=%d start=%d", response.Code, f.count("create"), f.count("start"))
	}
}

func TestHostRebootRawHostKeyCannotAuthorizeRequest(t *testing.T) {
	s, f := newHostRebootServer(t, rebootTestBoot)
	body, _ := json.Marshal(hostRebootRequest(s))
	request := httptest.NewRequest("POST", "/api/v1/host/reboot", strings.NewReader(string(body)))
	request.Header.Set("Authorization", "Bearer "+s.config.Token)
	request.Header.Set(model.HostRebootAuthHeader, s.config.HostRebootToken)
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != 401 || f.count("create") != 0 || f.count("start") != 0 {
		t.Fatalf("a static host key authorized reboot without a request signature: status=%d create=%d start=%d", response.Code, f.count("create"), f.count("start"))
	}
}

func TestHostRebootHTTPRequiresIndependentHostCredential(t *testing.T) {
	for _, test := range []struct {
		name, header string
		status       int
	}{
		{"missing", "", 401}, {"wrong", "not-the-host-credential", 401}, {"peer-key", "fake-internal-test-token", 401}, {"raw-host-key", "fake-host-control-key", 401}, {"signed-body", "", 202},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, f := newHostRebootServer(t, rebootTestBoot)
			body, _ := json.Marshal(hostRebootRequest(s))
			request := httptest.NewRequest("POST", "/api/v1/host/reboot", strings.NewReader(string(body)))
			request.Header.Set("Authorization", "Bearer "+s.config.Token)
			if test.name == "signed-body" {
				test.header = hostRebootTestSignature(s, hostRebootRequest(s))
			}
			if test.header != "" {
				request.Header.Set(model.HostRebootAuthHeader, test.header)
			}
			response := httptest.NewRecorder()
			s.Handler().ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("host credential policy returned status=%d, expected=%d", response.Code, test.status)
			}
			if test.status == 401 && (f.count("create") != 0 || f.count("start") != 0) {
				t.Fatal("a missing or Peer-only host credential started a helper")
			}
			if test.status == 202 && f.count("start") != 1 {
				t.Fatal("valid host control authentication did not dispatch")
			}
		})
	}
}

func TestHostRebootSignatureCannotBeMovedToAnotherRequest(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*model.HostRebootRequest)
	}{
		{"operation", func(r *model.HostRebootRequest) { r.OperationID = "another-signed-operation" }},
		{"node", func(r *model.HostRebootRequest) { r.NodeID = rebootTestControl }},
		{"boot", func(r *model.HostRebootRequest) { r.BootID = "22222222-2222-4222-8222-222222222222" }},
		{"instance", func(r *model.HostRebootRequest) { r.AgentStartedAt = r.AgentStartedAt.Add(time.Nanosecond) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, f := newHostRebootServer(t, rebootTestBoot)
			original := hostRebootRequest(s)
			changed := original
			test.change(&changed)
			body, _ := json.Marshal(changed)
			r := httptest.NewRequest("POST", "/api/v1/host/reboot", strings.NewReader(string(body)))
			r.Header.Set("Authorization", "Bearer "+s.config.Token)
			r.Header.Set(model.HostRebootAuthHeader, hostRebootTestSignature(s, original))
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != 401 || len(f.calls) != 0 {
				t.Fatalf("request signature accepted altered %s: status=%d Docker calls=%d", test.name, w.Code, len(f.calls))
			}
		})
	}
}

func TestHostRebootSignatureCanonicalizesUTCAndKeepsReplayIdempotent(t *testing.T) {
	s, f := newHostRebootServer(t, rebootTestBoot)
	original := hostRebootRequest(s)
	signature := hostRebootTestSignature(s, original)
	for _, zone := range []*time.Location{time.FixedZone("offset", 9*60*60), time.UTC} {
		request := original
		request.AgentStartedAt = request.AgentStartedAt.In(zone)
		body, _ := json.Marshal(request)
		r := httptest.NewRequest("POST", "/api/v1/host/reboot", strings.NewReader(string(body)))
		r.Header.Set("Authorization", "Bearer "+s.config.Token)
		r.Header.Set(model.HostRebootAuthHeader, signature)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 202 {
			t.Fatalf("equivalent canonical signed request failed: status=%d", w.Code)
		}
	}
	if f.count("create") != 1 || f.count("start") != 1 {
		t.Fatal("signed identical operation was executed more than once")
	}
}

func TestHostRebootCommittedOperationFinishesAfterClientCancellation(t *testing.T) {
	s, f := newHostRebootServer(t, rebootTestBoot)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.beforeCreate = cancel
	f.beforeStart = func(execution context.Context) {
		if execution.Err() != nil {
			t.Error("request cancellation aborted a committed host operation")
		}
	}
	result, status := s.rebootHost(ctx, hostRebootRequest(s))
	if status != 202 || result.State != "requested" || f.count("start") != 1 {
		t.Fatalf("committed operation lost: %+v %d", result, status)
	}
}

func TestHostRebootSerializesConcurrentRequestsWithoutBlockingStatus(t *testing.T) {
	s, f := newHostRebootServer(t, rebootTestBoot)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	underlying := f.command
	s.hostDockerCommand = func(ctx context.Context, args ...string) ([]byte, error) {
		if args[0] == "run" {
			once.Do(func() { close(entered) })
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return underlying(ctx, args...)
	}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	type answer struct {
		result model.HostRebootResult
		status int
	}
	answers := make(chan answer, 2)
	request := hostRebootRequest(s)
	invoke := func() {
		result, status := s.rebootHost(context.Background(), request)
		answers <- answer{result, status}
	}
	go invoke()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("preflight did not start")
	}
	go invoke()
	status := make(chan model.Agent, 1)
	go func() { status <- s.snapshotAgent() }()
	select {
	case agent := <-status:
		if agent.Host == nil || !agent.Host.RebootPending {
			t.Fatal("preflight did not publish its admission gate")
		}
	case <-time.After(time.Second):
		t.Fatal("Docker preflight held the Agent status lock")
	}
	if err := s.lockAdmission(context.Background(), model.CreateNodeRequest{ID: "racing-peer", RunID: "racing-run"}); err == nil {
		s.mu.Unlock()
		t.Fatal("Peer admission raced a reboot gate")
	}
	releaseOnce.Do(func() { close(release) })
	for range 2 {
		select {
		case answer := <-answers:
			if answer.status != 202 || answer.result.State != "requested" {
				t.Fatalf("concurrent request failed: %+v", answer)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent reboot did not finish")
		}
	}
	if f.count("create") != 1 || f.count("start") != 1 {
		t.Fatalf("concurrent operation was replayed: %v", f.calls)
	}
}

func TestHostRebootPinsDockerToLocalSocket(t *testing.T) {
	s, _ := newHostRebootServer(t, rebootTestBoot)
	s.hostDockerCommand = nil
	s.config.DockerSocket = "/test/host-docker.sock"
	t.Setenv("DOCKER_HOST", "tcp://other-daemon.invalid:2375")
	var captured []string
	s.docker.commandContext = func(ctx context.Context, binary string, args ...string) *exec.Cmd {
		captured = slices.Clone(args)
		return exec.CommandContext(ctx, "/bin/true")
	}
	if _, err := s.runHostDocker(context.Background(), "info"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(captured, []string{"--host", "unix:///test/host-docker.sock", "info"}) {
		t.Fatalf("Docker target was not pinned: %v", captured)
	}
}

func TestHostRebootFailedPreflightDoesNotReopenShutdownTelemetry(t *testing.T) {
	s, f := newHostRebootServer(t, rebootTestBoot)
	s.hostDockerCommand = func(ctx context.Context, args ...string) ([]byte, error) {
		if args[0] == "run" {
			s.closeTelemetry()
			return nil, errors.New("systemd unavailable during shutdown")
		}
		return f.command(ctx, args...)
	}
	result, status := s.rebootHost(context.Background(), hostRebootRequest(s))
	if status != 409 || result.State != "rejected" || !s.telemetryClosed {
		t.Fatalf("failed preflight reopened shutdown telemetry: %+v %d closed=%v", result, status, s.telemetryClosed)
	}
}

func TestHostRebootShutdownDuringPreflightDoesNotDispatch(t *testing.T) {
	s, f := newHostRebootServer(t, rebootTestBoot)
	s.hostDockerCommand = func(ctx context.Context, args ...string) ([]byte, error) {
		if args[0] == "run" {
			s.beginShutdown()
		}
		return f.command(ctx, args...)
	}
	result, status := s.rebootHost(context.Background(), hostRebootRequest(s))
	if status != 409 || result.State != "rejected" || f.count("create") != 0 || f.count("start") != 0 {
		t.Fatalf("shutdown allowed host reboot dispatch: %+v %d calls=%v", result, status, f.calls)
	}
}

func TestHostRebootRejectsUntrustedReceipt(t *testing.T) {
	for _, kind := range []string{"symlink", "invalid", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := newHostRebootServer(t, rebootTestBoot)
			file := filepath.Join(s.config.DataDir, hostRebootReceiptFile)
			switch kind {
			case "symlink":
				if err := os.Symlink("peer-history", file); err != nil {
					t.Fatal(err)
				}
			case "invalid":
				if err := os.WriteFile(file, []byte(`{"version":1}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "oversized":
				if err := os.WriteFile(file, make([]byte, 17<<10), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.readHostRebootReceipt(); err == nil {
				t.Fatal("untrusted host receipt was accepted")
			}
		})
	}
}

func TestHostRebootProbeWaitsForStartupPeerReconciliation(t *testing.T) {
	s, path := newContainerTestServer(t, map[string]string{"PS": fakeContainerID + "\n"})
	s.config.Listen = "127.0.0.1:0"
	s.config.HostRebootEnabled = true
	s.config.ControlNodeID = rebootTestControl
	s.config.SelfContainer = "agent-task"
	s.config.Token = "fake-internal-test-token"
	s.config.HostRebootToken = "fake-host-control-key"
	s.config.Labels = map[string]string{"swarmNodeId": rebootTestNode}
	s.hostBootIDReader = func() (string, error) { return rebootTestBoot, nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probed := false
	s.hostDockerCommand = func(context.Context, ...string) ([]byte, error) {
		probed = true
		removed := false
		for _, call := range dockerCalls(t, path) {
			if call.Args[0] == "rm" {
				removed = true
			}
		}
		if !removed {
			t.Error("host capability probe delayed startup Peer reconciliation")
		}
		cancel()
		return nil, errors.New("isolated capability probe ends here")
	}
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	select {
	case <-done:
		if !probed {
			t.Fatal("startup did not inspect enabled host capability")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("isolated Agent startup did not finish")
	}
}

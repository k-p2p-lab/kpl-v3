package master

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/auth"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type runtimeRoleLog struct {
	ctx    context.Context
	msg    string
	fields map[string]slog.Value
}

type runtimeRoleLogHandler struct {
	records chan runtimeRoleLog
	attrs   []slog.Attr
}

func (*runtimeRoleLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *runtimeRoleLogHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message != "runtime health" && record.Message != "agent listening" && record.Message != "controller listening" {
		return nil
	}
	fields := make(map[string]slog.Value)
	for _, attr := range h.attrs {
		fields[attr.Key] = attr.Value.Resolve()
	}
	record.Attrs(func(attr slog.Attr) bool { fields[attr.Key] = attr.Value.Resolve(); return true })
	h.records <- runtimeRoleLog{ctx: ctx, msg: record.Message, fields: fields}
	return nil
}
func (h *runtimeRoleLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &runtimeRoleLogHandler{records: h.records, attrs: append(append([]slog.Attr(nil), h.attrs...), attrs...)}
}
func (h *runtimeRoleLogHandler) WithGroup(name string) slog.Handler {
	return h.WithAttrs([]slog.Attr{slog.String("testLogGroup", name)})
}

func runtimeRoleTestLogger() (*slog.Logger, <-chan runtimeRoleLog) {
	h := &runtimeRoleLogHandler{records: make(chan runtimeRoleLog, 16)}
	return slog.New(h), h.records
}

func runtimeRoleTestEnvironment(t *testing.T) string {
	t.Helper()
	t.Setenv("KPL_USER", "runtime-health-test")
	t.Setenv("KPL_PASSWORD", "fake-runtime-health-password")
	t.Setenv("KPL_AGENT_HOST_REBOOT_ENABLED", "false")
	t.Setenv("KPL_SWARM_TASK_NAME", "")
	t.Setenv("KPL_RUN_MIN_FREE_BYTES", "0")
	t.Setenv("KPL_ARCHIVE_MIB_PER_SECOND", "0")
	return auth.InternalToken("runtime-health-test", "fake-runtime-health-password")
}

func awaitRuntimeRoleStartup(t *testing.T, records <-chan runtimeRoleLog, done <-chan error, role string) (context.Context, string) {
	t.Helper()
	var health context.Context
	address := ""
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for health == nil || address == "" {
		select {
		case record := <-records:
			if record.msg == "runtime health" {
				if record.fields["role"].String() != role || record.fields["pid"].Int64() != int64(os.Getpid()) {
					t.Fatalf("health observation belongs to another runtime: role=%s pid=%d", record.fields["role"], record.fields["pid"].Int64())
				}
				health = record.ctx
			}
			if record.msg == role+" listening" {
				address = record.fields["address"].String()
			}
		case err := <-done:
			t.Fatalf("%s exited before startup: %v", role, err)
		case <-deadline.C:
			t.Fatalf("%s did not emit initial health and listening records", role)
		}
	}
	return health, address
}

func runtimeRoleRequest(t *testing.T, method, endpoint, token string, body any) {
	t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	defer client.CloseIdleConnections()
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		t.Fatalf("role fixture request returned HTTP %d", response.StatusCode)
	}
}

func assertRuntimeHealthDuringCleanup(t *testing.T, parent, health context.Context, done <-chan error) {
	t.Helper()
	if parent.Err() == nil || health.Err() != nil {
		t.Fatalf("health lifetime ended with the canceled service instead of cleanup: parent=%v health=%v", parent.Err(), health.Err())
	}
	select {
	case err := <-done:
		t.Fatalf("role returned before its blocked finalization: %v", err)
	default:
	}
}

func awaitRuntimeRoleReturn(t *testing.T, health context.Context, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("role did not return after finalization was released")
	}
	if health.Err() != context.Canceled {
		t.Fatalf("role return retained its health observer: %v", health.Err())
	}
}

func TestControllerRuntimeHealthSurvivesCleanupAndStopsOnReturn(t *testing.T) {
	token := runtimeRoleTestEnvironment(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce, enteredOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	fakeAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/api/v1/nodes" {
			enteredOnce.Do(func() { close(entered) })
			<-release
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer func() { releaseOnce.Do(func() { close(release) }); fakeAgent.Close() }()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	logger, records := runtimeRoleTestLogger()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	finished := make(chan struct{})
	defer func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("Controller fixture did not exit during test cleanup")
		}
	}()
	directory := t.TempDir()
	go func() {
		defer close(finished)
		done <- runController(ctx, logger, []string{"--listen", address, "--data-dir", directory, "--network-manager=", "--agent-discovery-dns=", "--metrics-url=", "--prometheus-url", fakeAgent.URL, "--prometheus-port=9090", "--grafana-port=3000"})
	}()
	health, _ := awaitRuntimeRoleStartup(t, records, done, "controller")
	runtimeRoleRequest(t, http.MethodPost, "http://"+address+"/api/v1/agents/register", token, model.Agent{ID: "fixture-agent", URL: fakeAgent.URL, Capacity: 1, RunDrain: true})
	cancel()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Controller did not enter Agent finalization")
	}
	assertRuntimeHealthDuringCleanup(t, ctx, health, done)
	releaseOnce.Do(func() { close(release) })
	awaitRuntimeRoleReturn(t, health, done)
}

func TestAgentRuntimeHealthSurvivesFinalDrainAndStopsOnReturn(t *testing.T) {
	token := runtimeRoleTestEnvironment(t)
	first, final, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var deliveries atomic.Int64
	fakeController := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/api/v1/events/batch" {
			switch deliveries.Add(1) {
			case 1:
				close(first)
			case 2:
				close(final)
			}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer func() { releaseOnce.Do(func() { close(release) }); fakeController.Close() }()
	// The real Agent rejects loopback Controller URLs because Peers have their
	// own network namespace. Route this accepted fixture hostname directly to
	// our test listener, with no DNS, proxy, or external endpoint access.
	previousTransport := http.DefaultTransport
	dialer := &net.Dialer{}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		if host == "runtime-controller.invalid" {
			address = fakeController.Listener.Addr().String()
		} else if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			return nil, fmt.Errorf("unexpected test endpoint")
		}
		return dialer.DialContext(ctx, network, address)
	}}
	http.DefaultTransport = transport
	defer func() { http.DefaultTransport = previousTransport; transport.CloseIdleConnections() }()
	directory := t.TempDir()
	docker := filepath.Join(directory, "fake-docker")
	script := `#!/bin/sh
case "$1" in
 info) printf '%s\n' '{"OSType":"linux","SwarmState":"active"}' ;;
 image) case "$4" in '{{.Os}}') echo linux ;; '{{.Id}}') echo sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ;; *) exit 1 ;; esac ;;
 network) printf '%s\n' '[{"Name":"test-overlay","Driver":"overlay","Attachable":true,"Scope":"swarm"}]' ;;
 ps) ;;
 *) exit 1 ;;
esac
`
	if err := os.WriteFile(docker, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	logger, records := runtimeRoleTestLogger()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	finished := make(chan struct{})
	defer func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("Agent fixture did not exit during test cleanup")
		}
	}()
	go func() {
		defer close(finished)
		done <- runAgent(ctx, logger, []string{"--id=health-fixture", "--listen=127.0.0.1:0", "--advertise-url=http://fixture-agent:8090", "--controller-url=http://runtime-controller.invalid",
			"--data-dir", filepath.Join(directory, "agent-data"), "--docker-image=kpl:fixture", "--docker-network=test-overlay", "--docker-binary", docker,
			"--docker-socket", filepath.Join(directory, "no-docker.sock"), "--self-container=", "--host-reboot-enabled=false"})
	}()
	health, address := awaitRuntimeRoleStartup(t, records, done, "agent")
	runtimeRoleRequest(t, http.MethodPost, "http://"+address+"/api/v1/telemetry", token, model.EventBatch{Events: []model.TraceEvent{{EventID: "fixture-event", RunID: "fixture-run", NodeID: "fixture-peer", Type: "join", Timestamp: time.Now().UTC()}}})
	select {
	case <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("Agent did not attempt event delivery")
	}
	cancel()
	select {
	case <-final:
	case <-time.After(5 * time.Second):
		t.Fatal("Agent did not retry its pending tail during final drain")
	}
	assertRuntimeHealthDuringCleanup(t, ctx, health, done)
	releaseOnce.Do(func() { close(release) })
	awaitRuntimeRoleReturn(t, health, done)
}

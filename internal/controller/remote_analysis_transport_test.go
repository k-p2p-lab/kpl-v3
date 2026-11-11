package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const remoteTestToken = "0123456789abcdef0123456789abcdef"

func TestRemoteAnalysisBuildCompatibility(t *testing.T) {
	build := "sha256:" + strings.Repeat("1", 64)
	otherBuild := "sha256:" + strings.Repeat("2", 64)
	image := "registry.example:5000/kpl@sha256:" + strings.Repeat("3", 64)
	otherImage := "registry.example:5000/kpl@sha256:" + strings.Repeat("4", 64)
	for _, tc := range []struct {
		name            string
		controllerBuild string
		workerBuild     string
		controllerImage string
		workerImage     string
		want            bool
	}{
		{"same executable", build, build, "", "", true},
		{"same executable with mutable deployment", build, build, "kpl:latest", "kpl:latest", true},
		{"multiarch immutable release", build, otherBuild, image, image, true},
		{"different releases", build, otherBuild, image, otherImage, false},
		{"matching mutable tag", build, otherBuild, "kpl:latest", "kpl:latest", false},
		{"missing runtime images", build, otherBuild, "", "", false},
		{"bare digest", build, otherBuild, strings.TrimPrefix(image, "registry.example:5000/kpl@"), strings.TrimPrefix(image, "registry.example:5000/kpl@"), false},
		{"missing repository", build, otherBuild, "@sha256:" + strings.Repeat("3", 64), "@sha256:" + strings.Repeat("3", 64), false},
		{"malformed digest", build, otherBuild, "kpl@sha256:short", "kpl@sha256:short", false},
		{"URL in repository", build, otherBuild, "http://" + image, "http://" + image, false},
		{"missing image name", build, otherBuild, "registry.example/@sha256:" + strings.Repeat("3", 64), "registry.example/@sha256:" + strings.Repeat("3", 64), false},
		{"unknown builds", "unknown", "unknown", image, image, false},
		{"empty builds", "", "", image, image, false},
		{"unknown controller executable", "unknown", build, image, image, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hello := remoteAnalysisHello{Protocol: remoteAnalysisProtocol, AnalysisVersion: currentAnalysisVersion, Build: tc.controllerBuild, Input: analysisProcessInput{RuntimeImage: tc.controllerImage}}
			if got := compatibleRemoteAnalysis(hello, tc.workerBuild, tc.workerImage); got != tc.want {
				t.Fatalf("compatibility = %v, want %v", got, tc.want)
			}
			if tc.want {
				hello.Protocol++
				if compatibleRemoteAnalysis(hello, tc.workerBuild, tc.workerImage) {
					t.Fatal("mismatched protocol accepted")
				}
				hello.Protocol = remoteAnalysisProtocol
				hello.AnalysisVersion++
				if compatibleRemoteAnalysis(hello, tc.workerBuild, tc.workerImage) {
					t.Fatal("mismatched analysis definition accepted")
				}
			}
		})
	}
}

func remoteAnalysisTransportFixture(t *testing.T) (*Server, analysisProcessInput, []*resultSnapshot, *os.Root) {
	t.Helper()
	s, _ := isolatedAnalysisServer(t)
	batchFixture(t, s, "remote-batch", "first", "completed", 1, 2, 1)
	batchFixture(t, s, "remote-batch", "second", "completed", 2, 2, 3)
	// A sequential read across a prefetch boundary, without retaining thousands
	// of unrelated events in the scientific accumulator.
	file, err := os.OpenFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "first", "events.jsonl"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.Write(bytes.Repeat([]byte("{\"runId\":\"unrelated\"}\n"), 50_000))
	if err := errors.Join(writeErr, file.Close()); err != nil {
		t.Fatal(err)
	}
	input := analysisProcessInput{RunID: "remote-batch", BatchID: "remote-batch", AnalysisID: "remote-analysis", Name: "Repeated scenario", ExpectedRuns: 2}
	var snapshots []*resultSnapshot
	for _, id := range []string{"first", "second"} {
		snapshot, err := s.captureResultFilesContext(t.Context(), id, false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(snapshot.close)
		snapshot.result.SourceHash, err = analysisSourceHash(t.Context(), snapshot.files, nil)
		if err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, snapshot)
		input.Snapshots = append(input.Snapshots, describeWorkerSnapshot(snapshot))
	}
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return s, input, snapshots, root
}

func readRemoteAnalysisArtifact(t *testing.T, root *os.Root, artifacts *workerAnalysisArtifacts) batchAnalysisResult {
	t.Helper()
	data, err := root.ReadFile(artifacts.sinks["full"].staged.temp)
	if err != nil {
		t.Fatal(err)
	}
	var result batchAnalysisResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Excluded == nil {
		t.Fatal("batch artifact must encode an empty excluded array when every Run contributes")
	}
	// The output publication timestamp is measured at completion.
	result.AsOf = time.Time{}
	return result
}

func TestRemoteAnalysisTransportMatchesLocalProcess(t *testing.T) {
	s, input, snapshots, root := remoteAnalysisTransportFixture(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	worker := newRemoteAnalysisWorker(remoteTestToken, logger, s.workerCommand)
	var admissionAttempts atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if admissionAttempts.Add(1) == 1 {
			http.Error(w, "previous analysis reaping", http.StatusTooManyRequests)
			return
		}
		worker.ServeHTTP(w, r)
	}))
	defer endpoint.Close()
	metadata := func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		var request analysisBatchMetadataRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		if method != "analysis.batch-metadata" || len(request.Runs) != len(snapshots) {
			return nil, errors.New("unexpected remote metadata request")
		}
		for _, snapshot := range snapshots {
			if request.Hashes[snapshot.result.ID] != snapshot.result.SourceHash {
				return nil, errors.New("worker changed its source hash")
			}
		}
		return analysisBatchMetadata{SourceHash: analysisSourceHashPrefix + strings.Repeat("e", 64)}, nil
	}
	local, expected, err := s.computeAnalysisProcess(t.Context(), input, snapshots, root, nil, metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer local.discard()
	source := workerSourceHandler(snapshots)
	reads := 0
	var processed int64
	remote, actual, err := computeRemoteAnalysis(t.Context(), endpoint.URL, remoteTestToken, input, root, func(ctx context.Context, method string, body json.RawMessage) (any, error) {
		reads++
		return source(ctx, method, body)
	}, func(progress analysisProcessProgress) {
		processed += progress.Bytes
	}, metadata, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer remote.discard()
	if actual.SourceHash != expected.SourceHash || !reflect.DeepEqual(readRemoteAnalysisArtifact(t, root, local), readRemoteAnalysisArtifact(t, root, remote)) {
		t.Fatal("remote and local results differ")
	}
	if reads < 2 || reads > 5 || admissionAttempts.Load() != 2 {
		t.Fatalf("unexpected source reads or admission attempts: %d / %d", reads, admissionAttempts.Load())
	}
	var eventBytes int64
	for _, snapshot := range snapshots {
		for _, file := range snapshot.files {
			if file.name == "events.jsonl" {
				eventBytes += file.size
			}
		}
	}
	if processed != eventBytes {
		t.Fatalf("progress reported %d of %d source bytes", processed, eventBytes)
	}
}

func TestRemoteAnalysisTransportSingleRunReturnsBothArtifacts(t *testing.T) {
	s, _, snapshots, root := remoteAnalysisTransportFixture(t)
	snapshots = snapshots[:1]
	input := analysisProcessInput{RunID: snapshots[0].result.ID, AnalysisID: "single-analysis", Snapshots: []workerSnapshotDescriptor{describeWorkerSnapshot(snapshots[0])}}
	worker := newRemoteAnalysisWorker(remoteTestToken, slog.New(slog.NewTextHandler(io.Discard, nil)), s.workerCommand)
	endpoint := httptest.NewServer(worker)
	defer endpoint.Close()
	local, _, err := s.computeAnalysisProcess(t.Context(), input, snapshots, root, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer local.discard()
	remote, _, err := computeRemoteAnalysis(t.Context(), endpoint.URL, remoteTestToken, input, root, workerSourceHandler(snapshots), nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer remote.discard()
	for _, name := range []string{"full", "compact"} {
		want, err := root.ReadFile(local.sinks[name].staged.temp)
		if err != nil {
			t.Fatal(err)
		}
		got, err := root.ReadFile(remote.sinks[name].staged.temp)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("remote %s artifact differs from local analysis: %v", name, err)
		}
	}
}

func TestRemoteAnalysisTransportRejectsUnauthenticatedAndConcurrentConnection(t *testing.T) {
	worker := newRemoteAnalysisWorker(remoteTestToken, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	endpoint := httptest.NewServer(worker)
	defer endpoint.Close()
	if err := probeRemoteAnalysisIdle(t.Context(), endpoint.URL, "invalid"); err == nil || !strings.Contains(err.Error(), "HTTP 401") {
		t.Fatalf("unauthenticated idle probe succeeded: %v", err)
	}
	if err := probeRemoteAnalysisIdle(t.Context(), endpoint.URL, remoteTestToken); err != nil {
		t.Fatal(err)
	}
	u, err := remoteAnalysisURL(endpoint.URL)
	if err != nil {
		t.Fatal(err)
	}
	dialer := websocket.Dialer{HandshakeTimeout: time.Second}
	for i, tc := range []struct {
		token string
		code  int
	}{{"invalid", http.StatusUnauthorized}, {remoteTestToken, http.StatusSwitchingProtocols}, {remoteTestToken, http.StatusTooManyRequests}} {
		conn, response, err := dialer.DialContext(t.Context(), u.String(), http.Header{"Authorization": {"Bearer " + tc.token}})
		if conn != nil {
			defer conn.Close()
		}
		if response == nil || response.StatusCode != tc.code || (err == nil) != (tc.code == http.StatusSwitchingProtocols) {
			t.Fatalf("handshake %d did not produce %d: %v, %v", i, tc.code, response, err)
		}
		_ = response.Body.Close()
	}
	if err := probeRemoteAnalysisIdle(t.Context(), endpoint.URL, remoteTestToken); err == nil || !strings.Contains(err.Error(), "HTTP 429") {
		t.Fatalf("busy worker reported idle: %v", err)
	}
}

func TestRemoteAnalysisTransportRejectsCorruptBinaryFrame(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := readRemoteAnalysisFrame(conn); err != nil {
			return
		}
		_ = conn.WriteJSON(remoteAnalysisFrame{Method: "analysis.write", Bytes: 3, SHA256: strings.Repeat("0", 64)})
		_ = conn.WriteMessage(websocket.BinaryMessage, []byte("bad"))
	}))
	defer endpoint.Close()
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	artifacts, _, err := computeRemoteAnalysis(t.Context(), endpoint.URL, remoteTestToken, analysisProcessInput{}, root, nil, nil, nil, 0)
	if artifacts != nil {
		defer artifacts.discard()
	}
	if err == nil || !strings.Contains(err.Error(), "binary frame checksum mismatch") || !errors.Is(err, errRemoteAnalysisCleanupUnconfirmed) {
		t.Fatalf("corrupt binary accepted: %v", err)
	}
	entries, err := os.ReadDir(root.Name())
	if err != nil || len(entries) != 0 {
		t.Fatalf("corrupt transfer left staging files: %v %v", entries, err)
	}
}

func TestRemoteAnalysisTransportCancelReapsChild(t *testing.T) {
	s, input, _, root := remoteAnalysisTransportFixture(t)
	var command *exec.Cmd
	worker := newRemoteAnalysisWorker(remoteTestToken, slog.New(slog.NewTextHandler(io.Discard, nil)), func(kind string) *exec.Cmd {
		command = s.workerCommand(kind)
		return command
	})
	endpoint := httptest.NewServer(worker)
	defer endpoint.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{}, 1)
	finished := make(chan error, 1)
	go func() {
		_, _, err := computeRemoteAnalysis(ctx, endpoint.URL, remoteTestToken, input, root, func(callCtx context.Context, _ string, _ json.RawMessage) (any, error) {
			entered <- struct{}{}
			<-callCtx.Done()
			return nil, callCtx.Err()
		}, nil, nil, 0)
		finished <- err
	}()
	select {
	case <-entered:
	case err := <-finished:
		t.Fatalf("remote process stopped before reading its source: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("remote process did not reach its source")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) || errors.Is(err, errRemoteAnalysisCleanupUnconfirmed) {
			t.Fatalf("wrong canceled analysis result: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Controller analysis remained blocked")
	}
	select {
	case worker.slot <- struct{}{}:
		<-worker.slot
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not release its slot after process cleanup")
	}
	if command == nil || command.ProcessState == nil {
		t.Fatal("worker admitted another analysis before reaping its canceled child")
	}
}

func TestAgentAnalysisWorkerStopReapsAcceptedCalculation(t *testing.T) {
	s, input, _, root := remoteAnalysisTransportFixture(t)
	roleCtx, cancelRole := context.WithCancel(t.Context())
	defer cancelRole()
	image := "registry.example/kpl@sha256:" + strings.Repeat("a", 64)
	handler, stopWorker := NewAgentAnalysisWorker(roleCtx, remoteTestToken, image, slog.New(slog.NewTextHandler(io.Discard, nil)))
	worker := handler.(*remoteAnalysisWorker)
	if worker.runtimeImage != image {
		t.Fatal("Agent analysis handler lost its deployment image identity")
	}
	var command *exec.Cmd
	worker.command = func(kind string) *exec.Cmd {
		command = s.workerCommand(kind)
		return command
	}
	endpoint := httptest.NewServer(handler)
	defer endpoint.Close()
	entered := make(chan struct{}, 1)
	finished := make(chan error, 1)
	go func() {
		_, _, err := computeRemoteAnalysis(t.Context(), endpoint.URL, remoteTestToken, input, root, func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
			entered <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}, nil, nil, 0)
		finished <- err
	}()
	select {
	case <-entered:
	case err := <-finished:
		t.Fatalf("Agent analysis stopped before reading its source: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Agent analysis did not start")
	}
	stopCtx, cancelStop := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelStop()
	if err := stopWorker(stopCtx); err != nil {
		t.Fatal(err)
	}
	if command == nil || command.ProcessState == nil {
		t.Fatal("Agent stop returned before its analysis child was reaped")
	}
	select {
	case err := <-finished:
		if !errors.Is(err, errRemoteAnalysisCleanupUnconfirmed) {
			t.Fatalf("interrupted Agent connection did not preserve its cleanup obligation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Agent shutdown left a Controller transfer blocked")
	}
	if err := probeRemoteAnalysisIdle(t.Context(), endpoint.URL, remoteTestToken); err == nil || !strings.Contains(err.Error(), "HTTP 503") {
		t.Fatalf("stopped Agent analysis handler accepted a new request: %v", err)
	}
	if err := stopWorker(stopCtx); err != nil {
		t.Fatalf("repeated Agent analysis stop failed: %v", err)
	}
}

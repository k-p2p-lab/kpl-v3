package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

type observationTestStart struct {
	RunID string `json:"runId"`
}

func TestObservationWorkerHelper(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-2] != "observation-worker-helper" {
		return
	}
	kind := os.Args[len(os.Args)-1]
	err := workerproc.Serve(context.Background(), kind, func(_ context.Context, _ string, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		var start observationTestStart
		if err := json.Unmarshal(raw, &start); err != nil {
			return nil, err
		}
		client := &runWorkerClient{peer: peer, runID: start.RunID}
		return func(ctx context.Context, method string, _ json.RawMessage) (any, error) {
			at := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
			observation := analysisObservation{RunID: start.RunID, At: at, Groups: []analysisGroup{}}
			if method == "large" {
				// Two complete 1,400-node graph layers are a valid observation
				// shape whose single encoded row exceeds the IPC frame limit.
				const count = 1400
				graph := analysisGraph{Protocol: "transport", Nodes: make([]string, count), Edges: make([][2]int, 0, count*(count-1)/2)}
				for from := range count {
					graph.Nodes[from] = fmt.Sprintf("node-%04d", from)
					for to := from + 1; to < count; to++ {
						graph.Edges = append(graph.Edges, [2]int{from, to})
					}
				}
				other := graph
				other.Protocol = "gossipSub"
				observation.Graphs = []analysisGraph{graph, other}
			}
			data, err := json.Marshal(observation)
			if err != nil {
				return nil, err
			}
			sum := sha256.Sum256(data)
			info := workerObservationInfo{RunID: start.RunID, Epoch: peer.Epoch(), At: at, Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
			switch method {
			case "large", "small":
				err = client.streamObservation(ctx, at, data)
			case "begin", "bad-checksum", "bad-offset", "oversize", "wrong-run", "wrong-epoch":
				if method == "wrong-run" {
					info.RunID = "foreign-run"
				}
				if method == "wrong-epoch" {
					info.Epoch = "foreign-epoch"
				}
				err = client.call(ctx, "observation.begin", info, nil)
				if err == nil && method != "begin" {
					chunk := workerObservationChunk{Data: data}
					if method == "bad-checksum" {
						chunk.Data = append([]byte(nil), data...)
						chunk.Data[0] = '['
					}
					if method == "bad-offset" {
						chunk.Offset = 1
					}
					if method == "oversize" {
						chunk.Data = make([]byte, workerObservationChunkBytes+1)
					}
					err = client.call(ctx, "observation.chunk", chunk, nil)
					if err == nil {
						err = client.call(ctx, "observation.commit", info, nil)
					}
				}
			default:
				err = errors.New("unknown observation test command")
			}
			return info, err
		}, nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "observation worker:", err)
		os.Exit(2)
	}
	os.Exit(0)
}

func observationWorkerFixture(t *testing.T, intercept func(context.Context, string)) (*Server, *runWorkerBroker, *workerproc.Process, string, []byte) {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "observation-run"
	run := model.Experiment{ID: id, State: "running", StartedAt: time.Now().UTC()}
	s.state.experiments[id] = run
	if err := s.persistManifest(run, []byte("name: observations\nphases: [{action: wait, duration: 1ms}]\n")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.config.DataDir, currentRunsDirectory, id, "observations.jsonl")
	prefix := []byte("{\"runId\":\"observation-run\",\"groups\":[]}\n")
	if err := os.WriteFile(path, prefix, 0600); err != nil {
		t.Fatal(err)
	}
	broker := &runWorkerBroker{server: s, runID: id}
	var workerStderr bytes.Buffer
	options := workerproc.Options{Command: func(kind string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestObservationWorkerHelper$", "--", "observation-worker-helper", kind)
		cmd.Stdout, cmd.Stderr = io.Discard, &workerStderr
		return cmd
	}}
	process, err := workerproc.Start(t.Context(), "controller-run", observationTestStart{RunID: id}, func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		if intercept != nil {
			intercept(ctx, method)
		}
		return broker.handle(ctx, method, raw)
	}, options)
	if err != nil {
		t.Fatal(err)
	}
	broker.process = process
	t.Cleanup(func() {
		if err := broker.close(); err != nil {
			t.Errorf("close observation broker: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := process.CloseContext(ctx); err != nil {
			stderr := "worker has not been reaped"
			select {
			case <-process.Done():
				stderr = workerStderr.String()
			default:
			}
			t.Errorf("reap observation worker: %v; stderr: %s", err, stderr)
		}
	})
	return s, broker, process, path, prefix
}

func assertObservationStagesReleased(t *testing.T, b *runWorkerBroker, path string) {
	t.Helper()
	b.observationMu.Lock()
	active := b.observationStage != nil
	b.observationMu.Unlock()
	files, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".observation-*.tmp"))
	if err != nil || active || len(files) != 0 {
		t.Fatalf("observation stage was retained: active=%v files=%v err=%v", active, files, err)
	}
}

func TestWorkerObservationStreamsLargeGraphThroughActualIPC(t *testing.T) {
	_, broker, process, path, prefix := observationWorkerFixture(t, nil)
	var info workerObservationInfo
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	if err := process.Call(ctx, "large", nil, &info); err != nil {
		t.Fatal(err)
	}
	if info.Bytes <= workerproc.MaxMessageBytes {
		t.Fatalf("fixture does not exceed one IPC frame: %d", info.Bytes)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gotPrefix := make([]byte, len(prefix))
	if _, err := io.ReadFull(f, gotPrefix); err != nil || !bytes.Equal(gotPrefix, prefix) {
		t.Fatalf("existing observations changed: %q %v", gotPrefix, err)
	}
	digest := sha256.New()
	if n, err := io.CopyN(digest, f, info.Bytes); err != nil || n != info.Bytes || hex.EncodeToString(digest.Sum(nil)) != info.SHA256 {
		t.Fatalf("graph bytes were lost or changed: n=%d expected=%d err=%v", n, info.Bytes, err)
	}
	if tail, err := io.ReadAll(f); err != nil || string(tail) != "\n" {
		t.Fatalf("observation is not one complete appended row: %q %v", tail, err)
	}
	assertObservationStagesReleased(t, broker, path)
	t.Logf("transferred complete %d-byte observation in <=%d-byte chunks", info.Bytes, workerObservationChunkBytes)
}

func TestWorkerObservationRejectsInvalidTransfersWithoutPartialRows(t *testing.T) {
	for _, method := range []string{"bad-checksum", "bad-offset", "oversize", "wrong-run", "wrong-epoch"} {
		t.Run(method, func(t *testing.T) {
			_, broker, process, path, prefix := observationWorkerFixture(t, nil)
			if err := process.Call(t.Context(), method, nil, nil); err == nil {
				t.Fatal("invalid transfer was accepted")
			}
			if data, err := os.ReadFile(path); err != nil || !bytes.Equal(data, prefix) {
				t.Fatalf("failed transfer leaked a partial row: %q %v", data, err)
			}
			assertObservationStagesReleased(t, broker, path)
		})
	}
}

func TestWorkerObservationSingleStageAndLeaseCloseCleanup(t *testing.T) {
	_, broker, process, path, prefix := observationWorkerFixture(t, nil)
	if err := process.Call(t.Context(), "begin", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := process.Call(t.Context(), "begin", nil, nil); err == nil {
		t.Fatal("more than one observation stage was admitted")
	}
	files, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".observation-*.tmp"))
	if err != nil || len(files) != 1 {
		t.Fatalf("single stage bound was violated: %v %v", files, err)
	}
	if err := broker.close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := process.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	assertObservationStagesReleased(t, broker, path)
	if data, err := os.ReadFile(path); err != nil || !bytes.Equal(data, prefix) {
		t.Fatalf("abandoned stage changed canonical observations: %q %v", data, err)
	}
}

func TestWorkerObservationBrokerClosePreservesCleanupFailure(t *testing.T) {
	_, broker, process, path, _ := observationWorkerFixture(t, nil)
	if err := process.Call(t.Context(), "begin", nil, nil); err != nil {
		t.Fatal(err)
	}
	broker.observationMu.Lock()
	if err := broker.observationStage.file.Close(); err != nil {
		broker.observationMu.Unlock()
		t.Fatal(err)
	}
	broker.observationMu.Unlock()
	if err := broker.close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("broker lost an observation cleanup error: %v", err)
	}
	assertObservationStagesReleased(t, broker, path)
}

func TestWorkerObservationCanceledCallerDoesNotPoisonRunIntegrity(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "canceled-observation"
	s.state.experiments[id] = model.Experiment{ID: id, State: "running", DataState: "collecting"}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.state.recordAnalysisObservation(id, time.Now().UTC(), ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("observation did not respect its caller cancellation: %v", err)
	}
	if run := s.state.experiments[id]; run.IntegrityError != "" || run.DataState != "collecting" {
		t.Fatalf("normal sampler shutdown poisoned experiment integrity: %+v", run)
	}
}

func TestWorkerObservationRecoveryRemovesOnlyOwnedRegularStages(t *testing.T) {
	for _, unsafeKind := range []string{"symlink", "directory"} {
		t.Run(unsafeKind, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			const id = "orphan-stage-run"
			run := model.Experiment{ID: id, State: "failed", DataState: "incomplete"}
			s.state.experiments[id] = run
			if err := s.persistManifest(run, []byte("name: stages\nphases: [{action: wait, duration: 1ms}]\n")); err != nil {
				t.Fatal(err)
			}
			directory := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
			owned := []string{".observation-" + strings.Repeat("a", 32) + ".tmp", ".analysis-" + strings.Repeat("b", 32) + ".tmp"}
			preserved := []string{"analysis.json", ".observation-user-note.tmp", ".analysis-user-note.tmp"}
			for _, name := range append(append([]string(nil), owned...), preserved...) {
				if err := os.WriteFile(filepath.Join(directory, name), []byte("preserved original bytes"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			unsafePath := filepath.Join(directory, ".observation-"+strings.Repeat("c", 32)+".tmp")
			target := filepath.Join(t.TempDir(), "outside-result")
			if err := os.WriteFile(target, []byte("outside bytes"), 0600); err != nil {
				t.Fatal(err)
			}
			if unsafeKind == "symlink" {
				if err := os.Symlink(target, unsafePath); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Mkdir(unsafePath, 0700); err != nil {
				t.Fatal(err)
			}
			if err := s.retireRunRuntime(t.Context(), id); err == nil {
				t.Fatal("unsafe temporary permitted a clean runtime claim")
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, retiredRuntimeDirectory, id+".json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed stage cleanup retired the Run: %v", err)
			}
			if err := os.Remove(unsafePath); err != nil {
				t.Fatal(err)
			}
			if err := s.retireRunRuntime(t.Context(), id); err != nil {
				t.Fatalf("repaired stage cleanup did not recover: %v", err)
			}
			for _, name := range owned {
				if _, err := os.Stat(filepath.Join(directory, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("owned stage survived recovery: %s %v", name, err)
				}
			}
			for _, name := range preserved {
				if data, err := os.ReadFile(filepath.Join(directory, name)); err != nil || string(data) != "preserved original bytes" {
					t.Fatalf("cleanup changed a user or canonical artifact: %s %v", name, err)
				}
			}
			if data, err := os.ReadFile(target); err != nil || string(data) != "outside bytes" {
				t.Fatalf("cleanup followed a stage symlink: %q %v", data, err)
			}
		})
	}
}

func TestWorkerObservationStageRecoveryDoesNotRecreateMissingOrDeletedRun(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	for _, id := range []string{"missing-stage-run", "deleted-stage-run"} {
		if id == "deleted-stage-run" {
			s.state.persistMu.Lock()
			err := s.markResultDeletedLocked(id)
			s.state.persistMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := s.cleanupRunWorkerStages(t.Context(), id); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, id)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stage cleanup recreated a missing/deleted Run: %s %v", id, err)
		}
	}
}

func TestWorkerObservationCancellationCleansTransferBeforeReaping(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	var chunks atomic.Int32
	_, broker, process, path, prefix := observationWorkerFixture(t, func(ctx context.Context, method string) {
		if method == "observation.chunk" && chunks.Add(1) == 2 {
			once.Do(func() { close(entered) })
			<-ctx.Done()
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- process.Call(ctx, "large", nil, nil) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("transfer did not reach a pending chunk")
	}
	broker.observationMu.Lock()
	stagedBytes := broker.observationStage.bytes
	broker.observationMu.Unlock()
	if stagedBytes != workerObservationChunkBytes {
		t.Fatalf("cancellation did not interrupt an actual partial transfer: %d", stagedBytes)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("transfer cancellation was lost: %v", err)
	}
	if err := broker.close(); err != nil {
		t.Fatal(err)
	}
	stopCtx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	if err := process.CloseContext(stopCtx); err != nil {
		t.Fatal(err)
	}
	assertObservationStagesReleased(t, broker, path)
	if data, err := os.ReadFile(path); err != nil || !bytes.Equal(data, prefix) {
		t.Fatalf("canceled transfer leaked a partial row: %q %v", data, err)
	}
}

type failingObservationFile struct {
	*os.File
	writeErr, syncErr, rollbackErr error
	writes                         int
}

func (f *failingObservationFile) Write(data []byte) (int, error) {
	f.writes++
	if f.writeErr != nil {
		n, _ := f.File.Write(data[:min(7, len(data))])
		return n, f.writeErr
	}
	return f.File.Write(data)
}
func (f *failingObservationFile) Sync() error {
	if f.syncErr != nil {
		err := f.syncErr
		f.syncErr = nil
		return err
	}
	return f.File.Sync()
}
func (f *failingObservationFile) Truncate(size int64) error {
	return errors.Join(f.File.Truncate(size), f.rollbackErr)
}

func TestWorkerObservationAppendRollsBackPartialWriteAndFsyncFailures(t *testing.T) {
	for _, mode := range []string{"write", "fsync", "rollback", "cancel-after-prefix"} {
		t.Run(mode, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "observations")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			prefix := []byte("original complete row\n")
			if _, err := file.Write(prefix); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected observation I/O failure")
			rollback := errors.New("injected rollback durability failure")
			target := &failingObservationFile{File: file}
			switch mode {
			case "write", "rollback":
				target.writeErr = injected
			case "fsync":
				target.syncErr = injected
			}
			if mode == "rollback" {
				target.rollbackErr = rollback
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var source io.Reader = strings.NewReader(strings.Repeat("x", 128<<10))
			if mode == "cancel-after-prefix" {
				source = &cancelObservationReader{Reader: source, cancel: cancel}
			}
			err = appendObservationBytes(ctx, target, source, int64(len(prefix)), 128<<10)
			if mode == "cancel-after-prefix" {
				if !errors.Is(err, context.Canceled) || target.writes == 0 {
					t.Fatalf("partial copy cancellation was not exercised: %v", err)
				}
			} else if !errors.Is(err, injected) || mode == "rollback" && !errors.Is(err, rollback) {
				t.Fatalf("append or rollback failure was discarded: %v", err)
			}
			if data, err := os.ReadFile(file.Name()); err != nil || !bytes.Equal(data, prefix) {
				t.Fatalf("failed append exposed partial bytes: len=%d err=%v", len(data), err)
			}
		})
	}
}

type cancelObservationReader struct {
	io.Reader
	cancel context.CancelFunc
}

func (r *cancelObservationReader) Read(data []byte) (int, error) {
	n, err := r.Reader.Read(data)
	r.cancel()
	return n, err
}

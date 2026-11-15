//go:build linux

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
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

func TestObservationDeadlineWorkerHelper(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-1] != "observation-deadline-helper" {
		return
	}
	gate := os.Args[len(os.Args)-2]
	err := workerproc.Serve(context.Background(), "controller-run", func(factoryCtx context.Context, _ string, _ json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		client := &runWorkerClient{peer: peer, runID: peer.RunID(), ctx: factoryCtx, server: New(ServerConfig{}, nil)}
		return func(ctx context.Context, method string, _ json.RawMessage) (any, error) {
			if method == "cancel" {
				return nil, nil
			}
			if method != "execute" {
				return nil, errors.New("unsupported observation deadline helper command")
			}
			ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			if err := client.beginFinalization(ctx); err != nil {
				return nil, err
			}
			at := time.Now().UTC()
			data, err := json.Marshal(analysisObservation{RunID: peer.RunID(), At: at, Groups: []analysisGroup{}})
			if err != nil {
				return nil, err
			}
			sum := sha256.Sum256(data)
			info := workerObservationInfo{RunID: peer.RunID(), Epoch: peer.Epoch(), At: at, Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
			if err := client.call(ctx, "observation.begin", info, nil); err != nil {
				return nil, err
			}
			if err := client.call(ctx, "observation.chunk", workerObservationChunk{Data: data}, nil); err != nil {
				return nil, err
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, gate+"?pid="+strconv.Itoa(os.Getpid()), nil)
			if err != nil {
				return nil, err
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				return nil, err
			}
			response.Body.Close()
			return nil, client.call(ctx, "observation.commit", info, nil)
		}, nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "observation deadline worker:", err)
		os.Exit(2)
	}
	os.Exit(0)
}

func observationDeadlineProcessRunning(pid int) bool {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return true
	}
	fields := strings.Fields(string(data[end+1:]))
	return len(fields) == 0 || fields[0] != "Z"
}

func TestWorkerFinalizationStartsReapingWithBlockedObservationCommit(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	contacted, entered := make(chan int, 1), make(chan struct{})
	allowHold, release := make(chan struct{}), make(chan struct{})
	var allowOnce, releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	t.Cleanup(func() { allowOnce.Do(func() { close(allowHold) }) })
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pid, err := strconv.Atoi(r.URL.Query().Get("pid"))
		if err != nil || pid <= 0 {
			http.Error(w, "invalid helper PID", http.StatusBadRequest)
			return
		}
		contacted <- pid
		select {
		case <-allowHold:
		case <-r.Context().Done():
			return
		}
		s.state.persistMu.Lock()
		defer s.state.persistMu.Unlock()
		close(entered)
		w.WriteHeader(http.StatusNoContent)
		w.(http.Flusher).Flush()
		<-release
	}))
	t.Cleanup(func() {
		allowOnce.Do(func() { close(allowHold) })
		releaseOnce.Do(func() { close(release) })
		gate.Close()
	})
	var starts atomic.Int32
	nextStarted := make(chan struct{})
	s.workerCommand = func(kind string) *exec.Cmd {
		if kind == "controller-run" {
			switch starts.Add(1) {
			case 1:
				command := exec.Command(os.Args[0], "-test.run=^TestObservationDeadlineWorkerHelper$", "--", gate.URL, "observation-deadline-helper")
				command.Stdout, command.Stderr = io.Discard, io.Discard
				return command
			case 2:
				close(nextStarted)
			}
		}
		command := exec.Command(os.Args[0], "-test.run=^TestControllerOSWorkerHelper$", "--", "controller-worker-helper", kind)
		command.Stdout, command.Stderr = io.Discard, io.Discard
		return command
	}
	raw := []byte("version: 4\nname: observation-deadline\njobShutdownTimeout: 2s\nphases: [{action: wait, duration: 1ms}]\n")
	first, err := s.StartScenario(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	var pid int
	select {
	case pid = <-contacted:
	case <-time.After(3 * time.Second):
		t.Fatal("the real worker did not stage its observation before finalization")
	}
	prefix, _ := json.Marshal(analysisObservation{RunID: first.ID, At: time.Now().Add(-time.Second).UTC(), Groups: []analysisGroup{}})
	prefix = append(prefix, '\n')
	canonical := filepath.Join(s.config.DataDir, currentRunsDirectory, first.ID, "observations.jsonl")
	if err := os.WriteFile(canonical, prefix, 0600); err != nil {
		t.Fatal(err)
	}
	second, err := s.StartScenario(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	allowOnce.Do(func() { close(allowHold) })
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the persistence gate did not start")
	}
	// Confirm the production callback, rather than just a test mutex holder,
	// owns observationMu and waits on the real persistence mutex.
	retirementReviewWait(t, "observation.commit did not enter the persistence boundary", func() bool {
		stacks := make([]byte, 2<<20)
		n := runtime.Stack(stacks, true)
		return bytes.Contains(stacks[:n], []byte("commitObservationStage"))
	})
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	reaping := false
	for !reaping {
		s.workerMu.Lock()
		reaping = len(s.unreapedWorkers[first.ID]) > 0
		s.workerMu.Unlock()
		reaping = reaping || !observationDeadlineProcessRunning(pid)
		if reaping {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			releaseOnce.Do(func() { close(release) })
			waitWorkerRuns(t, s)
			t.Fatal("expired finalization remained blocked in broker.close before starting worker reaping")
		}
	}
	select {
	case <-nextStarted:
		t.Fatal("next Run started while the previous observation callback still owned persistence")
	default:
	}
	if data, err := os.ReadFile(canonical); err != nil || !bytes.Equal(data, prefix) {
		t.Fatalf("blocked observation changed the canonical prefix: %q, %v", data, err)
	}
	releaseOnce.Do(func() { close(release) })
	waitWorkerRuns(t, s)
	s.state.mu.RLock()
	failed, completed := s.state.experiments[first.ID], s.state.experiments[second.ID]
	s.state.mu.RUnlock()
	if failed.State != "failed" || failed.DataState != "incomplete" || completed.State != "completed" {
		t.Fatalf("observation cleanup changed scientific failure or blocked the next Run: first=%+v next=%+v", failed, completed)
	}
	if data, err := os.ReadFile(canonical); err != nil || !bytes.Equal(data, prefix) {
		t.Fatalf("expired observation was appended after its persistence wait ended: %q, %v", data, err)
	}
	stages, err := filepath.Glob(filepath.Join(filepath.Dir(canonical), ".observation-*.tmp"))
	if err != nil || len(stages) != 0 {
		t.Fatalf("worker cleanup retained its staged observation: %v, %v", stages, err)
	}
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if err == nil && strings.HasPrefix(target, filepath.Dir(canonical)+"/") && strings.Contains(target, ".observation-") {
			t.Fatalf("worker cleanup retained an open observation descriptor: %s", target)
		}
	}
}

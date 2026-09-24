//go:build linux

package agent

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestBlockedConfigWriteKeepsAgentResponsiveAndHonorsRunFence(t *testing.T) {
	s, dockerLog := newContainerTestServer(t, nil)
	s.config.Capacity = 1
	request := model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "workers", Generation: 2}
	dir := filepath.Join(s.config.DataDir, "nodes", request.ID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "peer.json")
	// A writer cannot open this FIFO until a reader exists, simulating stalled
	// config storage without sleeping while holding the Agent's mutex.
	if err := syscall.Mkfifo(configPath, 0o600); err != nil {
		t.Fatal(err)
	}
	created := make(chan struct{})
	var createErr error
	go func() {
		defer close(created)
		_, createErr = s.createNode(context.Background(), request)
	}()
	var reader *os.File
	var drained chan struct{}
	releaseWrite := func() {
		if reader != nil {
			return
		}
		var err error
		reader, err = os.OpenFile(configPath, os.O_RDWR|syscall.O_NONBLOCK, 0)
		if err != nil {
			t.Fatal(err)
		}
		drained = make(chan struct{})
		go func() { defer close(drained); _, _ = io.Copy(io.Discard, reader) }()
	}
	defer func() {
		releaseWrite()
		select {
		case <-created:
		case <-time.After(5 * time.Second):
			t.Error("config writer did not exit")
		}
		_ = reader.Close()
		<-drained
	}()

	var pending *process
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.mu.TryRLock() {
			pending = s.processes[request.ID]
			s.mu.RUnlock()
			if pending != nil {
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if pending == nil {
		t.Fatal("config write blocked the Agent state lock or did not reserve admission")
	}
	select {
	case <-created:
		t.Fatal("config write was not blocked")
	default:
	}
	if snapshot := s.snapshot(); snapshot.Agent.ActiveNodes != 1 || len(snapshot.Nodes) != 1 {
		t.Fatalf("pending admission is missing from heartbeat: %+v", snapshot)
	}
	if _, err := s.createNode(context.Background(), model.CreateNodeRequest{ID: "other", RunID: "other-run", Group: "workers"}); err == nil || !strings.Contains(err.Error(), "capacity") {
		t.Fatalf("pending admission did not reserve capacity: %v", err)
	}
	s.stopRunGeneration(request.RunID, request.Generation)
	if snapshot := s.snapshot(); snapshot.Nodes[0].State != model.NodeStopping {
		t.Fatalf("stop could not reach a preparing peer: %+v", snapshot.Nodes)
	}
	releaseWrite()
	select {
	case <-created:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled create did not finish after storage recovered")
	}
	if !errors.Is(createErr, context.Canceled) {
		t.Fatalf("create error = %v, want cancellation", createErr)
	}
	select {
	case <-pending.done:
	default:
		t.Fatal("cancelled preparation did not finish the process")
	}
	if snapshot := s.snapshot(); snapshot.Agent.ActiveNodes != 0 || snapshot.Nodes[0].State != model.NodeStopped {
		t.Fatalf("cancelled preparation retained capacity: %+v", snapshot)
	}
	if calls := dockerCalls(t, dockerLog); len(calls) != 0 {
		t.Fatalf("fenced peer reached Docker: %+v", calls)
	}
}

func TestConfigWriteFailureReleasesAdmission(t *testing.T) {
	s, dockerLog := newContainerTestServer(t, nil)
	if err := os.WriteFile(filepath.Join(s.config.DataDir, "nodes"), []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := s.createNode(context.Background(), model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "workers"})
	if err == nil {
		t.Fatal("invalid storage did not fail creation")
	}
	snapshot := s.snapshot()
	if snapshot.Agent.ActiveNodes != 0 || len(snapshot.Nodes) != 1 || snapshot.Nodes[0].State != model.NodeFailed {
		t.Fatalf("failed preparation retained capacity or lost failure status: %+v", snapshot)
	}
	select {
	case <-s.processes["peer"].done:
	default:
		t.Fatal("failed preparation did not signal completion")
	}
	if calls := dockerCalls(t, dockerLog); len(calls) != 0 {
		t.Fatalf("invalid configuration reached Docker: %+v", calls)
	}
}

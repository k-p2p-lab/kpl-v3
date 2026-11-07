package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestCleanupRetryPreservesPeerExitFailure(t *testing.T) {
	settings := map[string]string{"EXIT": "7", "FAIL": "rm"}
	s, _ := newContainerTestServer(t, settings)
	if _, err := s.createNode(t.Context(), model.CreateNodeRequest{ID: "failed", RunID: "run", Group: "workers"}); err != nil {
		t.Fatal(err)
	}
	waitForNodeState(t, s, "failed", model.NodeFailed)
	first := s.nodes()[0]
	if !strings.Contains(first.Error, "exited with code 7") || s.snapshotAgent().ActiveNodes != 1 {
		t.Fatalf("initial process/cleanup failure missing: %+v", first)
	}
	delete(settings, "FAIL")
	if err := s.stopNode("failed"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.waitRunContainers(ctx, "run", 0); err != nil {
		t.Fatal(err)
	}
	last := s.nodes()[0]
	if last.State != model.NodeFailed || !strings.Contains(last.Error, "exited with code 7") || strings.Contains(last.Error, "daemon unavailable during rm") || last.Metadata["cleanupComplete"] != "true" || s.snapshotAgent().ActiveNodes != 0 {
		t.Fatalf("successful removal rewrote process failure: %+v", last)
	}
}

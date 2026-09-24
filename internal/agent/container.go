package agent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const (
	containerCleanupTimeout = 2 * time.Minute
	// An in-flight create may need its remaining 45 seconds to return the
	// daemon's container ID before its independent cleanup can begin.
	containerStopTimeout = 175 * time.Second
)

// A Docker create runs outside Server.mu: peers report status during startup,
// and stop-all must be able to fence/cancel a create while the daemon is busy.
func (s *Server) runDockerProcess(ctx context.Context, proc *process, config []byte, lifetime string) {
	defer proc.cancel()
	s.mu.RLock()
	node := proc.node
	s.mu.RUnlock()
	id, apiURL, err := s.docker.createWithAdmission(ctx, node, config, func() {
		// Match v2's ServiceCreate admission boundary. Docker CLI/daemon create
		// latency must not consume a peer's lifetime before it exists. Copy,
		// startup and bootstrap still consume lifetime, as scheduling did in v2.
		s.mu.Lock()
		setProcessMetadata(proc, "containerCreatedAt", time.Now().UTC().Format(time.RFC3339Nano))
		s.mu.Unlock()
		scheduleLifetimeStop(ctx, lifetime, func() { _ = s.stopNode(node.ID) })
	})
	if err != nil {
		var cleanupErr error
		if errors.Is(err, errDockerCleanup) {
			cleanupErr = err
			s.mu.Lock()
			proc.containerID = id
			s.mu.Unlock()
		}
		s.finishProcess(node.ID, proc, fmt.Errorf("create peer container: %w", err), cleanupErr)
		return
	}
	s.mu.Lock()
	proc.containerID = id
	proc.apiURL = apiURL
	metadata := make(map[string]string, len(proc.node.Metadata)+1)
	for key, value := range proc.node.Metadata {
		metadata[key] = value
	}
	metadata["containerId"] = id
	metadata["containerStartedAt"] = time.Now().UTC().Format(time.RFC3339Nano)
	proc.node.Metadata = metadata
	s.mu.Unlock()
	err = s.docker.wait(ctx, id)
	// Killing the Docker CLI does not kill the container. Always ask the daemon
	// to remove this exact container using an independent cleanup context.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), containerCleanupTimeout)
	cleanupErr := s.docker.remove(cleanupCtx, id)
	cleanupCancel()
	if cleanupErr != nil {
		cleanupErr = fmt.Errorf("remove peer container %s: %w", id, cleanupErr)
		s.logger.Error("peer container cleanup failed", "node", node.ID, "container", id, "error", cleanupErr)
	}
	s.finishProcess(node.ID, proc, err, cleanupErr)
}

// Caller holds Server.mu. Clone metadata because previously returned snapshots
// and the startup configuration may still reference the earlier map.
func setProcessMetadata(proc *process, key, value string) {
	metadata := make(map[string]string, len(proc.node.Metadata)+1)
	for k, v := range proc.node.Metadata {
		metadata[k] = v
	}
	metadata[key] = value
	proc.node.Metadata = metadata
}

func (s *Server) beginShutdown() {
	s.mu.Lock()
	s.shuttingDown = true
	s.mu.Unlock()
	s.stopAll()
}

// Caller holds Server.mu. Preserve the target of a failed rm so a subsequent
// stop request can recover after a temporary Docker daemon outage.
func (s *Server) retryContainerCleanupLocked(proc *process) {
	proc.exited = false
	proc.node.State = model.NodeStopping
	proc.done = make(chan struct{})
	id, nodeID := proc.containerID, proc.node.ID
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), containerCleanupTimeout)
		defer cancel()
		err := s.docker.remove(ctx, id)
		s.finishProcess(nodeID, proc, nil, err)
	}()
}

func (s *Server) waitRunContainers(ctx context.Context, runID string, generation uint64) error {
	s.mu.RLock()
	type pendingCleanup struct {
		proc *process
		done <-chan struct{}
	}
	var pending []pendingCleanup
	for _, proc := range s.processes {
		if proc.node.RunID == runID && proc.node.Generation <= generation {
			pending = append(pending, pendingCleanup{proc: proc, done: proc.done})
		}
	}
	s.mu.RUnlock()
	var failures []error
	for _, item := range pending {
		if item.done != nil {
			select {
			case <-item.done:
			case <-ctx.Done():
				return fmt.Errorf("wait for peer container cleanup: %w", ctx.Err())
			}
		}
		s.mu.RLock()
		if item.proc.cleanupErr != nil {
			failures = append(failures, item.proc.cleanupErr)
		}
		s.mu.RUnlock()
	}
	return errors.Join(failures...)
}

func (s *Server) waitStopped(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.waitStoppedContext(ctx)
}

func (s *Server) waitStoppedContext(ctx context.Context) error {
	s.mu.RLock()
	pending := make([]*process, 0, len(s.processes))
	for _, proc := range s.processes {
		pending = append(pending, proc)
	}
	s.mu.RUnlock()
	var failures []error
	for _, proc := range pending {
		for {
			s.mu.RLock()
			done, exited, cleanupErr, nodeID := proc.done, proc.exited, proc.cleanupErr, proc.node.ID
			s.mu.RUnlock()
			if exited {
				if cleanupErr != nil {
					failures = append(failures, fmt.Errorf("peer %q cleanup failed: %w", nodeID, cleanupErr))
				}
				break
			}
			if done == nil {
				failures = append(failures, fmt.Errorf("peer %q has no cleanup completion signal", nodeID))
				break
			}
			select {
			case <-done:
				// A retry can replace the completion channel. Re-read the
				// process rather than treating an earlier failed attempt as done.
			case <-ctx.Done():
				return errors.Join(append(failures, fmt.Errorf("wait for peer %q cleanup: %w", nodeID, ctx.Err()))...)
			}
		}
	}
	return errors.Join(failures...)
}

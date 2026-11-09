package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/auth"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const hostRebootConfirmationTimeout = 3 * time.Minute

// The Agent has separate 45s preparation and committed-execution budgets.
const hostRebootRequestTimeout = 100 * time.Second

func (s *Server) updateHostReboot(job *hostRebootJob, update func(*hostRebootStatus)) error {
	s.hostRebootMu.Lock()
	defer s.hostRebootMu.Unlock()
	status := cloneHostRebootStatus(job.status)
	update(&status)
	if err := s.persistHostReboot(status); err != nil {
		return err
	}
	job.status = status
	return nil
}

func (s *Server) copyHostReboot(job *hostRebootJob) hostRebootStatus {
	s.hostRebootMu.Lock()
	defer s.hostRebootMu.Unlock()
	return cloneHostRebootStatus(job.status)
}

func (s *Server) setRebootHost(job *hostRebootJob, index int, state, message string) error {
	return s.updateHostReboot(job, func(status *hostRebootStatus) {
		status.Hosts[index].State, status.Hosts[index].Error = state, message
	})
}

func (s *Server) runHostReboot(ctx context.Context, job *hostRebootJob) {
	defer s.finishWork(job.slot)
	defer func() {
		s.cancelMu.Lock()
		for nodeID, operation := range s.hostRebootActiveNodes {
			if operation == job.status.ID {
				delete(s.hostRebootActiveNodes, nodeID)
			}
		}
		s.cancelMu.Unlock()
	}()
	err := func() error {
		if err := s.waitWork(ctx, job.slot); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
		defer cancel()
		now := time.Now().UTC()
		if err := s.updateHostReboot(job, func(status *hostRebootStatus) { status.State, status.StartedAt = "running", &now }); err != nil {
			return err
		}
		s.cancelMu.Lock()
		if s.hostRebootActiveNodes == nil {
			s.hostRebootActiveNodes = make(map[string]string)
		}
		for _, target := range job.status.Targets {
			s.hostRebootActiveNodes[target.NodeID] = job.status.ID
		}
		s.cancelMu.Unlock()
		s.updateWorkStatus(job.slot.id, "Preparing Linux host reboot", 0)
		// Hold all targets durably before dispatch, including future registrations
		// under different Agent IDs. Verified return restores their previous state.
		for _, target := range job.status.Targets {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := s.state.acquireHostRebootHold(target.NodeID, job.status.ID); err != nil {
				return err
			}
		}
		var workers sync.WaitGroup
		var errorsMu sync.Mutex
		var dispatchErrors []error
		semaphore := make(chan struct{}, 4)
		for index, target := range job.status.Targets {
			workers.Add(1)
			go func() {
				defer workers.Done()
				select {
				case semaphore <- struct{}{}:
					defer func() { <-semaphore }()
				case <-ctx.Done():
					return
				}
				if err := s.dispatchHostReboot(ctx, job, index, target); err != nil {
					errorsMu.Lock()
					dispatchErrors = append(dispatchErrors, err)
					errorsMu.Unlock()
				}
			}()
		}
		workers.Wait()
		if err := errors.Join(dispatchErrors...); err != nil {
			return err
		}
		return s.confirmHostReboots(ctx, job, hostRebootConfirmationTimeout)
	}()
	finished := time.Now().UTC()
	if persistErr := s.updateHostReboot(job, func(status *hostRebootStatus) {
		status.FinishedAt = &finished
		if err == nil {
			status.State, status.Error = "completed", ""
			return
		}
		status.State, status.Error = "failed", err.Error()
		if errors.Is(err, context.Canceled) {
			status.State = "interrupted"
		}
		for i := range status.Hosts {
			host := &status.Hosts[i]
			if host.State == "queued" {
				host.State, host.Error = "rejected", "The operation ended before this host's reboot request was sent."
			} else if host.State == "checking" {
				host.State, host.Error = "unknown", "No conclusive Agent response was recorded; do not automatically retry."
			}
		}
	}); persistErr != nil {
		s.logger.Error("persist host reboot outcome", "operation", job.status.ID, "error", persistErr)
	} else {
		s.cleanupRestoredHostRebootHolds(s.copyHostReboot(job))
	}
}

func (s *Server) dispatchHostReboot(ctx context.Context, job *hostRebootJob, index int, target hostRebootTarget) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	agent, err := s.currentRebootAgent(target)
	if err != nil {
		return s.setRebootHost(job, index, "rejected", err.Error())
	}
	proof, err := s.requestHostRebootProof(ctx, agent)
	if err != nil || proof.proof.BootID != target.BootID || proof.proof.RebootPending || !proof.proof.StartupReconciled || !proof.proof.RebootSupported {
		return s.setRebootHost(job, index, "rejected", "An authenticated, ready host matching this boot could not be verified; no reboot request was sent.")
	}
	// Persist possible dispatch before the irreversible call. A restart never
	// treats an unrecorded response as permission to submit this operation again.
	if err := s.updateHostReboot(job, func(status *hostRebootStatus) {
		now := time.Now().UTC()
		status.Hosts[index].State, status.Hosts[index].Error, status.Hosts[index].RequestedAt = "checking", "", &now
	}); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, s.setRebootHost(job, index, "rejected", "Canceled before dispatch."))
	}
	status := s.copyHostReboot(job)
	fence := hostRebootFence{OperationID: status.ID, BootID: target.BootID, AgentStartedAt: target.AgentStartedAt, RequestedAt: *status.Hosts[index].RequestedAt}
	if err := s.state.setHostRebootFence(target.NodeID, &fence); err != nil {
		return errors.Join(err, s.setRebootHost(job, index, "rejected", "The reboot fence could not be saved; no request was sent."))
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, s.setRebootHost(job, index, "rejected", "Canceled before dispatch."), s.state.setHostRebootFence(target.NodeID, nil))
	}
	request := model.HostRebootRequest{OperationID: job.status.ID, NodeID: target.NodeID, BootID: target.BootID, AgentStartedAt: target.AgentStartedAt}
	result, err := s.sendHostReboot(ctx, agent.URL, request)
	if err != nil {
		return s.setRebootHost(job, index, "unknown", err.Error())
	}
	if err := s.setRebootHost(job, index, result.State, result.Error); err != nil {
		return err
	}
	if result.State == "rejected" {
		return s.state.setHostRebootFence(target.NodeID, nil)
	}
	return nil
}

// One attempt only. Redirects, invalid acknowledgments and transport failures
// are unknown, never instructions to resend a Linux reboot.
func (s *Server) sendHostReboot(ctx context.Context, agentURL string, input model.HostRebootRequest) (model.HostRebootResult, error) {
	input.AgentStartedAt = input.AgentStartedAt.UTC()
	data, err := json.Marshal(input)
	if err != nil {
		return model.HostRebootResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, hostRebootRequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(agentURL, "/")+"/api/v1/host/reboot", bytes.NewReader(data))
	if err != nil {
		return model.HostRebootResult{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	if s.config.Token != "" {
		request.Header.Set("Authorization", "Bearer "+s.config.Token)
	}
	if s.config.HostRebootToken != "" {
		request.Header.Set(model.HostRebootAuthHeader, auth.HostRebootSignature(s.config.HostRebootToken, data))
	}
	client := *s.client
	client.Timeout = hostRebootRequestTimeout
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(request)
	if err != nil {
		return model.HostRebootResult{}, fmt.Errorf("Agent response is unknown: %w", err)
	}
	defer response.Body.Close()
	data, err = io.ReadAll(io.LimitReader(response.Body, 4097))
	var result model.HostRebootResult
	if err != nil || len(data) > 4096 || json.Unmarshal(data, &result) != nil || result.OperationID != input.OperationID || result.NodeID != input.NodeID || result.BootID != input.BootID {
		return result, errors.New("Agent returned an invalid reboot acknowledgment; dispatch is unknown")
	}
	signature := result.Signature
	result.Signature = ""
	resultBytes, err := json.Marshal(result)
	if err != nil || s.config.HostRebootToken == "" || signature == "" || !auth.Equal(signature, auth.HostRebootAckSignature(s.config.HostRebootToken, dataForRebootRequest(input), resultBytes)) {
		return result, errors.New("Agent reboot acknowledgment is not authenticated; dispatch is unknown")
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 && (result.State == "requested" || result.State == "unknown" || result.State == "rejected") {
		return result, nil
	}
	if response.StatusCode >= 400 && response.StatusCode < 600 && result.State == "rejected" {
		return result, nil
	}
	return result, fmt.Errorf("Agent returned %s; reboot dispatch is unknown", response.Status)
}

func (s *Server) confirmHostReboots(ctx context.Context, job *hostRebootJob, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		status := s.copyHostReboot(job)
		proofErr := s.confirmRebootProofs(ctx, &status)
		confirmed, pending := 0, 0
		for _, host := range status.Hosts {
			if host.State == "rebooted" {
				confirmed++
			} else if host.State == "requested" || host.State == "unknown" {
				pending++
			}
		}
		if err := s.updateHostReboot(job, func(current *hostRebootStatus) { current.Hosts = status.Hosts }); err != nil {
			return err
		}
		if proofErr != nil {
			return proofErr
		}
		s.updateWorkStatus(job.slot.id, fmt.Sprintf("Waiting for host boot confirmation · %d/%d", confirmed, len(status.Hosts)), 100*float64(confirmed)/float64(len(status.Hosts)))
		if confirmed == len(status.Hosts) {
			return nil
		}
		if pending == 0 {
			return errors.New("one or more hosts rejected reboot; inspect the host outcomes")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

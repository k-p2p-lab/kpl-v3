package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const analysisWorkerCleanupFile = ".analysis-worker-cleanup.json"

type analysisWorkerCleanupRecord struct {
	Endpoint  string    `json:"endpoint"`
	AgentID   string    `json:"agentId,omitempty"`
	HostID    string    `json:"hostId,omitempty"`
	StartedAt time.Time `json:"startedAt,omitempty"`
}

type analysisWorkerCleanupLedger struct {
	Version int                           `json:"version"`
	Workers []analysisWorkerCleanupRecord `json:"workers"`
}

func readAnalysisWorkerCleanup(root *os.Root) ([]analysisWorkerCleanupRecord, error) {
	data, err := readResumeFile(root, analysisWorkerCleanupFile, resultMetadataLimit)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var legacy []string
	var records []analysisWorkerCleanupRecord
	if json.Unmarshal(data, &legacy) == nil && legacy != nil {
		for _, endpoint := range legacy {
			records = append(records, analysisWorkerCleanupRecord{Endpoint: endpoint})
		}
	} else {
		var ledger analysisWorkerCleanupLedger
		if json.Unmarshal(data, &ledger) != nil || ledger.Version != 1 || ledger.Workers == nil {
			return nil, errors.New("invalid analysis worker cleanup ledger")
		}
		records = ledger.Workers
	}
	seen := make(map[string]bool, len(records))
	for _, record := range records {
		if _, err := remoteAnalysisURL(record.Endpoint); err != nil || seen[record.Endpoint] {
			return nil, errors.New("invalid worker endpoint in analysis cleanup ledger")
		}
		if record.AgentID != "" {
			if !validResultID(record.AgentID) || !validResultID(record.HostID) || record.StartedAt.IsZero() {
				return nil, errors.New("invalid Agent identity in analysis cleanup ledger")
			}
		} else if record.HostID != "" || !record.StartedAt.IsZero() {
			return nil, errors.New("incomplete Agent identity in analysis cleanup ledger")
		}
		seen[record.Endpoint] = true
	}
	return records, nil
}

func writeAnalysisWorkerCleanup(ctx context.Context, root *os.Root, records []analysisWorkerCleanupRecord) error {
	data, err := json.Marshal(analysisWorkerCleanupLedger{Version: 1, Workers: records})
	if err != nil {
		return err
	}
	if len(data)+1 > resultMetadataLimit {
		return errors.New("analysis worker cleanup ledger exceeds its size limit")
	}
	staged, err := stageAnalysisJSON(ctx, root, json.RawMessage(data))
	if err != nil {
		return err
	}
	defer staged.discard()
	if err := ctx.Err(); err != nil {
		return err
	}
	return staged.publish(analysisWorkerCleanupFile)
}

// Record admission durably before sending any start message. The scheduler
// assigns at most one active job to an endpoint.
func (s *Server) trackAnalysisWorker(ctx context.Context, endpoint string, agents ...model.Agent) error {
	if _, err := remoteAnalysisURL(endpoint); err != nil {
		return err
	}
	record := analysisWorkerCleanupRecord{Endpoint: endpoint}
	if len(agents) > 1 {
		return errors.New("analysis cleanup expects one Agent identity")
	}
	if len(agents) == 1 {
		agent := agents[0]
		record.AgentID, record.HostID, record.StartedAt = agent.ID, agentHostNodeID(agent), agent.StartedAt
		validIdentity := validResultID(record.AgentID) && validResultID(record.HostID) && !record.StartedAt.IsZero() && agent.URL == endpoint
		labelHost := agent.Labels["swarmNodeId"]
		if !validIdentity || labelHost != "" && labelHost != record.HostID {
			return errors.New("cannot record analysis ownership without a verified Agent and host identity")
		}
	}
	s.analysisWorkerCleanupMu.Lock()
	defer s.analysisWorkerCleanupMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer root.Close()
	records, err := readAnalysisWorkerCleanup(root)
	if err != nil {
		return err
	}
	if slices.ContainsFunc(records, func(record analysisWorkerCleanupRecord) bool { return record.Endpoint == endpoint }) {
		return fmt.Errorf("%w: worker %s has an unfinished cleanup record", errRemoteAnalysisCleanupUnconfirmed, endpoint)
	}
	records = append(records, record)
	slices.SortFunc(records, func(a, b analysisWorkerCleanupRecord) int { return strings.Compare(a.Endpoint, b.Endpoint) })
	return writeAnalysisWorkerCleanup(ctx, root, records)
}

// Called after a terminal worker response, an idle proof or a failure before
// start. The scheduler cannot reuse an endpoint while its cleanup is unverified.
func (s *Server) clearAnalysisWorker(endpoint string) error {
	s.analysisWorkerCleanupMu.Lock()
	defer s.analysisWorkerCleanupMu.Unlock()
	root, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer root.Close()
	records, err := readAnalysisWorkerCleanup(root)
	if err != nil {
		return err
	}
	index := slices.IndexFunc(records, func(record analysisWorkerCleanupRecord) bool { return record.Endpoint == endpoint })
	if index < 0 {
		return nil
	}
	if len(records) > 1 {
		return writeAnalysisWorkerCleanup(context.Background(), root, slices.Delete(records, index, index+1))
	}
	if err := root.Remove(analysisWorkerCleanupFile); err != nil {
		return err
	}
	return syncRunDirectory(root)
}

func (s *Server) pendingAnalysisWorkerCleanupRecords() ([]analysisWorkerCleanupRecord, error) {
	s.analysisWorkerCleanupMu.Lock()
	defer s.analysisWorkerCleanupMu.Unlock()
	root, err := os.OpenRoot(s.config.DataDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return readAnalysisWorkerCleanup(root)
}

func (s *Server) pendingAnalysisWorkerCleanup() ([]string, error) {
	records, err := s.pendingAnalysisWorkerCleanupRecords()
	var endpoints []string
	for _, record := range records {
		endpoints = append(endpoints, record.Endpoint)
	}
	return endpoints, err
}

func (s *Server) analysisWorkerCleanupTarget(record analysisWorkerCleanupRecord) (string, string, error) {
	if record.AgentID == "" {
		return record.Endpoint, s.config.AnalysisWorkerToken, nil
	}
	var replacement *model.Agent
	for _, agent := range s.state.agentInventory() {
		labelHost := agent.Labels["swarmNodeId"]
		if !agent.AnalysisProcesses || !agentIsOnline(agent, time.Now()) || agentHostNodeID(agent) != record.HostID || labelHost != "" && labelHost != record.HostID {
			continue
		}
		if agent.ID == record.AgentID && agent.StartedAt.Equal(record.StartedAt) {
			return agent.URL, s.config.AnalysisAgentToken, nil
		}
		if !agent.StartupReconciled || !agent.StartedAt.After(record.StartedAt) {
			continue
		}
		if replacement == nil || agent.StartedAt.After(replacement.StartedAt) {
			current := agent
			replacement = &current
		}
	}
	if replacement != nil {
		return replacement.URL, s.config.AnalysisAgentToken, nil
	}
	return "", "", fmt.Errorf("%w: waiting for Agent %s on host %s to report its analysis state", errRemoteAnalysisCleanupUnconfirmed, record.AgentID, record.HostID)
}

// The queue calls this before opening a new analysis group or admitting the
// next experiment. At this boundary no tracked endpoint can receive a new job.
// Keep probing recorded endpoints even if configuration removed them.
func (s *Server) waitAnalysisWorkerCleanup(ctx context.Context, slot *workSlot) error {
	records, err := s.pendingAnalysisWorkerCleanupRecords()
	if err != nil {
		return fmt.Errorf("read analysis worker cleanup: %w", err)
	}
	if len(records) == 0 {
		return ctx.Err()
	}
	for _, record := range records {
		s.updateWorkStatus(slot.id, "Waiting for analysis worker cleanup", 0)
		reported := false
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			endpoint, token, err := s.analysisWorkerCleanupTarget(record)
			if err == nil {
				if tokenErr := ValidateAnalysisWorkerToken(token); tokenErr != nil {
					return fmt.Errorf("%w: restore the recorded analysis worker credential to verify cleanup", errRemoteAnalysisCleanupUnconfirmed)
				}
				err = probeRemoteAnalysisIdle(ctx, endpoint, token)
			}
			if err == nil {
				break
			} else if !reported && ctx.Err() == nil {
				s.logger.Warn("waiting for analysis worker cleanup", "worker", record.Endpoint, "error", err)
				reported = true
			}
			if err := sleepContext(ctx, 500*time.Millisecond); err != nil {
				return err
			}
		}
		if err := s.clearAnalysisWorker(record.Endpoint); err != nil {
			return fmt.Errorf("clear verified analysis worker cleanup: %w", err)
		}
	}
	s.updateWorkStatus(slot.id, "", 0)
	return ctx.Err()
}

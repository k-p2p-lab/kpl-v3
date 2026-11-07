package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const hostRebootDirectory = "host-reboots"
const hostRebootTargetLimit = 128
const hostRebootQueueLimit = 16

var hostRebootIdentity = regexp.MustCompile(`^[A-Za-z0-9_-]{8,128}$`)

type hostRebootTarget struct {
	AgentID        string    `json:"agentId"`
	NodeID         string    `json:"nodeId"`
	BootID         string    `json:"bootId"`
	AgentStartedAt time.Time `json:"agentStartedAt"`
}

type hostRebootSubmission struct {
	ID      string             `json:"id"`
	Confirm bool               `json:"confirm"`
	Targets []hostRebootTarget `json:"targets"`
}

type hostRebootHostStatus struct {
	AgentID     string     `json:"agentId"`
	NodeID      string     `json:"nodeId"`
	Hostname    string     `json:"hostname"`
	BootID      string     `json:"bootId"`
	State       string     `json:"state"`
	Error       string     `json:"error,omitempty"`
	RequestedAt *time.Time `json:"requestedAt,omitempty"`
}

type hostRebootStatus struct {
	Version     int                    `json:"version"`
	ID          string                 `json:"id"`
	RequestHash string                 `json:"requestHash"`
	State       string                 `json:"state"`
	Targets     []hostRebootTarget     `json:"targets"`
	Hosts       []hostRebootHostStatus `json:"hosts"`
	CreatedAt   time.Time              `json:"createdAt"`
	StartedAt   *time.Time             `json:"startedAt,omitempty"`
	FinishedAt  *time.Time             `json:"finishedAt,omitempty"`
	Error       string                 `json:"error,omitempty"`
}

type hostRebootJob struct {
	status hostRebootStatus // hostRebootMu
	slot   *workSlot
}

func cloneHostRebootStatus(value hostRebootStatus) hostRebootStatus {
	value.Targets, value.Hosts = slices.Clone(value.Targets), slices.Clone(value.Hosts)
	for i := range value.Hosts {
		if value.Hosts[i].RequestedAt != nil {
			v := *value.Hosts[i].RequestedAt
			value.Hosts[i].RequestedAt = &v
		}
	}
	if value.StartedAt != nil {
		v := *value.StartedAt
		value.StartedAt = &v
	}
	if value.FinishedAt != nil {
		v := *value.FinishedAt
		value.FinishedAt = &v
	}
	return value
}

func normalizeHostRebootTargets(values []hostRebootTarget) ([]hostRebootTarget, string, error) {
	if len(values) == 0 || len(values) > hostRebootTargetLimit {
		return nil, "", fmt.Errorf("select between 1 and %d Agent hosts", hostRebootTargetLimit)
	}
	values = slices.Clone(values)
	for i := range values {
		value := &values[i]
		if !validResultID(value.AgentID) || len(value.AgentID) > 200 || !validResultID(value.NodeID) || len(value.NodeID) > 128 || !validResultID(value.BootID) || len(value.BootID) > 128 || value.AgentStartedAt.IsZero() {
			return nil, "", errors.New("each target requires an Agent, Swarm node, boot ID and Agent start time")
		}
		value.AgentStartedAt = value.AgentStartedAt.UTC()
	}
	slices.SortFunc(values, func(a, b hostRebootTarget) int {
		if c := strings.Compare(a.NodeID, b.NodeID); c != 0 {
			return c
		}
		return strings.Compare(a.AgentID, b.AgentID)
	})
	unique := make([]hostRebootTarget, 0, len(values))
	for _, value := range values {
		if len(unique) > 0 && unique[len(unique)-1].NodeID == value.NodeID {
			previous := unique[len(unique)-1]
			if previous.BootID != value.BootID || previous.AgentID == value.AgentID && !previous.AgentStartedAt.Equal(value.AgentStartedAt) {
				return nil, "", errors.New("the same host has conflicting boot or Agent identities; refresh the Agent list")
			}
			continue
		}
		unique = append(unique, value)
	}
	data, err := json.Marshal(unique)
	if err != nil {
		return nil, "", err
	}
	hash := sha256.Sum256(data)
	return unique, hex.EncodeToString(hash[:]), nil
}

func (s *Server) hostRebootRoot(create bool) (*os.Root, error) {
	if create {
		if err := os.MkdirAll(s.config.DataDir, 0700); err != nil {
			return nil, err
		}
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return nil, err
	}
	defer data.Close()
	if create {
		if err := data.Mkdir(hostRebootDirectory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		if err := syncRunDirectory(data); err != nil {
			return nil, err
		}
	}
	return openResultDirectory(data, hostRebootDirectory)
}

func (s *Server) persistHostReboot(status hostRebootStatus) error {
	root, err := s.hostRebootRoot(true)
	if err != nil {
		return err
	}
	defer root.Close()
	return writeAnalysisJSON(root, status.ID+".json", status)
}

// Caller holds hostRebootMu. Interrupted operations are observable, never replayed.
func (s *Server) loadHostRebootLocked(id string) (*hostRebootJob, error) {
	if job := s.hostReboots[id]; job != nil {
		return job, nil
	}
	root, err := s.hostRebootRoot(false)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	data, err := readResumeFile(root, id+".json", resultMetadataLimit)
	if err != nil {
		return nil, err
	}
	var status hostRebootStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return nil, err
	}
	_, hash, err := normalizeHostRebootTargets(status.Targets)
	if err != nil || status.Version != 1 || status.ID != id || status.RequestHash != hash || len(status.Hosts) != len(status.Targets) {
		return nil, errors.New("invalid stored host reboot operation")
	}
	for i, host := range status.Hosts {
		target := status.Targets[i]
		if host.AgentID != target.AgentID || host.NodeID != target.NodeID || host.BootID != target.BootID {
			return nil, errors.New("stored host reboot target does not match its outcome")
		}
		switch host.State {
		case "queued", "checking", "requested", "rebooted", "unknown", "rejected":
		default:
			return nil, errors.New("invalid stored host reboot outcome")
		}
	}
	switch status.State {
	case "queued", "running":
		status.State, status.Error = "interrupted", "Controller restarted. No reboot request will be replayed; inspect the hosts before submitting another operation."
		for i := range status.Hosts {
			if status.Hosts[i].State == "checking" {
				status.Hosts[i].State, status.Hosts[i].Error = "unknown", "Controller restarted before the Agent response was recorded."
			} else if status.Hosts[i].State == "queued" {
				status.Hosts[i].State, status.Hosts[i].Error = "rejected", "Controller restarted before this reboot request was sent."
			}
		}
	case "completed", "failed", "interrupted":
	default:
		return nil, errors.New("invalid stored host reboot state")
	}
	job := &hostRebootJob{status: status}
	if s.hostReboots == nil {
		s.hostReboots = make(map[string]*hostRebootJob)
	}
	s.cacheHostRebootLocked(job)
	return job, nil
}

func (s *Server) cacheHostRebootLocked(job *hostRebootJob) {
	if s.hostReboots == nil {
		s.hostReboots = make(map[string]*hostRebootJob)
	}
	if len(s.hostReboots) >= 256 {
		oldest := ""
		for id, candidate := range s.hostReboots {
			if id != job.status.ID && candidate.status.State != "queued" && candidate.status.State != "running" && (oldest == "" || candidate.status.CreatedAt.Before(s.hostReboots[oldest].status.CreatedAt)) {
				oldest = id
			}
		}
		delete(s.hostReboots, oldest)
	}
	s.hostReboots[job.status.ID] = job
}

func (s *Server) currentRebootAgent(target hostRebootTarget) (model.Agent, error) {
	s.state.mu.RLock()
	agent, exists := s.state.agents[target.AgentID]
	fence, fenced := s.state.hostRebootFences[target.NodeID]
	s.state.mu.RUnlock()
	if !exists {
		return model.Agent{}, errors.New("Agent is no longer registered")
	}
	if !agentIsOnline(agent, time.Now()) {
		return model.Agent{}, errors.New("Agent is offline; refresh the Agent list")
	}
	if fenced && !hostHasNewBoot(agent, target.NodeID, fence, time.Now()) {
		return model.Agent{}, errors.New("a previous reboot is unresolved on this host boot")
	}
	host := agent.Host
	if host == nil || !host.RebootEnabled || !host.RebootSupported {
		return model.Agent{}, errors.New("host reboot is not enabled or supported by this Agent")
	}
	if host.RebootPending || host.NodeID != target.NodeID || host.BootID != target.BootID || !agent.StartedAt.Equal(target.AgentStartedAt) || agent.Labels["swarmNodeId"] != "" && agent.Labels["swarmNodeId"] != host.NodeID {
		return model.Agent{}, errors.New("host or Agent identity changed, or a reboot is already pending; refresh the Agent list")
	}
	return agent, nil
}

func writeHostRebootError(w http.ResponseWriter, status int, err error, accepted bool) {
	writeJSON(w, status, map[string]any{"error": err.Error(), "accepted": accepted})
}

func (s *Server) handleHostRebootSubmission(ctx context.Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		var input hostRebootSubmission
		if err := decodeJSON(w, r, &input); err != nil {
			writeHostRebootError(w, http.StatusBadRequest, err, false)
			return
		}
		if !input.Confirm || !hostRebootIdentity.MatchString(input.ID) {
			writeHostRebootError(w, http.StatusBadRequest, errors.New("confirm: true and an immutable operation id of 8–128 ASCII letters, digits, underscores or hyphens are required"), false)
			return
		}
		targets, hash, err := normalizeHostRebootTargets(input.Targets)
		if err != nil {
			writeHostRebootError(w, http.StatusBadRequest, err, false)
			return
		}
		s.cancelMu.Lock()
		defer s.cancelMu.Unlock()
		s.hostRebootMu.Lock()
		defer s.hostRebootMu.Unlock()
		existing, err := s.loadHostRebootLocked(input.ID)
		if err == nil {
			if existing.status.RequestHash != hash {
				writeHostRebootError(w, http.StatusConflict, errors.New("operation ID already belongs to a different host selection"), true)
				return
			}
			code := http.StatusOK
			if existing.status.State == "queued" || existing.status.State == "running" {
				code = http.StatusAccepted
			}
			writeJSON(w, code, s.hostRebootEvidence(cloneHostRebootStatus(existing.status)))
			return
		}
		if !errors.Is(err, os.ErrNotExist) {
			writeHostRebootError(w, http.StatusInternalServerError, err, true)
			return
		}
		if s.shuttingDown || ctx.Err() != nil {
			writeHostRebootError(w, http.StatusServiceUnavailable, errors.New("Controller is shutting down"), false)
			return
		}
		active := 0
		for _, job := range s.hostReboots {
			if job.status.State == "queued" || job.status.State == "running" {
				active++
			}
		}
		if active >= hostRebootQueueLimit {
			writeHostRebootError(w, http.StatusServiceUnavailable, errors.New("host reboot queue is full"), false)
			return
		}
		status := hostRebootStatus{Version: 1, ID: input.ID, RequestHash: hash, State: "queued", Targets: targets, Hosts: make([]hostRebootHostStatus, 0, len(targets)), CreatedAt: time.Now().UTC()}
		for _, target := range targets {
			agent, err := s.currentRebootAgent(target)
			if err != nil {
				writeHostRebootError(w, http.StatusConflict, fmt.Errorf("host %s: %w", target.NodeID, err), false)
				return
			}
			status.Hosts = append(status.Hosts, hostRebootHostStatus{AgentID: target.AgentID, NodeID: target.NodeID, Hostname: agent.Host.Hostname, BootID: target.BootID, State: "queued"})
		}
		if err := s.persistHostReboot(status); err != nil {
			writeHostRebootError(w, http.StatusInternalServerError, err, true)
			return
		}
		workerCtx, cancel := context.WithCancel(ctx)
		job := &hostRebootJob{status: status}
		job.slot = s.enqueueWorkLocked(workerCtx, model.WorkQueueItem{Kind: "host-reboot", Name: fmt.Sprintf("Reboot %d Linux host(s)", len(targets)), OperationID: status.ID}, cancel)
		s.cacheHostRebootLocked(job)
		go s.runHostReboot(workerCtx, job)
		writeJSON(w, http.StatusAccepted, cloneHostRebootStatus(status))
	}
}

func (s *Server) handleHostRebootStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("operationID")
	if !hostRebootIdentity.MatchString(id) {
		http.NotFound(w, r)
		return
	}
	s.hostRebootMu.Lock()
	job, err := s.loadHostRebootLocked(id)
	var status hostRebootStatus
	if err == nil {
		status = cloneHostRebootStatus(job.status)
	}
	s.hostRebootMu.Unlock()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.NotFound(w, r)
		} else {
			writeError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, s.hostRebootEvidence(status))
}

func (s *Server) hostRebootEvidence(status hostRebootStatus) hostRebootStatus {
	// Ordinary Agent heartbeats use the service key also available to Peers.
	// Only the worker's nonce-bound host proof can publish a completed reboot.
	return status
}

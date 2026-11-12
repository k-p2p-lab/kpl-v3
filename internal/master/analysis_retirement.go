package master

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/controller"
)

type analysisRetirementRecord struct {
	Endpoint           string    `json:"endpoint"`
	ServiceID          string    `json:"serviceId,omitempty"`
	TaskIDs            []string  `json:"taskIds,omitempty"`
	VerifiedAt         time.Time `json:"verifiedAt,omitzero"`
	SourceControllerID string    `json:"sourceControllerId,omitempty"`
	ObservedAt         time.Time `json:"observedAt,omitzero"`
}

type analysisRetirementJournal struct {
	Version       int                        `json:"version"`
	Stack         string                     `json:"stack"`
	ControlNodeID string                     `json:"controlNodeId"`
	ClusterID     string                     `json:"clusterId"`
	Services      []analysisRetirementRecord `json:"services"`
}

func (s *swarm) analysisRetirementPath() string {
	return s.a.configPath() + ".analysis-retirement.json"
}

func (s *swarm) analysisRetirementCluster(ctx context.Context) (string, error) {
	if s.retirementClusterID != "" {
		return s.retirementClusterID, nil
	}
	cluster, err := s.output(ctx, "info", "--format", "{{.Swarm.Cluster.ID}}")
	if err != nil {
		return "", fmt.Errorf("inspect analysis retirement cluster: %w", err)
	}
	if !swarmID.MatchString(cluster) {
		return "", errors.New("cannot verify Swarm cluster identity for analysis retirement")
	}
	s.retirementClusterID = cluster
	return cluster, nil
}

func (s *swarm) readAnalysisRetirementJournal(ctx context.Context) (analysisRetirementJournal, []byte, os.FileInfo, error) {
	var journal analysisRetirementJournal
	path := s.analysisRetirementPath()
	before, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return journal, nil, nil, nil
	}
	if err != nil {
		return journal, nil, nil, fmt.Errorf("inspect analysis retirement journal: %w", err)
	}
	if !before.Mode().IsRegular() || before.Mode().Perm()&0022 != 0 {
		return journal, nil, nil, errors.New("analysis retirement journal must be a regular file without group or public write permissions")
	}
	data, info, err := readConfigBytes(path)
	if err != nil {
		return journal, nil, nil, fmt.Errorf("read analysis retirement journal: %w", err)
	}
	if !os.SameFile(before, info) {
		return journal, nil, nil, errors.New("analysis retirement journal changed while opening")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&journal); err != nil {
		return journal, nil, nil, fmt.Errorf("decode analysis retirement journal: %w", err)
	}
	var tail any
	if err := decoder.Decode(&tail); !errors.Is(err, io.EOF) {
		return journal, nil, nil, errors.New("analysis retirement journal has trailing data")
	}
	if journal.Version != 1 || journal.Stack != s.stack || journal.ControlNodeID != s.config["KPL_CONTROL_NODE_ID"] {
		return journal, nil, nil, errors.New("analysis retirement journal does not match this stack and control node")
	}
	cluster, err := s.analysisRetirementCluster(ctx)
	if err != nil {
		return journal, nil, nil, err
	}
	if journal.ClusterID != cluster {
		return journal, nil, nil, errors.New("analysis retirement journal belongs to a different Swarm cluster")
	}
	if len(journal.Services) == 0 {
		return journal, nil, nil, errors.New("analysis retirement journal has no verified service records")
	}
	if err := validateAnalysisRetirementRecords(journal.Services); err != nil {
		return journal, nil, nil, err
	}
	return journal, data, info, nil
}

func validateAnalysisRetirementRecords(records []analysisRetirementRecord) error {
	endpoints := make([]string, 0, len(records))
	for _, record := range records {
		endpoints = append(endpoints, record.Endpoint)
		taskProof := record.ServiceID != ""
		controllerProof := record.SourceControllerID != ""
		if taskProof == controllerProof {
			return errors.New("analysis retirement journal must identify exactly one proof source")
		}
		if taskProof {
			if !swarmID.MatchString(record.ServiceID) || record.VerifiedAt.IsZero() || !record.ObservedAt.IsZero() {
				return errors.New("analysis retirement journal contains incomplete shutdown evidence")
			}
			seen := map[string]bool{}
			for _, task := range record.TaskIDs {
				if !swarmID.MatchString(task) || seen[task] {
					return errors.New("analysis retirement journal contains invalid task evidence")
				}
				seen[task] = true
			}
		} else if !swarmID.MatchString(record.SourceControllerID) || record.ObservedAt.IsZero() || !record.VerifiedAt.IsZero() || len(record.TaskIDs) != 0 {
			return errors.New("analysis retirement journal contains invalid Controller proof provenance")
		}
	}
	if err := controller.ValidateRetiredAnalysisEndpoints(endpoints); err != nil {
		return fmt.Errorf("invalid analysis retirement journal: %w", err)
	}
	return nil
}

// A durable verified-stop record survives rm -> publish/deploy failure. A
// currently present legacy service is still stopped and verified again before
// any deployment can pass these endpoints to the new Controller.
func (s *swarm) loadAnalysisRetirementJournal(ctx context.Context) error {
	journal, _, _, err := s.readAnalysisRetirementJournal(ctx)
	if err != nil {
		return err
	}
	for _, service := range journal.Services {
		s.rememberRetiredAnalysisEndpoint(service.Endpoint)
	}
	return nil
}

func (s *swarm) persistAnalysisRetirements(ctx context.Context, records []analysisRetirementRecord) error {
	if len(records) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path := s.analysisRetirementPath()
	budget := s.timeout
	if budget <= 0 {
		budget = time.Minute
	}
	lockCtx, cancelLock := context.WithTimeout(ctx, budget)
	defer cancelLock()
	lock, err := lockAnalysisRetirementJournal(lockCtx, path+".lock")
	if err != nil {
		return fmt.Errorf("lock analysis retirement journal: %w", err)
	}
	defer lock.Close()
	journal, original, info, err := s.readAnalysisRetirementJournal(ctx)
	if err != nil {
		return err
	}
	if info == nil {
		cluster, err := s.analysisRetirementCluster(ctx)
		if err != nil {
			return err
		}
		journal = analysisRetirementJournal{Version: 1, Stack: s.stack, ControlNodeID: s.config["KPL_CONTROL_NODE_ID"], ClusterID: cluster}
	}
	byEndpoint := map[string]analysisRetirementRecord{}
	for _, record := range journal.Services {
		byEndpoint[record.Endpoint] = record
	}
	if err := validateAnalysisRetirementRecords(records); err != nil {
		return err
	}
	for _, record := range records {
		// Imported Controller provenance must not weaken an existing exact
		// verified-task record for the same retired endpoint.
		if previous, ok := byEndpoint[record.Endpoint]; ok && previous.ServiceID != "" && record.SourceControllerID != "" {
			continue
		}
		byEndpoint[record.Endpoint] = record
	}
	journal.Services = journal.Services[:0]
	endpoints := make([]string, 0, len(byEndpoint))
	for endpoint, record := range byEndpoint {
		endpoints = append(endpoints, endpoint)
		journal.Services = append(journal.Services, record)
	}
	if err := controller.ValidateRetiredAnalysisEndpoints(endpoints); err != nil {
		return err
	}
	sort.Slice(journal.Services, func(i, j int) bool { return journal.Services[i].Endpoint < journal.Services[j].Endpoint })
	data, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := ctx.Err(); err != nil {
		return err
	}
	// Reuse the private atomic publisher: sync staging file, atomically link
	// or replace the destination, then sync its parent directory.
	if err := saveConfig(path, data, original, info, info == nil); err != nil {
		return fmt.Errorf("persist analysis retirement journal before service removal: %w", err)
	}
	for _, record := range journal.Services {
		s.rememberRetiredAnalysisEndpoint(record.Endpoint)
	}
	return nil
}

func (s *swarm) recordAnalysisRetirement(ctx context.Context, worker legacyAnalysisService, tasks []string) error {
	component := strings.TrimPrefix(worker.name, s.stack+"_")
	if !legacyAnalysisWorker(component) {
		return errors.New("cannot retire an unrecognized analysis service")
	}
	return s.persistAnalysisRetirements(ctx, []analysisRetirementRecord{{Endpoint: "http://" + component + ":8093", ServiceID: worker.id, TaskIDs: append([]string{}, tasks...), VerifiedAt: time.Now().UTC()}})
}

func (s *swarm) journalLegacyAnalysisServices(ctx context.Context) error {
	var records []analysisRetirementRecord
	for _, worker := range s.legacyAnalysisServices() {
		tasks, err := s.captureTasks(ctx, worker.id, "")
		if err != nil {
			return err
		}
		if err := s.waitTasks(ctx, tasks, s.agentTimeout); err != nil {
			return err
		}
		component := strings.TrimPrefix(worker.name, s.stack+"_")
		records = append(records, analysisRetirementRecord{Endpoint: "http://" + component + ":8093", ServiceID: worker.id, TaskIDs: append([]string{}, tasks...), VerifiedAt: time.Now().UTC()})
	}
	return s.persistAnalysisRetirements(ctx, records)
}

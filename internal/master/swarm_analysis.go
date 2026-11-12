package master

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/controller"
)

// Recognize only previously deployed, owned services during migration. There
// is no role, Compose definition or configurable deployment for these names.
func legacyAnalysisWorker(component string) bool {
	number, ok := strings.CutPrefix(component, "analysis-worker-")
	if !ok {
		return false
	}
	index, err := strconv.Atoi(number)
	return err == nil && index >= 1 && index <= 16 && strconv.Itoa(index) == number
}

type legacyAnalysisService struct{ name, id string }

func (s *swarm) legacyAnalysisServices() []legacyAnalysisService {
	var workers []legacyAnalysisService
	for i, name := range s.services {
		if legacyAnalysisWorker(strings.TrimPrefix(name, s.stack+"_")) {
			workers = append(workers, legacyAnalysisService{name: name, id: s.serviceIDs[i]})
		}
	}
	return workers
}

const retiredAnalysisEnvironment = "KPL_RETIRED_ANALYSIS_ENDPOINTS"
const retiredAnalysisEnvironmentFormat = `{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{if eq (index (split . "=") 0) "KPL_RETIRED_ANALYSIS_ENDPOINTS"}}{{println .}}{{end}}{{end}}`

// Read only the non-secret retirement proof from an ownership-verified
// Controller service. Keeping it in the desired service environment preserves
// it across a failed Controller startup and the next deployment attempt.
func (s *swarm) loadRetiredAnalysisProof(ctx context.Context) error {
	if !s.present["controller"] {
		return nil
	}
	var serviceID string
	for index, name := range s.services {
		if name == s.stack+"_controller" {
			serviceID = s.serviceIDs[index]
			break
		}
	}
	if serviceID == "" {
		return fmt.Errorf("Controller service ownership is unavailable")
	}
	value, err := s.output(ctx, "service", "inspect", "--format", retiredAnalysisEnvironmentFormat, serviceID)
	if err != nil {
		return fmt.Errorf("inspect retired analysis proof: %w", err)
	}
	var endpoints []string
	for _, line := range strings.Split(value, "\n") {
		if line == "" {
			continue
		}
		proof, ok := strings.CutPrefix(line, retiredAnalysisEnvironment+"=")
		if !ok {
			return fmt.Errorf("invalid retired analysis proof metadata")
		}
		endpoints = append(endpoints, strings.Fields(proof)...)
	}
	if err := controller.ValidateRetiredAnalysisEndpoints(endpoints); err != nil {
		return err
	}
	var records []analysisRetirementRecord
	for _, endpoint := range endpoints {
		records = append(records, analysisRetirementRecord{Endpoint: endpoint, SourceControllerID: serviceID, ObservedAt: time.Now().UTC()})
	}
	// Preserve already accepted, ownership-verified Controller provenance
	// before a restart/republish can destroy its desired service environment.
	return s.persistAnalysisRetirements(ctx, records)
}

func (s *swarm) rememberRetiredAnalysisEndpoint(endpoint string) {
	for _, existing := range s.retiredAnalysisEndpoints {
		if existing == endpoint {
			return
		}
	}
	s.retiredAnalysisEndpoints = append(s.retiredAnalysisEndpoints, endpoint)
}

func (s *swarm) withRetiredAnalysisProof() {
	proof := strings.Join(s.retiredAnalysisEndpoints, " ")
	s.a = s.a.WithEnv(map[string]*string{retiredAnalysisEnvironment: &proof})
}

// The old Controller can still own remote calculations. Stop admission and
// verify its cleanup before stopping/removing the retired service endpoints.
func (s *swarm) stopLegacyAnalysisServices(ctx context.Context) error {
	workers := s.legacyAnalysisServices()
	if len(workers) == 0 {
		return nil
	}
	for _, worker := range workers {
		if _, err := s.captureTasks(ctx, worker.id, ""); err != nil {
			return err
		}
	}
	if s.present["controller"] {
		if err := s.stopService(ctx, s.stack+"_controller", s.controllerTimeout); err != nil {
			return err
		}
	}
	for _, worker := range workers {
		if err := s.stopService(ctx, worker.id, s.agentTimeout); err != nil {
			return err
		}
	}
	return nil
}

func (s *swarm) removeLegacyAnalysisServices(ctx context.Context) error {
	for _, worker := range s.legacyAnalysisServices() {
		// Recheck task evidence at the removal boundary; a successful earlier
		// stop is not a substitute for observing the final owned tasks.
		tasks, err := s.captureTasks(ctx, worker.id, "")
		if err != nil {
			return err
		}
		if err := s.waitTasks(ctx, tasks, s.agentTimeout); err != nil {
			return err
		}
		if err := s.recordAnalysisRetirement(ctx, worker, tasks); err != nil {
			return err
		}
		if err := s.run(ctx, "service", "rm", worker.id); err != nil {
			return fmt.Errorf("remove retired analysis service: %w", err)
		}
		// The name came from ownership-checked inventory and all tasks were
		// verified stopped before removal. Absence alone is never proof.
		component := strings.TrimPrefix(worker.name, s.stack+"_")
		s.rememberRetiredAnalysisEndpoint("http://" + component + ":8093")
	}
	return nil
}

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type attemptRecord struct {
	State     string `json:"state"`
	Started   bool   `json:"started"`
	Retry     bool   `json:"retry"`
	Iteration int    `json:"iteration"`
}
type environmentGroup struct {
	Fingerprint       string        `json:"fingerprint"`
	RunIDs            []string      `json:"runIds"`
	Agents            []model.Agent `json:"agents"`
	ControllerVersion string        `json:"controllerVersion,omitempty"`
}
type batchReliability struct {
	Attempts           int                `json:"attempts"`
	Failed             int                `json:"failed"`
	Canceled           int                `json:"canceled"`
	Interrupted        int                `json:"interrupted"`
	Retries            int                `json:"retries"`
	FailedAttemptRate  float64            `json:"failedAttemptRate"`
	UnknownAttempts    int                `json:"unknownAttempts"`
	UnverifiedRuns     int                `json:"unverifiedRuns"`
	IncompleteRuns     int                `json:"incompleteRuns"`
	Environments       []environmentGroup `json:"environments"`
	EnvironmentChanged bool               `json:"environmentChanged"`
	Warnings           []string           `json:"warnings"`
}

func attemptOf(run savedResult) attemptRecord {
	return attemptRecord{State: run.State, Started: !run.StartedAt.IsZero(), Retry: len(run.PreviousRunIDs) > 0, Iteration: run.Iteration}
}
func (s *Server) batchReliability(ctx context.Context, id string, analyses []resultAnalysis) (batchReliability, error) {
	result := batchReliability{Environments: []environmentGroup{}, Warnings: []string{}}
	members, err := s.allBatchMembers(ctx, id)
	if err != nil {
		return result, err
	}
	record, err := s.readBatchExtension(id)
	if err != nil {
		return result, err
	}
	attempts := map[string]attemptRecord{}
	if record != nil {
		for id, attempt := range record.Deleted {
			attempts[id] = attempt
		}
	}
	for _, run := range members {
		attempts[run.ID] = attemptOf(run)
	}
	if record != nil {
		for id := range record.Retired {
			if _, ok := attempts[id]; !ok {
				result.UnknownAttempts++
			}
		}
	}
	for _, attempt := range attempts {
		if !attempt.Started {
			continue
		}
		result.Attempts++
		if attempt.Retry {
			result.Retries++
		}
		switch attempt.State {
		case "failed":
			result.Failed++
		case "canceled":
			result.Canceled++
		case "interrupted":
			result.Interrupted++
		}
	}
	if result.Attempts > 0 {
		result.FailedAttemptRate = float64(result.Failed) / float64(result.Attempts)
	}
	groups := map[string]*environmentGroup{}
	for _, analysis := range analyses {
		run := analysis.Result
		if run.DataState != "complete" {
			result.UnverifiedRuns++
		}
		if run.DataState == "incomplete" || analysis.Metrics.MeasurementIncomplete {
			result.IncompleteRuns++
		}
		agents := append([]model.Agent{}, run.Agents...)
		for i := range agents {
			agents[i].LastSeen = agents[i].StartedAt
			agents[i].ActiveNodes = 0
			agents[i].State = ""
			agents[i].CapacityRevision = ""
			agents[i].CapacityPending = false
		}
		sort.Slice(agents, func(i, j int) bool {
			if agents[i].ID != agents[j].ID {
				return agents[i].ID < agents[j].ID
			}
			return agents[i].StartedAt.Before(agents[j].StartedAt)
		})
		data, _ := json.Marshal(struct {
			Controller string
			Agents     []model.Agent
		}{run.ControllerVersion, agents})
		sum := sha256.Sum256(data)
		fingerprint := hex.EncodeToString(sum[:])
		group := groups[fingerprint]
		if group == nil {
			group = &environmentGroup{Fingerprint: fingerprint, Agents: agents, ControllerVersion: run.ControllerVersion}
			groups[fingerprint] = group
		}
		group.RunIDs = append(group.RunIDs, run.ID)
	}
	for _, group := range groups {
		sort.Strings(group.RunIDs)
		result.Environments = append(result.Environments, *group)
	}
	sort.Slice(result.Environments, func(i, j int) bool { return result.Environments[i].Fingerprint < result.Environments[j].Fingerprint })
	result.EnvironmentChanged = len(result.Environments) > 1
	if result.Failed+result.Canceled+result.Interrupted > 0 {
		result.Warnings = append(result.Warnings, "Means describe completed runs only; failed, canceled and interrupted attempts are reported separately.")
	}
	if result.EnvironmentChanged {
		result.Warnings = append(result.Warnings, "Recorded execution environments differ across runs. Compare the environment groups before interpreting the combined mean.")
	}
	if result.UnverifiedRuns > 0 || result.IncompleteRuns > 0 {
		result.Warnings = append(result.Warnings, "Some runs lack verified complete telemetry. Metric sample counts and missing evidence must be considered.")
	}
	if result.UnknownAttempts > 0 {
		result.Warnings = append(result.Warnings, "Some historical attempts were deleted before attempt accounting was introduced; their outcomes are unknown.")
	}
	return result, nil
}

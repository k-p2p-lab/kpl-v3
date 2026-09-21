package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

const batchAnalysisVersion = 1
const batchJobFile = "job.json"
const batchResultFile = "result.json"

type batchAnalysisStatus struct {
	analysisJobStatus
	BatchID       string   `json:"batchId"`
	Membership    string   `json:"membership"`
	RunIDs        []string `json:"runIds"`
	CompletedRuns int      `json:"completedRuns"`
	TotalRuns     int      `json:"totalRuns"`
	ExpectedRuns  int      `json:"expectedRuns"`
}
type batchAnalysisJob struct {
	status batchAnalysisStatus
	cancel context.CancelFunc
}
type batchAnalysisResult struct {
	Version         int                          `json:"version"`
	AnalysisVersion int                          `json:"analysisVersion"`
	AnalysisID      string                       `json:"analysisId"`
	BatchID         string                       `json:"batchId"`
	Name            string                       `json:"name"`
	AsOf            time.Time                    `json:"asOf"`
	Aggregation     string                       `json:"aggregation"`
	ExpectedRuns    int                          `json:"expectedRuns"`
	MissingRuns     int                          `json:"missingRuns"`
	Excluded        []savedResult                `json:"excluded"`
	Summary         map[string]analysisStatistic `json:"summary"`
	Runs            []resultAnalysis             `json:"runs"`
}

// Batch artifacts live independently of the first repetition's result directory.
// Callers hold analysisJobMu; os.Root rejects symlinked directory aliases.
func (s *Server) batchAnalysisDirectory(id string, create bool) (*os.Root, error) {
	if !validResultID(id) {
		return nil, errResultNotFound
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return nil, err
	}
	defer data.Close()
	if create {
		if err = data.Mkdir("batch-analyses", 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	parent, err := openResultDirectory(data, "batch-analyses")
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	if create {
		if err = parent.Mkdir(id, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	return openResultDirectory(parent, id)
}
func (s *Server) persistBatchAnalysis(status batchAnalysisStatus) error {
	root, err := s.batchAnalysisDirectory(status.BatchID, true)
	if err != nil {
		return err
	}
	defer root.Close()
	return writeAnalysisJSON(root, batchJobFile, status)
}
func (s *Server) loadBatchAnalysis(id string) (*batchAnalysisJob, error) {
	if job := s.batchAnalysisJobs[id]; job != nil {
		return job, nil
	}
	idle := &batchAnalysisJob{status: batchAnalysisStatus{analysisJobStatus: analysisJobStatus{Version: 1, State: "idle"}, BatchID: id}}
	root, err := s.batchAnalysisDirectory(id, false)
	if errors.Is(err, os.ErrNotExist) {
		return idle, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	file, err := openResultFile(root, batchJobFile)
	if errors.Is(err, os.ErrNotExist) {
		return idle, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.file.Close()
	if file.size > resultMetadataLimit {
		return nil, errors.New("batch analysis metadata is too large")
	}
	var status batchAnalysisStatus
	if err = json.NewDecoder(io.NewSectionReader(file.file, 0, file.size)).Decode(&status); err != nil {
		return nil, err
	}
	if status.Version != batchAnalysisVersion || status.BatchID != id || !validResultID(status.ID) {
		return nil, errors.New("invalid batch analysis metadata")
	}
	switch status.State {
	case "queued", "running":
		status.State, status.Error = "interrupted", "Controller restarted before batch analysis completed. Retry to start again."
	case "completed", "failed", "interrupted":
	default:
		return nil, errors.New("invalid batch analysis state")
	}
	job := &batchAnalysisJob{status: status}
	s.batchAnalysisJobs[id] = job
	return job, nil
}

// Prior attempts remain downloadable, but only the latest attempt of each
// iteration participates in continuation and batch statistics.
func currentBatchMembers(members []savedResult) []savedResult {
	previous := make(map[string]bool)
	for _, member := range members {
		for _, id := range member.PreviousRunIDs {
			if id != member.ID {
				previous[id] = true
			}
		}
	}
	current := make([]savedResult, 0, len(members))
	for _, member := range members {
		if !previous[member.ID] {
			current = append(current, member)
		}
	}
	return current
}

func (s *Server) batchMembers(ctx context.Context, id string) ([]savedResult, error) {
	members, err := s.allBatchMembers(ctx, id)
	return currentBatchMembers(members), err
}

func (s *Server) allBatchMembers(ctx context.Context, id string) ([]savedResult, error) {
	if !validResultID(id) {
		return nil, errResultNotFound
	}
	root, err := s.openResultRuns()
	if errors.Is(err, os.ErrNotExist) {
		return nil, errResultNotFound
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	members := []savedResult{}
	for {
		entries, readErr := dir.ReadDir(100)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if !entry.IsDir() || !validResultID(entry.Name()) {
				continue
			}
			member, err := s.readSavedResult(root, entry.Name())
			if err == nil && member.BatchID == id {
				members = append(members, member)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	if len(members) == 0 {
		return nil, errResultNotFound
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	return members, nil
}
func batchMembership(members []savedResult) string {
	members = currentBatchMembers(members)
	// Ignore display-only download sizes and job statuses, and normalize order.
	canonical := make([]savedResult, 0, len(members))
	for _, m := range members {
		canonical = append(canonical, savedResult{ID: m.ID, Name: m.Name, State: m.State, Active: m.Active, BatchID: m.BatchID, Iteration: m.Iteration, Repetitions: m.Repetitions, StartedAt: m.StartedAt, FinishedAt: m.FinishedAt})
	}
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].ID < canonical[j].ID })
	data, _ := json.Marshal(canonical)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func (s *Server) batchAnalysisStatus(id string) (batchAnalysisStatus, error) {
	s.analysisJobMu.Lock()
	defer s.analysisJobMu.Unlock()
	job, err := s.loadBatchAnalysis(id)
	if err != nil {
		return batchAnalysisStatus{}, err
	}
	return job.status, nil
}
func (s *Server) startBatchAnalysis(ctx context.Context, id string, refresh bool) (batchAnalysisStatus, error) {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if s.shuttingDown || ctx.Err() != nil {
		return batchAnalysisStatus{}, errors.New("Controller is shutting down")
	}
	members, err := s.batchMembers(ctx, id)
	if err != nil {
		return batchAnalysisStatus{}, err
	}
	selected, excluded := []savedResult{}, []savedResult{}
	expected, iterations := 0, map[int]bool{}
	for _, m := range members {
		if m.Active || m.State == "running" || m.State == "queued" || s.cancels[m.ID] != nil || s.repeatBatches[m.ID] != nil {
			return batchAnalysisStatus{}, errResultBusy
		}
		if m.Repetitions < 2 || m.Repetitions > maxScenarioRepetitions || m.Iteration < 1 || m.Iteration > m.Repetitions || iterations[m.Iteration] || expected != 0 && expected != m.Repetitions {
			return batchAnalysisStatus{}, errors.New("inconsistent repeated-run metadata")
		}
		expected = m.Repetitions
		iterations[m.Iteration] = true
		if m.State == "completed" {
			selected = append(selected, m)
		} else {
			excluded = append(excluded, m)
		}
	}
	if len(selected) < 2 {
		return batchAnalysisStatus{}, errors.New("batch mean requires at least two completed runs")
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Iteration < selected[j].Iteration })
	s.analysisJobMu.Lock()
	defer s.analysisJobMu.Unlock()
	existing, err := s.loadBatchAnalysis(id)
	if err != nil {
		return batchAnalysisStatus{}, err
	}
	membership := batchMembership(members)
	if existing.status.State == "queued" || existing.status.State == "running" || existing.status.State == "completed" && !refresh && existing.status.Membership == membership && existing.status.AnalysisVersion == currentAnalysisVersion {
		return existing.status, nil
	}
	count := 0
	for _, job := range s.batchAnalysisJobs {
		if job.status.State == "queued" || job.status.State == "running" {
			count++
		}
	}
	for _, job := range s.analysisJobs {
		if job.status.State == "queued" || job.status.State == "running" {
			count++
		}
	}
	if count >= analysisJobLimit {
		return batchAnalysisStatus{}, errAnalysisQueueFull
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return batchAnalysisStatus{}, err
	}
	now := time.Now().UTC()
	status := batchAnalysisStatus{analysisJobStatus: analysisJobStatus{Version: batchAnalysisVersion, AnalysisVersion: currentAnalysisVersion, ID: hex.EncodeToString(nonce[:]), State: "queued", Phase: "queued", CreatedAt: now, UpdatedAt: now}, BatchID: id, Membership: membership, TotalRuns: len(selected), ExpectedRuns: expected, RunIDs: []string{}}
	for _, run := range selected {
		status.RunIDs = append(status.RunIDs, run.ID)
	}
	if err := s.persistBatchAnalysis(status); err != nil {
		return batchAnalysisStatus{}, err
	}
	work, cancel := context.WithCancel(ctx)
	job := &batchAnalysisJob{status: status, cancel: cancel}
	s.batchAnalysisJobs[id] = job
	s.analysisWorkers.Add(1)
	go s.runBatchAnalysis(work, job, selected, excluded, max(0, expected-len(members)))
	return status, nil
}
func (s *Server) runBatchAnalysis(ctx context.Context, job *batchAnalysisJob, selected, excluded []savedResult, missing int) {
	defer s.analysisWorkers.Done()
	result := batchAnalysisResult{Version: batchAnalysisVersion, AnalysisVersion: currentAnalysisVersion, AnalysisID: job.status.ID, BatchID: job.status.BatchID, Name: selected[0].Name, AsOf: time.Now().UTC(), Aggregation: "equal-run-mean-v1", ExpectedRuns: job.status.ExpectedRuns, MissingRuns: missing, Excluded: excluded, Runs: []resultAnalysis{}}
	err := func() error {
		for i, member := range selected {
			err := func() error {
				select {
				case s.analysisSlots <- struct{}{}:
					defer func() { <-s.analysisSlots }()
				case <-ctx.Done():
					return ctx.Err()
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				snapshot, err := s.captureResultFiles(member.ID, false)
				if err != nil {
					return fmt.Errorf("run %s: %w", member.ID, err)
				}
				defer snapshot.close()
				if snapshot.active || snapshot.result.State != "completed" || snapshot.result.BatchID != result.BatchID {
					return errors.New("batch membership changed; retry after all runs stop")
				}
				total := int64(0)
				for _, file := range snapshot.files {
					if file.name == "events.jsonl" || file.name == "observations.jsonl" {
						total += file.size
					}
				}
				s.analysisJobMu.Lock()
				job.status.State, job.status.Phase, job.status.CompletedRuns = "running", fmt.Sprintf("Run %d/%d", i+1, len(selected)), i
				job.status.TotalBytes, job.status.ProcessedBytes, job.status.Progress = total, 0, 100*float64(i)/float64(len(selected))
				if job.status.StartedAt.IsZero() {
					job.status.StartedAt = time.Now().UTC()
				}
				job.status.SnapshotAt, job.status.UpdatedAt = snapshot.exportedAt, time.Now().UTC()
				err = s.persistBatchAnalysis(job.status)
				s.analysisJobMu.Unlock()
				if err != nil {
					return err
				}
				processed, last := int64(0), time.Time{}
				report := analysisProgress(func(phase string, bytes int64) {
					processed += bytes
					now := time.Now().UTC()
					if now.Sub(last) < time.Second {
						return
					}
					last = now
					s.analysisJobMu.Lock()
					defer s.analysisJobMu.Unlock()
					job.status.Phase = fmt.Sprintf("Run %d/%d · %s", i+1, len(selected), phase)
					job.status.ProcessedBytes, job.status.UpdatedAt = processed, now
					fraction := 0.
					if total > 0 {
						fraction = min(.99, float64(processed)/float64(total))
					}
					job.status.Progress = 100 * (float64(i) + fraction) / float64(len(selected))
				})
				analysis, err := analyzeResult(context.WithValue(ctx, analysisProgressKey{}, report), snapshot)
				if err != nil {
					return fmt.Errorf("run %s: %w", member.ID, err)
				}
				compactBatchAnalysis(&analysis)
				result.Runs = append(result.Runs, analysis)
				return nil
			}()
			if err != nil {
				return err
			}
		}
		members, err := s.batchMembers(ctx, result.BatchID)
		if err != nil {
			return err
		}
		if batchMembership(members) != job.status.Membership {
			return errors.New("saved batch members changed during analysis; retry to capture the current batch")
		}
		if err := validateBatchDefinitions(result.Runs); err != nil {
			return err
		}
		result.Summary = batchSummary(result.Runs)
		result.AsOf = time.Now().UTC()
		s.analysisJobMu.Lock()
		job.status.Phase, job.status.CompletedRuns = "saving", len(selected)
		job.status.UpdatedAt = time.Now().UTC()
		s.analysisJobMu.Unlock()
		return s.saveBatchAnalysis(ctx, job, result)
	}()
	s.analysisJobMu.Lock()
	defer s.analysisJobMu.Unlock()
	job.cancel()
	job.cancel = nil
	if err != nil {
		job.status.State, job.status.Phase, job.status.Error = "failed", "failed", err.Error()
		if errors.Is(err, context.Canceled) {
			job.status.State, job.status.Error = "interrupted", "Batch analysis was interrupted. Retry to start again."
		}
		job.status.FinishedAt, job.status.UpdatedAt = time.Now().UTC(), time.Now().UTC()
		if s.batchAnalysisJobs[job.status.BatchID] == job {
			if err := s.persistBatchAnalysis(job.status); err != nil {
				s.logger.Error("persist batch analysis failure", "batch", job.status.BatchID, "error", err)
			}
		}
	}
}
func (s *Server) saveBatchAnalysis(ctx context.Context, job *batchAnalysisJob, result any) error {
	root, err := func() (*os.Root, error) {
		s.analysisJobMu.Lock()
		defer s.analysisJobMu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if s.batchAnalysisJobs[job.status.BatchID] != job {
			return nil, context.Canceled
		}
		return s.batchAnalysisDirectory(job.status.BatchID, false)
	}()
	if err != nil {
		return err
	}
	defer root.Close()
	artifact, err := stageAnalysisJSON(ctx, root, result)
	if err != nil {
		return err
	}
	defer artifact.discard()

	s.analysisJobMu.Lock()
	defer s.analysisJobMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.batchAnalysisJobs[job.status.BatchID] != job {
		return context.Canceled
	}
	// Deletion also takes analysisJobMu, so membership stays stable through
	// publication even if a member was removed while the large file was saved.
	members, err := s.batchMembers(ctx, job.status.BatchID)
	if err != nil {
		return err
	}
	if batchMembership(members) != job.status.Membership {
		return errors.New("saved batch members changed during analysis; retry to capture the current batch")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := artifact.publish(batchResultFile); err != nil {
		return err
	}
	status := job.status
	status.State, status.Phase, status.Progress = "completed", "completed", 100
	status.CompletedRuns = status.TotalRuns
	status.ProcessedBytes = status.TotalBytes
	status.FinishedAt, status.UpdatedAt = time.Now().UTC(), time.Now().UTC()
	status.ResultURL = "/api/v1/batch-analysis-jobs/" + status.BatchID + "/result?jobId=" + status.ID
	if err := s.persistBatchAnalysis(status); err != nil {
		return err
	}
	job.status = status
	return nil
}

func (s *Server) handleBatchAnalysis(ctx context.Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/batch-analysis-jobs/"), "/")
		if len(parts) > 2 || !validResultID(parts[0]) || len(parts) == 2 && parts[1] != "result" {
			http.NotFound(w, r)
			return
		}
		id := parts[0]
		if len(parts) == 2 {
			s.handleBatchArtifact(w, r, id)
			return
		}
		var status batchAnalysisStatus
		var err error
		switch r.Method {
		case http.MethodGet:
			var members []savedResult
			members, err = s.batchMembers(r.Context(), id)
			if err == nil {
				status, err = s.batchAnalysisStatus(id)
				if err == nil && status.State == "completed" && status.Membership != batchMembership(members) {
					status.State = "idle"
				}
			}
		case http.MethodPost:
			status, err = s.startBatchAnalysis(ctx, id, r.URL.Query().Get("refresh") == "1")
		default:
			methodNotAllowed(w)
			return
		}
		if err != nil {
			writeBatchAnalysisError(w, err)
			return
		}
		code := http.StatusOK
		if r.Method == http.MethodPost && (status.State == "queued" || status.State == "running") {
			code = http.StatusAccepted
		}
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, code, status)
	}
}
func writeBatchAnalysisError(w http.ResponseWriter, err error) {
	code := http.StatusUnprocessableEntity
	if errors.Is(err, errResultNotFound) || errors.Is(err, os.ErrNotExist) {
		code = http.StatusNotFound
	}
	if errors.Is(err, errResultBusy) {
		code = http.StatusConflict
	}
	if errors.Is(err, errAnalysisQueueFull) {
		code = http.StatusServiceUnavailable
	}
	writeError(w, code, err.Error())
}
func (s *Server) handleBatchArtifact(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w)
		return
	}
	file, err := func() (resultFile, error) {
		s.analysisJobMu.Lock()
		defer s.analysisJobMu.Unlock()
		job, err := s.loadBatchAnalysis(id)
		if err != nil {
			return resultFile{}, err
		}
		if job.status.State != "completed" || r.URL.Query().Get("jobId") != "" && r.URL.Query().Get("jobId") != job.status.ID {
			return resultFile{}, errResultBusy
		}
		root, err := s.batchAnalysisDirectory(id, false)
		if err != nil {
			return resultFile{}, err
		}
		defer root.Close()
		return openResultFile(root, batchResultFile)
	}()
	if err != nil {
		writeBatchAnalysisError(w, err)
		return
	}
	defer file.file.Close()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s-batch-analysis.json"`, id))
	http.ServeContent(w, r, id+"-batch-analysis.json", file.info.ModTime(), file.file)
}

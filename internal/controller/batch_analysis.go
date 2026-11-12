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

	"github.com/k-p2p-lab/kpl-v3/internal/model"
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
	ReusedRuns    int      `json:"reusedRuns,omitempty"`
	AnalyzedRuns  int      `json:"analyzedRuns,omitempty"`
}
type batchAnalysisJob struct {
	status   batchAnalysisStatus
	cancel   context.CancelFunc
	previous *batchAnalysisStatus
	slot     *workSlot
	force    bool
}
type batchAnalysisResult struct {
	SourceHash      string                       `json:"sourceHash,omitempty"`
	Reliability     batchReliability             `json:"reliability"`
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
	decoder := json.NewDecoder(io.NewSectionReader(file.file, 0, file.size))
	if err = decoder.Decode(&status); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("batch analysis metadata contains trailing data")
	}
	if status.Version != batchAnalysisVersion || status.BatchID != id || !validResultID(status.ID) || status.SavedAnalysisID != "" && !validResultID(status.SavedAnalysisID) || status.ArtifactSlot < 0 || status.ArtifactSlot > 1 {
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
		if !member.Superseded && !previous[member.ID] {
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
	// Include historical attempts because they contribute to reliability statistics.
	// Ignore display-only download sizes and job statuses, and normalize order.
	contributors := make(map[string]bool, len(members))
	for _, member := range currentBatchMembers(members) {
		contributors[member.ID] = member.State == "completed"
	}
	canonical := make([]savedResult, 0, len(members))
	for _, m := range members {
		revision := ""
		if contributors[m.ID] {
			revision = m.SourceRevision
		}
		canonical = append(canonical, savedResult{SourceRevision: revision, PreviousRunIDs: m.PreviousRunIDs, Superseded: m.Superseded, ID: m.ID, Name: m.Name, State: m.State, Active: m.Active, BatchID: m.BatchID, Iteration: m.Iteration, Repetitions: m.Repetitions, StartedAt: m.StartedAt, FinishedAt: m.FinishedAt})
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

func selectBatchAnalysisMembers(members []savedResult) (selected, excluded []savedResult, expected int, pending bool, err error) {
	selected, excluded = []savedResult{}, []savedResult{}
	iterations := map[int]bool{}
	for _, member := range currentBatchMembers(members) {
		if member.Repetitions < 2 || member.Repetitions > maxScenarioRepetitions || member.Iteration < 1 || member.Iteration > member.Repetitions || iterations[member.Iteration] || expected != 0 && expected != member.Repetitions {
			return nil, nil, 0, false, errors.New("inconsistent repeated-run metadata")
		}
		expected = member.Repetitions
		iterations[member.Iteration] = true
		pending = pending || member.Active || member.State == "running" || member.State == "queued"
		if member.State == "completed" {
			selected = append(selected, member)
		} else {
			excluded = append(excluded, member)
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Iteration < selected[j].Iteration })
	return selected, excluded, expected, pending, nil
}

func (s *Server) startBatchAnalysis(ctx context.Context, id string, refresh bool) (batchAnalysisStatus, error) {
	return s.startBatchAnalysisExpected(ctx, id, refresh, nil)
}

func (s *Server) startBatchAnalysisExpected(ctx context.Context, id string, refresh bool, expectation *analysisExpectation) (batchAnalysisStatus, error) {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if s.shuttingDown || ctx.Err() != nil {
		return batchAnalysisStatus{}, errors.New("Controller is shutting down")
	}
	members, err := s.allBatchMembers(ctx, id)
	if err != nil {
		return batchAnalysisStatus{}, err
	}
	selected, _, expected, pending, err := selectBatchAnalysisMembers(members)
	if err != nil {
		return batchAnalysisStatus{}, err
	}
	if len(selected) < 2 && !pending {
		return batchAnalysisStatus{}, errors.New("batch mean requires at least two completed runs")
	}
	s.analysisJobMu.Lock()
	defer s.analysisJobMu.Unlock()
	existing, err := s.loadBatchAnalysis(id)
	if err != nil {
		return batchAnalysisStatus{}, err
	}
	membership := batchMembership(members)
	if existing.status.State == "queued" || existing.status.State == "running" || existing.status.State == "completed" && !refresh && existing.status.Membership == membership && existing.status.AnalysisVersion == currentAnalysisVersion && reusableSourceHash(existing.status.SourceHash) {
		return existing.status, nil
	}
	if !expectation.matchesBatch(existing.status.analysisJobStatus) {
		return batchAnalysisStatus{}, errAnalysisAttemptChanged
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
	status.SavedAnalysisID = existing.status.savedAnalysisID()
	status.ArtifactSlot = existing.status.ArtifactSlot
	for _, run := range selected {
		status.RunIDs = append(status.RunIDs, run.ID)
	}
	if err := s.persistBatchAnalysis(status); err != nil {
		return batchAnalysisStatus{}, err
	}
	work, cancel := context.WithCancel(ctx)
	job := &batchAnalysisJob{status: status, cancel: cancel, force: refresh}
	job.slot = s.enqueueResultAnalysisLocked(work, model.WorkQueueItem{Kind: "batch-analysis", Name: members[0].Name, BatchID: id}, cancel)
	if !refresh && existing.status.State == "completed" && existing.status.AnalysisVersion == currentAnalysisVersion && reusableSourceHash(existing.status.SourceHash) {
		previous := existing.status
		job.previous = &previous
	}
	s.batchAnalysisJobs[id] = job
	s.analysisWorkers.Add(1)
	go s.runBatchAnalysis(work, job)
	return status, nil
}

// Re-read after the slot is granted. The batch may have changed since admission.
func (s *Server) prepareBatchAnalysisJob(ctx context.Context, job *batchAnalysisJob) ([]savedResult, []savedResult, int, error) {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, nil, 0, err
	}
	members, err := s.allBatchMembers(ctx, job.status.BatchID)
	if err != nil {
		return nil, nil, 0, err
	}
	selected, excluded, expected, pending, err := selectBatchAnalysisMembers(members)
	if err != nil {
		return nil, nil, 0, err
	}
	if pending {
		return nil, nil, 0, errResultBusy
	}
	if len(selected) < 2 {
		return nil, nil, 0, errors.New("batch mean requires at least two completed runs")
	}
	s.analysisJobMu.Lock()
	defer s.analysisJobMu.Unlock()
	if s.batchAnalysisJobs[job.status.BatchID] != job {
		return nil, nil, 0, context.Canceled
	}
	job.status.Membership = batchMembership(members)
	job.status.TotalRuns, job.status.ExpectedRuns = len(selected), expected
	job.status.RunIDs = []string{}
	for _, member := range selected {
		job.status.RunIDs = append(job.status.RunIDs, member.ID)
	}
	if err := s.persistBatchAnalysis(job.status); err != nil {
		return nil, nil, 0, err
	}
	return selected, excluded, max(0, expected-len(selected)-len(excluded)), nil
}

func (s *Server) runBatchAnalysis(ctx context.Context, job *batchAnalysisJob) {
	defer s.analysisWorkers.Done()
	defer s.finishWork(job.slot)
	err := func() error {
		if err := s.waitWork(ctx, job.slot); err != nil {
			return err
		}
		s.updateWorkStatus(job.slot.id, "starting", 0)
		selected, excluded, missing, err := s.prepareBatchAnalysisJob(ctx, job)
		if err != nil {
			return err
		}
		if !job.force {
			if err := s.seedBatchRunAnalysisCache(ctx, job.status.BatchID); err != nil {
				s.logger.Warn("seed Run analysis cache", "batch", job.status.BatchID, "error", err)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if s.config.RunWorkers || job.slot.analysisWorkerURL != "" {
			return s.analyzeBatchWithWorker(ctx, job, selected, excluded, missing)
		}
		result := batchAnalysisResult{Version: batchAnalysisVersion, AnalysisVersion: currentAnalysisVersion, AnalysisID: job.status.ID, BatchID: job.status.BatchID, Name: selected[0].Name, AsOf: time.Now().UTC(), Aggregation: "equal-run-mean-v1", ExpectedRuns: job.status.ExpectedRuns, MissingRuns: missing, Excluded: excluded, Runs: []resultAnalysis{}}
		hashes := make(map[string]string, len(selected))
		if job.previous != nil {
			var sourceHash string
			var err error
			hashes, sourceHash, err = s.hashBatchAnalysisSources(ctx, job, selected)
			if err != nil {
				return err
			}
			if job.previous.SourceHash == sourceHash {
				return s.reuseBatchAnalysis(ctx, job)
			}
		}
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
				snapshot, err := s.captureBatchAnalysisSource(ctx, job, member)
				if err != nil {
					return fmt.Errorf("run %s: %w", member.ID, err)
				}
				defer snapshot.close()
				if snapshot.active || snapshot.result.State != "completed" || snapshot.result.BatchID != result.BatchID || snapshot.result.SourceRevision != member.SourceRevision {
					return errors.New("batch membership changed; retry after all runs stop")
				}
				if snapshot.analysisCacheKind == "batch" {
					analysis, err := decodeCachedBatchRunAnalysis(ctx, snapshot)
					if err != nil {
						return fmt.Errorf("read cached run %s: %w", member.ID, err)
					}
					hashes[member.ID] = snapshot.result.SourceHash
					result.Runs = append(result.Runs, analysis)
					s.analysisJobMu.Lock()
					job.status.ReusedRuns++
					job.status.Phase = fmt.Sprintf("Run %d/%d · reusing-analysis", i+1, len(selected))
					s.updateWorkStatus(job.slot.id, job.status.Phase, 100*float64(i+1)/float64(len(selected)))
					s.analysisJobMu.Unlock()
					return nil
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
				lastPhase := ""
				report := analysisProgress(func(phase string, bytes int64) {
					processed += bytes
					now := time.Now().UTC()
					if phase == lastPhase && now.Sub(last) < time.Second {
						return
					}
					last, lastPhase = now, phase
					s.analysisJobMu.Lock()
					defer s.analysisJobMu.Unlock()
					job.status.Phase = fmt.Sprintf("Run %d/%d · %s", i+1, len(selected), phase)
					job.status.ProcessedBytes, job.status.UpdatedAt = processed, now
					phaseTotal := total
					if phase == "checking-sources" {
						phaseTotal = analysisSourceSampleBytes(snapshot.files)
					}
					job.status.TotalBytes = phaseTotal
					fraction := 0.
					if phaseTotal > 0 {
						fraction = min(.99, float64(processed)/float64(phaseTotal))
					}
					job.status.Progress = 100 * (float64(i) + fraction) / float64(len(selected))
					s.updateWorkStatus(job.slot.id, job.status.Phase, job.status.Progress)
				})
				// Fresh/forced analyses hash the same captured files they analyze;
				// only potentially reusable batches need a separate verification pass.
				if hashes[member.ID] == "" {
					report("checking-sources", 0)
					hash, err := analysisSourceHash(ctx, snapshot.files, func(n int64) { report("checking-sources", n) })
					if err != nil {
						return fmt.Errorf("run %s: %w", member.ID, err)
					}
					hashes[member.ID] = hash
					processed = 0
				}
				snapshot.result.SourceHash = hashes[member.ID]
				analysis, err := analyzeResult(context.WithValue(ctx, analysisProgressKey{}, report), snapshot)
				if err != nil {
					return fmt.Errorf("run %s: %w", member.ID, err)
				}
				compactBatchAnalysis(&analysis)
				result.Runs = append(result.Runs, analysis)
				s.analysisJobMu.Lock()
				job.status.AnalyzedRuns++
				s.analysisJobMu.Unlock()
				return nil
			}()
			if err != nil {
				return err
			}
		}
		members, err := s.allBatchMembers(ctx, result.BatchID)
		if err != nil {
			return err
		}
		if batchMembership(members) != job.status.Membership {
			return errors.New("saved batch members changed during analysis; retry to capture the current batch")
		}
		if err := validateBatchDefinitions(result.Runs); err != nil {
			return err
		}
		result.Reliability, err = s.batchReliability(ctx, result.BatchID, result.Runs)
		if err != nil {
			return err
		}
		result.SourceHash, err = s.batchSourceHash(ctx, result.BatchID, members, hashes)
		if err != nil {
			return err
		}
		result.Summary = batchSummary(result.Runs)
		result.AsOf = time.Now().UTC()
		s.analysisJobMu.Lock()
		job.status.SourceHash = result.SourceHash
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
	var artifact *stagedAnalysisJSON
	if staged, ok := result.(*workerAnalysisArtifacts); ok {
		if staged.sinks["full"] == nil {
			return errors.New("batch analysis worker output is incomplete")
		}
		artifact = staged.sinks["full"].staged
	} else {
		artifact, err = stageAnalysisJSON(ctx, root, result)
		if err != nil {
			return err
		}
	}
	defer artifact.discard()
	cacheFile, cacheErr := openResultFile(artifact.root, artifact.temp)
	if cacheErr == nil {
		cacheErr = s.seedBatchRunAnalysisCacheFile(ctx, job.status.BatchID, job.status.ID, cacheFile, job.force)
		cacheFile.close()
	}
	if cacheErr != nil {
		s.logger.Warn("save Run analysis cache", "batch", job.status.BatchID, "error", cacheErr)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

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
	members, err := s.allBatchMembers(ctx, job.status.BatchID)
	if err != nil {
		return err
	}
	if batchMembership(members) != job.status.Membership {
		return errors.New("saved batch members changed during analysis; retry to capture the current batch")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	slot := job.status.nextArtifactSlot()
	if err := artifact.publish(analysisArtifactFile(batchResultFile, slot)); err != nil {
		return err
	}
	status := job.status
	status.ArtifactSlot, status.SavedAnalysisID = slot, ""
	status.State, status.Phase, status.Progress = "completed", "completed", 100
	status.CompletedRuns = status.TotalRuns
	status.ProcessedBytes = status.TotalBytes
	status.FinishedAt, status.UpdatedAt = time.Now().UTC(), time.Now().UTC()
	status.ResultURL = "/api/v1/batch-analysis-jobs/" + status.BatchID + "/result?jobId=" + status.ID
	stagedStatus, err := stageAnalysisJSON(ctx, root, status)
	if err != nil {
		return err
	}
	defer stagedStatus.discard()
	if err := s.commitBatchAnalysisStatus(ctx, stagedStatus, members); err != nil {
		return err
	}
	job.status = status
	return nil
}

// The caller holds analysisJobMu, which serializes batch membership changes.
// Telemetry appends use persistMu, so recheck each contributing source and
// commit the job's artifact pointer together. Large artifact writes and the
// complete membership scan are already finished when this lock is acquired.
func (s *Server) commitBatchAnalysisStatus(ctx context.Context, staged *stagedAnalysisJSON, members []savedResult) error {
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	for _, member := range currentBatchMembers(members) {
		if member.State != "completed" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		root, err := s.analysisDirectory(member.ID)
		if err != nil {
			return err
		}
		revision, err := sourceRevisionAt(root, member.ID)
		_ = root.Close()
		if err != nil {
			return err
		}
		if revision != member.SourceRevision {
			return errors.New("saved batch members changed during analysis; retry to capture the current batch")
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return staged.publish(batchJobFile)
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
			members, err = s.allBatchMembers(r.Context(), id)
			if errors.Is(err, errResultNotFound) {
				// A saved batch artifact and replay inputs can outlive every raw
				// result. Keep its last completed snapshot downloadable.
				record, readErr := s.readBatchExtension(id)
				if readErr != nil {
					err = readErr
				} else if record != nil && len(record.Current) > 0 {
					err = nil
				}
			}
			if err == nil {
				status, err = s.batchAnalysisStatus(id)
				if err == nil && status.State == "completed" && (status.Membership != batchMembership(members) || !reusableSourceHash(status.SourceHash) || status.AnalysisVersion != currentAnalysisVersion) {
					status.State, status.Stale = "idle", true
				}
			}
		case http.MethodPost:
			var expected *analysisExpectation
			expected, err = decodeAnalysisExpectation(w, r)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			status, err = s.startBatchAnalysisExpected(ctx, id, r.URL.Query().Get("refresh") == "1", expected)
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
	if errors.Is(err, errAnalysisAttemptChanged) {
		writeAnalysisAttemptChanged(w)
		return
	}
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
		if !job.status.servesArtifact(r.URL.Query().Get("jobId"), r.URL.Query().Get("saved") == "1") {
			return resultFile{}, errResultBusy
		}
		root, err := s.batchAnalysisDirectory(id, false)
		if err != nil {
			return resultFile{}, err
		}
		defer root.Close()
		return openResultFile(root, analysisArtifactFile(batchResultFile, job.status.ArtifactSlot))
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

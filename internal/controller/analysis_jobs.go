package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const currentAnalysisVersion = 4
const analysisJobLimit = 32
const analysisJobFile = "analysis-job.json"
const analysisResultFile = "analysis-result.json"
const analysisSummaryFile = "analysis-summary.json"

var errAnalysisQueueFull = errors.New("analysis queue is full; try again after a job finishes")

type analysisJobStatus struct {
	Version         int       `json:"version"`
	AnalysisVersion int       `json:"analysisVersion"`
	ID              string    `json:"id,omitempty"`
	RunID           string    `json:"runId"`
	State           string    `json:"state"`
	Phase           string    `json:"phase,omitempty"`
	ProcessedBytes  int64     `json:"processedBytes"`
	TotalBytes      int64     `json:"totalBytes"`
	Progress        float64   `json:"progress"`
	CreatedAt       time.Time `json:"createdAt"`
	StartedAt       time.Time `json:"startedAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
	FinishedAt      time.Time `json:"finishedAt"`
	SnapshotAt      time.Time `json:"snapshotAt"`
	Error           string    `json:"error,omitempty"`
	ResultURL       string    `json:"resultUrl,omitempty"`
}

type analysisJob struct {
	status analysisJobStatus
	cancel context.CancelFunc
}

// Callers acquire analysisJobMu before persistMu. Never recreate a deleted run.
func (s *Server) analysisDirectory(id string) (*os.Root, error) {
	if !validResultID(id) {
		return nil, errResultNotFound
	}
	deleted, err := s.state.resultDeletedLocked(id)
	if err != nil {
		return nil, err
	}
	if deleted {
		return nil, errResultNotFound
	}
	runs, err := s.openResultRuns()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = errResultNotFound
		}
		return nil, err
	}
	defer runs.Close()
	root, err := openResultDirectory(runs, id)
	if errors.Is(err, os.ErrNotExist) {
		err = errResultNotFound
	}
	return root, err
}

// Large artifacts are prepared without holding Controller locks. A caller must
// validate the live job and deletion fence again before publishing the file.
type stagedAnalysisJSON struct {
	root *os.Root
	temp string
}

func (staged *stagedAnalysisJSON) discard() { _ = staged.root.Remove(staged.temp) }
func (staged *stagedAnalysisJSON) publish(name string) error {
	// Rename replaces a link rather than following it.
	return staged.root.Rename(staged.temp, name)
}

func stageAnalysisJSON(ctx context.Context, root *os.Root, value any) (*stagedAnalysisJSON, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	staged := &stagedAnalysisJSON{root: root, temp: ".analysis-" + hex.EncodeToString(nonce[:]) + ".tmp"}
	file, err := root.OpenFile(staged.temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	if err = json.NewEncoder(resultContextWriter{ctx: ctx, writer: file}).Encode(value); err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close(), ctx.Err())
	if err != nil {
		staged.discard()
		return nil, err
	}
	return staged, nil
}

func writeAnalysisJSON(root *os.Root, name string, value any) error {
	staged, err := stageAnalysisJSON(context.Background(), root, value)
	if err != nil {
		return err
	}
	defer staged.discard()
	return staged.publish(name)
}

func (s *Server) persistAnalysisJob(status analysisJobStatus) error {
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	root, err := s.analysisDirectory(status.RunID)
	if err != nil {
		return err
	}
	defer root.Close()
	return writeAnalysisJSON(root, analysisJobFile, status)
}

// Completed results survive restart. In-flight jobs are explicitly interrupted,
// so a page reload cannot display an orphaned job as running forever.
func (s *Server) loadAnalysisJob(id string) (*analysisJob, error) {
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	root, err := s.analysisDirectory(id)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if job := s.analysisJobs[id]; job != nil {
		return job, nil
	}
	file, err := openResultFile(root, analysisJobFile)
	if errors.Is(err, os.ErrNotExist) {
		return &analysisJob{status: analysisJobStatus{Version: 1, RunID: id, State: "idle"}}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.file.Close()
	if file.size > resultMetadataLimit {
		return nil, errors.New("analysis job metadata is too large")
	}
	var status analysisJobStatus
	if err := json.NewDecoder(io.NewSectionReader(file.file, 0, file.size)).Decode(&status); err != nil {
		return nil, fmt.Errorf("read analysis job: %w", err)
	}
	if status.Version != 1 || status.RunID != id || !validResultID(status.ID) {
		return nil, errors.New("invalid analysis job metadata")
	}
	switch status.State {
	case "queued", "running":
		status.State, status.Phase, status.Error = "interrupted", "interrupted", "Controller restarted before analysis completed. Retry to start again."
	case "completed", "failed", "interrupted", "canceled":
	default:
		return nil, errors.New("invalid analysis job state")
	}
	job := &analysisJob{status: status}
	s.analysisJobs[id] = job
	return job, nil
}

func (s *Server) analysisJobStatus(id string) (analysisJobStatus, error) {
	s.analysisJobMu.Lock()
	defer s.analysisJobMu.Unlock()
	job, err := s.loadAnalysisJob(id)
	if err != nil {
		return analysisJobStatus{}, err
	}
	return job.status, nil
}

func (s *Server) startAnalysisJob(ctx context.Context, id string, refresh bool) (analysisJobStatus, error) {
	// Use server lifetime, not the POST request context. Fence Add against shutdown.
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if s.shuttingDown || ctx.Err() != nil {
		return analysisJobStatus{}, errors.New("Controller is shutting down")
	}
	s.analysisJobMu.Lock()
	defer s.analysisJobMu.Unlock()
	existing, err := s.loadAnalysisJob(id)
	if err != nil {
		return analysisJobStatus{}, err
	}
	if existing.status.State == "queued" || existing.status.State == "running" || existing.status.State == "completed" && !refresh && existing.status.AnalysisVersion == currentAnalysisVersion {
		return existing.status, nil
	}
	count := 0
	for _, job := range s.analysisJobs {
		if job.status.State == "queued" || job.status.State == "running" {
			count++
		}
	}
	for _, job := range s.batchAnalysisJobs {
		if job.status.State == "queued" || job.status.State == "running" {
			count++
		}
	}
	if count >= analysisJobLimit {
		return analysisJobStatus{}, errAnalysisQueueFull
	}
	// Fail promptly for unreadable or queued results; snapshots are pinned when
	// a worker starts, keeping queued jobs from retaining file descriptors.
	snapshot, err := s.captureResultFiles(id, false)
	if err != nil {
		return analysisJobStatus{}, err
	}
	queued := snapshot.result.State == "queued"
	snapshot.close()
	if queued {
		return analysisJobStatus{}, errResultBusy
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return analysisJobStatus{}, err
	}
	now := time.Now().UTC()
	job := &analysisJob{status: analysisJobStatus{Version: 1, AnalysisVersion: currentAnalysisVersion, ID: hex.EncodeToString(nonce[:]), RunID: id, State: "queued", Phase: "queued", CreatedAt: now, UpdatedAt: now}}
	if err := s.persistAnalysisJob(job.status); err != nil {
		return analysisJobStatus{}, err
	}
	workerCtx, cancel := context.WithCancel(ctx)
	job.cancel = cancel
	s.analysisJobs[id] = job
	s.analysisWorkers.Add(1)
	go s.runAnalysisJob(workerCtx, job)
	return job.status, nil
}

func (s *Server) runAnalysisJob(ctx context.Context, job *analysisJob) {
	defer s.analysisWorkers.Done()
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
		snapshot, err := s.captureResultFiles(job.status.RunID, false)
		if err != nil {
			return err
		}
		defer snapshot.close()
		var total int64
		for _, file := range snapshot.files {
			if file.name == "events.jsonl" || file.name == "observations.jsonl" {
				total += file.size
			}
		}
		s.analysisJobMu.Lock()
		job.status.State, job.status.Phase = "running", "starting"
		job.status.StartedAt, job.status.UpdatedAt = time.Now().UTC(), time.Now().UTC()
		job.status.SnapshotAt, job.status.TotalBytes = snapshot.exportedAt, total
		err = s.persistAnalysisJob(job.status)
		s.analysisJobMu.Unlock()
		if err != nil {
			return err
		}
		var processed int64
		var lastPhase string
		var lastUpdate time.Time
		report := analysisProgress(func(phase string, bytes int64) {
			processed += bytes
			now := time.Now().UTC()
			if strings.SplitN(phase, " ", 2)[0] == strings.SplitN(lastPhase, " ", 2)[0] && now.Sub(lastUpdate) < time.Second {
				return
			}
			s.analysisJobMu.Lock()
			defer s.analysisJobMu.Unlock()
			if ctx.Err() != nil {
				return
			}
			job.status.Phase, job.status.ProcessedBytes, job.status.UpdatedAt = phase, processed, now
			if total > 0 {
				job.status.Progress = min(100, 100*float64(processed)/float64(total))
			}
			// Persist once per second; a failed write is reported at finalization too.
			if err := s.persistAnalysisJob(job.status); err != nil {
				s.logger.Warn("persist analysis progress", "run", job.status.RunID, "error", err)
			}
			lastPhase, lastUpdate = phase, now
		})
		analysis, err := analyzeResult(context.WithValue(ctx, analysisProgressKey{}, report), snapshot)
		if err != nil {
			return err
		}
		report("saving", 0)
		analysis.AnalysisID = job.status.ID
		compact := analysis
		compact.Observations = []analysisObservation{}
		compact.Timeline = []analysisBin{}
		compact.BandwidthTimeline = []bandwidthBin{}
		if analysis.Research != nil {
			research := *analysis.Research
			research.MessageCount = len(research.Messages)
			research.Messages = []researchMessage{}
			compact.Research = &research
		}
		return s.saveAnalysisJob(ctx, job, analysis, compact)
	}()
	s.analysisJobMu.Lock()
	defer s.analysisJobMu.Unlock()
	job.cancel()
	job.cancel = nil
	if err != nil {
		job.status.State, job.status.Phase, job.status.Error = "failed", "failed", err.Error()
		if errors.Is(err, context.Canceled) {
			job.status.State, job.status.Phase, job.status.Error = "interrupted", "interrupted", "Analysis was interrupted. Retry to start again."
		}
		job.status.UpdatedAt, job.status.FinishedAt = time.Now().UTC(), time.Now().UTC()
		if s.analysisJobs[job.status.RunID] == job {
			if persistErr := s.persistAnalysisJob(job.status); persistErr != nil && !errors.Is(persistErr, errResultNotFound) {
				s.logger.Error("save analysis failure", "run", job.status.RunID, "error", persistErr)
			}
		}
	}
}

// Only directory pinning and publication hold shared locks; JSON encoding and
// fsync may take much longer than telemetry and status requests can wait.
func (s *Server) saveAnalysisJob(ctx context.Context, job *analysisJob, analysis, summary any) error {
	root, err := func() (*os.Root, error) {
		s.analysisJobMu.Lock()
		defer s.analysisJobMu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if s.analysisJobs[job.status.RunID] != job {
			return nil, context.Canceled
		}
		s.state.persistMu.Lock()
		defer s.state.persistMu.Unlock()
		return s.analysisDirectory(job.status.RunID)
	}()
	if err != nil {
		return err
	}
	defer root.Close()
	artifact, err := stageAnalysisJSON(ctx, root, analysis)
	if err != nil {
		return err
	}
	defer artifact.discard()
	compact, err := stageAnalysisJSON(ctx, root, summary)
	if err != nil {
		return err
	}
	defer compact.discard()

	s.analysisJobMu.Lock()
	defer s.analysisJobMu.Unlock()
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.analysisJobs[job.status.RunID] != job {
		return context.Canceled
	}
	current, err := s.analysisDirectory(job.status.RunID)
	if err != nil {
		return err
	}
	defer current.Close()
	if err := artifact.publish(analysisResultFile); err != nil {
		return err
	}
	if err := compact.publish(analysisSummaryFile); err != nil {
		return err
	}
	completed := job.status
	completed.State, completed.Phase, completed.Progress = "completed", "completed", 100
	completed.ProcessedBytes = completed.TotalBytes
	completed.FinishedAt, completed.UpdatedAt = time.Now().UTC(), time.Now().UTC()
	completed.ResultURL = "/api/v1/analysis-jobs/" + completed.RunID + "/result?jobId=" + completed.ID
	if err := writeAnalysisJSON(root, analysisJobFile, completed); err != nil {
		return err
	}
	job.status = completed
	return nil
}

func (s *Server) handleAnalysisJob(ctx context.Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/analysis-jobs/"), "/")
		if len(parts) < 1 || !validResultID(parts[0]) || len(parts) > 2 {
			http.NotFound(w, r)
			return
		}
		id := parts[0]
		if len(parts) == 2 {
			if parts[1] != "result" && parts[1] != "summary" {
				http.NotFound(w, r)
				return
			}
			s.handleAnalysisArtifact(w, r, id)
			return
		}
		var status analysisJobStatus
		var err error
		switch r.Method {
		case http.MethodGet:
			status, err = s.analysisJobStatus(id)
		case http.MethodPost:
			status, err = s.startAnalysisJob(ctx, id, r.URL.Query().Get("refresh") == "1")
		default:
			methodNotAllowed(w)
			return
		}
		if err != nil {
			code := http.StatusUnprocessableEntity
			if errors.Is(err, errResultNotFound) {
				code = http.StatusNotFound
			}
			if errors.Is(err, errResultBusy) {
				code = http.StatusConflict
			}
			if errors.Is(err, errAnalysisQueueFull) {
				code = http.StatusServiceUnavailable
			}
			writeError(w, code, err.Error())
			return
		}
		code := http.StatusOK
		if r.Method == http.MethodPost && (status.State == "queued" || status.State == "running") {
			code = http.StatusAccepted
		}
		writeJSON(w, code, status)
	}
}

func (s *Server) handleAnalysisArtifact(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w)
		return
	}
	file, err := func() (resultFile, error) {
		s.analysisJobMu.Lock()
		defer s.analysisJobMu.Unlock()
		job, err := s.loadAnalysisJob(id)
		if err != nil {
			return resultFile{}, err
		}
		if job.status.State != "completed" || r.URL.Query().Get("jobId") != "" && r.URL.Query().Get("jobId") != job.status.ID {
			return resultFile{}, errResultBusy
		}
		s.state.persistMu.Lock()
		defer s.state.persistMu.Unlock()
		root, err := s.analysisDirectory(id)
		if err != nil {
			return resultFile{}, err
		}
		defer root.Close()
		name := analysisResultFile
		if strings.HasSuffix(r.URL.Path, "/summary") {
			name = analysisSummaryFile
		}
		return openResultFile(root, name)
	}()
	if err != nil {
		code := http.StatusUnprocessableEntity
		if errors.Is(err, errResultNotFound) {
			code = http.StatusNotFound
		}
		if errors.Is(err, errResultBusy) {
			code = http.StatusConflict
		}
		writeError(w, code, err.Error())
		return
	}
	defer file.file.Close()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s-analysis.json"`, id))
	http.ServeContent(w, r, id+"-analysis.json", file.info.ModTime(), file.file)
}

package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"os"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

type analysisStageSink struct {
	mu     sync.Mutex
	staged *stagedAnalysisJSON
	file   *os.File
	hash   hash.Hash
	bytes  int64
}

type workerAnalysisArtifacts struct{ sinks map[string]*analysisStageSink }

func newWorkerAnalysisArtifacts(root *os.Root, names ...string) (*workerAnalysisArtifacts, error) {
	result := &workerAnalysisArtifacts{sinks: make(map[string]*analysisStageSink)}
	for _, name := range names {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			result.discard()
			return nil, err
		}
		staged := &stagedAnalysisJSON{root: root, temp: ".analysis-" + hex.EncodeToString(nonce[:]) + ".tmp"}
		file, err := root.OpenFile(staged.temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			result.discard()
			return nil, err
		}
		result.sinks[name] = &analysisStageSink{staged: staged, file: file, hash: sha256.New()}
	}
	return result, nil
}

func (a *workerAnalysisArtifacts) discard() {
	for _, sink := range a.sinks {
		sink.mu.Lock()
		if sink.file != nil {
			_ = sink.file.Close()
			sink.file = nil
		}
		sink.staged.discard()
		sink.mu.Unlock()
	}
}

func (a *workerAnalysisArtifacts) write(ctx context.Context, chunk analysisOutputChunk) error {
	sink := a.sinks[chunk.Name]
	if sink == nil || len(chunk.Data) > workerSourceChunk/2 {
		return errors.New("invalid analysis output target or chunk size")
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if sink.file == nil || chunk.Offset != sink.bytes {
		return errors.New("analysis output cursor does not match committed prefix")
	}
	n, err := sink.file.Write(chunk.Data)
	if n > 0 {
		_, _ = sink.hash.Write(chunk.Data[:n])
		sink.bytes += int64(n)
	}
	return errors.Join(err, ctx.Err())
}

func (a *workerAnalysisArtifacts) seal(ctx context.Context, infos []analysisOutputInfo) error {
	if len(infos) != len(a.sinks) {
		return errors.New("analysis process omitted an output")
	}
	seen := make(map[string]bool, len(infos))
	for _, info := range infos {
		sink := a.sinks[info.Name]
		if sink == nil || seen[info.Name] {
			return errors.New("analysis process returned an unknown or duplicate output")
		}
		seen[info.Name] = true
		sink.mu.Lock()
		err := ctx.Err()
		if err == nil && (info.Bytes <= 0 || sink.bytes != info.Bytes || hex.EncodeToString(sink.hash.Sum(nil)) != info.SHA256 || sink.file == nil) {
			err = errors.New("analysis output checksum or length does not match")
		}
		if err == nil {
			err = sink.file.Sync()
		}
		if sink.file != nil {
			err = errors.Join(err, sink.file.Close())
			sink.file = nil
		}
		sink.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

// Only bounded source blocks and encoded output blocks cross the connection.
// The full analysis, graph arrays and batch accumulators stay in the child.
func (s *Server) computeAnalysisProcess(ctx context.Context, input analysisProcessInput, snapshots []*resultSnapshot, root *os.Root, progress func(analysisProcessProgress), metadata workerproc.Handler) (*workerAnalysisArtifacts, analysisProcessResult, error) {
	input = s.analysisProcessResources(input)
	options, err := analysisProcessOptions(input, s.workerCommand)
	if err != nil {
		return nil, analysisProcessResult{}, err
	}
	names := []string{"full"}
	if input.BatchID == "" {
		names = append(names, "compact")
	}
	outputs, err := newWorkerAnalysisArtifacts(root, names...)
	if err != nil {
		return nil, analysisProcessResult{}, err
	}
	failed := true
	defer func() {
		if failed {
			outputs.discard()
		}
	}()
	source, err := s.controllerAnalysisPrefetch(ctx, input, snapshots)
	if err != nil {
		return nil, analysisProcessResult{}, err
	}
	defer source.close()
	var progressMu sync.Mutex
	broker := func(callCtx context.Context, method string, raw json.RawMessage) (any, error) {
		if method != "analysis.write" && len(workerproc.RequestData(callCtx)) != 0 {
			return nil, errors.New("unexpected binary analysis request")
		}
		switch method {
		case "source.read":
			var read workerSourceRead
			if err := json.Unmarshal(raw, &read); err != nil {
				return nil, err
			}
			return workerBinarySourceResult(source.read(callCtx, read))
		case "analysis.write":
			source.close()
			chunk, err := decodeAnalysisOutputChunk(callCtx, raw)
			if err != nil {
				return nil, err
			}
			return nil, outputs.write(callCtx, chunk)
		case "analysis.progress":
			var value analysisProcessProgress
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, err
			}
			if value.Snapshot < 0 || value.Snapshot >= len(snapshots) || value.Bytes < 0 || len(value.Phase) > 256 {
				return nil, errors.New("invalid analysis progress")
			}
			progressMu.Lock()
			defer progressMu.Unlock()
			if err := callCtx.Err(); err != nil {
				return nil, err
			}
			if progress != nil {
				progress(value)
			}
			return nil, nil
		case "analysis.batch-metadata":
			source.close()
			if metadata != nil {
				return metadata(callCtx, method, raw)
			}
		}
		return nil, invalidAnalysisOperation(method)
	}
	process, err := workerproc.Start(ctx, "controller-analysis", input, broker, options)
	if err != nil {
		if process != nil {
			s.rememberUnreapedWorker(input.RunID, process)
		}
		return nil, analysisProcessResult{}, err
	}
	stopAttempted := false
	defer func() {
		if !stopAttempted {
			stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			if closeErr := process.CloseContext(stopCtx); closeErr != nil {
				s.rememberUnreapedWorker(input.RunID, process)
			}
			cancel()
		}
	}()
	s.logger.Info("analysis worker started", "run", input.RunID, "analysis", input.AnalysisID, "workerPid", process.PID(), "workerEpoch", process.Epoch(), "cpus", options.MaxProcs, "memoryTargetBytes", options.MemoryLimitBytes)
	var result analysisProcessResult
	computeErr := process.Call(ctx, "analyze", nil, &result)
	source.close()
	if computeErr != nil {
		return nil, result, computeErr
	}
	s.logger.Info("analysis calculation resources", "analysis", input.AnalysisID, "cpus", result.CPUs, "memoryTargetBytes", result.MemoryLimitBytes)
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	err = process.CloseContext(closeCtx)
	cancel()
	stopAttempted = true
	if err != nil {
		s.rememberUnreapedWorker(input.RunID, process)
		return nil, result, fmt.Errorf("reap analysis worker: %w", err)
	}
	if err := outputs.seal(ctx, result.Outputs); err != nil {
		return nil, result, err
	}
	failed = false
	return outputs, result, nil
}

func (s *Server) analyzeJobWithWorker(ctx context.Context, job *analysisJob, snapshot *resultSnapshot, report analysisProgress) error {
	s.analysisJobMu.Lock()
	if s.analysisJobs[job.status.RunID] != job {
		s.analysisJobMu.Unlock()
		return context.Canceled
	}
	s.state.persistMu.Lock()
	root, err := s.analysisDirectory(job.status.RunID)
	s.state.persistMu.Unlock()
	s.analysisJobMu.Unlock()
	if err != nil {
		return err
	}
	defer root.Close()
	input := analysisProcessInput{RunID: snapshot.result.ID, AnalysisID: job.status.ID, Snapshots: []workerSnapshotDescriptor{describeWorkerSnapshot(snapshot)}}
	artifacts, result, err := s.computeAssignedAnalysis(ctx, job.slot, input, []*resultSnapshot{snapshot}, root, func(p analysisProcessProgress) { report(p.Phase, p.Bytes) }, nil)
	if err != nil {
		return err
	}
	defer artifacts.discard()
	if result.SourceHash != snapshot.result.SourceHash {
		return errors.New("analysis worker returned a different source identity")
	}
	return s.saveAnalysisJob(ctx, job, artifacts, nil)
}

func (s *Server) captureAnalysisSource(ctx context.Context, slot *workSlot, id string) (*resultSnapshot, error) {
	if slot.analysisWorkerURL != "" {
		return s.captureRemoteAnalysisResultFilesContext(ctx, id)
	}
	return s.captureResultFilesContext(ctx, id, false)
}

func (s *Server) computeAssignedAnalysis(ctx context.Context, slot *workSlot, input analysisProcessInput, snapshots []*resultSnapshot, root *os.Root, progress func(analysisProcessProgress), metadata workerproc.Handler) (*workerAnalysisArtifacts, analysisProcessResult, error) {
	input = s.analysisProcessResources(input)
	if slot.analysisWorkerURL == "" {
		return s.computeAnalysisProcess(ctx, input, snapshots, root, progress, metadata)
	}
	input.RuntimeImage = s.config.RuntimeImage
	if slot.analysisAgent == nil {
		return nil, analysisProcessResult{}, errors.New("remote analysis requires an assigned Agent identity")
	}
	if err := s.trackAnalysisWorker(ctx, slot.analysisWorkerURL, *slot.analysisAgent); err != nil {
		return nil, analysisProcessResult{}, err
	}
	source := s.remoteAnalysisSnapshotSource(snapshots)
	artifacts, result, err := computeRemoteAnalysis(ctx, slot.analysisWorkerURL, s.config.AnalysisAgentToken, input, root, source, progress, metadata, s.config.AnalysisTransferMiBPerSecond, s.config.AnalysisPrefetchMiB)
	if !errors.Is(err, errRemoteAnalysisCleanupUnconfirmed) {
		err = errors.Join(err, s.clearAnalysisWorker(slot.analysisWorkerURL))
	}
	if err != nil && artifacts != nil {
		artifacts.discard()
		artifacts = nil
	}
	return artifacts, result, err
}

func (s *Server) analyzeBatchWithWorker(ctx context.Context, job *batchAnalysisJob, selected, excluded []savedResult, missing int) error {
	remote := job.slot.analysisWorkerURL != ""
	verifiedHashes := map[string]string{}
	// The Controller samples remote inputs once so the worker does not make a
	// network round trip for each small sample or choose its own source identity.
	if job.previous != nil || remote {
		var sourceHash string
		var err error
		verifiedHashes, sourceHash, err = s.hashBatchAnalysisSources(ctx, job, selected)
		if err != nil {
			return err
		}
		if job.previous != nil && sourceHash == job.previous.SourceHash {
			return s.reuseBatchAnalysis(ctx, job)
		}
	}
	var snapshots []*resultSnapshot
	defer func() {
		for _, snapshot := range snapshots {
			snapshot.close()
		}
	}()
	input := analysisProcessInput{RunID: job.status.BatchID, AnalysisID: job.status.ID, BatchID: job.status.BatchID, Name: selected[0].Name, ExpectedRuns: job.status.ExpectedRuns, MissingRuns: missing, Excluded: excluded}
	for _, member := range selected {
		snapshot, err := s.captureBatchAnalysisSource(ctx, job, member)
		if err != nil {
			return err
		}
		snapshots = append(snapshots, snapshot)
		snapshot.releaseArchiveReader()
		if snapshot.active || snapshot.result.State != "completed" || snapshot.result.BatchID != input.BatchID || snapshot.result.SourceRevision != member.SourceRevision {
			return errors.New("batch membership changed; retry after all runs stop")
		}
		if snapshot.analysisCacheKind != "batch" {
			snapshot.result.SourceHash = verifiedHashes[member.ID]
		}
		input.Snapshots = append(input.Snapshots, describeWorkerSnapshot(snapshot))
	}
	s.analysisJobMu.Lock()
	if s.batchAnalysisJobs[job.status.BatchID] != job {
		s.analysisJobMu.Unlock()
		return context.Canceled
	}
	root, err := s.batchAnalysisDirectory(job.status.BatchID, false)
	if err == nil {
		job.status.State, job.status.Phase = "running", "starting"
		job.status.StartedAt, job.status.UpdatedAt = time.Now().UTC(), time.Now().UTC()
		err = s.persistBatchAnalysis(job.status)
	}
	s.analysisJobMu.Unlock()
	if root != nil {
		defer root.Close()
	}
	if err != nil {
		return err
	}
	lastIndex, lastPhase, processed := -1, "", int64(0)
	lastUpdate := time.Time{}
	progress := func(p analysisProcessProgress) {
		if p.Snapshot != lastIndex || lastPhase == "checking-sources" && p.Phase != "checking-sources" {
			processed = 0
		}
		processed += p.Bytes
		now := time.Now().UTC()
		if p.Snapshot == lastIndex && p.Phase == lastPhase && now.Sub(lastUpdate) < time.Second {
			return
		}
		lastIndex, lastPhase, lastUpdate = p.Snapshot, p.Phase, now
		var total int64
		for _, file := range snapshots[p.Snapshot].files {
			if file.name == "events.jsonl" || file.name == "observations.jsonl" {
				total += file.size
			}
		}
		if p.Phase == "checking-sources" {
			total = analysisSourceSampleBytes(snapshots[p.Snapshot].files)
		}
		s.analysisJobMu.Lock()
		defer s.analysisJobMu.Unlock()
		if ctx.Err() != nil || s.batchAnalysisJobs[job.status.BatchID] != job {
			return
		}
		job.status.Phase = fmt.Sprintf("Run %d/%d · %s", p.Snapshot+1, len(snapshots), p.Phase)
		job.status.CompletedRuns, job.status.TotalBytes, job.status.ProcessedBytes = p.Snapshot, total, processed
		fraction := 0.0
		if total > 0 {
			fraction = min(.99, float64(processed)/float64(total))
		}
		job.status.Progress = 100 * (float64(p.Snapshot) + fraction) / float64(len(snapshots))
		job.status.SnapshotAt, job.status.UpdatedAt = snapshots[p.Snapshot].exportedAt, now
		s.updateWorkStatus(job.slot.id, job.status.Phase, job.status.Progress)
	}
	var metadataMu sync.Mutex
	expectedSourceHash := ""
	metadata := func(callCtx context.Context, _ string, raw json.RawMessage) (any, error) {
		var request analysisBatchMetadataRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		if len(request.Runs) != len(snapshots) || len(request.Hashes) != len(snapshots) {
			return nil, errors.New("batch analysis source membership does not match")
		}
		original := make(map[string]savedResult, len(snapshots))
		for _, snapshot := range snapshots {
			original[snapshot.result.ID] = snapshot.result
		}
		seen := make(map[string]bool, len(snapshots))
		var reliabilityRuns []resultAnalysis
		for _, run := range request.Runs {
			id := run.Result.ID
			sourceHash := request.Hashes[id]
			base, exists := original[id]
			if !exists || seen[id] || len(sourceHash) > 128 || !currentSourceHash(sourceHash) || base.SourceHash != "" && base.SourceHash != sourceHash {
				return nil, errors.New("batch analysis process changed a captured source")
			}
			seen[id] = true
			reliabilityRuns = append(reliabilityRuns, resultAnalysis{Result: base, Metrics: model.Metrics{MeasurementIncomplete: run.Metrics.MeasurementIncomplete}})
		}
		members, err := s.allBatchMembers(callCtx, input.BatchID)
		if err != nil {
			return nil, err
		}
		if batchMembership(members) != job.status.Membership {
			return nil, errors.New("saved batch members changed during analysis")
		}
		reliability, err := s.batchReliability(callCtx, input.BatchID, reliabilityRuns)
		if err != nil {
			return nil, err
		}
		sourceHash, err := s.batchSourceHash(callCtx, input.BatchID, members, request.Hashes)
		if err != nil {
			return nil, err
		}
		metadataMu.Lock()
		expectedSourceHash = sourceHash
		metadataMu.Unlock()
		return analysisBatchMetadata{SourceHash: sourceHash, Reliability: reliability}, nil
	}
	artifacts, result, err := s.computeAssignedAnalysis(ctx, job.slot, input, snapshots, root, progress, metadata)
	if err != nil {
		return err
	}
	defer artifacts.discard()
	metadataMu.Lock()
	validHash := expectedSourceHash != "" && expectedSourceHash == result.SourceHash
	metadataMu.Unlock()
	if !validHash {
		return errors.New("batch analysis output has no confirmed source identity")
	}
	s.analysisJobMu.Lock()
	job.status.SourceHash = result.SourceHash
	job.status.ReusedRuns = 0
	for _, snapshot := range snapshots {
		if snapshot.analysisCacheKind == "batch" {
			job.status.ReusedRuns++
		}
	}
	job.status.AnalyzedRuns = len(snapshots) - job.status.ReusedRuns
	job.status.Phase, job.status.CompletedRuns, job.status.UpdatedAt = "saving", len(snapshots), time.Now().UTC()
	s.analysisJobMu.Unlock()
	return s.saveBatchAnalysis(ctx, job, artifacts)
}

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

type analysisProcessInput struct {
	RunID        string                     `json:"runId"`
	AnalysisID   string                     `json:"analysisId"`
	RuntimeImage string                     `json:"runtimeImage,omitempty"`
	BatchID      string                     `json:"batchId,omitempty"`
	Name         string                     `json:"name,omitempty"`
	ExpectedRuns int                        `json:"expectedRuns,omitempty"`
	MissingRuns  int                        `json:"missingRuns,omitempty"`
	Excluded     []savedResult              `json:"excluded,omitempty"`
	Snapshots    []workerSnapshotDescriptor `json:"snapshots"`
}

type analysisProcessProgress struct {
	Snapshot int    `json:"snapshot"`
	Phase    string `json:"phase"`
	Bytes    int64  `json:"bytes"`
}

type analysisOutputChunk struct {
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
	Data   []byte `json:"data"`
}

type analysisOutputInfo struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type analysisProcessResult struct {
	Outputs    []analysisOutputInfo `json:"outputs"`
	SourceHash string               `json:"sourceHash,omitempty"`
}

type analysisBatchMetadataRequest struct {
	Runs   []resultAnalysis  `json:"runs"`
	Hashes map[string]string `json:"hashes"`
}

type analysisBatchMetadata struct {
	SourceHash  string           `json:"sourceHash"`
	Reliability batchReliability `json:"reliability"`
}

type analysisProcessWriter struct {
	ctx    context.Context
	peer   *workerproc.Peer
	name   string
	offset int64
	hash   hash.Hash
}

func (w *analysisProcessWriter) Write(data []byte) (int, error) {
	total := 0
	for len(data) > 0 {
		length := min(len(data), workerSourceChunk/2)
		chunk := data[:length]
		if err := w.peer.Call(w.ctx, "analysis.write", analysisOutputChunk{Name: w.name, Offset: w.offset, Data: chunk}, nil); err != nil {
			return total, err
		}
		_, _ = w.hash.Write(chunk)
		w.offset += int64(length)
		total += length
		data = data[length:]
	}
	return total, nil
}

func writeAnalysisProcessJSON(ctx context.Context, peer *workerproc.Peer, name string, value any) (analysisOutputInfo, error) {
	writer := &analysisProcessWriter{ctx: ctx, peer: peer, name: name, hash: sha256.New()}
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		return analysisOutputInfo{}, err
	}
	return analysisOutputInfo{Name: name, Bytes: writer.offset, SHA256: hex.EncodeToString(writer.hash.Sum(nil))}, ctx.Err()
}

func compactSingleAnalysis(analysis resultAnalysis) resultAnalysis {
	compact := analysis
	compact.Observations = []analysisObservation{}
	compact.ScoreTimeline = []analysisScoreSeries{}
	compact.Timeline = []analysisBin{}
	compact.BandwidthTimeline = []bandwidthBin{}
	if analysis.Research != nil {
		research := *analysis.Research
		research.MessageCount = len(research.Messages)
		research.Messages = []researchMessage{}
		compact.Research = &research
	}
	return compact
}

func newAnalysisProcessHandler(_ context.Context, data json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
	var input analysisProcessInput
	if err := json.Unmarshal(data, &input); err != nil {
		return nil, err
	}
	if !validResultID(input.RunID) || input.RunID != peer.RunID() || !validResultID(input.AnalysisID) || len(input.Snapshots) == 0 || len(input.Snapshots) > maxScenarioRepetitions || input.BatchID == "" && (len(input.Snapshots) != 1 || input.Snapshots[0].Result.ID != input.RunID) {
		return nil, errors.New("invalid analysis process input")
	}
	if input.BatchID != "" && (input.BatchID != input.RunID || input.ExpectedRuns < 1 || input.ExpectedRuns > maxScenarioRepetitions || input.MissingRuns < 0 || input.MissingRuns > input.ExpectedRuns) {
		return nil, errors.New("invalid batch analysis identity or run counts")
	}
	seen := make(map[string]bool, len(input.Snapshots))
	for _, snapshot := range input.Snapshots {
		if !validResultID(snapshot.Result.ID) || snapshot.Active || seen[snapshot.Result.ID] || input.BatchID != "" && (snapshot.Result.BatchID != input.BatchID || snapshot.Result.State != "completed") {
			return nil, errors.New("analysis snapshot is active or invalid")
		}
		seen[snapshot.Result.ID] = true
		for _, file := range snapshot.Files {
			if file.Size < 0 {
				return nil, errors.New("negative analysis source size")
			}
		}
	}
	return func(ctx context.Context, method string, _ json.RawMessage) (any, error) {
		if method != "analyze" {
			return nil, invalidAnalysisOperation(method)
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		var progressErr error
		report := func(index int) analysisProgress {
			return func(phase string, bytes int64) {
				if progressErr == nil {
					progressErr = peer.Call(ctx, "analysis.progress", analysisProcessProgress{Snapshot: index, Phase: phase, Bytes: bytes}, nil)
					if progressErr != nil {
						cancel()
					}
				}
			}
		}
		if input.BatchID == "" {
			snapshot := workerSnapshotFromDescriptor(ctx, peer, 0, input.Snapshots[0])
			analysis, err := analyzeResult(context.WithValue(ctx, analysisProgressKey{}, report(0)), snapshot)
			if err != nil || progressErr != nil {
				return nil, errors.Join(err, progressErr)
			}
			analysis.AnalysisID = input.AnalysisID
			full, err := writeAnalysisProcessJSON(ctx, peer, "full", analysis)
			if err != nil {
				return nil, err
			}
			compact, err := writeAnalysisProcessJSON(ctx, peer, "compact", compactSingleAnalysis(analysis))
			return analysisProcessResult{Outputs: []analysisOutputInfo{full, compact}, SourceHash: snapshot.result.SourceHash}, err
		}
		result := batchAnalysisResult{Version: batchAnalysisVersion, AnalysisVersion: currentAnalysisVersion, AnalysisID: input.AnalysisID, BatchID: input.BatchID, Name: input.Name, AsOf: time.Now().UTC(), Aggregation: "equal-run-mean-v1", ExpectedRuns: input.ExpectedRuns, MissingRuns: input.MissingRuns, Excluded: input.Excluded, Runs: []resultAnalysis{}}
		hashes := make(map[string]string, len(input.Snapshots))
		for index, descriptor := range input.Snapshots {
			snapshot := workerSnapshotFromDescriptor(ctx, peer, index, descriptor)
			progress := report(index)
			progress("checking-sources", 0)
			sourceHash := snapshot.result.SourceHash
			var err error
			if sourceHash == "" {
				sourceHash, err = analysisSourceHash(ctx, snapshot.files, func(n int64) { progress("checking-sources", n) })
			}
			if err != nil || progressErr != nil {
				return nil, errors.Join(err, progressErr)
			}
			snapshot.result.SourceHash = sourceHash
			hashes[snapshot.result.ID] = sourceHash
			analysis, err := analyzeResult(context.WithValue(ctx, analysisProgressKey{}, progress), snapshot)
			if err != nil || progressErr != nil {
				return nil, errors.Join(err, progressErr)
			}
			compactBatchAnalysis(&analysis)
			result.Runs = append(result.Runs, analysis)
		}
		if err := validateBatchDefinitions(result.Runs); err != nil {
			return nil, err
		}
		// Environment/retry accounting remains authoritative in the parent.
		// Send only the small fields that calculation needs, not graph arrays.
		metadataRequest := analysisBatchMetadataRequest{Hashes: hashes}
		for _, run := range result.Runs {
			metadataRequest.Runs = append(metadataRequest.Runs, resultAnalysis{Result: run.Result, Metrics: model.Metrics{MeasurementIncomplete: run.Metrics.MeasurementIncomplete}})
		}
		var metadata analysisBatchMetadata
		if err := peer.Call(ctx, "analysis.batch-metadata", metadataRequest, &metadata); err != nil {
			return nil, err
		}
		result.Reliability, result.SourceHash = metadata.Reliability, metadata.SourceHash
		result.Summary, result.AsOf = batchSummary(result.Runs), time.Now().UTC()
		full, err := writeAnalysisProcessJSON(ctx, peer, "full", result)
		return analysisProcessResult{Outputs: []analysisOutputInfo{full}, SourceHash: result.SourceHash}, err
	}, nil
}

var _ io.Writer = (*analysisProcessWriter)(nil)

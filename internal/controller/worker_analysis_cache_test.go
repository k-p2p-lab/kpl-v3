package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type countedAnalysisCacheReader struct {
	reader *bytes.Reader
	bytes  *atomic.Int64
}

func (r countedAnalysisCacheReader) ReadAt(data []byte, offset int64) (int, error) {
	n, err := r.reader.ReadAt(data, offset)
	r.bytes.Add(int64(n))
	return n, err
}

func cachedAnalysisWorkerFixture(t *testing.T) (*Server, analysisProcessInput, []*resultSnapshot, []resultAnalysis, *atomic.Int64, int64) {
	t.Helper()
	s, input, originals, _ := remoteAnalysisTransportFixture(t)
	var cached []*resultSnapshot
	var expected []resultAnalysis
	readBytes, totalBytes := new(atomic.Int64), int64(0)
	input.Snapshots = nil
	input.ExpectedRuns, input.MissingRuns = 3, 1
	for _, original := range originals {
		analysis, err := analyzeResult(t.Context(), original)
		if err != nil {
			t.Fatal(err)
		}
		compactBatchAnalysis(&analysis)
		if analysis.Research == nil || analysis.Research.MessageCount == 0 || analysis.Research.Overview == nil {
			t.Fatal("fixture must retain nonempty batch research projections")
		}
		data, err := json.Marshal(analysis)
		if err != nil {
			t.Fatal(err)
		}
		current := original.result
		current.Repetitions = 3
		snapshot := &resultSnapshot{result: current, exportedAt: time.Now().UTC(), storedState: "completed", analysisCacheKind: "batch",
			files: []resultFile{{name: batchRunCacheSourceName, size: int64(len(data)), readAt: countedAnalysisCacheReader{reader: bytes.NewReader(data), bytes: readBytes}}}}
		cached = append(cached, snapshot)
		input.Snapshots = append(input.Snapshots, describeWorkerSnapshot(snapshot))
		var decoded resultAnalysis
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}
		decoded.Result = current
		decoded.Result.Analysis, decoded.Result.BatchAnalysis = nil, nil
		expected = append(expected, decoded)
		totalBytes += int64(len(data))
	}
	return s, input, cached, expected, readBytes, totalBytes
}

func TestBatchAnalysisWorkersReuseCachedRunProjectionWithoutRawSources(t *testing.T) {
	for _, test := range []struct {
		name          string
		remote, mixed bool
	}{{"local", false, false}, {"remote", true, false}, {"mixed-local", false, true}, {"mixed-remote", true, true}} {
		t.Run(test.name, func(t *testing.T) {
			s, input, snapshots, want, readBytes, totalBytes := cachedAnalysisWorkerFixture(t)
			if test.mixed {
				index := len(snapshots) - 1
				raw, err := s.captureResultFilesContext(t.Context(), snapshots[index].result.ID, false)
				if err != nil {
					t.Fatal(err)
				}
				defer raw.close()
				raw.result = snapshots[index].result
				analysis, err := analyzeResult(t.Context(), raw)
				if err != nil {
					t.Fatal(err)
				}
				compactBatchAnalysis(&analysis)
				encoded, err := json.Marshal(analysis)
				if err != nil {
					t.Fatal(err)
				}
				analysis = resultAnalysis{}
				if err := json.Unmarshal(encoded, &analysis); err != nil {
					t.Fatal(err)
				}
				snapshots[index], want[index] = raw, analysis
				input.Snapshots[index] = describeWorkerSnapshot(raw)
			}
			root, err := os.OpenRoot(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			hash := analysisSourceHashPrefix + strings.Repeat("e", 64)
			metadataCalls := 0
			metadata := func(_ context.Context, method string, raw json.RawMessage) (any, error) {
				metadataCalls++
				var request analysisBatchMetadataRequest
				if method != "analysis.batch-metadata" || json.Unmarshal(raw, &request) != nil || len(request.Runs) != len(want) || len(request.Hashes) != len(want) {
					return nil, errors.New("cached batch omitted authoritative contributor validation")
				}
				for _, result := range want {
					if request.Hashes[result.Result.ID] != result.Result.SourceHash {
						return nil, errors.New("cached contributor changed its captured source hash")
					}
				}
				return analysisBatchMetadata{SourceHash: hash}, nil
			}
			var phases []string
			progress := func(p analysisProcessProgress) { phases = append(phases, p.Phase) }
			var artifacts *workerAnalysisArtifacts
			var result analysisProcessResult
			if test.remote {
				worker := newRemoteAnalysisWorker(remoteTestToken, s.logger, s.workerCommand)
				endpoint := httptest.NewServer(worker)
				defer endpoint.Close()
				artifacts, result, err = computeRemoteAnalysis(t.Context(), endpoint.URL, remoteTestToken, input, root, workerSourceHandler(snapshots), progress, metadata, 1024)
			} else {
				artifacts, result, err = s.computeAnalysisProcess(t.Context(), input, snapshots, root, progress, metadata)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer artifacts.discard()
			got := readRemoteAnalysisArtifact(t, root, artifacts)
			if result.SourceHash != hash || metadataCalls != 1 || len(got.Runs) != len(want) || got.ExpectedRuns != 3 || got.MissingRuns != 1 {
				t.Fatalf("cached batch identity or aggregation counts changed: %+v", got)
			}
			for index, expected := range want {
				actual := got.Runs[index]
				if !reflect.DeepEqual(actual.Metrics, expected.Metrics) || !reflect.DeepEqual(actual.Research, expected.Research) ||
					!reflect.DeepEqual(actual.Observations, expected.Observations) || !actual.AsOf.Equal(expected.AsOf) || actual.Result.Repetitions != 3 {
					t.Fatalf("cached Run %s changed: metrics=%t research=%t observations=%t asOf=%t repetitions=%d", expected.Result.ID,
						reflect.DeepEqual(actual.Metrics, expected.Metrics), reflect.DeepEqual(actual.Research, expected.Research), reflect.DeepEqual(actual.Observations, expected.Observations), actual.AsOf.Equal(expected.AsOf), actual.Result.Repetitions)
				}
			}
			if !reflect.DeepEqual(got.Summary, batchSummary(want)) {
				t.Fatal("cached batch summary differs from its complete per-Run projections")
			}
			if readBytes.Load() <= 0 || readBytes.Load() > totalBytes {
				t.Fatalf("worker read bytes outside its captured cache artifacts: %d / %d", readBytes.Load(), totalBytes)
			}
			if len(phases) == 0 {
				t.Fatal("cached reuse did not report progress")
			}
			reused := false
			for _, phase := range phases {
				reused = reused || phase == "reusing-analysis"
				if !test.mixed && phase != "reusing-analysis" {
					t.Fatalf("cached reuse performed raw analysis work: %q", phase)
				}
			}
			if !reused {
				t.Fatal("batch did not reuse its cached contributor")
			}
		})
	}
}

func TestCachedBatchRunDecoderRejectsUntrustedOrUnsettledProjection(t *testing.T) {
	_, _, snapshots, expected, _, _ := cachedAnalysisWorkerFixture(t)
	base := snapshots[0]
	for _, name := range []string{"analysis-version", "format-version", "run-id", "hash", "revision", "pending-window", "trailing-data", "oversized", "wrong-kind", "raw-source"} {
		t.Run(name, func(t *testing.T) {
			value := expected[0]
			snapshot := *base
			data, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "analysis-version":
				value.AnalysisVersion--
			case "format-version":
				value.Version++
			case "run-id":
				value.Result.ID = "another-run"
			case "hash":
				value.Result.SourceHash = analysisSourceHashPrefix + strings.Repeat("1", 64)
			case "revision":
				value.Result.SourceRevision = "outdated"
			case "pending-window":
				value.Metrics.PendingPublications = 1
			case "wrong-kind":
				snapshot.analysisCacheKind = "single"
			case "raw-source":
				snapshot.analysisCacheKind = ""
			}
			data, err = json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if name == "trailing-data" {
				data = append(data, []byte("{}")...)
			}
			snapshot.files = []resultFile{{name: batchRunCacheSourceName, size: int64(len(data)), readAt: bytes.NewReader(data)}}
			if name == "oversized" {
				snapshot.files[0].size = batchRunCacheMaxBytes + 1
			}
			if _, err := decodeCachedBatchRunAnalysis(t.Context(), &snapshot); err == nil {
				t.Fatal("invalid cached Run was reused")
			}
		})
	}
}

func TestRemoteCachedSourceRequiresExplicitDescriptorAndCurrentProtocol(t *testing.T) {
	_, input, _, _, _, _ := cachedAnalysisWorkerFixture(t)
	read := workerSourceRead{Snapshot: 0, File: 0, Length: workerSourceChunk}
	if _, _, err := remoteAnalysisReadBoundary(input, read); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"", "single", "unknown"} {
		mutated := input
		mutated.Snapshots = append([]workerSnapshotDescriptor(nil), input.Snapshots...)
		mutated.Snapshots[0].AnalysisCacheKind = kind
		if _, _, err := remoteAnalysisReadBoundary(mutated, read); err == nil {
			t.Fatalf("undeclared cached source kind %q was remotely readable", kind)
		}
	}
	input.BatchID = ""
	if _, _, err := remoteAnalysisReadBoundary(input, read); err == nil {
		t.Fatal("batch-only cache was accepted for single-Run full analysis")
	}
	build := "sha256:" + strings.Repeat("1", 64)
	if compatibleRemoteAnalysis(remoteAnalysisHello{Protocol: 1, AnalysisVersion: currentAnalysisVersion, Build: build}, build, "") {
		t.Fatal("pre-cache protocol accepted inputs it cannot interpret safely")
	}
}

package controller

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
)

func resultFixture(t *testing.T, server *Server, id, state string, started time.Time) (model.Experiment, []byte) {
	t.Helper()
	experiment := model.Experiment{ID: id, Name: "Saved " + id, State: state, StartedAt: started}
	if state != "running" {
		experiment.FinishedAt = started.Add(time.Minute)
	}
	if err := server.persistManifest(experiment, []byte("version: 3\nname: original scenario\n")); err != nil {
		t.Fatal(err)
	}
	metadata, err := os.ReadFile(filepath.Join(server.config.DataDir, "runs", id, "experiment.json"))
	if err != nil {
		t.Fatal(err)
	}
	return experiment, metadata
}

func resultRequest(server *Server, method, path string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(recorder, httptest.NewRequest(method, path, nil))
	return recorder
}

func decodeResultZIP(t *testing.T, data []byte) map[string][]byte {
	t.Helper()
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	files := make(map[string][]byte)
	for _, file := range reader.File {
		if _, duplicate := files[file.Name]; duplicate {
			t.Fatalf("duplicate ZIP entry %q", file.Name)
		}
		opened, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		contents, err := io.ReadAll(opened)
		_ = opened.Close()
		if err != nil {
			t.Fatal(err)
		}
		files[file.Name] = contents
	}
	return files
}

func TestResultsListReadsDiskAfterRestartAndReportsUnreadableMetadata(t *testing.T) {
	var logs bytes.Buffer
	server := New(ServerConfig{DataDir: t.TempDir(), Token: "secret"}, slog.New(slog.NewTextHandler(&logs, nil)))
	started := time.Date(2026, 9, 4, 1, 0, 0, 0, time.UTC)
	_, original := resultFixture(t, server, "run-old", "running", started)
	resultFixture(t, server, "run-new", "completed", started.Add(time.Hour))
	resultFixture(t, server, "run-corrupt", "completed", started)
	if err := os.WriteFile(filepath.Join(server.config.DataDir, "runs", "run-corrupt", "experiment.json"), []byte("broken json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(server.config.DataDir, "runs", "run-missing"), 0o755); err != nil {
		t.Fatal(err)
	}
	// A new Controller has no active runs, even though old metadata says running.
	restarted := New(server.config, server.logger)
	response := resultRequest(restarted, http.MethodGet, "/api/v1/results")
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body)
	}
	var results []savedResult
	if err := json.Unmarshal(response.Body.Bytes(), &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 4 || results[0].ID != "run-new" || results[1].ID != "run-old" || results[1].State != "interrupted" || results[1].Active {
		t.Fatalf("unexpected results: %+v", results)
	}
	for _, result := range results[2:] {
		if result.State != "unreadable" || result.Active || result.Name != result.ID {
			t.Fatalf("corrupt result was hidden or misrepresented: %+v", result)
		}
	}
	if !strings.Contains(logs.String(), "run-corrupt") || !strings.Contains(logs.String(), "run-missing") {
		t.Fatalf("missing warnings: %s", logs.String())
	}
	unchanged, _ := os.ReadFile(filepath.Join(server.config.DataDir, "runs", "run-old", "experiment.json"))
	if !bytes.Equal(original, unchanged) {
		t.Fatal("listing changed stored metadata")
	}
}

func TestResultDownloadIncludesFullPersistedFilesAndOriginalInterruptedState(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir(), Token: "secret"}, nil)
	_, original := resultFixture(t, server, "run-export", "running", time.Now().UTC())
	dir := filepath.Join(server.config.DataDir, "runs", "run-export")
	events := []byte(strings.Repeat("{\"kind\":\"deliver\"}\n", recentEventLimit+200))
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl"), events, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "agent-secret.json"), []byte("must not export"), 0o600); err != nil {
		t.Fatal(err)
	}
	response := resultRequest(server, http.MethodGet, "/api/v1/experiments/run-export/download")
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/zip" || response.Header().Get("Content-Disposition") != `attachment; filename="run-export.zip"` || response.Header().Get("Content-Length") != strconv.Itoa(response.Body.Len()) {
		t.Fatalf("download status=%d headers=%v body=%s", response.Code, response.Header(), response.Body)
	}
	archiveReader, err := zip.NewReader(bytes.NewReader(response.Body.Bytes()), int64(response.Body.Len()))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range archiveReader.File {
		if file.Name == "export.json" && file.Method != zip.Store {
			t.Fatalf("export.json compression method=%d, want Store", file.Method)
		}
	}
	files := decodeResultZIP(t, response.Body.Bytes())
	if len(files) != 5 || !json.Valid(files["metrics.json"]) || !bytes.Equal(files["experiment.json"], original) || !bytes.Equal(files["events.jsonl"], events) || string(files["scenario.yaml"]) != "version: 3\nname: original scenario\n" {
		t.Fatalf("incorrect archive contents: %v", files)
	}
	var exported struct {
		State, StoredState string
		Partial, Active    bool
		EventBytes         int64
		Files              map[string]int64
		ExportedAt         time.Time
	}
	if err := json.Unmarshal(files["export.json"], &exported); err != nil {
		t.Fatal(err)
	}
	if exported.State != "interrupted" || exported.StoredState != "running" || !exported.Partial || exported.Active || exported.EventBytes != int64(len(events)) || exported.ExportedAt.IsZero() || exported.Files["events.jsonl"] != int64(len(events)) {
		t.Fatalf("incorrect export metadata: %+v", exported)
	}
	var rawExport struct {
		ExportedAt string `json:"exportedAt"`
	}
	if err := json.Unmarshal(files["export.json"], &rawExport); err != nil || len(rawExport.ExportedAt) != len("2026-09-05T12:34:56.123456789Z") {
		t.Fatalf("export time is not fixed-width RFC3339: %q, %v", rawExport.ExportedAt, err)
	}
}

func TestResultListReportsExactStableDownloadBytesWithFreshExportTime(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	experiment, _ := resultFixture(t, server, "run-sized", "completed", time.Now().UTC())
	if err := server.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{{
		RunID: experiment.ID, Type: "operation_failed", Timestamp: time.Now().UTC(),
	}}}); err != nil {
		t.Fatal(err)
	}

	readListed := func() savedResult {
		t.Helper()
		response := resultRequest(server, http.MethodGet, "/api/v1/results")
		if response.Code != http.StatusOK {
			t.Fatalf("list status=%d body=%s", response.Code, response.Body)
		}
		var results []savedResult
		if err := json.Unmarshal(response.Body.Bytes(), &results); err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatalf("saved result missing from list: %+v", results)
		}
		return results[0]
	}
	assertDownloadSize := func(want int64) time.Time {
		t.Helper()
		response := resultRequest(server, http.MethodGet, "/api/v1/experiments/"+experiment.ID+"/download")
		if response.Code != http.StatusOK || int64(response.Body.Len()) != want || response.Header().Get("Content-Length") != strconv.FormatInt(want, 10) || response.Header().Get(resultSizeMaxAgeHeader) != "" {
			t.Fatalf("download size: listed=%d status=%d header=%q body=%d", want, response.Code, response.Header().Get("Content-Length"), response.Body.Len())
		}
		files := decodeResultZIP(t, response.Body.Bytes())
		var exported struct {
			ExportedAt time.Time `json:"exportedAt"`
		}
		if err := json.Unmarshal(files["export.json"], &exported); err != nil {
			t.Fatal(err)
		}
		return exported.ExportedAt
	}

	listed := readListed()
	if listed.DownloadBytes != nil || listed.DownloadSizeMaxAgeMS != nil {
		t.Fatalf("cold list synchronously measured a ZIP: %+v", listed)
	}
	server.resultArchiveMu.Lock()
	_, cachedBeforeHead := server.resultArchives[experiment.ID]
	server.resultArchiveMu.Unlock()
	if cachedBeforeHead {
		t.Fatal("cold list populated the archive cache")
	}
	head := resultRequest(server, http.MethodHead, "/api/v1/experiments/"+experiment.ID+"/download")
	headBytes, err := strconv.ParseInt(head.Header().Get("Content-Length"), 10, 64)
	if err != nil || head.Code != http.StatusOK || head.Body.Len() != 0 || headBytes <= 0 || head.Header().Get(resultSizeMaxAgeHeader) != "" {
		t.Fatalf("HEAD did not prepare an exact size: status=%d headers=%v body=%d error=%v", head.Code, head.Header(), head.Body.Len(), err)
	}
	listed = readListed()
	if listed.DownloadBytes == nil || *listed.DownloadBytes != headBytes || listed.DownloadSizeMaxAgeMS != nil {
		t.Fatalf("prepared size missing from list: head=%d result=%+v", headBytes, listed)
	}
	server.resultArchiveMu.Lock()
	cached := server.resultArchives[experiment.ID]
	server.resultArchiveMu.Unlock()
	if cached.pendingPublications != 0 || !cached.expiresAt.IsZero() {
		t.Fatalf("stable archive received a TTL: %+v", cached)
	}
	time.Sleep(time.Millisecond)
	downloadedAt := assertDownloadSize(headBytes)
	if !downloadedAt.After(cached.exportedAt) {
		t.Fatalf("stable cached size pinned an old export time: %s <= %s", downloadedAt, cached.exportedAt)
	}
}

func TestPendingResultArchivePinsBoundaryAndListExtendsTTL(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	experiment, _ := resultFixture(t, server, "run-pending-size", "completed", time.Now().UTC())
	publishedAt := time.Now().UTC()
	if err := server.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{{
		RunID: experiment.ID, NodeID: "publisher", Type: "publish", Topic: "topic", MessageID: "message", Timestamp: publishedAt,
		Fields: map[string]any{"measurementDefinition": sessionWindowDefinition, "deliveryWindow": "1h"},
	}}}); err != nil {
		t.Fatal(err)
	}
	response := resultRequest(server, http.MethodGet, "/api/v1/results")
	var listed []savedResult
	if err := json.Unmarshal(response.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].DownloadBytes != nil || listed[0].DownloadSizeMaxAgeMS != nil {
		t.Fatalf("cold pending result was synchronously measured: %s", response.Body)
	}
	head := resultRequest(server, http.MethodHead, "/api/v1/experiments/"+experiment.ID+"/download")
	headBytes, err := strconv.ParseInt(head.Header().Get("Content-Length"), 10, 64)
	if err != nil || head.Code != http.StatusOK || head.Body.Len() != 0 || headBytes <= 0 {
		t.Fatalf("pending HEAD measurement failed: status=%d headers=%v body=%d error=%v", head.Code, head.Header(), head.Body.Len(), err)
	}
	headMaxAgeMS, err := strconv.ParseInt(head.Header().Get(resultSizeMaxAgeHeader), 10, 64)
	maxAdvertisedAgeMS := (resultArchiveCacheTTL - resultArchiveResponseGrace).Milliseconds()
	if err != nil || headMaxAgeMS <= 0 || headMaxAgeMS > maxAdvertisedAgeMS {
		t.Fatalf("pending HEAD max age is missing or invalid: %q, %v", head.Header().Get(resultSizeMaxAgeHeader), err)
	}
	response = resultRequest(server, http.MethodGet, "/api/v1/results")
	if err := json.Unmarshal(response.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].DownloadBytes == nil || *listed[0].DownloadBytes != headBytes || listed[0].DownloadSizeMaxAgeMS == nil || *listed[0].DownloadSizeMaxAgeMS <= 0 || *listed[0].DownloadSizeMaxAgeMS > maxAdvertisedAgeMS {
		t.Fatalf("prepared pending size missing: head=%d list=%s", headBytes, response.Body)
	}
	server.resultArchiveMu.Lock()
	cached := server.resultArchives[experiment.ID]
	server.resultArchiveMu.Unlock()
	if cached.pendingPublications != 1 || !cached.expiresAt.After(time.Now().UTC()) || !cached.listGraceExtended {
		t.Fatalf("pending archive cache is not bounded: %+v", cached)
	}

	snapshot, err := server.captureResult(experiment.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.exportedAt = cached.exportedAt.Add(time.Second)
	bytes, err := server.prepareResultArchive(context.Background(), snapshot)
	if err != nil {
		snapshot.close()
		t.Fatal(err)
	}
	if bytes != cached.bytes || !snapshot.exportedAt.Equal(cached.exportedAt) {
		snapshot.close()
		t.Fatalf("pending cache did not pin its boundary: bytes=%d snapshot=%s cache=%+v", bytes, snapshot.exportedAt, cached)
	}
	snapshot.close()

	server.resultArchiveMu.Lock()
	cached = server.resultArchives[experiment.ID]
	cached.expiresAt = time.Now().UTC().Add(resultArchiveResponseGrace + 2*time.Second)
	cached.listGraceExtended = false
	server.resultArchives[experiment.ID] = cached
	server.resultArchiveMu.Unlock()
	beforeRefresh := time.Now().UTC()
	response = resultRequest(server, http.MethodGet, "/api/v1/results")
	if response.Code != http.StatusOK {
		t.Fatalf("refresh status=%d body=%s", response.Code, response.Body)
	}
	listed = nil
	if err := json.Unmarshal(response.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	server.resultArchiveMu.Lock()
	extended := server.resultArchives[experiment.ID]
	server.resultArchiveMu.Unlock()
	if !extended.listGraceExtended || extended.expiresAt.Before(beforeRefresh.Add(resultArchiveCacheTTL-time.Second)) {
		t.Fatalf("list did not extend pending cache after preparing all rows: %s", extended.expiresAt)
	}
	if len(listed) != 1 || listed[0].DownloadSizeMaxAgeMS == nil || *listed[0].DownloadSizeMaxAgeMS != maxAdvertisedAgeMS {
		t.Fatalf("list response omitted its extended relative max age: cache=%s list=%s", extended.expiresAt, response.Body)
	}
	firstExtendedExpiry := extended.expiresAt
	firstMaxAgeMS := *listed[0].DownloadSizeMaxAgeMS
	response = resultRequest(server, http.MethodGet, "/api/v1/results")
	if response.Code != http.StatusOK {
		t.Fatalf("second refresh status=%d body=%s", response.Code, response.Body)
	}
	server.resultArchiveMu.Lock()
	notSlid := server.resultArchives[experiment.ID]
	server.resultArchiveMu.Unlock()
	if !notSlid.expiresAt.Equal(firstExtendedExpiry) {
		t.Fatalf("repeated list refresh slid pending cache expiry: %s != %s", notSlid.expiresAt, firstExtendedExpiry)
	}
	listed = nil
	if err := json.Unmarshal(response.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].DownloadSizeMaxAgeMS == nil || *listed[0].DownloadSizeMaxAgeMS <= 0 || *listed[0].DownloadSizeMaxAgeMS > firstMaxAgeMS {
		t.Fatalf("repeated list returned an invalid relative max age: %s", response.Body)
	}

	server.resultArchiveMu.Lock()
	expired := server.resultArchives[experiment.ID]
	expired.expiresAt = time.Now().UTC().Add(resultArchiveResponseGrace / 2)
	server.resultArchives[experiment.ID] = expired
	server.resultArchiveMu.Unlock()
	response = resultRequest(server, http.MethodGet, "/api/v1/results")
	if response.Code != http.StatusOK {
		t.Fatalf("expired refresh status=%d body=%s", response.Code, response.Body)
	}
	listed = nil
	if err := json.Unmarshal(response.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].DownloadBytes != nil || listed[0].DownloadSizeMaxAgeMS != nil {
		t.Fatalf("pending size inside the response grace was returned without HEAD: %s", response.Body)
	}
	server.resultArchiveMu.Lock()
	afterList := server.resultArchives[experiment.ID]
	server.resultArchiveMu.Unlock()
	if !afterList.exportedAt.Equal(expired.exportedAt) {
		t.Fatalf("list remeasured an expired pending archive: %s != %s", afterList.exportedAt, expired.exportedAt)
	}
	head = resultRequest(server, http.MethodHead, "/api/v1/experiments/"+experiment.ID+"/download")
	if head.Code != http.StatusOK || head.Body.Len() != 0 || head.Header().Get("Content-Length") == "" {
		t.Fatalf("near-expiry pending HEAD failed: status=%d headers=%v body=%d", head.Code, head.Header(), head.Body.Len())
	}
	refreshedMaxAgeMS, err := strconv.ParseInt(head.Header().Get(resultSizeMaxAgeHeader), 10, 64)
	if err != nil || refreshedMaxAgeMS <= 0 || refreshedMaxAgeMS > maxAdvertisedAgeMS {
		t.Fatalf("refreshed pending HEAD max age=%q: %v", head.Header().Get(resultSizeMaxAgeHeader), err)
	}
	server.resultArchiveMu.Lock()
	refreshed := server.resultArchives[experiment.ID]
	server.resultArchiveMu.Unlock()
	if refreshed.listGraceExtended || !refreshed.expiresAt.After(expired.expiresAt.Add(3*time.Second)) {
		t.Fatalf("near-expiry pending cache was not remeasured: refreshed=%+v expired=%+v header=%d", refreshed, expired, refreshedMaxAgeMS)
	}
	if !refreshed.expiresAt.After(time.Now().UTC().Add(resultArchiveResponseGrace)) {
		t.Fatalf("refreshed HEAD cache has no server-side response grace: %+v", refreshed)
	}
}

func TestResultArchiveRelativeMaxAgeReservesResponseGrace(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	pending := resultArchiveInfo{
		pendingPublications: 1,
		expiresAt:           now.Add(resultArchiveResponseGrace + 1500*time.Millisecond),
	}
	if maxAgeMS, usable := resultArchiveSizeMaxAgeMS(pending, now); !usable || maxAgeMS != 1500 {
		t.Fatalf("relative max age=(%d, %t), want (1500, true)", maxAgeMS, usable)
	}
	pending.expiresAt = now.Add(resultArchiveResponseGrace + time.Millisecond - time.Nanosecond)
	if maxAgeMS, usable := resultArchiveSizeMaxAgeMS(pending, now); usable || maxAgeMS != 0 {
		t.Fatalf("sub-millisecond max age=(%d, %t), want unusable", maxAgeMS, usable)
	}
}

func TestResultArchiveInfoInvalidatesForChangedFilesAndHonorsCancellation(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	experiment, _ := resultFixture(t, server, "run-changing", "running", time.Now().UTC())
	server.state.experiments[experiment.ID] = experiment
	listed := resultRequest(server, http.MethodGet, "/api/v1/results")
	var activeResults []savedResult
	if err := json.Unmarshal(listed.Body.Bytes(), &activeResults); err != nil {
		t.Fatal(err)
	}
	if len(activeResults) != 1 || activeResults[0].DownloadBytes != nil || activeResults[0].DownloadSizeMaxAgeMS != nil {
		t.Fatalf("active result was measured during list refresh: %+v", activeResults)
	}
	server.resultArchiveMu.Lock()
	_, activeCached := server.resultArchives[experiment.ID]
	server.resultArchiveMu.Unlock()
	if activeCached {
		t.Fatal("active result list populated the archive cache")
	}

	first, err := server.captureResult(experiment.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.prepareResultArchive(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	firstVersion := first.archiveVersion()
	first.close()
	activeDownload := resultRequest(server, http.MethodGet, "/api/v1/experiments/"+experiment.ID+"/download")
	if activeDownload.Code != http.StatusOK || activeDownload.Header().Get("Content-Length") != strconv.Itoa(activeDownload.Body.Len()) {
		t.Fatalf("active download has an inexact size: status=%d headers=%v body=%d", activeDownload.Code, activeDownload.Header(), activeDownload.Body.Len())
	}

	if err := server.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{{
		RunID: experiment.ID, Type: "operation_failed", Timestamp: time.Now().UTC(),
	}}}); err != nil {
		t.Fatal(err)
	}
	changed, err := server.captureResult(experiment.ID)
	if err != nil {
		t.Fatal(err)
	}
	if sameResultArchiveVersion(firstVersion, changed.archiveVersion()) {
		changed.close()
		t.Fatal("an appended event did not invalidate the archive version")
	}
	if _, err := server.prepareResultArchive(context.Background(), changed); err != nil {
		changed.close()
		t.Fatal(err)
	}
	changed.close()

	server.resultArchiveMu.Lock()
	delete(server.resultArchives, experiment.ID)
	server.resultArchiveMu.Unlock()
	canceled, err := server.captureResult(experiment.ID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = server.prepareResultArchive(ctx, canceled)
	canceled.close()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled measurement error=%v", err)
	}
	server.resultArchiveMu.Lock()
	_, cachedAfterCancel := server.resultArchives[experiment.ID]
	server.resultArchiveMu.Unlock()
	if cachedAfterCancel {
		t.Fatal("a canceled archive measurement was cached")
	}
}

func TestCanceledResultDownloadStopsWithoutWritingAnErrorResponse(t *testing.T) {
	var logs bytes.Buffer
	server := New(ServerConfig{DataDir: t.TempDir()}, slog.New(slog.NewTextHandler(&logs, nil)))
	resultFixture(t, server, "run-canceled-download", "completed", time.Now().UTC())
	request := httptest.NewRequest(http.MethodGet, "/api/v1/experiments/run-canceled-download/download", nil)
	ctx, cancel := context.WithCancel(request.Context())
	cancel()
	response := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(response, request.WithContext(ctx))
	if response.Body.Len() != 0 || response.Header().Get("Content-Type") != "" {
		t.Fatalf("canceled download wrote a response: headers=%v body=%s", response.Header(), response.Body)
	}
	if strings.Contains(logs.String(), "measure saved result archive") {
		t.Fatalf("canceled download was logged as a server failure: %s", logs.String())
	}
}

func TestResultArchiveMeasurementSlotsLimitConcurrencyAndHonorCancellation(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	if cap(server.resultArchiveSlots) != resultArchiveMeasureLimit {
		t.Fatalf("archive measurement capacity=%d", cap(server.resultArchiveSlots))
	}
	for range resultArchiveMeasureLimit {
		if err := server.acquireResultArchiveSlot(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := server.acquireResultArchiveSlot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("full measurement semaphore ignored cancellation: %v", err)
	}
	server.releaseResultArchiveSlot()
	if err := server.acquireResultArchiveSlot(context.Background()); err != nil {
		t.Fatal(err)
	}
	for range resultArchiveMeasureLimit {
		server.releaseResultArchiveSlot()
	}
	if len(server.resultArchiveSlots) != 0 {
		t.Fatalf("archive measurement slots leaked: %d", len(server.resultArchiveSlots))
	}
}

func TestResultArchiveVersionIncludesExportedRunState(t *testing.T) {
	base := resultArchiveVersion{active: true, state: "running", storedState: "running"}
	for name, changed := range map[string]resultArchiveVersion{
		"active":       {active: false, state: "running", storedState: "running"},
		"state":        {active: true, state: "completed", storedState: "running"},
		"stored state": {active: true, state: "running", storedState: "completed"},
	} {
		t.Run(name, func(t *testing.T) {
			if sameResultArchiveVersion(base, changed) {
				t.Fatalf("archive version ignored %s", name)
			}
		})
	}
}

func TestConcurrentResultArchiveMeasurementsShareOneBoundary(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	experiment, _ := resultFixture(t, server, "run-concurrent-size", "completed", time.Now().UTC())
	if err := server.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{{
		RunID: experiment.ID, NodeID: "publisher", Type: "publish", Topic: "topic", MessageID: "message", Timestamp: time.Now().UTC(),
		Fields: map[string]any{"measurementDefinition": sessionWindowDefinition, "deliveryWindow": "1h"},
	}}}); err != nil {
		t.Fatal(err)
	}
	const workers = 12
	type measurement struct {
		bytes      int64
		exportedAt time.Time
		err        error
	}
	results := make(chan measurement, workers)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(workers)
	for range workers {
		go func() {
			snapshot, err := server.captureResult(experiment.ID)
			ready.Done()
			if err != nil {
				results <- measurement{err: err}
				return
			}
			defer snapshot.close()
			<-start
			bytes, err := server.prepareResultArchive(context.Background(), snapshot)
			results <- measurement{bytes: bytes, exportedAt: snapshot.exportedAt, err: err}
		}()
	}
	ready.Wait()
	close(start)
	var first measurement
	for index := 0; index < workers; index++ {
		measured := <-results
		if measured.err != nil {
			t.Fatal(measured.err)
		}
		if index == 0 {
			first = measured
			continue
		}
		if measured.bytes != first.bytes || !measured.exportedAt.Equal(first.exportedAt) {
			t.Fatalf("concurrent measurements diverged: first=%+v current=%+v", first, measured)
		}
	}
}

func TestResultSnapshotPinsFileDescriptorsAndEventLengths(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	experiment, original := resultFixture(t, server, "run-active", "running", time.Now().UTC())
	server.state.experiments[experiment.ID] = experiment
	listed := resultRequest(server, http.MethodGet, "/api/v1/results")
	var activeResults []savedResult
	if err := json.Unmarshal(listed.Body.Bytes(), &activeResults); err != nil {
		t.Fatal(err)
	}
	if len(activeResults) != 1 || !activeResults[0].Active || activeResults[0].State != "running" {
		t.Fatalf("current run not marked active: %+v", activeResults)
	}
	first := model.EventBatch{Events: []model.TraceEvent{{RunID: experiment.ID, Type: "before"}}}
	if err := server.state.appendEvents(first); err != nil {
		t.Fatal(err)
	}
	eventPath := filepath.Join(server.config.DataDir, "runs", experiment.ID, "events.jsonl")
	originalEvents, err := os.ReadFile(eventPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := server.captureResult(experiment.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	if err := server.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{{RunID: experiment.ID, Type: "after"}}}); err != nil {
		t.Fatal(err)
	}
	server.updateExperiment(experiment.ID, func(run *model.Experiment) { run.State = "completed" })
	var output bytes.Buffer
	if err := snapshot.writeZIP(context.Background(), &output); err != nil {
		t.Fatal(err)
	}
	files := decodeResultZIP(t, output.Bytes())
	if !bytes.Equal(files["experiment.json"], original) || !bytes.Equal(files["events.jsonl"], originalEvents) {
		t.Fatal("snapshot included replaced metadata or later events")
	}
	if !snapshot.result.Active || snapshot.result.State != "running" {
		t.Fatalf("active state was not captured: %+v", snapshot.result)
	}
}

type blockedResultWriter struct {
	bytes.Buffer
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (writer *blockedResultWriter) Write(data []byte) (int, error) {
	writer.once.Do(func() { close(writer.started); <-writer.release })
	return writer.Buffer.Write(data)
}

func TestResultStreamingDoesNotBlockPersistence(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	experiment, _ := resultFixture(t, server, "run-stream", "running", time.Now().UTC())
	server.state.experiments[experiment.ID] = experiment
	snapshot, err := server.captureResult(experiment.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	writer := &blockedResultWriter{started: make(chan struct{}), release: make(chan struct{})}
	streamed := make(chan error, 1)
	go func() { streamed <- snapshot.writeZIP(context.Background(), writer) }()
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("ZIP did not start streaming")
	}
	persisted := make(chan error, 1)
	go func() {
		err := server.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{{RunID: experiment.ID, Type: "while-downloading"}}})
		server.updateExperiment(experiment.ID, func(run *model.Experiment) { run.State = "completed" })
		persisted <- err
	}()
	select {
	case err := <-persisted:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(5 * time.Second):
		t.Error("slow download blocked event or metadata persistence")
	}
	close(writer.release)
	if err := <-streamed; err != nil {
		t.Fatal(err)
	}
	files := decodeResultZIP(t, writer.Bytes())
	if data, exists := files["events.jsonl"]; !exists || len(data) != 0 {
		t.Fatal("snapshot before the first event must contain empty events.jsonl")
	}
}

func TestResultDownloadRejectsUnreadableFilesBeforeZIPHeaders(t *testing.T) {
	for _, failure := range []string{"missing-metadata", "malformed-metadata", "mismatched-id", "oversized-metadata", "missing-scenario", "directory-events"} {
		t.Run(failure, func(t *testing.T) {
			server := New(ServerConfig{DataDir: t.TempDir()}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			resultFixture(t, server, "run-test", "completed", time.Now().UTC())
			dir := filepath.Join(server.config.DataDir, "runs", "run-test")
			metadata := filepath.Join(dir, "experiment.json")
			var err error
			switch failure {
			case "missing-metadata":
				err = os.Remove(metadata)
			case "malformed-metadata":
				err = os.WriteFile(metadata, []byte("not JSON"), 0o644)
			case "mismatched-id":
				err = os.WriteFile(metadata, []byte(`{"id":"another-run","state":"completed"}`), 0o644)
			case "oversized-metadata":
				err = os.WriteFile(metadata, bytes.Repeat([]byte(" "), resultMetadataLimit+1), 0o644)
			case "missing-scenario":
				err = os.Remove(filepath.Join(dir, "scenario.yaml"))
			case "directory-events":
				err = os.Mkdir(filepath.Join(dir, "events.jsonl"), 0o755)
			}
			if err != nil {
				t.Fatal(err)
			}
			response := resultRequest(server, http.MethodGet, "/api/v1/experiments/run-test/download")
			if response.Code != http.StatusInternalServerError || response.Header().Get("Content-Disposition") != "" || !strings.HasPrefix(response.Header().Get("Content-Type"), "application/json") {
				t.Fatalf("unreadable result response=%d %v %s", response.Code, response.Header(), response.Body)
			}
		})
	}
}

func TestResultSnapshotTruncationCannotProduceSuccessfulZIP(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, server, "run-truncated", "completed", time.Now().UTC())
	path := filepath.Join(server.config.DataDir, "runs", "run-truncated", "events.jsonl")
	if err := os.WriteFile(path, []byte("{\"kind\":\"deliver\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	snapshot, err := server.captureResult("run-truncated")
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := snapshot.writeZIP(context.Background(), &output); err == nil {
		t.Fatal("truncated input was accepted")
	}
	if _, err := zip.NewReader(bytes.NewReader(output.Bytes()), int64(output.Len())); err == nil {
		t.Fatal("failed export produced a valid ZIP")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := snapshot.writeZIP(ctx, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled export error=%v", err)
	}
}

func TestResultExportPreservesAlreadyInterruptedStoredState(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, server, "run-interrupted", "interrupted", time.Now().UTC())
	snapshot, err := server.captureResult("run-interrupted")
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	if snapshot.storedState != "interrupted" || snapshot.result.State != "interrupted" || snapshot.result.Active {
		t.Fatalf("original state was inferred incorrectly: %+v", snapshot)
	}
}

func TestResultMetricsUseTheSamePersistedSnapshotAsExportedEvents(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, server, "run", "completed", time.Now().UTC())
	initial := model.EventBatch{Events: []model.TraceEvent{
		cohortDelivery("first", "topic", "receiver", 12),
		cohortPublish("first", "topic", []string{"receiver"}),
		{RunID: "run", AgentID: "agent-a", NodeID: "sender", EventID: "control-first", Type: "send_ihave", Fields: map[string]any{"controlEntries": 2, "messageIdCount": 6}},
	}}
	if err := server.state.appendEvents(initial); err != nil {
		t.Fatal(err)
	}
	snapshot, err := server.captureResult("run")
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	// A transport retry and a new publication arrive after the file boundary.
	if err := server.state.appendEvents(initial); err != nil {
		t.Fatal(err)
	}
	if err := server.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{
		cohortPublish("later", "topic", []string{"receiver", "departed"}),
		{RunID: "run", AgentID: "agent-a", NodeID: "sender", EventID: "control-later", Type: "recv_iwant", Fields: map[string]any{"controlEntries": 1, "messageIdCount": 2}},
	}}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := snapshot.writeZIP(context.Background(), &output); err != nil {
		t.Fatal(err)
	}
	files := decodeResultZIP(t, output.Bytes())
	var exported model.Metrics
	if err := json.Unmarshal(files["metrics.json"], &exported); err != nil {
		t.Fatal(err)
	}
	if exported.Published != 1 || exported.ExpectedDeliveries != 1 || exported.EligibleDeliveries != 1 || exported.AverageLatencyMS != 12 {
		t.Fatalf("exported metrics included later telemetry: %+v", exported)
	}
	if want := []model.GossipSubControlMetric{{AgentID: "agent-a", Direction: "send", ControlType: "ihave", RPCs: 1, Entries: 2, MessageIDs: 6}}; !reflect.DeepEqual(exported.GossipSubControl, want) {
		t.Fatalf("exported control metrics crossed the snapshot boundary: %+v", exported.GossipSubControl)
	}
	rebuilt, err := summarizeRunEvents("run", bytes.NewReader(files["events.jsonl"]))
	if err != nil {
		t.Fatal(err)
	}
	rebuiltJSON, err := json.MarshalIndent(rebuilt, "", "  ")
	if err != nil || !bytes.Equal(bytes.TrimSpace(files["metrics.json"]), rebuiltJSON) {
		t.Fatalf("metrics do not reproduce the exported event bytes: %s, %v", files["metrics.json"], err)
	}
	server.state.mu.RLock()
	live, _ := server.state.runMetrics["run"].summarize("run")
	server.state.mu.RUnlock()
	if live.Published != 2 || live.ExpectedDeliveries != 3 || len(live.GossipSubControl) != 2 {
		t.Fatalf("later telemetry was not processed independently: %+v", live)
	}
}

func TestResultDownloadRejectsSymlinksAndUnsafeIDs(t *testing.T) {
	for _, target := range []string{"run", "scenario.yaml", "experiment.json", "events.jsonl", "runs"} {
		t.Run(target, func(t *testing.T) {
			server := New(ServerConfig{DataDir: t.TempDir()}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			resultFixture(t, server, "run-safe", "completed", time.Now().UTC())
			outside := t.TempDir()
			secret := filepath.Join(outside, "private")
			if err := os.WriteFile(secret, []byte("PRIVATE CONTENT"), 0o600); err != nil {
				t.Fatal(err)
			}
			runs := filepath.Join(server.config.DataDir, "runs")
			path := filepath.Join(runs, "run-safe", target)
			if target == "run" {
				path = filepath.Join(runs, "run-link")
				secret = filepath.Join(runs, "run-safe")
			} else if target == "runs" {
				path = runs
				if err := os.Rename(runs, runs+"-original"); err != nil {
					t.Fatal(err)
				}
				secret = runs + "-original"
			} else if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if err := os.Symlink(secret, path); err != nil {
				t.Skipf("symlink creation unavailable: %v", err)
			}
			id := "run-safe"
			if target == "run" {
				id = "run-link"
			}
			response := resultRequest(server, http.MethodGet, "/api/v1/experiments/"+id+"/download")
			if response.Code == http.StatusOK || response.Header().Get("Content-Disposition") != "" || strings.Contains(response.Body.String(), "PRIVATE CONTENT") {
				t.Fatalf("symlink exported: %d %s", response.Code, response.Body)
			}
		})
	}
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	for _, id := range []string{"..", "../secret", `..\secret`, "/absolute", "C:private", "run.other", "", strings.Repeat("x", 129)} {
		recorder := httptest.NewRecorder()
		server.handleResultDownload(recorder, httptest.NewRequest(http.MethodGet, "/", nil), id)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("unsafe id %q status=%d", id, recorder.Code)
		}
	}
}

func TestResultsMethodsEmptyListMissingRunAndExistingStop(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	response := resultRequest(server, http.MethodGet, "/api/v1/results")
	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != "[]" {
		t.Fatalf("empty list=%d %s", response.Code, response.Body)
	}
	for _, request := range []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/api/v1/experiments/missing/download", http.StatusNotFound},
		{http.MethodPost, "/api/v1/results", http.StatusMethodNotAllowed},
		{http.MethodPost, "/api/v1/experiments/missing/download", http.StatusMethodNotAllowed},
		{http.MethodGet, "/api/v1/experiments/missing/stop", http.StatusMethodNotAllowed},
		{http.MethodGet, "/api/v1/experiments/missing/unknown", http.StatusNotFound},
	} {
		response := resultRequest(server, request.method, request.path)
		if response.Code != request.status {
			t.Fatalf("%s %s = %d, want %d", request.method, request.path, response.Code, request.status)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server.cancels["run-stop"] = cancel
	response = resultRequest(server, http.MethodPost, "/api/v1/experiments/run-stop/stop")
	if response.Code != http.StatusAccepted || ctx.Err() != context.Canceled {
		t.Fatalf("existing stop action failed: status=%d error=%v", response.Code, ctx.Err())
	}
}

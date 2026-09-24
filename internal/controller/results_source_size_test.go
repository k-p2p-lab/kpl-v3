package controller

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func readSourceSizeResult(t *testing.T, server *Server) savedResult {
	t.Helper()
	response := resultRequest(server, http.MethodGet, "/api/v1/results")
	var results []savedResult
	if response.Code != http.StatusOK {
		t.Fatalf("list: %d %s", response.Code, response.Body)
	}
	if err := json.Unmarshal(response.Body.Bytes(), &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("unexpected result count: %d", len(results))
	}
	if len(server.resultArchives) != 0 {
		t.Fatal("listing prepared a ZIP")
	}
	return results[0]
}

func TestSavedResultSourceBytesUseStatsWithoutReadingLargeLogs(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	experiment, _ := resultFixture(t, server, "large-source", "completed", time.Now().UTC())
	dir := filepath.Join(server.config.DataDir, "runs", experiment.ID)
	// A large sparse, invalid JSONL log is still sizeable without decoding it,
	// walking its contents, reconstructing metrics, or running a ZIP encoder.
	file, err := os.Create(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(16 << 30); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "observations.jsonl"), []byte("observation\n"), 0600); err != nil {
		t.Fatal(err)
	}
	// Generated analysis files and unrelated files are not source inputs.
	if err := os.WriteFile(filepath.Join(dir, "metrics.json"), make([]byte, 4096), 0600); err != nil {
		t.Fatal(err)
	}
	var want int64
	for _, name := range []string{"scenario.yaml", "experiment.json", "events.jsonl", "observations.jsonl"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		want += info.Size()
	}
	first := readSourceSizeResult(t, server)
	if first.SourceBytes == nil || *first.SourceBytes != want {
		t.Fatalf("source bytes: %+v, want %d", first.SourceBytes, want)
	}
	file, err = os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("appended\n"); err != nil {
		file.Close()
		t.Fatal(err)
	}
	file.Close()
	next := readSourceSizeResult(t, server)
	if next.SourceBytes == nil || *next.SourceBytes != want+9 {
		t.Fatalf("list did not refresh appended bytes: %+v", next.SourceBytes)
	}
}

func TestSourceBytesAvailableForRunningQueuedInterruptedAndUnreadableResults(t *testing.T) {
	for _, state := range []string{"running", "queued", "interrupted", "completed", "unreadable"} {
		t.Run(state, func(t *testing.T) {
			server := New(ServerConfig{DataDir: t.TempDir()}, nil)
			experiment, _ := resultFixture(t, server, "source", state, time.Now().UTC())
			if state == "running" || state == "queued" {
				server.state.experiments[experiment.ID] = model.Experiment{ID: experiment.ID, State: state}
			}
			dir := filepath.Join(server.config.DataDir, "runs", experiment.ID)
			if state == "unreadable" {
				if err := os.WriteFile(filepath.Join(dir, "experiment.json"), []byte("invalid metadata"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			result := readSourceSizeResult(t, server)
			if result.SourceBytes == nil || *result.SourceBytes <= 0 {
				t.Fatalf("missing source size for %s", state)
			}
			if state == "unreadable" && result.State != "unreadable" {
				t.Fatal("unreadable metadata was hidden")
			}
			if result.DownloadBytes != nil {
				t.Fatal("list prepared an archive size")
			}
		})
	}
}

func TestSourceSizeDoesNotFollowLinksOrTrustStoredSize(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	experiment, _ := resultFixture(t, server, "source", "completed", time.Now().UTC())
	dir := filepath.Join(server.config.DataDir, "runs", experiment.ID)
	metadata := []byte(`{"id":"source","state":"completed","sourceBytes":1}`)
	if err := os.WriteFile(filepath.Join(dir, "experiment.json"), metadata, 0600); err != nil {
		t.Fatal(err)
	}
	result := readSourceSizeResult(t, server)
	if result.SourceBytes == nil || *result.SourceBytes <= int64(len(metadata)) {
		t.Fatal("trusted stored size instead of source file stats")
	}
	if err := os.Symlink(filepath.Join(dir, "experiment.json"), filepath.Join(dir, "events.jsonl")); err != nil {
		t.Fatal(err)
	}
	result = readSourceSizeResult(t, server)
	if result.SourceBytes != nil {
		t.Fatal("followed source symlink or returned a partial size")
	}
}

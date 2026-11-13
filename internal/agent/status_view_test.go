package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAgentOnlyStatusOmitsRetainedPeerPayloads(t *testing.T) {
	s := historyTestServer(t)
	s.config.RunWorkerFactory = agentRunProcessFactory()
	s.startupReconciled = true
	const count = 1024
	for i := range count {
		id := fmt.Sprintf("retained-%04d", i)
		proc := historyTerminal(id, "finished-run", time.Now().UTC())
		proc.node.Metadata["retainedEvidence"] = strings.Repeat("peer-metadata", 256)
		s.processes[id] = proc
	}
	api := httptest.NewServer(s.Handler())
	defer api.Close()
	for _, view := range []string{"?view=agent", "", "?view=other"} {
		response, err := api.Client().Get(api.URL + "/api/v1/status" + view)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("status %q: code=%d read=%v", view, response.StatusCode, err)
		}
		var report model.AgentHeartbeat
		if err := json.Unmarshal(body, &report); err != nil {
			t.Fatal(err)
		}
		t.Logf("status %q: nodes=%d bytes=%d partial=%t", view, len(report.Nodes), len(body), report.Partial)
		if report.Agent.ID != s.config.ID || !report.Agent.StartedAt.Equal(s.startedAt) || !report.Agent.RunDrain || !report.Agent.RunProcesses || !report.Agent.StartupReconciled || report.Agent.ActiveNodes != 0 {
			t.Fatalf("status %q lost current Agent identity or capabilities: %+v", view, report.Agent)
		}
		if view == "?view=agent" {
			if !report.Partial || len(report.Nodes) != 0 || len(body) > 4096 || strings.Contains(string(body), "retainedEvidence") {
				t.Fatalf("Agent-only status copied retained Peer payloads: partial=%t nodes=%d bytes=%d", report.Partial, len(report.Nodes), len(body))
			}
		} else if report.Partial || len(report.Nodes) != count {
			t.Fatalf("full status %q changed inventory semantics: partial=%t nodes=%d", view, report.Partial, len(report.Nodes))
		}
	}
	request, _ := http.NewRequest(http.MethodPost, api.URL+"/api/v1/status?view=agent", nil)
	response, err := api.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("Agent-only view accepted a mutation method: %d", response.StatusCode)
	}
}

func TestAgentOnlyStatusDoesNotWaitForRunWorkerOrHistory(t *testing.T) {
	s := historyTestServer(t)
	s.config.RunWorkerFactory = agentRunProcessFactory()
	s.processes["active"] = &process{node: model.Node{ID: "active", RunID: "run", State: model.NodeReady}}
	if err := os.WriteFile(filepath.Join(s.history.directory, "nodes"), []byte(strings.Repeat("corrupt history", 1024)), 0600); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(s.Handler())
	defer api.Close()
	client := api.Client()
	client.Timeout = time.Second
	// An identity check must still work while history storage and the worker
	// registry are unavailable. No worker process is started in this fixture.
	s.history.mu.Lock()
	s.history.storeMu.Lock()
	s.runWorkersMu.Lock()
	func() {
		defer s.history.mu.Unlock()
		defer s.history.storeMu.Unlock()
		defer s.runWorkersMu.Unlock()
		response, err := client.Get(api.URL + "/api/v1/status?view=agent")
		if err != nil {
			t.Errorf("Agent identity waited for Peer runtime or history: %v", err)
			return
		}
		defer response.Body.Close()
		var report model.AgentHeartbeat
		if err := json.NewDecoder(response.Body).Decode(&report); err != nil {
			t.Error(err)
			return
		}
		if response.StatusCode != http.StatusOK || !report.Partial || len(report.Nodes) != 0 || report.Agent.ActiveNodes != 1 || report.Agent.ID != s.config.ID {
			t.Errorf("Agent-only response lost the active reservation: code=%d report=%+v", response.StatusCode, report)
		}
	}()
	if _, err := s.history.store(); err == nil {
		t.Fatal("fixture history corruption was not detected")
	}
	response, err := client.Get(api.URL + "/api/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("full status hid an unavailable active Run snapshot: %d", response.StatusCode)
	}
}

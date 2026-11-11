package controller

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAnalysisAgentCleanupPersistsInstanceAndUsesAgentCredential(t *testing.T) {
	config := ServerConfig{DataDir: t.TempDir(), AnalysisAgentToken: remoteTestToken}
	s := New(config, nil)
	worker := newRemoteAnalysisWorker(remoteTestToken, s.logger, nil)
	endpoint := httptest.NewServer(worker)
	defer endpoint.Close()
	original := analysisPoolAgent("agent-original", "host-original", endpoint.URL)
	if err := s.trackAnalysisWorker(t.Context(), endpoint.URL, original); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(config.DataDir, analysisWorkerCleanupFile))
	if err != nil {
		t.Fatal(err)
	}
	var ledger analysisWorkerCleanupLedger
	if err := json.Unmarshal(data, &ledger); err != nil || ledger.Version != 1 || len(ledger.Workers) != 1 {
		t.Fatalf("invalid durable Agent ownership: %s, %v", data, err)
	}
	record := ledger.Workers[0]
	if record.AgentID != original.ID || record.HostID != "host-original" || !record.StartedAt.Equal(original.StartedAt) {
		t.Fatalf("Agent instance identity was lost: %+v", record)
	}
	restarted := New(config, nil)
	registerAnalysisPoolAgent(t, restarted, original)
	if err := restarted.waitAnalysisWorkerCleanup(t.Context(), &workSlot{id: "next"}); err != nil {
		t.Fatal(err)
	}
	if records, err := restarted.pendingAnalysisWorkerCleanup(); err != nil || len(records) != 0 {
		t.Fatalf("authenticated original idle proof did not clear ownership: %v, %v", records, err)
	}
}

func TestAnalysisAgentCleanupVerifiesReplacementHostAndStartup(t *testing.T) {
	config := ServerConfig{DataDir: t.TempDir(), AnalysisAgentToken: remoteTestToken}
	s := New(config, nil)
	original := analysisPoolAgent("old-agent", "host-original", "http://replaced-task:9091")
	if err := s.trackAnalysisWorker(t.Context(), original.URL, original); err != nil {
		t.Fatal(err)
	}
	restarted := New(config, nil)
	records, err := restarted.pendingAnalysisWorkerCleanupRecords()
	if err != nil || len(records) != 1 {
		t.Fatalf("restore Agent cleanup: %v %v", records, err)
	}
	record := records[0]
	for _, tc := range []struct {
		name string
		edit func(*model.Agent)
	}{
		{"same hostname on another host", func(agent *model.Agent) {
			agent.Labels["swarmNodeId"] = "other-host"
			agent.Host.NodeID = "other-host"
			agent.Hostname = original.Hostname
		}},
		{"unknown host", func(agent *model.Agent) {
			agent.Labels = nil
			agent.Host = nil
			agent.Hostname = original.Hostname
		}},
		{"contradictory Swarm host label", func(agent *model.Agent) { agent.Labels["swarmNodeId"] = "other-host" }},
		{"same start with another Agent ID", func(agent *model.Agent) { agent.StartedAt = original.StartedAt }},
		{"unreconciled startup", func(agent *model.Agent) { agent.StartupReconciled = false }},
		{"unsupported analysis", func(agent *model.Agent) { agent.AnalysisProcesses = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := analysisPoolAgent("candidate", "host-original", "http://candidate:9091")
			candidate.StartedAt = original.StartedAt.Add(time.Second)
			tc.edit(&candidate)
			restarted.state.mu.Lock()
			restarted.state.agents = map[string]model.Agent{candidate.ID: candidate}
			restarted.state.mu.Unlock()
			if endpoint, _, err := restarted.analysisWorkerCleanupTarget(record); err == nil {
				t.Fatalf("unverified replacement authorized cleanup through %s", endpoint)
			}
			if pending, err := restarted.pendingAnalysisWorkerCleanup(); err != nil || len(pending) != 1 {
				t.Fatalf("unknown replacement lost ownership: %v %v", pending, err)
			}
		})
	}
	worker := newRemoteAnalysisWorker(remoteTestToken, s.logger, nil)
	endpoint := httptest.NewServer(worker)
	defer endpoint.Close()
	replacement := analysisPoolAgent("new-agent", "host-original", endpoint.URL)
	replacement.StartedAt = original.StartedAt.Add(time.Second)
	registerAnalysisPoolAgent(t, restarted, replacement)
	if err := restarted.waitAnalysisWorkerCleanup(t.Context(), &workSlot{id: "next"}); err != nil {
		t.Fatal(err)
	}
	if pending, err := restarted.pendingAnalysisWorkerCleanup(); err != nil || len(pending) != 0 {
		t.Fatalf("verified same-host replacement did not resolve old endpoint: %v %v", pending, err)
	}
}

func TestAnalysisWorkerCleanupReadsLegacyDedicatedEndpoints(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), AnalysisWorkerToken: remoteTestToken}, nil)
	worker := newRemoteAnalysisWorker(remoteTestToken, s.logger, nil)
	endpoint := httptest.NewServer(worker)
	defer endpoint.Close()
	legacy, err := json.Marshal([]string{endpoint.URL})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.config.DataDir, analysisWorkerCleanupFile), legacy, 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.waitAnalysisWorkerCleanup(t.Context(), &workSlot{id: "next"}); err != nil {
		t.Fatal(err)
	}
	if pending, err := s.pendingAnalysisWorkerCleanup(); err != nil || len(pending) != 0 {
		t.Fatalf("legacy dedicated worker was not cleared after idle proof: %v %v", pending, err)
	}
}

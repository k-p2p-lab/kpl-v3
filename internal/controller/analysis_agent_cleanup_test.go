package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
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

func TestAnalysisAgentCleanupAdmissionRequiresVerifiedInstance(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), AnalysisAgentToken: remoteTestToken}, nil)
	original := analysisPoolAgent("registered-agent", "registered-host", "http://agent:9091")
	for _, check := range []struct {
		name string
		edit func(*model.Agent)
	}{
		{"missing Agent", func(agent *model.Agent) { *agent = model.Agent{} }},
		{"different endpoint", func(agent *model.Agent) { agent.URL = "http://another-agent:9091" }},
		{"missing host", func(agent *model.Agent) { agent.Host = nil; agent.Labels = nil }},
		{"missing process identity", func(agent *model.Agent) { agent.StartedAt = time.Time{} }},
		{"contradictory host identity", func(agent *model.Agent) { agent.Labels["swarmNodeId"] = "different-host" }},
	} {
		t.Run(check.name, func(t *testing.T) {
			agent := analysisPoolAgent(original.ID, original.Host.NodeID, original.URL)
			check.edit(&agent)
			if err := s.trackAnalysisWorker(t.Context(), original.URL, agent); err == nil {
				t.Fatal("analysis admitted an endpoint without a verified Agent instance")
			}
			if pending, err := s.pendingAnalysisWorkerCleanup(); err != nil || len(pending) != 0 {
				t.Fatalf("failed admission created cleanup ownership: %v %v", pending, err)
			}
		})
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

func TestAnalysisAgentCleanupRetainsUnverifiedLegacyEndpoints(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), AnalysisAgentToken: remoteTestToken}, nil)
	var probes atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer endpoint.Close()
	legacy, err := json.Marshal([]string{endpoint.URL})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.config.DataDir, analysisWorkerCleanupFile), legacy, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := s.waitAnalysisWorkerCleanup(ctx, &workSlot{id: "next"}); err == nil {
		t.Fatal("legacy endpoint without verified retirement released the experiment barrier")
	}
	if pending, err := s.pendingAnalysisWorkerCleanup(); err != nil || len(pending) != 1 || pending[0] != endpoint.URL {
		t.Fatalf("unverified legacy ownership was lost: %v %v", pending, err)
	}
	if probes.Load() != 0 {
		t.Fatal("Agent credentials were sent to an unverified legacy endpoint")
	}
}

func TestAnalysisAgentCleanupRetiresOnlyProvenLegacyEndpoints(t *testing.T) {
	const endpoint = "http://analysis-worker-1:8093"
	for _, agentOwned := range []bool{false, true} {
		name := "legacy service"
		if agentOwned {
			name = "Agent instance"
		}
		t.Run(name, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir(), AnalysisAgentToken: remoteTestToken, RetiredAnalysisEndpoints: []string{endpoint}}, nil)
			var document any = []string{endpoint}
			if agentOwned {
				agent := analysisPoolAgent("owned-agent", "owned-host", endpoint)
				document = analysisWorkerCleanupLedger{Version: 1, Workers: []analysisWorkerCleanupRecord{{Endpoint: endpoint, AgentID: agent.ID, HostID: agentHostNodeID(agent), StartedAt: agent.StartedAt}}}
			}
			data, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(s.config.DataDir, analysisWorkerCleanupFile), data, 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			err = s.waitAnalysisWorkerCleanup(ctx, &workSlot{id: "next"})
			pending, readErr := s.pendingAnalysisWorkerCleanup()
			if readErr != nil {
				t.Fatal(readErr)
			}
			if agentOwned {
				if err == nil || len(pending) != 1 {
					t.Fatalf("legacy service retirement released live Agent ownership: pending=%v error=%v", pending, err)
				}
			} else if err != nil || len(pending) != 0 {
				t.Fatalf("verified retired service blocked the analysis queue: pending=%v error=%v", pending, err)
			}
		})
	}
}

func TestAnalysisAgentCleanupRejectsMissingForeignAndInvalidRetirementEvidence(t *testing.T) {
	const endpoint = "http://analysis-worker-1:8093"
	tooMany := make([]string, 17)
	for index := range tooMany {
		tooMany[index] = fmt.Sprintf("http://analysis-worker-%d:8093", index+1)
	}
	for _, check := range []struct {
		name      string
		endpoints []string
	}{
		{"missing", nil},
		{"different service", []string{"http://analysis-worker-2:8093"}},
		{"foreign host", []string{"http://another-host:8093"}},
		{"noncanonical endpoint", []string{endpoint + "/"}},
		{"duplicate evidence", []string{endpoint, endpoint}},
		{"excessive evidence", tooMany},
	} {
		t.Run(check.name, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir(), AnalysisAgentToken: remoteTestToken, RetiredAnalysisEndpoints: check.endpoints}, nil)
			data, err := json.Marshal([]string{endpoint})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(s.config.DataDir, analysisWorkerCleanupFile), data, 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			if err := s.waitAnalysisWorkerCleanup(ctx, &workSlot{id: "next"}); err == nil {
				t.Fatal("unverified deployment evidence released the experiment barrier")
			}
			if pending, err := s.pendingAnalysisWorkerCleanup(); err != nil || len(pending) != 1 || pending[0] != endpoint {
				t.Fatalf("unverified deployment evidence removed ownership: pending=%v error=%v", pending, err)
			}
		})
	}
}

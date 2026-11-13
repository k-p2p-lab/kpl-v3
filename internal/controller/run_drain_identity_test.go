package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestRunWorkerDrainRequestsAgentOnlyIdentity(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%t", legacy), func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			agent := model.Agent{ID: "owner", StartedAt: time.Now().Add(-time.Minute).UTC(), RunDrain: true, RunProcesses: true}
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/api/v1/status" || r.URL.Query().Get("view") != "agent" {
					t.Errorf("identity verification requested a full Peer inventory: %s %s", r.Method, r.URL)
				}
				report := model.AgentHeartbeat{Agent: agent, Partial: !legacy}
				if legacy {
					// Older Agents ignore the query and still return a full view.
					report.Nodes = []model.Node{{ID: "retained-peer", RunID: "previous-run", State: model.NodeStopped}}
				}
				writeJSON(w, http.StatusOK, report)
			}))
			defer endpoint.Close()
			agent.URL = endpoint.URL
			if err := s.confirmRunWorkerDrain(t.Context(), agent); err != nil {
				t.Fatalf("matching identity rejected: %v", err)
			}
		})
	}
}

// Retained pending/recent Peer records enlarge a full status response even
// though drain confirmation uses only the Agent instance and capabilities.
func BenchmarkRunDrainIdentityStatus(b *testing.B) {
	for _, count := range []int{0, 1024} {
		b.Run(fmt.Sprintf("retained-peers=%d", count), func(b *testing.B) {
			agent := model.Agent{ID: "owner", StartedAt: time.Unix(1, 0).UTC(), RunDrain: true, RunProcesses: true}
			full := model.AgentHeartbeat{Agent: agent}
			for i := range count {
				full.Nodes = append(full.Nodes, model.Node{ID: fmt.Sprintf("retained-%04d", i), RunID: "previous-run", AgentID: agent.ID, State: model.NodeStopped, Metadata: map[string]string{"cleanupComplete": "true"}})
			}
			fullPayload, err := json.Marshal(full)
			if err != nil {
				b.Fatal(err)
			}
			compactPayload, err := json.Marshal(model.AgentHeartbeat{Agent: agent, Partial: true})
			if err != nil {
				b.Fatal(err)
			}
			var payloadBytes atomic.Int64
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload := fullPayload
				if r.URL.Query().Get("view") == "agent" {
					payload = compactPayload
				}
				w.Header().Set("Content-Type", "application/json")
				n, _ := w.Write(payload)
				payloadBytes.Add(int64(n))
			}))
			defer endpoint.Close()
			s := New(ServerConfig{DataDir: b.TempDir()}, nil)
			agent.URL = endpoint.URL
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := s.confirmRunWorkerDrain(b.Context(), agent); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(payloadBytes.Load())/float64(b.N), "payload-bytes/op")
		})
	}
}

package controller

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func churnHistoryController(b *testing.B, history int) *Server {
	b.Helper()
	s := New(ServerConfig{DataDir: b.TempDir()}, nil)
	now := time.Now()
	s.state.agents["agent"] = model.Agent{ID: "agent", State: model.AgentOnline, LastSeen: now}
	for i := 0; i < history; i++ {
		id := fmt.Sprintf("old-%06d", i)
		s.state.setNodeLocked(model.Node{ID: id, RunID: "previous", AgentID: "agent", State: model.NodeStopped})
	}
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("live-%03d", i)
		s.state.setNodeLocked(model.Node{ID: id, RunID: "current", AgentID: "agent", PeerID: "peer-" + id,
			Role: "boot", State: model.NodeReady, LastSeen: now, Addresses: []string{"/ip4/10.0.0.1/tcp/20000"}})
	}
	return s
}

func BenchmarkBootstrapChurnHistory(b *testing.B) {
	for _, history := range []int{0, 65000} {
		b.Run(fmt.Sprintf("history=%d", history), func(b *testing.B) {
			s := churnHistoryController(b, history)
			request := httptest.NewRequest("GET", "/api/v1/bootstrap?runId=current", nil)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				s.handleBootstrap(httptest.NewRecorder(), request)
			}
		})
	}
}

func BenchmarkDashboardChurnHistory(b *testing.B) {
	for _, history := range []int{0, 65000} {
		b.Run(fmt.Sprintf("history=%d", history), func(b *testing.B) {
			s := churnHistoryController(b, history)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := newDashboardFrame(s.state.dashboardSnapshot(), time.Now()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

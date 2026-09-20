package controller

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
)

func TestEventBatchRejectsTrailingJSONBeforePersistence(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	handler := server.apiTestHandler(context.Background())
	for _, suffix := range []string{` {}`, ` garbage`, strings.Repeat(" ", 10<<20)} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/events/batch", strings.NewReader(`{"events":[{"runId":"invalid-body","eventId":"must-not-persist"}]}`+suffix)))
		if response.Code != 400 {
			t.Errorf("invalid body accepted: status=%d body=%s", response.Code, response.Body.String())
		}
	}
	if _, err := os.Stat(filepath.Join(server.config.DataDir, "runs", "invalid-body", "events.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("invalid request persisted data: %v", err)
	}
}

func TestAnalysisScoreMeanRemainsFiniteForFiniteInputs(t *testing.T) {
	now := time.Now().UTC()
	agents := map[string]model.Agent{"agent": {ID: "agent", State: model.AgentOnline, LastSeen: now}}
	for _, scores := range [][]float64{{math.MaxFloat64, math.MaxFloat64}, {-math.MaxFloat64, -math.MaxFloat64}, {math.MaxFloat64, -math.MaxFloat64}} {
		nodes := []model.Node{}
		for i, score := range scores {
			id := string(rune('a' + i))
			nodes = append(nodes, model.Node{ID: id, PeerID: id, AgentID: "agent", State: model.NodeReady, LastSeen: now, PeerScores: map[string]float64{"other": score}})
		}
		observation := makeAnalysisObservation("run", now, nodes, agents)
		if _, err := json.Marshal(observation); err != nil {
			t.Errorf("finite scores lost entire observation: %v", err)
			continue
		}
		mean := observation.Groups[0].ScoreMean
		want := scores[0]/2 + scores[1]/2
		if mean == nil || *mean != want {
			t.Errorf("mean=%v want %v", mean, want)
		}
	}
}

func TestEventBatchRejectsUnboundedCanonicalEventBeforePersistence(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	// This request is below 10 MiB, but JSON escaping would persist a line above
	// the archive reader's 16 MiB limit. The earlier valid event must not commit.
	body := `{"events":[{"runId":"oversize","eventId":"valid-prefix"},{"runId":"oversize","fields":{"padding":"` + strings.Repeat("<", 3<<20) + `"}}]}`
	response := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/events/batch", strings.NewReader(body)))
	if response.Code != 413 {
		t.Errorf("unreadable event accepted: status=%d", response.Code)
	}
	if _, err := os.Stat(filepath.Join(server.config.DataDir, "runs", "oversize", "events.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("rejected batch partially persisted: %v", err)
	}
}

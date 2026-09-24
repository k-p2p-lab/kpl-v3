package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func applyDashboardTestPatch(t *testing.T, base, data []byte) []byte {
	t.Helper()
	var snapshot, patch map[string]json.RawMessage
	if err := json.Unmarshal(base, &snapshot); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &patch); err != nil {
		t.Fatal(err)
	}
	for key, value := range patch {
		if key != "agents" && key != "nodes" && key != "experiments" {
			snapshot[key] = value
			continue
		}
		var rows []json.RawMessage
		var changes struct {
			Upsert []json.RawMessage `json:"upsert"`
			Remove []string          `json:"remove"`
			Order  *[]string         `json:"order"`
		}
		if err := json.Unmarshal(snapshot[key], &rows); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(value, &changes); err != nil {
			t.Fatal(err)
		}
		items := make(map[string]json.RawMessage)
		order := []string{}
		for _, row := range rows {
			var identity struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(row, &identity); err != nil {
				t.Fatal(err)
			}
			items[identity.ID] = row
			order = append(order, identity.ID)
		}
		for _, id := range changes.Remove {
			delete(items, id)
		}
		for _, row := range changes.Upsert {
			var identity struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(row, &identity); err != nil {
				t.Fatal(err)
			}
			if _, ok := items[identity.ID]; !ok {
				order = append(order, identity.ID)
			}
			items[identity.ID] = row
		}
		if changes.Order != nil {
			order = *changes.Order
		}
		rows = []json.RawMessage{}
		for _, id := range order {
			if row, ok := items[id]; ok {
				rows = append(rows, row)
			}
		}
		encoded, err := json.Marshal(rows)
		if err != nil {
			t.Fatal(err)
		}
		snapshot[key] = encoded
	}
	result, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestDashboardSnapshotBoundsEventsWithoutChangingSource(t *testing.T) {
	original := model.Snapshot{Nodes: []model.Node{
		{ID: "live", State: model.NodeReady, PeerScores: map[string]float64{"peer": 1.5}},
		{ID: "starting", State: model.NodeStarting},
		{ID: "stopping", State: model.NodeStopping},
		{ID: "failed", State: model.NodeFailed},
		{ID: "stopped", State: model.NodeStopped},
	}}
	for i := 0; i < recentEventLimit; i++ {
		original.Events = append(original.Events, model.TraceEvent{
			NodeID: fmt.Sprint(i), RunID: "run", Type: "gossipsub_ihave", RemotePeerID: "peer", LatencyMS: 2,
			Fields:    map[string]any{"messageIds": []string{strings.Repeat("a", 100000)}, "direction": "send", "controlType": "IHAVE", "messageIdCount": 200, "controlEntries": 1, "latencyAvailable": false},
			Bandwidth: &model.BandwidthSample{},
		})
	}
	before, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	projected := dashboardSnapshot(original)
	if len(projected.Nodes) != 2 || projected.Nodes[0].PeerScores["peer"] != 1.5 {
		t.Fatal("live topology was lost or terminated nodes retained")
	}
	if len(projected.Events) != 40 || projected.Events[0].NodeID != "260" {
		t.Fatal("dashboard did not retain the latest 40 events")
	}
	for _, event := range projected.Events {
		if event.Bandwidth != nil || len(event.Fields) != 5 || event.Fields["messageIdCount"] != 200 || event.Fields["latencyAvailable"] != false {
			t.Fatalf("incorrect summary: %+v", event)
		}
	}
	after, _ := json.Marshal(original)
	if string(before) != string(after) {
		t.Fatal("projection mutated source records used by REST/export/analysis")
	}
}

func TestDashboardDeltaReconstructsSkippedFramesAndRemovals(t *testing.T) {
	at := time.Now().UTC()
	base := model.Snapshot{
		GeneratedAt: at,
		Agents:      []model.Agent{{ID: "a", State: model.AgentOnline}},
		Nodes:       []model.Node{{ID: "n1", State: model.NodeReady}, {ID: "n2", State: model.NodeReady}},
		Experiments: []model.Experiment{{ID: "r1", State: "running"}, {ID: "r2", State: "queued"}},
		Events:      []model.TraceEvent{}, Edges: []model.Edge{{Source: "n1", Target: "n2"}},
	}
	first, err := newDashboardFrame(base, at)
	if err != nil {
		t.Fatal(err)
	}
	base.GeneratedAt = at.Add(time.Second)
	unchanged, _ := newDashboardFrame(base, base.GeneratedAt)
	if data, err := unchanged.delta(first); err != nil || data != nil {
		t.Fatalf("timestamp-only changes repeated the snapshot: %s %v", data, err)
	}
	// Skip several intermediate frames, including a run reorder and node removal.
	base.GeneratedAt = at.Add(10 * time.Second)
	base.Nodes = []model.Node{{ID: "n2", State: model.NodeReady, PeerScores: map[string]float64{"p": 2}}, {ID: "n3", State: model.NodeStarting}}
	base.Experiments = []model.Experiment{{ID: "r2", State: "running"}, {ID: "r1", State: "failed"}}
	base.Edges = []model.Edge{}
	base.Events = []model.TraceEvent{{Type: "new", NodeID: "n3"}}
	base.Metrics.RunID = "r2"
	last, err := newDashboardFrame(base, base.GeneratedAt)
	if err != nil {
		t.Fatal(err)
	}
	patch, err := last.delta(first)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(patch), `"agents"`) {
		t.Fatal("unchanged agents retransmitted")
	}
	var got, want any
	if err := json.Unmarshal(applyDashboardTestPatch(t, first.data, patch), &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(last.data, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("delta did not reconstruct full snapshot: %s", patch)
	}
	base.Nodes, base.Experiments, base.Agents = []model.Node{}, []model.Experiment{}, []model.Agent{}
	empty, _ := newDashboardFrame(base, base.GeneratedAt)
	patch, _ = empty.delta(last)
	if err := json.Unmarshal(applyDashboardTestPatch(t, last.data, patch), &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(empty.data, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("empty collections were not cleared: %s", patch)
	}
}

func TestDashboardStreamTrafficBudget(t *testing.T) {
	// Approximate the reported 4 MB/second trace-heavy workload, without relying
	// on private production data. Change a live heartbeat and event each second.
	ids := make([]string, 200)
	for i := range ids {
		ids[i] = fmt.Sprintf("%064x", i)
	}
	snapshot := model.Snapshot{
		GeneratedAt: time.Now().UTC(),
		Nodes:       []model.Node{{ID: "live", State: model.NodeReady}},
		Experiments: []model.Experiment{{ID: "run", State: "running"}},
	}
	for i := 0; i < recentEventLimit; i++ {
		snapshot.Events = append(snapshot.Events, model.TraceEvent{RunID: "run", NodeID: "live", Type: "gossipsub_ihave", Timestamp: snapshot.GeneratedAt,
			Fields: map[string]any{"messageIds": ids, "direction": "send", "controlType": "IHAVE", "controlEntries": 1, "messageIdCount": len(ids)}})
	}
	var previous *dashboardFrame
	var rawBytes, dashboardBytes int
	for tick := 0; tick <= 20; tick++ {
		snapshot.GeneratedAt = snapshot.GeneratedAt.Add(time.Second)
		snapshot.Nodes[0].LastSeen = snapshot.GeneratedAt
		snapshot.Events[len(snapshot.Events)-1].Timestamp = snapshot.GeneratedAt
		raw, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		rawBytes += len(raw)
		frame, err := newDashboardFrame(snapshot, snapshot.GeneratedAt)
		if err != nil {
			t.Fatal(err)
		}
		data := frame.data
		if previous != nil {
			data, err = frame.delta(previous)
		}
		if err != nil {
			t.Fatal(err)
		}
		dashboardBytes += len(data)
		previous = frame
	}
	t.Logf("21 frames / 20 seconds: full=%d bytes, dashboard=%d bytes (%.2f%% reduction)", rawBytes, dashboardBytes, 100*(1-float64(dashboardBytes)/float64(rawBytes)))
	if rawBytes < 80<<20 || dashboardBytes > 400<<10 || dashboardBytes*100 > rawBytes {
		t.Fatal("trace metadata escaped the dashboard transfer budget")
	}
}

func TestDashboardStreamKeepsNotificationsAcrossSharedCacheAndReconnects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(ServerConfig{DataDir: t.TempDir()}, nil)
		ctx, cancel := context.WithCancel(context.Background())
		response := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.handleStream(response, httptest.NewRequest(http.MethodGet, "/api/v1/stream?view=dashboard", nil).WithContext(ctx))
		}()
		synctest.Wait()
		time.Sleep(snapshotInterval / 2)
		s.snapshotMu.Lock()
		s.dashboardFrame = nil
		s.snapshotMu.Unlock()
		cached, err := s.dashboardStreamSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		again, _ := s.dashboardStreamSnapshot()
		if cached != again {
			t.Fatal("clients did not share frame encoding")
		}
		s.state.mu.Lock()
		s.state.experiments["new"] = model.Experiment{ID: "new", State: "queued"}
		s.state.mu.Unlock()
		s.state.notify()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		// With no changing data, the next idle refresh is a small observable heartbeat.
		time.Sleep(15 * time.Second)
		synctest.Wait()
		cancel()
		<-done
		body := response.Body.String()
		if strings.Count(body, "event: snapshot\n") != 1 || strings.Count(body, "event: snapshot_delta\n") != 1 || !strings.Contains(body, "event: heartbeat\ndata: {}\n\n") {
			t.Fatalf("unexpected snapshot/delta/heartbeat sequence: %s", body)
		}
		if !strings.Contains(body, `"upsert":[{"id":"new"`) {
			t.Fatal("shared cache swallowed the experiment update")
		}
		if response.Header().Get("X-Accel-Buffering") != "no" || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("stream permits buffering/caching")
		}
		ctx, cancel = context.WithCancel(context.Background())
		reconnected := httptest.NewRecorder()
		done = make(chan struct{})
		go func() {
			defer close(done)
			s.handleStream(reconnected, httptest.NewRequest(http.MethodGet, "/api/v1/stream?view=dashboard", nil).WithContext(ctx))
		}()
		synctest.Wait()
		cancel()
		<-done
		if !strings.Contains(reconnected.Body.String(), "event: snapshot\n") || strings.Contains(reconnected.Body.String(), "snapshot_delta") || !strings.Contains(reconnected.Body.String(), `"id":"new"`) {
			t.Fatal("reconnect did not receive a fresh complete baseline")
		}
		s.state.mu.RLock()
		watchers := len(s.state.watchers)
		s.state.mu.RUnlock()
		if watchers != 0 {
			t.Fatal("disconnected stream retained subscription")
		}
	})
}

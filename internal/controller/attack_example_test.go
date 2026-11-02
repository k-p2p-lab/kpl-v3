package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func TestCovertFlashExamplePublishesBeforeDuringAndAfterAttack(t *testing.T) {
	raw, err := os.ReadFile("../../examples/attacks/covert-flash.yaml")
	if err != nil {
		t.Fatal(err)
	}
	spec, err := scenario.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	var stage atomic.Int32
	var publications [3]atomic.Int32
	base := spec.Profiles["attacker"].WithDefaults()
	s := profileControllerFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/profile") {
			var request model.ProfileUpdateRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				return
			}
			updated, err := request.Set.Apply(base)
			if err != nil {
				t.Error(err)
				return
			}
			if request.Stage == "apply" {
				if updated.Sink.Active() {
					stage.Store(1)
				} else {
					stage.Store(2)
				}
			}
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/nodes/"), "/profile")
			writeJSON(w, http.StatusOK, model.ProfileUpdateResponse{NodeID: id, PeerID: "peer-" + id, Revision: request.Revision, Stage: request.Stage, Effective: request.Set.Snapshot(updated)})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/publish") {
			publications[stage.Load()].Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		t.Errorf("unexpected runtime command: %s %s", r.Method, r.URL.Path)
		http.Error(w, "unexpected command", http.StatusNotFound)
	})
	// Use the example's actual cohort and the actual Runner. Only container
	// creation is replaced by a ready fixture, and all traffic/schedule times
	// share the same scale (one real second becomes 20 ms).
	var phases []scenario.Phase
	for _, phase := range spec.Phases {
		switch phase.Action {
		case "join":
			for i := 0; i < phase.Count; i++ {
				id := fmt.Sprintf("%s-%d", phase.Group, i)
				s.state.setNodeLocked(model.Node{ID: id, PeerID: "peer-" + id, RunID: "run", Generation: 1, AgentID: "agent", Group: phase.Group, Role: phase.Role, Type: phase.NodeType, Profile: phase.Profile, State: model.NodeReady, Metadata: map[string]string{"pubsubEnabled": "true", "pubsubRouter": "gossipsub", "topics": "blocks", "allowPublish": fmt.Sprint(phase.Node.PublishAllowed())}})
			}
		case "schedule":
			for i := range phase.Schedule.Changes {
				after, err := time.ParseDuration(phase.Schedule.Changes[i].After)
				if err != nil {
					t.Fatal(err)
				}
				phase.Schedule.Changes[i].After = (after / 50).String()
			}
			phases = append(phases, phase)
		case "publish":
			interval, err := time.ParseDuration(phase.Interval.Value)
			if err != nil {
				t.Fatal(err)
			}
			phase.Interval.Value = (interval / 50).String()
			phases = append(phases, phase)
		case "wait-jobs":
			phases = append(phases, phase)
		}
	}
	spec.Phases = phases
	experiment := s.state.experiments["run"]
	experiment.Seed, experiment.Repetitions, experiment.State = spec.Seed, 1, "running"
	s.state.experiments["run"] = experiment
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	s.runScenario(ctx, experiment, spec)
	if run := s.state.experiments["run"]; run.State != "completed" {
		t.Fatalf("fixture scenario failed: %s %s", run.State, run.Error)
	}
	before, during, after := publications[0].Load(), publications[1].Load(), publications[2].Load()
	if before == 0 || during == 0 || after == 0 || before+during+after != 120 {
		t.Fatalf("expected 120 publications spanning the attack: before=%d during=%d after=%d", before, during, after)
	}
	t.Logf("actual Runner publish requests: before=%d during=%d after=%d", before, during, after)
}

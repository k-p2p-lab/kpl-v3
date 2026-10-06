package peer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func profileTestRequest(t *testing.T, raw string) model.ProfileUpdateRequest {
	t.Helper()
	var patch model.RuntimeProfilePatch
	if err := json.Unmarshal([]byte(raw), &patch); err != nil {
		t.Fatal(err)
	}
	return model.ProfileUpdateRequest{RunID: "run", Generation: 2, Revision: 1, Stage: "prepare", Set: patch}
}

func TestProfileUpdateAppliesActualGossipAndNetworkWithoutRestart(t *testing.T) {
	s := newMeshFreezeTestServer(t, true)
	s.config.NetworkMutable = true
	if err := s.pubsub.FreezeMesh(t.Context()); err != nil {
		t.Fatal(err)
	}
	request := profileTestRequest(t, `{"gossipsub":{"params":{"d":8,"gossipFactor":0}},"network":{"delay":"50ms"}}`)
	calls := 0
	apply := func(_ context.Context, old, next model.NetworkConfig, port int) error {
		calls++
		if old.Delay != "" || next.Delay != "50ms" || port != 20000 {
			t.Errorf("incorrect tc update: %+v -> %+v", old, next)
		}
		return nil
	}
	peerID := s.host.ID()
	if _, err := s.executeProfileUpdate(t.Context(), request, apply); err != nil || calls != 0 {
		t.Fatalf("prepare changed network: %v", err)
	}
	before, err := s.pubsub.GossipSubRuntimeParamsSnapshot(t.Context())
	if err != nil || before.D != 6 {
		t.Fatalf("prepare changed D: %+v %v", before, err)
	}
	request.Stage = "apply"
	for i := 0; i < 2; i++ {
		response, err := s.executeProfileUpdate(t.Context(), request, apply)
		if err != nil || model.ValidateProfileAcknowledgement(request, response) != nil {
			t.Fatalf("apply: %+v %v", response, err)
		}
	}
	actual, err := s.pubsub.GossipSubRuntimeParamsSnapshot(t.Context())
	if err != nil || actual.D != 8 || actual.GossipFactor != 0 || calls != 1 || s.host.ID() != peerID {
		t.Fatalf("wrong runtime state: %+v calls=%d err=%v", actual, calls, err)
	}
	mesh, err := s.pubsub.MeshFreezeSnapshot(t.Context())
	if err != nil || !mesh.Frozen {
		t.Fatal("runtime update thawed the mesh")
	}
	node := model.Node{}
	s.addNetworkStatus(&node)
	s.addProfileStatus(&node)
	if node.Metadata["profileRevision"] != "1" || node.Metadata["networkRevision"] != "1" || !strings.Contains(node.Metadata["runtimeProfile"], `"d":8`) {
		t.Fatalf("missing applied status: %v", node.Metadata)
	}
	if len(s.telemetry.events) != 1 {
		t.Fatal("idempotent apply emitted duplicate evidence")
	}
	// Stale commands cannot overwrite a later revision.
	next := profileTestRequest(t, `{"network":{"delay":"0s"}}`)
	next.Revision = 2
	noop := func(context.Context, model.NetworkConfig, model.NetworkConfig, int) error { return nil }
	if _, err := s.executeProfileUpdate(t.Context(), next, noop); err != nil {
		t.Fatal(err)
	}
	next.Stage = "apply"
	if _, err := s.executeProfileUpdate(t.Context(), next, noop); err != nil {
		t.Fatal(err)
	}
	if _, err := s.executeProfileUpdate(t.Context(), request, noop); err == nil {
		t.Fatal("stale apply reverted newer settings")
	}
	node = model.Node{}
	s.addProfileStatus(&node)
	if !strings.Contains(node.Metadata["runtimeProfile"], `"d":8`) {
		t.Fatal("network-only change lost the applied D observation")
	}
}

func TestProfileUpdatePreflightAndCancellationLeaveStateUnchanged(t *testing.T) {
	s := newMeshFreezeTestServer(t, false)
	request := profileTestRequest(t, `{"network":{"delay":"50ms"}}`)
	apply := func(context.Context, model.NetworkConfig, model.NetworkConfig, int) error {
		t.Error("invalid request reached tc")
		return nil
	}
	if _, err := s.executeProfileUpdate(t.Context(), request, apply); err == nil {
		t.Fatal("missing network capability accepted")
	}
	request = profileTestRequest(t, `{"gossipsub":{"params":{"d":30}}}`)
	if _, err := s.executeProfileUpdate(t.Context(), request, apply); err == nil {
		t.Fatal("invalid merged degrees accepted")
	}
	request = profileTestRequest(t, `{"gossipsub":{"params":{"d":8}}}`)
	request.Stage = "apply"
	if _, err := s.executeProfileUpdate(t.Context(), request, apply); err == nil {
		t.Fatal("unprepared update accepted")
	}
	request.Stage = "prepare"
	if _, err := s.executeProfileUpdate(t.Context(), request, apply); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request.Stage = "apply"
	if _, err := s.executeProfileUpdate(ctx, request, apply); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	got, err := s.pubsub.GossipSubRuntimeParamsSnapshot(t.Context())
	if err != nil || got.D != 6 || s.profileApplied.Load() != nil {
		t.Fatalf("canceled apply changed state: %+v %v", got, err)
	}
}

func TestProfileUpdateFailureDoesNotClaimSuccessfulConfiguration(t *testing.T) {
	s := newMeshFreezeTestServer(t, false)
	s.config.NetworkMutable = true
	request := profileTestRequest(t, `{"network":{"delay":"50ms"}}`)
	failure := errors.New("tc failed")
	apply := func(context.Context, model.NetworkConfig, model.NetworkConfig, int) error { return failure }
	if _, err := s.executeProfileUpdate(t.Context(), request, apply); err != nil {
		t.Fatal(err)
	}
	request.Stage = "apply"
	if _, err := s.executeProfileUpdate(t.Context(), request, apply); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if s.profileApplied.Load() != nil || s.networkApplied.Load() != nil {
		t.Fatal("failed tc application reported success")
	}
	if event := <-s.telemetry.events; event.Type != "profile_config_failed" {
		t.Fatal(event.Type)
	}
	request.Revision++
	request.Stage = "prepare"
	if _, err := s.executeProfileUpdate(t.Context(), request, apply); err == nil {
		t.Fatal("partially failed peer accepted further updates")
	}
}

func TestProfileUpdateHTTPRejectsStaleIdentityAndUnauthenticatedRequests(t *testing.T) {
	s := newMeshFreezeTestServer(t, false)
	request := profileTestRequest(t, `{"gossipsub":{"params":{"d":8}}}`)
	for _, kind := range []string{"identity", "generation", "auth", "unknown", "cancel"} {
		rq := request
		if kind == "generation" {
			rq.Generation--
		}
		data, _ := json.Marshal(rq)
		body := string(data)
		if kind == "unknown" {
			body = strings.TrimSuffix(body, "}") + `,"unsupported":true}`
		}
		r := httptest.NewRequest(http.MethodPost, "/profile", strings.NewReader(body))
		r.Header.Set("X-KPL-Node-ID", "node")
		r.Header.Set("Authorization", "Bearer test-token")
		if kind == "identity" {
			r.Header.Set("X-KPL-Node-ID", "old")
		}
		if kind == "auth" {
			r.Header.Set("Authorization", "wrong")
		}
		if kind == "cancel" {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			cancel()
			r = r.WithContext(ctx)
		}
		w := httptest.NewRecorder()
		s.handler().ServeHTTP(w, r)
		if w.Code < 400 {
			t.Fatalf("%s accepted: %s", kind, w.Body)
		}
	}
}

func TestProfileUpdateGossipStatusDoesNotRevertLocalNetworkSchedule(t *testing.T) {
	s := newMeshFreezeTestServer(t, false)
	// Explicit-ID schedules may provision capability conservatively on joins
	// which are ultimately selected only for GossipSub updates.
	s.config.NetworkMutable = true
	s.config.NodeConfig.Network = model.NetworkConfig{Delay: "10ms", Schedule: &model.NetworkSchedule{Changes: []model.NetworkChange{{After: "1s", Set: model.NetworkConfig{Delay: "100ms"}}}}}
	s.networkApplied.Store(&networkAppliedState{configJSON: `{"delay":"100ms"}`, revision: 1, appliedAt: time.Now().UTC()})
	request := profileTestRequest(t, `{"gossipsub":{"params":{"d":8}}}`)
	apply := func(context.Context, model.NetworkConfig, model.NetworkConfig, int) error {
		t.Error("gossip-only update touched tc")
		return nil
	}
	if _, err := s.executeProfileUpdate(t.Context(), request, apply); err != nil {
		t.Fatal(err)
	}
	request.Stage = "apply"
	if _, err := s.executeProfileUpdate(t.Context(), request, apply); err != nil {
		t.Fatal(err)
	}
	var node model.Node
	s.addNetworkStatus(&node)
	s.addProfileStatus(&node)
	var snapshot model.RuntimeProfilePatch
	if err := json.Unmarshal([]byte(node.Metadata["runtimeProfile"]), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Network != nil || node.Metadata["network"] != `{"delay":"100ms"}` {
		t.Fatalf("profile status overwrote local timer state: %+v", node.Metadata)
	}
}

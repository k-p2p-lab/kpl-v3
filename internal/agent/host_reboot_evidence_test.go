package agent

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/auth"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func signedRebootRequest(t *testing.T, s *Server, input model.HostRebootRequest) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/api/v1/host/reboot", strings.NewReader(string(body)))
	r.Header.Set("Authorization", "Bearer "+s.config.Token)
	r.Header.Set(model.HostRebootAuthHeader, hostRebootTestSignature(s, input))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}

func TestHostRebootAcknowledgmentAuthenticatesRequestAndResult(t *testing.T) {
	s, _ := newHostRebootServer(t, rebootTestBoot)
	request := hostRebootRequest(s)
	w := signedRebootRequest(t, s, request)
	var result model.HostRebootResult
	if w.Code != 202 || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.State != "requested" || result.Signature == "" {
		t.Fatalf("missing authenticated acknowledgment: status=%d", w.Code)
	}
	signature := result.Signature
	result.Signature = ""
	body, _ := json.Marshal(result)
	request.AgentStartedAt = request.AgentStartedAt.UTC()
	input, _ := json.Marshal(request)
	if !auth.Equal(signature, auth.HostRebootAckSignature(s.config.HostRebootToken, input, body)) {
		t.Fatal("Agent acknowledgment signature did not match its request")
	}
	changed := result
	changed.State = "rejected"
	changedBody, _ := json.Marshal(changed)
	if auth.Equal(signature, auth.HostRebootAckSignature(s.config.HostRebootToken, input, changedBody)) {
		t.Fatal("result tampering preserved acknowledgment authentication")
	}
	request.OperationID = "different-operation"
	input, _ = json.Marshal(request)
	if auth.Equal(signature, auth.HostRebootAckSignature(s.config.HostRebootToken, input, body)) {
		t.Fatal("acknowledgment moved to a different operation")
	}
}

func TestHostRebootWrongActorNeverSignsRejectionForAnotherHost(t *testing.T) {
	for _, change := range []func(*model.HostRebootRequest){
		func(r *model.HostRebootRequest) { r.NodeID = rebootTestControl },
		func(r *model.HostRebootRequest) { r.BootID = "22222222-2222-4222-8222-222222222222" },
		func(r *model.HostRebootRequest) { r.AgentStartedAt = r.AgentStartedAt.Add(-time.Second) },
	} {
		s, f := newHostRebootServer(t, rebootTestBoot)
		r := hostRebootRequest(s)
		change(&r)
		w := signedRebootRequest(t, s, r)
		var result model.HostRebootResult
		_ = json.Unmarshal(w.Body.Bytes(), &result)
		if w.Code != 409 || result.Signature != "" || len(f.calls) != 0 {
			t.Fatal("an Agent authenticated a rejection for a different actor")
		}
	}
}

func TestHostRebootRejectedSignatureCannotRunAfterConditionsChange(t *testing.T) {
	s, f := newHostRebootServer(t, rebootTestBoot)
	request := hostRebootRequest(s)
	s.processes["retained"] = &process{node: model.Node{State: model.NodeReady}}
	first := signedRebootRequest(t, s, request)
	var rejection model.HostRebootResult
	if first.Code != 409 || json.Unmarshal(first.Body.Bytes(), &rejection) != nil || rejection.Signature == "" || rejection.State != "rejected" {
		t.Fatal("current actor did not persist/authenticate its rejection")
	}
	delete(s.processes, "retained")
	s.hostRebootReceipt = nil
	second := signedRebootRequest(t, s, request)
	var repeated model.HostRebootResult
	if second.Code != 409 || json.Unmarshal(second.Body.Bytes(), &repeated) != nil || repeated != rejection || len(f.calls) != 0 {
		t.Fatal("a previously rejected signed request executed after a transient condition cleared")
	}
	stored, err := s.readHostRebootRejection(request)
	if err != nil || stored == nil || stored.Result.Signature != "" {
		t.Fatalf("unsigned durable rejection was not retained: %v", err)
	}
}

func TestHostRebootRejectionStorageFailureIsUnknownAndClosesAdmission(t *testing.T) {
	s, f := newHostRebootServer(t, rebootTestBoot)
	if err := os.WriteFile(filepath.Join(s.config.DataDir, hostRebootRejectionsDirectory), []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	w := signedRebootRequest(t, s, hostRebootRequest(s))
	var result model.HostRebootResult
	if w.Code != 202 || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.State != "unknown" || result.Signature == "" || !s.host.RebootPending || len(f.calls) != 0 {
		t.Fatal("rejection storage failure was acknowledged as a safe rejection or allowed execution")
	}
}

func TestHostRebootStatusIsFreshNonceBoundAndReadOnly(t *testing.T) {
	s, f := newHostRebootServer(t, rebootTestBoot)
	freshBoot := "22222222-2222-4222-8222-222222222222"
	s.hostBootIDReader = func() (string, error) { return freshBoot, nil }
	const nonce = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	r := httptest.NewRequest("POST", "/api/v1/host/reboot-status", strings.NewReader(`{"nonce":"`+nonce+`"}`))
	r.Header.Set("Authorization", "Bearer "+s.config.Token)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	var proof model.HostRebootProof
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &proof) != nil || proof.Nonce != nonce || proof.BootID != freshBoot || proof.AgentID != s.config.ID || len(f.calls) != 0 {
		t.Fatal("host status did not capture the actual boot without executing a helper")
	}
	signature := proof.Signature
	proof.Signature = ""
	data, _ := json.Marshal(proof)
	if signature == "" || !auth.Equal(signature, auth.HostRebootStatusSignature(s.config.HostRebootToken, data)) {
		t.Fatal("status proof was not authenticated")
	}
	proof.Nonce = "BCDEFGHIJKLMNOPQRSTUVWXYZA"
	data, _ = json.Marshal(proof)
	if auth.Equal(signature, auth.HostRebootStatusSignature(s.config.HostRebootToken, data)) {
		t.Fatal("status proof was reusable for a different nonce")
	}
	proof.Nonce = nonce
	proof.RebootPending = !proof.RebootPending
	data, _ = json.Marshal(proof)
	if auth.Equal(signature, auth.HostRebootStatusSignature(s.config.HostRebootToken, data)) {
		t.Fatal("status proof allowed status tampering")
	}
}

func TestHostRebootStatusRejectsInvalidNonceAndMissingAuthentication(t *testing.T) {
	for _, tc := range []struct {
		nonce, token string
		status       int
	}{
		{"short", "fake-internal-test-token", 400},
		{strings.Repeat("A", 26), "", 401},
		{strings.Repeat("a", 26), "fake-internal-test-token", 400},
	} {
		s, f := newHostRebootServer(t, rebootTestBoot)
		r := httptest.NewRequest("POST", "/api/v1/host/reboot-status", strings.NewReader(`{"nonce":"`+tc.nonce+`"}`))
		if tc.token != "" {
			r.Header.Set("Authorization", "Bearer "+tc.token)
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != tc.status || len(f.calls) != 0 {
			t.Fatalf("status proof policy returned %d, expected %d", w.Code, tc.status)
		}
	}
}

func TestHostRebootDurableRejectionDominatesUncommittedPreparedReceipt(t *testing.T) {
	boot, err := readHostBootID()
	if err != nil {
		t.Skip("Linux boot identity unavailable")
	}
	s, _ := newHostRebootServer(t, boot)
	request := hostRebootRequest(s)
	prepared := model.HostRebootResult{OperationID: request.OperationID, NodeID: request.NodeID, BootID: request.BootID, State: "unknown"}
	if err := s.persistHostRebootReceipt(&hostRebootReceipt{Version: 1, Request: request, Result: prepared, UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	rejected := prepared
	rejected.State = "rejected"
	rejected.Error = "No helper was started."
	if err := s.persistHostRebootRejection(request, rejected, 503); err != nil {
		t.Fatal(err)
	}
	config := s.config
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if restarted.host.RebootPending || restarted.telemetryHostRebootClosed {
		t.Fatal("a confirmed durable rejection became pending again after restart")
	}
}

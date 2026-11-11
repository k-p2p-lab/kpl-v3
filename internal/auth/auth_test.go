package auth

import "testing"

func TestCredentialValidationAndInternalKeySeparation(t *testing.T) {
	for _, credentials := range [][2]string{{"", ""}, {"admin", ""}, {"", "secret"}, {" admin", "secret"}, {"admin", "line\nbreak"}} {
		if Validate(credentials[0], credentials[1]) == nil {
			t.Fatal("accepted incomplete or invalid credentials")
		}
	}
	if err := Validate("admin", " spaces remain literal "); err != nil {
		t.Fatal(err)
	}
	key := InternalToken("admin", "secret")
	if len(key) != 64 || key == "secret" || key != InternalToken("admin", "secret") {
		t.Fatal("invalid deterministic internal key")
	}
	if Equal(key, InternalToken("other", "secret")) || Equal(key, InternalToken("admin", "changed")) {
		t.Fatal("credential changes did not rotate internal key")
	}
	if !Equal("same", "same") || Equal("same", "different") {
		t.Fatal("credential comparison failed")
	}
}

func TestHostRebootCredentialIsSeparateFromPeerCredential(t *testing.T) {
	user, password := "fake-operator", "fake-password-for-auth-test"
	host := HostRebootToken(user, password)
	peer := InternalToken(user, password)
	if len(host) != 64 || Equal(host, peer) || Equal(host, password) || !Equal(host, HostRebootToken(user, password)) {
		t.Fatal("host reboot key is not independently domain-separated and deterministic")
	}
	if Equal(host, HostRebootToken("other-operator", password)) || Equal(host, HostRebootToken(user, "different-password")) {
		t.Fatal("credential changes did not rotate the host reboot key")
	}
	if Equal(host, HostRebootToken(user, peer)) {
		t.Fatal("a Peer base credential could derive the host reboot credential")
	}
}

func TestAnalysisWorkerCredentialIsSeparateFromPeerAndHostCredentials(t *testing.T) {
	user, password := "fake-analyst", "fake-password-for-analysis-auth-test"
	key := AnalysisWorkerToken(user, password)
	if len(key) != 64 || !Equal(key, AnalysisWorkerToken(user, password)) || Equal(key, password) ||
		Equal(key, InternalToken(user, password)) || Equal(key, HostRebootToken(user, password)) {
		t.Fatal("analysis key is not independently domain-separated and deterministic")
	}
	if Equal(key, AnalysisWorkerToken("other-analyst", password)) || Equal(key, AnalysisWorkerToken(user, "different-password")) ||
		Equal(key, AnalysisWorkerToken(user, InternalToken(user, password))) {
		t.Fatal("analysis credential rotation or separation from the Peer credential failed")
	}
}

func TestHostRebootSignatureBindsRequestWithoutExposingKey(t *testing.T) {
	key := HostRebootToken("fake-operator", "fake-password-for-signature-test")
	payload := []byte(`{"operationId":"first-operation","nodeId":"worker-node"}`)
	signature := HostRebootSignature(key, payload)
	if len(signature) != 64 || Equal(signature, key) || !Equal(signature, HostRebootSignature(key, payload)) {
		t.Fatal("invalid deterministic request signature")
	}
	if Equal(signature, HostRebootSignature(key, []byte(`{"operationId":"another-operation","nodeId":"worker-node"}`))) || Equal(signature, HostRebootSignature(HostRebootToken("other-operator", "fake-password-for-signature-test"), payload)) {
		t.Fatal("host reboot signature was reusable for another request or key")
	}
}

func TestHostRebootEvidenceSignaturesAreDomainSeparated(t *testing.T) {
	key := "fake-host-control-key"
	request := []byte(`{"operationId":"first"}`)
	result := []byte(`{"state":"rejected"}`)
	ack := HostRebootAckSignature(key, request, result)
	if len(ack) != 64 || !Equal(ack, HostRebootAckSignature(key, request, result)) || Equal(ack, HostRebootSignature(key, append(append([]byte{}, request...), result...))) || Equal(ack, HostRebootAckSignature(key, request, []byte(`{"state":"requested"}`))) {
		t.Fatal("acknowledgment signature did not separate the response and its request")
	}
	proof := HostRebootStatusSignature(key, result)
	if len(proof) != 64 || Equal(proof, HostRebootSignature(key, result)) || Equal(proof, ack) || Equal(proof, HostRebootStatusSignature(key, request)) {
		t.Fatal("status proof signature was not domain-separated and bound to its fields")
	}
}

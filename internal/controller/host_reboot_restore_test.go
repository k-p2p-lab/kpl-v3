package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/auth"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func prepareHostRebootRecovery(t *testing.T, f *controllerHostRebootFixture, operation, outcome string) hostRebootStatus {
	t.Helper()
	targets, hash, err := normalizeHostRebootTargets([]hostRebootTarget{controllerHostRebootTarget(f.agent)})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	status := hostRebootStatus{Version: 1, ID: operation, RequestHash: hash, State: "interrupted", Targets: targets, CreatedAt: now, StartedAt: &now,
		Hosts: []hostRebootHostStatus{{AgentID: f.agent.ID, NodeID: f.agent.Host.NodeID, BootID: f.agent.Host.BootID, State: outcome, RequestedAt: &now}}}
	if err := f.server.persistHostReboot(status); err != nil {
		t.Fatal(err)
	}
	if err := f.server.state.acquireHostRebootHold(f.agent.Host.NodeID, operation); err != nil {
		t.Fatal(err)
	}
	if outcome != "rejected" {
		if err := f.server.state.setHostRebootFence(f.agent.Host.NodeID, &hostRebootFence{OperationID: operation, BootID: f.agent.Host.BootID, AgentStartedAt: f.agent.StartedAt, RequestedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	return status
}

func returnHostRebootAgent(t *testing.T, f *controllerHostRebootFixture, id string) model.Agent {
	t.Helper()
	agent := controllerHostRebootAgent(id, f.agent.Host.NodeID, f.api.URL)
	agent.Host.BootID, agent.StartedAt = "boot-restored", time.Now().UTC()
	f.setPhysicalAgent(agent)
	// A new instance with the same ID must replace an offline prior instance.
	f.server.state.mu.Lock()
	if previous, exists := f.server.state.agents[id]; exists {
		previous.State = model.AgentOffline
		f.server.state.agents[id] = previous
	}
	f.server.state.mu.Unlock()
	if _, err := f.server.state.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	return agent
}

func assertHostRebootDisabled(t *testing.T, server *Server, id string, want bool) {
	t.Helper()
	server.state.mu.RLock()
	agent := server.state.agents[id]
	server.state.mu.RUnlock()
	if agent.Disabled != want {
		t.Fatalf("Agent %s disabled=%v, want %v", id, agent.Disabled, want)
	}
}

func TestHostRebootAutomaticallyRestoresPriorParticipation(t *testing.T) {
	for _, test := range []struct {
		name                    string
		manual, hostHold, newID bool
		wantHold                bool
	}{
		{name: "enabled"},
		{name: "enabled-replacement-id", newID: true},
		{name: "manual-disabled", manual: true},
		{name: "prior-host-hold", hostHold: true, newID: true, wantHold: true},
		{name: "disabled-replacement-ambiguous", manual: true, newID: true, wantHold: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newControllerHostRebootFixture(t, nil)
			if test.manual {
				setEnabledTest(t, f.server.state, f.agent.ID, false)
			}
			if test.hostHold {
				if err := f.server.state.setHostDisabled(f.agent.Host.NodeID, true); err != nil {
					t.Fatal(err)
				}
			}
			const operation = "reboot-restore-previous-1"
			prepareHostRebootRecovery(t, f, operation, "unknown")
			id := f.agent.ID
			if test.newID {
				id = "replacement-agent"
			}
			returned := returnHostRebootAgent(t, f, id)
			err := f.server.recoverHostRebootHolds(t.Context())
			ambiguous := test.manual && test.newID && !test.hostHold
			if (err != nil) != ambiguous {
				t.Fatalf("unexpected recovery error: %v", err)
			}
			status := controllerHostRebootStatus(t, f.server, operation)
			if ambiguous {
				if status.State == "completed" || status.Hosts[0].State == "rebooted" || !strings.Contains(status.Hosts[0].Error, "identity") {
					t.Fatalf("ambiguous disabled identity was restored: %+v", status)
				}
			} else if status.State != "completed" || status.Hosts[0].State != "rebooted" {
				t.Fatalf("verified return was not completed: %+v", status)
			}
			assertHostRebootDisabled(t, f.server, returned.ID, test.manual || test.wantHold)
			stored, err := loadHostAvailability(f.server.config.DataDir)
			if err != nil || stored.Disabled[f.agent.Host.NodeID] != test.wantHold {
				t.Fatalf("previous host state was not retained: %+v %v", stored, err)
			}
			if test.manual && !f.server.state.agentDisabled[f.agent.ID] {
				t.Fatal("automatic restoration erased a manual Agent setting")
			}
			if f.requests.Load() != 0 {
				t.Fatal("restoration sent another Linux reboot")
			}
		})
	}
}

func TestHostRebootRecoveryRetainsUnconfirmedHosts(t *testing.T) {
	for _, outcome := range []string{"requested", "unknown"} {
		t.Run(outcome, func(t *testing.T) {
			f := newControllerHostRebootFixture(t, nil)
			const operation = "reboot-restore-unconfirmed-1"
			prepareHostRebootRecovery(t, f, operation, outcome)
			if err := f.server.recoverHostRebootHolds(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertHostRebootDisabled(t, f.server, f.agent.ID, true)
			if status := controllerHostRebootStatus(t, f.server, operation); status.State == "completed" {
				t.Fatal("unchanged boot completed recovery")
			}
			returned := returnHostRebootAgent(t, f, "replacement-agent")
			returned.StartupReconciled = false
			f.setPhysicalAgent(returned)
			if _, err := f.server.state.registerAgent(returned); err != nil {
				t.Fatal(err)
			}
			if err := f.server.recoverHostRebootHolds(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertHostRebootDisabled(t, f.server, returned.ID, true)
			if f.requests.Load() != 0 {
				t.Fatal("unconfirmed recovery retried reboot")
			}
		})
	}
}

func TestHostRebootRefusalRestoresWithoutReboot(t *testing.T) {
	f := newControllerHostRebootFixture(t, nil)
	const operation = "reboot-restore-rejected-1"
	prepareHostRebootRecovery(t, f, operation, "rejected")
	if err := f.server.recoverHostRebootHolds(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertHostRebootDisabled(t, f.server, f.agent.ID, false)
	status := controllerHostRebootStatus(t, f.server, operation)
	if status.State == "completed" || status.Hosts[0].State != "rejected" {
		t.Fatalf("refusal was reported as a completed reboot: %+v", status)
	}
	if f.requests.Load() != 0 {
		t.Fatal("refusal recovery sent a reboot")
	}
}

func TestHostRebootRecoverySurvivesControllerRestartAndRestoreCommitWindow(t *testing.T) {
	for _, commitWindow := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-return", true: "after-restore-before-job-commit"}[commitWindow], func(t *testing.T) {
			f := newControllerHostRebootFixture(t, nil)
			const operation = "reboot-restore-crash-1"
			status := prepareHostRebootRecovery(t, f, operation, "requested")
			returned := returnHostRebootAgent(t, f, "replacement-agent")
			if commitWindow {
				proof, err := f.server.requestHostRebootProof(t.Context(), returned)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.server.restoreHostRebootParticipation(status, 0, returned, proof); err != nil {
					t.Fatal(err)
				}
				// This models rename having happened while the original process
				// has not published a successful directory-fsync/job commit yet.
				f.server.state.mu.Lock()
				f.server.state.hostDisabled[f.agent.Host.NodeID] = true
				f.server.state.mu.Unlock()
			}
			restarted := New(f.server.config, nil)
			if _, err := restarted.state.registerAgent(returned); err != nil {
				t.Fatal(err)
			}
			if err := restarted.recoverHostRebootHolds(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertHostRebootDisabled(t, restarted, returned.ID, false)
			status = controllerHostRebootStatus(t, restarted, operation)
			if status.State != "completed" || status.Hosts[0].State != "rebooted" {
				t.Fatalf("restart lost restoration: %+v", status)
			}
			stored, err := loadHostAvailability(restarted.config.DataDir)
			if err != nil || len(stored.Reboots) != 0 {
				t.Fatalf("committed recovery retained a marker: %+v %v", stored, err)
			}
			if f.requests.Load() != 0 {
				t.Fatal("restart replayed a Linux reboot")
			}
		})
	}
}

func TestHostRebootStatusPersistenceFailureDoesNotPublishCompleted(t *testing.T) {
	f := newControllerHostRebootFixture(t, nil)
	const operation = "reboot-restore-persist-1"
	prepareHostRebootRecovery(t, f, operation, "requested")
	returnHostRebootAgent(t, f, "replacement-agent")
	// Cache the old status so a failed final write can be distinguished from
	// the filesystem failure used to inject it.
	controllerHostRebootStatus(t, f.server, operation)
	name := filepath.Join(f.server.config.DataDir, hostRebootDirectory, operation+".json")
	if err := os.Rename(name, name+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(name, 0700); err != nil {
		t.Fatal(err)
	}
	if err := f.server.recoverHostRebootHolds(t.Context()); err == nil {
		t.Fatal("status persistence fault was ignored")
	}
	if status := controllerHostRebootStatus(t, f.server, operation); status.State == "completed" || status.Hosts[0].State == "rebooted" {
		t.Fatalf("failed status write published completion: %+v", status)
	}
	stored, err := loadHostAvailability(f.server.config.DataDir)
	if err != nil || !stored.Reboots[f.agent.Host.NodeID].Restored {
		t.Fatalf("retry marker disappeared after status failure: %+v %v", stored, err)
	}
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(name+".saved", name); err != nil {
		t.Fatal(err)
	}
	if err := f.server.recoverHostRebootHolds(t.Context()); err != nil {
		t.Fatal(err)
	}
	if status := controllerHostRebootStatus(t, f.server, operation); status.State != "completed" {
		t.Fatalf("restoration did not retry metadata: %+v", status)
	}
	if f.requests.Load() != 0 {
		t.Fatal("metadata retry sent a reboot")
	}
}

func TestHostRebootOwnershipAndExplicitSettingsWinOverOldRecovery(t *testing.T) {
	f := newControllerHostRebootFixture(t, nil)
	const operation = "reboot-restore-owner-1"
	status := prepareHostRebootRecovery(t, f, operation, "requested")
	returned := returnHostRebootAgent(t, f, f.agent.ID)
	setEnabledTest(t, f.server.state, returned.ID, false)
	if err := f.server.recoverHostRebootHolds(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertHostRebootDisabled(t, f.server, returned.ID, true)
	if err := f.server.state.acquireHostRebootHold(f.agent.Host.NodeID, "reboot-new-owner-2"); err != nil {
		t.Fatal(err)
	}
	proof, err := f.server.requestHostRebootProof(t.Context(), returned)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.server.restoreHostRebootParticipation(status, 0, returned, proof); err == nil {
		t.Fatal("old recovery cleared another operation's hold")
	}
	assertHostRebootDisabled(t, f.server, returned.ID, true)
	if err := f.server.state.setHostDisabled(f.agent.Host.NodeID, false); err != nil {
		t.Fatal(err)
	}
	if err := f.server.restoreHostRebootParticipation(status, 0, returned, proof); err != nil {
		t.Fatal(err)
	}
	assertHostRebootDisabled(t, f.server, returned.ID, true)
}

func TestHostRebootLegacyHoldsAndJobsKeepManualRecovery(t *testing.T) {
	f := newControllerHostRebootFixture(t, nil)
	const operation = "reboot-restore-legacy-1"
	prepareHostRebootRecovery(t, f, operation, "requested")
	legacy, _ := json.Marshal(map[string]bool{f.agent.Host.NodeID: true})
	if err := os.WriteFile(filepath.Join(f.server.config.DataDir, hostAvailabilityFile), legacy, 0600); err != nil {
		t.Fatal(err)
	}
	returned := returnHostRebootAgent(t, f, "replacement-agent")
	if err := f.server.recoverHostRebootHolds(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertHostRebootDisabled(t, f.server, returned.ID, true)
	if status := controllerHostRebootStatus(t, f.server, operation); status.State == "completed" {
		t.Fatal("legacy previous participation was guessed")
	}
}

func TestHostRebootMixedAgentParticipationRequiresKnownDisabledIdentity(t *testing.T) {
	f := newControllerHostRebootFixture(t, nil)
	alias := controllerHostRebootAgent("disabled-alias", f.agent.Host.NodeID, f.api.URL)
	if _, err := f.server.state.registerAgent(alias); err != nil {
		t.Fatal(err)
	}
	setEnabledTest(t, f.server.state, alias.ID, false)
	const operation = "reboot-restore-mixed-1"
	prepareHostRebootRecovery(t, f, operation, "requested")
	returned := returnHostRebootAgent(t, f, f.agent.ID)
	if err := f.server.recoverHostRebootHolds(t.Context()); err == nil {
		t.Fatal("mixed settings restored before the disabled Agent returned")
	}
	assertHostRebootDisabled(t, f.server, returned.ID, true)
	f.server.state.mu.Lock()
	alias.StartedAt, alias.Host.BootID, alias.LastSeen = returned.StartedAt, returned.Host.BootID, time.Now().UTC()
	alias.Disabled = true
	f.server.state.agents[alias.ID] = alias
	f.server.state.mu.Unlock()
	if err := f.server.recoverHostRebootHolds(t.Context()); err == nil {
		t.Fatal("an unsigned identity claim substituted for the disabled Agent's fresh host proof")
	}
	assertHostRebootDisabled(t, f.server, returned.ID, true)
	aliasProof := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input model.HostRebootProofRequest
		if json.NewDecoder(r.Body).Decode(&input) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		proof := model.HostRebootProof{Nonce: input.Nonce, AgentID: alias.ID, NodeID: alias.Host.NodeID, BootID: alias.Host.BootID, AgentStartedAt: alias.StartedAt.UTC(), StartupReconciled: true, RebootSupported: true}
		canonical, _ := json.Marshal(proof)
		proof.Signature = auth.HostRebootStatusSignature(f.server.config.HostRebootToken, canonical)
		_ = json.NewEncoder(w).Encode(proof)
	}))
	defer aliasProof.Close()
	f.server.state.mu.Lock()
	alias.URL = aliasProof.URL
	f.server.state.agents[alias.ID] = alias
	f.server.state.mu.Unlock()
	if err := f.server.recoverHostRebootHolds(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertHostRebootDisabled(t, f.server, returned.ID, false)
	assertHostRebootDisabled(t, f.server, alias.ID, true)
}

func TestHostRebootCapturesKnownOfflineDisabledIdentity(t *testing.T) {
	f := newControllerHostRebootFixture(t, nil)
	alias := controllerHostRebootAgent("offline-disabled-alias", f.agent.Host.NodeID, f.api.URL)
	if _, err := f.server.state.registerAgent(alias); err != nil {
		t.Fatal(err)
	}
	setEnabledTest(t, f.server.state, alias.ID, false)
	f.server.state.mu.Lock()
	alias.State, alias.LastSeen, alias.Disabled = model.AgentOffline, time.Now().Add(-time.Hour), true
	f.server.state.agents[alias.ID] = alias
	f.server.state.mu.Unlock()
	const operation = "reboot-restore-offline-disabled-1"
	prepareHostRebootRecovery(t, f, operation, "requested")
	stored, err := loadHostAvailability(f.server.config.DataDir)
	if err != nil || !stored.Reboots[f.agent.Host.NodeID].Agents[alias.ID] {
		t.Fatalf("offline manual disable was omitted from the previous state: %+v %v", stored, err)
	}
	returned := returnHostRebootAgent(t, f, f.agent.ID)
	if err := f.server.recoverHostRebootHolds(t.Context()); err == nil {
		t.Fatal("an unresolved disabled identity was enabled by another Agent's return")
	}
	assertHostRebootDisabled(t, f.server, returned.ID, true)
}

func TestHostRebootHoldSaveFailureRetainsExclusionAndRetries(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permission failure requires an unprivileged test user")
	}
	f := newControllerHostRebootFixture(t, nil)
	const operation = "reboot-restore-hold-write-1"
	status := prepareHostRebootRecovery(t, f, operation, "requested")
	returned := returnHostRebootAgent(t, f, "replacement-agent")
	proof, err := f.server.requestHostRebootProof(t.Context(), returned)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.server.state.setHostRebootFence(f.agent.Host.NodeID, nil); err != nil {
		t.Fatal(err)
	}
	// The existing hold remains readable while staging or renaming its updated
	// metadata is denied. No completed host outcome may be published.
	if err := os.Chmod(f.server.state.dataDir, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(f.server.state.dataDir, 0700) })
	if err := f.server.restoreHostRebootParticipation(status, 0, returned, proof); err == nil || !strings.Contains(err.Error(), "save host availability") {
		t.Fatalf("host availability persistence fault was ignored: %v", err)
	}
	if err := os.Chmod(f.server.state.dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	assertHostRebootDisabled(t, f.server, returned.ID, true)
	if err := f.server.recoverHostRebootHolds(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertHostRebootDisabled(t, f.server, returned.ID, false)
	if f.requests.Load() != 0 {
		t.Fatal("hold save retry resent a reboot")
	}
}

func TestHostRebootExplicitEnableSupersedesAutomaticRecovery(t *testing.T) {
	f := newControllerHostRebootFixture(t, nil)
	const operation = "reboot-restore-explicit-1"
	status := prepareHostRebootRecovery(t, f, operation, "unknown")
	returned := returnHostRebootAgent(t, f, "replacement-agent")
	request := httptest.NewRequest(http.MethodPut, "/api/v1/agents/"+returned.ID+"/enabled", strings.NewReader(`{"enabled":true}`))
	request.Header.Set("X-KPL-Request", "dashboard")
	response := httptest.NewRecorder()
	f.handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("explicit verified recovery failed: %d %s", response.Code, response.Body)
	}
	assertHostRebootDisabled(t, f.server, returned.ID, false)
	stored, err := loadHostAvailability(f.server.config.DataDir)
	if err != nil || len(stored.Reboots) != 0 {
		t.Fatalf("explicit recovery retained automatic ownership: %+v %v", stored, err)
	}
	// A later operator hold belongs to the operator, not the completed reboot.
	if err := f.server.state.setHostDisabled(returned.Host.NodeID, true); err != nil {
		t.Fatal(err)
	}
	proof, err := f.server.requestHostRebootProof(t.Context(), returned)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.server.restoreHostRebootParticipation(status, 0, returned, proof); err != nil {
		t.Fatal(err)
	}
	assertHostRebootDisabled(t, f.server, returned.ID, true)
}

func TestHostAvailabilityRejectsUnversionedOrNullOwnershipRecords(t *testing.T) {
	for _, raw := range []string{`null`, `{"disabled":{}}`, `{"version":2,"disabled":null}`, `{"version":2,"disabled":{},"reboots":{"node-one":{"operationId":"reboot-owner-1","agents":{"agent-one":false}}}}`} {
		directory := t.TempDir()
		if err := os.WriteFile(filepath.Join(directory, hostAvailabilityFile), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadHostAvailability(directory); err == nil {
			t.Fatalf("invalid availability became an empty enabled setting: %s", raw)
		}
	}
}

func TestHostRebootUnchangedRecoveryDoesNotRewriteJob(t *testing.T) {
	f := newControllerHostRebootFixture(t, nil)
	const operation = "reboot-restore-noop-1"
	prepareHostRebootRecovery(t, f, operation, "unknown")
	name := filepath.Join(f.server.config.DataDir, hostRebootDirectory, operation+".json")
	before, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := f.server.recoverHostRebootHolds(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) || !bytes.Equal(raw, current) {
		t.Fatal("unchanged host polling replaced durable job metadata")
	}
	assertHostRebootDisabled(t, f.server, f.agent.ID, true)
}

func TestHostRebootRestoredMarkerStillCommitsMatchingJob(t *testing.T) {
	f := newControllerHostRebootFixture(t, nil)
	const operation = "reboot-restore-noop-marker-1"
	status := prepareHostRebootRecovery(t, f, operation, "requested")
	returned := returnHostRebootAgent(t, f, "replacement-agent")
	proof, err := f.server.requestHostRebootProof(t.Context(), returned)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.server.restoreHostRebootParticipation(status, 0, returned, proof); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	status.State, status.Hosts[0].State, status.FinishedAt = "completed", "rebooted", &now
	if err := f.server.persistHostReboot(status); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(f.server.config.DataDir, hostRebootDirectory, operation+".json")
	before, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.server.recoverHostRebootHolds(t.Context()); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(before, after) || !bytes.Equal(raw, current) {
		t.Fatal("restored marker did not force a durable commit of the matching job")
	}
	stored, err := loadHostAvailability(f.server.config.DataDir)
	if err != nil || len(stored.Reboots) != 0 {
		t.Fatalf("restored marker cleanup failed: %+v %v", stored, err)
	}
	assertHostRebootDisabled(t, f.server, returned.ID, false)
}

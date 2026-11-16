package master

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestSwarmServiceLifecycleKeepsCapturedIdentityWithDockerCLI(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker CLI is not installed")
	}
	for _, operation := range []string{"stop", "capture", "placement"} {
		for _, replaced := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/replaced=%t", operation, replaced), func(t *testing.T) {
				oldID, newID := strings.Repeat("a", 25), strings.Repeat("b", 25)
				var replacement, paused atomic.Bool
				var namedLookups, unsafeTaskLookups, foreignMutations, originalMutations atomic.Int32
				service := func(id string) map[string]any {
					owner := swarmApplication
					if id == newID {
						owner = "foreign-application"
					}
					return map[string]any{"ID": id, "Version": map[string]any{"Index": 3}, "Spec": map[string]any{
						"Name": "kpl_agent", "Labels": map[string]string{"io.kpl.application": owner, "com.docker.stack.namespace": "kpl"},
						"TaskTemplate": map[string]any{"ContainerSpec": map[string]any{"Image": "registry.example.test/lab:v3"}}, "Mode": map[string]any{"Global": map[string]any{}},
					}}
				}
				node := map[string]any{"ID": "controlnode", "Spec": map[string]any{"Availability": "active"}, "Description": map[string]any{"Platform": map[string]string{"OS": "linux"}}, "Status": map[string]string{"State": "ready"}}
				task := func() map[string]any {
					state, pid := "running", 123
					if paused.Load() {
						state, pid = "shutdown", 0
					}
					return map[string]any{"ID": "ownedtask", "NodeID": "controlnode", "ServiceID": oldID, "DesiredState": "shutdown", "Status": map[string]any{"State": state, "ContainerStatus": map[string]any{"ContainerID": strings.Repeat("c", 64), "PID": pid, "ExitCode": 0}}}
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					switch {
					case r.URL.Path == "/_ping":
						w.Header().Set("API-Version", "1.44")
						w.Header().Set("OSType", "linux")
						_, _ = w.Write([]byte("OK"))
					case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/services"):
						if replacement.Load() {
							if strings.Contains(r.URL.Query().Get("filters"), "kpl_agent") {
								_ = json.NewEncoder(w).Encode([]map[string]any{service(newID)})
							} else {
								// No replacement is named after the original full ID.
								_, _ = w.Write([]byte("[]"))
							}
						} else {
							_ = json.NewEncoder(w).Encode([]map[string]any{service(oldID)})
						}
					case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/services/"):
						reference := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
						id := reference
						if reference == "kpl_agent" {
							namedLookups.Add(1)
							id = oldID
							if replacement.Load() {
								id = newID
							}
						} else if len(reference) >= 12 && strings.HasPrefix(oldID, reference) {
							id = oldID
						}
						if id == oldID && replacement.Load() || id != oldID && id != newID {
							w.WriteHeader(http.StatusNotFound)
							_ = json.NewEncoder(w).Encode(map[string]string{"message": "original service is absent"})
						} else {
							_ = json.NewEncoder(w).Encode(service(id))
						}
					case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/update"):
						if strings.Contains(r.URL.Path, "/services/"+newID+"/") {
							foreignMutations.Add(1)
						} else if strings.Contains(r.URL.Path, "/services/"+oldID+"/") {
							originalMutations.Add(1)
						}
						paused.Store(true)
						_, _ = w.Write([]byte(`{"Warnings":[]}`))
					case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/tasks"):
						if !strings.Contains(r.URL.Query().Get("filters"), oldID) {
							unsafeTaskLookups.Add(1)
							if replacement.Load() {
								foreignTask := task()
								foreignTask["ID"], foreignTask["ServiceID"] = "foreigntask", newID
								_ = json.NewEncoder(w).Encode([]map[string]any{foreignTask})
								return
							}
						}
						_ = json.NewEncoder(w).Encode([]map[string]any{task()})
					case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/tasks/"):
						_ = json.NewEncoder(w).Encode(task())
					case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/nodes"):
						_ = json.NewEncoder(w).Encode([]map[string]any{node})
					case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/nodes/"):
						_ = json.NewEncoder(w).Encode(node)
					default:
						w.WriteHeader(http.StatusNotFound)
						_ = json.NewEncoder(w).Encode(map[string]string{"message": "unsupported isolated fixture endpoint"})
					}
				}))
				defer server.Close()
				s, _, output := newSwarmFixture(t, "restart")
				s.a.Runner = ExecRunner{}
				s.a.Environ = []string{"PATH=" + os.Getenv("PATH"), "DOCKER_CONFIG=" + t.TempDir(), "DOCKER_HOST=tcp://" + strings.TrimPrefix(server.URL, "http://"), "DOCKER_API_VERSION=1.44"}
				s.timeout = 3 * time.Second
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				if err := s.loadServices(ctx); err != nil {
					t.Fatalf("capture original service ownership: %v output=%s", err, output)
				}
				replacement.Store(replaced)
				var err error
				switch operation {
				case "stop":
					err = s.stopService(ctx, "kpl_agent", 3*time.Second)
				case "capture":
					_, err = s.captureTasks(ctx, "kpl_agent", "")
				case "placement":
					err = s.placementExclusions(ctx, "add", []string{"controlnode"})
				}
				if (err != nil) != replaced || namedLookups.Load() != 0 || unsafeTaskLookups.Load() != 0 || foreignMutations.Load() != 0 {
					t.Fatalf("lifecycle rebound a captured name: replaced=%t err=%v namedLookups=%d unsafeTaskLookups=%d foreignMutations=%d output=%s", replaced, err, namedLookups.Load(), unsafeTaskLookups.Load(), foreignMutations.Load(), output)
				}
				wantMutations := int32(0)
				if !replaced && operation != "capture" {
					wantMutations = 1
				}
				if originalMutations.Load() != wantMutations {
					t.Fatalf("unexpected original service mutation count: got=%d want=%d", originalMutations.Load(), wantMutations)
				}
			})
		}
	}
}

func TestSwarmRestartRejectsServiceChangesDuringCleanShutdown(t *testing.T) {
	for _, replacementOwner := range []string{swarmApplication, "foreign-application", "absent"} {
		t.Run(replacementOwner, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "restart")
			model := &swarmLifecycleModel{paused: map[string]bool{}}
			replaced, replacementTouched := false, false
			runner.handle = func(ctx context.Context, c Command) (string, error) {
				a := c.Args
				last := a[len(a)-1]
				if replaced && slices.Equal(a[:min(len(a), 2)], []string{"service", "ls"}) {
					if replacementOwner == "absent" {
						return "controller monitor", nil
					}
					return "controller replacementagent monitor", nil
				}
				if replaced && slices.Equal(a[:min(len(a), 2)], []string{"service", "inspect"}) && last == "replacementagent" {
					if a[3] == "{{.ID}}" {
						return last, nil
					}
					return "kpl_agent|" + replacementOwner, nil
				}
				if replaced && slices.Equal(a[:min(len(a), 2)], []string{"service", "ps"}) && last == "agent" {
					return "", errors.New("original Agent service is absent")
				}
				if replaced && slices.Equal(a[:min(len(a), 2)], []string{"service", "update"}) && (last == "kpl_agent" || last == "replacementagent") {
					replacementTouched = true
					return "", errors.New("fixture replacement service was paused")
				}
				result, err := model.handle(ctx, c)
				if a[0] == "inspect" && last == "kplcontrollertask" && model.paused["kpl_controller"] && err == nil {
					replaced = true
				}
				return result, err
			}
			if err := s.a.RunSwarm(t.Context(), "restart", nil); err == nil || replacementTouched || swarmCallIndex(runner.calls, "stack", "deploy") >= 0 {
				t.Fatalf("service replacement advanced restart: touched=%t err=%v", replacementTouched, err)
			}
		})
	}
}

func TestSwarmDeployRechecksCapturedServicesAtSubmission(t *testing.T) {
	for _, change := range []string{"owned-replacement", "foreign-replacement", "foreign-original", "removed", "inspection-error", "duplicate", "unchanged"} {
		t.Run(change, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "deploy")
			model := &swarmLifecycleModel{paused: map[string]bool{}}
			checked := false
			runner.handle = func(ctx context.Context, c Command) (string, error) {
				a := c.Args
				last := a[len(a)-1]
				if a[0] == "node" && a[1] == "update" {
					checked = true
				}
				if checked && slices.Equal(a[:min(len(a), 2)], []string{"service", "ls"}) {
					switch change {
					case "owned-replacement", "foreign-replacement":
						return "controller replacementagent monitor", nil
					case "removed":
						return "controller monitor", nil
					case "inspection-error":
						return "", errors.New("fixture final inventory unavailable")
					case "duplicate":
						return "controller agent agent monitor", nil
					}
				}
				if checked && slices.Equal(a[:min(len(a), 2)], []string{"service", "inspect"}) && last == "replacementagent" {
					owner := swarmApplication
					if change == "foreign-replacement" {
						owner = "foreign-application"
					}
					if a[3] == "{{.ID}}" {
						return last, nil
					}
					return "kpl_agent|" + owner, nil
				}
				if checked && change == "foreign-original" && slices.Equal(a[:min(len(a), 2)], []string{"service", "inspect"}) && last == "agent" && a[3] != "{{.ID}}" {
					return "kpl_agent|foreign-application", nil
				}
				return model.handle(ctx, c)
			}
			err := s.a.RunSwarm(t.Context(), "deploy", []string{"new-worker"})
			if (err == nil) != (change == "unchanged") || (swarmCallIndex(runner.calls, "stack", "deploy") >= 0) != (change == "unchanged") {
				t.Fatalf("final inventory did not fence deployment: change=%s err=%v", change, err)
			}
		})
	}
}

func TestSwarmRestartRecoversAnAmbiguousPartialShutdownOnTheSameIDs(t *testing.T) {
	for _, failure := range []string{"controller", "agent", "final-inventory"} {
		t.Run(failure, func(t *testing.T) {
			s, runner, output := newSwarmFixture(t, "restart")
			model := &swarmLifecycleModel{paused: map[string]bool{}}
			failed := false
			runner.handle = func(ctx context.Context, c Command) (string, error) {
				a := c.Args
				if slices.Equal(a[:min(len(a), 2)], []string{"service", "update"}) {
					id := a[len(a)-1]
					if !slices.Contains([]string{"controller", "agent", "monitor"}, id) {
						t.Fatalf("retry used a mutable service name: %v", a)
					}
					if id == failure && !failed {
						failed = true
						_, _ = model.handle(ctx, c)
						return "", errors.New("fixture shutdown mutation response lost")
					}
				}
				if !failed && failure == "final-inventory" && model.paused["kpl_prometheus"] && slices.Equal(a[:min(len(a), 2)], []string{"service", "ls"}) {
					failed = true
					return "", errors.New("fixture final inventory unavailable")
				}
				return model.handle(ctx, c)
			}
			if err := s.a.RunSwarm(t.Context(), "restart", nil); err == nil || !failed || swarmCallIndex(runner.calls, "stack", "deploy") >= 0 {
				t.Fatalf("partial shutdown was reported complete: injected=%t err=%v", failed, err)
			}
			if err := s.a.RunSwarm(t.Context(), "restart", nil); err != nil {
				t.Fatalf("verified original service IDs could not resume after a partial failure: %v", err)
			}
			if swarmCallIndex(runner.calls, "stack", "deploy") < 0 || !strings.Contains(output.String(), "Stopping kpl_controller and verifying clean exits") {
				t.Fatal("retry omitted deployment or lost the operator-facing service name")
			}
		})
	}
}

func TestSwarmServiceShutdownRequiresOwnedIdentityAndTaskEvidence(t *testing.T) {
	for _, owned := range []bool{false, true} {
		t.Run(fmt.Sprint(owned), func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "restart")
			if owned {
				s.services, s.serviceIDs = []string{"kpl_agent"}, []string{"ownedagent"}
			}
			runner.handle = func(_ context.Context, c Command) (string, error) {
				if !owned || !slices.Equal(c.Args, []string{"service", "ps", "--quiet", "--no-trunc", "ownedagent"}) {
					t.Fatalf("missing identity or task history allowed a lifecycle mutation: %v", c.Args)
				}
				return "", nil
			}
			if err := s.stopService(t.Context(), "kpl_agent", time.Second); err == nil {
				t.Fatal("missing ownership or empty task evidence was accepted")
			}
			if swarmCallIndex(runner.calls, "service", "update") >= 0 {
				t.Fatal("service was paused before its task history could be verified")
			}
		})
	}
}

func TestSwarmServiceInventoryResolvesAndValidatesFullID(t *testing.T) {
	for _, fullID := range []string{"originalservice", "foreignservice", "invalid-id", "first\nsecond", ""} {
		t.Run(fullID, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "restart")
			runner.handle = func(_ context.Context, c Command) (string, error) {
				if slices.Equal(c.Args[:min(len(c.Args), 2)], []string{"service", "ls"}) {
					return "original", nil
				}
				if c.Args[3] == "{{.ID}}" {
					return fullID, nil
				}
				if c.Args[len(c.Args)-1] != "originalservice" {
					t.Fatalf("ownership was inspected through an abbreviated or mismatched ID: %v", c.Args)
				}
				return "kpl_agent|" + swarmApplication, nil
			}
			err := s.loadServices(t.Context())
			if (err == nil) != (fullID == "originalservice") {
				t.Fatalf("invalid full service ID classification: fullID=%q err=%v", fullID, err)
			}
			if err == nil && !slices.Equal(s.serviceIDs, []string{"originalservice"}) {
				t.Fatalf("abbreviated identity remained in the ownership inventory: %v", s.serviceIDs)
			}
		})
	}
}

func TestSwarmRemovalWaitsForAbbreviatedKnownIDsWithoutAdoptingThem(t *testing.T) {
	for _, mode := range []string{"one-pending-view", "retained-prefix", "foreign-prefix"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, runner, _ := newSwarmFixture(t, "remove")
				fullID := strings.Repeat("a", 25)
				s.services, s.serviceIDs = []string{"kpl_agent"}, []string{fullID}
				polls := 0
				runner.handle = func(_ context.Context, c Command) (string, error) {
					a := c.Args
					switch {
					case slices.Equal(a[:min(len(a), 2)], []string{"service", "rm"}):
						if !slices.Equal(a, []string{"service", "rm", fullID}) {
							t.Fatalf("removal used an abbreviated identity: %v", a)
						}
						return "", nil
					case slices.Equal(a[:min(len(a), 2)], []string{"service", "ls"}):
						polls++
						if mode == "foreign-prefix" {
							return strings.Repeat("b", 12), nil
						}
						if mode == "retained-prefix" || polls == 1 {
							return fullID[:12], nil
						}
						return "", nil
					case a[0] == "stack" || a[0] == "node":
						return "", nil
					}
					return "", fmt.Errorf("unexpected Docker command: %v", a)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				err := s.remove(ctx)
				if (err == nil) != (mode == "one-pending-view") || (swarmCallIndex(runner.calls, "stack", "rm") >= 0) != (mode == "one-pending-view") {
					t.Fatalf("nonempty service view authorized stack cleanup: mode=%s polls=%d err=%v", mode, polls, err)
				}
			})
		})
	}
}

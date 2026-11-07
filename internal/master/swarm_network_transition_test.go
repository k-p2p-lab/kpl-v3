package master

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

type fixtureNetworkCreateExitError struct{}

func (fixtureNetworkCreateExitError) Error() string { return "exit status 1" }
func (fixtureNetworkCreateExitError) ExitCode() int { return 1 }

func TestSwarmNetworkResetWaitsForDockerConvergence(t *testing.T) {
	for _, mode := range []string{"removal-lag", "name-conflict", "creation-lag", "retiring-after-create", "both-known-after-create", "foreign-after-create", "replacement", "never-removed", "uncertain-create"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, r, _ := newSwarmFixture(t, "reset-peer-network")
				s.timeout = 2 * time.Second
				expected := s.network + "|overlay|swarm|true|false|false|false|false||default|0|1|0|" + swarmApplication + "|kpl||"
				removed, created := false, false
				removalPolls, creationPolls, creates := 0, 0, 0
				r.handle = func(ctx context.Context, c Command) (string, error) {
					a := c.Args
					if err := ctx.Err(); err != nil {
						return "", err
					}
					switch {
					case a[0] == "service":
						return "", nil
					case a[0] == "node" && a[1] == "ls":
						return "control worker", nil
					case a[0] == "node" && a[1] == "inspect":
						return "ready\nready", nil
					case a[0] == "network" && a[1] == "rm":
						if a[2] != "oldnetwork" {
							t.Error("unexpected replacement was removed")
						}
						removed = true
						return "", nil
					case a[0] == "network" && a[1] == "ls":
						if !slices.Contains(a, "--no-trunc") {
							t.Error("network list can truncate identity")
						}
						if mode == "replacement" && removed {
							return "foreignreplacement", nil
						}
						if removed && !created {
							removalPolls++
							if mode == "never-removed" || mode == "removal-lag" && removalPolls < 3 {
								return "oldnetwork", nil
							}
							return "", nil
						}
						if created {
							creationPolls++
							if mode == "retiring-after-create" && creationPolls < 3 {
								return "oldnetwork", nil
							}
							if mode == "both-known-after-create" && creationPolls < 3 {
								return "oldnetwork newnetwork", nil
							}
							if mode == "foreign-after-create" {
								return "foreignreplacement", nil
							}
							if mode == "creation-lag" && creationPolls < 3 {
								return "", nil
							}
							return "newnetwork", nil
						}
						return "oldnetwork", nil
					case a[0] == "network" && a[1] == "create":
						creates++
						if !removed || mode == "removal-lag" && removalPolls < 3 || mode == "name-conflict" && creates == 1 {
							_, _ = io.WriteString(c.Stderr, "Error response from daemon: network with name kpl-peers already exists\n")
							return "", fixtureNetworkCreateExitError{}
						}
						if mode == "uncertain-create" {
							return "", errors.New("Docker create response lost")
						}
						created = true
						return "newnetwork", nil
					case a[0] == "network" && a[1] == "inspect":
						switch a[3] {
						case "{{.Id}}":
							return "oldnetwork", nil
						case swarmIPAMFormat:
							return `"10.42.0.0/24"|"10.42.0.1"|""|0`, nil
						case swarmResetFormat:
							if created && mode == "creation-lag" && creationPolls < 3 {
								return "", errors.New("network is not yet visible")
							}
							return expected, nil
						}
					}
					return "", fmt.Errorf("unexpected Docker call: %v", a)
				}
				err := s.resetPeerNetwork(t.Context())
				success := mode == "removal-lag" || mode == "name-conflict" || mode == "creation-lag" || mode == "retiring-after-create" || mode == "both-known-after-create"
				if success && (err != nil || !created) {
					t.Fatalf("transient Docker convergence failed reset: %v", err)
				}
				if !success && err == nil {
					t.Fatal("unverified reset was reported successful")
				}
				if mode == "replacement" && (creates != 0 || !strings.Contains(err.Error(), "replacement")) {
					t.Fatalf("replacement was not protected: creates=%d err=%v", creates, err)
				}
				if mode == "never-removed" && creates != 0 {
					t.Fatal("create ran before confirmed removal")
				}
				if mode == "foreign-after-create" && (creates != 1 || !strings.Contains(err.Error(), "replacement")) {
					t.Fatalf("unexpected replacement after create was accepted or replayed: creates=%d err=%v", creates, err)
				}
				if mode == "uncertain-create" && creates != 1 {
					t.Fatal("ambiguous creation was replayed")
				}
			})
		})
	}
}

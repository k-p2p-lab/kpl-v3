package master

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestImageLayoutRequiresExactGroupedMarker(t *testing.T) {
	for _, label := range []string{"grouped-v1", "grouped-v1\n", "grouped-v1\r\n", "", "<no value>", "legacy", " grouped-v1", "grouped-v1 ", "grouped-v1\ngrouped-v1"} {
		t.Run(label, func(t *testing.T) {
			s, r, _ := newSwarmFixture(t, "deploy")
			s.config["KPL_IMAGE"] = "registry.example.test/lab@sha256:" + strings.Repeat("b", 64)
			r.handle = func(_ context.Context, c Command) (string, error) {
				if !slices.Equal(c.Args, []string{"image", "inspect", "--format", cliImageLayoutFormat, s.config["KPL_IMAGE"]}) {
					t.Fatalf("unexpected image gate command: %q", c.Args)
				}
				if swarmCommandEnv(c, "DOCKER_DEFAULT_PLATFORM") != "" {
					t.Fatal("image gate inherited an unrelated default platform")
				}
				return label, nil
			}
			wantSuccess := label == cliImageLayout || label == cliImageLayout+"\n" || label == cliImageLayout+"\r\n"
			if err := s.verifyImageLayout(t.Context()); (err == nil) != wantSuccess {
				t.Fatalf("layout %q accepted=%t error=%v", label, wantSuccess, err)
			}
		})
	}
}

func TestImageLayoutMismatchStopsBeforeAnyDeploymentMutation(t *testing.T) {
	for _, command := range []string{"deploy", "restart"} {
		for _, mode := range []string{"missing-marker", "legacy", "inspect-failure"} {
			t.Run(command+"/"+mode, func(t *testing.T) {
				s, r, _ := newSwarmFixture(t, command)
				m := &swarmLifecycleModel{paused: map[string]bool{}}
				r.handle = func(ctx context.Context, c Command) (string, error) {
					if len(c.Args) > 1 && c.Args[0] == "image" && c.Args[1] == "inspect" {
						if mode == "inspect-failure" {
							return "", errors.New("local image unavailable")
						}
						if mode == "legacy" {
							return "legacy", nil
						}
						return "<no value>", nil
					}
					return m.handle(ctx, c)
				}
				err := s.a.RunSwarm(t.Context(), command, []string{"--image", "sha256:" + strings.Repeat("b", 64)})
				if err == nil || !strings.Contains(err.Error(), "no deployment changes made") {
					t.Fatalf("incompatible image gate result=%v", err)
				}
				if swarmCallIndex(r.calls, "pull") < 0 || swarmCallIndex(r.calls, "image", "inspect") < 0 {
					t.Fatal("gate ran without checking the actual pulled image")
				}
				for _, parts := range [][]string{{"service", "update"}, {"stack", "deploy"}, {"node", "update"}, {"network", "create"}, {"build"}, {"push"}} {
					if swarmCallIndex(r.calls, parts...) >= 0 {
						t.Fatalf("image layout failure changed deployment: %v", parts)
					}
				}
			})
		}
	}
}

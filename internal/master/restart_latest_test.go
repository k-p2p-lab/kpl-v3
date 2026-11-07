package master

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
)

func TestSwarmDeployAndRestartResolveFreshTagEveryInvocation(t *testing.T) {
	for _, operation := range []string{"deploy", "restart"} {
		t.Run(operation, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, operation)
			before, err := os.ReadFile(s.a.EnvFile)
			if err != nil {
				t.Fatal(err)
			}
			for _, character := range []string{"a", "b"} {
				digest := "sha256:" + strings.Repeat(character, 64)
				model := &swarmLifecycleModel{paused: map[string]bool{}}
				runner.calls = nil
				runner.handle = func(ctx context.Context, c Command) (string, error) {
					if c.Args[0] == "pull" {
						return "Digest: " + digest, nil
					}
					return model.handle(ctx, c)
				}
				if err := s.a.RunSwarm(t.Context(), operation, nil); err != nil {
					t.Fatal(err)
				}
				pull := swarmCallIndex(runner.calls, "pull", "registry.example.test/lab:v3")
				deploy := swarmCallIndex(runner.calls, "stack", "deploy")
				if pull < 0 || deploy <= pull || swarmCommandEnv(runner.calls[deploy], "KPL_IMAGE") != "registry.example.test/lab@"+digest {
					t.Fatalf("%s reused a previous digest or did not resolve the current tag", operation)
				}
				for _, command := range []string{"build", "buildx", "push"} {
					if swarmCallIndex(runner.calls, command) >= 0 {
						t.Fatalf("%s unexpectedly invoked %s", operation, command)
					}
				}
			}
			after, err := os.ReadFile(s.a.EnvFile)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("resolving a current image pinned or changed the saved configuration")
			}
		})
	}
}

func TestSwarmRestartRequiresSeparateImagePublishForPlatforms(t *testing.T) {
	for _, args := range [][]string{{"--platforms", "linux/amd64"}, {"--platforms=linux/arm64"}} {
		s, runner, _ := newSwarmFixture(t, "restart")
		err := s.a.RunSwarm(t.Context(), "restart", args)
		if ExitCode(err) != 2 || !strings.Contains(err.Error(), "kpl image publish --platforms CSV") || len(runner.calls) != 0 {
			t.Fatalf("restart rebuilt source or omitted migration guidance: %q => %v", args, err)
		}
	}
}

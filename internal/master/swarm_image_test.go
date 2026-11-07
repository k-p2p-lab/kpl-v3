package master

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestSwarmImageSelectionOptions(t *testing.T) {
	digest := "sha256:" + strings.Repeat("b", 64)
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"deploy", []string{"--image", digest, "--workers"}},
		{"deploy", []string{"--workers", "--image=" + digest}},
		{"restart", []string{"node1", "--image", "registry.test:5000/lab:v3@" + digest, "node2"}},
	} {
		o, err := parseSwarmOptions(tc.name, tc.args)
		if err != nil || o.image == "" {
			t.Fatalf("valid selection %v: %+v, %v", tc.args, o, err)
		}
	}
	for _, args := range [][]string{
		{"--image"}, {"--image="}, {"--image", "registry.test/lab:latest"},
		{"--image", "sha256:bad"}, {"--image", "registry.test/lab@sha256:BAD"},
		{"--image", digest, "--image", digest},
		{"--platforms", "linux/amd64", "--image", digest},
		{"--image", digest, "--platforms=linux/amd64"},
		{"--platforms=linux/amd64", "--platforms", "linux/arm64"},
		{"--image", digest, "--workers", "node1"},
	} {
		s, r, _ := newSwarmFixture(t, "restart")
		if err := s.a.RunSwarm(t.Context(), "restart", args); err == nil {
			t.Fatalf("accepted invalid selection: %v", args)
		}
		if len(r.calls) != 0 {
			t.Fatalf("invalid selection contacted Docker: %v", args)
		}
	}
}

func TestRequestedImageReferencePreservesRepositoryAndRegistryPort(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, configured := range []string{
		"registry.test:5000/team/lab:latest",
		"registry.test:5000/team/lab@sha256:" + strings.Repeat("b", 64),
		"registry.test:5000/team/lab:old@sha256:" + strings.Repeat("b", 64),
	} {
		got, err := requestedImageReference(configured, digest)
		if err != nil || got != "registry.test:5000/team/lab@"+digest {
			t.Fatalf("repository resolution %q: %q, %v", configured, got, err)
		}
	}
	if _, err := requestedImageReference("", digest); err == nil {
		t.Fatal("bare digest without a repository accepted")
	}
	full := "other.test/lab@" + digest
	if got, err := requestedImageReference("", full); err != nil || got != full {
		t.Fatalf("explicit repository changed: %q, %v", got, err)
	}
}

func TestSwarmRestartReusesSelectedDigestWithoutBuildOrPersistentChanges(t *testing.T) {
	digest := "sha256:" + strings.Repeat("b", 64)
	for _, mode := range []string{"bare", "full", "configured"} {
		t.Run(mode, func(t *testing.T) {
			s, r, _ := newSwarmFixture(t, "restart")
			image := "registry.example.test/lab@" + digest
			args := []string{"--image", digest}
			if mode == "full" {
				image = "other.test:5000/team/lab@" + digest
				args = []string{"--image", image}
			} else if mode == "configured" {
				s.config["KPL_IMAGE"] = image
				s.a = s.a.WithConfig(s.config)
				args = nil
			}
			before, err := os.ReadFile(s.a.EnvFile)
			if err != nil {
				t.Fatal(err)
			}
			m := &swarmLifecycleModel{paused: map[string]bool{}}
			r.handle = m.handle
			if err := s.a.RunSwarm(t.Context(), "restart", args); err != nil {
				t.Fatal(err)
			}
			if swarmCallIndex(r.calls, "build") >= 0 || swarmCallIndex(r.calls, "buildx") >= 0 || swarmCallIndex(r.calls, "push") >= 0 {
				t.Fatal("digest selection built or pushed an image")
			}
			pull := swarmCallIndex(r.calls, "pull", image)
			stop := swarmCallIndex(r.calls, "service", "update")
			deploy := swarmCallIndex(r.calls, "stack", "deploy")
			if pull < 0 || stop <= pull || deploy <= stop {
				t.Fatalf("image not checked before services stopped: pull=%d stop=%d deploy=%d", pull, stop, deploy)
			}
			for _, call := range r.calls {
				if got := swarmCommandEnv(call, "KPL_IMAGE"); got != image {
					t.Fatalf("operation used a different image: %v: %q", call.Args, got)
				}
			}
			after, err := os.ReadFile(s.a.EnvFile)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("digest selection changed the saved configuration")
			}
		})
	}
}

func TestSwarmRestartSelectedImageFailureKeepsExistingServices(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, mode := range []string{"missing", "mismatch", "ambiguous", "platforms"} {
		t.Run(mode, func(t *testing.T) {
			s, r, _ := newSwarmFixture(t, "restart")
			m := &swarmLifecycleModel{paused: map[string]bool{}}
			r.handle = func(ctx context.Context, c Command) (string, error) {
				if c.Args[0] == "pull" {
					if mode == "missing" {
						return "", errors.New("manifest not found")
					}
					if mode == "ambiguous" {
						return "Digest: " + digest + "\nDigest: " + digest, nil
					}
				}
				return m.handle(ctx, c)
			}
			args := []string{"--image", digest}
			if mode == "platforms" {
				s.config["KPL_IMAGE"] = "registry.example.test/lab@" + digest
				s.a = s.a.WithConfig(s.config)
				args = []string{"--platforms", "linux/amd64"}
			}
			if err := s.a.RunSwarm(t.Context(), "restart", args); err == nil {
				t.Fatal("invalid or unavailable selected image accepted")
			}
			for _, parts := range [][]string{{"service", "update"}, {"stack", "deploy"}, {"node", "update"}, {"network", "create"}, {"build"}, {"push"}} {
				if swarmCallIndex(r.calls, parts...) >= 0 {
					t.Fatalf("failed image check changed deployment: %v", parts)
				}
			}
		})
	}
}

func TestSwarmImageInspectReadsOnlyLocalMetadata(t *testing.T) {
	s, r, out := newSwarmFixture(t, "image-inspect")
	digest := "sha256:" + strings.Repeat("c", 64)
	imageID := "sha256:" + strings.Repeat("d", 64)
	r.handle = func(_ context.Context, c Command) (string, error) {
		if !slices.Equal(c.Args, []string{"image", "inspect", "--format", "{{json .}}", "registry.example.test/lab@" + digest}) {
			t.Fatalf("inspect performed an unexpected operation: %v", c.Args)
		}
		return `{"Id":"` + imageID + `","RepoDigests":["registry.example.test/lab@` + digest + `"],"Config":{"Env":["PASSWORD=do-not-print"]}}`, nil
	}
	if err := s.a.RunSwarm(t.Context(), "image-inspect", []string{digest}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Configured image: registry.example.test/lab:v3", "Local image ID: " + imageID, "Registry digest: registry.example.test/lab@" + digest} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing image metadata %q: %s", want, out.String())
		}
	}
	if len(r.calls) != 1 || strings.Contains(out.String(), "do-not-print") {
		t.Fatal("image inspection contacted registry or exposed image environment")
	}
}

func TestSwarmImageInspectRejectsMalformedMetadata(t *testing.T) {
	for _, metadata := range []string{`null`, `{}`, `{"Id":"sha256:bad"}`, `{"Id":"sha256:` + strings.Repeat("a", 64) + `","RepoDigests":["image:tag"]}`} {
		s, r, _ := newSwarmFixture(t, "image-inspect")
		r.handle = func(context.Context, Command) (string, error) { return metadata, nil }
		if err := s.a.RunSwarm(t.Context(), "image-inspect", nil); err == nil {
			t.Fatalf("accepted malformed metadata: %s", metadata)
		}
	}
}

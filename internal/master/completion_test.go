package master

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type completionRunner struct {
	t       *testing.T
	calls   []Command
	failure bool
}

func (r *completionRunner) Run(context.Context, Command) error {
	r.t.Fatal("completion tried to run a command")
	return errors.New("unexpected execution")
}

func (r *completionRunner) Output(ctx context.Context, c Command) ([]byte, error) {
	r.t.Helper()
	r.calls = append(r.calls, c)
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > time.Second || time.Until(deadline) <= 0 {
		r.t.Fatalf("completion query has no bounded deadline: %v", deadline)
	}
	if c.Name != "docker" || c.Stdin != nil || c.Stderr != io.Discard || c.CancelGrace != 100*time.Millisecond {
		r.t.Fatalf("unexpected completion process contract: %+v", c)
	}
	if r.failure {
		return nil, errors.New("offline")
	}
	switch strings.Join(c.Args, " ") {
	case "node ls --format {{.ID}} {{.Hostname}}":
		return []byte("node1 worker-a\nnode2 worker-b\n"), nil
	case "node ls --format {{.ID}}":
		return []byte("node1\nnode2\n"), nil
	case "context ls --format {{.Name}}":
		return []byte("control-node\nremote-node\n"), nil
	default:
		r.t.Fatalf("completion performed a non-read-only query: %v", c.Args)
		return nil, errors.New("unexpected query")
	}
}

func completionFixture(t *testing.T) (*App, *completionRunner, *bytes.Buffer) {
	t.Helper()
	out := &bytes.Buffer{}
	runner := &completionRunner{t: t}
	return &App{Repo: "/unavailable/runtime", EnvFile: "/unavailable/private.env", Environ: []string{"DOCKER_CONTEXT=manager-context"}, Runner: runner, Stdout: out, Stderr: io.Discard}, runner, out
}

func completionCandidates(t *testing.T, a *App, out *bytes.Buffer, args ...string) []string {
	t.Helper()
	out.Reset()
	if err := a.Complete(t.Context(), args); err != nil {
		t.Fatal(err)
	}
	if out.Len() == 0 {
		return nil
	}
	return strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
}

func TestCompletionPlatformsAndNodeSelection(t *testing.T) {
	a, runner, out := completionFixture(t)
	for _, command := range []string{"publish", "restart"} {
		got := completionCandidates(t, a, out, "swarm", command, "--platforms", "linux/")
		if len(got) != 4 || !strings.Contains(strings.Join(got, "\n"), "linux/amd64,linux/arm64") {
			t.Fatalf("%s platform candidates: %q", command, got)
		}
	}
	if len(runner.calls) != 0 {
		t.Fatal("static completion contacted Docker")
	}
	for _, command := range []string{"deploy", "restart", "add-node", "remove-node"} {
		got := completionCandidates(t, a, out, "swarm", command, "worker-a", "--")
		for _, candidate := range got {
			if candidate == "--all" || candidate == "--workers" || candidate == "--all-excluding-self" || candidate == "--platforms" {
				t.Fatalf("%s offered an invalid option after an explicit node: %q", command, got)
			}
		}
		got = completionCandidates(t, a, out, "swarm", command, "worker-a", "")
		if !reflect.DeepEqual(got, []string{"node2", "worker-b"}) {
			t.Fatalf("%s failed to exclude both forms of an already-selected node: %q", command, got)
		}
		if got := completionCandidates(t, a, out, "swarm", command, "--workers", ""); len(got) != 0 {
			t.Fatalf("%s offered nodes/options after a selector: %q", command, got)
		}
	}
	got := completionCandidates(t, a, out, "swarm", "restart", "--platforms", "linux/amd64", "--")
	if !strings.Contains(strings.Join(got, "\n"), "--workers") {
		t.Fatalf("platform value counted as a node: %q", got)
	}
}

func TestCompletionDynamicQueriesPreserveContextAndNeverReadConfig(t *testing.T) {
	a, runner, out := completionFixture(t)
	if got := completionCandidates(t, a, out, "swarm", "logs", "auth", "--context", "control"); !reflect.DeepEqual(got, []string{"control-node"}) {
		t.Fatalf("context candidates: %q", got)
	}
	if got := completionCandidates(t, a, out, "swarm", "configure", "KPL_CONTROL_NODE_ID=node"); !reflect.DeepEqual(got, []string{"KPL_CONTROL_NODE_ID=node1", "KPL_CONTROL_NODE_ID=node2"}) {
		t.Fatalf("node ID setting candidates: %q", got)
	}
	for _, call := range runner.calls {
		if !reflect.DeepEqual(call.Env, a.Environ) {
			t.Fatal("completion changed the caller's Docker context")
		}
	}
	before := len(runner.calls)
	if got := completionCandidates(t, a, out, "swarm", "configure", "KPL_PASSWORD="); len(got) != 0 || len(runner.calls) != before {
		t.Fatalf("completion tried to suggest secret values: %q", got)
	}
	runner.failure = true
	if got := completionCandidates(t, a, out, "swarm", "deploy", "worker"); len(got) != 0 {
		t.Fatalf("failed Docker query produced candidates: %q", got)
	}
	if got := completionCandidates(t, a, out, "swarm", "de"); !reflect.DeepEqual(got, []string{"deploy"}) {
		t.Fatalf("offline Docker prevented static completion: %q", got)
	}
}

func TestCompletionPreservesLiteralFilePaths(t *testing.T) {
	a, runner, out := completionFixture(t)
	dir := t.TempDir()
	name := "scenario with space\tand-tab.yaml"
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("not read by completion"), 0600); err != nil {
		t.Fatal(err)
	}
	prefix := filepath.Join(dir, "scenario with space\t")
	for _, args := range [][]string{
		{"swarm", "scenario", prefix},
		{"validate", "--scenario", prefix},
		{"--env-file", prefix},
	} {
		if got := completionCandidates(t, a, out, args...); !reflect.DeepEqual(got, []string{path}) {
			t.Fatalf("file path was split or rewritten: %q => %q", args, got)
		}
	}
	if got := completionCandidates(t, a, out, "--env-file="+prefix); !reflect.DeepEqual(got, []string{"--env-file=" + path}) {
		t.Fatalf("equals-form path candidates: %q", got)
	}
	if len(runner.calls) != 0 {
		t.Fatal("file completion contacted Docker")
	}
}

func runCompletionBash(t *testing.T, body string, extraEnv ...string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	client := filepath.Join(dir, "operator with space")
	calls := filepath.Join(dir, "calls")
	count := filepath.Join(dir, "count")
	fake := "#!/bin/bash\nprintf x >> \"$KPL_COMPLETION_COUNT\"\nprintf '%s\\0' \"$@\" >> \"$KPL_COMPLETION_CALLS\"\nprintf '%s\\n' \"$KPL_COMPLETION_CANDIDATE\"\n"
	if err := os.WriteFile(client, []byte(fake), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "--noprofile", "--norc", "-c", "set -eu\n"+bashCompletion+"\n"+body)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "KPL_COMPLETION_CLIENT="+client, "KPL_COMPLETION_CALLS="+calls, "KPL_COMPLETION_COUNT="+count, "KPL_COMPLETION_CANDIDATE=worker-a")
	cmd.Env = append(cmd.Env, extraEnv...)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated Bash completion failed: %v\n%s", err, output)
	}
	data, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	countData, err := os.ReadFile(count)
	if err != nil {
		t.Fatal(err)
	}
	return string(data), string(countData)
}

func TestCompletionBashCacheFollowsDirectoryAndDockerContext(t *testing.T) {
	_, count := runCompletionBash(t, `
COMP_WORDS=("$KPL_COMPLETION_CLIENT" swarm deploy worker)
COMP_CWORD=3
_kpl_complete
_kpl_complete
[[ ${COMPREPLY[0]} == worker-a ]]
mkdir elsewhere
cd elsewhere
_kpl_complete
for key in DOCKER_CONTEXT DOCKER_HOST DOCKER_CONFIG DOCKER_TLS_VERIFY DOCKER_CERT_PATH; do
    export "$key=alternate-value"
    _kpl_complete
    _kpl_complete
done
_KPL_CACHE_AT=$((SECONDS-5))
_kpl_complete
`)
	if count != strings.Repeat("x", 8) {
		t.Fatalf("wrong cache invalidation count: %q", count)
	}
}

func TestCompletionBashPreservesSpacesTabsAndEquals(t *testing.T) {
	path := "some directory/scenario\twith spaces.yaml"
	calls, count := runCompletionBash(t, `
COMP_WORDS=("$KPL_COMPLETION_CLIENT" swarm scenario $'some directory/scenario\t')
COMP_CWORD=3
_kpl_complete
[[ ${#COMPREPLY[@]} == 1 && ${COMPREPLY[0]} == "$KPL_COMPLETION_CANDIDATE" ]]
`, "KPL_COMPLETION_CANDIDATE="+path)
	want := "__complete\x00swarm\x00scenario\x00some directory/scenario\t\x00"
	if calls != want || count != "x" {
		t.Fatalf("Bash split literal file input: %q, calls %q", calls, count)
	}
	calls, count = runCompletionBash(t, `
COMP_WORDS=("$KPL_COMPLETION_CLIENT" swarm configure KPL_CONTROL_NODE_ID = node)
COMP_CWORD=5
_kpl_complete
[[ ${#COMPREPLY[@]} == 1 && ${COMPREPLY[0]} == node1 ]]
COMP_WORDS=("$KPL_COMPLETION_CLIENT" swarm configure KPL_CONTROL_NODE_ID =)
COMP_CWORD=4
_kpl_complete
[[ ${#COMPREPLY[@]} == 1 && ${COMPREPLY[0]} == node1 ]]
`, "KPL_COMPLETION_CANDIDATE=KPL_CONTROL_NODE_ID=node1")
	want = "__complete\x00swarm\x00configure\x00KPL_CONTROL_NODE_ID=node\x00__complete\x00swarm\x00configure\x00KPL_CONTROL_NODE_ID=\x00"
	if calls != want || count != "xx" {
		t.Fatalf("Bash did not reconstruct the setting value: %q, calls %q", calls, count)
	}
}

func TestCompletionUsesAliasesAndLiteralArgumentBoundaries(t *testing.T) {
	a, runner, out := completionFixture(t)
	cwd := t.TempDir()
	t.Chdir(cwd)
	for _, name := range []string{"--help.yaml", "--repo=fixture.yaml", "scenario.yaml"} {
		if err := os.WriteFile(name, []byte("completion must not read this file"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{
		{"validate", "--", "--h"},
		{"validate", "--scenario", "--h"},
		{"validate", "--scenario", "--", "--h"},
		{"swarm", "scenario", "--", "--h"},
		{"swarm-scenario", "--", "--h"},
	} {
		if got := completionCandidates(t, a, out, args...); !reflect.DeepEqual(got, []string{"--help.yaml"}) {
			t.Fatalf("literal path boundary produced option candidates: %q => %q", args, got)
		}
	}
	if got := completionCandidates(t, a, out, "swarm-scenario", "--", "--repo="); !reflect.DeepEqual(got, []string{"--repo=fixture.yaml"}) {
		t.Fatalf("literal equals-form filename became a global option: %q", got)
	}
	for _, args := range [][]string{
		{"swarm", "deploy", "--", "--"},
		{"swarm-deploy", "--", "--"},
		{"swarm-publish", "--platforms", "--"},
		{"swarm", "publish", "--", "--"},
		{"swarm-logs", "auth", "--", "--"},
	} {
		if got := completionCandidates(t, a, out, args...); len(got) != 0 {
			t.Fatalf("opaque or option value position offered options: %q => %q", args, got)
		}
	}
	for _, command := range []string{"swarm-publish", "swarm-restart"} {
		if got := completionCandidates(t, a, out, command, "--platforms", "linux/"); len(got) != 4 {
			t.Fatalf("alias lost platform completion: %q", got)
		}
	}
	if got := completionCandidates(t, a, out, "test-", ""); len(got) != 0 {
		t.Fatalf("unknown scope produced candidates: %q", got)
	}
	if len(runner.calls) != 0 {
		t.Fatal("static literal completion queried Docker")
	}
	if got := completionCandidates(t, a, out, "swarm-deploy", "worker-a", ""); !reflect.DeepEqual(got, []string{"node2", "worker-b"}) {
		t.Fatalf("alias lost explicit node exclusion: %q", got)
	}
}

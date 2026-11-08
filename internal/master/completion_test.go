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
	"sort"
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

func TestCompletionRestartRepublishOptions(t *testing.T) {
	a, runner, out := completionFixture(t)
	for _, tc := range []struct {
		words []string
		want  string
		omit  []string
	}{
		{[]string{"swarm", "restart", "--"}, "--republish", []string{"--platforms"}},
		{[]string{"swarm", "restart", "--workers", "--"}, "--republish", nil},
		{[]string{"swarm", "restart", "--platforms", "linux/amd64", "--"}, "--republish", []string{"--image", "--platforms"}},
		{[]string{"swarm", "restart", "--republish", "--"}, "--platforms", []string{"--image", "--republish"}},
		{[]string{"swarm", "restart", "worker-a", "--republish", "--"}, "--platforms", []string{"--all", "--workers", "--image"}},
		{[]string{"swarm", "restart", "--republish", "--workers", "--"}, "--platforms", []string{"--image"}},
		{[]string{"swarm", "restart", "--image", "sha256:" + strings.Repeat("a", 64), "--"}, "--workers", []string{"--republish", "--platforms"}},
	} {
		got := completionCandidates(t, a, out, tc.words...)
		contains := func(value string) bool {
			for _, candidate := range got {
				if candidate == value {
					return true
				}
			}
			return false
		}
		if !contains(tc.want) {
			t.Fatalf("%q missing %s: %q", tc.words, tc.want, got)
		}
		for _, value := range tc.omit {
			if contains(value) {
				t.Fatalf("%q suggested incompatible %s", tc.words, value)
			}
		}
	}
	for _, words := range [][]string{
		{"swarm", "restart", "--republish", "--platforms", "linux/"},
		{"swarm", "restart", "--platforms", "linux/"},
		{"swarm", "restart", "--republish", "--platforms=linux/"},
	} {
		if got := completionCandidates(t, a, out, words...); len(got) != len(completionPlatforms) {
			t.Fatalf("republish platform completion %q: %q", words, got)
		}
	}
	if len(runner.calls) != 0 {
		t.Fatal("republish flag completion contacted Docker")
	}
}

func TestCompletionPlatformsAndNodeSelection(t *testing.T) {
	a, runner, out := completionFixture(t)
	for _, command := range []string{"publish"} {
		got := completionCandidates(t, a, out, "image", command, "--platforms", "linux/")
		if len(got) != 4 || !strings.Contains(strings.Join(got, "\n"), "linux/amd64,linux/arm64") {
			t.Fatalf("%s platform candidates: %q", command, got)
		}
	}
	if len(runner.calls) != 0 {
		t.Fatal("static completion contacted Docker")
	}
	for _, command := range [][]string{{"swarm", "deploy"}, {"swarm", "restart"}, {"swarm", "node", "add"}, {"swarm", "node", "remove"}} {
		got := completionCandidates(t, a, out, append(append([]string{}, command...), "worker-a", "--")...)
		for _, candidate := range got {
			if candidate == "--all" || candidate == "--workers" || candidate == "--all-excluding-self" || candidate == "--platforms" {
				t.Fatalf("%q offered an invalid option after an explicit node: %q", command, got)
			}
		}
		got = completionCandidates(t, a, out, append(append([]string{}, command...), "worker-a", "")...)
		if !reflect.DeepEqual(got, []string{"node2", "worker-b"}) {
			t.Fatalf("%q failed to exclude both forms of an already-selected node: %q", command, got)
		}
		if got := completionCandidates(t, a, out, append(append([]string{}, command...), "--workers", "")...); len(got) != 0 {
			t.Fatalf("%q offered nodes/options after a selector: %q", command, got)
		}
	}
	got := completionCandidates(t, a, out, "swarm", "restart", "--image", "sha256:"+strings.Repeat("a", 64), "--")
	if !strings.Contains(strings.Join(got, "\n"), "--workers") {
		t.Fatalf("image value counted as a node: %q", got)
	}
}

func TestCompletionModernTreeAndImageOptionBoundaries(t *testing.T) {
	a, runner, out := completionFixture(t)
	for _, test := range []struct {
		args []string
		want []string
	}{
		{[]string{"dev", ""}, []string{"build", "check", "test", "vet"}},
		{[]string{"config", ""}, []string{"credentials", "init", "set", "show", "version"}},
		{[]string{"image", ""}, []string{"inspect", "login", "publish"}},
		{[]string{"scenario", ""}, []string{"show", "validate"}},
		{[]string{"swarm", "node", ""}, []string{"add", "list", "remove"}},
		{[]string{"swarm", "network", ""}, []string{"reset"}},
		{[]string{"dev", "test", ""}, []string{"go", "linux", "race", "swarm", "ui"}},
		{[]string{"config", "version", "--j"}, []string{"--json"}},
		{[]string{"version", "--j"}, []string{"--json"}},
		{[]string{"help", "config", "s"}, []string{"set", "show"}},
		{[]string{"image", "publish", "--platforms", "linux/"}, completionPlatforms},
	} {
		want := append([]string(nil), test.want...)
		sort.Strings(want)
		if got := completionCandidates(t, a, out, test.args...); !reflect.DeepEqual(got, want) {
			t.Fatalf("%q => %q; want %q", test.args, got, want)
		}
	}
	for _, args := range [][]string{
		{"swarm", "deploy", "--image", "sha256:"},
		{"swarm", "restart", "--image", "--repo="},
		{"swarm", "deploy", "--image=sha256:"},
		{"run", "peer", "--repo", "/tmp/"},
		{"--repo", "/missing-checkout", "run", "peer", "--env-file", "/tmp/"},
	} {
		if got := completionCandidates(t, a, out, args...); len(got) != 0 {
			t.Fatalf("image value became an option or node query: %q => %q", args, got)
		}
	}
	if len(runner.calls) != 0 {
		t.Fatal("static modern completion contacted Docker")
	}
	if got := completionCandidates(t, a, out, "swarm", "node", "add", "worker-a", ""); !reflect.DeepEqual(got, []string{"node2", "worker-b"}) {
		t.Fatalf("modern node selection lost exclusion: %q", got)
	}
	if got := completionCandidates(t, a, out, "swarm", "restart", "--image", "sha256:"+strings.Repeat("a", 64), "--"); strings.Contains(strings.Join(got, "\n"), "--platforms") || !strings.Contains(strings.Join(got, "\n"), "--workers") {
		t.Fatalf("image value counted as a node or allowed rebuild: %q", got)
	}
}

func TestCompletionExcludesRemovedEntrypoints(t *testing.T) {
	a, runner, out := completionFixture(t)
	for _, args := range [][]string{
		{"build", ""}, {"validate", ""}, {"test-race", ""}, {"swarm-restart", ""},
		{"controller", ""}, {"config-version", ""}, {"swarm", "configure", "KPL_"},
		{"swarm", "add-node", "worker"}, {"swarm", "publish", "--platforms", "linux/"},
	} {
		if got := completionCandidates(t, a, out, args...); len(got) != 0 {
			t.Fatalf("removed entrypoint or build option remained in completion: %q => %q", args, got)
		}
	}
	if len(runner.calls) != 0 {
		t.Fatal("removed entrypoint completion contacted Docker")
	}
}

func TestCompletionDynamicQueriesPreserveContextAndNeverReadConfig(t *testing.T) {
	a, runner, out := completionFixture(t)
	if got := completionCandidates(t, a, out, "swarm", "logs", "auth", "--context", "control"); !reflect.DeepEqual(got, []string{"control-node"}) {
		t.Fatalf("context candidates: %q", got)
	}
	if got := completionCandidates(t, a, out, "config", "set", "KPL_CONTROL_NODE_ID=node"); !reflect.DeepEqual(got, []string{"KPL_CONTROL_NODE_ID=node1", "KPL_CONTROL_NODE_ID=node2"}) {
		t.Fatalf("node ID setting candidates: %q", got)
	}
	for _, call := range runner.calls {
		if !reflect.DeepEqual(call.Env, a.Environ) {
			t.Fatal("completion changed the caller's Docker context")
		}
	}
	before := len(runner.calls)
	if got := completionCandidates(t, a, out, "config", "set", "KPL_PASSWORD="); len(got) != 0 || len(runner.calls) != before {
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
		{"scenario", "show", prefix},
		{"scenario", "validate", "--scenario", prefix},
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
COMP_WORDS=("$KPL_COMPLETION_CLIENT" scenario show $'some directory/scenario\t')
COMP_CWORD=3
_kpl_complete
[[ ${#COMPREPLY[@]} == 1 && ${COMPREPLY[0]} == "$KPL_COMPLETION_CANDIDATE" ]]
`, "KPL_COMPLETION_CANDIDATE="+path)
	want := "__complete\x00scenario\x00show\x00some directory/scenario\t\x00"
	if calls != want || count != "x" {
		t.Fatalf("Bash split literal file input: %q, calls %q", calls, count)
	}
	calls, count = runCompletionBash(t, `
COMP_WORDS=("$KPL_COMPLETION_CLIENT" config set KPL_CONTROL_NODE_ID = node)
COMP_CWORD=5
_kpl_complete
[[ ${#COMPREPLY[@]} == 1 && ${COMPREPLY[0]} == node1 ]]
COMP_WORDS=("$KPL_COMPLETION_CLIENT" config set KPL_CONTROL_NODE_ID =)
COMP_CWORD=4
_kpl_complete
[[ ${#COMPREPLY[@]} == 1 && ${COMPREPLY[0]} == node1 ]]
`, "KPL_COMPLETION_CANDIDATE=KPL_CONTROL_NODE_ID=node1")
	want = "__complete\x00config\x00set\x00KPL_CONTROL_NODE_ID=node\x00__complete\x00config\x00set\x00KPL_CONTROL_NODE_ID=\x00"
	if calls != want || count != "xx" {
		t.Fatalf("Bash did not reconstruct the setting value: %q, calls %q", calls, count)
	}
}

func TestCompletionUsesCanonicalPathsAndLiteralArgumentBoundaries(t *testing.T) {
	a, runner, out := completionFixture(t)
	cwd := t.TempDir()
	t.Chdir(cwd)
	for _, name := range []string{"--help.yaml", "--repo=fixture.yaml", "scenario.yaml"} {
		if err := os.WriteFile(name, []byte("completion must not read this file"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{
		{"scenario", "validate", "--", "--h"},
		{"scenario", "validate", "--scenario", "--h"},
		{"scenario", "validate", "--scenario", "--", "--h"},
		{"scenario", "show", "--", "--h"},
	} {
		if got := completionCandidates(t, a, out, args...); !reflect.DeepEqual(got, []string{"--help.yaml"}) {
			t.Fatalf("literal path boundary produced option candidates: %q => %q", args, got)
		}
	}
	if got := completionCandidates(t, a, out, "scenario", "show", "--", "--repo="); !reflect.DeepEqual(got, []string{"--repo=fixture.yaml"}) {
		t.Fatalf("literal equals-form filename became a global option: %q", got)
	}
	for _, args := range [][]string{
		{"swarm", "node", "add", "--", "--"},
		{"swarm", "deploy", "--", "--"},
		{"image", "publish", "--platforms", "--"},
		{"image", "publish", "--", "--"},
		{"swarm", "logs", "auth", "--", "--"},
	} {
		if got := completionCandidates(t, a, out, args...); len(got) != 0 {
			t.Fatalf("opaque or option value position offered options: %q => %q", args, got)
		}
	}
	for _, command := range []string{"publish"} {
		if got := completionCandidates(t, a, out, "image", command, "--platforms", "linux/"); len(got) != 4 {
			t.Fatalf("publish lost platform completion: %q", got)
		}
	}
	if got := completionCandidates(t, a, out, "test-", ""); len(got) != 0 {
		t.Fatalf("unknown scope produced candidates: %q", got)
	}
	if len(runner.calls) != 0 {
		t.Fatal("static literal completion queried Docker")
	}
	if got := completionCandidates(t, a, out, "swarm", "deploy", "worker-a", ""); !reflect.DeepEqual(got, []string{"node2", "worker-b"}) {
		t.Fatalf("node selection lost explicit exclusion: %q", got)
	}
}

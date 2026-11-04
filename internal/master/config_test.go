package master

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
)

type configRunner struct {
	t       *testing.T
	calls   []Command
	respond func(context.Context, Command) (string, error)
}

func (r *configRunner) Run(ctx context.Context, cmd Command) error {
	_, err := r.Output(ctx, cmd)
	return err
}

func (r *configRunner) Output(ctx context.Context, cmd Command) ([]byte, error) {
	r.t.Helper()
	r.calls = append(r.calls, cmd)
	if cmd.Name != "docker" {
		r.t.Fatalf("unexpected executable: %s", cmd.Name)
	}
	if _, ok := ctx.Deadline(); !ok {
		r.t.Fatal("Docker inspection has no timeout")
	}
	if r.respond != nil {
		value, err := r.respond(ctx, cmd)
		return []byte(value), err
	}
	switch strings.Join(cmd.Args, " ") {
	case "info --format {{.Swarm.LocalNodeState}} {{.Swarm.ControlAvailable}}":
		return []byte("active true\n"), nil
	case "info --format {{.Swarm.NodeID}}":
		return []byte("manager1\n"), nil
	default:
		return nil, fmt.Errorf("unexpected Docker call: %v", cmd.Args)
	}
}

func configApp(t *testing.T) (*App, *configRunner, *bytes.Buffer) {
	t.Helper()
	out := &bytes.Buffer{}
	runner := &configRunner{t: t}
	root := t.TempDir()
	return &App{Repo: root, EnvFile: filepath.Join(root, "config with spaces.env"), EnvFileExplicit: true, Runner: runner, Stdout: out, Stderr: io.Discard, Environ: []string{"DOCKER_CONTEXT=fixture-manager"}}, runner, out
}

func TestConfigSettingValidation(t *testing.T) {
	valid := map[string][]string{
		"KPL_STACK_NAME": {"kpl", "other-stack_2"}, "KPL_CONTROL_NODE_ID": {"manager1"}, "KPL_PEER_NETWORK": {"kpl-peers"},
		"KPL_PEER_SUBNET": {"", "10.11.0.0/16", "0.0.0.0/1"}, "KPL_RUN_MIN_FREE_BYTES": {"0", "2147483648"},
		"KPL_IMAGE":    {"registry.example:5000/team/kpl:v3", "team/kpl@sha256:" + strings.Repeat("a", 64), "team/kpl:tag@sha256:" + strings.Repeat("1", 64)},
		"KPL_PASSWORD": {"$(touch should-not-run)", "'literal quote'"}, "KPL_USER": {"operator"}, "KPL_AGENT_METRICS_PORT": {"1", "65535"},
	}
	for key, values := range valid {
		for _, value := range values {
			if err := ValidateSetting(key, value); err != nil {
				t.Errorf("valid %s rejected: %v", key, err)
			}
		}
	}
	invalid := map[string][]string{
		"UNKNOWN": {"ignored"}, "KPL_STACK_NAME": {"Kpl", "_kpl", "-kpl", "kpl.name", "two stacks"}, "KPL_CONTROL_NODE_ID": {"", "-manager", "node/id"},
		"KPL_PEER_NETWORK": {"", "-peers", "peers/name"}, "KPL_PEER_SUBNET": {"10.11.0.1/24", "010.11.0.0/24", "10.0.0.0/08", "10.0.0.0/31", "::/64", "10.11.0.0/33", "10.0.0.0/24 extra"},
		"KPL_RUN_MIN_FREE_BYTES": {"-1", "01", "abc", "9223372036854775808"}, "KPL_MIN_AGENTS": {"0", "08", "-1", "999999999999999999999999"},
		"KPL_DOCKER_TIMEOUT": {"0", "9223372036854775807"}, "KPL_IMAGE": {"repo", "https://repo:v3", "repo:", "/repo:v3", "repo//foo:v3", "repo@other:abc", "repo@sha256:" + strings.Repeat("A", 64), "repo:bad tag", "repo:-bad"},
		"KPL_USER": {"", " operator", "operator\t", strings.Repeat("u", 257)}, "KPL_PASSWORD": {"", "line1\nline2", "nul\x00byte", strings.Repeat("p", 4097)},
		"KPL_AGENT_METRICS_PORT": {"0", "09091", "65536", "-1"},
	}
	for key, values := range invalid {
		for i, value := range values {
			if err := ValidateSetting(key, value); err == nil {
				t.Errorf("invalid %s case %d accepted", key, i)
			}
		}
	}
}

func TestConfigLiteralParsingAndEnvironmentPriority(t *testing.T) {
	values := Config{"KPL_PASSWORD": "$(echo never-executed)", "GRAFANA_ADMIN_PASSWORD": "'quoted password'", "KPL_USER": "\"quoted user\"", "KPL_IMAGE": "registry/team:old"}
	encoded := encodeConfig(values)
	parsed, legacy, err := parseConfig(encoded)
	if err != nil || legacy {
		t.Fatalf("parse: %v legacy=%v", err, legacy)
	}
	for key, value := range values {
		if parsed[key] != value {
			t.Errorf("literal %s changed", key)
		}
	}
	parsed, legacy, err = parseConfig([]byte("# comment\r\nKPL_AGENT_CAPACITY=11\r\nKPL_AGENT_CAPACITY=22\r\nKPL_API_TOKEN=retired-fixture\n"))
	if err != nil || !legacy || parsed["KPL_AGENT_CAPACITY"] != "11" {
		t.Fatal("legacy or duplicate loading compatibility changed")
	}
	a, _, _ := configApp(t)
	if err := os.WriteFile(a.EnvFile, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	a.Environ = append(a.Environ, "KPL_IMAGE=registry/team:environment")
	c, err := a.LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if c["KPL_IMAGE"] != "registry/team:environment" || c["KPL_PASSWORD"] != values["KPL_PASSWORD"] || c["KPL_PEER_NETWORK"] != "kpl-peers" {
		t.Fatal("effective configuration precedence changed")
	}
	for _, input := range []string{"export KPL_USER=user\n", "UNKNOWN=value\n", "not-a-setting\n"} {
		if _, _, err := parseConfig([]byte(input)); err == nil {
			t.Error("malformed config accepted")
		}
	}
}

func TestConfigInitPrivateAndPreservesExisting(t *testing.T) {
	a, runner, out := configApp(t)
	if err := a.ConfigCommand(t.Context(), "init", []string{"KPL_IMAGE=registry.example/team/kpl:v3", "KPL_AGENT_CAPACITY=40", "KPL_MIN_AGENTS=2"}); err != nil {
		t.Fatal(err)
	}
	c, _, err := readConfig(a.EnvFile, true)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(a.EnvFile)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 || c["KPL_CONTROL_NODE_ID"] != "manager1" || c["KPL_AGENT_CAPACITY"] != "40" || c["KPL_MIN_AGENTS"] != "2" {
		t.Fatal("init did not retain private requested settings")
	}
	if !digestPattern.MatchString(c["KPL_PASSWORD"]) || !digestPattern.MatchString(c["GRAFANA_ADMIN_PASSWORD"]) || c["KPL_PASSWORD"] == c["GRAFANA_ADMIN_PASSWORD"] {
		t.Fatal("independent random passwords were not generated")
	}
	if strings.Contains(out.String(), c["KPL_PASSWORD"]) || strings.Contains(out.String(), c["GRAFANA_ADMIN_PASSWORD"]) {
		t.Fatal("init output exposed generated credentials")
	}
	before, _ := os.ReadFile(a.EnvFile)
	if err := a.ConfigCommand(t.Context(), "init", nil); err == nil {
		t.Fatal("second init overwrote config")
	}
	after, _ := os.ReadFile(a.EnvFile)
	if !bytes.Equal(before, after) || len(runner.calls) != 2 {
		t.Fatal("refused init changed files or contacted Docker")
	}
	out.Reset()
	if err := a.ConfigCommand(t.Context(), "config", nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "KPL_PASSWORD=[redacted]") || strings.Contains(out.String(), c["KPL_PASSWORD"]) || strings.Contains(out.String(), c["GRAFANA_ADMIN_PASSWORD"]) {
		t.Fatal("config output did not redact credentials")
	}
}

func TestConfigConfigurePreservesFileValuesAndMigratesLegacy(t *testing.T) {
	a, runner, out := configApp(t)
	original := []byte("KPL_API_TOKEN=retired-fixture\nKPL_IMAGE=registry/team:old\nKPL_AGENT_CAPACITY=37\nGRAFANA_ADMIN_PASSWORD=fixture-grafana\n")
	if err := os.WriteFile(a.EnvFile, original, 0640); err != nil {
		t.Fatal(err)
	}
	a.Environ = append(a.Environ, "KPL_IMAGE=registry/team:environment", "KPL_AGENT_CAPACITY=999", "GRAFANA_ADMIN_PASSWORD=environment-fixture")
	if err := a.ConfigCommand(t.Context(), "configure", []string{"KPL_IMAGE=registry/team:updated", "KPL_USER=operator"}); err != nil {
		t.Fatal(err)
	}
	c, legacy, err := readConfig(a.EnvFile, true)
	if err != nil {
		t.Fatal(err)
	}
	if legacy || c["KPL_IMAGE"] != "registry/team:updated" || c["KPL_AGENT_CAPACITY"] != "37" || c["GRAFANA_ADMIN_PASSWORD"] != "fixture-grafana" || !digestPattern.MatchString(c["KPL_PASSWORD"]) {
		t.Fatal("configure changed unrelated file settings or did not migrate legacy")
	}
	if strings.Contains(out.String(), "retired-fixture") || strings.Contains(out.String(), c["KPL_PASSWORD"]) {
		t.Fatal("migration exposed credentials")
	}
	info, _ := os.Stat(a.EnvFile)
	if info.Mode().Perm() != 0600 || len(runner.calls) != 0 {
		t.Fatal("configure did not remain private and local")
	}
	if err := a.ConfigCommand(t.Context(), "configure", []string{"KPL_AGENT_CAPACITY=38"}); err != nil {
		t.Fatal(err)
	}
	after, _, _ := readConfig(a.EnvFile, true)
	if after["KPL_PASSWORD"] != c["KPL_PASSWORD"] {
		t.Fatal("ordinary configure regenerated password")
	}
}

func TestConfigInvalidChangesAreTransactional(t *testing.T) {
	a, runner, _ := configApp(t)
	original := []byte("KPL_USER=operator\nKPL_PASSWORD=fixture-password\nGRAFANA_ADMIN_PASSWORD=fixture-grafana\n")
	if err := os.WriteFile(a.EnvFile, original, 0600); err != nil {
		t.Fatal(err)
	}
	cases := [][]string{{"KPL_AGENT_CAPACITY=0"}, {"KPL_PASSWORD=line1\nline2"}, {"KPL_HTTP_PORT=8000", "KPL_HTTP_PORT=8001"}, {"KPL_AGENT_METRICS_PORT=8080"}, {"KPL_AGENT_METRICS_PORT=2377"}, {"KPL_HTTP_PORT=18080", "KPL_AGENT_METRICS_PORT=18080"}, {"UNKNOWN=value"}, nil}
	for _, args := range cases {
		if err := a.ConfigCommand(t.Context(), "configure", args); err == nil {
			t.Error("invalid change accepted")
		}
		after, _ := os.ReadFile(a.EnvFile)
		if !bytes.Equal(after, original) {
			t.Fatal("invalid change modified config")
		}
	}
	if len(runner.calls) != 0 {
		t.Fatal("invalid changes contacted Docker")
	}
	if err := os.Mkdir(a.EnvFile+".lock", 0700); err != nil {
		t.Fatal(err)
	}
	if err := a.ConfigCommand(t.Context(), "configure", []string{"KPL_AGENT_CAPACITY=50"}); err == nil {
		t.Fatal("concurrent editor lock ignored")
	}
	if _, err := os.Stat(a.EnvFile + ".lock"); err != nil {
		t.Fatal("another editor lock was removed")
	}
	if err := os.Remove(a.EnvFile + ".lock"); err != nil {
		t.Fatal(err)
	}
	target := a.EnvFile
	a.EnvFile = filepath.Join(a.Repo, "symlink.env")
	if err := os.Symlink(target, a.EnvFile); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"init", "configure"} {
		if err := a.ConfigCommand(t.Context(), command, []string{"KPL_AGENT_CAPACITY=10"}); err == nil {
			t.Fatalf("%s followed symlink", command)
		}
	}
	a.EnvFile = filepath.Join(a.Repo, "missing.env")
	for _, command := range []string{"config", "configure"} {
		args := []string(nil)
		if command == "configure" {
			args = []string{"KPL_AGENT_CAPACITY=10"}
		}
		if err := a.ConfigCommand(t.Context(), command, args); err == nil {
			t.Fatalf("%s accepted missing config", command)
		}
	}
	matches, _ := filepath.Glob(filepath.Join(a.Repo, ".env.swarm.stage.*"))
	if len(matches) != 0 {
		t.Fatal("staging files leaked")
	}
}

func TestConfigInitRejectsInvalidInputBeforeDocker(t *testing.T) {
	for _, args := range [][]string{{"KPL_IMAGE=missing-tag"}, {"KPL_AGENT_METRICS_PORT=9090"}, {"KPL_AGENT_CAPACITY=40", "KPL_AGENT_CAPACITY=50"}, {"--all"}} {
		a, runner, _ := configApp(t)
		if err := a.ConfigCommand(t.Context(), "init", args); err == nil {
			t.Fatal("invalid init accepted")
		}
		if len(runner.calls) != 0 {
			t.Fatal("invalid init contacted Docker")
		}
		if _, err := os.Lstat(a.EnvFile); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("invalid init created config")
		}
	}
	a, runner, _ := configApp(t)
	a.Environ = append(a.Environ, "KPL_PASSWORD=trailing-newline\n")
	if err := a.ConfigCommand(t.Context(), "init", nil); err == nil || len(runner.calls) != 0 {
		t.Fatal("inherited newline accepted or inspected Docker")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := a.ConfigCommand(ctx, "init", nil); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled init accepted")
	}
}

func TestConfigAtomicCreationAndExternalChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.env")
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- saveConfig(path, []byte("KPL_USER=fixture\n"), nil, nil, true) }()
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		}
	}
	if success != 1 {
		t.Fatalf("atomic initialization had %d winners", success)
	}
	info, _ := os.Stat(path)
	before, _ := os.ReadFile(path)
	if err := os.WriteFile(path, []byte("KPL_USER=external\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := saveConfig(path, []byte("KPL_USER=replacement\n"), before, info, false); err == nil {
		t.Fatal("external edit overwritten")
	}
	after, _ := os.ReadFile(path)
	if string(after) != "KPL_USER=external\n" {
		t.Fatal("external change was lost")
	}
	matches, _ := filepath.Glob(filepath.Join(dir, ".env.swarm.stage.*"))
	if len(matches) != 0 {
		t.Fatal("staging files leaked")
	}
}

func TestConfigRejectsFIFOWithoutWaitingForWriter(t *testing.T) {
	a, _, _ := configApp(t)
	if err := syscall.Mkfifo(a.EnvFile, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := a.LoadConfig(); err == nil {
		t.Fatal("FIFO accepted as configuration")
	}
	if err := a.ConfigCommand(t.Context(), "configure", []string{"KPL_AGENT_CAPACITY=30"}); err == nil {
		t.Fatal("FIFO accepted for configuration write")
	}
}

func TestConfigCredentialsRequireExplicitCommandAndUseOnlyFixture(t *testing.T) {
	a, runner, out := configApp(t)
	fixture := Config{"KPL_USER": "fixture-user", "KPL_PASSWORD": "fixture-dashboard-only", "GRAFANA_ADMIN_USER": "fixture-grafana-user", "GRAFANA_ADMIN_PASSWORD": "fixture-grafana-only"}
	if err := os.WriteFile(a.EnvFile, encodeConfig(fixture), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.ConfigCommand(t.Context(), "config", nil); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), fixture["KPL_PASSWORD"]) || strings.Contains(out.String(), fixture["GRAFANA_ADMIN_PASSWORD"]) {
		t.Fatal("ordinary config exposed fixture secrets")
	}
	out.Reset()
	if err := a.ConfigCommand(t.Context(), "credentials", []string{"--all"}); err == nil || out.Len() != 0 {
		t.Fatal("credentials accepted unexpected arguments")
	}
	a.Environ = append(a.Environ, "KPL_PASSWORD=fixture-environment-only")
	if err := a.ConfigCommand(t.Context(), "credentials", nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "KPL_PASSWORD=fixture-environment-only\n") || !strings.Contains(out.String(), "GRAFANA_ADMIN_PASSWORD=fixture-grafana-only\n") {
		t.Fatal("explicit credentials omitted the effective fixture values")
	}
	if len(runner.calls) != 0 {
		t.Fatal("credentials command contacted Docker")
	}
	data, _, err := readConfig(a.EnvFile, true)
	if err != nil {
		t.Fatal(err)
	}
	if data["KPL_PASSWORD"] != fixture["KPL_PASSWORD"] {
		t.Fatal("credentials command persisted environment override")
	}
}

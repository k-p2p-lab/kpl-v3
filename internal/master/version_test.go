package master

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestConfiguredVersionIsReadOnlyAndDoesNotDiscloseCredentials(t *testing.T) {
	a, runner, out := configApp(t)
	digest := "sha256:" + strings.Repeat("a", 64)
	fixture := ConfigDefaults(Config{"KPL_IMAGE": "registry.example.test/lab@" + digest, "KPL_PASSWORD": "fake-dashboard-private", "GRAFANA_ADMIN_PASSWORD": "fake-grafana-private"})
	before := encodeConfig(fixture)
	if err := os.WriteFile(a.EnvFile, before, 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.printVersion("fixture-version", []string{"--json"}, true); err != nil {
		t.Fatal(err)
	}
	var report versionReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.CLI.Version != "fixture-version" || report.CLI.Revision == "" || !swarmDigest.MatchString(report.CLI.BinaryHash) || report.ConfiguredImage != fixture["KPL_IMAGE"] || report.ImageDigest != digest {
		t.Fatalf("incomplete version report: %+v", report)
	}
	for _, secret := range []string{fixture["KPL_PASSWORD"], fixture["GRAFANA_ADMIN_PASSWORD"]} {
		if strings.Contains(out.String(), secret) {
			t.Fatal("version report disclosed credentials")
		}
	}
	after, err := os.ReadFile(a.EnvFile)
	if err != nil || string(after) != string(before) || len(runner.calls) != 0 {
		t.Fatal("version report changed config or contacted Docker")
	}
}

func TestVersionDoesNotResolveMutableTagOrReadConfiguration(t *testing.T) {
	a, runner, out := configApp(t)
	if err := a.printVersion("test", nil, false); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "kpl test\n") || len(runner.calls) != 0 {
		t.Fatal("standalone version depends on deployment configuration")
	}
	out.Reset()
	fixture := ConfigDefaults(Config{"KPL_IMAGE": "registry.example.test/lab:v3"})
	if err := os.WriteFile(a.EnvFile, encodeConfig(fixture), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.printVersion("test", []string{"--json"}, true); err != nil {
		t.Fatal(err)
	}
	var report versionReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.ImageDigest != "" || len(runner.calls) != 0 {
		t.Fatal("configured tag was falsely reported as an observed digest")
	}
	for _, args := range [][]string{{"--unknown"}, {"--json", "--json"}} {
		if err := a.printVersion("test", args, true); err == nil {
			t.Fatal("unsupported version options accepted")
		}
	}
}

func TestNewConfigurationDefaultsToLatestImageTag(t *testing.T) {
	a, _, out := configApp(t)
	if err := a.ConfigCommand(t.Context(), "init", nil); err != nil {
		t.Fatal(err)
	}
	config, err := a.LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config["KPL_IMAGE"] != "registry.example.com/kpl-v3:latest" || !strings.Contains(out.String(), "kpl config show") {
		t.Fatal("new config did not select latest or suggested a removed command")
	}
}

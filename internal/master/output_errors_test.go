package master

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"
)

var errSyntheticOutput = errors.New("synthetic output unavailable")

type failedCommandOutput struct{ partial bool }

func (w failedCommandOutput) Write(p []byte) (int, error) {
	if w.partial {
		return min(7, len(p)), errSyntheticOutput
	}
	return 0, errSyntheticOutput
}

func syntheticOutputApp(t *testing.T) (*App, *configRunner, []byte) {
	t.Helper()
	a, runner, _ := configApp(t)
	// Every lookup is explicit and synthetic; no user environment or default
	// credential location participates in output/error tests.
	a.Environ = []string{}
	data := encodeConfig(ConfigDefaults(Config{
		"KPL_IMAGE": "registry.example.test/output:v3", "KPL_USER": "synthetic-user",
		"KPL_PASSWORD": "synthetic-dashboard-private", "GRAFANA_ADMIN_PASSWORD": "synthetic-grafana-private",
	}))
	if err := os.WriteFile(a.EnvFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	return a, runner, data
}

func runReadOnlyOutputCommand(ctx context.Context, a *App, operation string) error {
	switch operation {
	case "version-text":
		return a.printVersion("output-fixture", nil, false)
	case "version-json":
		return a.printVersion("output-fixture", []string{"--json"}, false)
	case "configured-version-text":
		return a.printVersion("output-fixture", nil, true)
	case "configured-version-json":
		return a.printVersion("output-fixture", []string{"--json"}, true)
	case "config-show":
		return a.ConfigCommand(ctx, "config", nil)
	case "config-credentials":
		return a.ConfigCommand(ctx, "credentials", nil)
	case "help":
		return a.Help(nil)
	default:
		return errors.New("unknown synthetic output command")
	}
}

func TestReadOnlyCLICommandsReportOutputFailures(t *testing.T) {
	for _, operation := range []string{"version-text", "version-json", "configured-version-text", "configured-version-json", "config-show", "config-credentials", "help"} {
		for _, partial := range []bool{false, true} {
			t.Run(operation+map[bool]string{false: "/failed", true: "/partial"}[partial], func(t *testing.T) {
				a, runner, before := syntheticOutputApp(t)
				a.Stdout = failedCommandOutput{partial: partial}
				if err := runReadOnlyOutputCommand(t.Context(), a, operation); !errors.Is(err, errSyntheticOutput) {
					t.Fatalf("output failure was reported as success: %v", err)
				}
				after, err := os.ReadFile(a.EnvFile)
				if err != nil || string(after) != string(before) || len(runner.calls) != 0 {
					t.Fatal("read-only output failure changed configuration or invoked Docker")
				}
			})
		}
	}
}

func TestReadOnlyCLICommandsReportClosedOutputPipes(t *testing.T) {
	for _, operation := range []string{"version-text", "version-json", "configured-version-text", "config-show", "config-credentials", "help"} {
		t.Run(operation, func(t *testing.T) {
			a, _, _ := syntheticOutputApp(t)
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			a.Stdout = writer
			if err := runReadOnlyOutputCommand(t.Context(), a, operation); !errors.Is(err, syscall.EPIPE) {
				t.Fatalf("closed output pipe was reported as success: %v", err)
			}
		})
	}
}

func TestConfigOutputCancellationDoesNotChangeReadOnlySettings(t *testing.T) {
	for _, operation := range []string{"config-show", "config-credentials"} {
		t.Run(operation, func(t *testing.T) {
			a, _, before := syntheticOutputApp(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			a.Stdout = &cancelScenarioOutput{cancel: cancel}
			if err := runReadOnlyOutputCommand(ctx, a, operation); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled output was reported as success: %v", err)
			}
			after, err := os.ReadFile(a.EnvFile)
			if err != nil || string(after) != string(before) {
				t.Fatal("output cancellation changed saved configuration")
			}
		})
	}
}

func TestConfigMutationOutputFailurePreservesCommittedPrivateFile(t *testing.T) {
	for _, operation := range []string{"init", "configure"} {
		t.Run(operation, func(t *testing.T) {
			a, _, _ := syntheticOutputApp(t)
			if operation == "init" {
				if err := os.Remove(a.EnvFile); err != nil {
					t.Fatal(err)
				}
			}
			a.Stdout = failedCommandOutput{}
			args := []string{"KPL_USER=updated-synthetic-user", "KPL_IMAGE=registry.example.test/output:v3",
				"KPL_PASSWORD=synthetic-dashboard-private", "GRAFANA_ADMIN_PASSWORD=synthetic-grafana-private"}
			if err := a.ConfigCommand(t.Context(), operation, args); !errors.Is(err, errSyntheticOutput) {
				t.Fatalf("output failure was hidden after configuration commit: %v", err)
			}
			config, _, err := readConfig(a.EnvFile, true)
			if err != nil || config["KPL_USER"] != "updated-synthetic-user" {
				t.Fatal("output failure abandoned or rolled back an already committed configuration")
			}
			info, err := os.Stat(a.EnvFile)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatal("output failure changed configuration privacy")
			}
		})
	}
}

func TestSuccessfulCLITextOutputPreservesRedactionAndFormatting(t *testing.T) {
	for _, operation := range []string{"version-text", "version-json", "configured-version-text", "configured-version-json", "config-show", "config-credentials", "help"} {
		t.Run(operation, func(t *testing.T) {
			a, runner, before := syntheticOutputApp(t)
			var output strings.Builder
			a.Stdout = &output
			if err := runReadOnlyOutputCommand(t.Context(), a, operation); err != nil || output.Len() == 0 {
				t.Fatalf("normal output failed: %v", err)
			}
			containsSecrets := strings.Contains(output.String(), "synthetic-dashboard-private") || strings.Contains(output.String(), "synthetic-grafana-private")
			if containsSecrets != (operation == "config-credentials") {
				t.Fatal("successful output changed explicit credential disclosure or redaction")
			}
			if operation == "config-show" && strings.Count(output.String(), "[redacted]") != 2 {
				t.Fatal("configuration output changed secret redaction markers")
			}
			after, err := os.ReadFile(a.EnvFile)
			if err != nil || string(after) != string(before) || len(runner.calls) != 0 {
				t.Fatal("successful read-only output changed saved configuration or invoked Docker")
			}
		})
	}
}

var _ io.Writer = failedCommandOutput{}

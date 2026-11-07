package master

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScenarioValidationUsageUsesCanonicalCommand(t *testing.T) {
	a, _, _ := appFixture(t)
	output := &bytes.Buffer{}
	a.Stderr = output
	var usage *UsageError
	if err := a.RunDevelopment(t.Context(), "validate", []string{"--unknown"}); !errors.As(err, &usage) {
		t.Fatalf("unknown validation option did not return usage: %v", err)
	}
	if !strings.Contains(output.String(), "Usage of kpl scenario validate:") {
		t.Fatalf("validation diagnostic suggested a removed command: %s", output.String())
	}
}

func scenarioFileWithBytes(t *testing.T, size int) string {
	t.Helper()
	base := validScenarioFixture + "\n#"
	if size < len(base) {
		t.Fatal("scenario fixture size is too small")
	}
	file := filepath.Join(t.TempDir(), "scenario.yaml")
	if err := os.WriteFile(file, []byte(base+strings.Repeat("x", size-len(base))), 0600); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestScenarioValidationMatchesDashboardYAMLByteLimit(t *testing.T) {
	for _, size := range []int{1 << 20, (1 << 20) + 1} {
		a, runner, output := appFixture(t)
		file := scenarioFileWithBytes(t, size)
		err := a.RunDevelopment(t.Context(), "validate", []string{"--scenario", file})
		if size == 1<<20 {
			if err != nil || !strings.HasPrefix(output.String(), "valid scenario:") {
				t.Fatalf("document at dashboard limit was rejected: %v", err)
			}
		} else if err == nil || !strings.Contains(err.Error(), "1048576") || output.Len() != 0 {
			t.Fatalf("dashboard-inadmissible YAML was accepted or parsed/output: %v, %q", err, output.String())
		}
		if len(runner.commands) != 0 {
			t.Fatal("scenario validation invoked an external command")
		}
	}
}

func TestCanceledScenarioCommandsDoNotReadOrReportSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, command := range []string{"validate", "show"} {
		t.Run(command, func(t *testing.T) {
			a, runner, output := appFixture(t)
			file := filepath.Join(t.TempDir(), "does-not-exist.yaml")
			var err error
			if command == "validate" {
				err = a.RunDevelopment(ctx, "validate", []string{"--scenario", file})
			} else {
				err = a.RunSwarm(ctx, "scenario", []string{file})
			}
			if !errors.Is(err, context.Canceled) || output.Len() != 0 || len(runner.commands) != 0 {
				t.Fatalf("canceled scenario command performed I/O or reported success: %v, %q", err, output.String())
			}
		})
	}
}

type cancelScenarioOutput struct {
	cancel context.CancelFunc
	bytes  int
}

func (w *cancelScenarioOutput) Write(p []byte) (int, error) {
	w.bytes += len(p)
	w.cancel()
	return len(p), nil
}

func TestScenarioShowStopsStreamingWhenCanceledByOutput(t *testing.T) {
	a, _, _ := appFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	output := &cancelScenarioOutput{cancel: cancel}
	a.Stdout = output
	const size = (2 << 20) + 1
	file := scenarioFileWithBytes(t, size)
	err := a.RunSwarm(ctx, "scenario", []string{file})
	if !errors.Is(err, context.Canceled) || output.bytes <= 0 || output.bytes >= size {
		t.Fatalf("scenario output ignored cancellation: bytes=%d/%d error=%v", output.bytes, size, err)
	}
}

func TestScenarioShowStreamsFilesBeyondValidationLimit(t *testing.T) {
	a, runner, output := appFixture(t)
	const size = (2 << 20) + 1
	file := scenarioFileWithBytes(t, size)
	if err := a.RunSwarm(t.Context(), "scenario", []string{file}); err != nil {
		t.Fatal(err)
	}
	if output.Len() != size || len(runner.commands) != 0 {
		t.Fatalf("raw scenario output was truncated or delegated: bytes=%d/%d", output.Len(), size)
	}
}

func TestScenarioValidateReportsCancellationDuringOutput(t *testing.T) {
	a, _, _ := appFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	a.Stdout = &cancelScenarioOutput{cancel: cancel}
	file := scenarioFileWithBytes(t, 4096)
	err := a.RunDevelopment(ctx, "validate", []string{"--scenario", file})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("validation claimed success although its output canceled the command: %v", err)
	}
}

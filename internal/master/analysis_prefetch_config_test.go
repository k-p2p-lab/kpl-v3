package master

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestAnalysisPrefetchConfigDefaultsPersistsAndPreservesInvalidEdits(t *testing.T) {
	if got := ConfigDefaults(nil)["KPL_ANALYSIS_PREFETCH_MIB"]; got != "64" {
		t.Fatalf("default analysis prefetch queue = %s, want 64 MiB", got)
	}
	a, runner, _ := configApp(t)
	if err := a.ConfigCommand(t.Context(), "init", []string{"KPL_IMAGE=registry.example/team/kpl:v3"}); err != nil {
		t.Fatal(err)
	}
	initial, _, err := readConfig(a.EnvFile, true)
	if err != nil || initial["KPL_ANALYSIS_PREFETCH_MIB"] != "64" {
		t.Fatalf("new configuration did not persist the default prefetch queue: %v", err)
	}
	if err := a.ConfigCommand(t.Context(), "configure", []string{"KPL_ANALYSIS_PREFETCH_MIB=128"}); err != nil {
		t.Fatal(err)
	}
	loaded, err := a.LoadConfig()
	if err != nil || loaded["KPL_ANALYSIS_PREFETCH_MIB"] != "128" {
		t.Fatalf("saved analysis prefetch queue was not loaded: %v", err)
	}
	before, err := os.ReadFile(a.EnvFile)
	if err != nil {
		t.Fatal(err)
	}
	calls := len(runner.calls)
	for _, invalid := range []string{"0", "1025", "-1", "1.5", "064", "18446744073709551616"} {
		if err := a.ConfigCommand(t.Context(), "configure", []string{"KPL_ANALYSIS_PREFETCH_MIB=" + invalid}); err == nil {
			t.Fatalf("accepted invalid prefetch size %q", invalid)
		}
		after, err := os.ReadFile(a.EnvFile)
		if err != nil || !bytes.Equal(before, after) || len(runner.calls) != calls {
			t.Fatalf("rejected prefetch setting changed configuration or contacted Docker: %v", err)
		}
	}
}

func TestControllerPrefetchFlagAndEnvironment(t *testing.T) {
	t.Setenv("KPL_USER", "")
	t.Setenv("KPL_PASSWORD", "")
	t.Setenv("KPL_ANALYSIS_TRANSFER_MIB_PER_SECOND", "")
	t.Setenv("KPL_RUN_MIN_FREE_BYTES", "")
	t.Setenv("KPL_ARCHIVE_MIB_PER_SECOND", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, raw := range []string{"", "1", "64", "1024"} {
		t.Setenv("KPL_ANALYSIS_PREFETCH_MIB", raw)
		err := runController(t.Context(), logger, []string{"--data-dir", t.TempDir()})
		if err == nil || !strings.Contains(err.Error(), "KPL_USER and KPL_PASSWORD are required") {
			t.Fatalf("valid prefetch default %q failed before service configuration: %v", raw, err)
		}
	}
	for _, raw := range []string{"0", "1025", "-1", "invalid", "18446744073709551616"} {
		t.Setenv("KPL_ANALYSIS_PREFETCH_MIB", raw)
		if err := runController(t.Context(), logger, nil); err == nil || (!strings.Contains(err.Error(), "prefetch") && !strings.Contains(err.Error(), "KPL_ANALYSIS_PREFETCH_MIB")) {
			t.Fatalf("invalid prefetch default %q was accepted: %v", raw, err)
		}
		if err := runController(t.Context(), logger, []string{"--help"}); !errors.Is(err, flag.ErrHelp) {
			t.Fatalf("invalid prefetch environment prevented role help: %v", err)
		}
		err := runController(t.Context(), logger, []string{"--data-dir", t.TempDir(), "--analysis-prefetch-mib=128"})
		if err == nil || !strings.Contains(err.Error(), "KPL_USER and KPL_PASSWORD are required") {
			t.Fatalf("explicit prefetch flag did not override invalid environment %q: %v", raw, err)
		}
	}
}

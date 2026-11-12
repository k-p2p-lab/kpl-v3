package master

import (
	"errors"
	"flag"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestAnalysisResourcesApplyToControllerFlagsAndConfiguration(t *testing.T) {
	if ConfigDefaults(nil)["KPL_ANALYSIS_CPUS"] != "4" || ConfigDefaults(nil)["KPL_ANALYSIS_MEMORY_MIB"] != "4096" {
		t.Fatal("automatic Agent analysis defaults changed")
	}
	t.Setenv("KPL_USER", "")
	t.Setenv("KPL_PASSWORD", "")
	t.Setenv("KPL_ANALYSIS_PREFETCH_MIB", "")
	t.Setenv("KPL_ANALYSIS_TRANSFER_MIB_PER_SECOND", "")
	t.Setenv("KPL_RETIRED_ANALYSIS_ENDPOINTS", "")
	t.Setenv("KPL_RUN_MIN_FREE_BYTES", "")
	t.Setenv("KPL_ARCHIVE_MIB_PER_SECOND", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, check := range []struct {
		key, flag, valid string
		invalid          []string
	}{
		{"KPL_ANALYSIS_CPUS", "analysis-cpus", "64", []string{"0", "4097", "invalid", "-1", "18446744073709551616"}},
		{"KPL_ANALYSIS_MEMORY_MIB", "analysis-memory-mib", "8192", []string{"0", "invalid", "-1", "8796093022208", "18446744073709551616"}},
	} {
		t.Run(check.key, func(t *testing.T) {
			t.Setenv("KPL_ANALYSIS_CPUS", "")
			t.Setenv("KPL_ANALYSIS_MEMORY_MIB", "")
			t.Setenv(check.key, check.valid)
			err := runController(t.Context(), logger, []string{"--data-dir", t.TempDir()})
			if err == nil || !strings.Contains(err.Error(), "KPL_USER and KPL_PASSWORD are required") {
				t.Fatalf("valid common analysis resources were rejected before service configuration: %v", err)
			}
			for _, invalid := range check.invalid {
				t.Setenv(check.key, invalid)
				if err := runController(t.Context(), logger, nil); err == nil {
					t.Fatalf("invalid resource %s=%s accepted", check.key, invalid)
				}
				if err := runController(t.Context(), logger, []string{"--help"}); !errors.Is(err, flag.ErrHelp) {
					t.Fatalf("invalid environment broke role help: %v", err)
				}
				err := runController(t.Context(), logger, []string{"--data-dir", t.TempDir(), "--" + check.flag + "=" + check.valid})
				if err == nil || !strings.Contains(err.Error(), "KPL_USER and KPL_PASSWORD are required") {
					t.Fatalf("explicit flag did not override invalid resource environment: %v", err)
				}
			}
		})
	}
}

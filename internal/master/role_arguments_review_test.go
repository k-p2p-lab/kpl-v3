package master

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeRolesRejectUnexpectedPositionalsBeforeStarting(t *testing.T) {
	t.Setenv("KPL_USER", "")
	t.Setenv("KPL_PASSWORD", "")
	t.Setenv("KPL_RUN_MIN_FREE_BYTES", "")
	t.Setenv("KPL_ARCHIVE_MIB_PER_SECOND", "")
	t.Setenv("KPL_AGENT_HOST_REBOOT_ENABLED", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	privateArg := "fake-positional-private-value"
	missing := filepath.Join(t.TempDir(), "never-created.json")
	tests := []struct {
		role string
		args []string
		run  func(context.Context, []string) error
	}{
		{"controller", []string{"--data-dir", t.TempDir()}, func(ctx context.Context, args []string) error { return runController(ctx, logger, args) }},
		{"agent", nil, func(ctx context.Context, args []string) error { return runAgent(ctx, logger, args) }},
		{"resource-monitor", nil, func(ctx context.Context, args []string) error { return runServiceMonitor(ctx, logger, args) }},
		{"network-manager", nil, runNetworkManager},
		{"network-gateway", []string{"--config", missing}, runNetworkGateway},
		{"peer", []string{"--config", missing}, func(ctx context.Context, args []string) error { return runPeer(ctx, logger, args) }},
	}
	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			args := append(append([]string(nil), tt.args...), privateArg, "--help")
			err := tt.run(t.Context(), args)
			var usage *UsageError
			if !errors.As(err, &usage) {
				t.Fatalf("unexpected positional reached role setup instead of usage validation: %v", err)
			}
			if strings.Contains(err.Error(), privateArg) {
				t.Fatal("positional rejection printed the supplied private value")
			}
		})
	}
}

func TestRuntimeRoleHelpIgnoresInvalidEnvironmentDefaults(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, setting := range []string{"KPL_RUN_MIN_FREE_BYTES", "KPL_ARCHIVE_MIB_PER_SECOND"} {
		t.Run(setting, func(t *testing.T) {
			t.Setenv(setting, "not-a-number")
			if err := runController(t.Context(), logger, []string{"--help"}); !errors.Is(err, flag.ErrHelp) {
				t.Fatalf("invalid environment prevented role help: %v", err)
			}
		})
	}
	t.Setenv("KPL_AGENT_HOST_REBOOT_ENABLED", "not-a-boolean")
	if err := runAgent(t.Context(), logger, []string{"--help"}); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("invalid environment prevented Agent role help: %v", err)
	}
}

func TestExplicitRoleFlagsOverrideInvalidEnvironmentDefaults(t *testing.T) {
	t.Setenv("KPL_USER", "")
	t.Setenv("KPL_PASSWORD", "")
	t.Setenv("KPL_RUN_MIN_FREE_BYTES", "not-a-number")
	t.Setenv("KPL_ARCHIVE_MIB_PER_SECOND", "not-a-number")
	t.Setenv("KPL_AGENT_HOST_REBOOT_ENABLED", "not-a-boolean")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := runController(t.Context(), logger, []string{"--data-dir", t.TempDir(), "--run-min-free-bytes=0", "--archive-mib-per-second=0"})
	if err == nil || !strings.Contains(err.Error(), "KPL_USER and KPL_PASSWORD are required") {
		t.Fatalf("explicit Controller flags did not take precedence over defaults: %v", err)
	}
	err = runAgent(t.Context(), logger, []string{"--host-reboot-enabled=false"})
	if err == nil || !strings.Contains(err.Error(), "KPL_USER and KPL_PASSWORD are required") {
		t.Fatalf("explicit Agent flag did not take precedence over default: %v", err)
	}
}

package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestParsePort(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		fallback int
		want     int
		wantErr  bool
	}{
		{name: "empty uses default", fallback: 9090, want: 9090},
		{name: "whitespace uses default", raw: "  ", fallback: 3000, want: 3000},
		{name: "minimum", raw: "1", fallback: 9090, want: 1},
		{name: "maximum", raw: "65535", fallback: 9090, want: 65535},
		{name: "custom", raw: "19090", fallback: 9090, want: 19090},
		{name: "zero", raw: "0", fallback: 9090, wantErr: true},
		{name: "negative", raw: "-1", fallback: 9090, wantErr: true},
		{name: "too large", raw: "65536", fallback: 9090, wantErr: true},
		{name: "not numeric", raw: "http", fallback: 9090, wantErr: true},
		{name: "number with spaces", raw: " 9090 ", fallback: 3000, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parsePort(test.raw, test.fallback, "test")
			if (err != nil) != test.wantErr {
				t.Fatalf("parsePort(%q) error=%v, wantErr=%t", test.raw, err, test.wantErr)
			}
			if got != test.want {
				t.Fatalf("parsePort(%q)=%d, want %d", test.raw, got, test.want)
			}
		})
	}
}

func TestRunAgentPassesMetricsEndpointFlagsToConfiguration(t *testing.T) {
	t.Setenv("KPL_USER", "admin")
	t.Setenv("KPL_PASSWORD", "test-password")
	err := runAgent(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), []string{
		"--id", "agent", "--advertise-url", "http://agent:8090", "--controller-url", "http://controller:8080",
		"--docker-image", "sha256:test", "--docker-network", "test-overlay", "--metrics-listen", ":9091", "--metrics-url", "http://worker.example:9091/wrong",
	})
	if err == nil || !strings.Contains(err.Error(), "path must be exactly /metrics") {
		t.Fatalf("metrics flags were not validated by Agent configuration: %v", err)
	}
}

func TestRunAgentRejectsRemovedLocalFlags(t *testing.T) {
	for _, flag := range []string{"--runtime", "--peer-api-port", "--peer-p2p-port"} {
		t.Run(flag, func(t *testing.T) {
			err := runAgent(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), []string{flag, "process"})
			if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
				t.Fatalf("removed local flag %s was not rejected: %v", flag, err)
			}
		})
	}
}

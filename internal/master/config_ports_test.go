package master

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestPublishedPortConflictsRejectedBeforeConfigWrite(t *testing.T) {
	ports := []string{"KPL_HTTP_PORT", "PROMETHEUS_PORT", "GRAFANA_PORT", "KPL_AGENT_METRICS_PORT"}
	for i, first := range ports {
		for _, second := range ports[i+1:] {
			t.Run(first+"/"+second, func(t *testing.T) {
				a, runner, _ := configApp(t)
				original := []byte("KPL_USER=operator\nKPL_PASSWORD=fixture-password\nGRAFANA_ADMIN_PASSWORD=fixture-grafana\n")
				if err := os.WriteFile(a.EnvFile, original, 0600); err != nil {
					t.Fatal(err)
				}
				err := a.ConfigCommand(t.Context(), "configure", []string{first + "=18080", second + "=18080"})
				if err == nil || !strings.Contains(err.Error(), first) || !strings.Contains(err.Error(), second) {
					t.Fatalf("same-host published port conflict was not identified: %v", err)
				}
				after, err := os.ReadFile(a.EnvFile)
				if err != nil || !bytes.Equal(after, original) || len(runner.calls) != 0 {
					t.Fatalf("invalid port configuration had side effects: unchanged=%t calls=%d err=%v", bytes.Equal(after, original), len(runner.calls), err)
				}
			})
		}
	}
}

func TestPublishedPortsRejectSwarmTCPListenersBeforeDocker(t *testing.T) {
	for _, key := range []string{"KPL_HTTP_PORT", "PROMETHEUS_PORT", "GRAFANA_PORT", "KPL_AGENT_METRICS_PORT"} {
		for _, port := range []string{"2377", "7946"} {
			t.Run(key+"/"+port, func(t *testing.T) {
				a, runner, _ := configApp(t)
				err := a.ConfigCommand(t.Context(), "init", []string{key + "=" + port})
				if err == nil || !strings.Contains(err.Error(), key) || !strings.Contains(err.Error(), "Swarm") {
					t.Fatalf("Swarm TCP listener collision was not rejected: %v", err)
				}
				if len(runner.calls) != 0 {
					t.Fatal("invalid ports contacted Docker")
				}
				if _, err := os.Lstat(a.EnvFile); !os.IsNotExist(err) {
					t.Fatalf("invalid ports created configuration: %v", err)
				}
			})
		}
	}
}

func TestPublishedPortsAllowDistinctValidSettings(t *testing.T) {
	cfg := ConfigDefaults(Config{"KPL_HTTP_PORT": "80", "PROMETHEUS_PORT": "19090", "GRAFANA_PORT": "13000", "KPL_AGENT_METRICS_PORT": "19091"})
	if err := ValidatePortConflicts(cfg); err != nil {
		t.Fatal(err)
	}
}

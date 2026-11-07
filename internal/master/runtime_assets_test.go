package master

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestStackServicesUseCanonicalRuntimeCommands(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repositoryForTest(t), "stack.swarm.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var stack struct {
		Services map[string]struct {
			Command []string `yaml:"command"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &stack); err != nil {
		t.Fatal(err)
	}
	for service, role := range map[string]string{"controller": "controller", "agent": "swarm-agent", "network-manager": "network-manager", "resource-monitor": "resource-monitor"} {
		command := stack.Services[service].Command
		if len(command) < 2 || !slices.Equal(command[:2], []string{"run", role}) {
			t.Fatalf("service %s invokes a removed runtime entrypoint: %q", service, command)
		}
	}
}

func TestRuntimeImageDeclaresRequiredCLILayout(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repositoryForTest(t), "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	// The final runtime stage must advertise the layout checked before any
	// service shutdown; a builder-only label would be lost at the final FROM.
	_, runtime, ok := strings.Cut(string(data), " AS runtime\n")
	if !ok || !strings.Contains(runtime, `LABEL io.kpl.cli.layout="`+cliImageLayout+`"`) {
		t.Fatal("runtime image does not declare the CLI layout required by deployment")
	}
}

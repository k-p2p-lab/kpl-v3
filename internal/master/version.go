package master

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type versionReport struct {
	CLI             model.BuildMetadata `json:"cli"`
	ConfiguredImage string              `json:"configuredImage,omitempty"`
	ImageDigest     string              `json:"imageDigest,omitempty"`
}

func (a *App) printVersion(version string, args []string, configured bool) error {
	if len(args) > 1 || len(args) == 1 && args[0] != "--json" {
		return usageError("version accepts only --json")
	}
	report := versionReport{CLI: model.BuildDetails()}
	if version != "" && version != "dev" {
		report.CLI.Version = version
	}
	if configured {
		config, err := a.LoadConfig()
		if err != nil {
			return err
		}
		report.ConfiguredImage = config["KPL_IMAGE"]
		if _, digest, ok := strings.Cut(report.ConfiguredImage, "@"); ok && swarmDigest.MatchString(digest) {
			report.ImageDigest = digest
		}
	}
	if len(args) == 1 {
		encoder := json.NewEncoder(a.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	fmt.Fprintf(a.Stdout, "kpl %s\nSource revision: %s\nModified source: %s\nGo version: %s\nExecutable SHA256: %s\n", report.CLI.Version, report.CLI.Revision, sourceModifiedField(report.CLI), report.CLI.GoVersion, knownVersionField(report.CLI.BinaryHash))
	if report.CLI.BuildTime != "" {
		fmt.Fprintf(a.Stdout, "Built at: %s\n", report.CLI.BuildTime)
	}
	if configured {
		fmt.Fprintf(a.Stdout, "Configured image: %s\nConfigured registry digest: %s\n", knownVersionField(report.ConfiguredImage), knownVersionField(report.ImageDigest))
	}
	return nil
}

func knownVersionField(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}

func sourceModifiedField(build model.BuildMetadata) string {
	if build.Revision == "unknown" || build.Revision == "development" || build.Revision == "" {
		return "unknown"
	}
	return strconv.FormatBool(build.Modified)
}

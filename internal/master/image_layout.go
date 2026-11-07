package master

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const cliImageLayout = "grouped-v1"
const cliImageLayoutFormat = `{{index .Config.Labels "io.kpl.cli.layout"}}`

// verifyImageLayout rejects images that still require removed top-level roles.
// It only reads the image already pulled by resolveImage; services are intact.
func (s *swarm) verifyImageLayout(ctx context.Context) error {
	inspectCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	app := s.a.WithEnv(map[string]*string{"DOCKER_DEFAULT_PLATFORM": nil})
	raw, err := app.Runner.Output(inspectCtx, app.command("docker", []string{"image", "inspect", "--format", cliImageLayoutFormat, s.config["KPL_IMAGE"]}))
	if err != nil {
		return fmt.Errorf("check image CLI layout; no deployment changes made: %w", err)
	}
	label := strings.TrimSuffix(strings.TrimSuffix(string(raw), "\n"), "\r")
	if label != cliImageLayout {
		return errors.New("image does not support kpl run ROLE (io.kpl.cli.layout=grouped-v1 required); rebuild the image with the current Dockerfile; no deployment changes made")
	}
	return nil
}

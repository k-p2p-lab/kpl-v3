package netem

import (
	"context"
	"fmt"
	"os/exec"
	"reflect"
	"runtime"
	"strings"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Update changes only this Peer namespace's KPL qdiscs. Delay-only changes keep
// the qdisc tree and classifiers. Structural changes rebuild the owned tree so
// removed TBF, rate and other options cannot survive an explicit reset to zero.
func Update(ctx context.Context, previous, next model.NetworkConfig, port int) error {
	if err := validateUpdate(previous, next); err != nil {
		return err
	}
	if reflect.DeepEqual(previous, next) || !previous.Enabled() && !next.Enabled() {
		return nil
	}
	if runtime.GOOS != "linux" {
		return fmt.Errorf("network changes require an isolated Linux peer container")
	}
	path, err := exec.LookPath("tc")
	if err != nil {
		return err
	}
	interfaces, err := systemInterfaces()
	if err != nil {
		return err
	}
	run := func(ctx context.Context, args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, path, args...).CombinedOutput()
	}
	return update(ctx, previous, next, port, interfaces, run)
}

func validateUpdate(previous, next model.NetworkConfig) error {
	for _, config := range []model.NetworkConfig{previous, next} {
		if err := config.Validate(); err != nil {
			return err
		}
		if config.Schedule != nil || config.DelayDistribution != nil {
			return fmt.Errorf("network update requires concrete settings without a schedule")
		}
	}
	return nil
}

func update(ctx context.Context, previous, next model.NetworkConfig, port int, interfaces []networkInterface, run commandRunner) error {
	if err := validateUpdate(previous, next); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if reflect.DeepEqual(previous, next) || !previous.Enabled() && !next.Enabled() {
		return nil
	}
	devices := activeIPv4Interfaces(interfaces)
	if len(devices) == 0 {
		return fmt.Errorf("network update: no active non-loopback IPv4 interface")
	}
	comparable := previous
	comparable.Delay = next.Delay
	if previous.Enabled() && next.Enabled() && reflect.DeepEqual(comparable, next) {
		for _, device := range devices {
			args := []string{"qdisc", "change", "dev", device, "parent", "1:3", "handle", "30:", "netem"}
			if next.Scope == "all" {
				args = []string{"qdisc", "change", "dev", device, "root", "handle", "1:", "netem"}
			}
			args = append(args, options(next)...)
			if output, err := run(ctx, args...); err != nil {
				return fmt.Errorf("network update: tc %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
			}
		}
		return nil
	}
	if previous.Enabled() {
		for _, device := range devices {
			if output, err := run(ctx, "qdisc", "del", "dev", device, "root", "handle", "1:"); err != nil {
				return fmt.Errorf("remove prior network conditions on %s: %w: %s", device, err, strings.TrimSpace(string(output)))
			}
		}
	}
	return apply(ctx, next, port, interfaces, run)
}

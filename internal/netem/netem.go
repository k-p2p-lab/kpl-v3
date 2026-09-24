// Package netem configures outbound network impairments inside an isolated
// Linux peer container. Callers must never apply these rules in a host network
// namespace. By default control HTTP and telemetry bypass netem; scope all
// deliberately includes them for whole-interface experiments.
package netem

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// DefaultP2PPort is shared by every container; peer addresses must advertise
// the container IP and this port, rather than a translated host port.
const DefaultP2PPort = 20000

type commandRunner func(context.Context, ...string) ([]byte, error)

type networkInterface struct {
	name      string
	flags     net.Flags
	addresses []net.Addr
}

// Apply installs tc netem before P2P sockets are opened. A disabled configuration
// is a no-op on every OS. Enabled configurations require Linux, iproute2's tc,
// CAP_NET_ADMIN, and kernel sch_prio/sch_netem/cls_u32 support. An error aborts
// startup and removes any qdiscs installed earlier in this call.
func Apply(ctx context.Context, config model.NetworkConfig, p2pPort int) error {
	if err := config.Validate(); err != nil {
		return fmt.Errorf("network impairment: %w", err)
	}
	if config.DelayDistribution != nil {
		return fmt.Errorf("network impairment: delayDistribution must be resolved to a per-peer delay before applying tc")
	}
	if !config.Enabled() {
		return nil
	}
	if runtime.GOOS != "linux" {
		return fmt.Errorf("network impairment requires an isolated Linux peer container (current OS: %s)", runtime.GOOS)
	}
	path, err := exec.LookPath("tc")
	if err != nil {
		return fmt.Errorf("network impairment requires iproute2 tc in the peer container: %w", err)
	}
	interfaces, err := systemInterfaces()
	if err != nil {
		return fmt.Errorf("network impairment: enumerate container interfaces: %w", err)
	}
	run := func(ctx context.Context, args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, path, args...).CombinedOutput()
	}
	return apply(ctx, config, p2pPort, interfaces, run)
}

func systemInterfaces() ([]networkInterface, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	result := make([]networkInterface, 0, len(interfaces))
	for _, device := range interfaces {
		if device.Flags&net.FlagUp == 0 || device.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, err := device.Addrs()
		if err != nil {
			return nil, fmt.Errorf("interface %s addresses: %w", device.Name, err)
		}
		result = append(result, networkInterface{name: device.Name, flags: device.Flags, addresses: addresses})
	}
	return result, nil
}

func activeIPv4Interfaces(interfaces []networkInterface) []string {
	var devices []string
	for _, device := range interfaces {
		if device.flags&net.FlagUp == 0 || device.flags&net.FlagLoopback != 0 {
			continue
		}
		for _, address := range device.addresses {
			var ip net.IP
			switch address := address.(type) {
			case *net.IPNet:
				ip = address.IP
			case *net.IPAddr:
				ip = address.IP
			}
			if ip.To4() != nil && !ip.IsLoopback() && !ip.IsUnspecified() {
				devices = append(devices, device.name)
				break
			}
		}
	}
	sort.Strings(devices)
	return devices
}

func apply(ctx context.Context, config model.NetworkConfig, p2pPort int, interfaces []networkInterface, run commandRunner) error {
	if err := config.Validate(); err != nil {
		return fmt.Errorf("network impairment: %w", err)
	}
	if config.DelayDistribution != nil {
		return fmt.Errorf("network impairment: delayDistribution must be resolved to a per-peer delay before applying tc")
	}
	if !config.Enabled() {
		return nil
	}
	if p2pPort == 0 {
		p2pPort = DefaultP2PPort
	}
	if p2pPort < 1 || p2pPort > 65535 {
		return fmt.Errorf("network impairment: P2P port must be between 1 and 65535")
	}
	devices := activeIPv4Interfaces(interfaces)
	if len(devices) == 0 {
		return fmt.Errorf("network impairment: no active non-loopback IPv4 interface in the peer container")
	}
	var installed []string
	for _, device := range devices {
		for index, args := range commands(device, config, p2pPort) {
			output, err := run(ctx, args...)
			if err != nil {
				failure := fmt.Errorf("network impairment: tc %s: %w: %s; requires CAP_NET_ADMIN and kernel support for the configured qdiscs (sch_prio, sch_netem, cls_u32, and sch_tbf when enabled)", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
				return errors.Join(failure, rollback(installed, run))
			}
			if index == 0 {
				installed = append(installed, device)
			}
		}
	}
	return nil
}

func rollback(devices []string, run commandRunner) error {
	// Cleanup still needs to run if the startup context was canceled.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var failures []error
	for i := len(devices) - 1; i >= 0; i-- {
		if output, err := run(ctx, "qdisc", "del", "dev", devices[i], "root", "handle", "1:"); err != nil {
			failures = append(failures, fmt.Errorf("remove partial network impairment on %s: %w: %s", devices[i], err, strings.TrimSpace(string(output))))
		}
	}
	return errors.Join(failures...)
}

func commands(device string, config model.NetworkConfig, p2pPort int) [][]string {
	if config.Scope == "all" {
		root := []string{"qdisc", "replace", "dev", device, "root", "handle", "1:", "netem"}
		root = append(root, options(config)...)
		result := [][]string{root}
		if config.TBF != nil {
			result = append(result, tbfCommand(device, "1:1", "2:", *config.TBF))
		}
		return result
	}
	// Every unclassified priority goes to band 0 (class 1:1), keeping the
	// peer's HTTP server and telemetry usable even with lossPercent: 100.
	root := []string{"qdisc", "replace", "dev", device, "root", "handle", "1:", "prio", "bands", "3", "priomap"}
	for i := 0; i < 16; i++ {
		root = append(root, "0")
	}
	child := []string{"qdisc", "replace", "dev", device, "parent", "1:3", "handle", "30:", "netem"}
	child = append(child, options(config)...)
	result := [][]string{root, child}
	if config.TBF != nil {
		result = append(result, tbfCommand(device, "30:1", "31:", *config.TBF))
	}
	for index, direction := range []string{"sport", "dport"} {
		result = append(result, []string{
			"filter", "add", "dev", device, "protocol", "ip", "parent", "1:", "prio", strconv.Itoa(index + 1), "u32",
			"match", "ip", "protocol", "6", "0xff",
			"match", "ip", direction, strconv.Itoa(p2pPort), "0xffff", "flowid", "1:3",
		})
	}
	return result
}

func tbfCommand(device, parent, handle string, config model.TBFConfig) []string {
	latency, _ := time.ParseDuration(config.Latency)
	// tc's TBF parser uses an unsigned 32-bit microsecond duration and does
	// not accept netem's nanosecond suffix. Round up to retain the requested
	// minimum queueing allowance at the parser's microsecond resolution.
	latencyMicros := (latency + time.Microsecond - 1) / time.Microsecond
	return []string{"qdisc", "replace", "dev", device, "parent", parent, "handle", handle, "tbf",
		"rate", strconv.FormatFloat(config.RateMbps, 'f', -1, 64) + "mbit",
		"burst", strconv.FormatFloat(config.BurstKbit, 'f', -1, 64) + "kbit",
		"latency", strconv.FormatInt(int64(latencyMicros), 10) + "us"}
}

func options(config model.NetworkConfig) []string {
	var args []string
	delay, _ := time.ParseDuration(config.Delay)
	jitter, _ := time.ParseDuration(config.Jitter)
	if delay > 0 {
		args = append(args, "delay", strconv.FormatInt(int64(delay), 10)+"ns")
		if jitter > 0 {
			args = append(args, strconv.FormatInt(int64(jitter), 10)+"ns")
			distribution := config.JitterDistribution
			if distribution == "" {
				distribution = "normal"
			}
			// Uniform is netem's built-in default; unlike normal/pareto it
			// does not require a distribution table installed with iproute2.
			if distribution != "uniform" {
				args = append(args, "distribution", distribution)
			}
		}
	}
	for _, field := range []struct {
		name  string
		value *float64
	}{
		{"loss", config.LossPercent},
		{"duplicate", config.DuplicatePercent},
		{"corrupt", config.CorruptPercent},
		{"reorder", config.ReorderPercent},
	} {
		if field.value != nil && *field.value > 0 {
			args = append(args, field.name, strconv.FormatFloat(*field.value, 'f', -1, 64)+"%")
			if field.name == "reorder" && config.ReorderCorrelationPercent != nil {
				args = append(args, strconv.FormatFloat(*config.ReorderCorrelationPercent, 'f', -1, 64)+"%")
			}
		}
	}
	if config.RateMbps != nil && *config.RateMbps > 0 {
		args = append(args, "rate", strconv.FormatFloat(*config.RateMbps, 'f', -1, 64)+"mbit")
	}
	if config.QueueLimit != nil {
		args = append(args, "limit", strconv.Itoa(*config.QueueLimit))
	}
	return args
}

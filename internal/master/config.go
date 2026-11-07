package master

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/controller"
)

// Config is a literal deployment configuration. It is never interpreted by a shell.
type Config map[string]string

var ConfigKeys = []string{
	"KPL_STACK_NAME", "KPL_CONTROL_NODE_ID", "KPL_PEER_NETWORK", "KPL_PEER_SUBNET", "KPL_IMAGE",
	"KPL_AGENT_CAPACITY", "KPL_AGENT_HOST_REBOOT_ENABLED", "KPL_RUN_MIN_FREE_BYTES", "KPL_ARCHIVE_MIB_PER_SECOND", "KPL_AGENT_METRICS_PORT", "KPL_USER", "KPL_PASSWORD",
	"GRAFANA_ADMIN_USER", "GRAFANA_ADMIN_PASSWORD", "KPL_HTTP_PORT", "PROMETHEUS_PORT", "GRAFANA_PORT",
	"KPL_MIN_AGENTS", "KPL_DOCKER_TIMEOUT", "KPL_IMAGE_BUILD_TIMEOUT", "KPL_IMAGE_PUSH_TIMEOUT",
	"KPL_IMAGE_PULL_TIMEOUT", "KPL_CONTROLLER_STOP_TIMEOUT", "KPL_AGENT_STOP_TIMEOUT",
}

var (
	stackNamePattern       = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
	nodeIDPattern          = regexp.MustCompile(`^[a-zA-Z0-9]+$`)
	networkNamePattern     = regexp.MustCompile(`^[a-zA-Z0-9_.][a-zA-Z0-9_.-]*$`)
	imageRepositoryPattern = regexp.MustCompile(`^[a-zA-Z0-9._:][a-zA-Z0-9._:/-]*$`)
	imageTagPattern        = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$`)
	digestPattern          = regexp.MustCompile(`^[a-f0-9]{64}$`)
	positiveIntegerPattern = regexp.MustCompile(`^[1-9][0-9]*$`)
)

func configKey(key string) bool {
	for _, allowed := range ConfigKeys {
		if key == allowed {
			return true
		}
	}
	return false
}

func ValidateSetting(key, value string) error {
	if !configKey(key) {
		return errors.New("unsupported config key")
	}
	if strings.ContainsAny(value, "\r\n\x00") {
		return fmt.Errorf("%s must fit on one line without NUL bytes", key)
	}
	switch key {
	case "KPL_AGENT_HOST_REBOOT_ENABLED":
		if value != "true" && value != "false" {
			return errors.New("KPL_AGENT_HOST_REBOOT_ENABLED must be true or false")
		}
	case "KPL_STACK_NAME":
		if !stackNamePattern.MatchString(value) {
			return errors.New("invalid KPL_STACK_NAME")
		}
	case "KPL_CONTROL_NODE_ID":
		if !nodeIDPattern.MatchString(value) {
			return errors.New("KPL_CONTROL_NODE_ID must be an exact node ID")
		}
	case "KPL_PEER_NETWORK":
		if !networkNamePattern.MatchString(value) {
			return errors.New("invalid KPL_PEER_NETWORK")
		}
	case "KPL_PEER_SUBNET":
		if value == "" {
			return nil
		}
		prefix, err := netip.ParsePrefix(value)
		if err != nil || !prefix.Addr().Is4() || prefix.Bits() < 1 || prefix.Bits() > 30 || prefix.Masked() != prefix || prefix.String() != value {
			return errors.New("KPL_PEER_SUBNET must be a canonical IPv4 network CIDR with prefix 1 through 30")
		}
	case "KPL_HTTP_PORT", "KPL_AGENT_METRICS_PORT", "PROMETHEUS_PORT", "GRAFANA_PORT":
		n, err := strconv.ParseUint(value, 10, 16)
		if !positiveIntegerPattern.MatchString(value) || err != nil || n == 0 {
			return fmt.Errorf("%s must be a port between 1 and 65535", key)
		}
	case "KPL_ARCHIVE_MIB_PER_SECOND":
		if value != "0" && !positiveIntegerPattern.MatchString(value) {
			return errors.New("KPL_ARCHIVE_MIB_PER_SECOND must be a nonnegative whole number without leading zeros")
		}
		if _, err := strconv.ParseUint(value, 10, 64); err != nil {
			return fmt.Errorf("invalid KPL_ARCHIVE_MIB_PER_SECOND: %w", err)
		}
	case "KPL_RUN_MIN_FREE_BYTES":
		if value == "0" {
			return nil
		}
		fallthrough
	case "KPL_AGENT_CAPACITY", "KPL_MIN_AGENTS", "KPL_DOCKER_TIMEOUT", "KPL_IMAGE_BUILD_TIMEOUT", "KPL_IMAGE_PUSH_TIMEOUT", "KPL_IMAGE_PULL_TIMEOUT", "KPL_CONTROLLER_STOP_TIMEOUT", "KPL_AGENT_STOP_TIMEOUT":
		n, err := strconv.ParseInt(value, 10, 64)
		if !positiveIntegerPattern.MatchString(value) || err != nil || n <= 0 {
			return fmt.Errorf("%s must be a supported positive integer without leading zeros", key)
		}
		if strings.HasSuffix(key, "_TIMEOUT") && n > math.MaxInt64/int64(time.Second) {
			return fmt.Errorf("%s exceeds the supported timeout range", key)
		}
	case "KPL_IMAGE":
		return validateImage(value)
	case "KPL_USER":
		if value == "" || strings.TrimSpace(value) != value || len(value) > 256 {
			return errors.New("KPL_USER must contain 1 to 256 bytes without surrounding whitespace")
		}
	case "KPL_PASSWORD":
		if value == "" || len(value) > 4096 {
			return errors.New("KPL_PASSWORD must contain 1 to 4096 bytes")
		}
	case "GRAFANA_ADMIN_USER", "GRAFANA_ADMIN_PASSWORD":
		if value == "" {
			return fmt.Errorf("%s cannot be empty", key)
		}
	}
	return nil
}

func validateImage(value string) error {
	name := value
	digest := ""
	if i := strings.IndexByte(name, '@'); i >= 0 {
		digest = strings.TrimPrefix(name[i+1:], "sha256:")
		if !strings.HasPrefix(name[i+1:], "sha256:") || !digestPattern.MatchString(digest) {
			return errors.New("KPL_IMAGE digest must use sha256 with 64 lowercase hex digits")
		}
		name = name[:i]
	}
	repository := name
	last := name[strings.LastIndexByte(name, '/')+1:]
	if strings.Contains(last, ":") {
		i := strings.LastIndexByte(name, ':')
		if !imageTagPattern.MatchString(name[i+1:]) {
			return errors.New("KPL_IMAGE has an invalid tag")
		}
		repository = name[:i]
	} else if digest == "" {
		return errors.New("KPL_IMAGE requires an explicit tag or sha256 digest")
	}
	if !imageRepositoryPattern.MatchString(repository) || strings.HasSuffix(repository, "/") || strings.Contains(repository, "//") || strings.Contains(repository, "://") {
		return errors.New("KPL_IMAGE has an invalid repository reference")
	}
	return nil
}

func ConfigDefaults(c Config) Config {
	if c == nil {
		c = Config{}
	}
	defaults := map[string]string{
		"KPL_AGENT_HOST_REBOOT_ENABLED": "false",
		"KPL_ARCHIVE_MIB_PER_SECOND":    strconv.FormatUint(controller.DefaultArchiveMiBPerSecond, 10),
		"KPL_STACK_NAME":                "kpl", "KPL_RUN_MIN_FREE_BYTES": "1073741824", "KPL_AGENT_CAPACITY": "20", "KPL_MIN_AGENTS": "1",
		"KPL_USER": "admin", "GRAFANA_ADMIN_USER": "admin", "KPL_HTTP_PORT": "8080", "KPL_AGENT_METRICS_PORT": "9091",
		"PROMETHEUS_PORT": "9090", "GRAFANA_PORT": "3000", "KPL_DOCKER_TIMEOUT": "60", "KPL_IMAGE_BUILD_TIMEOUT": "1800",
		"KPL_IMAGE_PUSH_TIMEOUT": "600", "KPL_IMAGE_PULL_TIMEOUT": "300", "KPL_CONTROLLER_STOP_TIMEOUT": "960", "KPL_AGENT_STOP_TIMEOUT": "240",
	}
	for key, value := range defaults {
		if c[key] == "" {
			c[key] = value
		}
	}
	if c["KPL_PEER_NETWORK"] == "" {
		c["KPL_PEER_NETWORK"] = c["KPL_STACK_NAME"] + "-peers"
	}
	return c
}

func ValidatePortConflicts(c Config) error {
	for _, key := range []string{"KPL_HTTP_PORT", "KPL_AGENT_METRICS_PORT", "PROMETHEUS_PORT", "GRAFANA_PORT"} {
		if err := ValidateSetting(key, c[key]); err != nil {
			return err
		}
	}
	for _, key := range []string{"KPL_HTTP_PORT", "PROMETHEUS_PORT", "GRAFANA_PORT"} {
		if c["KPL_AGENT_METRICS_PORT"] == c[key] {
			return fmt.Errorf("KPL_AGENT_METRICS_PORT conflicts with %s", key)
		}
	}
	if c["KPL_AGENT_METRICS_PORT"] == "2377" || c["KPL_AGENT_METRICS_PORT"] == "7946" {
		return errors.New("KPL_AGENT_METRICS_PORT conflicts with a Docker Swarm TCP port (2377 or 7946)")
	}
	return nil
}

func validateConfig(c Config) error {
	for _, key := range ConfigKeys {
		if value, ok := c[key]; ok {
			if err := ValidateSetting(key, value); err != nil {
				return err
			}
		}
	}
	return ValidatePortConflicts(c)
}

func configEnvironment(environ []string) Config {
	c := Config{}
	for _, entry := range environ {
		key, value, ok := strings.Cut(entry, "=")
		if ok && configKey(key) {
			c[key] = value
		}
	}
	return c
}

// parseConfig keeps the historical first-value-wins and one-pair quote rules.
// Shell expansion, escape processing and command substitution are never used.
func parseConfig(data []byte) (Config, bool, error) {
	c := Config{}
	legacy := false
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	line := 0
	for scanner.Scan() {
		line++
		entry := scanner.Text()
		if entry == "" || strings.HasPrefix(entry, "#") {
			continue
		}
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, false, fmt.Errorf("invalid config entry at line %d", line)
		}
		if key == "KPL_API_TOKEN" {
			legacy = true
			continue
		}
		if !configKey(key) {
			return nil, false, fmt.Errorf("unsupported config key at line %d", line)
		}
		if len(value) >= 2 && (value[0] == '\'' && value[len(value)-1] == '\'' || value[0] == '"' && value[len(value)-1] == '"') {
			value = value[1 : len(value)-1]
		}
		if _, exists := c[key]; !exists {
			c[key] = value
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, false, errors.New("config line exceeds supported length")
	}
	return c, legacy, nil
}

func readConfig(path string, required bool) (Config, bool, error) {
	data, _, err := readConfigBytes(path)
	if errors.Is(err, os.ErrNotExist) && !required {
		return Config{}, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read config %s: %w", path, err)
	}
	return parseConfig(data)
}

func readConfigBytes(path string) ([]byte, os.FileInfo, error) {
	// Nonblocking open prevents a replaced path/FIFO from hanging before the
	// regular-file check. It does not change reads from ordinary files.
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, errors.New("configuration must be a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return nil, nil, err
	}
	if len(data) > 1<<20 {
		return nil, nil, errors.New("configuration exceeds 1 MiB")
	}
	return data, info, nil
}

func encodeConfig(c Config) []byte {
	var output strings.Builder
	output.WriteString("# Literal KEY=VALUE entries; environment variables take precedence.\n")
	for _, key := range ConfigKeys {
		value, exists := c[key]
		if !exists {
			continue
		}
		if len(value) >= 2 && (value[0] == '\'' && value[len(value)-1] == '\'' || value[0] == '"' && value[len(value)-1] == '"') {
			value = "'" + value + "'"
		}
		fmt.Fprintf(&output, "%s=%s\n", key, value)
	}
	return []byte(output.String())
}

func configSettings(args []string) (Config, error) {
	settings := Config{}
	for _, arg := range args {
		key, value, ok := strings.Cut(arg, "=")
		if !ok {
			return nil, errors.New("use literal KEY=VALUE arguments with init or configure")
		}
		if err := ValidateSetting(key, value); err != nil {
			return nil, err
		}
		if _, exists := settings[key]; exists {
			return nil, fmt.Errorf("duplicate setting: %s", key)
		}
		settings[key] = value
	}
	return settings, nil
}

func randomPassword() (string, error) {
	data := make([]byte, 32)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}

func configTimeout(ctx context.Context, c Config, key string) (context.Context, context.CancelFunc) {
	seconds, _ := strconv.ParseInt(c[key], 10, 64)
	return context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// saveConfig creates a private staging directory next to the destination, so
// publication is atomic. Init uses link(2) to avoid overwriting another writer.
func saveConfig(path string, data []byte, original []byte, originalInfo os.FileInfo, initialize bool) error {
	if len(data) > 1<<20 {
		return errors.New("configuration exceeds 1 MiB")
	}
	parent := filepath.Dir(path)
	stage, err := os.MkdirTemp(parent, ".env.swarm.stage.")
	if err != nil {
		return fmt.Errorf("create private config staging directory: %w", err)
	}
	defer os.RemoveAll(stage)
	name := filepath.Join(stage, "config")
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	if initialize {
		if err := os.Link(name, path); err != nil {
			return fmt.Errorf("create config (existing files are never overwritten): %w", err)
		}
	} else {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || !os.SameFile(originalInfo, info) {
			return errors.New("config changed while editing; retry configure")
		}
		current, currentInfo, err := readConfigBytes(path)
		if err != nil || !os.SameFile(originalInfo, currentInfo) || !bytes.Equal(current, original) {
			return errors.New("config changed while editing; retry configure")
		}
		if err := os.Rename(name, path); err != nil {
			return err
		}
	}
	return syncDirectory(parent)
}

func (a *App) configPath() string {
	if a.EnvFile != "" {
		return a.EnvFile
	}
	return filepath.Join(a.Repo, ".env.swarm")
}

func (a *App) LoadConfig() (Config, error) {
	c, _, err := readConfig(a.configPath(), a.EnvFileExplicit)
	if err != nil {
		return nil, err
	}
	for key, value := range configEnvironment(a.Environ) {
		c[key] = value
	}
	ConfigDefaults(c)
	if err := validateConfig(c); err != nil {
		return nil, err
	}
	return c, nil
}

func (a *App) ConfigCommand(ctx context.Context, name string, args []string) error {
	switch name {
	case "config", "credentials":
		if len(args) != 0 {
			return fmt.Errorf("%s takes no arguments", name)
		}
	case "configure":
		if len(args) == 0 {
			return errors.New("configure requires at least one KEY=VALUE setting")
		}
	case "init":
	default:
		return errors.New("unknown configuration command")
	}
	settings, err := configSettings(args)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path := a.configPath()
	c := Config{}
	var original []byte
	var originalInfo os.FileInfo
	legacy := false
	switch name {
	case "init":
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("config already exists: %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		c = configEnvironment(a.Environ)
	case "configure":
		lock := path + ".lock"
		if err := os.Mkdir(lock, 0700); err != nil {
			return fmt.Errorf("another configuration edit is active, or a stale lock exists: %s", lock)
		}
		defer os.Remove(lock)
		originalInfo, err = os.Lstat(path)
		if err != nil || !originalInfo.Mode().IsRegular() {
			return errors.New("configure requires an existing regular config file, not a symlink")
		}
		var current os.FileInfo
		original, current, err = readConfigBytes(path)
		if err != nil {
			return err
		}
		if !os.SameFile(originalInfo, current) {
			return errors.New("config changed while opening; retry configure")
		}
		c, legacy, err = parseConfig(original)
		if err != nil {
			return err
		}
	default:
		c, _, err = readConfig(path, true)
		if err != nil {
			return err
		}
		for key, value := range configEnvironment(a.Environ) {
			c[key] = value
		}
	}
	for key, value := range settings {
		c[key] = value
	}
	ConfigDefaults(c)
	if err := validateConfig(c); err != nil {
		return err
	}
	if name == "init" {
		queryCtx, cancel := configTimeout(ctx, c, "KPL_DOCKER_TIMEOUT")
		status, err := a.WithConfig(c).Output(queryCtx, "docker", "info", "--format", "{{.Swarm.LocalNodeState}} {{.Swarm.ControlAvailable}}")
		cancel()
		if err != nil {
			return fmt.Errorf("inspect Swarm manager: %w", err)
		}
		if status != "active true" {
			return errors.New("run against an active Swarm manager")
		}
		if c["KPL_CONTROL_NODE_ID"] == "" {
			queryCtx, cancel := configTimeout(ctx, c, "KPL_DOCKER_TIMEOUT")
			c["KPL_CONTROL_NODE_ID"], err = a.WithConfig(c).Output(queryCtx, "docker", "info", "--format", "{{.Swarm.NodeID}}")
			cancel()
			if err != nil {
				return fmt.Errorf("identify current Swarm manager: %w", err)
			}
		}
		if c["GRAFANA_ADMIN_PASSWORD"] == "" {
			c["GRAFANA_ADMIN_PASSWORD"], err = randomPassword()
			if err != nil {
				return err
			}
		}
		if c["KPL_IMAGE"] == "" {
			c["KPL_IMAGE"] = "registry.example.com/kpl-v3:v3"
		}
	}
	if (name == "init" || name == "configure") && c["KPL_PASSWORD"] == "" {
		c["KPL_PASSWORD"], err = randomPassword()
		if err != nil {
			return err
		}
	}
	if err := validateConfig(c); err != nil {
		return err
	}
	if name == "config" {
		fmt.Fprintf(a.Stdout, "Effective configuration (environment overrides %s):\n", path)
		for _, key := range ConfigKeys {
			value, ok := c[key]
			if !ok {
				continue
			}
			if key == "KPL_PASSWORD" || key == "GRAFANA_ADMIN_PASSWORD" {
				value = "[redacted]"
			}
			fmt.Fprintf(a.Stdout, "%s=%s\n", key, value)
		}
		return nil
	}
	// This is the explicit operator-requested disclosure command. Ordinary
	// configuration output and every other command continue to redact secrets.
	if name == "credentials" {
		if c["KPL_PASSWORD"] == "" || c["GRAFANA_ADMIN_PASSWORD"] == "" {
			return errors.New("dashboard and Grafana passwords must both be configured")
		}
		fmt.Fprintln(a.Stdout, "Credentials from the effective configuration; keep this output private.")
		for _, key := range []string{"KPL_USER", "KPL_PASSWORD", "GRAFANA_ADMIN_USER", "GRAFANA_ADMIN_PASSWORD"} {
			fmt.Fprintf(a.Stdout, "%s=%s\n", key, c[key])
		}
		fmt.Fprintln(a.Stdout, "Grafana retains its existing username and synchronizes the original administrator password on startup.")
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := saveConfig(path, encodeConfig(c), original, originalInfo, name == "init"); err != nil {
		return err
	}
	if name == "init" {
		fmt.Fprintf(a.Stdout, "Created %s (mode 0600). Review it with: kpl swarm config\n", path)
	} else {
		fmt.Fprintf(a.Stdout, "Updated %s (mode 0600); existing credentials are preserved unless explicitly changed.\n", path)
		if legacy {
			fmt.Fprintln(a.Stdout, "Migrated KPL_API_TOKEN to dashboard login settings.")
		}
		fmt.Fprintln(a.Stdout, "Exported environment variables still override saved settings. Run deploy to apply changes to services.")
	}
	return nil
}

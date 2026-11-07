package master

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"
)

var swarmCommands = []string{"deploy", "restart", "status", "check", "access", "logs", "remove", "node", "network"}

func usageError(message string) error { return &UsageError{Message: message} }

// IsRuntimeInvocation also recognizes role commands after operator path options.
// Role flags themselves are never scanned as operator options.
func IsRuntimeInvocation(args []string) bool {
	i := 0
	for i < len(args) {
		if args[i] == "--" {
			i++
			break
		}
		key, value, equals := strings.Cut(args[i], "=")
		if key != "--repo" && key != "--env-file" {
			break
		}
		if !equals {
			i++
			if i >= len(args) {
				return false
			}
			value = args[i]
		}
		if value == "" {
			return false
		}
		i++
	}
	return i+1 < len(args) && args[i] == "run" && IsRuntimeCommand(args[i+1])
}

// validateCLIPath admits only public command paths. Handler names used after
// normalization are private implementation details and cannot be invoked as
// alternate command entrypoints.
func validateCLIPath(words []string) error {
	if len(words) == 0 {
		return nil
	}
	known := func(value string, names ...string) bool {
		for _, name := range names {
			if value == name {
				return true
			}
		}
		return value == "--help" || value == "-h"
	}
	bad := func() error { return usageError("unknown command path; run kpl help") }
	if words[0] == "help" {
		return validateCLIPath(words[1:])
	}
	switch words[0] {
	case "version", "--help", "-h":
		return nil
	case "completion":
		if len(words) < 2 || known(words[1], "bash") {
			return nil
		}
	case "dev":
		if len(words) < 2 || known(words[1], "build", "check", "vet", "test") {
			return nil
		}
	case "config":
		if len(words) < 2 || known(words[1], "init", "set", "show", "credentials", "version") {
			return nil
		}
	case "image":
		if len(words) < 2 || known(words[1], "login", "publish", "inspect") {
			return nil
		}
	case "scenario":
		if len(words) < 2 || known(words[1], "validate", "show") {
			return nil
		}
	case "run":
		if len(words) < 2 || IsRuntimeCommand(words[1]) || known(words[1]) {
			return nil
		}
	case "swarm":
		if len(words) < 2 || known(words[1], "deploy", "restart", "status", "check", "access", "logs", "remove") {
			return nil
		}
		if words[1] == "node" && (len(words) < 3 || known(words[2], "list", "add", "remove")) {
			return nil
		}
		if words[1] == "network" && (len(words) < 3 || known(words[2], "reset")) {
			return nil
		}
	}
	return bad()
}

// canonicalCLICommand maps the public command tree to native handlers.
// Container roles retain their own option parsing.
func canonicalCLICommand(command string, args []string) (string, []string) {
	if len(args) == 0 {
		return command, args
	}
	switch command {
	case "dev":
		switch args[0] {
		case "build", "check", "vet", "test":
			return args[0], args[1:]
		}
	case "config":
		name := map[string]string{"init": "init", "set": "configure", "show": "config", "credentials": "credentials"}[args[0]]
		if name != "" {
			return "swarm", append([]string{name}, args[1:]...)
		}
		if args[0] == "version" {
			return "config-version", args[1:]
		}
	case "image":
		name := map[string]string{"login": "login", "publish": "publish", "inspect": "image-inspect"}[args[0]]
		if name != "" {
			return "swarm", append([]string{name}, args[1:]...)
		}
	case "scenario":
		switch args[0] {
		case "validate":
			return "validate", args[1:]
		case "show":
			return "swarm", append([]string{"scenario"}, args[1:]...)
		}
	case "swarm":
		if len(args) > 1 && args[0] == "node" {
			name := map[string]string{"list": "nodes", "add": "add-node", "remove": "remove-node"}[args[1]]
			if name != "" {
				return "swarm", append([]string{name}, args[2:]...)
			}
		}
		if len(args) > 1 && args[0] == "network" && args[1] == "reset" {
			return "swarm", append([]string{"reset-peer-network"}, args[2:]...)
		}
	case "run":
		if IsRuntimeCommand(args[0]) {
			return args[0], args[1:]
		}
	}
	return command, args
}

func cliHelpRequested(words []string) bool {
	for i := 0; i < len(words); i++ {
		switch words[i] {
		case "--scenario", "--platforms", "--image", "--tail", "--context":
			// These options consume literal string values, including a filename
			// named --help. KEY=VALUE settings never match a help flag either.
			i++
		case "--help", "-h":
			return true
		}
	}
	return false
}

func (a *App) Execute(ctx context.Context, args []string, version string) error {
	if len(args) > 1 && args[0] == "run" && IsRuntimeCommand(args[1]) {
		return a.executeRuntime(ctx, args[1:])
	}
	if len(args) > 0 && args[0] == "__complete" {
		return a.Complete(ctx, args[1:])
	}
	var repo string
	var rest []string
	separator := -1
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if len(rest) == 0 {
			if arg == "run" && i+1 < len(args) && IsRuntimeCommand(args[i+1]) {
				return a.executeRuntime(ctx, args[i+1:])
			}
		}
		if arg == "--" {
			separator = len(rest)
			rest = append(rest, args[i+1:]...)
			break
		}
		key, value, hasValue := strings.Cut(arg, "=")
		switch key {
		case "--scenario", "--platforms", "--image", "--tail", "--context":
			// A subcommand value may itself look like a global option. Keep
			// it literal, while allowing -- to introduce a positional value.
			rest = append(rest, arg)
			if !hasValue && i+1 < len(args) && args[i+1] != "--" {
				i++
				rest = append(rest, args[i])
			}
		case "--repo", "--env-file":
			if !hasValue {
				i++
				if i >= len(args) {
					return usageError(key + " needs a path")
				}
				value = args[i]
			}
			if value == "" {
				return usageError(key + " needs a nonempty path")
			}
			absolute, err := filepath.Abs(value)
			if err != nil {
				return err
			}
			if key == "--repo" {
				repo = absolute
			} else {
				a.EnvFile = absolute
				a.EnvFileExplicit = true
			}
		default:
			rest = append(rest, arg)
		}
	}
	if len(rest) == 0 {
		return a.Help(nil)
	}
	if err := validateCLIPath(rest); err != nil {
		return err
	}
	if rest[0] == "help" {
		return a.Help(rest[1:])
	}
	command, arguments := canonicalCLICommand(rest[0], rest[1:])
	// Inserting a subcommand for an alias also shifts the separator position.
	if separator > 0 {
		separator += len(arguments) - (len(rest) - 1)
	}
	normalized := append([]string{command}, arguments...)
	helpWords := normalized
	if separator >= 0 {
		helpWords = normalized[:separator]
	}
	if cliHelpRequested(helpWords) {
		return a.Help(rest)
	}
	if command == "version" {
		return a.printVersion(version, arguments, false)
	}
	if command == "completion" {
		if len(arguments) != 1 || arguments[0] != "bash" {
			return usageError("usage: kpl completion bash")
		}
		_, err := io.WriteString(a.Stdout, bashCompletion)
		return err
	}
	if command == "run" || command == "dev" || command == "config" || command == "image" || command == "scenario" {
		if len(arguments) == 0 {
			return a.Help([]string{command})
		}
		return usageError("unknown " + command + " command " + arguments[0] + "; run kpl " + command + " --help")
	}
	if command == "config-version" && (len(arguments) > 1 || len(arguments) == 1 && arguments[0] != "--json") {
		return usageError("version accepts only --json")
	}
	if IsRuntimeCommand(command) {
		return a.executeRuntime(ctx, normalized)
	}
	if runtime.GOOS != "linux" {
		return fmt.Errorf("KPL operations and executable checks require Linux")
	}
	switch command {
	case "build", "check", "test", "vet", "validate", "swarm", "config-version":
	default:
		return usageError("unknown command " + command + "; run kpl help")
	}
	if command == "swarm" && len(arguments) == 0 {
		return a.Help([]string{"swarm"})
	}
	if command == "swarm" && len(arguments) == 1 && (arguments[0] == "node" || arguments[0] == "network") {
		return a.Help(rest)
	}
	// Explicit scenarios also work in images without a source checkout.
	if command != "validate" || len(arguments) == 0 || repo != "" || a.Env("KPL_REPO") != "" {
		if err := a.resolveRepository(repo); err != nil {
			return err
		}
	}
	if a.EnvFile == "" {
		a.EnvFile = filepath.Join(a.Repo, ".env.swarm")
	}
	if command == "config-version" {
		return a.printVersion(version, arguments, true)
	}
	if command != "swarm" {
		if command == "validate" && separator == 1 && len(arguments) == 1 {
			// Preserve a positional filename after --, even if it resembles an
			// option. The Go runtime receives one absolute path as a value.
			path, err := filepath.Abs(arguments[0])
			if err != nil {
				return err
			}
			arguments = []string{"--scenario", path}
		}
		return a.RunDevelopment(ctx, command, arguments)
	}
	name, tail := arguments[0], arguments[1:]
	if name == "scenario" {
		if len(tail) > 1 {
			return usageError("usage: kpl scenario show [FILE]")
		}
		if len(tail) == 1 {
			path, err := filepath.Abs(tail[0])
			if err != nil {
				return err
			}
			tail = []string{path}
		}
	} else if separator >= 2 {
		for _, argument := range normalized[separator:] {
			if strings.HasPrefix(argument, "-") {
				return usageError("arguments after -- are positional; option-like values are not valid nodes or settings")
			}
		}
	}
	switch name {
	case "init", "configure", "config", "credentials":
		return a.ConfigCommand(ctx, name, tail)
	}
	return a.RunSwarm(ctx, name, tail)
}

func (a *App) Help(path []string) error {
	if err := validateCLIPath(path); err != nil {
		return err
	}
	if len(path) > 1 && path[0] == "run" && IsRuntimeCommand(path[1]) {
		return a.executeRuntime(context.Background(), []string{path[1], "--help"})
	}
	if len(path) > 0 {
		var help string
		switch path[0] {
		case "dev":
			help = `Usage: kpl [--repo DIR] dev COMMAND

  build                        Rebuild the single bin/kpl executable
  check                        Build, vet, Go/PubSub, operator and UI checks
  vet                          Run Go static checks
  test [go|swarm|ui|race|linux]  Run tests (default: Go/PubSub)

test swarm uses simulated Docker responses; it does not change a live cluster.
test linux builds the runtime Docker test target. test race checks data races.
`
		case "config":
			help = `Usage: kpl [--repo DIR] [--env-file PATH] config COMMAND

  init [KEY=VALUE...]     Create a private configuration and credentials
  set KEY=VALUE...        Validate and atomically update configuration
  show                   Print effective configuration with secrets redacted
  credentials            Explicitly display saved login credentials
  version [--json]       Print CLI build and configured image information

The default file is .env.swarm in the selected checkout. Environment variables
override file values. Explicit --env-file paths are relative to the caller.
`
		case "image":
			help = `Usage: kpl [--repo DIR] [--env-file PATH] image COMMAND

  login                     Log in to the configured image registry
  publish [--platforms CSV]  Build and push the configured image tag
  inspect [REF]             Show configured image and local image metadata

Example platforms: linux/amd64,linux/arm64. Deploy an existing registry digest
with kpl swarm deploy --image REPOSITORY@sha256:HASH (or --image sha256:HASH
to use the configured repository).
`
		case "scenario":
			help = `Usage: kpl [--repo DIR] scenario COMMAND

  validate [--scenario FILE]  Validate input without starting an experiment
  show [FILE]                Print scenario YAML for the Dashboard

validate defaults to examples/smoke.yaml; show defaults to examples/swarm-smoke.yaml.
Explicit files are relative to the caller. Use -- before option-like filenames.
`
		case "run":
			help = `Usage: kpl run ROLE [OPTIONS]

Roles: controller, agent, swarm-agent, peer, resource-monitor, network-manager,
       network-gateway.

Use kpl run ROLE --help for role-specific flags. Roles run without a source
checkout. Runtime options belong after ROLE.
`
		case "version":
			help = "Usage: kpl version [--json]\n\nPrint this executable's build information. Use kpl config version for the configured image.\n"
		case "completion":
			help = "Usage: kpl completion bash\n\nPrint Bash completion. Load with: source <(kpl completion bash)\n"
		}
		if help != "" {
			_, err := io.WriteString(a.Stdout, help)
			return err
		}
	}
	if len(path) > 0 && path[0] == "swarm" {
		if len(path) > 1 && path[1] == "node" {
			_, err := io.WriteString(a.Stdout, `Usage: kpl [--repo DIR] [--env-file PATH] swarm node COMMAND

  list                    List existing Swarm nodes
  add NODE...|SELECTOR    Add Agent placement on existing Swarm nodes
  remove NODE...|SELECTOR Remove Agent placement after Agent/Peer cleanup

Selectors: --all | --all-excluding-self | --workers.
Use one selector or explicit node names/IDs. These commands manage Agent
placement; joining or leaving Docker Swarm remains a Docker operation.
`)
			return err
		}
		if len(path) > 1 && path[1] == "network" {
			_, err := io.WriteString(a.Stdout, `Usage: kpl [--repo DIR] [--env-file PATH] swarm network reset

Recreate an unused KPL-owned Peer overlay while retaining its IPv4 allocation.
Remove the stack first with kpl swarm remove. Persistent data volumes are retained.
`)
			return err
		}
		_, err := fmt.Fprint(a.Stdout, `Usage: kpl [--repo DIR] [--env-file PATH] swarm COMMAND [OPTIONS]

  deploy [--image REF] [NODE...|SELECTOR]  Deploy or update services
  restart [--image REF] [NODE...|SELECTOR] Pull the registry image, then restart
  status | check | access                 Inspect deployment and access URLs
  logs [COMPONENT] [--tail N|all] [--context NAME]
  node list                              List Swarm nodes
  node add NODE...|SELECTOR               Add Agent placement
  node remove NODE...|SELECTOR            Remove Agent placement after cleanup
  network reset                          Recreate an unused owned Peer overlay
  remove                                 Remove the stack, retaining data volumes

Selectors: --all | --all-excluding-self | --workers (one selector, no explicit nodes).
Bare deploy/restart reuse existing Agent labels; initial deployment needs a selection.
--image accepts REPOSITORY@sha256:HASH or sha256:HASH in the configured repository.
Without a digest, deploy/restart pull the configured tag's latest registry version
on every invocation. A pinned digest selects that exact image. Build and publish
source changes separately with kpl image publish [--platforms CSV].
Restart verifies Controller, Agent and remaining service shutdown before redeployment.
Removal retains data volumes. Network reset requires an unused owned overlay.
Log components: controller, agent, network-manager, resource-monitor, prometheus,
grafana, access, auth. --context applies only to access/auth file reads.
Configuration: kpl config --help. Images: kpl image --help.
`)
		return err
	}
	_, err := fmt.Fprint(a.Stdout, `Usage: kpl [--repo DIR] [--env-file PATH] COMMAND

  config     init | set | show | credentials | version
  image      login | publish | inspect
  swarm      deploy | restart | status | check | access | logs | remove
             node list | add | remove; network reset
  scenario   validate | show
  dev        build | check | vet | test
  run ROLE   Run a container role
  version [--json]          Print this executable's build information
  completion bash          Print Bash completion
  help [COMMAND...]        Show command help

Container roles (use run ROLE --help for role-specific flags):
  controller | agent | swarm-agent | peer | resource-monitor | network-manager | network-gateway

Use kpl COMMAND --help for options.
--repo selects the kpl-v3 checkout (also KPL_REPO); discovery searches the current
directory, its ancestors and sibling v3/kpl-v3 directories, then the binary location.
Make only bootstraps bin/kpl. Development and operation commands use kpl.
Deployment still uses the runtime Dockerfile, stack manifest and monitoring assets.
`)
	return err
}

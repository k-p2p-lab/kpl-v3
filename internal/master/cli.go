package master

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"
)

var swarmCommands = []string{"init", "configure", "config", "credentials", "nodes", "login", "publish", "check", "access", "logs", "scenario", "deploy", "restart", "status", "add-node", "remove-node", "remove", "reset-peer-network"}

func usageError(message string) error { return &UsageError{Message: message} }

// canonicalCLICommand keeps Make-style aliases on the same native dispatch path.
func canonicalCLICommand(command string, args []string) (string, []string) {
	if command == "check-swarm" {
		return "swarm", append([]string{"check"}, args...)
	}
	if strings.HasPrefix(command, "swarm-") {
		name := strings.TrimPrefix(command, "swarm-")
		if name == "reset-network" {
			name = "reset-peer-network"
		}
		return "swarm", append([]string{name}, args...)
	}
	if strings.HasPrefix(command, "test-") {
		return "test", append([]string{strings.TrimPrefix(command, "test-")}, args...)
	}
	return command, args
}

func cliHelpRequested(words []string) bool {
	for i := 0; i < len(words); i++ {
		switch words[i] {
		case "--scenario", "--platforms", "--tail", "--context":
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
	if len(args) > 0 && args[0] == "__complete" {
		return a.Complete(ctx, args[1:])
	}
	var repo string
	var rest []string
	separator := -1
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			separator = len(rest)
			rest = append(rest, args[i+1:]...)
			break
		}
		key, value, hasValue := strings.Cut(arg, "=")
		switch key {
		case "--scenario", "--platforms", "--tail", "--context":
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
	if rest[0] == "help" {
		path := rest[1:]
		if len(path) > 0 {
			command, tail := canonicalCLICommand(path[0], path[1:])
			path = append([]string{command}, tail...)
		}
		return a.Help(path)
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
		return a.Help(normalized)
	}
	if command == "version" || command == "--version" && separator != 0 {
		if len(arguments) != 0 {
			return usageError("version takes no arguments")
		}
		fmt.Fprintln(a.Stdout, "kpl-master", version)
		return nil
	}
	if command == "completion" {
		if len(arguments) != 1 || arguments[0] != "bash" {
			return usageError("usage: kpl completion bash")
		}
		_, err := io.WriteString(a.Stdout, bashCompletion)
		return err
	}
	if runtime.GOOS != "linux" {
		return fmt.Errorf("KPL operations and executable checks require Linux")
	}
	switch command {
	case "build", "check", "test", "vet", "validate", "swarm":
	default:
		return usageError("unknown command " + command + "; run kpl help")
	}
	if command == "swarm" && (len(arguments) == 0 || arguments[0] == "help") {
		return a.Help([]string{"swarm"})
	}
	if err := a.resolveRepository(repo); err != nil {
		return err
	}
	if a.EnvFile == "" {
		a.EnvFile = filepath.Join(a.Repo, ".env.swarm")
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
	if name == "reset-network" {
		name = "reset-peer-network"
	}
	if name == "scenario" {
		if len(tail) > 1 {
			return usageError("usage: kpl swarm scenario [FILE]")
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
	if len(path) > 0 && path[0] == "swarm" {
		_, err := fmt.Fprint(a.Stdout, `Usage: kpl [--repo DIR] [--env-file PATH] swarm COMMAND [OPTIONS]

Configuration: init [KEY=VALUE...] | configure KEY=VALUE... | config | credentials
Images:        login | publish [--platforms CSV]
Cluster:       nodes | check | access | status
Operations:    deploy [NODE...|SELECTOR] | restart [--platforms CSV] [NODE...|SELECTOR]
               add-node NODE...|SELECTOR | remove-node NODE...|SELECTOR
               remove | reset-peer-network
Inspection:    logs [COMPONENT] [--tail N|all] [--context NAME] | scenario [FILE]

Selectors: --all | --all-excluding-self | --workers (one selector, no explicit nodes).
Bare deploy/restart reuse existing Agent labels; initial deployment needs a selection.
Restart publishes first, then verifies Controller, Agent and remaining service shutdown.
Removal retains data volumes. Network reset requires an unused owned overlay.
Log components: controller, agent, network-manager, resource-monitor, prometheus,
grafana, access, auth. --context applies only to access/auth file reads.
Environment overrides the literal configuration file; config redacts secrets.
Explicit --env-file and scenario paths are relative to the invocation directory.
`)
		return err
	}
	_, err := fmt.Fprint(a.Stdout, `Usage: kpl [--repo DIR] [--env-file PATH] COMMAND

  build                    Build operator bin/kpl and runtime bin/kpl-runtime
  check                    Build, vet, Go/PubSub, native operator and UI checks
  test [go|swarm|ui|race|linux|compat]
                           Default: Go/PubSub; compat explicitly checks old entrypoints
  vet                      Run v3 Go static checks
  validate [--scenario FILE] Validate input without executing an experiment
  swarm COMMAND            Configure and operate Linux Docker Swarm (see swarm help)
  completion bash          Print standalone Bash completion
  version                  Print the operator CLI version

--repo selects the kpl-v3 checkout (also KPL_REPO); discovery searches the current
directory, its ancestors and sibling v3/kpl-v3 directories, then the binary location.
Make, swarm.sh and check-swarm.sh are not used by the new operational commands.
Deployment still uses the runtime Dockerfile, stack manifest and monitoring assets.
The operator bin/kpl and application bin/kpl-runtime are different programs.
`)
	return err
}

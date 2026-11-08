package master

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const bashCompletion = `# Load with: source <(kpl completion bash)
_kpl_complete() {
    local current="${COMP_WORDS[COMP_CWORD]}" key now candidate trim='' previous=''
    local -a words=("${COMP_WORDS[@]:1:COMP_CWORD}")
    # Bash normally splits KEY=value at '='. Reassemble only the current value,
    # then remove its prefix from replies so Bash inserts the correct suffix.
    (( COMP_CWORD > 0 )) && previous=${COMP_WORDS[COMP_CWORD-1]}
    if [[ $current == = ]] && (( COMP_CWORD > 1 )); then
        trim=$previous=
        words=("${words[@]:0:${#words[@]}-2}" "$trim")
        current=''
    elif [[ $previous == = ]] && (( COMP_CWORD > 2 )); then
        trim=${COMP_WORDS[COMP_CWORD-2]}=
        words=("${words[@]:0:${#words[@]}-3}" "$trim$current")
    elif [[ $previous == *= ]] && (( COMP_CWORD > 1 )); then
        trim=$previous
        words=("${words[@]:0:${#words[@]}-2}" "$trim$current")
    fi
    printf -v key '%q ' "$PWD" "${DOCKER_CONTEXT-}" "${DOCKER_HOST-}" "${DOCKER_CONFIG-}" "${DOCKER_TLS_VERIFY-}" "${DOCKER_CERT_PATH-}" "${COMP_WORDS[0]}" "${words[@]}"
    now=$SECONDS
    if [[ ${_KPL_CACHE_KEY-} != "$key" ]] || ((now < ${_KPL_CACHE_AT:-0} || now - ${_KPL_CACHE_AT:-0} >= 5)); then
        _KPL_CACHE_DATA=$("${COMP_WORDS[0]}" __complete "${words[@]}" 2>/dev/null)
        _KPL_CACHE_KEY=$key
        _KPL_CACHE_AT=$now
    fi
    COMPREPLY=()
    while IFS= read -r candidate; do
        if [[ -n $trim ]]; then
            [[ $candidate == "$trim"* ]] || continue
            candidate=${candidate#"$trim"}
        fi
        [[ -n $candidate && $candidate == "$current"* ]] && COMPREPLY+=("$candidate")
    done <<< "${_KPL_CACHE_DATA-}"
    compopt -o filenames 2>/dev/null || true
    if [[ ${COMPREPLY[0]-} == *= || ${COMPREPLY[0]-} == */ ]]; then compopt -o nospace 2>/dev/null || true; fi
}
complete -F _kpl_complete kpl
`

var completionPlatforms = []string{"linux/amd64", "linux/arm64", "linux/arm/v7", "linux/amd64,linux/arm64"}

func (a *App) Complete(ctx context.Context, args []string) error {
	if len(args) == 0 {
		args = []string{""}
	}
	prefix := args[len(args)-1]
	words := args[:len(args)-1]
	if IsRuntimeInvocation(words) {
		return printCandidates(a.Stdout, []string{"--help"}, prefix)
	}
	var filtered []string
	separated, valueExpected := false, false
	globalSeparated := false
	separator := -1
	for i := 0; i < len(words); i++ {
		if words[i] == "--" {
			separator = len(filtered)
			globalSeparated = separator == 0
			separated = !globalSeparated
			filtered = append(filtered, words[i+1:]...)
			break
		}
		switch words[i] {
		case "--repo", "--env-file":
			if i == len(words)-1 {
				if validateCLIPath(filtered) != nil {
					return nil
				}
				return printCandidates(a.Stdout, fileCandidates(prefix, words[i] != "--env-file"), prefix)
			}
			i++
		case "--scenario", "--platforms", "--image", "--tail", "--context":
			filtered = append(filtered, words[i])
			if i == len(words)-1 {
				valueExpected = true
			} else if words[i+1] != "--" {
				i++
				filtered = append(filtered, words[i])
			}
		default:
			if !strings.HasPrefix(words[i], "--repo=") && !strings.HasPrefix(words[i], "--env-file=") {
				filtered = append(filtered, words[i])
			}
		}
	}
	words = filtered
	if globalSeparated {
		words, separator = splitLeafSeparator(words)
		separated = separator >= 0
	}
	if len(words) > 0 && words[0] == "help" {
		words = words[1:]
		if separator > 0 {
			separator--
		}
	}
	if validateCLIPath(words) != nil {
		return nil
	}
	if separator > 0 && len(words) > 0 && words[0] != "help" && separator < cliCommandDepth(words) {
		return nil
	}
	if len(words) > 0 {
		command, arguments := canonicalCLICommand(words[0], words[1:])
		if separator > 0 {
			separator += len(arguments) - (len(words) - 1)
		}
		words = append([]string{command}, arguments...)
		if IsRuntimeCommand(command) {
			return printCandidates(a.Stdout, []string{"--help"}, prefix)
		}
	}
	if !separated && !globalSeparated && !valueExpected {
		for _, option := range []string{"--repo", "--env-file"} {
			if value, ok := strings.CutPrefix(prefix, option+"="); ok {
				var candidates []string
				for _, path := range fileCandidates(value, option != "--env-file") {
					candidates = append(candidates, option+"="+path)
				}
				return printCandidates(a.Stdout, candidates, prefix)
			}
		}
	}
	var candidates []string
	valueOptionActive := !separated || len(words)-1 < separator
	if !separated && !globalSeparated && !valueExpected && strings.HasPrefix(prefix, "--") {
		candidates = append(candidates, "--repo", "--env-file", "--help")
	}
	if len(words) == 0 {
		candidates = append(candidates, "config", "image", "swarm", "scenario", "dev", "run", "completion", "version", "help")
		return printCandidates(a.Stdout, candidates, prefix)
	}
	switch words[0] {
	case "dev":
		if len(words) == 1 {
			candidates = append(candidates, "build", "check", "vet", "test")
		}
	case "config":
		if len(words) == 1 {
			candidates = append(candidates, "init", "set", "show", "credentials", "version")
		}
	case "image":
		if len(words) == 1 {
			candidates = append(candidates, "login", "publish", "inspect")
		}
	case "scenario":
		if len(words) == 1 {
			candidates = append(candidates, "validate", "show")
		}
	case "run":
		if len(words) == 1 {
			candidates = append(candidates, roleCommands...)
		}
	case "version", "config-version":
		if !separated && len(words) == 1 {
			candidates = append(candidates, "--json")
		}
	case "completion":
		candidates = append(candidates, "bash")
	case "test":
		if len(words) == 1 {
			candidates = append(candidates, "go", "swarm", "ui", "race", "linux")
		}
	case "validate":
		if !separated && len(words) == 1 && strings.HasPrefix(prefix, "-") {
			candidates = append(candidates, "--scenario")
		} else {
			candidates = append(candidates, fileCandidates(prefix, false)...)
		}
	case "swarm":
		if len(words) == 1 {
			candidates = append(candidates, swarmCommands...)
			break
		}
		switch words[1] {
		case "node":
			if len(words) == 2 {
				candidates = append(candidates, "list", "add", "remove")
			}
		case "network":
			if len(words) == 2 {
				candidates = append(candidates, "reset")
			}
		case "scenario":
			candidates = append(candidates, fileCandidates(prefix, false)...)
		case "init", "configure":
			if strings.HasPrefix(prefix, "KPL_AGENT_HOST_REBOOT_ENABLED=") {
				candidates = append(candidates, "KPL_AGENT_HOST_REBOOT_ENABLED=false", "KPL_AGENT_HOST_REBOOT_ENABLED=true")
			} else if strings.HasPrefix(prefix, "KPL_CONTROL_NODE_ID=") {
				for _, id := range a.completionDocker(ctx, "node", "ls", "--format", "{{.ID}}") {
					candidates = append(candidates, "KPL_CONTROL_NODE_ID="+id)
				}
			} else if !strings.Contains(prefix, "=") {
				for _, key := range ConfigKeys {
					candidates = append(candidates, key+"=")
				}
			}
		case "publish":
			if valueOptionActive && words[len(words)-1] == "--platforms" {
				candidates = append(candidates, completionPlatforms...)
			} else if !separated && len(words) == 2 {
				candidates = append(candidates, "--platforms")
			}
		case "deploy", "restart", "add-node", "remove-node":
			if words[len(words)-1] == "--image" || strings.HasPrefix(prefix, "--image=") {
				return printCandidates(a.Stdout, nil, prefix)
			}
			if words[len(words)-1] == "--platforms" || strings.HasPrefix(prefix, "--platforms=") {
				if words[1] == "restart" && !separated {
					if strings.HasPrefix(prefix, "--platforms=") {
						for _, value := range completionPlatforms {
							candidates = append(candidates, "--platforms="+value)
						}
					} else {
						candidates = append(candidates, completionPlatforms...)
					}
				}
				return printCandidates(a.Stdout, candidates, prefix)
			}
			selected, platforms, image, republish := false, false, false, false
			var nodes []string
			for i := 2; i < len(words); i++ {
				switch words[i] {
				case "--all", "--all-excluding-self", "--workers":
					selected = true
				case "--platforms":
					platforms = true
					i++
				case "--republish":
					republish = true
				case "--image":
					image = true
					i++
				default:
					if strings.HasPrefix(words[i], "--image=") {
						image = true
					} else if strings.HasPrefix(words[i], "--platforms=") {
						platforms = true
					} else {
						nodes = append(nodes, words[i])
					}
				}
			}
			if !selected {
				if !separated && len(nodes) == 0 {
					candidates = append(candidates, "--all", "--all-excluding-self", "--workers")
				}
				if !strings.HasPrefix(prefix, "-") {
					candidates = append(candidates, a.completionNodes(ctx, nodes)...)
				}
			}
			if !separated && (words[1] == "deploy" || words[1] == "restart") && !platforms && !image && !republish && !selected && len(nodes) == 0 {
				candidates = append(candidates, "--image")
			}
			if !separated && words[1] == "restart" && !image && (!selected && len(nodes) == 0 || strings.HasPrefix(prefix, "-")) {
				if republish && !platforms {
					candidates = append(candidates, "--platforms")
				} else if !republish {
					candidates = append(candidates, "--republish")
				}
			}
		case "logs":
			if len(words) == 2 {
				candidates = append(candidates, "controller", "agent", "network-manager", "resource-monitor", "prometheus", "grafana", "access", "auth")
			}
			if valueOptionActive && words[len(words)-1] == "--context" {
				candidates = append(candidates, a.completionDocker(ctx, "context", "ls", "--format", "{{.Name}}")...)
			} else if valueOptionActive && words[len(words)-1] == "--tail" {
				candidates = append(candidates, "100", "all")
			} else if !separated {
				candidates = append(candidates, "--tail")
				if len(words) > 2 && (words[2] == "access" || words[2] == "auth") {
					candidates = append(candidates, "--context")
				}
			}
		}
	}
	return printCandidates(a.Stdout, candidates, prefix)
}

func (a *App) completionDocker(ctx context.Context, args ...string) []string {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	command := a.command("docker", args)
	command.Stdin = nil
	command.Stderr = io.Discard
	command.CancelGrace = 100 * time.Millisecond
	data, err := a.Runner.Output(ctx, command)
	if err != nil {
		return nil
	}
	var result []string
	for _, value := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if value != "" && !strings.ContainsAny(value, "\r\t") {
			result = append(result, value)
		}
	}
	return result
}

func (a *App) completionNodes(ctx context.Context, used []string) []string {
	var candidates []string
	for _, record := range a.completionDocker(ctx, "node", "ls", "--format", "{{.ID}} {{.Hostname}}") {
		fields := strings.Fields(record)
		if len(fields) != 2 || !swarmID.MatchString(fields[0]) {
			continue
		}
		present := false
		for _, node := range used {
			if node == fields[0] || node == fields[1] {
				present = true
				break
			}
		}
		if !present {
			candidates = append(candidates, fields[0])
			if swarmNodeName.MatchString(fields[1]) && !strings.HasPrefix(fields[1], "-") {
				candidates = append(candidates, fields[1])
			}
		}
	}
	return candidates
}

func fileCandidates(prefix string, dirs bool) []string {
	dir, base := filepath.Split(prefix)
	readDir := dir
	if readDir == "" {
		readDir = "."
	}
	entries, err := os.ReadDir(readDir)
	if err != nil {
		return nil
	}
	var result []string
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), base) || dirs && !entry.IsDir() {
			continue
		}
		name := dir + entry.Name()
		if entry.IsDir() {
			name += "/"
		}
		result = append(result, name)
	}
	return result
}

func printCandidates(out io.Writer, candidates []string, prefix string) error {
	candidates = append([]string(nil), candidates...)
	sort.Strings(candidates)
	previous := ""
	for _, candidate := range candidates {
		if candidate != previous && strings.HasPrefix(candidate, prefix) {
			if _, err := fmt.Fprintln(out, candidate); err != nil {
				return err
			}
			previous = candidate
		}
	}
	return nil
}

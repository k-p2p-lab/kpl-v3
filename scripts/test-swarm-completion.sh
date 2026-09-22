#!/usr/bin/env bash
# Completion tests use a fake Docker CLI; no daemon or credentials are accessed.
set -euo pipefail
root=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/bin" "$scratch/files/directory with spaces"
export KPL_COMPLETION_CALLS=$scratch/docker-calls
: > "$KPL_COMPLETION_CALLS"
cat > "$scratch/bin/docker" <<'MOCK'
#!/bin/sh
printf '%s|%s\n' "${DOCKER_CONTEXT-}" "$*" >> "$KPL_COMPLETION_CALLS"
[ "${KPL_COMPLETION_FAIL:-0}" != 1 ] || exit 1
if [ "${KPL_COMPLETION_STALL:-0}" = 1 ]; then exec sleep 10; fi
case "$*" in
    'node ls --format {{.ID}} {{.Hostname}}') printf 'idcontrol manager-main\nidworker1 worker-a\nidworker2 worker-b\n' ;;
    'context ls --format {{.Name}}') printf 'default\ncontrol-remote\n' ;;
    *) printf 'Unexpected Docker call: %s\n' "$*" >&2; exit 99 ;;
esac
MOCK
chmod +x "$scratch/bin/docker"
export PATH=$scratch/bin:$PATH
before_flags=$-
before_pwd=$PWD
before_ifs=$IFS
source "$root/scripts/activate-swarm.sh"
[[ $before_flags == "$-" && $before_pwd == "$PWD" && $before_ifs == "$IFS" ]]
[[ $(complete -p swarm) == 'complete -F _kpl_swarm_complete swarm' ]]
[[ ! -s $KPL_COMPLETION_CALLS ]]
# Sourcing twice is harmless, and activation never rewrites sh/bash completion.
complete -F _kpl_existing_sh sh
source "$root/scripts/activate-swarm.sh"
[[ $(complete -p sh) == 'complete -F _kpl_existing_sh sh' ]]

# compopt requires Readline in production; record its requested behavior here.
compopt() { options+=("$*"); }
complete_words() {
    COMP_WORDS=("$@")
    COMP_CWORD=$((${#COMP_WORDS[@]} - 1))
    options=()
    _kpl_swarm_complete
}
has() {
    local candidate
    for candidate in "${COMPREPLY[@]}"; do [[ $candidate != "$1" ]] || return 0; done
    printf 'Missing completion %q in %s\n' "$1" "${COMPREPLY[*]}" >&2
    return 1
}
lacks() {
    local candidate
    for candidate in "${COMPREPLY[@]}"; do
        if [[ $candidate == "$1" ]]; then printf 'Unexpected completion %q\n' "$1" >&2; return 1; fi
    done
}
empty() { [[ ${#COMPREPLY[@]} == 0 ]]; }

complete_words swarm ''
for command in init configure config credentials nodes login publish check access logs scenario deploy status add-node remove-node remove help --help --env-file; do has "$command"; done
complete_words swarm lo
has logs; has login; lacks deploy
complete_words swarm --env-file
has --env-file
complete_words swarm --env-file custom.env lo
has logs; lacks --env-file
for command in help config credentials nodes login check access status remove; do complete_words swarm "$command" ''; empty; done
[[ ! -s $KPL_COMPLETION_CALLS ]]

complete_words swarm logs ''
for component in controller agent prometheus grafana access auth --tail; do has "$component"; done
lacks --context
complete_words swarm logs auth --
has --context; has --tail
complete_words swarm logs controller --
has --tail; lacks --context
complete_words swarm logs access --tail ''
has 100; has 500; has all
complete_words swarm logs --tail a
has all
complete_words swarm logs access --tail 500 --
has --context; lacks --tail
complete_words swarm logs auth --context co
has control-remote; lacks default
complete_words swarm logs auth --context control-remote --tail all ''
empty
complete_words swarm logs controller --context ''
empty
complete_words swarm logs unknown ''
empty
complete_words swarm publish --p
has --platforms
complete_words swarm publish --platforms linux/a
has linux/amd64; has linux/arm64
complete_words swarm publish --platforms linux/amd64,linux/ar
has linux/amd64,linux/arm64
complete_words swarm publish --platforms linux/amd64 ''
empty

# Node/selector completion respects mutually exclusive selector syntax and
# suppresses nodes already present by ID or hostname.
complete_words swarm deploy --
has --workers; has --all; has --all-excluding-self
complete_words swarm deploy worker
has worker-a; has worker-b
complete_words swarm add-node idw
has idworker1; has idworker2
complete_words swarm add-node worker-a
has worker-a
complete_words swarm remove-node worker-a ''
has worker-b; lacks worker-a; lacks idworker1; lacks --workers
complete_words swarm --env-file custom.env deploy idworker1 ''
has worker-b; lacks worker-a; lacks idworker1
complete_words swarm deploy --workers ''
empty

# Literal keys match the config helper. No current config/password values are
# read or proposed, including values containing shell metacharacters.
complete_words swarm configure ''
[[ ${options[*]} == '-o nospace' ]]
printf '%s\n' "${COMPREPLY[@]%=}" | sort > "$scratch/actual-keys"
(source "$root/scripts/swarm-config.sh"; printf '%s\n' $swarm_config_keys) | sort > "$scratch/expected-keys"
cmp "$scratch/expected-keys" "$scratch/actual-keys"
complete_words swarm init KPL_IM
has KPL_IMAGE=; has KPL_IMAGE_PULL_TIMEOUT=
complete_words swarm configure KPL_CONTROL_NODE_ID = idw
has idworker1; has idworker2; lacks worker-a
complete_words swarm configure KPL_CONTROL_NODE_ID =
has idcontrol
complete_words swarm configure KPL_CONTROL_NODE_ID=idw
has KPL_CONTROL_NODE_ID=idworker1; lacks KPL_CONTROL_NODE_ID=worker-a
for password in KPL_PASSWORD GRAFANA_ADMIN_PASSWORD; do
    complete_words swarm configure "$password" = ''; empty
    complete_words swarm configure "$password=private"; empty
done
complete_words swarm configure 'KPL_IMAGE=$(touch should-not-exist)'
empty
[[ ! -e should-not-exist ]]

# File candidates retain spaces and ask Readline to quote them as filenames.
printf 'name: completion fixture\n' > "$scratch/files/scenario with spaces.yaml"
printf 'not-evaluated\n' > "$scratch/files/.env.swarm"
complete_words swarm scenario "$scratch/files/scenario"
has "$scratch/files/scenario with spaces.yaml"
[[ ${options[*]} == '-o filenames' ]]
complete_words swarm --env-file "$scratch/files/.env"
has "$scratch/files/.env.swarm"
complete_words swarm scenario "$scratch/files/directory"
has "$scratch/files/directory with spaces"
complete_words swarm scenario one.yaml ''
empty

# The shortcut keeps the selected checkout while forwarding quoted arguments.
(cd "$scratch"; swarm --env-file missing.env scenario "$scratch/files/scenario with spaces.yaml") > "$scratch/scenario-output"
cmp "$scratch/files/scenario with spaces.yaml" "$scratch/scenario-output"
# Activation also works when its own checkout directory contains spaces.
mkdir "$scratch/checkout with spaces"
cp "$root/scripts/activate-swarm.sh" "$root/scripts/swarm-completion.bash" "$scratch/checkout with spaces/"
(source "$scratch/checkout with spaces/activate-swarm.sh"; [[ $_KPL_SWARM_SCRIPT == "$scratch/checkout with spaces/swarm.sh" ]])

# Cache results in the interactive shell, and invalidate when context changes.
unset _KPL_SWARM_CACHE_KEY
: > "$KPL_COMPLETION_CALLS"
complete_words swarm deploy worker
complete_words swarm deploy work
[[ $(wc -l < "$KPL_COMPLETION_CALLS") == 1 ]]
export DOCKER_CONTEXT=another-manager
complete_words swarm deploy worker
[[ $(wc -l < "$KPL_COMPLETION_CALLS") == 2 ]]
grep -q '^another-manager|node ls ' "$KPL_COMPLETION_CALLS"
unset _KPL_SWARM_CACHE_KEY
export KPL_COMPLETION_FAIL=1
complete_words swarm deploy ''
has --workers; lacks worker-a
complete_words swarm logs auth --context ''
empty
unset KPL_COMPLETION_FAIL _KPL_SWARM_CACHE_KEY
export KPL_COMPLETION_STALL=1
started=$SECONDS
complete_words swarm deploy ''
(( SECONDS - started <= 3 ))
has --workers; lacks worker-a
unset KPL_COMPLETION_STALL

# Executing the activation script cannot enable completion in its parent shell.
if bash "$root/scripts/activate-swarm.sh" > "$scratch/usage" 2>&1; then exit 1; fi
grep -q 'source' "$scratch/usage"
if sh "$root/scripts/activate-swarm.sh" > "$scratch/usage" 2>&1; then exit 1; fi
grep -q 'Use Bash' "$scratch/usage"
printf '%s\n' 'PASS: Swarm Bash activation, command/option/key/path completion, bounded read-only Docker lookup, caching and quoted argument forwarding.'

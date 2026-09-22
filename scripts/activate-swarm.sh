#!/usr/bin/env bash
# Source this file in Bash; it only changes the current shell.
if [ -z "${BASH_VERSION:-}" ]; then
    printf 'KPL Swarm: Use Bash and run: source scripts/activate-swarm.sh\n' >&2
    return 1 2>/dev/null || exit 1
fi
if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
    printf 'KPL Swarm: Run: source %q\n' "${BASH_SOURCE[0]}" >&2
    exit 1
fi

_kpl_swarm_activate() {
    local script_dir
    script_dir=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P) || return 1
    # Keep the checkout selected at activation, even after the user changes cwd.
    source "$script_dir/swarm-completion.bash" || return 1
    _KPL_SWARM_SCRIPT=$script_dir/swarm.sh
    swarm() { command sh "$_KPL_SWARM_SCRIPT" "$@"; }
    complete -F _kpl_swarm_complete swarm
}
_kpl_swarm_activate

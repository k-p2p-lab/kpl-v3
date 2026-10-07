#!/bin/sh
# Exercise gateway retirement using an in-process fake Docker only.
set -efu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
. "$root/scripts/swarm-network-gateway.sh"
fail() { printf '%s\n' "$*" >&2; exit 1; }
export KPL_STACK_NAME=lab KPL_CONTROL_NODE_ID=control1 KPL_PEER_NETWORK=lab-peers
agent_present=yes
agent_timeout=240
capture_tasks() {
    [ "${fault:-}" != history ] || return 1
    printf '%s-task\n' "$1"
}
wait_tasks() {
    [ "$1" = 'lab_network-manager-task
lab_agent-task' ] && [ "$2" = 240 ] || fail 'Wrong shutdown acknowledgment set'
    [ "${fault:-}" != shutdown ] || fail 'Unclean task shutdown'
    printf 'wait\n' >> "$scratch/events"
}
dock() {
    printf '%s\n' "$*" >> "$scratch/calls"
    case "$1 $2" in
        'service update') : ;;
        'info --format') if [ "${fault:-}" = remote ]; then echo manager2; else echo control1; fi ;;
        'service ls') if [ "${fault:-}" = services ]; then echo remaining-service; fi ;;
        'ps --all')
            case "$*" in
                *io.kpl.managed*) if [ "${fault:-}" = peers ]; then echo remaining-peer; fi ;;
                *io.kpl.network-gateway*) echo gatewayid ;;
                *) fail 'Unexpected container query' ;;
            esac ;;
        'inspect --type')
            [ "$6" = gatewayid ] || fail 'Wrong gateway ID'
            case "${fault:-}" in
                owner) echo 'lab|other|' ;;
                peer-label) echo 'lab|lab|true' ;;
                missing-value) echo 'lab|lab|<no value>' ;;
                *) echo 'lab|lab|' ;;
            esac ;;
        'rm --force')
            [ "$#" = 3 ] && [ "$3" = gatewayid ] || fail 'Wrong container removal'
            printf 'remove\n' >> "$scratch/events" ;;
        *) fail 'Unexpected Docker operation' ;;
    esac
}
for fault in none missing-value remote history shutdown services peers owner peer-label; do
    : > "$scratch/calls"
    : > "$scratch/events"
    if (swarm_prepare_gateway_removal && swarm_remove_network_gateway) > "$scratch/output" 2>&1; then
        case "$fault" in none|missing-value) ;; *) fail "Unexpected cleanup: $fault" ;; esac
        printf 'wait\nwait\nremove\n' > "$scratch/expected"
        cmp "$scratch/expected" "$scratch/events"
    else
        case "$fault" in none|missing-value) cat "$scratch/output" >&2; exit 1 ;; esac
        if grep -q '^rm ' "$scratch/calls"; then fail "Removed gateway despite $fault"; fi
    fi
done
printf '%s\n' 'PASS: gateway removal waits for clean shutdown and checks local ownership; uncertain cleanup preserves it.'

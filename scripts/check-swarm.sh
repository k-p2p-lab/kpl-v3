#!/bin/sh
# Read-only deployment checks on a Swarm manager. No registry login or pull.
set -efu

fail() { printf 'Swarm preflight: %s\n' "$*" >&2; exit 1; }
root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
. "$root/scripts/swarm-config.sh"
command -v docker >/dev/null 2>&1 || fail 'Docker CLI is required.'
command -v awk >/dev/null 2>&1 || fail 'awk is required.'
: "${KPL_CONTROL_NODE_ID:?Set KPL_CONTROL_NODE_ID to the exact control node ID}"
: "${KPL_IMAGE:?Set KPL_IMAGE to a registry image with an explicit tag or sha256 digest}"
: "${KPL_USER:?Set KPL_USER}" "${KPL_PASSWORD:?Set KPL_PASSWORD}"
swarm_validate_setting KPL_USER "$KPL_USER"
swarm_validate_setting KPL_PASSWORD "$KPL_PASSWORD"
peer_network=${KPL_PEER_NETWORK:-kpl-swarm-peers}
capacity=${KPL_AGENT_CAPACITY:-20}
stack_name=${KPL_STACK_NAME:-kpl}
minimum_agents=${KPL_MIN_AGENTS:-2}
image_pull_timeout=${KPL_IMAGE_PULL_TIMEOUT:-300}
agent_metrics_port=${KPL_AGENT_METRICS_PORT:-9091}
swarm_validate_setting KPL_PEER_SUBNET "${KPL_PEER_SUBNET:-}"
swarm_validate_setting KPL_IMAGE_BUILD_TIMEOUT "${KPL_IMAGE_BUILD_TIMEOUT:-1800}"
swarm_validate_setting KPL_IMAGE_PUSH_TIMEOUT "${KPL_IMAGE_PUSH_TIMEOUT:-600}"
swarm_validate_port_conflicts

case "$image_pull_timeout" in
    ''|0*|*[!0-9]*) fail 'KPL_IMAGE_PULL_TIMEOUT must be a positive decimal integer without leading zeros.' ;;
esac
if ! [ "$image_pull_timeout" -gt 0 ] 2>/dev/null; then fail 'KPL_IMAGE_PULL_TIMEOUT exceeds the supported integer range.'; fi

case "$stack_name" in
    ''|[!a-z0-9]*|*[!a-z0-9_-]*) fail 'KPL_STACK_NAME must start with a lowercase letter or digit and contain only lowercase letters, digits, underscores or hyphens.' ;;
esac
agent_label="kpl.$stack_name.agent"
case "$minimum_agents" in
    ''|0*|*[!0-9]*) fail 'KPL_MIN_AGENTS must be a positive decimal integer without leading zeros.' ;;
esac
if ! [ "$minimum_agents" -gt 0 ] 2>/dev/null; then fail 'KPL_MIN_AGENTS exceeds the supported integer range.'; fi

case "$KPL_CONTROL_NODE_ID" in
    -*|*[!a-zA-Z0-9]*) fail 'KPL_CONTROL_NODE_ID must be the exact Swarm node ID.' ;;
esac
case "$peer_network" in
    -*|*[!a-zA-Z0-9_.-]*) fail 'KPL_PEER_NETWORK must be a Docker network name.' ;;
esac
case "$capacity" in
    ''|0*|*[!0-9]*) fail 'KPL_AGENT_CAPACITY must be a positive decimal integer without leading zeros.' ;;
esac
if ! [ "$capacity" -gt 0 ] 2>/dev/null; then fail 'KPL_AGENT_CAPACITY exceeds the supported integer range.'; fi

image_name=$KPL_IMAGE
image_digest=''
case "$KPL_IMAGE" in
    *@sha256:*)
        image_name=${KPL_IMAGE%@sha256:*}
        image_digest=${KPL_IMAGE##*@sha256:}
        case "$image_digest" in ''|*[!a-f0-9]*) fail 'KPL_IMAGE has an invalid sha256 digest.' ;; esac
        [ "${#image_digest}" -eq 64 ] || fail 'KPL_IMAGE sha256 digest must contain 64 hex digits.'
        ;;
    *@*) fail 'KPL_IMAGE digest must use sha256:<64 lowercase hex digits>.' ;;
esac
repository=$image_name
# A registry port belongs to an earlier path component, not to the image tag.
case "${image_name##*/}" in
    *:*)
        image_tag=${image_name##*:}
        repository=${image_name%:*}
        case "$image_tag" in
            ''|[!a-zA-Z0-9_]*|*[!a-zA-Z0-9_.-]*) fail 'KPL_IMAGE has an invalid tag.' ;;
        esac
        [ "${#image_tag}" -le 128 ] || fail 'KPL_IMAGE tag cannot exceed 128 characters.'
        ;;
    *) [ -n "$image_digest" ] || fail 'KPL_IMAGE requires an explicit tag, such as registry/repository:v3, or a sha256 digest.' ;;
esac
case "$repository" in
    ''|-*|/*|*/|*//*|*://*|*[!a-zA-Z0-9._:/-]*) fail 'KPL_IMAGE has an invalid repository reference.' ;;
esac

case "${1:-}" in
    --config-only) [ "$#" -eq 1 ] || fail 'Unexpected arguments.'; exit 0 ;;
    '') [ "$#" -eq 0 ] || fail 'Unexpected arguments.' ;;
    *) fail 'Usage: check-swarm.sh [--config-only]' ;;
esac

swarm=$(docker info --format '{{.Swarm.LocalNodeState}} {{.Swarm.ControlAvailable}}')
[ "$swarm" = 'active true' ] || fail 'Run this check against an active Swarm manager.'

# Inspect only the required fields; avoid dumping task/container inventories.
control=$(docker node inspect --format '{{.ID}} {{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}}' "$KPL_CONTROL_NODE_ID")
[ "$control" = "$KPL_CONTROL_NODE_ID linux ready active" ] || fail 'The exact control node must be Linux, Ready and Active.'
network=$(docker network inspect --format '{{.Name}} {{.Driver}} {{.Attachable}}' "$peer_network")
[ "$network" = "$peer_network overlay true" ] || fail 'The named Peer network must be an attachable overlay.'
if [ -n "${KPL_PEER_SUBNET:-}" ]; then
    network_subnets=$(docker network inspect --format '{{range .IPAM.Config}}{{println .Subnet}}{{end}}' "$peer_network") || fail 'Cannot inspect Peer network subnets.'
    [ "$network_subnets" = "$KPL_PEER_SUBNET" ] || fail 'The existing Peer network subnet differs from KPL_PEER_SUBNET.'
fi

agent_ids=$(docker node ls --quiet --filter "node.label=$agent_label=true")
[ -n "$agent_ids" ] || fail "At least $minimum_agents $agent_label=true Linux, Ready, Active nodes are required."
# Word splitting is intentional for Docker-generated IDs; globbing is disabled.
set -- $agent_ids
for id do
    case "$id" in ''|*[!a-zA-Z0-9]*) fail 'Docker returned an invalid node ID.' ;; esac
done
agents=$(docker node inspect --format '{{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}}' "$@")
eligible=$(printf '%s\n' "$agents" | awk '$1 == "linux" && $2 == "ready" && $3 == "active" { n++ } END { print n+0 }')
[ "$eligible" -ge "$minimum_agents" ] || fail "At least $minimum_agents $agent_label=true Linux, Ready, Active nodes are required."

printf 'PASS: Swarm manager, pinned control node, attachable overlay, %s eligible Agents for stack %s, image reference, capacity %s and Agent metrics port %s.\n' "$eligible" "$stack_name" "$capacity" "$agent_metrics_port"
printf '%s\n' 'This checks deployment settings only; it does not validate registry access, physical-node kernels, VXLAN connectivity, host-port availability, firewall reachability or capacity under load.'

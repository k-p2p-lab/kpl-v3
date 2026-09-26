#!/bin/sh
# Swarm expands the task/service/node templates in stack.swarm.yaml.
set -eu

fail() { printf 'Swarm Agent: %s\n' "$*" >&2; exit 1; }
: "${KPL_SWARM_TASK_NAME:?Swarm task name is required}"
: "${KPL_SWARM_SERVICE_NAME:?Swarm service name is required}"
: "${KPL_SWARM_NODE_ID:?Swarm node ID is required}"
: "${KPL_PEER_NETWORK:?Peer overlay network is required}"

# Reject configuration errors before retrying Docker discovery.
metrics_port=${KPL_AGENT_METRICS_PORT:-9091}
case "$metrics_port" in
    ''|0*|*[!0-9]*) fail 'KPL_AGENT_METRICS_PORT must be a port between 1 and 65535' ;;
esac
[ "$metrics_port" -le 65535 ] 2>/dev/null || fail 'KPL_AGENT_METRICS_PORT must be a port between 1 and 65535'

# A task can start while its daemon/overlay metadata is still becoming ready.
# Bound each Docker call and retry discovery without recreating the task. Never
# fall back to the service VIP: node-specific operations must reach this Agent.
docker_read() { timeout -k 1 5 docker "$@"; }
discover_task() {
    startup_stage='task Peer overlay address'
    addresses=$(docker_read inspect --type container --format '{{range $name, $net := .NetworkSettings.Networks}}{{printf "%s %s\n" $name $net.IPAddress}}{{end}}' "$KPL_SWARM_TASK_NAME") || return 1
    address=$(printf '%s\n' "$addresses" | while read -r network ip; do
        if [ "$network" = "$KPL_PEER_NETWORK" ]; then printf '%s\n' "$ip"; fi
    done)
    [ -n "$address" ] || return 1
    case "$address" in
        *[!0-9.]*) fail "No unambiguous IPv4 address on $KPL_PEER_NETWORK" ;;
    esac

    startup_stage='Swarm node address'
    node_address=$(docker_read info --format '{{.Swarm.NodeAddr}}') || return 1
    [ -n "$node_address" ] || return 1
    case "$node_address" in
        *[!0-9a-fA-F:.]*) fail 'Docker returned an invalid Swarm node address' ;;
    esac

    # The task has already pulled this image. Pin Peers to its local content ID.
    startup_stage='running task image'
    image=$(docker_read inspect --type container --format '{{.Image}}' "$KPL_SWARM_TASK_NAME") || return 1
    [ -n "$image" ] || return 1
    case "$image" in sha256:*) ;; *) fail 'Cannot resolve the running service image' ;; esac
}
# Stop discovery promptly when Swarm cancels a starting task.
trap 'exit 143' TERM
trap 'exit 130' INT
startup_attempt=1
until discover_task; do
    [ "$startup_attempt" -lt 5 ] || fail "Cannot resolve $startup_stage after $startup_attempt attempts"
    printf 'Swarm Agent: Waiting for %s (attempt %s/5); retrying in 2s.\n' "$startup_stage" "$startup_attempt" >&2
    startup_attempt=$((startup_attempt + 1))
    sleep 2
done

# Prometheus uses the host-mode published metrics port; control stays on overlay.
metrics_host=$node_address
case "$metrics_host" in *:*) metrics_host=[$metrics_host] ;; esac

exec kpl agent \
    --id "$KPL_SWARM_SERVICE_NAME-$KPL_SWARM_NODE_ID" \
    --name "${KPL_SWARM_NODE_HOSTNAME:-$KPL_SWARM_NODE_ID}" \
    --labels "swarmNodeId=$KPL_SWARM_NODE_ID" \
    --listen :8090 --advertise-url "http://$address:8090" --self-url "http://$address:8090" \
    --metrics-listen :9091 --metrics-url "http://$metrics_host:$metrics_port/metrics" \
    --controller-url "${KPL_CONTROLLER_URL:-http://controller:8080}" \
    --capacity "${KPL_AGENT_CAPACITY:-20}" --data-dir /var/lib/kpl/agent \
    --docker-image "$image" --docker-network "$KPL_PEER_NETWORK"

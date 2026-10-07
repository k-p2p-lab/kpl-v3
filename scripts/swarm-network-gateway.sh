#!/bin/sh
# Sourced by swarm.sh. Service removal remains failure tolerant; a standalone
# gateway must survive until Agent Peer cleanup and manager shutdown are proven.

swarm_capture_gateway_tasks() {
    [ "${agent_present:-no}" = yes ] || fail 'Agent task history is required before gateway cleanup.'
    [ -n "${KPL_CONTROL_NODE_ID:-}" ] || fail 'Gateway cleanup requires the configured control node.'
    [ "$(dock info --format '{{.Swarm.NodeID}}')" = "$KPL_CONTROL_NODE_ID" ] || fail 'Run remove against the control node Docker daemon to clean its gateway.'
    capture_tasks "${KPL_STACK_NAME}_network-manager" || return 1
    capture_tasks "${KPL_STACK_NAME}_agent" || return 1
}

swarm_prepare_gateway_removal() {
    gateway_tasks=$(swarm_capture_gateway_tasks) || return 1
    # Service deletion can immediately erase terminal task history. Preserve
    # the service definitions until clean exits are observed, using impossible
    # placement instead of scaling down (which also deletes task history).
    for gateway_service in network-manager agent; do
        dock service update --detach=true --no-resolve-image \
            --constraint-add node.role==manager --constraint-add node.role==worker \
            "${KPL_STACK_NAME}_$gateway_service" || return 1
    done
    wait_tasks "$gateway_tasks" "$agent_timeout"
    # Include replacements racing with the placement change.
    gateway_tasks=$(swarm_capture_gateway_tasks) || return 1
    wait_tasks "$gateway_tasks" "$agent_timeout"
}

swarm_remove_network_gateway() {
    gateway_services=$(dock service ls --quiet --filter "label=com.docker.stack.namespace=$KPL_STACK_NAME") || return 1
    [ -z "$gateway_services" ] || fail 'Stack services still exist; gateway retained.'
    gateway_peers=$(dock ps --all --quiet --filter label=io.kpl.managed=true --filter "label=io.kpl.network=$KPL_PEER_NETWORK") || return 1
    [ -z "$gateway_peers" ] || fail 'Peer containers remain on the control node; gateway retained.'
    gateway_ids=$(dock ps --all --quiet --no-trunc --filter "label=io.kpl.network-gateway=$KPL_STACK_NAME") || return 1
    for gateway_id in $gateway_ids; do
        case "$gateway_id" in ''|*[!a-zA-Z0-9]*) fail 'Invalid gateway container ID.' ;; esac
        gateway_owner=$(dock inspect --type container --format '{{index .Config.Labels "io.kpl.network-gateway"}}|{{index .Config.Labels "com.docker.stack.namespace"}}|{{index .Config.Labels "io.kpl.managed"}}' "$gateway_id") || return 1
        case "$gateway_owner" in "$KPL_STACK_NAME|$KPL_STACK_NAME|"|"$KPL_STACK_NAME|$KPL_STACK_NAME|<no value>") ;; *) fail 'Gateway ownership mismatch; container retained.' ;; esac
        dock rm --force "$gateway_id" || return 1
    done
}

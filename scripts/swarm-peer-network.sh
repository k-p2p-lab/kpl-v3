#!/bin/sh
# Sourced by swarm.sh after literal config validation and the manager check.
# This maintenance operation never stops services or disconnects containers.

swarm_peer_network_unused() {
    reset_services=$(dock service ls --quiet --filter "label=com.docker.stack.namespace=$KPL_STACK_NAME") || fail 'Cannot verify that stack services are removed.'
    [ -z "$reset_services" ] || fail 'Peer network reset requires all stack services to be removed first; no services were stopped.'
    reset_nodes=$(dock node ls --quiet) || fail 'Cannot list Swarm nodes before Peer network reset.'
    [ -n "$reset_nodes" ] || fail 'Cannot verify an empty Swarm node list.'
    # Require a usable cluster view; an offline node may retain standalone Peers.
    for reset_node in $reset_nodes; do
        case "$reset_node" in ''|*[!a-zA-Z0-9]*) fail 'Docker returned an invalid Swarm node ID.' ;; esac
    done
    # Docker-generated IDs, with globbing disabled by swarm.sh.
    reset_states=$(dock node inspect --format '{{.Status.State}}' $reset_nodes) || fail 'Cannot verify Swarm node readiness.'
    reset_count=0
    for reset_state in $reset_states; do
        [ "$reset_state" = ready ] || fail 'Every Swarm node must be Ready before resetting the Peer network; inspect unavailable nodes for leftover Peers.'
        reset_count=$((reset_count + 1))
    done
    set -- $reset_nodes
    [ "$reset_count" -eq "$#" ] || fail 'Docker returned incomplete Swarm node readiness information.'
}

swarm_reset_peer_network() {
    swarm_peer_network_unused
    # Capture the immutable ID. Never delete a replacement which acquired the
    # same name while this command was checking the original network.
    reset_id=$(dock network inspect --format '{{.Id}}' "$KPL_PEER_NETWORK") || fail 'Cannot inspect the Peer network; it must exist before reset.'
    case "$reset_id" in ''|*[!a-zA-Z0-9]*) fail 'Docker returned an invalid or ambiguous Peer network ID.' ;; esac
    reset_format='{{.Name}}|{{.Driver}}|{{.Scope}}|{{.Attachable}}|{{.Internal}}|{{.Ingress}}|{{.ConfigOnly}}|{{.EnableIPv6}}|{{.ConfigFrom.Network}}|{{.IPAM.Driver}}|{{len .IPAM.Options}}|{{len .IPAM.Config}}|{{len .Containers}}|{{index .Labels "io.kpl.application"}}|{{index .Labels "io.kpl.stack"}}|{{len .Labels}}|{{range $key, $value := .Options}}{{if ne $key "com.docker.network.driver.overlay.vxlanid_list"}}custom{{end}}{{end}}'
    reset_expected="$KPL_PEER_NETWORK|overlay|swarm|true|false|false|false|false||default|0|1|0|$application|$KPL_STACK_NAME|2|"
    reset_metadata=$(dock network inspect --format "$reset_format" "$reset_id") || fail 'Cannot inspect Peer network ownership and settings.'
    [ "$reset_metadata" = "$reset_expected" ] || fail 'Peer network reset requires an empty helper-owned IPv4 overlay with default IPAM, only the two KPL ownership labels, and no custom options.'
    # JSON preserves empty address values across Docker CLI versions which use
    # either strings or netip types. AuxAddress is the CLI's typed Go field.
    reset_ipam_format='{{range .IPAM.Config}}{{json .Subnet}}|{{json .Gateway}}|{{json .IPRange}}|{{len .AuxAddress}}{{end}}'
    reset_ipam=$(dock network inspect --format "$reset_ipam_format" "$reset_id") || fail 'Cannot inspect Peer network IPv4 allocation.'
    reset_subnet=${reset_ipam%%|*}
    reset_rest=${reset_ipam#*|}
    reset_gateway=${reset_rest%%|*}
    reset_subnet=${reset_subnet#\"}; reset_subnet=${reset_subnet%\"}
    reset_gateway=${reset_gateway#\"}; reset_gateway=${reset_gateway%\"}
    [ "$reset_ipam" = "\"$reset_subnet\"|\"$reset_gateway\"|\"\"|0" ] || fail 'Peer network reset does not support IP ranges or auxiliary addresses.'
    case "$reset_subnet" in ''|*[!0-9./]*) fail 'Peer network has no valid IPv4 subnet to preserve.' ;; esac
    swarm_validate_setting KPL_PEER_SUBNET "$reset_subnet"
    if [ -n "$KPL_PEER_SUBNET" ] && [ "$KPL_PEER_SUBNET" != "$reset_subnet" ]; then
        fail 'Existing Peer subnet differs from KPL_PEER_SUBNET; reset does not change the subnet.'
    fi
    if [ -n "$reset_gateway" ]; then
        case "$reset_gateway" in *[!0-9.]*) fail 'Docker returned an invalid IPv4 gateway.' ;; esac
        printf '%s\n' "$reset_gateway" | awk -F. -v subnet="$reset_subnet" '
            NF != 4 { exit 1 }
            {
                gateway=0
                for (i=1; i<=NF; i++) {
                    if ($i !~ /^[0-9]+$/ || $i > 255 || (length($i)>1 && substr($i,1,1)=="0")) exit 1
                    gateway=gateway*256+$i
                }
                split(subnet,cidr,"/"); split(cidr[1],octet,"."); network=0
                for (i=1; i<=4; i++) network=network*256+octet[i]
                if (gateway<=network || gateway>=network+2^(32-cidr[2])-1) exit 1
            }
        ' || fail 'Docker returned an invalid IPv4 gateway.'
    fi
    set -- network create --driver overlay --attachable \
        --label "io.kpl.application=$application" --label "io.kpl.stack=$KPL_STACK_NAME" --subnet "$reset_subnet"
    if [ -n "$reset_gateway" ]; then set -- "$@" --gateway "$reset_gateway"; fi
    set -- "$@" "$KPL_PEER_NETWORK"
    # Recheck immediately before the mutation. Docker's remove API also rejects
    # networks referenced by services or active attachment tasks on other nodes.
    swarm_peer_network_unused
    reset_current=$(dock network inspect --format "$reset_format" "$reset_id") || fail 'Peer network disappeared before reset; no replacement was created.'
    [ "$reset_current" = "$reset_expected" ] || fail 'Peer network changed or acquired containers before reset; no changes made.'
    printf 'Recreating Peer network %s (%s), preserving subnet %s and gateway %s.\n' "$KPL_PEER_NETWORK" "$reset_id" "$reset_subnet" "${reset_gateway:-automatic}"
    # Print the exact reconstruction arguments before deletion so an interrupted
    # or ambiguous daemon response does not lose the original allocation settings.
    printf 'Recovery command (only if the network is absent): docker'
    printf ' %s' "$@"
    printf '\n'
    dock network rm "$reset_id" || fail 'Peer network removal failed or is unconfirmed; inspect Docker state before retrying. No replacement was requested.'
    reset_new_id=$(dock "$@") || fail 'Peer network was removed, but recreation failed or is unconfirmed. Inspect Docker state and use the recovery command only if absent.'
    case "$reset_new_id" in ''|*[!a-zA-Z0-9]*) fail 'Peer network recreation returned an invalid ID; inspect Docker state before deploying.' ;; esac
    [ "$reset_new_id" != "$reset_id" ] || fail 'Docker returned the old Peer network ID; reset is unverified.'
    reset_current=$(dock network inspect --format "$reset_format" "$reset_new_id") || fail 'Cannot verify the recreated Peer network; inspect Docker state before deploying.'
    reset_current_ipam=$(dock network inspect --format "$reset_ipam_format" "$reset_new_id") || fail 'Cannot verify recreated Peer network IPAM.'
    [ "$reset_current" = "$reset_expected" ] && [ "$reset_current_ipam" = "$reset_ipam" ] || fail 'Recreated Peer network settings differ from the original; inspect Docker state before deploying.'
    printf 'Peer network recreated with new ID %s. Services, containers, placement labels, configuration and data volumes were not changed. Deploy separately, then verify cross-host connectivity.\n' "$reset_new_id"
}

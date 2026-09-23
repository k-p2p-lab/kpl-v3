#!/bin/sh
# Manager-side lifecycle commands. Never sources the credentials file.
set -efu

fail() { printf 'KPL Swarm: %s\n' "$*" >&2; exit 1; }
usage() {
    cat <<'EOF'
Usage: sh scripts/swarm.sh [--env-file PATH] COMMAND [NODE... | SELECTOR]
  init [KEY=VALUE...]   Create a private config with random credentials
  configure KEY=VALUE...  Update configuration while preserving credentials
  config               Show configuration with secrets redacted
  credentials          Show configured credentials explicitly
  nodes                List current Swarm nodes
  login                Log in to the image registry interactively
  publish [--platforms CSV]  Build and push the configured image tag
  check                Check the loaded deployment configuration and cluster
  access               Show control URLs and selected Agent metrics URLs
  logs [COMPONENT] [--tail N|all] [--context NAME]  Show service or web logs
  scenario [FILE]      Print a scenario (default: examples/swarm-smoke.yaml)
  deploy [NODE... | SELECTOR]  Deploy; bare deploy reuses existing Agent labels
  status               Show services, Agent tasks and selected nodes
  add-node NODE... | SELECTOR     Enable one Agent per selected Linux node
  remove-node NODE... | SELECTOR  Stop Agents and wait for their Peer cleanup
  remove               Delete stack services even when tasks or nodes are unhealthy
Environment overrides the config file. NODE is a Swarm node ID or hostname.
Init/configure accept literal KEY=VALUE arguments; quote values containing spaces.
Use one selector without NODE arguments:
  --all                 All nodes, including managers
  --all-excluding-self  All except the current Docker daemon's Swarm node
  --workers             Worker nodes only
Deploy/add selectors skip nodes that are not Linux, Ready and Active.
Remove selectors target only this stack's labeled Agents; unavailable targets fail.
Selection uses the current cluster snapshot, not nodes that join later.
KPL_IMAGE accepts an explicit tag or digest; deploy resolves tags without editing the file.
KPL_IMAGE_PULL_TIMEOUT sets the tag pull timeout in seconds (default: 300).
KPL_IMAGE_BUILD_TIMEOUT and KPL_IMAGE_PUSH_TIMEOUT default to 1800 and 600 seconds.
KPL_PEER_SUBNET optionally fixes the Peer network's IPv4 CIDR at creation.
Publish uses the repository root; --platforms requires a configured Buildx builder.
Bash completion: source scripts/activate-swarm.sh, then use swarm COMMAND.
Log components: controller (default), agent, prometheus, grafana, access, auth.
Logs default to the last 100 lines; --tail all prints all available lines.
Access/auth print JSONL from the running Controller (rotated backups excluded).
For access/auth, --context selects its node's Docker daemon for file reads only;
service/task discovery still uses the caller's manager Docker context.
Removal preserves experiment/monitoring volumes and the external Peer network.
Full remove does not wait for clean task exits or verify standalone Peer cleanup.
EOF
}

root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd -P)
env_file=$root/.env.swarm
explicit_env_file=no
if [ "${1:-}" = --env-file ]; then
    [ "$#" -ge 2 ] || fail '--env-file needs a path.'
    env_file=$2
    explicit_env_file=yes
    shift 2
fi
command_name=${1:-help}
[ "$#" -eq 0 ] || shift
. "$root/scripts/swarm-config.sh"
case "$command_name" in
    help|-h|--help) usage; exit 0 ;;
    init|configure|config|credentials) swarm_config_command "$command_name" "$@"; exit ;;
    deploy|status|add-node|remove-node|remove|nodes|login|publish|check|access|logs|scenario) ;;
    *) usage >&2; fail "Unknown command: $command_name" ;;
esac
case "$command_name" in
    status|remove|nodes|login|check|access) [ "$#" -eq 0 ] || fail "$command_name takes no arguments." ;;
    add-node|remove-node) [ "$#" -gt 0 ] || fail "$command_name requires at least one node or a selector." ;;
    scenario)
        [ "$#" -le 1 ] || fail 'Usage: scenario [FILE]'
        scenario_file=${1:-$root/examples/swarm-smoke.yaml}
        case "$scenario_file" in -*) fail 'Usage: scenario [FILE]' ;; esac
        [ -f "$scenario_file" ] || fail "Scenario file not found: $scenario_file"
        cat -- "$scenario_file"
        exit
        ;;
    logs)
        log_component=controller
        log_tail=100
        log_context=''
        if [ "$#" -gt 0 ]; then
            case "$1" in
                --*) ;;
                *) log_component=$1; shift ;;
            esac
        fi
        case "$log_component" in controller|agent|prometheus|grafana|access|auth) ;; *) fail 'Unknown log component.' ;; esac
        while [ "$#" -gt 0 ]; do
            case "$1" in
                --tail)
                    [ "$#" -ge 2 ] || fail '--tail needs a positive line count or all.'
                    log_tail=$2
                    case "$log_tail" in
                        all) ;;
                        ''|0*|*[!0-9]*) fail '--tail needs a positive integer without leading zeros, or all.' ;;
                        *) [ "$log_tail" -gt 0 ] 2>/dev/null || fail 'Log line count is out of range.' ;;
                    esac
                    shift 2 ;;
                --context)
                    [ "$#" -ge 2 ] || fail '--context needs a Docker context name.'
                    log_context=$2
                    case "$log_context" in ''|-*) fail 'Invalid Docker context name.' ;; esac
                    shift 2 ;;
                *) fail "Unknown logs option: $1" ;;
            esac
        done
        case "$log_component:$log_context" in
            access:*|auth:*|*:) ;;
            *) fail '--context is only supported for access/auth file reads.' ;;
        esac
        ;;
    publish)
        publish_platforms=''
        if [ "$#" -gt 0 ]; then
            [ "$#" -eq 2 ] && [ "$1" = --platforms ] || fail 'Usage: publish [--platforms linux/amd64,linux/arm64]'
            publish_platforms=$2
            case "$publish_platforms" in ''|,*|*,|*,,*|*[!a-z0-9_/,.-]*) fail 'Invalid platform list.' ;; esac
            # Accept explicit Linux OS/architecture[/variant] tuples only.
            printf '%s\n' "$publish_platforms" | awk -F, '
                { for (i=1; i<=NF; i++) {
                    n=split($i, part, "/")
                    if (n < 2 || n > 3 || part[1] != "linux" || part[2] !~ /^[a-z0-9][a-z0-9_]*$/ || (n == 3 && part[3] !~ /^[a-z0-9][a-z0-9_.-]*$/)) exit 1
                } }
            ' || fail 'Platforms must be Linux OS/architecture[/variant] tuples.'
        fi
        set --
        ;;
esac
# Reject ambiguous selectors and options before contacting Docker or loading config.
node_selector=''
explicit_nodes=no
for argument do
    case "$argument" in
        --all|--all-excluding-self|--workers)
            [ -z "$node_selector" ] && [ "$explicit_nodes" = no ] || fail 'Use exactly one selector without explicit nodes.'
            node_selector=$argument
            ;;
        -*) fail "Unknown option: $argument" ;;
        *)
            [ -z "$node_selector" ] || fail 'Use exactly one selector without explicit nodes.'
            case "$argument" in ''|*[!a-zA-Z0-9_.-]*) fail "Invalid node: $argument" ;; esac
            explicit_nodes=yes
            ;;
    esac
done
[ -z "$node_selector" ] || set --

swarm_load_config "$env_file" "$explicit_env_file"

KPL_STACK_NAME=${KPL_STACK_NAME:-kpl}
case "$KPL_STACK_NAME" in ''|[!a-z0-9]*|*[!a-z0-9_-]*) fail 'Invalid KPL_STACK_NAME; use lowercase letters, digits, hyphens or underscores.' ;; esac
KPL_PEER_NETWORK=${KPL_PEER_NETWORK:-$KPL_STACK_NAME-peers}
case "$KPL_PEER_NETWORK" in ''|-*|*[!a-zA-Z0-9_.-]*) fail 'Invalid KPL_PEER_NETWORK.' ;; esac
KPL_PEER_SUBNET=${KPL_PEER_SUBNET:-}
swarm_validate_setting KPL_PEER_SUBNET "$KPL_PEER_SUBNET"
KPL_MIN_AGENTS=${KPL_MIN_AGENTS:-1}
docker_timeout=${KPL_DOCKER_TIMEOUT:-60}
image_pull_timeout=${KPL_IMAGE_PULL_TIMEOUT:-300}
image_build_timeout=${KPL_IMAGE_BUILD_TIMEOUT:-1800}
image_push_timeout=${KPL_IMAGE_PUSH_TIMEOUT:-600}
agent_timeout=${KPL_AGENT_STOP_TIMEOUT:-240}
for number in "$docker_timeout" "$image_pull_timeout" "$image_build_timeout" "$image_push_timeout" "$agent_timeout" "$KPL_MIN_AGENTS"; do
    case "$number" in ''|0*|*[!0-9]*) fail 'Timeouts and KPL_MIN_AGENTS must be positive integers without leading zeros.' ;; esac
    [ "$number" -gt 0 ] 2>/dev/null || fail 'Integer is out of range.'
done
swarm_validate_port_conflicts
export KPL_STACK_NAME KPL_PEER_NETWORK KPL_PEER_SUBNET KPL_MIN_AGENTS
agent_label=kpl.$KPL_STACK_NAME.agent
application=kp2plab-v3
command -v docker >/dev/null 2>&1 || fail 'Docker CLI is required.'
command -v timeout >/dev/null 2>&1 || fail 'Linux timeout (coreutils) is required.'
command -v awk >/dev/null 2>&1 || fail 'awk is required.'
dock() { timeout -s TERM -k 5 "$docker_timeout" docker "$@"; }
check_publish_config_path() {
    case "$1" in
        "$root"/*)
            config_relative=${1#"$root"/}
            case "$config_relative" in
                .env) ignore_rule=.env ;;
                .env.*) ignore_rule='.env.*' ;;
                *) fail 'Config is inside the build context and is not protected by the supported .dockerignore rules. Move it outside the repository or use a root .env.* file.' ;;
            esac
            # Prove the expected exclusion still exists. Negated rules need a
            # full Docker pattern evaluator, so reject them conservatively.
            awk -v rule="$ignore_rule" '
                { sub(/\r$/, ""); sub(/^[ \t]+/, ""); sub(/[ \t]+$/, "") }
                $0 == rule { found=1 }
                /^!/ { negation=1 }
                END { if (!found || negation) exit 1 }
            ' "$root/.dockerignore" || fail 'Cannot verify that .dockerignore excludes the config; move the config outside the repository before publishing.'
            ;;
    esac
}
check_publish_context() {
    [ -f "$root/Dockerfile" ] || fail 'Dockerfile not found in the repository root.'
    if [ -e "$root/Dockerfile.dockerignore" ] || [ -L "$root/Dockerfile.dockerignore" ]; then
        fail 'Dockerfile.dockerignore overrides the verified config exclusions; remove that override before publishing with this command.'
    fi
    [ -f "$env_file" ] || return 0
    command -v readlink >/dev/null 2>&1 || fail 'readlink with -f support is required to verify the config build-context boundary.'
    config_parent=$(CDPATH='' cd -- "$(dirname -- "$env_file")" && pwd -P) || fail 'Cannot locate the config directory.'
    config_path=$config_parent/$(basename -- "$env_file")
    config_real_path=$(readlink -f -- "$env_file") || fail 'Cannot resolve the config file before publishing.'
    [ -n "$config_real_path" ] || fail 'Cannot resolve the config file before publishing.'
    # An external symlink may point at an unignored file inside the repository;
    # inspect both the configured directory entry and the final file target.
    check_publish_config_path "$config_path"
    check_publish_config_path "$config_real_path"
}
case "$command_name" in
    login|publish)
        : "${KPL_IMAGE:?Set KPL_IMAGE in .env.swarm or environment}"
        swarm_validate_setting KPL_IMAGE "$KPL_IMAGE"
        if [ "$command_name" = login ]; then
            registry=''
            case "$KPL_IMAGE" in
                */*)
                    image_first=${KPL_IMAGE%%/*}
                    case "$image_first" in
                        docker.io|index.docker.io|registry-1.docker.io) ;;
                        *.*|*:*|localhost) registry=$image_first ;;
                    esac
                    ;;
            esac
            # Let Docker handle the interactive prompt and credential store;
            # do not apply the short daemon-command timeout to user input.
            if [ -n "$registry" ]; then exec docker login "$registry"; else exec docker login; fi
        fi
        case "$KPL_IMAGE" in *@*) fail 'Publish requires a mutable image tag, not a digest.' ;; esac
        check_publish_context
        if [ -n "$publish_platforms" ]; then
            (
                unset DOCKER_DEFAULT_PLATFORM
                timeout -s TERM -k 5 "$image_build_timeout" docker buildx build --platform "$publish_platforms" --tag "$KPL_IMAGE" --push "$root"
            ) || fail 'Multi-platform build/push failed.'
        else
            (
                unset DOCKER_DEFAULT_PLATFORM
                timeout -s TERM -k 5 "$image_build_timeout" docker build --tag "$KPL_IMAGE" "$root"
            ) || fail 'Image build failed; push was not started.'
            (
                unset DOCKER_DEFAULT_PLATFORM
                timeout -s TERM -k 5 "$image_push_timeout" docker push "$KPL_IMAGE"
            ) || fail 'Image push failed.'
        fi
        printf 'Published %s. Deploy resolves this tag to its current registry digest.\n' "$KPL_IMAGE"
        exit 0
        ;;
esac

control_node_host() {
    control_host=$(dock node inspect --format '{{.Status.Addr}}' "$KPL_CONTROL_NODE_ID") || fail 'Cannot inspect the control node address.'
    case "$control_host" in ''|*[!0-9a-fA-F:.]*) fail 'Docker returned an invalid control node address.' ;; esac
    case "$control_host" in *:*) control_host=[$control_host] ;; esac
    printf '%s\n' "$control_host"
}

[ "$(dock info --format '{{.Swarm.LocalNodeState}} {{.Swarm.ControlAvailable}}')" = 'active true' ] || fail 'Run against an active Swarm manager.'
case "$command_name" in
    nodes) dock node ls; exit ;;
    check) timeout -s TERM -k 5 "$docker_timeout" sh "$root/scripts/check-swarm.sh"; exit ;;
    access)
        : "${KPL_CONTROL_NODE_ID:?Set KPL_CONTROL_NODE_ID}"
        swarm_validate_setting KPL_CONTROL_NODE_ID "$KPL_CONTROL_NODE_ID"
        http_port=${KPL_HTTP_PORT:-8080}
        agent_metrics_port=${KPL_AGENT_METRICS_PORT:-9091}
        prometheus_port=${PROMETHEUS_PORT:-9090}
        grafana_port=${GRAFANA_PORT:-3000}
        swarm_validate_setting KPL_HTTP_PORT "$http_port"
        swarm_validate_setting KPL_AGENT_METRICS_PORT "$agent_metrics_port"
        swarm_validate_setting PROMETHEUS_PORT "$prometheus_port"
        swarm_validate_setting GRAFANA_PORT "$grafana_port"
        access_host=$(control_node_host) || exit 1
        printf 'Controller: http://%s:%s\nPrometheus: http://%s:%s\nGrafana: http://%s:%s\n' "$access_host" "$http_port" "$access_host" "$prometheus_port" "$access_host" "$grafana_port"
        agent_nodes=$(dock node ls --quiet --filter "node.label=$agent_label=true") || fail 'Cannot list selected Agent nodes.'
        if [ -n "$agent_nodes" ]; then
            for agent_node in $agent_nodes; do
                case "$agent_node" in ''|*[!a-zA-Z0-9]*) fail 'Docker returned an invalid Agent node ID.' ;; esac
                agent_host=$(dock node inspect --format '{{.Status.Addr}}' "$agent_node") || fail "Cannot inspect Agent node $agent_node address."
                case "$agent_host" in ''|*[!0-9a-fA-F:.]*) fail "Docker returned an invalid address for Agent node $agent_node." ;; esac
                case "$agent_host" in *:*) agent_host=[$agent_host] ;; esac
                printf 'Agent metrics (%s): http://%s:%s/metrics\n' "$agent_node" "$agent_host" "$agent_metrics_port"
            done
        else
            printf '%s\n' 'Agent metrics: no nodes are selected for this stack.'
        fi
        printf '%s\n' 'These configured URLs use Swarm node addresses; reachability, firewall rules and service readiness are not checked.'
        exit 0
        ;;
esac

# Refuse to change an unrelated or unmarked stack with the same name.
services=$(dock service ls --quiet --filter "label=com.docker.stack.namespace=$KPL_STACK_NAME")
controller_present=no
agent_present=no
for service in $services; do
    identity=$(dock service inspect --format '{{.Spec.Name}}|{{index .Spec.Labels "io.kpl.application"}}' "$service")
    case "$identity" in
        "$KPL_STACK_NAME"_controller\|"$application"|"$KPL_STACK_NAME"_agent\|"$application"|"$KPL_STACK_NAME"_prometheus\|"$application"|"$KPL_STACK_NAME"_grafana\|"$application") ;;
        *) fail 'Existing stack contains an unrecognized service. See https://github.com/k-p2p-lab/v3/wiki/Swarm-Deployment for migration; no changes made.' ;;
    esac
    case "$identity" in
        "${KPL_STACK_NAME}_controller|$application") controller_present=yes ;;
        "${KPL_STACK_NAME}_agent|$application") agent_present=yes ;;
    esac
done
service_exists() {
    case "$1" in
        "${KPL_STACK_NAME}_controller") [ "$controller_present" = yes ] ;;
        "${KPL_STACK_NAME}_agent") [ "$agent_present" = yes ] ;;
        *) return 1 ;;
    esac
}
resolve_nodes() {
    resolved_seen=' '
    for node do
        case "$node" in ''|-*|*[!a-zA-Z0-9_.-]*) fail "Invalid node: $node" ;; esac
        resolved_id=$(dock node inspect --format '{{.ID}}' "$node") || return 1
        case "$resolved_id" in ''|*[!a-zA-Z0-9]*) fail "Docker returned an invalid node ID for $node." ;; esac
        case "$resolved_seen" in *" $resolved_id "*) continue ;; esac
        resolved_seen="$resolved_seen$resolved_id "
        printf '%s\n' "$resolved_id"
    done
}
ready_node() {
    state=$(dock node inspect --format '{{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}}' "$1")
    [ "$state" = 'linux ready active' ] || fail "Node $1 must be Linux, Ready and Active; got $state."
}
selected_nodes() { dock node ls --quiet --filter "node.label=$agent_label=true"; }
command_nodes() {
    if [ -z "$node_selector" ]; then resolve_nodes "$@"; return; fi
    if [ "$command_name" = remove-node ]; then
        candidates=$(selected_nodes) || fail 'Cannot list Agent nodes for this stack; no changes made.'
    else
        candidates=$(dock node ls --quiet) || fail 'Cannot list Swarm nodes; no changes made.'
    fi
    selector_self=''
    if [ "$node_selector" = --all-excluding-self ]; then
        selector_self=$(dock info --format '{{.Swarm.NodeID}}') || fail 'Cannot identify the Swarm node of the current Docker daemon.'
        case "$selector_self" in ''|*[!a-zA-Z0-9]*) fail 'Docker returned an invalid current Swarm node ID.' ;; esac
    fi
    selection=''
    selection_seen=' '
    for candidate in $candidates; do
        case "$candidate" in ''|*[!a-zA-Z0-9]*) fail 'Docker returned an invalid node list.' ;; esac
        selection_record=$(dock node inspect --format '{{.ID}}|{{.Spec.Role}}|{{.Description.Platform.OS}}|{{.Status.State}}|{{.Spec.Availability}}' "$candidate") || fail "Cannot inspect node $candidate; no changes made."
        selection_id=${selection_record%%|*}; selection_rest=${selection_record#*|}
        selection_role=${selection_rest%%|*}; selection_rest=${selection_rest#*|}
        selection_os=${selection_rest%%|*}; selection_rest=${selection_rest#*|}
        selection_state=${selection_rest%%|*}; selection_availability=${selection_rest#*|}
        case "$selection_id" in ''|*[!a-zA-Z0-9]*) fail "Docker returned an invalid node ID for $candidate." ;; esac
        case "$selection_role" in manager|worker) ;; *) fail "Docker returned an invalid role for node $selection_id." ;; esac
        for field in "$selection_os" "$selection_state" "$selection_availability"; do
            case "$field" in ''|*[!a-zA-Z0-9_.-]*) fail "Docker returned invalid node metadata for $selection_id." ;; esac
        done
        # Require the complete five-field record even if missing delimiters would
        # otherwise leave plausible values in the shell parameter expansions.
        [ "$selection_record" = "$selection_id|$selection_role|$selection_os|$selection_state|$selection_availability" ] || fail "Docker returned incomplete node metadata for $selection_id."
        case "$selection_seen" in *" $selection_id "*) continue ;; esac
        selection_seen="$selection_seen$selection_id "
        [ "$selection_id" != "$selector_self" ] || continue
        if [ "$node_selector" = --workers ] && [ "$selection_role" != worker ]; then continue; fi
        # Removal must retain unavailable labeled nodes so the strict preflight
        # refuses cleanup that cannot be verified, before changing any placement.
        if [ "$command_name" != remove-node ] && [ "$selection_os $selection_state $selection_availability" != 'linux ready active' ]; then
            printf 'Skipping node %s: requires Linux, Ready and Active; got %s %s %s.\n' "$selection_id" "$selection_os" "$selection_state" "$selection_availability" >&2
            continue
        fi
        selection="$selection $selection_id"
    done
    [ -n "$selection" ] || fail "Selector $node_selector matched no eligible nodes."
    for selection_id in $selection; do printf '%s\n' "$selection_id"; done
}
report_nodes() {
    [ -n "$1" ] || return 0
    set -- $1
    printf 'Selected %s node(s): %s\n' "$#" "$*" >&2
}
resolve_deployment_image() {
    case "$KPL_IMAGE" in
        *@sha256:*) export KPL_IMAGE; return 0 ;;
    esac
    requested_image=$KPL_IMAGE
    printf 'Resolving image tag %s (pull timeout: %ss)...\n' "$requested_image" "$image_pull_timeout"
    # Pull through the selected daemon to retain its registry TLS/HTTP policy.
    # Use that daemon's native platform regardless of the caller's default;
    # Docker still reports the top-level manifest/index digest for this pull.
    if ! pull_output=$(
        unset DOCKER_DEFAULT_PLATFORM
        timeout -s TERM -k 5 "$image_pull_timeout" docker pull "$requested_image"
    ); then
        [ -z "$pull_output" ] || printf '%s\n' "$pull_output" >&2
        fail 'Image pull failed; no deployment changes made.'
    fi
    # Do not fall back to local RepoDigests: those can contain stale or multiple
    # references. A successful pull must identify exactly one registry digest.
    resolved_digest=$(printf '%s\n' "$pull_output" | awk '
        { sub(/\r$/, "") }
        /^Digest:/ {
            count++
            if (NF != 2 || $0 != "Digest: " $2 || $2 !~ /^sha256:[a-f0-9]+$/ || length($2) != 71) invalid=1
            digest=$2
        }
        END {
            if (count != 1 || invalid) exit 1
            print digest
        }
    ') || fail 'Cannot resolve exactly one sha256 digest from the successful pull; no deployment changes made.'
    # Static validation already required a tag on the final path component.
    KPL_IMAGE=${requested_image%:*}@$resolved_digest
    export KPL_IMAGE
    sh "$root/scripts/check-swarm.sh" --config-only
    printf 'Deploying resolved image %s\n' "$KPL_IMAGE"
}
placement_exclusions() {
    action=$1
    excluded_nodes=$2
    [ -n "$excluded_nodes" ] || return 0
    set -- service update --detach=true --no-resolve-image
    for node in $excluded_nodes; do
        set -- "$@" "--constraint-$action" "node.id!=$node"
    done
    dock "$@" "${KPL_STACK_NAME}_agent"
}

task_format='{{.NodeID}}|{{.Status.State}}|{{if .Status.ContainerStatus}}{{.Status.ContainerStatus.ContainerID}}|{{.Status.ContainerStatus.PID}}|{{.Status.ContainerStatus.ExitCode}}{{else}}-|0|-{{end}}|{{if .Status.Err}}error{{else}}ok{{end}}'
# Keep active tasks, the latest task per node, and every retained failure.
# Docker sorts each replicated slot / global node newest first. A later task
# canceled before process startup cannot prove that an earlier failure recovered.
capture_tasks() {
    task_service=$1
    task_node=${2:-}
    if [ -n "$task_node" ]; then
        task_ids=$(dock service ps --quiet --no-trunc --filter "node=$task_node" "$task_service") || return 1
    else
        task_ids=$(dock service ps --quiet --no-trunc "$task_service") || return 1
    fi
    [ -n "$task_ids" ] || fail "No task history for $task_service ${task_node:-}; cleanup cannot be verified automatically."
    seen=' '
    for task_id in $task_ids; do
        record=$(dock inspect --type task --format "$task_format" "$task_id") || return 1
        node=${record%%|*}; rest=${record#*|}; state=${rest%%|*}
        # Pausing a replicated service preserves its slot and shutdown history,
        # with a replacement that cannot be assigned. It has never run a process;
        # resuming the service can cancel that unassigned replacement as Shutdown.
        if [ -z "$node" ]; then
            case "$record" in
                '|new|-|0|-|'*|'|pending|-|0|-|'*|'|allocated|-|0|-|'*|'|shutdown|-|0|-|'*) continue ;;
            esac
            fail "Task $task_id has no verifiable node/container assignment."
        fi
        include=yes
        case "$state" in complete|shutdown)
            case "$record" in *'|0|0|ok')
                case "$seen" in *" $node "*) include=no ;; esac ;;
            esac ;;
        esac
        seen="$seen$node "
        if [ "$include" = yes ]; then
            # Readiness is needed to trust a remote container stop acknowledgment.
            node_state=$(dock node inspect --format '{{.Description.Platform.OS}} {{.Status.State}}' "$node") || return 1
            [ "$node_state" = 'linux ready' ] || fail "Task $task_id is on unavailable node $node; cleanup cannot be verified."
            printf '%s\n' "$task_id"
        fi
    done
}
# Bound each CLI call by the remaining stop budget, including completed tasks.
# A busy daemon must not add another full Docker timeout on every inspection.
wait_dock() {
    wait_remaining=$(( deadline - $(date +%s) ))
    [ "$wait_remaining" -gt 0 ] || fail 'Timed out verifying task cleanup; stack retained for inspection.'
    [ "$wait_remaining" -le "$docker_timeout" ] || wait_remaining=$docker_timeout
    timeout -s TERM -k 5 "$wait_remaining" docker "$@"
}
wait_tasks() {
    wait_ids=$1
    deadline=$(( $(date +%s) + $2 ))
    for task_id in $wait_ids; do
        while :; do
            record=$(wait_dock inspect --type task --format "$task_format" "$task_id") || fail "Cannot inspect task $task_id; cleanup is unverified."
            task_node=${record%%|*}; rest=${record#*|}
            state=${rest%%|*}; rest=${rest#*|}
            container=${rest%%|*}; rest=${rest#*|}
            pid=${rest%%|*}; rest=${rest#*|}
            exit_code=${rest%%|*}; task_error=${rest#*|}
            node_state=$(wait_dock node inspect --format '{{.Status.State}}' "$task_node")
            [ "$node_state" = ready ] || fail "Node $task_node became unavailable; task $task_id cleanup is unverified."
            case "$state" in
                complete|shutdown)
                    [ -n "$container" ] && [ "$container" != - ] && [ "$pid" = 0 ] && [ "$exit_code" = 0 ] && [ "$task_error" = ok ] || fail "Task $task_id stopped without a clean container exit (state=$state, exit=$exit_code). Inspect it before retrying."
                    break ;;
                failed|rejected|orphaned|remove) fail "Task $task_id is $state; cleanup is unverified. Inspect it before retrying." ;;
            esac
            [ "$(date +%s)" -lt "$deadline" ] || fail "Timed out waiting for task $task_id ($state); stack retained for inspection."
            sleep 2
        done
    done
}

case "$command_name" in
    logs)
        case "$log_component" in
            access|auth)
                service_exists "${KPL_STACK_NAME}_controller" || fail 'Controller service does not exist; deploy the stack before reading web logs.'
                log_tasks=$(dock service ps --quiet --no-trunc --filter desired-state=running "${KPL_STACK_NAME}_controller") || fail 'Cannot list Controller tasks.'
                log_container=''
                log_node=''
                for log_task in $log_tasks; do
                    case "$log_task" in ''|*[!a-zA-Z0-9]*) fail 'Docker returned an invalid Controller task ID.' ;; esac
                    log_record=$(dock inspect --type task --format '{{.NodeID}}|{{.Status.State}}|{{if .Status.ContainerStatus}}{{.Status.ContainerStatus.ContainerID}}{{end}}' "$log_task") || fail 'Cannot inspect Controller task; retry after it settles.'
                    task_node=${log_record%%|*}; log_rest=${log_record#*|}
                    task_state=${log_rest%%|*}; task_container=${log_rest#*|}
                    [ "$task_state" = running ] || continue
                    for log_field in "$task_node" "$task_container"; do
                        case "$log_field" in ''|*[!a-zA-Z0-9]*) fail 'Docker returned invalid Controller task metadata.' ;; esac
                    done
                    [ "$log_record" = "$task_node|running|$task_container" ] || fail 'Docker returned incomplete Controller task metadata.'
                    [ -z "$log_container" ] || fail 'Multiple Controller tasks are running; retry after the service update settles.'
                    log_node=$task_node
                    log_container=$task_container
                done
                [ -n "$log_container" ] || fail 'No running Controller task; start the Controller before reading web logs.'
                log_dock() {
                    if [ -n "$log_context" ]; then dock --context "$log_context" "$@"; else dock "$@"; fi
                }
                log_daemon_node=$(log_dock info --format '{{.Swarm.NodeID}}') || fail 'Cannot connect to the Docker daemon selected for web logs.'
                [ "$log_daemon_node" = "$log_node" ] || fail "Controller runs on Swarm node $log_node. Use logs $log_component --context NAME with a Docker context connected to that node."
                # The manager supplied this exact task's container ID. Do not
                # guess a local container name or start a helper service.
                log_dock exec "$log_container" sh -c '
                    if [ ! -f "$1" ]; then
                        printf "KPL Swarm: Web log file is missing: %s. Deploy a Controller image with web logging enabled.\n" "$1" >&2
                        exit 1
                    fi
                    if [ "$2" = all ]; then cat "$1"; else tail -n "$2" "$1"; fi
                ' kpl-web-log "/var/lib/kpl/data/logs/$log_component.jsonl" "$log_tail" || fail 'Cannot read Controller web logs; check the error above and retry if the task restarted.'
                ;;
            *) dock service logs --tail "$log_tail" --timestamps "${KPL_STACK_NAME}_$log_component" ;;
        esac
        ;;
    status)
        dock stack services "$KPL_STACK_NAME"
        if service_exists "${KPL_STACK_NAME}_agent"; then dock service ps --no-trunc "${KPL_STACK_NAME}_agent"; fi
        printf '\nAgent placement label: %s=true\n' "$agent_label"
        dock node ls --filter "node.label=$agent_label=true"
        ;;
    deploy)
        : "${KPL_IMAGE:?Set KPL_IMAGE in .env.swarm or environment}"
        : "${KPL_CONTROL_NODE_ID:?Set KPL_CONTROL_NODE_ID}"
        : "${KPL_USER:?Set KPL_USER}" "${KPL_PASSWORD:?Set KPL_PASSWORD}"
        : "${GRAFANA_ADMIN_PASSWORD:?Set GRAFANA_ADMIN_PASSWORD}"
        sh "$root/scripts/check-swarm.sh" --config-only
        # Published endpoints must also be usable by browsers outside the overlay.
        public_control_host=$(control_node_host) || exit 1
        export KPL_CONTROLLER_METRICS_URL="http://$public_control_host:${KPL_HTTP_PORT:-8080}/metrics"
        export KPL_PROMETHEUS_EXTERNAL_URL="http://$public_control_host:${PROMETHEUS_PORT:-9090}/"
        # Validate interpolation without printing credentials.
        dock stack config --compose-file "$root/stack.swarm.yaml" >/dev/null
        ready_node "$KPL_CONTROL_NODE_ID"
        nodes=$(command_nodes "$@")
        for node in $nodes; do ready_node "$node"; done
        report_nodes "$nodes"
        resolve_deployment_image
        networks=$(dock network ls --quiet --filter "name=^$KPL_PEER_NETWORK$")
        if [ -z "$networks" ]; then
            set -- network create --driver overlay --attachable --label "io.kpl.application=$application" --label "io.kpl.stack=$KPL_STACK_NAME"
            if [ -n "$KPL_PEER_SUBNET" ]; then set -- "$@" --subnet "$KPL_PEER_SUBNET"; fi
            dock "$@" "$KPL_PEER_NETWORK"
        else
            [ "$(dock network inspect --format '{{.Name}} {{.Driver}} {{.Attachable}}' "$KPL_PEER_NETWORK")" = "$KPL_PEER_NETWORK overlay true" ] || fail 'Peer network must be an attachable overlay.'
            if [ -n "$KPL_PEER_SUBNET" ]; then
                network_subnets=$(dock network inspect --format '{{range .IPAM.Config}}{{println .Subnet}}{{end}}' "$KPL_PEER_NETWORK") || fail 'Cannot inspect Peer network subnets.'
                [ "$network_subnets" = "$KPL_PEER_SUBNET" ] || fail 'Existing Peer network subnet differs from KPL_PEER_SUBNET; no network or placement changes made.'
            fi
        fi
        for node in $nodes; do dock node update --label-add "$agent_label=true" "$node"; done
        timeout -s TERM -k 5 "$docker_timeout" sh "$root/scripts/check-swarm.sh"
        dock stack deploy --with-registry-auth --resolve-image always --detach=true --compose-file "$root/stack.swarm.yaml" "$KPL_STACK_NAME"
        printf 'Deployment submitted. Use status, then check registered Agents in the Controller before running an experiment.\n'
        ;;
    add-node)
        service_exists "${KPL_STACK_NAME}_agent" || fail 'Deploy the stack before adding nodes.'
        nodes=$(command_nodes "$@")
        for node in $nodes; do ready_node "$node"; done
        report_nodes "$nodes"
        for node in $nodes; do dock node update --label-add "$agent_label=true" "$node"; done
        # Resume a node left excluded by an interrupted removal.
        placement_exclusions rm "$nodes"
        printf 'Agent placement updated. Use status and wait for Controller registration.\n'
        ;;
    remove-node)
        service_exists "${KPL_STACK_NAME}_agent" || fail 'Agent service does not exist.'
        nodes=$(command_nodes "$@")
        tasks=''
        for node in $nodes; do
            ready_node "$node"
            captured=$(capture_tasks "${KPL_STACK_NAME}_agent" "$node")
            tasks="$tasks $captured"
        done
        report_nodes "$nodes"
        # Changing a node label first invalidates the old task's own constraints:
        # Docker can report Rejected with stale PID/exit data even after exit 0.
        # A service-only exclusion preserves the old task's valid node labels
        # while the orchestrator requests an ordinary, verifiable shutdown.
        placement_exclusions add "$nodes"
        wait_tasks "$tasks" "$agent_timeout"
        # Catch a scheduler replacement racing with the placement update.
        for node in $nodes; do
            remaining=$(capture_tasks "${KPL_STACK_NAME}_agent" "$node")
            wait_tasks "$remaining" "$agent_timeout"
        done
        for node in $nodes; do dock node update --label-rm "$agent_label" "$node"; done
        placement_exclusions rm "$nodes"
        printf 'Selected Agents stopped with clean exits. Other node labels were unchanged.\n'
        ;;
    remove)
        if [ -n "$services" ]; then
            # Ownership was checked above. Delete the service definitions
            # directly: failed tasks, missing history and offline workers must
            # not block removal or leave Swarm scheduling replacements.
            dock service rm $services || fail 'Service removal failed; inspect status and retry remove.'
        fi
        # Also remove stack-owned configs and monitoring networks. Repeat this
        # on an empty stack so an interrupted removal can finish its cleanup.
        dock stack rm "$KPL_STACK_NAME" || fail 'Stack resource removal failed; inspect status and retry remove.'
        deadline=$(( $(date +%s) + $docker_timeout ))
        while :; do
            remaining_services=$(dock service ls --quiet --filter "label=com.docker.stack.namespace=$KPL_STACK_NAME") || fail 'Cannot verify stack removal; Docker service lookup failed.'
            [ -n "$remaining_services" ] || break
            [ "$(date +%s)" -lt "$deadline" ] || fail 'Stack removal is still pending; inspect status.'
            sleep 2
        done
        # Placement labels are best effort after deletion. Retry them even if
        # a previous remove already deleted every service.
        if nodes=$(selected_nodes); then
            for node in $nodes; do
                dock node update --label-rm "$agent_label" "$node" || printf 'KPL Swarm: Could not clear Agent label on %s; retry remove to clear remaining labels.\n' "$node" >&2
            done
        else
            printf 'KPL Swarm: Could not list Agent labels; retry remove to clear remaining labels.\n' >&2
        fi
        printf 'Stack services removed. Data volumes and Peer network preserved.\n'
        printf 'Service deletion does not verify final experiment logs or standalone Peer cleanup.\n'
        ;;
esac

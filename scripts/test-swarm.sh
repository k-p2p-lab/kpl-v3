#!/bin/sh
# Stateful fake Docker tests. No daemon socket, registry or cluster is accessed.
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'result=$?; if [ "$result" -ne 0 ]; then printf "Manager test case %s failed\n" "${case_number:-setup}" >&2; if [ -n "${KPL_TEST_STATE:-}" ]; then cat "$KPL_TEST_STATE/output" "$KPL_TEST_STATE/events" "$KPL_TEST_STATE/calls" >&2; fi; fi; rm -rf "$scratch"; exit "$result"' EXIT
mkdir "$scratch/bin"
: > "$scratch/config.env"
KPL_TEST_REAL_TIMEOUT=$(command -v timeout)
export KPL_TEST_REAL_TIMEOUT
cat > "$scratch/bin/timeout" <<'MOCK_TIMEOUT'
#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$KPL_TEST_STATE/timeouts"
exec "$KPL_TEST_REAL_TIMEOUT" "$@"
MOCK_TIMEOUT
chmod +x "$scratch/bin/timeout"

cat > "$scratch/bin/docker" <<'MOCK_DOCKER'
#!/bin/sh
set -eu
s=$KPL_TEST_STATE
stack=${KPL_STACK_NAME:-kpl}
printf '%s\n' "$*" >> "$s/calls"
die() { printf 'Unexpected Docker call: %s\n' "$*" >&2; exit 97; }
event() { printf '%s\n' "$1" >> "$s/events"; }
if [ -n "${KPL_TEST_EXPECT_CONTEXT:-}" ] && [ "${DOCKER_CONTEXT:-}" != "$KPL_TEST_EXPECT_CONTEXT" ]; then
    die 'The selected Docker context was changed'
fi
log_context=''
if [ "$1" = --context ]; then
    log_context=$2
    [ "$log_context" = controller-node ] || die 'Unknown Docker context'
    shift 2
    case "$1 ${2:-}" in 'info --format'|'exec '*) ;; *) die 'Remote context used outside web log reads' ;; esac
fi
for last do :; done
case "$1 ${2:-}" in
    'build --tag')
        [ "$#" = 4 ] && [ "$4" = "$KPL_TEST_REPO_ROOT" ] || die "$@"
        [ "${DOCKER_DEFAULT_PLATFORM+x}" != x ] || die 'DOCKER_DEFAULT_PLATFORM reached build'
        event image-build
        if [ "${KPL_TEST_BUILD_FAIL:-0}" = 1 ]; then printf 'mock image build failed\n' >&2; exit 1; fi ;;
    'push '*)
        [ "$#" = 2 ] || die "$@"
        [ "${DOCKER_DEFAULT_PLATFORM+x}" != x ] || die 'DOCKER_DEFAULT_PLATFORM reached push'
        event image-push
        if [ "${KPL_TEST_PUSH_FAIL:-0}" = 1 ]; then printf 'mock image push failed\n' >&2; exit 1; fi ;;
    'buildx build')
        [ "$#" = 8 ] && [ "$3" = --platform ] && [ "$5" = --tag ] && [ "$7" = --push ] && [ "$8" = "$KPL_TEST_REPO_ROOT" ] || die "$@"
        [ "${DOCKER_DEFAULT_PLATFORM+x}" != x ] || die 'DOCKER_DEFAULT_PLATFORM reached buildx'
        event image-buildx
        if [ "${KPL_TEST_BUILD_FAIL:-0}" = 1 ]; then printf 'mock image buildx failed\n' >&2; exit 1; fi ;;
    'login '*)
        [ "$#" -le 2 ] || die "$@"
        event registry-login ;;
    'pull '*)
        [ "$#" = 2 ] || die "$@"
        # The selected platform must not collapse a multi-platform tag to the
        # Manager's local platform. Do not consult local image IDs/RepoDigests.
        [ "${DOCKER_DEFAULT_PLATFORM+x}" != x ] || die 'DOCKER_DEFAULT_PLATFORM reached pull'
        digest=${KPL_TEST_PULL_DIGEST:-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}
        printf 'Pulling from %s\n' "$2"
        case "${KPL_TEST_PULL_FAULT:-none}" in
            missing) printf 'Status: Image is up to date\n' ;;
            malformed) printf 'Digest: sha256:abcd\n' ;;
            uppercase) printf 'Digest: sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n' ;;
            multiple) printf 'Digest: sha256:%s\nDigest: sha256:%s\n' "$digest" "$digest" ;;
            mixed) printf 'Digest: sha256:%s\nDigest: malformed\n' "$digest" ;;
            trailing) printf 'Digest: sha256:%s trailing-data\n' "$digest" ;;
            fail) printf 'Digest: sha256:%s\n' "$digest"; printf 'mock registry unavailable\n' >&2; exit 1 ;;
            none) printf 'Digest: sha256:%s\n' "$digest" ;;
            *) die "${KPL_TEST_PULL_FAULT}" ;;
        esac ;;
    'info --format')
        case "$3" in
            '{{.Swarm.NodeID}}')
                if [ -n "$log_context" ]; then printf '%s\n' "${KPL_TEST_LOG_CONTEXT_NODE:-worker1}"; else printf '%s\n' "${KPL_TEST_SELF_ID:-control1}"; fi ;;
            '{{.Swarm.LocalNodeState}} {{.Swarm.ControlAvailable}}') printf 'active true\n' ;;
            *) die "$@" ;;
        esac ;;
    'service ls')
        case "$*" in *"label=com.docker.stack.namespace=$stack"*) ;; *) die "$@" ;; esac
        if [ -f "$s/removed" ]; then
            if [ "${KPL_TEST_FINAL_LIST_FAIL:-0}" = 1 ]; then printf 'mock final service listing failed\n' >&2; exit 2; fi
            [ "${KPL_TEST_REMOVAL_PENDING:-0}" = 1 ] || exit 0
        fi
        [ "${KPL_TEST_EMPTY_STACK:-0}" = 0 ] || exit 0
        case "$*" in
            *"name=${stack}_controller") printf 'svccontroller\n' ;;
            *"name=${stack}_agent") printf 'svcagent\n' ;;
            *name=*) die "$@" ;;
            *) printf 'svccontroller\nsvcagent\nsvcprometheus\nsvcgrafana\n' ;;
        esac ;;
    'service inspect')
        if [ "${KPL_TEST_INSPECT_FAIL:-0}" = 1 ]; then printf 'mock service inspection failed\n' >&2; exit 2; fi
        case "$last" in
            svccontroller|"${stack}_controller") name=${stack}_controller ;;
            svcagent|"${stack}_agent") name=${stack}_agent ;;
            svcprometheus) name=${stack}_prometheus ;;
            svcgrafana) name=${stack}_grafana ;;
            *) die "$@" ;;
        esac
        case "$4" in
            *io.kpl.application*)
                owner=kp2plab-v3
                if [ "${KPL_TEST_FOREIGN:-0}" = 1 ]; then owner=another-application; fi
                if [ "${KPL_TEST_FOREIGN_STACK:-0}" = 1 ]; then name=another_stack_agent; fi
                printf '%s|%s\n' "$name" "$owner" ;;
            '{{.Spec.Name}}') printf '%s\n' "$name" ;;
            *) die "$@" ;;
        esac ;;
    'service ps')
        [ "${KPL_TEST_EMPTY_HISTORY:-0}" = 0 ] || exit 0
        case "$last" in
            "${stack}_controller")
                case "$*" in
                    *desired-state=running*) printf '%s\n' "${KPL_TEST_LOG_TASKS-taskC}"; exit 0 ;;
                esac
                # Pausing placement preserves the running slot's old task and
                # introduces an unassigned replacement ahead of it in history.
                if [ -f "$s/controller-stopped" ]; then printf 'pendingC\n'; fi
                printf 'taskC\n' ;;
            "${stack}_agent")
                case "$*" in
                    *node=worker1*) printf 'taskA1\noldA1\n' ;;
                    *node=worker2*) printf 'taskA2\n' ;;
                    *node=control1*) printf 'taskA-control1\n' ;;
                    *node=manager2*) printf 'taskA-manager2\n' ;;
                    *node=unlabeled1*) printf 'taskA-unlabeled1\n' ;;
                    *) printf 'taskA1\noldA1\ntaskA2\n' ;;
                esac ;;
            *) die "$@" ;;
        esac ;;
    'service logs')
        [ "$#" = 6 ] && [ "$3" = --tail ] && [ "$5" = --timestamps ] || die "$@"
        case "$6" in "${stack}_controller"|"${stack}_agent"|"${stack}_prometheus"|"${stack}_grafana") ;; *) die "$@" ;; esac
        printf 'mock service log for %s\n' "$6" ;;
    'exec '*)
        [ "$#" = 8 ] && [ "$2 $3 $4 $6" = 'containerC sh -c kpl-web-log' ] || die "$@"
        case "$7" in /var/lib/kpl/data/logs/access.jsonl|/var/lib/kpl/data/logs/auth.jsonl) ;; *) die "$@" ;; esac
        if [ "${KPL_TEST_LOG_EXEC_FAIL:-0}" = 1 ]; then printf 'mock Controller container stopped\n' >&2; exit 1; fi
        # Execute the real reader with a fixture file; never use the real daemon
        # or Controller data path. This verifies tail/all and missing-file errors.
        sh -c "$5" "$6" "$s/logs/${7##*/}" "$8" ;;
    'inspect --type')
        [ "$3" = task ] || die "$@"
        if [ "$5" = '{{.NodeID}}|{{.Status.State}}|{{if .Status.ContainerStatus}}{{.Status.ContainerStatus.ContainerID}}{{end}}' ]; then
            case "$last" in
                taskC) printf '%s|running|containerC\n' "${KPL_TEST_CONTROLLER_NODE:-control1}" ;;
                pendingC) printf '|pending|\n' ;;
                failedC) printf 'control1|failed|oldContainer\n' ;;
                malformedC) printf 'control1|running|\n' ;;
                *) die "$@" ;;
            esac
            exit 0
        fi
        state=running; pid=42; code=0; issue=ok
        case "$last" in
            pendingC)
                [ -f "$s/controller-stopped" ] || die "$@"
                node=''; state=${KPL_TEST_PENDING_STATE:-pending}
                container=${KPL_TEST_PENDING_CONTAINER:--}; pid=0; code=-; issue=error ;;
            taskC)
                node=control1; container=containerC
                if [ -f "$s/controller-stopped" ]; then
                    if [ "${KPL_TEST_SLOW_STOP_INSPECT:-0}" = 1 ]; then sleep 4; fi
                    state=shutdown; pid=0
                    case "${KPL_TEST_CONTROLLER_STOP:-clean}" in
                        failed) state=failed; code=1; issue=error ;;
                        nonzero) code=1 ;;
                    esac
                fi ;;
            taskA1|taskA2|taskA-control1|taskA-manager2|taskA-unlabeled1)
                case "$last" in taskA1) node=worker1 ;; taskA2) node=worker2 ;; *) node=${last#taskA-} ;; esac
                container=container$node
                if [ -f "$s/stopped-$node" ]; then
                    if [ "${KPL_TEST_SLOW_STOP_INSPECT:-0}" = 1 ]; then sleep 4; fi
                    state=shutdown; pid=0
                    case "${KPL_TEST_AGENT_STOP:-clean}" in
                        failed|rejected|orphaned|remove) state=$KPL_TEST_AGENT_STOP; code=1; issue=error ;;
                    esac
                fi
                # Swarm can reject an existing task when its placement label
                # disappears before clean shutdown has been verified.
                if [ -f "$s/rejected-$node" ]; then state=rejected; pid=42; issue=error; fi ;;
            oldA1)
                node=worker1; container=oldContainer; state=shutdown; pid=0
                if [ "${KPL_TEST_OLD_FAILED:-0}" = 1 ]; then state=failed; code=1; issue=error; fi ;;
            *) die "$@" ;;
        esac
        if [ "$last" != oldA1 ] && [ "$state" = shutdown ] && [ "$code" = 0 ] && [ ! -f "$s/clean-$last" ]; then
            event "clean-$last"
            : > "$s/clean-$last"
        fi
        printf '%s|%s|%s|%s|%s|%s\n' "$node" "$state" "$container" "$pid" "$code" "$issue" ;;
    'node inspect')
        format=$4; shift 4
        for node do
            case "$node" in
                worker-a) node=worker1 ;;
                worker-b) node=worker2 ;;
                control1|manager2|worker1|worker2|unlabeled1|windows1|down1|paused1|drained1) ;;
                *) die "$@" ;;
            esac
            if [ "${KPL_TEST_NODE_INSPECT_FAIL:-}" = "$node" ]; then printf 'mock node inspection failed\n' >&2; exit 2; fi
            os=linux; state=ready; availability=active; role=worker; hostname=$node
            case "$node" in
                control1|manager2) role=manager ;;
                windows1) os=windows ;;
                down1) state=down ;;
                paused1) availability=pause ;;
                drained1) availability=drain ;;
            esac
            if [ "${KPL_TEST_DOWN_NODE:-}" = "$node" ]; then state=down; fi
            case "$format" in
                '{{.ID}}') printf '%s\n' "$node" ;;
                '{{.Status.State}}') printf '%s\n' "$state" ;;
                '{{.Status.Addr}}')
                    case "$node" in
                        control1) printf '%s\n' "${KPL_TEST_CONTROL_ADDR:-10.20.0.7}" ;;
                        worker1) printf '%s\n' "${KPL_TEST_WORKER1_ADDR:-10.20.0.11}" ;;
                        worker2) printf '%s\n' "${KPL_TEST_WORKER2_ADDR:-fd00::12}" ;;
                        *) printf '%s\n' "${KPL_TEST_CONTROL_ADDR:-10.20.0.7}" ;;
                    esac ;;
                '{{.Description.Platform.OS}} {{.Status.State}}') printf '%s %s\n' "$os" "$state" ;;
                '{{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}}') printf '%s %s %s\n' "$os" "$state" "$availability" ;;
                '{{.ID}} {{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}}') printf '%s %s %s %s\n' "$node" "$os" "$state" "$availability" ;;
                '{{.ID}}|{{.Spec.Role}}|{{.Description.Platform.OS}}|{{.Status.State}}|{{.Spec.Availability}}') printf '%s|%s|%s|%s|%s\n' "$node" "$role" "$os" "$state" "$availability" ;;
                *) die "$format" ;;
            esac
        done ;;
    'node ls')
        if [ "${KPL_TEST_NODE_LS_FAIL:-0}" = 1 ]; then printf 'mock node listing failed\n' >&2; exit 2; fi
        if [ "$#" = 2 ]; then printf 'ID HOSTNAME STATUS AVAILABILITY MANAGER STATUS\ncontrol1 manager-main Ready Active Leader\nworker1 worker-a Ready Active\n'; exit 0; fi
        if [ "$#" = 3 ] && [ "$3" = --quiet ]; then
            printf '%s\n' "${KPL_TEST_NODE_IDS-control1 worker1 worker2}" | tr ' ' '\n'
            exit 0
        fi
        case "$*" in *"node.label=kpl.$stack.agent=true") ;; *) die "$@" ;; esac
        seen=' '
        for node in ${KPL_TEST_LABEL_NODES-worker1 worker2} control1 manager2 worker1 worker2 unlabeled1 windows1 down1 paused1 drained1; do
            case "$seen" in *" $node "*) continue ;; esac
            seen="$seen$node "
            case " ${KPL_TEST_LABEL_NODES-worker1 worker2} " in
                *" $node "*) ;;
                *) [ -f "$s/labeled-$node" ] || continue ;;
            esac
            [ -f "$s/unlabeled-$node" ] || printf '%s\n' "$node"
        done ;;
    'node update')
        if [ "${KPL_TEST_NODE_UPDATE_FAIL:-}" = "$last" ]; then printf 'mock node label update failed\n' >&2; exit 2; fi
        case "$3 $4" in
            "--label-rm kpl.$stack.agent")
                case "$last" in worker1) task=taskA1 ;; worker2) task=taskA2 ;; control1|manager2|unlabeled1) task=taskA-$last ;; *) die "$@" ;; esac
                if [ ! -f "$s/clean-$task" ]; then : > "$s/rejected-$last"; fi
                : > "$s/unlabeled-$last"; event "unlabel-$last" ;;
            "--label-add kpl.$stack.agent=true")
                rm -f "$s/unlabeled-$last"; : > "$s/labeled-$last"; event "label-$last" ;;
            *) die "$@" ;;
        esac ;;
    'service rm')
        [ "$*" = 'service rm svccontroller svcagent svcprometheus svcgrafana' ] || die "$@"
        if [ "${KPL_TEST_SERVICE_RM_FAIL:-0}" = 1 ]; then printf 'mock service removal failed\n' >&2; exit 2; fi
        shift 2
        for service do event "service-rm-$service"; done
        : > "$s/removed" ;;
    'service update')
        [ "$3 $4" = '--detach=true --no-resolve-image' ] || die "$@"
        if [ "$last" = "${stack}_controller" ]; then
            [ "$#" = 9 ] && [ "$5 $6 $7 $8" = '--constraint-add node.role==manager --constraint-add node.role==worker' ] || die "$@"
            event pause-controller; : > "$s/controller-stopped"
            exit 0
        fi
        [ "$last" = "${stack}_agent" ] || die "$@"
        shift 4
        while [ "$#" -gt 1 ]; do
            action=$1; constraint=$2
            case "$constraint" in 'node.id!=control1'|'node.id!=manager2'|'node.id!=worker1'|'node.id!=worker2'|'node.id!=unlabeled1') node=${constraint#node.id!=} ;; *) die "$@" ;; esac
            case "$action" in
                --constraint-add)
                    : > "$s/excluded-$node"; : > "$s/stopped-$node"; event "exclude-$node" ;;
                --constraint-rm)
                    rm -f "$s/excluded-$node"; event "include-$node" ;;
                *) die "$@" ;;
            esac
            shift 2
        done ;;
    'network ls')
        [ "${KPL_TEST_NO_NETWORK:-0}" = 1 ] || printf 'network1\n' ;;
    'network inspect')
        case "$4" in
            '{{.Name}} {{.Driver}} {{.Attachable}}') printf '%s overlay true\n' "$KPL_PEER_NETWORK" ;;
            '{{range .IPAM.Config}}{{println .Subnet}}{{end}}') printf '%s\n' "${KPL_TEST_NETWORK_SUBNET:-10.60.0.0/24}" ;;
            *) die "$@" ;;
        esac ;;
    'network create') event network-create; printf 'network1\n' ;;
    'stack config')
        printf '%s\n%s\n' "${KPL_CONTROLLER_METRICS_URL:-}" "${KPL_PROMETHEUS_EXTERNAL_URL:-}" > "$s/config-public-urls"
        printf '%s' "${KPL_PASSWORD:-}" > "$s/config-password"
        printf '%s\n' "${KPL_IMAGE:-}" >> "$s/config-images" ;;
    'stack deploy')
        printf '%s\n%s\n' "${KPL_CONTROLLER_METRICS_URL:-}" "${KPL_PROMETHEUS_EXTERNAL_URL:-}" > "$s/deploy-public-urls"
        printf '%s\n' "${KPL_IMAGE:-}" >> "$s/deploy-images"
        event stack-deploy ;;
    'stack services') printf 'mock stack services\n' ;;
    'stack rm')
        [ "$3" = "$stack" ] || die "$@"
        event stack-rm
        if [ "${KPL_TEST_STACK_RM_FAIL:-0}" = 1 ]; then printf 'mock stack removal failed\n' >&2; exit 2; fi
        : > "$s/removed" ;;
    *) die "$@" ;;
esac
MOCK_DOCKER
chmod +x "$scratch/bin/docker"
export PATH="$scratch/bin:$PATH"

case_number=0
reset_case() {
    case_number=$((case_number + 1))
    KPL_TEST_STATE=$scratch/case-$case_number
    export KPL_TEST_STATE
    mkdir "$KPL_TEST_STATE"
    : > "$KPL_TEST_STATE/calls"
    : > "$KPL_TEST_STATE/events"
    : > "$KPL_TEST_STATE/timeouts"
    export KPL_STACK_NAME=lab KPL_PEER_NETWORK=lab-peers KPL_CONTROL_NODE_ID=control1
    export KPL_IMAGE=registry.example/kpl@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
    export KPL_USER=admin KPL_PASSWORD=private-dashboard-password GRAFANA_ADMIN_PASSWORD=private-test-password
    export KPL_DOCKER_TIMEOUT=3 KPL_CONTROLLER_STOP_TIMEOUT=3 KPL_AGENT_STOP_TIMEOUT=3
    export KPL_TEST_REPO_ROOT=$root
    unset KPL_TEST_FOREIGN KPL_TEST_FOREIGN_STACK KPL_TEST_DOWN_NODE KPL_TEST_CONTROLLER_STOP KPL_TEST_AGENT_STOP KPL_TEST_EMPTY_STACK KPL_TEST_NO_NETWORK KPL_MIN_AGENTS KPL_TEST_FINAL_LIST_FAIL KPL_TEST_INSPECT_FAIL KPL_TEST_EMPTY_HISTORY KPL_TEST_OLD_FAILED KPL_TEST_PENDING_STATE KPL_TEST_PENDING_CONTAINER KPL_TEST_PULL_DIGEST KPL_TEST_PULL_FAULT KPL_IMAGE_PULL_TIMEOUT DOCKER_DEFAULT_PLATFORM
    unset KPL_TEST_CONTROLLER_NODE KPL_TEST_LOG_CONTEXT_NODE KPL_TEST_LOG_TASKS KPL_TEST_LOG_EXEC_FAIL
    unset KPL_TEST_SLOW_STOP_INSPECT KPL_TEST_SERVICE_RM_FAIL KPL_TEST_STACK_RM_FAIL KPL_TEST_REMOVAL_PENDING KPL_TEST_NODE_UPDATE_FAIL
    unset KPL_TEST_SELF_ID KPL_TEST_NODE_IDS KPL_TEST_LABEL_NODES KPL_TEST_NODE_LS_FAIL KPL_TEST_NODE_INSPECT_FAIL
    unset KPL_PEER_SUBNET KPL_IMAGE_BUILD_TIMEOUT KPL_IMAGE_PUSH_TIMEOUT KPL_TEST_BUILD_FAIL KPL_TEST_PUSH_FAIL KPL_TEST_EXPECT_CONTEXT DOCKER_CONTEXT KPL_TEST_NETWORK_SUBNET KPL_TEST_CONTROL_ADDR KPL_TEST_WORKER1_ADDR KPL_TEST_WORKER2_ADDR KPL_HTTP_PORT KPL_AGENT_METRICS_PORT PROMETHEUS_PORT GRAFANA_PORT
}
run() { sh "$root/scripts/swarm.sh" --env-file "$scratch/config.env" "$@" > "$KPL_TEST_STATE/output" 2>&1; }
reject() {
    if run "$@"; then
        printf 'Unexpected acceptance of %s\n' "$*" >&2
        cat "$KPL_TEST_STATE/output" >&2
        exit 1
    fi
}
no_stack_removal() { ! grep -q '^stack-rm$' "$KPL_TEST_STATE/events"; }
no_mutation() { [ ! -s "$KPL_TEST_STATE/events" ]; }

# Full removal is scoped to verified services and never depends on task exits.
no_shutdown_checks() {
    ! grep -q '^service ps\|^inspect --type task\|^node inspect\|^service update' "$KPL_TEST_STATE/calls"
}
cat > "$scratch/expected-remove" <<'EXPECTED'
service-rm-svccontroller
service-rm-svcagent
service-rm-svcprometheus
service-rm-svcgrafana
stack-rm
unlabel-worker1
unlabel-worker2
EXPECTED
reset_case
run remove
cmp "$scratch/expected-remove" "$KPL_TEST_STATE/events"
no_shutdown_checks
grep -q 'Data volumes and Peer network preserved' "$KPL_TEST_STATE/output"
grep -q 'does not verify final experiment logs or standalone Peer cleanup' "$KPL_TEST_STATE/output"
# Repeated removal finishes without attempting to delete missing service IDs.
run remove
[ "$(grep -c '^service rm ' "$KPL_TEST_STATE/calls")" = 1 ]
[ "$(grep -c '^stack-rm$' "$KPL_TEST_STATE/events")" = 2 ]
[ "$(grep -c '^unlabel-' "$KPL_TEST_STATE/events")" = 2 ]

for task_state in failed rejected orphaned remove; do
    reset_case
    export KPL_TEST_AGENT_STOP=$task_state KPL_TEST_CONTROLLER_STOP=nonzero KPL_TEST_OLD_FAILED=1
    : > "$KPL_TEST_STATE/controller-stopped"
    : > "$KPL_TEST_STATE/stopped-worker1"
    : > "$KPL_TEST_STATE/stopped-worker2"
    run remove
    cmp "$scratch/expected-remove" "$KPL_TEST_STATE/events"
    no_shutdown_checks
done

for fault in KPL_TEST_DOWN_NODE=worker2 KPL_TEST_EMPTY_HISTORY=1 KPL_TEST_NODE_INSPECT_FAIL=worker1; do
    reset_case
    export "$fault"
    run remove
    cmp "$scratch/expected-remove" "$KPL_TEST_STATE/events"
    no_shutdown_checks
done

# Labels are cleaned even when services are already gone; this also allows
# recovery from an interrupted stack removal.
reset_case
export KPL_TEST_EMPTY_STACK=1
run remove
printf 'stack-rm\nunlabel-worker1\nunlabel-worker2\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"
no_shutdown_checks

reset_case
export KPL_TEST_NODE_LS_FAIL=1
run remove
grep -q '^stack-rm$' "$KPL_TEST_STATE/events"
grep -q 'Could not list Agent labels' "$KPL_TEST_STATE/output"
grep -q 'Stack services removed' "$KPL_TEST_STATE/output"
unset KPL_TEST_NODE_LS_FAIL
run remove
[ "$(grep -c '^service rm ' "$KPL_TEST_STATE/calls")" = 1 ]
[ "$(grep -c '^unlabel-' "$KPL_TEST_STATE/events")" = 2 ]

reset_case
export KPL_TEST_NODE_UPDATE_FAIL=worker1
run remove
grep -q 'Could not clear Agent label on worker1' "$KPL_TEST_STATE/output"
[ ! -e "$KPL_TEST_STATE/unlabeled-worker1" ]
[ -e "$KPL_TEST_STATE/unlabeled-worker2" ]
unset KPL_TEST_NODE_UPDATE_FAIL
run remove
[ -e "$KPL_TEST_STATE/unlabeled-worker1" ]
[ "$(grep -c '^service rm ' "$KPL_TEST_STATE/calls")" = 1 ]

for fault in KPL_TEST_FOREIGN=1 KPL_TEST_FOREIGN_STACK=1 KPL_TEST_INSPECT_FAIL=1; do
    reset_case
    export "$fault"
    reject remove
    no_mutation
done

reset_case
export KPL_TEST_SERVICE_RM_FAIL=1
reject remove
no_stack_removal
grep -q 'Service removal failed' "$KPL_TEST_STATE/output"
if grep -q 'Stack services removed' "$KPL_TEST_STATE/output"; then exit 1; fi

reset_case
export KPL_TEST_STACK_RM_FAIL=1
reject remove
grep -q 'Stack resource removal failed' "$KPL_TEST_STATE/output"
unset KPL_TEST_STACK_RM_FAIL
run remove
[ "$(grep -c '^service rm ' "$KPL_TEST_STATE/calls")" = 1 ]
[ "$(grep -c '^stack-rm$' "$KPL_TEST_STATE/events")" = 2 ]
grep -q 'Stack services removed' "$KPL_TEST_STATE/output"

reset_case
export KPL_TEST_FINAL_LIST_FAIL=1
reject remove
grep -q '^stack-rm$' "$KPL_TEST_STATE/events"
grep -q 'Cannot verify stack removal' "$KPL_TEST_STATE/output"
if grep -q 'Stack services removed' "$KPL_TEST_STATE/output"; then exit 1; fi

reset_case
export KPL_TEST_REMOVAL_PENDING=1 KPL_DOCKER_TIMEOUT=1
reject remove
grep -q 'Stack removal is still pending' "$KPL_TEST_STATE/output"
if grep -q 'Stack services removed' "$KPL_TEST_STATE/output"; then exit 1; fi

# Individual node removal still requires a verified, clean Agent exit.
reset_case
run remove-node worker-a
printf 'exclude-worker1\nclean-taskA1\nunlabel-worker1\ninclude-worker1\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"
[ ! -e "$KPL_TEST_STATE/excluded-worker1" ]
[ ! -e "$KPL_TEST_STATE/stopped-worker2" ]
[ ! -e "$KPL_TEST_STATE/unlabeled-worker2" ]
grep -q 'node update --label-rm kpl.lab.agent worker1' "$KPL_TEST_STATE/calls"
if grep -q 'label-rm kpl.agent\|label-rm kpl.other.agent' "$KPL_TEST_STATE/calls"; then exit 1; fi

# A busy daemon cannot make one stop inspection consume the full CLI timeout
# after only one second remains in the Agent stop budget.
reset_case
export KPL_DOCKER_TIMEOUT=30 KPL_AGENT_STOP_TIMEOUT=1 KPL_TEST_SLOW_STOP_INSPECT=1
reject remove-node worker-a
no_stack_removal
grep -q 'Cannot inspect task taskA1' "$KPL_TEST_STATE/output"
grep -q -- '-s TERM -k 5 1 docker inspect --type task .* taskA1$' "$KPL_TEST_STATE/timeouts"
if grep -q '^unlabel-' "$KPL_TEST_STATE/events"; then exit 1; fi

# The fake reproduces Swarm's stale-PID rejection for premature label removal.
reset_case
docker node update --label-rm kpl.lab.agent worker1 > "$KPL_TEST_STATE/output"
[ "$(docker inspect --type task --format unused taskA1)" = 'worker1|rejected|containerworker1|42|0|error' ]
reject remove-node worker-a
no_stack_removal
grep -q 'cleanup is unverified' "$KPL_TEST_STATE/output"
[ -e "$KPL_TEST_STATE/excluded-worker1" ]
if grep -q '^include-worker1$' "$KPL_TEST_STATE/events"; then exit 1; fi

for fault in KPL_TEST_DOWN_NODE=worker1 KPL_TEST_EMPTY_HISTORY=1; do
    reset_case
    export "$fault"
    reject remove-node worker-a
    no_mutation
done

reset_case
export KPL_TEST_AGENT_STOP=failed
reject remove-node worker-a
[ -e "$KPL_TEST_STATE/excluded-worker1" ]
[ ! -e "$KPL_TEST_STATE/unlabeled-worker1" ]
reject remove-node worker-a
grep -q 'cleanup is unverified' "$KPL_TEST_STATE/output"
if grep -q '^unlabel-\|^include-' "$KPL_TEST_STATE/events"; then exit 1; fi

reset_case
export KPL_TEST_OLD_FAILED=1
reject remove-node worker-a
no_stack_removal
# A later clean task cannot prove that an older failed Agent cleaned its peers.
grep -q 'oldA1' "$KPL_TEST_STATE/output"
reject remove-node worker-a
grep -q 'oldA1' "$KPL_TEST_STATE/output"

reset_case
: > "$KPL_TEST_STATE/excluded-worker1"
: > "$KPL_TEST_STATE/unlabeled-worker1"
run add-node worker-a
printf 'label-worker1\ninclude-worker1\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"
[ ! -e "$KPL_TEST_STATE/excluded-worker1" ]
[ ! -e "$KPL_TEST_STATE/unlabeled-worker1" ]

reset_case
run deploy worker-a
printf 'label-worker1\nstack-deploy\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"
[ "$(cat "$KPL_TEST_STATE/deploy-images")" = "$KPL_IMAGE" ]
if grep -q '^pull ' "$KPL_TEST_STATE/calls"; then exit 1; fi

reset_case
export KPL_IMAGE=registry.example/kpl@sha256:abcd KPL_TEST_NO_NETWORK=1
reject deploy worker-a
no_mutation
grep -q 'sha256' "$KPL_TEST_STATE/output"
if grep -q '^pull ' "$KPL_TEST_STATE/calls"; then exit 1; fi

# Explicit digests, including tag@digest references, are already immutable.
reset_case
export KPL_IMAGE=registry.example:5000/team/kpl:v3@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
run deploy worker-a
[ "$(cat "$KPL_TEST_STATE/deploy-images")" = "$KPL_IMAGE" ]
if grep -q '^pull \|^image inspect\|^inspect --format' "$KPL_TEST_STATE/calls"; then exit 1; fi

# Reusing the same mutable tag must resolve anew on every deploy. Registry
# ports and nested repository paths survive tag stripping, and config bytes
# remain unchanged. Deliberately set a platform override that pull must unset.
reset_case
unset KPL_IMAGE
tag=registry.example:5000/team/kpl:v3
printf 'KPL_IMAGE=%s\n' "$tag" > "$scratch/tag.env"
cp "$scratch/tag.env" "$scratch/tag-original.env"
export DOCKER_DEFAULT_PLATFORM=linux/arm64
export KPL_TEST_PULL_DIGEST=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
sh "$root/scripts/swarm.sh" --env-file "$scratch/tag.env" deploy worker-a > "$KPL_TEST_STATE/output" 2>&1
grep -Fxq -- "-s TERM -k 5 300 docker pull $tag" "$KPL_TEST_STATE/timeouts"
export KPL_TEST_PULL_DIGEST=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
sh "$root/scripts/swarm.sh" --env-file "$scratch/tag.env" deploy worker-a > "$KPL_TEST_STATE/output" 2>&1
printf 'registry.example:5000/team/kpl@sha256:%s\nregistry.example:5000/team/kpl@sha256:%s\n' aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/deploy-images"
cmp "$scratch/tag-original.env" "$scratch/tag.env"
[ "$(grep -Fc "pull $tag" "$KPL_TEST_STATE/calls")" = 2 ]
if grep -q '^image inspect\|^inspect --format\|--platform' "$KPL_TEST_STATE/calls"; then exit 1; fi
grep -q 'stack deploy .*--resolve-image always' "$KPL_TEST_STATE/calls"

# A failed or ambiguous pull cannot change placement, create the network, or
# submit a stack, even when a well-formed digest also appears in its output.
for fault in fail missing malformed uppercase multiple mixed trailing; do
    reset_case
    export KPL_IMAGE=registry.example/kpl:latest KPL_TEST_NO_NETWORK=1 KPL_TEST_PULL_FAULT=$fault
    reject deploy worker-a
    no_mutation
    [ "$(grep -c '^pull ' "$KPL_TEST_STATE/calls")" = 1 ]
    if grep -q '^network \|^node update \|^stack deploy ' "$KPL_TEST_STATE/calls"; then exit 1; fi
done

# Pull timeout accepts literal config values and honors environment overrides.
reset_case
unset KPL_IMAGE
printf 'KPL_IMAGE=registry.example/kpl:v3\nKPL_IMAGE_PULL_TIMEOUT=17\n' > "$scratch/pull-timeout.env"
cp "$scratch/pull-timeout.env" "$scratch/pull-timeout-original.env"
sh "$root/scripts/swarm.sh" --env-file "$scratch/pull-timeout.env" deploy > "$KPL_TEST_STATE/output" 2>&1
grep -Fxq -- '-s TERM -k 5 17 docker pull registry.example/kpl:v3' "$KPL_TEST_STATE/timeouts"
export KPL_IMAGE_PULL_TIMEOUT=23
sh "$root/scripts/swarm.sh" --env-file "$scratch/pull-timeout.env" deploy > "$KPL_TEST_STATE/output" 2>&1
grep -Fxq -- '-s TERM -k 5 23 docker pull registry.example/kpl:v3' "$KPL_TEST_STATE/timeouts"
cmp "$scratch/pull-timeout-original.env" "$scratch/pull-timeout.env"

for invalid_timeout in 0 -1 1.5 08 999999999999999999999999999; do
    reset_case
    export KPL_IMAGE_PULL_TIMEOUT=$invalid_timeout KPL_IMAGE=registry.example/kpl:v3
    reject deploy worker-a
    no_mutation
    [ ! -s "$KPL_TEST_STATE/calls" ]
done

# Public check and deploy reject host-port conflicts before any Docker query or
# deployment mutation, including conflicts introduced by multiple overrides.
for command in check deploy; do
    reset_case
    export KPL_AGENT_METRICS_PORT=9090
    reject "$command"
    grep -q 'KPL_AGENT_METRICS_PORT conflicts with PROMETHEUS_PORT' "$KPL_TEST_STATE/output"
    no_mutation
    [ ! -s "$KPL_TEST_STATE/calls" ]

    reset_case
    export KPL_AGENT_METRICS_PORT=18080 KPL_HTTP_PORT=18080
    reject "$command"
    grep -q 'KPL_AGENT_METRICS_PORT conflicts with KPL_HTTP_PORT' "$KPL_TEST_STATE/output"
    no_mutation
    [ ! -s "$KPL_TEST_STATE/calls" ]
done

reset_case
unset KPL_PASSWORD
literal='$(touch "'"$scratch"'/executed")'
printf 'KPL_PASSWORD=%s\n' "$literal" > "$scratch/literal.env"
sh "$root/scripts/swarm.sh" --env-file "$scratch/literal.env" deploy > "$KPL_TEST_STATE/output" 2>&1
[ ! -e "$scratch/executed" ]
[ "$(cat "$KPL_TEST_STATE/config-password")" = "$literal" ]
if grep -Fq "$literal" "$KPL_TEST_STATE/output"; then exit 1; fi
# Explicit environment values must win over config values without evaluating
# either source. This also exercises a config file with no trailing newline.
printf 'KPL_PASSWORD=from-file' > "$scratch/override.env"
export KPL_PASSWORD=from-environment
sh "$root/scripts/swarm.sh" --env-file "$scratch/override.env" deploy > "$KPL_TEST_STATE/output" 2>&1
[ "$(cat "$KPL_TEST_STATE/config-password")" = from-environment ]

reset_case
if sh "$root/scripts/swarm.sh" --env-file "$scratch/missing.env" remove > "$KPL_TEST_STATE/output" 2>&1; then exit 1; fi
no_mutation
[ ! -s "$KPL_TEST_STATE/calls" ]

# Bare deploy still leaves placement labels unchanged, while explicit aliases
# and IDs that resolve to the same node must cause only one placement update.
reset_case
run deploy
printf 'stack-deploy\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"
for command in deploy add-node remove-node; do
    reset_case
    run "$command" worker-a worker1 worker-a
    case "$command" in
        deploy) printf 'label-worker1\nstack-deploy\n' > "$scratch/expected" ;;
        add-node) printf 'label-worker1\ninclude-worker1\n' > "$scratch/expected" ;;
        remove-node) printf 'exclude-worker1\nclean-taskA1\nunlabel-worker1\ninclude-worker1\n' > "$scratch/expected" ;;
    esac
    cmp "$scratch/expected" "$KPL_TEST_STATE/events"
done

# --all includes Managers, but admission skips unsupported OS/state/availability
# with an explanation. These skips must not silently become placement updates.
for command in deploy add-node; do
    reset_case
    export KPL_TEST_NODE_IDS='control1 manager2 worker1 worker2 windows1 down1 paused1 drained1'
    run "$command" --all
    for node in control1 manager2 worker1 worker2; do
        [ "$(grep -c "^label-$node$" "$KPL_TEST_STATE/events")" = 1 ]
    done
    for node in windows1 down1 paused1 drained1; do
        grep -q "Skipping node $node:" "$KPL_TEST_STATE/output"
        if grep -q "^label-$node$\|^include-$node$" "$KPL_TEST_STATE/events"; then exit 1; fi
    done
    if [ "$command" = deploy ]; then
        [ "$(tail -n 1 "$KPL_TEST_STATE/events")" = stack-deploy ]
    else
        for node in control1 manager2 worker1 worker2; do
            [ "$(grep -c "^include-$node$" "$KPL_TEST_STATE/events")" = 1 ]
        done
    fi
done

# The Docker context's actual Manager is self, even when configuration pins the
# Controller to another Manager. --workers filters Spec.Role, not hostnames.
reset_case
export KPL_TEST_SELF_ID=manager2 KPL_TEST_NODE_IDS='control1 manager2 worker1'
run add-node --all-excluding-self
printf 'label-control1\nlabel-worker1\ninclude-control1\ninclude-worker1\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"
grep -Fq 'info --format {{.Swarm.NodeID}}' "$KPL_TEST_STATE/calls"

reset_case
export KPL_TEST_NODE_IDS='control1 manager2 worker1 worker2'
run add-node --workers
printf 'label-worker1\nlabel-worker2\ninclude-worker1\ninclude-worker2\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"

# Auto removal must start from this stack's placement labels. An unlabeled or
# differently labeled node must not be excluded, even if it is otherwise ready.
reset_case
export KPL_TEST_NODE_IDS='control1 worker1 worker2 unlabeled1 down1'
export KPL_TEST_LABEL_NODES=worker1
run remove-node --all
printf 'exclude-worker1\nclean-taskA1\nunlabel-worker1\ninclude-worker1\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"
grep -q 'node ls --quiet --filter node.label=kpl.lab.agent=true' "$KPL_TEST_STATE/calls"
if grep -q 'node inspect .* unlabeled1\|node inspect .* down1\|node.label=kpl.agent=' "$KPL_TEST_STATE/calls"; then exit 1; fi

reset_case
export KPL_TEST_SELF_ID=manager2 KPL_TEST_LABEL_NODES='control1 manager2 worker1'
run remove-node --all-excluding-self
cat > "$scratch/expected" <<'EXPECTED'
exclude-control1
exclude-worker1
clean-taskA-control1
clean-taskA1
unlabel-control1
unlabel-worker1
include-control1
include-worker1
EXPECTED
cmp "$scratch/expected" "$KPL_TEST_STATE/events"

reset_case
export KPL_TEST_LABEL_NODES='control1 worker1' KPL_TEST_DOWN_NODE=control1
run remove-node --workers
printf 'exclude-worker1\nclean-taskA1\nunlabel-worker1\ninclude-worker1\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"

# A selected labeled node cannot be silently skipped during cleanup. Validate
# the complete selected set before excluding even its healthy first member.
for invalid_node in down1 windows1 paused1 drained1; do
    reset_case
    export KPL_TEST_LABEL_NODES="worker1 $invalid_node"
    reject remove-node --all
    no_mutation
    grep -q "$invalid_node" "$KPL_TEST_STATE/output"
done

# Selection queries and empty selections fail before placement/network changes.
for command in deploy add-node remove-node; do
    reset_case
    export KPL_TEST_NODE_LS_FAIL=1
    reject "$command" --all
    no_mutation
    grep -q 'mock node listing failed' "$KPL_TEST_STATE/output"

    reset_case
    export KPL_TEST_NODE_INSPECT_FAIL=worker1
    reject "$command" --all
    no_mutation
    grep -q 'mock node inspection failed' "$KPL_TEST_STATE/output"

    reset_case
    export KPL_TEST_NODE_IDS='' KPL_TEST_LABEL_NODES=''
    reject "$command" --all
    no_mutation
    grep -q 'matched no eligible nodes' "$KPL_TEST_STATE/output"
done
for selector in --workers --all-excluding-self; do
    reset_case
    export KPL_TEST_NODE_IDS=control1
    reject add-node "$selector"
    no_mutation
    grep -q 'matched no eligible nodes' "$KPL_TEST_STATE/output"
done
reset_case
export KPL_TEST_NODE_IDS='windows1 down1 paused1 drained1'
reject add-node --all
no_mutation
grep -q 'matched no eligible nodes' "$KPL_TEST_STATE/output"

# Syntax mistakes must fail before the very first Docker query, not merely
# before a later mutation. Selectors cannot be combined with one another or
# with explicit node names, including the same selector appearing twice.
for command in deploy add-node remove-node; do
    for arguments in '--all --workers' '--all --all' '--workers --all-excluding-self' '--all worker-a' 'worker-a --all' '--unknown' '--workers=1'; do
        reset_case
        # Intentional splitting: all fixtures contain literal CLI tokens.
        reject "$command" $arguments
        no_mutation
        [ ! -s "$KPL_TEST_STATE/calls" ]
    done
done
for command in init status remove; do
    reset_case
    reject "$command" --all
    no_mutation
    [ ! -s "$KPL_TEST_STATE/calls" ]
done

# Operational helpers do not require an existing application stack. Nodes and
# access inspect the current Manager without consulting service ownership.
reset_case
export KPL_TEST_EMPTY_STACK=1
run nodes
grep -q 'control1.*manager-main' "$KPL_TEST_STATE/output"
grep -Fxq 'node ls' "$KPL_TEST_STATE/calls"
if grep -q '^service ls' "$KPL_TEST_STATE/calls"; then exit 1; fi
no_mutation

reset_case
export KPL_TEST_EMPTY_STACK=1 KPL_HTTP_PORT=18080 KPL_AGENT_METRICS_PORT=19091 PROMETHEUS_PORT=19090 GRAFANA_PORT=13000
run access
for port in 18080 19090 13000; do grep -Fq "http://10.20.0.7:$port" "$KPL_TEST_STATE/output"; done
grep -Fq 'Agent metrics (worker1): http://10.20.0.11:19091/metrics' "$KPL_TEST_STATE/output"
grep -Fq 'Agent metrics (worker2): http://[fd00::12]:19091/metrics' "$KPL_TEST_STATE/output"
grep -Fq 'firewall rules' "$KPL_TEST_STATE/output"
if grep -Fq "$KPL_PASSWORD" "$KPL_TEST_STATE/output" || grep -Fq "$GRAFANA_ADMIN_PASSWORD" "$KPL_TEST_STATE/output"; then exit 1; fi
if grep -q '^service ls' "$KPL_TEST_STATE/calls"; then exit 1; fi
no_mutation

reset_case
run config
grep -q 'KPL_PASSWORD' "$KPL_TEST_STATE/output"
if grep -Fq "$KPL_PASSWORD" "$KPL_TEST_STATE/output" || grep -Fq "$GRAFANA_ADMIN_PASSWORD" "$KPL_TEST_STATE/output"; then exit 1; fi
[ ! -s "$KPL_TEST_STATE/calls" ]
no_mutation

# The check wrapper must load the requested literal config before delegating.
reset_case
printf 'KPL_AGENT_CAPACITY=37\nKPL_MIN_AGENTS=1\n' > "$scratch/check.env"
(unset KPL_AGENT_CAPACITY KPL_MIN_AGENTS; sh "$root/scripts/swarm.sh" --env-file "$scratch/check.env" check) > "$KPL_TEST_STATE/output" 2>&1
grep -q 'capacity 37' "$KPL_TEST_STATE/output"
grep -q 'eligible Agents for stack lab' "$KPL_TEST_STATE/output"
no_mutation

# Scenario printing is offline and must not load even an explicitly missing
# config. Paths with spaces are passed intact, and output is the file verbatim.
reset_case
sh "$root/scripts/swarm.sh" --env-file "$scratch/absent-scenario.env" scenario > "$KPL_TEST_STATE/output" 2>&1
cmp "$root/examples/swarm-smoke.yaml" "$KPL_TEST_STATE/output"
printf 'version: 2\nname: printed-only\n' > "$scratch/custom scenario.yaml"
run scenario "$scratch/custom scenario.yaml"
cmp "$scratch/custom scenario.yaml" "$KPL_TEST_STATE/output"
reject scenario "$scratch/no-such-scenario.yaml"
[ ! -s "$KPL_TEST_STATE/calls" ]
no_mutation

reset_case
run logs
grep -Fxq 'service logs --tail 100 --timestamps lab_controller' "$KPL_TEST_STATE/calls"
for component in agent prometheus grafana; do
    run logs "$component"
    grep -Fxq "service logs --tail 100 --timestamps lab_$component" "$KPL_TEST_STATE/calls"
done
no_mutation
run logs agent --tail 25
grep -Fxq 'service logs --tail 25 --timestamps lab_agent' "$KPL_TEST_STATE/calls"
run logs --tail all
grep -Fxq 'service logs --tail all --timestamps lab_controller' "$KPL_TEST_STATE/calls"
no_mutation

# Web files are read from the manager-verified Controller task, with raw JSONL
# stdout suitable for piping and no changes to services, containers or volumes.
web_log_fixtures() {
    mkdir "$KPL_TEST_STATE/logs"
    for component in access auth; do
        awk -v event="$component" 'BEGIN { for (i=1; i<=150; i++) printf "{\"event\":\"%s\",\"sequence\":%d}\n", event, i }' > "$KPL_TEST_STATE/logs/$component.jsonl"
    done
}
for component in access auth; do
    reset_case
    web_log_fixtures
    export DOCKER_CONTEXT=test-manager KPL_TEST_EXPECT_CONTEXT=test-manager
    run logs "$component"
    tail -n 100 "$KPL_TEST_STATE/logs/$component.jsonl" > "$scratch/expected"
    cmp "$scratch/expected" "$KPL_TEST_STATE/output"
    grep -Fxq 'service ps --quiet --no-trunc --filter desired-state=running lab_controller' "$KPL_TEST_STATE/calls"
    run logs "$component" --tail 2
    tail -n 2 "$KPL_TEST_STATE/logs/$component.jsonl" > "$scratch/expected"
    cmp "$scratch/expected" "$KPL_TEST_STATE/output"
    run logs "$component" --tail all
    cmp "$KPL_TEST_STATE/logs/$component.jsonl" "$KPL_TEST_STATE/output"
    : > "$KPL_TEST_STATE/logs/$component.jsonl"
    run logs "$component"
    [ ! -s "$KPL_TEST_STATE/output" ]
    no_mutation
done

# A remote Controller can be on a worker; only its file read uses the explicit
# context. A stale configured control node must not override actual placement.
reset_case
web_log_fixtures
export KPL_TEST_CONTROLLER_NODE=worker1 DOCKER_CONTEXT=test-manager KPL_TEST_EXPECT_CONTEXT=test-manager
reject logs access
grep -Fq 'Controller runs on Swarm node worker1' "$KPL_TEST_STATE/output"
! grep -q '^exec ' "$KPL_TEST_STATE/calls"
run logs access --context controller-node --tail 3
tail -n 3 "$KPL_TEST_STATE/logs/access.jsonl" > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/output"
grep -Fxq -- '--context controller-node info --format {{.Swarm.NodeID}}' "$KPL_TEST_STATE/calls"
grep -q '^--context controller-node exec containerC ' "$KPL_TEST_STATE/calls"
no_mutation
reset_case
export KPL_TEST_LOG_CONTEXT_NODE=worker2
reject logs auth --context controller-node
! grep -q 'exec containerC ' "$KPL_TEST_STATE/calls"
no_mutation

# Task startup/failure, ambiguous updates, missing files and daemon errors are
# explicit failures, never a fallback to another container or ordinary logs.
for fault in KPL_TEST_EMPTY_STACK=1 KPL_TEST_EMPTY_HISTORY=1 KPL_TEST_LOG_TASKS=pendingC KPL_TEST_LOG_TASKS=failedC KPL_TEST_LOG_TASKS=malformedC KPL_TEST_FOREIGN=1 KPL_TEST_INSPECT_FAIL=1; do
    reset_case
    export "$fault"
    reject logs access
    ! grep -q '^exec ' "$KPL_TEST_STATE/calls"
    no_mutation
done
reset_case
export KPL_TEST_LOG_TASKS='taskC taskC'
reject logs auth
grep -Fq 'Multiple Controller tasks are running' "$KPL_TEST_STATE/output"
! grep -q '^exec ' "$KPL_TEST_STATE/calls"
no_mutation
reset_case
web_log_fixtures
export KPL_TEST_LOG_TASKS='pendingC failedC taskC'
run logs auth --tail 1
tail -n 1 "$KPL_TEST_STATE/logs/auth.jsonl" > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/output"
no_mutation
reset_case
reject logs auth
grep -Fq 'Web log file is missing' "$KPL_TEST_STATE/output"
no_mutation
reset_case
export KPL_TEST_LOG_EXEC_FAIL=1
reject logs access
grep -Fq 'Cannot read Controller web logs' "$KPL_TEST_STATE/output"
no_mutation
reset_case
reject logs access --context nonexistent
grep -Fq 'Cannot connect to the Docker daemon selected for web logs' "$KPL_TEST_STATE/output"
no_mutation

# Login uses only the image's registry authority; Docker Hub retains Docker's
# default login flow, with no password argv and no short command timeout.
for login_image in registry.example:5000/team/kpl:v3 team/kpl:v3 docker.io/team/kpl:v3 index.docker.io/team/kpl:v3 registry-1.docker.io/team/kpl:v3; do
    reset_case
    export KPL_IMAGE=$login_image
    run login
    case "$login_image" in
        registry.example:5000/*) printf 'login registry.example:5000\n' > "$scratch/expected" ;;
        *) printf 'login\n' > "$scratch/expected" ;;
    esac
    cmp "$scratch/expected" "$KPL_TEST_STATE/calls"
    [ ! -s "$KPL_TEST_STATE/timeouts" ]
done

# Native publish always builds the repository root in the caller's Docker
# context, even when invoked from another directory. Platform defaults do not
# leak into either stage, and the two operations have independent timeouts.
reset_case
export KPL_IMAGE=registry.example:5000/team/kpl:v3
export DOCKER_CONTEXT=test-remote-context KPL_TEST_EXPECT_CONTEXT=test-remote-context DOCKER_DEFAULT_PLATFORM=linux/arm64
(cd "$scratch"; sh "$root/scripts/swarm.sh" --env-file "$scratch/config.env" publish) > "$KPL_TEST_STATE/output" 2>&1
printf 'image-build\nimage-push\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"
grep -Fxq -- "-s TERM -k 5 1800 docker build --tag $KPL_IMAGE $root" "$KPL_TEST_STATE/timeouts"
grep -Fxq -- "-s TERM -k 5 600 docker push $KPL_IMAGE" "$KPL_TEST_STATE/timeouts"
[ "$(wc -l < "$KPL_TEST_STATE/calls")" = 2 ]

reset_case
export KPL_IMAGE=registry.example/kpl:v3 KPL_IMAGE_BUILD_TIMEOUT=41 KPL_IMAGE_PUSH_TIMEOUT=43
run publish
grep -Fxq -- "-s TERM -k 5 41 docker build --tag $KPL_IMAGE $root" "$KPL_TEST_STATE/timeouts"
grep -Fxq -- "-s TERM -k 5 43 docker push $KPL_IMAGE" "$KPL_TEST_STATE/timeouts"

reset_case
export KPL_IMAGE=registry.example/kpl:v3 KPL_TEST_BUILD_FAIL=1
reject publish
printf 'image-build\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"
if grep -q '^push \|^service \|^network \|^stack ' "$KPL_TEST_STATE/calls"; then exit 1; fi

reset_case
export KPL_IMAGE=registry.example/kpl:v3 KPL_TEST_PUSH_FAIL=1
reject publish
printf 'image-build\nimage-push\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"
[ "$(wc -l < "$KPL_TEST_STATE/calls")" = 2 ]

reset_case
reject publish
no_mutation
[ ! -s "$KPL_TEST_STATE/calls" ]

reset_case
export KPL_IMAGE=registry.example/kpl:v3 DOCKER_CONTEXT=test-remote-context KPL_TEST_EXPECT_CONTEXT=test-remote-context DOCKER_DEFAULT_PLATFORM=linux/arm64
run publish --platforms linux/amd64,linux/arm64
printf 'image-buildx\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"
grep -Fxq "buildx build --platform linux/amd64,linux/arm64 --tag $KPL_IMAGE --push $root" "$KPL_TEST_STATE/calls"
[ "$(wc -l < "$KPL_TEST_STATE/calls")" = 1 ]

for platforms in '' linux 'linux/amd64,' ',linux/arm64' 'linux/amd64,,linux/arm64' 'linux/amd64 linux/arm64' 'linux/amd64;echo'; do
    reset_case
    export KPL_IMAGE=registry.example/kpl:v3
    reject publish --platforms "$platforms"
    no_mutation
    [ ! -s "$KPL_TEST_STATE/calls" ]
done

# An arbitrary config within the Docker build context must never be sent to
# the daemon. Use a scratch repository so readonly source mounts stay intact.
reset_case
export KPL_IMAGE=registry.example/kpl:v3
publish_project=$scratch/leaky-project
mkdir -p "$publish_project/scripts"
cp "$root/scripts/swarm.sh" "$root/scripts/swarm-config.sh" "$root/scripts/check-swarm.sh" "$publish_project/scripts/"
cp "$root/Dockerfile" "$root/.dockerignore" "$publish_project/"
printf 'KPL_IMAGE=registry.example/kpl:v3\nKPL_PASSWORD=context-secret\n' > "$publish_project/private.conf"
export KPL_TEST_REPO_ROOT=$publish_project
if sh "$publish_project/scripts/swarm.sh" --env-file "$publish_project/private.conf" publish > "$KPL_TEST_STATE/output" 2>&1; then exit 1; fi
no_mutation
[ ! -s "$KPL_TEST_STATE/calls" ]

# An external symlink must not disguise an in-context config file. These
# fixtures contain dummy values only, and no real repository file is changed.
reset_case
export KPL_IMAGE=registry.example/kpl:v3 KPL_TEST_REPO_ROOT=$publish_project
ln -s "$publish_project/private.conf" "$scratch/linked-publish.env"
if sh "$publish_project/scripts/swarm.sh" --env-file "$scratch/linked-publish.env" publish > "$KPL_TEST_STATE/output" 2>&1; then exit 1; fi
no_mutation
[ ! -s "$KPL_TEST_STATE/calls" ]

# Docker trims ignore patterns before applying negations. Leading whitespace
# must not hide a rule that includes the config again in the build context.
reset_case
export KPL_IMAGE=registry.example/kpl:v3 KPL_TEST_REPO_ROOT=$publish_project
printf 'KPL_IMAGE=registry.example/kpl:v3\nKPL_PASSWORD=dummy-negated-config\n' > "$publish_project/.env.swarm"
printf '.env.*\n  !.env.swarm\n' > "$publish_project/.dockerignore"
if sh "$publish_project/scripts/swarm.sh" --env-file "$publish_project/.env.swarm" publish > "$KPL_TEST_STATE/output" 2>&1; then exit 1; fi
no_mutation
[ ! -s "$KPL_TEST_STATE/calls" ]
cp "$root/.dockerignore" "$publish_project/.dockerignore"

# Dockerfile.dockerignore takes precedence over the root ignore file. A root
# .env exclusion therefore cannot prove that this build excludes credentials.
reset_case
export KPL_IMAGE=registry.example/kpl:v3 KPL_TEST_REPO_ROOT=$publish_project
printf 'KPL_IMAGE=registry.example/kpl:v3\nKPL_PASSWORD=dummy-ignored-config\n' > "$publish_project/.env.swarm"
printf '# This override does not exclude configuration files.\n' > "$publish_project/Dockerfile.dockerignore"
if sh "$publish_project/scripts/swarm.sh" --env-file "$publish_project/.env.swarm" publish > "$KPL_TEST_STATE/output" 2>&1; then exit 1; fi
no_mutation
[ ! -s "$KPL_TEST_STATE/calls" ]

# Explicit subnet creation preserves CIDR argv, and an existing network must
# match exactly before any placement or stack updates are made.
reset_case
export KPL_PEER_SUBNET=10.60.0.0/24 KPL_TEST_NO_NETWORK=1
run deploy worker-a
grep -q 'network create .*--subnet 10.60.0.0/24 lab-peers$' "$KPL_TEST_STATE/calls"
printf 'network-create\nlabel-worker1\nstack-deploy\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"

reset_case
export KPL_PEER_SUBNET=10.60.0.0/24 KPL_TEST_NETWORK_SUBNET=10.60.0.0/24
run deploy worker-a
grep -Fq 'network inspect --format {{range .IPAM.Config}}{{println .Subnet}}{{end}} lab-peers' "$KPL_TEST_STATE/calls"
printf 'label-worker1\nstack-deploy\n' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/events"

for actual_subnet in 10.61.0.0/24 '10.60.0.0/24
10.61.0.0/24'; do
    reset_case
    export KPL_PEER_SUBNET=10.60.0.0/24 KPL_TEST_NETWORK_SUBNET=$actual_subnet
    reject deploy worker-a
    no_mutation
done
for invalid_subnet in 10.60.0.0/99 999.1.1.0/24 not-a-cidr 10.60.0.0; do
    reset_case
    export KPL_PEER_SUBNET=$invalid_subnet KPL_TEST_NO_NETWORK=1
    reject deploy worker-a
    no_mutation
    if grep -q '^network create\|^node update\|^stack deploy' "$KPL_TEST_STATE/calls"; then exit 1; fi
done

# Prometheus scrape and self links use the control-node address and published
# ports, consistently during Compose validation and deployment (including IPv6).
reset_case
run deploy worker-a
printf '%s\n' 'http://10.20.0.7:8080/metrics' 'http://10.20.0.7:9090/' > "$scratch/expected"
cmp "$scratch/expected" "$KPL_TEST_STATE/config-public-urls"
cmp "$scratch/expected" "$KPL_TEST_STATE/deploy-public-urls"
for address in 10.30.0.8 fd00::7; do
    reset_case
    export KPL_TEST_CONTROL_ADDR=$address KPL_HTTP_PORT=18080 PROMETHEUS_PORT=19090
    run deploy worker-a
    case "$address" in *:*) address=[$address] ;; esac
    printf 'http://%s:18080/metrics\nhttp://%s:19090/\n' "$address" "$address" > "$scratch/expected"
    cmp "$scratch/expected" "$KPL_TEST_STATE/config-public-urls"
    cmp "$scratch/expected" "$KPL_TEST_STATE/deploy-public-urls"
done
reset_case
export KPL_TEST_CONTROL_ADDR='bad/address'
reject deploy worker-a
no_mutation
[ ! -e "$KPL_TEST_STATE/config-public-urls" ]

# New helper flags and arity are also validated before contacting Docker.
for arguments in 'nodes --all' 'login unexpected' 'publish --platforms' 'publish --unknown' 'publish --platforms linux/amd64 extra' 'check extra' 'access extra' 'logs unknown' 'logs --follow' 'logs access --tail' 'logs auth --tail -1' 'logs auth --tail 0' 'logs auth --tail 01' 'logs auth --tail 1x' 'logs access --tail 999999999999999999999999999' 'logs access --context' 'logs access --context --tail' 'logs controller --context controller-node' 'logs auth extra' 'scenario --unknown'; do
    reset_case
    # Intentional splitting of fixed argument fixtures.
    reject $arguments
    no_mutation
    [ ! -s "$KPL_TEST_STATE/calls" ]
done

reset_case
(unset KPL_IMAGE KPL_PASSWORD GRAFANA_ADMIN_PASSWORD; sh "$root/scripts/swarm.sh" --env-file "$scratch/generated.env" init) > "$KPL_TEST_STATE/output" 2>&1
[ "$(stat -c '%a' "$scratch/generated.env")" = 600 ]
grep -Eq '^KPL_PASSWORD=[a-f0-9]{64}$' "$scratch/generated.env"
grep -Eq '^GRAFANA_ADMIN_PASSWORD=[a-f0-9]{64}$' "$scratch/generated.env"
grep -Fxq 'KPL_IMAGE=registry.example.com/kpl-v3:v3' "$scratch/generated.env"
grep -Fxq 'KPL_IMAGE_PULL_TIMEOUT=300' "$scratch/generated.env"
cp "$scratch/generated.env" "$scratch/original.env"
if sh "$root/scripts/swarm.sh" --env-file "$scratch/generated.env" init > "$KPL_TEST_STATE/output" 2>&1; then exit 1; fi
cmp "$scratch/original.env" "$scratch/generated.env"
no_mutation

printf '%s\n' 'PASS: Manager commands enforce direct stack removal, verified node cleanup, stack ownership, safe node selectors, deduplicated placement, literal config, and fresh tag-to-digest resolution before deployment mutations.'

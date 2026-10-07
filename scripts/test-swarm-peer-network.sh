#!/bin/sh
# Network maintenance regression checks against a fake Docker executable only.
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'result=$?; if [ "$result" -ne 0 ]; then cat "$scratch/output" "$scratch/calls" >&2; fi; rm -rf "$scratch"; exit "$result"' EXIT
mkdir "$scratch/bin"
cat > "$scratch/bin/docker" <<'MOCK'
#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$KPL_RESET_TEST_DIR/calls"
for last do :; done
case "$1 ${2:-}" in
    'info --format') printf '%s\n' "${KPL_RESET_TEST_MANAGER:-active true}" ;;
    'service ls')
        [ "${KPL_RESET_TEST_FAULT:-}" != service-query ] || exit 1
        n=$(cat "$KPL_RESET_TEST_DIR/service-count")
        n=$((n+1)); printf '%s\n' "$n" > "$KPL_RESET_TEST_DIR/service-count"
        if [ "${KPL_RESET_TEST_FAULT:-}" = services ] || { [ "${KPL_RESET_TEST_FAULT:-}" = services-race ] && [ "$n" -ge 2 ]; }; then printf 'service1\n'; fi ;;
    'node ls')
        [ "${KPL_RESET_TEST_FAULT:-}" != node-query ] || exit 1
        [ "${KPL_RESET_TEST_FAULT:-}" != empty-nodes ] || exit 0
        printf 'manager1\nworker1\n' ;;
    'node inspect')
        [ "${KPL_RESET_TEST_FAULT:-}" != node-inspect ] || exit 1
        case "${KPL_RESET_TEST_FAULT:-}" in
            offline) printf 'ready\ndown\n' ;;
            incomplete-nodes) printf 'ready\n' ;;
            *) printf 'ready\nready\n' ;;
        esac ;;
    'network inspect')
        case "$4" in
            '{{.Id}}')
                [ "${KPL_RESET_TEST_FAULT:-}" != missing ] || exit 1
                printf 'oldnetworkid\n' ;;
            *'json .Subnet'*)
                [ "${KPL_RESET_TEST_FAULT:-}" != ipam-query ] || exit 1
                ipam=${KPL_RESET_TEST_IPAM:-'"10.60.0.0/24"|"10.60.0.1"|""|0'}
                if [ "$last" = newnetworkid ] && [ "${KPL_RESET_TEST_FAULT:-}" = new-ipam ]; then ipam='"10.61.0.0/24"|"10.61.0.1"|""|0'; fi
                printf '%s\n' "$ipam" ;;
            *'.ConfigOnly'*)
                [ "${KPL_RESET_TEST_FAULT:-}" != metadata-query ] || exit 1
                metadata=${KPL_RESET_TEST_METADATA:-'lab-peers|overlay|swarm|true|false|false|false|false||default|0|1|0|kp2plab-v3|lab||'}
                n=$(cat "$KPL_RESET_TEST_DIR/metadata-count")
                n=$((n+1)); printf '%s\n' "$n" > "$KPL_RESET_TEST_DIR/metadata-count"
                if { [ "${KPL_RESET_TEST_FAULT:-}" = endpoint-race ] && [ "$n" -ge 2 ]; } || { [ "$last" = newnetworkid ] && [ "${KPL_RESET_TEST_FAULT:-}" = new-metadata ]; }; then metadata=changed; fi
                printf '%s\n' "$metadata" ;;
            *) exit 97 ;;
        esac ;;
    'network rm')
        [ "$#" -eq 3 ] && [ "$3" = oldnetworkid ] || exit 97
        if [ "${KPL_RESET_TEST_FAULT:-}" = remove-timeout ]; then sleep 10; fi
        [ "${KPL_RESET_TEST_FAULT:-}" != remove ] || { printf 'network in use by remote task\n' >&2; exit 1; }
        printf 'oldnetworkid\n' ;;
    'network create')
        if [ "${KPL_RESET_TEST_FAULT:-}" = create-timeout ]; then sleep 10; fi
        [ "${KPL_RESET_TEST_FAULT:-}" != create ] || exit 1
        printf '%s\n' "${KPL_RESET_TEST_NEW_ID:-newnetworkid}" ;;
    *) printf 'Unexpected Docker call: %s\n' "$*" >&2; exit 97 ;;
esac
MOCK
chmod +x "$scratch/bin/docker"
export PATH="$scratch/bin:$PATH" KPL_RESET_TEST_DIR="$scratch"
export KPL_STACK_NAME=lab KPL_PEER_NETWORK=lab-peers KPL_PEER_SUBNET='' KPL_DOCKER_TIMEOUT=10
printf '%s\n' 'KPL_STACK_NAME=lab' 'KPL_PEER_NETWORK=lab-peers' > "$scratch/config.env"
cp "$scratch/config.env" "$scratch/original.env"

reset_case() {
    unset KPL_RESET_TEST_FAULT KPL_RESET_TEST_MANAGER KPL_RESET_TEST_METADATA KPL_RESET_TEST_IPAM KPL_RESET_TEST_NEW_ID
    KPL_PEER_SUBNET=''
    KPL_DOCKER_TIMEOUT=10
    : > "$scratch/calls"
    printf '0\n' > "$scratch/service-count"
    printf '0\n' > "$scratch/metadata-count"
}
run_reset() { sh "$root/scripts/swarm.sh" --env-file "$scratch/config.env" reset-peer-network "$@" > "$scratch/output" 2>&1; }
reject() { if run_reset "$@"; then printf 'Expected network reset failure\n' >&2; exit 1; fi; }
no_mutation() { if grep -Eq '^network (rm|create) ' "$scratch/calls"; then printf 'Unexpected network mutation\n' >&2; exit 1; fi; }

reset_case
run_reset
grep -Fxq 'network rm oldnetworkid' "$scratch/calls"
grep -Fxq 'network create --driver overlay --attachable --label io.kpl.application=kp2plab-v3 --label io.kpl.stack=lab --subnet 10.60.0.0/24 --gateway 10.60.0.1 lab-peers' "$scratch/calls"
grep -Fq 'Peer network recreated with new ID newnetworkid.' "$scratch/output"
cmp "$scratch/config.env" "$scratch/original.env"

reset_case
KPL_PEER_SUBNET=10.60.0.0/24
run_reset

reset_case
export KPL_RESET_TEST_IPAM='"10.60.0.0/24"|""|""|0'
run_reset
grep -Fxq 'network create --driver overlay --attachable --label io.kpl.application=kp2plab-v3 --label io.kpl.stack=lab --subnet 10.60.0.0/24 lab-peers' "$scratch/calls"

for fault in services service-query node-query node-inspect empty-nodes offline incomplete-nodes missing metadata-query ipam-query services-race endpoint-race; do
    reset_case
    export KPL_RESET_TEST_FAULT=$fault
    reject
    no_mutation
done
reset_case
export KPL_RESET_TEST_MANAGER='active false'
reject
no_mutation
reset_case
reject --force
[ ! -s "$scratch/calls" ]
reset_case
KPL_PEER_SUBNET=10.61.0.0/24
reject
no_mutation

# Ownership, attached containers, and settings we cannot reconstruct must all
# fail before deletion, including custom labels/options and nondefault IPAM.
for metadata in \
    'other|overlay|swarm|true|false|false|false|false||default|0|1|0|kp2plab-v3|lab||' \
    'lab-peers|bridge|local|true|false|false|false|false||default|0|1|0|kp2plab-v3|lab||' \
    'lab-peers|overlay|swarm|true|false|false|false|false||default|0|1|1|kp2plab-v3|lab||' \
    'lab-peers|overlay|swarm|true|false|false|false|false||default|0|1|0|kp2plab-v3|other||' \
    'lab-peers|overlay|swarm|true|false|false|false|false||default|0|1|0|kp2plab-v3|lab|custom|' \
    'lab-peers|overlay|swarm|true|false|false|false|false||default|0|1|0|kp2plab-v3|lab||custom' \
    'lab-peers|overlay|swarm|true|false|false|false|true||default|0|2|0|kp2plab-v3|lab||' \
    'lab-peers|overlay|swarm|true|false|false|false|false||custom|0|1|0|kp2plab-v3|lab||'; do
    reset_case
    export KPL_RESET_TEST_METADATA=$metadata
    reject
    no_mutation
done
for ipam in \
    '"10.60.0.0/24"|"10.60.0.1"|"10.60.0.0/25"|0' \
    '"10.60.0.0/24"|"10.60.0.1"|""|1' \
    '"10.60.0.2/24"|"10.60.0.1"|""|0' \
    '""|"10.60.0.1"|""|0' \
    '"10.60.0.0/24"|"10.60.0.999"|""|0' \
    '"10.60.0.0/24"|"10.61.0.1"|""|0' \
    '"10.60.0.0/24"|"10.60.0.0"|""|0' \
    '"10.60.0.0/24"|"10.60.0.255"|""|0' \
    '"10.60.0.0/24"|"$(touch sentinel)"|""|0'; do
    reset_case
    export KPL_RESET_TEST_IPAM=$ipam
    reject
    no_mutation
done
reset_case
export KPL_RESET_TEST_FAULT=remove
reject
if grep -q '^network create ' "$scratch/calls"; then exit 1; fi
for fault in create new-ipam new-metadata; do
    reset_case
    export KPL_RESET_TEST_FAULT=$fault
    reject
    grep -Fq 'Recovery command (only if the network is absent): docker network create' "$scratch/output"
    if grep -q '^Peer network recreated' "$scratch/output"; then exit 1; fi
done
for fault in remove-timeout create-timeout; do
    reset_case
    KPL_DOCKER_TIMEOUT=1
    export KPL_RESET_TEST_FAULT=$fault
    reject
    if [ "$fault" = remove-timeout ] && grep -q '^network create ' "$scratch/calls"; then exit 1; fi
    grep -Fq 'unconfirmed' "$scratch/output"
done
for newid in oldnetworkid 'invalid id' ''; do
    reset_case
    # Empty output is tested with a newline, bypassing the mock's :- default.
    if [ -z "$newid" ]; then newid='
'; fi
    export KPL_RESET_TEST_NEW_ID=$newid
    reject
done
printf '%s\n' 'PASS: Peer network reset preserves allocation, refuses active/foreign/custom networks, checks races, and reports incomplete recovery.'

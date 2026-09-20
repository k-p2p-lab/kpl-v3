#!/bin/sh
# Fake Docker regression tests; no daemon socket, registry or cluster changes.
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
mkdir "$scratch/bin"
export KPL_TEST_CALLS="$scratch/calls"
cat > "$scratch/bin/docker" <<'MOCK_DOCKER'
#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$KPL_TEST_CALLS"
case "$1 $2" in
    'info --format') printf '%s\n' "${KPL_TEST_MANAGER:-active true}" ;;
    'network inspect')
        case "$4" in
            '{{range .IPAM.Config}}{{println .Subnet}}{{end}}') printf '%s\n' "${KPL_TEST_SUBNET:-10.11.0.0/24}" ;;
            *) printf '%s\n' "${KPL_TEST_NETWORK:-kpl-swarm-peers overlay true}" ;;
        esac ;;
    'node ls')
        [ "$3 $4" = '--quiet --filter' ]
        case "$5" in
            node.label=kpl.kpl.agent=true) printf 'worker1\nworker2\nworker3\n' ;;
            node.label=kpl.other-stack_2.agent=true) printf 'other1\nother2\n' ;;
            node.label=kpl.legacy-only.agent=true) : ;;
            # These legacy nodes are eligible but must never be adopted by a
            # stack that has no explicit stack-scoped placement labels.
            node.label=kpl.agent=true) printf 'legacy1\nlegacy2\n' ;;
            *) printf 'Unexpected node label: %s\n' "$5" >&2; exit 2 ;;
        esac ;;
    'node inspect')
        if [ "$5" = control1 ]; then
            printf '%s\n' "${KPL_TEST_CONTROL:-control1 linux ready active}"
        else
            case "$5" in
                worker1)
                    [ "$5 $6 $7" = 'worker1 worker2 worker3' ]
                    printf '%s\n' "${KPL_TEST_AGENTS:-linux ready active
linux ready active
linux down drain}" ;;
                other1)
                    [ "$5 $6" = 'other1 other2' ]
                    printf 'linux ready active\nlinux ready active\n' ;;
                legacy1)
                    [ "$5 $6" = 'legacy1 legacy2' ]
                    printf 'linux ready active\nlinux ready active\n' ;;
                *) printf 'Unexpected node inventory: %s\n' "$*" >&2; exit 2 ;;
            esac
        fi ;;
    *) printf 'Unexpected Docker mutation/query: %s\n' "$*" >&2; exit 2 ;;
esac
MOCK_DOCKER
chmod +x "$scratch/bin/docker"
export PATH="$scratch/bin:$PATH"
export KPL_CONTROL_NODE_ID=control1
export KPL_IMAGE=registry.example/kpl:v3@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
export KPL_USER=admin KPL_PASSWORD=must-not-appear-in-output
unset KPL_PEER_NETWORK KPL_PEER_SUBNET KPL_AGENT_CAPACITY KPL_AGENT_METRICS_PORT KPL_STACK_NAME KPL_MIN_AGENTS KPL_IMAGE_PULL_TIMEOUT KPL_IMAGE_BUILD_TIMEOUT KPL_IMAGE_PUSH_TIMEOUT

sh "$root/scripts/check-swarm.sh" > "$scratch/output"
grep -q '2 eligible Agents' "$scratch/output"
grep -q 'capacity 20' "$scratch/output"
grep -q 'Agent metrics port 9091' "$scratch/output"
grep -q 'for stack kpl' "$scratch/output"
grep -q 'does not validate' "$scratch/output"
grep -q 'host-port availability' "$scratch/output"
[ "$(wc -l < "$KPL_TEST_CALLS")" -eq 5 ]
grep -q 'node.label=kpl.kpl.agent=true' "$KPL_TEST_CALLS"
if grep -q "$KPL_PASSWORD" "$scratch/output" "$KPL_TEST_CALLS"; then exit 1; fi

reject() {
    if env "$@" sh "$root/scripts/check-swarm.sh" > "$scratch/output" 2>&1; then
        printf 'Unexpected acceptance: %s\n' "$*" >&2
        exit 1
    fi
}
env KPL_STACK_NAME=other-stack_2 sh "$root/scripts/check-swarm.sh" > "$scratch/output"
grep -q '2 eligible Agents for stack other-stack_2' "$scratch/output"
grep -q 'node.label=kpl.other-stack_2.agent=true' "$KPL_TEST_CALLS"
grep -q 'other1 other2' "$KPL_TEST_CALLS"
reject KPL_STACK_NAME=legacy-only
grep -q 'kpl.legacy-only.agent=true' "$scratch/output"
if grep -q 'node.label=kpl.agent=true' "$KPL_TEST_CALLS"; then
    printf '%s\n' 'Preflight queried legacy global Agent labels.' >&2
    exit 1
fi
for invalid_stack in Kpl -kpl _kpl .kpl kpl.name '../kpl' 'two stacks' 'kpl:one'; do
    calls_before=$(wc -l < "$KPL_TEST_CALLS")
    reject "KPL_STACK_NAME=$invalid_stack"
    grep -q 'KPL_STACK_NAME must start' "$scratch/output"
    [ "$(wc -l < "$KPL_TEST_CALLS")" -eq "$calls_before" ]
done
env KPL_MIN_AGENTS=1 KPL_TEST_AGENTS='linux ready active
linux down active
windows ready active' sh "$root/scripts/check-swarm.sh" > "$scratch/output"
grep -q '1 eligible Agents for stack kpl' "$scratch/output"
reject KPL_MIN_AGENTS=3
grep -q 'At least 3 kpl.kpl.agent=true' "$scratch/output"
for invalid_minimum in 0 -1 1.5 08 999999999999999999999999999; do
    calls_before=$(wc -l < "$KPL_TEST_CALLS")
    reject "KPL_MIN_AGENTS=$invalid_minimum"
    grep -q 'KPL_MIN_AGENTS' "$scratch/output"
    [ "$(wc -l < "$KPL_TEST_CALLS")" -eq "$calls_before" ]
done
for invalid_port in 0 -1 1.5 09091 65536 999999999999999999999999999; do
    calls_before=$(wc -l < "$KPL_TEST_CALLS")
    reject "KPL_AGENT_METRICS_PORT=$invalid_port"
    grep -q 'KPL_AGENT_METRICS_PORT' "$scratch/output"
    [ "$(wc -l < "$KPL_TEST_CALLS")" -eq "$calls_before" ]
done
for conflicting_port in 8080 9090 3000 2377 7946; do
    calls_before=$(wc -l < "$KPL_TEST_CALLS")
    reject "KPL_AGENT_METRICS_PORT=$conflicting_port"
    grep -q 'KPL_AGENT_METRICS_PORT conflicts' "$scratch/output"
    [ "$(wc -l < "$KPL_TEST_CALLS")" -eq "$calls_before" ]
done
calls_before=$(wc -l < "$KPL_TEST_CALLS")
reject KPL_AGENT_METRICS_PORT=18080 KPL_HTTP_PORT=18080
grep -q 'KPL_AGENT_METRICS_PORT conflicts with KPL_HTTP_PORT' "$scratch/output"
[ "$(wc -l < "$KPL_TEST_CALLS")" -eq "$calls_before" ]
calls_before=$(wc -l < "$KPL_TEST_CALLS")
env KPL_AGENT_METRICS_PORT=19091 sh "$root/scripts/check-swarm.sh" --config-only > "$scratch/output"
[ "$(wc -l < "$KPL_TEST_CALLS")" -eq "$calls_before" ]
reject KPL_CONTROL_NODE_ID=
reject KPL_TEST_CONTROL='different1 linux ready active'
reject KPL_TEST_CONTROL='control1 linux ready drain'
reject KPL_TEST_CONTROL='control1 windows ready active'
reject KPL_TEST_MANAGER='active false'
reject KPL_TEST_NETWORK='kpl-swarm-peers bridge true'
reject KPL_TEST_NETWORK='kpl-swarm-peers overlay false'
env KPL_PEER_SUBNET=10.11.0.0/24 sh "$root/scripts/check-swarm.sh" > "$scratch/output"
reject KPL_PEER_SUBNET=10.11.0.0/16
grep -q 'subnet differs' "$scratch/output"
calls_before=$(wc -l < "$KPL_TEST_CALLS")
reject KPL_PEER_SUBNET=10.11.0.1/24
reject KPL_IMAGE_BUILD_TIMEOUT=0
reject KPL_IMAGE_PUSH_TIMEOUT=08
[ "$(wc -l < "$KPL_TEST_CALLS")" -eq "$calls_before" ]
reject KPL_TEST_AGENTS='linux ready active
linux down active
windows ready active'
reject KPL_AGENT_CAPACITY=0
reject KPL_AGENT_CAPACITY=-1
reject KPL_AGENT_CAPACITY=1.5
reject KPL_AGENT_CAPACITY=08
reject KPL_AGENT_CAPACITY=999999999999999999999999999

# Read-only preflight accepts explicit tags and digests without resolving or
# pulling anything. A registry port is not a tag on the final path component.
for valid_image in registry.example/kpl:latest registry.example:5000/team/kpl:v3 registry.example/kpl@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef registry.example:5000/team/kpl:v3@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef; do
    calls_before=$(wc -l < "$KPL_TEST_CALLS")
    env "KPL_IMAGE=$valid_image" sh "$root/scripts/check-swarm.sh" --config-only > "$scratch/output"
    [ "$(wc -l < "$KPL_TEST_CALLS")" -eq "$calls_before" ]
    env "KPL_IMAGE=$valid_image" sh "$root/scripts/check-swarm.sh" > "$scratch/output"
    [ "$(wc -l < "$KPL_TEST_CALLS")" -eq "$((calls_before + 5))" ]
done
for invalid_image in registry.example/kpl registry.example:5000/team/kpl registry.example/kpl: 'registry.example/kpl:bad tag' registry.example/kpl@sha256:abcd registry.example/kpl:v3@sha256:abcd 'https://registry.example/kpl:v3'; do
    calls_before=$(wc -l < "$KPL_TEST_CALLS")
    reject "KPL_IMAGE=$invalid_image"
    grep -q 'KPL_IMAGE' "$scratch/output"
    [ "$(wc -l < "$KPL_TEST_CALLS")" -eq "$calls_before" ]
done
for invalid_timeout in 0 -1 1.5 08 999999999999999999999999999; do
    calls_before=$(wc -l < "$KPL_TEST_CALLS")
    reject "KPL_IMAGE_PULL_TIMEOUT=$invalid_timeout"
    grep -q 'KPL_IMAGE_PULL_TIMEOUT' "$scratch/output"
    [ "$(wc -l < "$KPL_TEST_CALLS")" -eq "$calls_before" ]
done
calls_before=$(wc -l < "$KPL_TEST_CALLS")
env KPL_IMAGE_PULL_TIMEOUT=17 sh "$root/scripts/check-swarm.sh" --config-only > "$scratch/output"
[ "$(wc -l < "$KPL_TEST_CALLS")" -eq "$calls_before" ]
if grep -q '^pull \|^image inspect' "$KPL_TEST_CALLS"; then exit 1; fi
printf '%s\n' 'PASS: read-only Swarm preflight isolates stack labels, validates tag/digest references and pull timeouts, and rejects legacy labels and invalid settings without pulling images.'

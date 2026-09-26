#!/bin/sh
# Contract test: multi-network tasks must advertise their Peer overlay IP.
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
mkdir "$scratch/bin"
export KPL_TEST_ARGUMENTS="$scratch/arguments"
export KPL_TEST_DISCOVERY_STATE="$scratch/discovery-state"
export KPL_TEST_SLEEPS="$scratch/sleeps"
cat > "$scratch/bin/docker" <<'MOCK_DOCKER'
#!/bin/sh
set -eu
stage=address
if [ "$1" = info ]; then stage=node; fi
if [ "${5:-}" = '{{.Image}}' ]; then stage=image; fi
if [ "${KPL_TEST_FAIL_STAGE:-}" = "$stage" ]; then
    attempt=0
    if [ -f "$KPL_TEST_DISCOVERY_STATE" ]; then attempt=$(cat "$KPL_TEST_DISCOVERY_STATE"); fi
    attempt=$((attempt + 1))
    printf '%s\n' "$attempt" > "$KPL_TEST_DISCOVERY_STATE"
    if [ "$attempt" -le "${KPL_TEST_FAILURES:-1}" ]; then
        if [ "${KPL_TEST_EMPTY_METADATA:-no}" = yes ]; then
            if [ "$stage" = address ]; then printf 'lab_monitoring 10.1.0.8\n'; fi
            exit 0
        fi
        printf 'temporary Docker metadata failure\n' >&2
        exit 1
    fi
fi
case "$1 $2" in
    'info --format')
        [ "$3" = '{{.Swarm.NodeAddr}}' ] || exit 2
        printf '%s\n' "${KPL_TEST_NODE_ADDR:-192.0.2.9}" ;;
    'inspect --type')
        [ "$3" = container ] && [ "$6" = 'lab_agent.node.task' ] || exit 2
        case "$5" in
            '{{.Image}}') echo sha256:0123456789abcdef ;;
            *) printf 'lab_monitoring 10.1.0.8\n%s %s\n' "$KPL_PEER_NETWORK" "${KPL_TEST_IP:-10.2.0.9}" ;;
        esac ;;
    *) exit 2 ;;
esac
MOCK_DOCKER
cat > "$scratch/bin/kpl" <<'MOCK_KPL'
#!/bin/sh
printf '%s\n' "$@" > "$KPL_TEST_ARGUMENTS"
MOCK_KPL
cat > "$scratch/bin/timeout" <<'MOCK_TIMEOUT'
#!/bin/sh
set -eu
[ "$1" = -k ] && [ "$2" = 1 ] && [ "$3" = 5 ] && [ "$4" = docker ] || exit 2
shift 3
if [ "${KPL_TEST_TIMEOUT:-no}" = yes ] && [ ! -f "$KPL_TEST_DISCOVERY_STATE" ]; then
    : > "$KPL_TEST_DISCOVERY_STATE"
    exit 124
fi
exec "$@"
MOCK_TIMEOUT
cat > "$scratch/bin/sleep" <<'MOCK_SLEEP'
#!/bin/sh
[ "$1" = 2 ] || exit 2
printf 'retry\n' >> "$KPL_TEST_SLEEPS"
MOCK_SLEEP
chmod +x "$scratch/bin/docker" "$scratch/bin/kpl" "$scratch/bin/timeout" "$scratch/bin/sleep"
export PATH="$scratch/bin:$PATH"
export KPL_SWARM_TASK_NAME=lab_agent.node.task KPL_SWARM_SERVICE_NAME=lab_agent KPL_SWARM_NODE_ID=node
export KPL_PEER_NETWORK=lab-peers KPL_AGENT_CAPACITY=37
sh "$root/scripts/swarm-agent.sh"
grep -qx 'lab_agent-node' "$KPL_TEST_ARGUMENTS"
[ "$(grep -cx 'http://10.2.0.9:8090' "$KPL_TEST_ARGUMENTS")" = 2 ]
grep -qx -- '--metrics-listen' "$KPL_TEST_ARGUMENTS"
grep -qx -- ':9091' "$KPL_TEST_ARGUMENTS"
grep -qx -- 'http://192.0.2.9:9091/metrics' "$KPL_TEST_ARGUMENTS"
grep -qx 'sha256:0123456789abcdef' "$KPL_TEST_ARGUMENTS"
grep -qx '37' "$KPL_TEST_ARGUMENTS"
grep -qx -- '--docker-network' "$KPL_TEST_ARGUMENTS"
grep -qx -- "$KPL_PEER_NETWORK" "$KPL_TEST_ARGUMENTS"
if grep -Eq '^--(runtime|peer-api-port|peer-p2p-port)$' "$KPL_TEST_ARGUMENTS"; then
    echo 'Swarm Agent received a removed local runtime option' >&2
    exit 1
fi
KPL_AGENT_METRICS_PORT=19091 KPL_TEST_NODE_ADDR='fd00::9' sh "$root/scripts/swarm-agent.sh"
grep -Fqx -- 'http://[fd00::9]:19091/metrics' "$KPL_TEST_ARGUMENTS"
if KPL_AGENT_METRICS_PORT=0 sh "$root/scripts/swarm-agent.sh" 2>/dev/null; then
    echo 'Invalid Agent metrics port was incorrectly admitted' >&2
    exit 1
fi
if KPL_TEST_NODE_ADDR='not an address' sh "$root/scripts/swarm-agent.sh" 2>/dev/null; then
    echo 'Invalid Swarm node address was incorrectly admitted' >&2
    exit 1
fi
if KPL_TEST_IP='fd00::9' sh "$root/scripts/swarm-agent.sh" 2>/dev/null; then
    echo 'IPv6-only task was incorrectly admitted' >&2
    exit 1
fi
# Transient Docker errors and not-yet-attached overlays must recover in place.
for stage in address node image; do
    for empty in no yes; do
        rm -f "$KPL_TEST_DISCOVERY_STATE" "$KPL_TEST_ARGUMENTS" "$KPL_TEST_SLEEPS"
        KPL_TEST_FAIL_STAGE=$stage KPL_TEST_EMPTY_METADATA=$empty sh "$root/scripts/swarm-agent.sh" 2> "$scratch/retry.log"
        grep -qx 'lab_agent-node' "$KPL_TEST_ARGUMENTS"
        [ "$(cat "$KPL_TEST_DISCOVERY_STATE")" = 2 ]
        [ "$(wc -l < "$KPL_TEST_SLEEPS" | tr -d ' ')" = 1 ]
        grep -Fq 'retrying in 2s' "$scratch/retry.log"
    done
done
rm -f "$KPL_TEST_DISCOVERY_STATE" "$KPL_TEST_ARGUMENTS"
KPL_TEST_TIMEOUT=yes sh "$root/scripts/swarm-agent.sh" 2> "$scratch/retry.log"
grep -qx 'lab_agent-node' "$KPL_TEST_ARGUMENTS"

# Persistent failure stays bounded and must never start with a guessed address.
rm -f "$KPL_TEST_DISCOVERY_STATE" "$KPL_TEST_ARGUMENTS" "$KPL_TEST_SLEEPS"
if KPL_TEST_FAIL_STAGE=address KPL_TEST_FAILURES=99 sh "$root/scripts/swarm-agent.sh" 2> "$scratch/retry.log"; then
    echo 'Agent started without task metadata' >&2
    exit 1
fi
[ ! -f "$KPL_TEST_ARGUMENTS" ]
[ "$(cat "$KPL_TEST_DISCOVERY_STATE")" = 5 ]
[ "$(wc -l < "$KPL_TEST_SLEEPS" | tr -d ' ')" = 4 ]
grep -Fq 'Cannot resolve task Peer overlay address after 5 attempts' "$scratch/retry.log"

agent_stack=$(sed -n '/^  agent:/,/^  prometheus:/p' "$root/stack.swarm.yaml")
printf '%s\n' "$agent_stack" | grep -Fq 'KPL_AGENT_METRICS_PORT: "${KPL_AGENT_METRICS_PORT:-9091}"'
printf '%s\n' "$agent_stack" | grep -Fq 'target: 9091'
if printf '%s\n' "$agent_stack" | grep -Eq 'target: (8090|18000|20000)'; then
    echo 'Agent control or Peer ports were published' >&2
    exit 1
fi
# Follow the config wired to Prometheus instead of pinning an obsolete version.
prometheus_config=$(sed -n '/^  prometheus:/,/^  grafana:/p' "$root/stack.swarm.yaml" | sed -n 's/^[[:space:]]*- source: \(prometheus-[^[:space:]]*\)$/\1/p')
[ -n "$prometheus_config" ]
grep -Fqx "  $prometheus_config:" "$root/stack.swarm.yaml"
awk -v key="  $prometheus_config:" '$0 == key { getline; print }' "$root/stack.swarm.yaml" | grep -Fq 'file: ./monitoring/prometheus/swarm.yml'
grep -Fq 'http_sd_configs:' "$root/monitoring/prometheus/swarm.yml"
grep -Fq 'honor_labels: true' "$root/monitoring/prometheus/swarm.yml"
if grep -Fq 'tasks.agent' "$root/monitoring/prometheus/swarm.yml"; then
    echo 'Legacy Swarm DNS discovery is still configured' >&2
    exit 1
fi
printf '%s\n' 'PASS: Swarm task identity, overlay address, node-address metrics URL, local image, port validation and bounded startup recovery.'

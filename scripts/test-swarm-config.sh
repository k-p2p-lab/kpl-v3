#!/bin/sh
# Exercise the public CLI with a fake Docker manager; never contact a real daemon.
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
mkdir "$scratch/bin"
cat > "$scratch/bin/docker" <<'MOCK'
#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$KPL_CONFIG_TEST_CALLS"
case "$*" in
    'info --format {{.Swarm.LocalNodeState}} {{.Swarm.ControlAvailable}}') printf 'active true\n' ;;
    'info --format {{.Swarm.NodeID}}') printf 'manager1\n' ;;
    *) printf 'Unexpected Docker call: %s\n' "$*" >&2; exit 97 ;;
esac
MOCK
chmod +x "$scratch/bin/docker"
export PATH="$scratch/bin:$PATH"
. "$root/scripts/swarm-config.sh"
for setting in $swarm_config_keys; do unset "$setting"; done
export KPL_CONFIG_TEST_CALLS="$scratch/calls" KPL_CONFIG_TEST_MARKER="$scratch/evaluated"
: > "$KPL_CONFIG_TEST_CALLS"
config_path="$scratch/config with spaces.env"
run() { sh "$root/scripts/swarm.sh" --env-file "$config_path" "$@" > "$scratch/output" 2>&1; }
reject() { if run "$@"; then printf 'Unexpected acceptance: %s\n' "$1" >&2; exit 1; fi; }

# First-time setup persists all requested values and independently generated secrets.
run init KPL_IMAGE=registry.example:5000/team/kpl:v3 KPL_AGENT_CAPACITY=40 KPL_MIN_AGENTS=2 KPL_PEER_SUBNET=10.11.0.0/24
[ "$(stat -c '%a' "$config_path")" = 600 ]
for line in KPL_IMAGE=registry.example:5000/team/kpl:v3 KPL_CONTROL_NODE_ID=manager1 KPL_AGENT_CAPACITY=40 KPL_RUN_MIN_FREE_BYTES=1073741824 KPL_AGENT_METRICS_PORT=9091 KPL_MIN_AGENTS=2 KPL_PEER_SUBNET=10.11.0.0/24; do
    grep -Fxq "$line" "$config_path"
done
grep -Fxq 'KPL_USER=admin' "$config_path"
grep -Eq '^KPL_PASSWORD=[a-f0-9]{64}$' "$config_path"
grep -Eq '^GRAFANA_ADMIN_PASSWORD=[a-f0-9]{64}$' "$config_path"
token=$(sed -n 's/^KPL_PASSWORD=//p' "$config_path")
password=$(sed -n 's/^GRAFANA_ADMIN_PASSWORD=//p' "$config_path")
[ "$token" != "$password" ]
if grep -Fq "$token" "$scratch/output" || grep -Fq "$password" "$scratch/output"; then exit 1; fi
cp "$config_path" "$scratch/original"
calls=$(wc -l < "$KPL_CONFIG_TEST_CALLS")
reject init KPL_IMAGE=registry.example/team/kpl:new
cmp "$config_path" "$scratch/original"
[ "$(wc -l < "$KPL_CONFIG_TEST_CALLS")" -eq "$calls" ]

# Configuration display hides secrets; the explicitly requested credential command reveals them.
run config
grep -Fxq 'KPL_PASSWORD=[redacted]' "$scratch/output"
grep -Fxq 'GRAFANA_ADMIN_PASSWORD=[redacted]' "$scratch/output"
if grep -Fq "$token" "$scratch/output" || grep -Fq "$password" "$scratch/output"; then exit 1; fi
run credentials
grep -Fxq "KPL_PASSWORD=$token" "$scratch/output"
grep -Fxq "GRAFANA_ADMIN_PASSWORD=$password" "$scratch/output"
[ "$(wc -l < "$KPL_CONFIG_TEST_CALLS")" -eq "$calls" ]

# Saved changes preserve unrelated file values despite conflicting exported values.
export KPL_IMAGE=registry.example/team/kpl:environment KPL_AGENT_CAPACITY=999 KPL_PASSWORD=environment-token
run configure KPL_IMAGE=registry.example/team/kpl:updated KPL_PEER_SUBNET= KPL_AGENT_METRICS_PORT=19091
grep -Fxq 'KPL_IMAGE=registry.example/team/kpl:updated' "$config_path"
grep -Fxq 'KPL_PEER_SUBNET=' "$config_path"
grep -Fxq 'KPL_AGENT_CAPACITY=40' "$config_path"
grep -Fxq 'KPL_AGENT_METRICS_PORT=19091' "$config_path"
grep -Fxq "KPL_PASSWORD=$token" "$config_path"
grep -Fxq "GRAFANA_ADMIN_PASSWORD=$password" "$config_path"
[ "$(stat -c '%a' "$config_path")" = 600 ]
run config
grep -Fxq 'KPL_IMAGE=registry.example/team/kpl:environment' "$scratch/output"
run credentials
grep -Fxq 'KPL_PASSWORD=environment-token' "$scratch/output"
unset KPL_IMAGE KPL_AGENT_CAPACITY KPL_PASSWORD

# A malformed edit is transactional: file contents, mode, and Docker calls stay unchanged.
cp "$config_path" "$scratch/original"
for setting in KPL_RUN_MIN_FREE_BYTES=-1 KPL_RUN_MIN_FREE_BYTES=01 KPL_RUN_MIN_FREE_BYTES=abc UNKNOWN=value KPL_IMAGE=registry.example/kpl KPL_IMAGE=https://registry.example/kpl:v3 KPL_AGENT_CAPACITY=0 KPL_AGENT_METRICS_PORT=0 KPL_AGENT_METRICS_PORT=09091 KPL_AGENT_METRICS_PORT=65536 KPL_IMAGE_BUILD_TIMEOUT=08 KPL_IMAGE_PUSH_TIMEOUT=-1 KPL_HTTP_PORT=65536 KPL_STACK_NAME=Bad KPL_CONTROL_NODE_ID=bad/id KPL_PASSWORD=; do
    reject configure KPL_AGENT_CAPACITY=50 "$setting"
    cmp "$config_path" "$scratch/original"
done
for conflicting_port in 8080 9090 3000 2377 7946; do
    reject configure "KPL_AGENT_METRICS_PORT=$conflicting_port"
    grep -q 'KPL_AGENT_METRICS_PORT conflicts' "$scratch/output"
    cmp "$config_path" "$scratch/original"
done
reject configure KPL_AGENT_METRICS_PORT=18080 KPL_HTTP_PORT=18080
grep -q 'KPL_AGENT_METRICS_PORT conflicts with KPL_HTTP_PORT' "$scratch/output"
cmp "$config_path" "$scratch/original"
reject configure KPL_HTTP_PORT=8000 KPL_HTTP_PORT=8001
reject configure 'KPL_PASSWORD GRAFANA_ADMIN_USER=must-not-be-exported'
if grep -q 'must-not-be-exported' "$scratch/output"; then exit 1; fi
reject configure 'KPL_PASSWORD=line1
line2'
reject configure
reject config --all
reject credentials --all
cmp "$config_path" "$scratch/original"
[ "$(wc -l < "$KPL_CONFIG_TEST_CALLS")" -eq "$calls" ]
[ "$(stat -c '%a' "$config_path")" = 600 ]

# An in-progress helper edit owns the lock until its final rename and cleanup.
mkdir "$config_path.lock"
reject configure KPL_AGENT_CAPACITY=50
cmp "$config_path" "$scratch/original"
grep -q 'configuration edit' "$scratch/output"
[ -d "$config_path.lock" ]
rmdir "$config_path.lock"

# CIDRs have canonical octets and zero host bits; empty means automatic allocation.
for subnet in 10.11.0.1/24 10.11.0.0/33 10.11.0.0/08 10.11.0.0 300.11.0.0/24 010.11.0.0/24 10.11.0.0/24/1 '10.11.0.0/24 extra'; do
    reject configure "KPL_PEER_SUBNET=$subnet"
    cmp "$config_path" "$scratch/original"
done
run configure KPL_PEER_SUBNET=10.11.0.0/16
grep -Fxq 'KPL_PEER_SUBNET=10.11.0.0/16' "$config_path"

# Literal shell syntax and enclosing quotes survive write/load without evaluation.
literal='$(touch "$KPL_CONFIG_TEST_MARKER")'
run configure "KPL_PASSWORD=$literal" "GRAFANA_ADMIN_PASSWORD='quoted password'"
run credentials
grep -Fxq "KPL_PASSWORD=$literal" "$scratch/output"
grep -Fxq "GRAFANA_ADMIN_PASSWORD='quoted password'" "$scratch/output"
[ ! -e "$KPL_CONFIG_TEST_MARKER" ]
run config
if grep -Fq "$literal" "$scratch/output"; then exit 1; fi

# Symlinks and absent files cannot turn configure into an unexpected overwrite.
cp "$config_path" "$scratch/original"
config_path="$scratch/symlink.env"
ln -s "$scratch/original" "$config_path"
reject configure KPL_AGENT_CAPACITY=10
reject init
config_path="$scratch/missing.env"
reject configure KPL_AGENT_CAPACITY=10
reject config
reject credentials
reject init --all
reject init KPL_IMAGE=missing-tag
[ ! -e "$config_path" ]
[ "$(wc -l < "$KPL_CONFIG_TEST_CALLS")" -eq "$calls" ]

# Inherited trailing newlines must be rejected rather than silently truncated.
export KPL_PASSWORD='trailing-newline
'
reject init
[ ! -e "$config_path" ]
[ "$(wc -l < "$KPL_CONFIG_TEST_CALLS")" -eq "$calls" ]
unset KPL_PASSWORD

# Initialization rejects every Agent metrics port collision before consulting
# Docker or creating a configuration file.
for conflicting_port in 8080 9090 3000 2377 7946; do
    config_path="$scratch/conflicting-$conflicting_port.env"
    calls_before=$(wc -l < "$KPL_CONFIG_TEST_CALLS")
    reject init "KPL_AGENT_METRICS_PORT=$conflicting_port"
    grep -q 'KPL_AGENT_METRICS_PORT conflicts' "$scratch/output"
    [ ! -e "$config_path" ]
    [ "$(wc -l < "$KPL_CONFIG_TEST_CALLS")" -eq "$calls_before" ]
done


# Old token files migrate transactionally without reusing their secret.
config_path="$scratch/legacy.env"
printf '%s\n' 'KPL_API_TOKEN=retired-secret' 'KPL_IMAGE=registry.example/kpl:legacy' 'KPL_AGENT_CAPACITY=37' 'GRAFANA_ADMIN_PASSWORD=grafana-kept' > "$config_path"
run configure KPL_USER=operator
if grep -q '^KPL_API_TOKEN=' "$config_path"; then exit 1; fi
grep -Fxq 'KPL_USER=operator' "$config_path"
grep -Eq '^KPL_PASSWORD=[a-f0-9]{64}$' "$config_path"
grep -Fxq 'GRAFANA_ADMIN_PASSWORD=grafana-kept' "$config_path"
grep -Fxq 'KPL_AGENT_CAPACITY=37' "$config_path"
[ "$(stat -c '%a' "$config_path")" = 600 ]
migrated_password=$(sed -n 's/^KPL_PASSWORD=//p' "$config_path")
if grep -Fq "$migrated_password" "$scratch/output" || grep -Fq retired-secret "$scratch/output"; then exit 1; fi
run configure KPL_AGENT_CAPACITY=38
grep -Fxq "KPL_PASSWORD=$migrated_password" "$config_path"
reject configure KPL_API_TOKEN=retired-secret
run config
grep -Fxq 'KPL_PASSWORD=[redacted]' "$scratch/output"

# Explicit initialization settings can come from the environment as before runtime commands.
config_path="$scratch/environment.env"
export KPL_IMAGE=registry.example/kpl:environment KPL_AGENT_CAPACITY=12
run init
grep -Fxq 'KPL_IMAGE=registry.example/kpl:environment' "$config_path"
grep -Fxq 'KPL_AGENT_CAPACITY=12' "$config_path"
if find "$scratch" -name '.env.swarm.stage.*' | grep -q .; then exit 1; fi
printf '%s\n' 'PASS: Swarm configuration persists validated settings atomically, preserves credentials, redacts secrets, and never evaluates configuration.'

run configure KPL_RUN_MIN_FREE_BYTES=0
grep -Fxq 'KPL_RUN_MIN_FREE_BYTES=0' "$config_path"
run configure KPL_RUN_MIN_FREE_BYTES=2147483648
grep -Fxq 'KPL_RUN_MIN_FREE_BYTES=2147483648' "$config_path"

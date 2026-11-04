#!/bin/sh
# Verify command forwarding with fake tools. No daemon, registry or test binary
# is used, and all Make targets run in a disposable checkout skeleton.
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'result=$?; if [ "$result" -ne 0 ]; then printf "Make test failed: %s\n" "${case_name:-setup}" >&2; cat "$scratch/output" "$scratch/calls" >&2 2>/dev/null || :; fi; rm -rf "$scratch"; exit "$result"' EXIT
project=$scratch/project
mkdir -p "$project/scripts" "$scratch/bin"
cp "$root/Makefile" "$project/Makefile"
: > "$scratch/output"
: > "$scratch/calls"
cat > "$scratch/bin/tool" <<'MOCK'
#!/bin/sh
set -eu
tool=${0##*/}
{
    printf 'tool=%s\n' "$tool"
    for argument do printf 'arg=%s\n' "$argument"; done
    printf 'end\n'
} >> "$KPL_TEST_MAKE_CALLS"
if [ "${KPL_TEST_MAKE_FAIL_TOOL:-}" = "$tool" ]; then exit 7; fi
MOCK
chmod +x "$scratch/bin/tool"
for tool in sh bash go node docker; do cp "$scratch/bin/tool" "$scratch/bin/$tool"; done
export PATH="$scratch/bin:$PATH" KPL_TEST_MAKE_CALLS="$scratch/calls"
# Do not let the caller's optional Make arguments select another config or
# replace test tools. Command-line values below exercise every supported input.
unset ENV_FILE ARGS SETTINGS NODES PLATFORMS COMPONENT TAIL CONTEXT SCENARIO
unset KPL_TEST_MAKE_FAIL_TOOL
export MAKEFLAGS= MAKEOVERRIDES=

run_make() {
    : > "$scratch/calls"
    make --no-print-directory -C "$project" "$@" > "$scratch/output" 2>&1
}
record() {
    printf 'tool=%s\n' "$1"
    shift
    for argument do printf 'arg=%s\n' "$argument"; done
    printf 'end\n'
}
expect() {
    record "$@" > "$scratch/expected"
    cmp "$scratch/expected" "$scratch/calls"
}
reject_without_tools() {
    if run_make "$@"; then
        printf 'Unexpected Make acceptance\n' >&2
        exit 1
    fi
    [ ! -s "$scratch/calls" ]
}

case_name='default build and non-operational help'
run_make
expect go build -o bin/kpl ./cmd/kpl
: > "$project/help"
run_make help
[ ! -s "$scratch/calls" ]
[ -s "$scratch/output" ]

case_name='one native Swarm entry point'
: > "$project/swarm"
run_make swarm
expect sh scripts/swarm.sh help
run_make swarm "ARGS=logs auth --tail all --context 'controller node'"
expect sh scripts/swarm.sh logs auth --tail all --context 'controller node'

case_name='named targets and phony dispatch'
for command in help init configure config credentials nodes login publish check access logs scenario deploy restart status add-node remove-node remove reset-peer-network; do
    : > "$project/swarm-$command"
    run_make "swarm-$command"
    expect sh scripts/swarm.sh "$command"
done

case_name='compatibility aliases'
: > "$project/check-swarm"
: > "$project/swarm-reset-network"
run_make check-swarm
expect sh scripts/swarm.sh check
run_make swarm-reset-network
expect sh scripts/swarm.sh reset-peer-network

case_name='literal config path on every Swarm target'
config_path="$scratch/config's \$(touch $scratch/injected) \$(shell touch $scratch/make-injected) ; literal.env"
for target in swarm swarm-help swarm-init swarm-configure swarm-config swarm-credentials swarm-nodes swarm-login swarm-publish swarm-check swarm-access swarm-logs swarm-scenario swarm-deploy swarm-restart swarm-status swarm-add-node swarm-remove-node swarm-remove swarm-reset-peer-network check-swarm swarm-reset-network; do
    case "$target" in
        swarm) command=help ;;
        check-swarm) command=check ;;
        swarm-reset-network) command=reset-peer-network ;;
        *) command=${target#swarm-} ;;
    esac
    run_make "$target" "ENV_FILE=$config_path"
    expect sh scripts/swarm.sh --env-file "$config_path" "$command"
    [ ! -e "$scratch/injected" ]
    [ ! -e "$scratch/make-injected" ]
done

case_name='settings and node forwarding'
for command in init configure; do
    run_make "swarm-$command" "SETTINGS=KPL_AGENT_CAPACITY=20 KPL_MIN_AGENTS=2"
    expect sh scripts/swarm.sh "$command" KPL_AGENT_CAPACITY=20 KPL_MIN_AGENTS=2
done
for command in deploy restart add-node remove-node; do
    run_make "swarm-$command" 'NODES=worker-a worker-b'
    expect sh scripts/swarm.sh "$command" worker-a worker-b
    run_make "swarm-$command" NODES=--workers
    expect sh scripts/swarm.sh "$command" --workers
done

case_name='publication and restart platform forwarding'
run_make swarm-publish PLATFORMS=linux/amd64,linux/arm64
expect sh scripts/swarm.sh publish --platforms linux/amd64,linux/arm64
run_make swarm-restart PLATFORMS=linux/amd64,linux/arm64 NODES=--workers
expect sh scripts/swarm.sh restart --platforms linux/amd64,linux/arm64 --workers

case_name='logs and scenario forwarding'
run_make swarm-logs COMPONENT=auth TAIL=all CONTEXT=controller-node
expect sh scripts/swarm.sh logs auth --tail all --context controller-node
run_make swarm-logs TAIL=200
expect sh scripts/swarm.sh logs --tail 200
scenario_path="$scratch/scenario's \$(shell touch $scratch/scenario-injected).yaml"
run_make swarm-scenario "SCENARIO=$scenario_path"
expect sh scripts/swarm.sh scenario "$scenario_path"
run_make validate "SCENARIO=$scenario_path"
expect go run ./cmd/kpl validate --scenario "$scenario_path"
[ ! -e "$scratch/scenario-injected" ]
run_make validate
expect go run ./cmd/kpl validate --scenario examples/smoke.yaml

case_name='raw native arguments cannot be silently ignored'
for target in swarm-status swarm-restart check-swarm swarm-reset-network; do
    reject_without_tools "$target" ARGS=--unsupported
done
reject_without_tools swarm-unknown

case_name='failure propagation'
export KPL_TEST_MAKE_FAIL_TOOL=sh
if run_make swarm-restart; then exit 1; fi
expect sh scripts/swarm.sh restart
if run_make check-swarm; then exit 1; fi
expect sh scripts/swarm.sh check
unset KPL_TEST_MAKE_FAIL_TOOL

case_name='Go test aliases use one definition'
run_make test-go
cp "$scratch/calls" "$scratch/go-tests"
grep -Fxq 'arg=-buildvcs=false' "$scratch/calls"
grep -Fxq 'arg=120s' "$scratch/calls"
grep -Fxq 'arg=./...' "$scratch/calls"
grep -Fxq 'arg=github.com/libp2p/go-libp2p-pubsub' "$scratch/calls"
grep -Fxq 'arg=^Test(KPL|Score)' "$scratch/calls"
[ "$(grep -c '^tool=go$' "$scratch/calls")" = 2 ]
run_make test
cmp "$scratch/go-tests" "$scratch/calls"
export KPL_TEST_MAKE_FAIL_TOOL=go
if run_make test-go; then exit 1; fi
[ "$(grep -c '^tool=go$' "$scratch/calls")" = 1 ]
unset KPL_TEST_MAKE_FAIL_TOOL

case_name='Swarm regression entry point includes each suite once'
run_make test-swarm
for script in test-swarm-agent.sh test-check-swarm.sh test-swarm-config.sh test-swarm.sh test-make.sh; do
    [ "$(grep -Fxc "arg=scripts/$script" "$scratch/calls")" = 1 ]
done
[ "$(grep -Fxc 'arg=scripts/test-swarm-completion.sh' "$scratch/calls")" = 1 ]
! grep -Fq 'arg=scripts/test-swarm-peer-network.sh' "$scratch/calls"
! grep -Fq 'arg=scripts/test-swarm-network-gateway.sh' "$scratch/calls"
grep -Fxq 'tool=bash' "$scratch/calls"

case_name='development checks and container test dispatch'
run_make vet
expect go vet ./...
run_make test-ui
grep -Fxq 'tool=node' "$scratch/calls"
grep -Fxq 'arg=--test' "$scratch/calls"
run_make test-race
grep -Fxq 'arg=-race' "$scratch/calls"
grep -Fxq 'arg=-buildvcs=false' "$scratch/calls"
grep -Fxq 'arg=-count=1' "$scratch/calls"
grep -Fxq 'arg=120s' "$scratch/calls"
run_make test-linux
expect docker build --target test -t kpl-v3:test .
run_make check
for tool in go sh bash node; do grep -Fxq "tool=$tool" "$scratch/calls"; done
for argument in build vet ./... scripts/test-swarm.sh scripts/test-make.sh --test; do
    grep -Fxq "arg=$argument" "$scratch/calls"
done
! grep -Fxq 'tool=docker' "$scratch/calls"

printf '%s\n' 'PASS: Make command aliases, literal configuration paths, native and named arguments, deployment defaults, failure propagation and shared verification targets.'

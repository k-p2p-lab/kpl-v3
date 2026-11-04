.DEFAULT_GOAL := build

SWARM_COMMANDS := help init configure config credentials nodes login publish check access logs scenario deploy restart status add-node remove-node remove reset-peer-network
SWARM_TARGETS := $(addprefix swarm-,$(SWARM_COMMANDS))

.PHONY: help build check test test-go test-swarm test-ui test-race vet test-linux validate
.PHONY: swarm $(SWARM_TARGETS) check-swarm swarm-reset-network

# Adapter inputs travel through argv only. Automatic environment export would
# recursively expand command-line Make values, even when read with $(value).
unexport ENV_FILE ARGS SETTINGS NODES PLATFORMS COMPONENT TAIL CONTEXT SCENARIO

# Quote scalar values as one literal shell argument. ARGS, SETTINGS and NODES
# are argument lists and retain the caller's shell quoting.
shell_quote = '$(subst ','"'"',$(1))'
SWARM = sh scripts/swarm.sh $(if $(strip $(value ENV_FILE)),--env-file $(call shell_quote,$(value ENV_FILE)))
swarm_args_init = $(value SETTINGS)
swarm_args_configure = $(value SETTINGS)
swarm_args_publish = $(if $(strip $(value PLATFORMS)),--platforms $(call shell_quote,$(value PLATFORMS)))
swarm_args_logs = $(if $(strip $(value COMPONENT)),$(call shell_quote,$(value COMPONENT))) $(if $(strip $(value TAIL)),--tail $(call shell_quote,$(value TAIL))) $(if $(strip $(value CONTEXT)),--context $(call shell_quote,$(value CONTEXT)))
swarm_args_scenario = $(if $(strip $(value SCENARIO)),$(call shell_quote,$(value SCENARIO)))
swarm_args_deploy = $(value NODES)
swarm_args_restart = $(swarm_args_publish) $(value NODES)
swarm_args_add-node = $(value NODES)
swarm_args_remove-node = $(value NODES)

help:
	@printf '%s\n' \
	  'Build and validate: make build | check | test | test-swarm | test-ui | test-race | vet | test-linux' \
	  '  check: build, vet, Go/PubSub tests, mocked Swarm/Make tests and UI tests' \
	  '  test: Go/PubSub tests (same as test-go); test-linux: Docker test target' \
	  '  validate: validate SCENARIO (default: examples/smoke.yaml)' \
	  '' \
	  'Swarm: make swarm ARGS="COMMAND [OPTIONS]" [ENV_FILE=PATH]' \
	  '  make swarm-help lists commands; make swarm-COMMAND is a named shortcut' \
	  '  Named options: SETTINGS, NODES, PLATFORMS, COMPONENT, TAIL, CONTEXT, SCENARIO' \
	  '  ARGS is for make swarm only; ENV_FILE works with every Swarm target' \
	  '  Bare deploy/restart reuse Agent labels; initial deployment needs NODES=--workers or --all' \
	  '  Compatibility aliases: check-swarm -> swarm-check; swarm-reset-network -> swarm-reset-peer-network'

build:
	go build -o bin/kpl ./cmd/kpl

check: build vet test-go test-swarm test-ui

test: test-go

test-go:
	go test -buildvcs=false ./... -timeout 120s
	go test -buildvcs=false github.com/libp2p/go-libp2p-pubsub -run '^Test(KPL|Score)' -timeout 120s

test-swarm:
	sh scripts/test-swarm-agent.sh
	sh scripts/test-check-swarm.sh
	sh scripts/test-swarm-config.sh
	sh scripts/test-swarm.sh
	bash scripts/test-swarm-completion.sh
	sh scripts/test-make.sh

test-ui:
	node --test internal/webui/*_test.cjs

test-race:
	GORACE=atexit_sleep_ms=0 go test -race -buildvcs=false ./... -count=1 -timeout 120s

vet:
	go vet ./...

test-linux:
	docker build --target test -t kpl-v3:test .

validate:
	go run ./cmd/kpl validate --scenario $(call shell_quote,$(if $(strip $(value SCENARIO)),$(value SCENARIO),examples/smoke.yaml))

# All operator entrypoints delegate to the same lifecycle implementation.
# Keep an explicit target list so misspelled commands fail at Make's boundary.
swarm:
	@$(SWARM) $(if $(strip $(value ARGS)),$(value ARGS),help)

$(SWARM_TARGETS):
	$(if $(strip $(value ARGS)),$(error ARGS is only supported by make swarm; use the named options or the common swarm target))
	@$(SWARM) $(patsubst swarm-%,%,$@) $(swarm_args_$(patsubst swarm-%,%,$@))

check-swarm: swarm-check

swarm-reset-network: swarm-reset-peer-network

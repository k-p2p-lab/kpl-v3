.PHONY: build test test-linux check-swarm swarm-init swarm-deploy swarm-status swarm-add-node swarm-remove-node swarm-remove validate
.PHONY: swarm-nodes swarm-configure swarm-config swarm-credentials swarm-login swarm-publish swarm-check swarm-access swarm-logs swarm-scenario

build:
	go build -o bin/kpl ./cmd/kpl

test:
	go test ./...
	go test github.com/libp2p/go-libp2p-pubsub -run '^Test(KPL|Score)' -timeout 120s

test-linux:
	docker build --target test -t kpl-v3:test .

check-swarm:
	sh scripts/swarm.sh check

swarm-nodes:
	sh scripts/swarm.sh nodes

swarm-init:
	sh scripts/swarm.sh init $(SETTINGS)

swarm-configure:
	sh scripts/swarm.sh configure $(SETTINGS)

swarm-config:
	sh scripts/swarm.sh config

swarm-credentials:
	sh scripts/swarm.sh credentials

swarm-login:
	sh scripts/swarm.sh login

swarm-publish:
	sh scripts/swarm.sh publish $(if $(PLATFORMS),--platforms $(PLATFORMS))

swarm-check:
	sh scripts/swarm.sh check

swarm-access:
	sh scripts/swarm.sh access

swarm-logs:
	sh scripts/swarm.sh logs $(COMPONENT)

swarm-scenario:
	sh scripts/swarm.sh scenario $(SCENARIO)

swarm-deploy:
	sh scripts/swarm.sh deploy $(NODES)

swarm-status:
	sh scripts/swarm.sh status

swarm-add-node:
	sh scripts/swarm.sh add-node $(NODES)

swarm-remove-node:
	sh scripts/swarm.sh remove-node $(NODES)

swarm-remove:
	sh scripts/swarm.sh remove

swarm-reset-network:
	sh scripts/swarm.sh reset-peer-network

swarm-restart:
	sh scripts/swarm.sh remove
	sleep 5
	sh scripts/swarm.sh publish
	sh scripts/swarm.sh deploy $(if $(strip $(NODES)),$(NODES),--workers)

validate:
	go run ./cmd/kpl validate --scenario examples/smoke.yaml

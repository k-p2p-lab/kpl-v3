# K-P2PLab v3

English | [Korean](README.kr.md)

[K-P2PLab Hub](https://github.com/k-p2p-lab/hub) contains the project-wide concepts and research context; this repository owns the runnable v3 implementation, deployment procedures, configuration, and version-specific behavior.

K-P2PLab v3 runs configurable libp2p Kademlia and PubSub experiments on Docker Swarm across one or more Linux hosts. The Controller schedules scenarios and serves the web Dashboard; Agents create and manage Peer containers through the local Docker daemon. Docker Swarm starts one Agent per selected host, and each Peer has its own container and network namespace. Prometheus/Grafana expose collected telemetry, and the Controller retains downloadable run results.

English is the default language for code, the UI, and documentation. Korean documentation is maintained in matching `.kr.md` files.

## Core features

- Version 1 and 2 YAML scenarios with joins, leaves, readiness barriers, publishing, repeated phases, background jobs, and seeded distributions
- Kademlia configuration and selectable GossipSub, FloodSub, and RandomSub routers
- Isolated Peer containers with per-Peer delay, jitter, loss, duplication, corruption, reordering, and bandwidth controls
- Docker Swarm deployment with capacity-aware Peer placement on one or more nodes
- Live Kademlia, GossipSub GRAFT, and transport topology with Agent sectors and topic filters
- Churn-aware delivery, latency, duplicate, coverage, and observation-quality metrics
- Reusable scenario library plus repeat runs, persisted results, ZIP export, and deletion
- Background analysis for individual runs and equal-run repetition averages, v2 research comparisons, bandwidth images and PNG/CSV/ZIP downloads
- Measured libp2p stream throughput and cumulative bytes by protocol, with collection-quality indicators

## Prerequisites

- Linux with rootful Docker Engine in an active Swarm; the supplied deployment does not support userns-remap
- `NET_ADMIN` and the kernel `sch_prio`, `sch_netem`, `cls_u32`, and optional `sch_tbf` modules for network conditions
- Go 1.25 or later only for development outside Docker
- An active Swarm manager and an image registry reachable and trusted by every selected node

See the [Swarm deployment guide](docs/swarm.md) for host preparation, permissions, remote access, storage, and safe shutdown.

A fixed scenario seed repeats distribution inputs, but does not make execution timing, generated payloads, Peer identities, or protocol outcomes deterministic.

## Swarm quick start

Run these commands from the repository on the active manager. Replace the image reference with a registry accessible to every node.

```sh
sh scripts/swarm.sh init KPL_IMAGE=registry.example.com/kpl-v3:v3 KPL_AGENT_CAPACITY=20 KPL_MIN_AGENTS=2
sh scripts/swarm.sh publish
sh scripts/swarm.sh deploy --workers
sh scripts/swarm.sh status
sh scripts/swarm.sh access
sh scripts/swarm.sh credentials
sh scripts/swarm.sh scenario
```

Use `--all` instead of `--workers` when the manager must also run an Agent. For a single-node Swarm, set `KPL_MIN_AGENTS=1` during `init` and deploy with `--all`. Open the Controller URL printed by `access`, log in with the `KPL_USER`/`KPL_PASSWORD` shown by `credentials`, then paste the scenario printed by `scenario`. `access` also lists the metrics URL on every selected Agent node. Allow TCP `KPL_AGENT_METRICS_PORT` (default `9091`) from the control node and, when operators open those links directly, from their trusted management network. The helper resolves and pins the current image digest during deployment, so tag updates do not require manual SHA edits. Read the [complete Swarm workflow](docs/swarm.md) before operating or removing a production cluster.

For a monitoring example, run [`examples/monitoring.yaml`](examples/monitoring.yaml). Follow [monitoring and results](docs/monitoring.md) to select the run in Grafana and download its event log and derived metrics. Saved run files remain downloadable after Controller restarts; live execution and counters are not restored from those files. Stop services with `sh scripts/swarm.sh remove`, which waits for Controller and Agent cleanup.

## Documentation

Start with the [Hub](https://github.com/k-p2p-lab/hub) for objectives, research, design principles, and conceptual architecture. The guides below describe the current v3 implementation and its limits.

| Guide | Contents |
|---|---|
| [Implementation architecture](docs/architecture.md) | v3 components, control and experiment paths, Docker networks, placement, and isolation boundaries |
| [Swarm deployment](docs/swarm.md) | Linux requirements, registry setup, node selection, deployment, storage, updates, and removal |
| [Scenario configuration](docs/scenario-reference.md) | YAML actions, profiles, protocol controls, distributions, and network conditions |
| [Scenario library](docs/scenario-library.md) | Save, name, load, update, and delete reusable scenarios |
| [REST API](docs/api.md) | Controller endpoints, authentication, results, and internal cleanup API |
| [Development](docs/development.md) | Go build, scenario validation, tests, and development deployment on Swarm |
| [Experiment metrics](docs/experiment-metrics.md) | Churn-aware delivery denominator, latency, duplicates, and limitations |
| [Monitoring and results](docs/monitoring.md) | Prometheus, Grafana, ZIP contents, retained data, and deletion |
| [Saved-result visualization](docs/visualization.md) | Result images, repeated-run comparisons and PNG/CSV/ZIP downloads |
| [Topology](docs/topology.md) | Agent sectors, graph layers, topic filters, and controls |
| [Swarm churn and publish](docs/swarm-churn-publish.md) | Multi-server continuous-churn experiment walkthrough |
| [Prysm block scoring under churn](docs/swarm-churn-prysm-block.md) | Three delay cohorts, concurrent churn, and score observation |
| [v2 reproduction](docs/v2-reproduction.md) | Compatibility mapping and intentional differences from K-P2PLab v2 |
| [Protocol configuration](docs/protocol-options.md) | Complete PubSub, scoring, Kademlia and named-policy controls |
| [Bandwidth measurement](docs/bandwidth.md) | Stream usage, rate calculation, protocol attribution and collection limits |
| [v2 analysis coverage](docs/v2-analysis-coverage.md) | v2 analysis/visualization mapping, definition differences, and required evidence |

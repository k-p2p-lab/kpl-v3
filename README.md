# K-P2PLab v3

> *K-P2PLab: An Elastic, Centrally Orchestrated Multi-Host P2P Testbed with Per-Peer Container Isolation*

A P2P research testbed that runs libp2p Kademlia and PubSub experiments in isolated Peer containers across Linux Docker Swarm hosts.

**[Documentation](https://github.com/k-p2p-lab/kpl-v3/wiki/Home)** · [Getting started](https://github.com/k-p2p-lab/kpl-v3/wiki/getting_started) · [Architecture](https://github.com/k-p2p-lab/kpl-v3/wiki/architecture)

This repository contains the implementation and runnable examples. Project, research, deployment, and development documentation is maintained in the wiki.

[Examples](examples) · [Swarm helper](scripts/swarm.sh)

Run `make help` for build and validation commands, or `make swarm-help` for Swarm operations. `make swarm ARGS='status'` uses the same helper as the named `make swarm-status` shortcut. See the [deployment guide](https://github.com/k-p2p-lab/kpl-v3/wiki/swarm_deployment) for configuration and node selection.

Scenario schema v4: [Worker attacks](https://github.com/k-p2p-lab/kpl-v3/wiki/worker_attacks) · [`examples/attacks`](examples/attacks).

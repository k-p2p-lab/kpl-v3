# K-P2PLab v3

> *K-P2PLab: An Elastic, Centrally Orchestrated Multi-Host P2P Testbed with Per-Peer Container Isolation*

A P2P research testbed that runs libp2p Kademlia and PubSub experiments in isolated Peer containers across Linux Docker Swarm hosts.

**[Documentation](https://github.com/k-p2p-lab/kpl-v3/wiki/Home)** · [Getting started](https://github.com/k-p2p-lab/kpl-v3/wiki/getting_started) · [Architecture](https://github.com/k-p2p-lab/kpl-v3/wiki/architecture)

This repository contains the implementation and runnable examples. Project, research, deployment, and development documentation is maintained in the wiki.

[Examples](examples) · [Swarm operations](https://github.com/k-p2p-lab/kpl-v3/wiki/swarm_deployment)

Build the Go operator as `bin/kpl` with `go build -o bin/kpl ./cmd/kpl-master`. `kpl help` lists build and validation commands, and `kpl swarm help` lists operations. See [development and installation](https://github.com/k-p2p-lab/kpl-v3/wiki/development) and the [deployment guide](https://github.com/k-p2p-lab/kpl-v3/wiki/swarm_deployment). The native CLI works without the legacy Make and manager shell entrypoints, which remain available for compatibility. The Controller/Agent/Peer runtime entrypoint remains `cmd/kpl`; local runtime builds use `bin/kpl-runtime`.

Scenario schema v4: [Worker attacks](https://github.com/k-p2p-lab/kpl-v3/wiki/worker_attacks) · [`examples/attacks`](examples/attacks).

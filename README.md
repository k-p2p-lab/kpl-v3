# K-P2PLab v3

> *K-P2PLab: An Elastic, Centrally Orchestrated Multi-Host P2P Testbed with Per-Peer Container Isolation*

A P2P research testbed that runs libp2p Kademlia and PubSub experiments in isolated Peer containers across Linux Docker Swarm hosts.

**[Documentation](https://github.com/k-p2p-lab/kpl-v3/wiki/Home)** · [Getting started](https://github.com/k-p2p-lab/kpl-v3/wiki/getting_started) · [Architecture](https://github.com/k-p2p-lab/kpl-v3/wiki/architecture)

This repository contains the implementation and runnable examples. Project, research, deployment, and development documentation is maintained in the wiki.

[Examples](examples) · [Swarm operations](https://github.com/k-p2p-lab/kpl-v3/wiki/swarm_deployment)

Run `make` to build the single `bin/kpl` executable from `cmd/kpl-master`. Use `bin/kpl help`, `bin/kpl check` and `bin/kpl swarm help` for development and operations. The same executable runs Controller, Agent and Peer container roles. See [development and installation](https://github.com/k-p2p-lab/kpl-v3/wiki/development) and [Swarm deployment](https://github.com/k-p2p-lab/kpl-v3/wiki/swarm_deployment).

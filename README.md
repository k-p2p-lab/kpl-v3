# K-P2PLab v3

English | [한국어](README.kr.md)

K-P2PLab v3 runs configurable libp2p Kademlia and PubSub experiments on Docker Swarm across Linux hosts. The Controller schedules experiments and serves the Dashboard; Agents manage isolated Peer containers through each host’s Docker daemon.

This repository contains the implementation, deployment scripts, runnable YAML examples, and tests. All detailed documentation is maintained in the **[K-P2PLab Wiki](https://github.com/k-p2p-lab/v3/wiki/Home)**.

| Task | Wiki guide |
| --- | --- |
| Start | [Getting started](https://github.com/k-p2p-lab/v3/wiki/Getting-Started) · [Complete index](https://github.com/k-p2p-lab/v3/wiki/Documentation-Index) |
| Deploy and configure | [Swarm deployment](https://github.com/k-p2p-lab/v3/wiki/Swarm-Deployment) · [Agent capacity](https://github.com/k-p2p-lab/v3/wiki/Agent-Capacity) |
| Run and recover | [Scenarios](https://github.com/k-p2p-lab/v3/wiki/Scenario-Library) · [Execution and recovery](https://github.com/k-p2p-lab/v3/wiki/Experiments) |
| Observe and analyze | [Dashboard](https://github.com/k-p2p-lab/v3/wiki/Dashboard) · [Monitoring](https://github.com/k-p2p-lab/v3/wiki/Monitoring) · [Results](https://github.com/k-p2p-lab/v3/wiki/Results) · [Figures](https://github.com/k-p2p-lab/v3/wiki/Visualization) |
| Integrate and develop | [API](https://github.com/k-p2p-lab/v3/wiki/API) · [Architecture](https://github.com/k-p2p-lab/v3/wiki/Architecture) · [Development](https://github.com/k-p2p-lab/v3/wiki/Development) |

Deployment requires Linux and rootful Docker in an active Swarm. Follow the wiki prerequisites before deployment, and run documented shell commands from this repository root. Code, UI, and default documentation are in English; Korean wiki pages use the `-KO` suffix.

[`examples/`](examples) · [`scripts/`](scripts) · [`stack.swarm.yaml`](stack.swarm.yaml) · [Project Hub](https://github.com/k-p2p-lab/hub)

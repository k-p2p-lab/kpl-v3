# K-P2PLab v3 Documentation

English | [Korean](README.kr.md)

[Repository and quick start](../README.md) · [Hub concepts](https://github.com/k-p2p-lab/hub/blob/master/docs/CONCEPTS.md) · [Documentation policy](https://github.com/k-p2p-lab/hub/blob/master/docs/DOCUMENTATION.md)

This is the task map for the current implementation. The Hub owns project-wide concepts, research design, and publications. This repository owns commands, UI procedures, schemas, formulas, and operational limits.

## Reading paths

- **First deployment:** [Swarm](swarm.md) → [Agent capacity](agents.md) → [run an experiment](experiments.md) → [save results](results.md).
- **Design an experiment:** [scenario library](scenario-library.md) → [scenario reference](scenario-reference.md) → [protocol options](protocol-options.md) → [worked examples](#worked-experiments).
- **Failed or interrupted run:** [recovery choices](experiments.md#recover-saved-work) → [Agent cleanup and capacity](agents.md) → [logs and connection diagnosis](monitoring.md).
- **Interpret and report:** [metric definitions](experiment-metrics.md) → [bandwidth scope](bandwidth.md) → [result images](visualization.md) → [Hub reporting principles](https://github.com/k-p2p-lab/hub/blob/master/docs/RESEARCH.md).
- **Change the implementation:** [architecture](architecture.md) → [development](development.md) → the relevant reference below.

## Deployment and operation

| Guide | Owns |
| --- | --- |
| [Swarm deployment](swarm.md) | Host preparation, CLI/completion, networking, updates, shutdown, storage, and deployment validation |
| [Agent capacity and placement](agents.md) | Startup defaults, per-Agent overrides, proportional scheduling, reservations, and cleanup |
| [Monitoring and diagnosis](monitoring.md) | Prometheus/Grafana, metric exposition, access/authentication logs, and SSE diagnosis |

## Experiment workflows

| Guide | Owns |
| --- | --- |
| [Scenario library](scenario-library.md) | Save, validate, edit, and reuse scenario inputs |
| [Execution and recovery](experiments.md) | Run, repeat, stop, continue/retry, progress order, and estimated finish times |
| [Dashboard](dashboard.md) | Live panels, metric cards, keyboard/touch controls, and connection state |
| [Topology viewer](topology.md) | Node/edge meaning, layers, filters, freshness, lifecycle, and sampled graph evidence |
| [Saved results](results.md) | Browse, export, delete, retain, and back up source records and analysis files |
| [Result images and comparison](visualization.md) | Analysis jobs, repetition means, figures, comparisons, PNG/CSV/JSON exports |

## Reference

| Guide | Owns |
| --- | --- |
| [Scenario schema](scenario-reference.md) | YAML fields, phases/jobs, distributions, network conditions, defaults, and limits |
| [Protocol options](protocol-options.md) | PubSub, score, validator, discovery, and Kademlia configuration |
| [HTTP API and event contracts](api.md) | Endpoints, authentication, SSE framing, log schemas, download headers, and analysis jobs |
| [Experiment metric definitions](experiment-metrics.md) | Populations, delivery bounds, latency, duplicates, graph metrics, and estimates |
| [Bandwidth measurement](bandwidth.md) | Measurement layer, rates/bytes, protocol attribution, and collection quality |

## Worked experiments

| Guide | Owns |
| --- | --- |
| [Continuous churn and publishing](swarm-churn-publish.md) | Multi-host walkthrough, cumulative arrival schedule, expected population, and evidence |
| [Prysm scoring with three delay cohorts](swarm-churn-prysm-block.md) | Synthetic workload, baseline parameters, cohort scheduling, and interpretation limits |

## Development and compatibility

| Guide | Owns |
| --- | --- |
| [Implementation architecture](architecture.md) | Components, communication, networks, isolation, and source map |
| [Development guide](development.md) | Build, test, validate, deploy development images, and maintain documentation |
| [Performance investigation](churn-performance.md) | Historical churn/SSE evidence, benchmark inputs, and validation limits |
| [v2 reproduction mapping](v2-reproduction.md) | Execution/network mappings, intentional differences, and historical validation |
| [v2 analysis coverage](v2-analysis-coverage.md) | Source inventory, analysis/visualization mapping, definitions, and evidence limits |

The [version comparison manifest](v2-analysis-coverage.json) supports the v2 audit. Runnable YAML inputs live in [`examples/`](../examples), not in the Hub. Historical results and benchmarks describe their recorded environment; they are not current deployment verification.

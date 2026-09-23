# Agent Capacity and Peer Placement

English | [Korean](agents.kr.md)

[Documentation index](README.md) · [Repository](../README.md)

Select Agent hosts using the [Swarm deployment guide](swarm.md). This guide covers their Peer admission limits and placement; [API settings](api.md#agent-capacity-settings) define request/response and acknowledgment fields.

## Set defaults and per-Agent overrides

Capacity is a Peer-count admission limit, not a CPU or memory reservation or a cgroup limit. Changing the Agent's Swarm resource limits does not apply those limits to its sibling Peer containers. The supplied stack uses `KPL_AGENT_CAPACITY` as the startup default for every Agent. In **Dashboard → Agent status → Configure**, choose **Custom for this Agent** to override that default for the selected Agent ID, or **CLI default** to remove its override. For example, keep `KPL_AGENT_CAPACITY=200` and set one Agent to `100`; the other Agents continue to use `200`. Choose capacity based on measured CPU, RAM, file-descriptor, conntrack, and Docker daemon load.

Per-Agent overrides are saved in `<controller-data-dir>/agent-capacities.json` and survive Controller/Agent restarts when the Controller volume and Agent ID remain the same. Keep this file in Controller backups. A changed Agent ID is a different configuration target. An offline Agent receives its saved setting when it reconnects. Both Controller and Agents must run a version supporting these settings; the UI disables Configure for older Agents.

Capacity changes apply through registration/heartbeats without a service restart. The table shows the CLI default or override and **Applying…** until acknowledged. A reduction limits new placement immediately and leaves existing Peers running; if occupancy exceeds the new limit, joins wait for space. Increases become available after the Agent acknowledges the current setting. The Agent also enforces the effective limit locally, and its capacity metric follows it.

## Placement policies

| Setting | Placement behavior |
|---|---|
| `placement: balanced` (default) | Choose the online Agent with the lowest projected occupancy after adding one Peer, divided by effective capacity. Break ties by current utilization, then Agent ID |
| `placement: random` | Use the seed to choose among online Agents with available capacity |
| `placement: single-agent` | Pin one join execution to one selected Agent. Each repeat selects again. Wait if that Agent is full rather than switching servers |
| `agentId` | Pin placement to the specified Agent. Use an ID from `/api/v1/agents` for experiments pinned to a physical server |
| `parallelism` | Number of concurrent jobs in a join with `parallel: true`. This is separate from the server count or Swarm replica count |

Balanced placement uses each Agent’s effective capacity, including Dashboard overrides. With otherwise idle Agents of capacity 100 and 50, 75 Peer reservations distribute as 50 and 25. Occupancy includes all experiments, pending reservations, and containers awaiting removal. Existing Peers stay in place; new joins and slots released during churn bring the ratios back into balance. Explicit `agentId`, `random`, and `single-agent` policies retain their requested placement behavior.

| Agent | Effective capacity | Peers after 75 reservations | Utilization |
| --- | ---: | ---: | ---: |
| A | 100 | 50 | 50% |
| B | 50 | 25 | 50% |

This example starts with otherwise idle Agents. Equal Peer utilization does not imply equal CPU or memory usage.

## Admission, cleanup, and load

The Controller serializes reservations across concurrent experiments. An Agent holds a slot from accepting a creation request **until deletion is confirmed**, including time spent in Docker creation and startup; failed cleanup continues to occupy capacity. The Controller does not reuse a slot based only on a DELETE response. An Agent is excluded from new placement after more than 10 seconds without a report, such as during a network partition. Checks prevent reports from an earlier Agent process or out-of-order reports from overwriting current node and capacity data. A new Agent with the same ID can register after the previous process's online validity window expires.

Docker creation admission receives a 45-second budget. After admission, configuration copying, startup, and address inspection share a fresh 45-second budget. A shorter original deadline limits both stages. Cancellation during admission is held until its bounded result is collected so cleanup can identify a late-created container; startup steps are immediately cancelable. See [`internal/agent/docker.go`](../internal/agent/docker.go). If a busy server repeatedly exceeds these limits, lower join `parallelism` and capacity and investigate disk and daemon load.

Check [overlay and deployment limits](swarm.md#distribution-and-capacity) before increasing total capacity. Use [monitoring](monitoring.md) for observed readiness, cleanup queues, and process load, and the [performance investigation](churn-performance.md) for a repeatable development benchmark.

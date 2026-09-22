# Scenario Configuration Reference

English | [Korean](scenario-reference.kr.md)

This guide describes the version 2 YAML format, Peer profiles, protocol controls, network conditions, and background jobs. For saving reusable YAML in the Dashboard, see the [scenario library](scenario-library.md).

## Top-level fields

Scenario format versions are independent of K-P2PLab release versions: `version: 2` is the recommended YAML format for the v3 executable. The parser also accepts `version: 1`; omitted or zero `version` resolves to `1`. Both use the same current validation and runner. Unknown YAML field names are rejected.

| Field | Required or default | Meaning |
|---|---|---|
| `version` | Recommended: `2` | Scenario schema version, not the application release. |
| `name` | Required, non-blank | Run name; independent of a saved library record's name. |
| `seed` | `0` | Non-zero seed controls application sampling; zero selects a time-based seed. |
| `onExit` | `cancel` | `cancel` or `drain` policy for background jobs at natural completion. |
| `jobShutdownTimeout` | `3m`, positive | Bound for job shutdown and a separate bound for Peer cleanup; not a total run timeout. |
| `profiles` | Empty map | Reusable per-node configuration overlays. |
| `phases` | At least one phase | Ordered actions, optionally starting tracked background work. |

The executable schema and validation are in [`internal/scenario/scenario.go`](../internal/scenario/scenario.go); scheduling is in [`internal/controller/runner.go`](../internal/controller/runner.go).

## Node roles, types, and profiles

`role` controls Kademlia bootstrap discovery: a `boot` node is advertised by the Controller as a bootstrap peer, while a `worker` node is not. Topic transport discovery is separate and considers ready PubSub participants in the same run and exact topic. `type` controls the node's libp2p, Kademlia, and PubSub behavior. These concepts are independent, so an experiment can use several worker behaviors without abusing the bootstrap role. When both `type` and `profile` are omitted, `role: boot` selects the `boot` preset and every other role selects `full`.

| Built-in type | Behavior |
|---|---|
| `boot` | Kademlia-only bootstrap node by default, matching v2 behavior. |
| `full` / `worker` | Full Kademlia server and standard GossipSub participant with the v2 hard connection cap of `55`. |
| `light` | Kademlia client with a lower default connection limit. |
| `publisher` | Publish-only topic participant. |
| `subscriber` / `observer` | Subscribe-only participant; publishing is rejected. |
| `relay` | Uses `Topic.Relay()` to forward topic traffic without an application subscription or application publishing. |
| `flood` | Uses FloodSub. |
| `random` | Uses RandomSub with separate minimum-degree and estimated-network-size controls. |
| `dht-only` | Runs Kademlia with PubSub disabled. |
| `gossip-only` | Runs PubSub with Kademlia disabled. |
| `non-gossip` / `mesh-only` | Uses the GossipSub mesh with lazy gossip disabled. |

Reusable node profiles belong in the top-level `profiles` map. A join phase resolves its configuration in this order: built-in `type`, named `profile`, then the phase's inline `node` overrides. If the phase omits `type`, the named profile's `type` chooses the preset; otherwise the role supplies the default. Pointer-backed boolean and numeric controls preserve explicit `false` and `0`, which allows experiments such as disabling lazy gossip with `historyGossip: 0`. This does not extend to legacy flat numeric fields, whose zero values mean omitted; use the nested controls for explicit zero overrides.

The built-in `boot` type sets `gossipsub.enabled: false`. Set it explicitly to `true` in a profile or inline `node` block when bootstrap nodes should also participate in PubSub. Once enabled, boot nodes can use the router-appropriate parameters, scoring, and inspection options described below; [`examples/mixed-workers.yaml`](../examples/mixed-workers.yaml) demonstrates this opt-in. PubSub-enabled Peers poll the Controller's same-run, exact-topic registry immediately after startup, then on a three-second loop. Each Peer ranks the full eligible set with rendezvous hashing and fills the deficit between its current topic peers and `DHigh`, subject to its total connection budget and retry backoff. See [bootstrap and topic discovery](topology.md#bootstrap-and-topic-discovery) for timing and fallback behavior. GossipSub still forms the actual GRAFT mesh; DHT bootstrap, transport candidacy, and mesh membership remain distinct.

```yaml
version: 2
name: mixed-workers
seed: 42
onExit: cancel
jobShutdownTimeout: 3m

profiles:
  tuned-mesh:
    type: full
    libp2p:
      connectionLimit: 128
      connectionManager:
        lowWater: 64
        highWater: 96
        gracePeriod: 30s
    kademlia:
      mode: server
      protocolPrefix: /k-p2p-lab/v3
      bucketSize: 20
      concurrency: 10
    gossipsub:
      router: gossipsub
      topics: [kpl/default]
      floodPublish: false
      peerExchange: true
      params:
        d: 8
        dLow: 6
        dHigh: 12
        dOut: 3
        dLazy: 8
        heartbeatInterval: 500ms

phases:
  - action: join
    group: tuned
    role: worker
    profile: tuned-mesh
    count: 100
    parallel: true
    parallelism: 16
```

### Protocol controls

The nested node configuration exposes scalar parameters and named callback/interface policies supported by the pinned libp2p packages. The [complete protocol reference](protocol-options.md) lists every mesh, score, PeerGater, filter, validation, discovery, publication, and Kademlia field; see also [the protocol example](../examples/protocol-options.yaml). Go duration strings such as `250ms`, `30s`, and `5m` are used for every duration field.

| Section | Configurable fields |
|---|---|
| `libp2p` | `userAgent`, `natPortMap`, `relay`, `relayService`, `connectionLimit`, `connectionManager.lowWater`, `connectionManager.highWater`, `connectionManager.gracePeriod`, `dialTimeout` |
| `kademlia` | `enabled`, `mode`, `protocolPrefix`, `protocolId`, `protocolExtension`, `bucketSize`, `concurrency`, `resiliency`, `lookupCheckConcurrency`, routing-table latency/refresh durations, `maxRecordAge`, provider/value/auto-refresh switches, optimistic-provide settings, bootstrap timeout/retry interval, and filter/diversity/datastore/validator/providerStore/bootstrapSource/requestHook policies |
| `gossipsub` | `enabled`, `router`, `topicMode`, `randomDegree`, `randomNetworkSize`, `subscribe`, `allowPublish`, `topics`, `floodPublish`, `peerExchange`, message/queue/validation limits, `signaturePolicy`, `seenMessagesTTL`, `subscriptionBufferSize`, `scoreInspectInterval`, and message-ID/topicOptions/filter/protocol/PeerGater/validator/RPC-inspector/discovery/author/publish policies |
| `gossipsub.params` | Every `GossipSubParams` field in the pinned library: mesh degrees, history, gossip factor/retransmission, heartbeat, fanout, prune/backoff, connectors, connection timeout, direct-connect and opportunistic-graft controls, IHAVE limits, IDONTWANT limits, and IWANT follow-up time |
| `gossipsub.score` | Explicit `enabled` switch, global PeerScore weights including declarative P5 and decay settings, score thresholds, IP-colocation whitelist, and a complete `topics.<topic>` score block for mesh, first-delivery, failure, and invalid-message terms |

GossipSub-only controls (`params`, active score/inspection, `floodPublish`, `peerExchange`, `directPeers`, and `peerGater`) apply only to the `gossipsub` router. FloodSub and RandomSub reject active scoring but can retain an inactive tuning block.

The base mesh defaults are `D=6`, `DLow=5`, `DHigh=12`, `DScore=4`, `DOut=2`, `DLazy=6`, history `5/3`, gossip factor `0.25`, and a `1s` heartbeat. When PubSub is enabled on `boot`, that preset uses `DScore=3`. `full`/`worker` and the role-focused GossipSub worker presets inherit the v2-derived `DLow=5`, `DScore=3`, `maxIHaveLength=5500`, and `1s` initial heartbeat. Most worker presets also use the v2 hard connection limit of `55`; `light` uses `32`. Kademlia defaults to bucket size `20` and protocol prefix `/k-p2p-lab/v3`. See [`internal/model/config.go`](../internal/model/config.go) for resolved defaults and the [protocol reference source map](protocol-options.md#source-boundaries) for the split typed schema.

`libp2p.connectionLimit` is only a resource-manager hard cap on total connections; it does not implicitly create a soft connection manager. The soft low-water/high-water trimming behavior is installed only when `libp2p.connectionManager` is explicitly present, using its `lowWater`, `highWater`, and `gracePeriod`. This lets a profile choose the hard cap, the soft manager, or both independently.

When migrating a v2 configuration, map v2's misleadingly named `protocol_id` to v3 `kademlia.protocolPrefix`: v2 used that value as a prefix. In v3, `kademlia.protocolId` instead overrides the exact Kademlia V1 wire protocol ID; it is mutually exclusive with `protocolPrefix` and `protocolExtension`, so do not copy a v2 prefix into it. The default prefix `/k-p2p-lab/v3` is an intentional new protocol namespace. Use `protocolPrefix: /k-p2p-lab/kad-dht` only when an experiment requires the legacy v2 namespace.

For RandomSub, `randomDegree` sets the minimum connection/degree target through libp2p's process-global `RandomSubD`. KPL isolates each Peer in its own container, so that global applies to one Peer. `randomNetworkSize` is the separate estimated network-size argument passed to `NewRandomSub`.

Scoring is **disabled by default**. Only `gossipsub.score.enabled: true` enables it; a score block or inspection interval alone does not. When explicitly enabled, `scoreInspectInterval` defaults to `1s`, while `0s` disables inspection. Each inspection updates the node's `peerScores` map. It is returned by `/api/v1/nodes`, `/api/v1/network`, `/api/v1/snapshot`, and the SSE snapshot stream; selecting a node in the topology displays the observed score count and average in its details.

`appSpecificScore` supplies P5 through a fixed `default` value and optional `peers` map keyed by libp2p Peer ID; explicit peer values, including zero, override the default. A nonzero active `appSpecificWeight` requires that policy. All values and weighted P5 results must be finite. Active selective validation safely defaults omitted decay groups to `1s`/`0.01` and an omitted disabled P1 quantum to `1s`; explicit zero ticker/divisor durations are rejected. Profile-to-inline `score` overrides replace the complete score block and preserve explicit zero/false values. See [scoring configuration](protocol-options.md#peer-scoring-explicitly-disabled-by-default).

### Per-node network conditions

Add `network` to a profile or a join phase's `node` block. The default `scope: p2p` shapes outgoing P2P TCP on port `20000` inside each Peer container. Select `scope: all` to reproduce v2's shaping of all egress, including control HTTP and telemetry. A configured delay is a one-way egress delay, not a round-trip latency target. `rateMbps` and `tbf.rateMbps` configure egress limits; [bandwidth measurement](bandwidth.md) separately records actual libp2p stream usage.

```yaml
node:
  network:
    delay: 100ms
    jitter: 10ms
    lossPercent: 1
    duplicatePercent: 0.1
    corruptPercent: 0.1
    reorderPercent: 1
    rateMbps: 10
    queueLimit: 1000
```

| Field | Meaning |
|---|---|
| `delay`, `jitter` | Non-negative Go durations for added delay and its variation. Jitter requires a positive delay and is capped at `2147483647ns` by Linux netem. |
| `lossPercent`, `duplicatePercent`, `corruptPercent`, `reorderPercent` | Packet percentages from `0` to `100`; reordering requires a positive delay. |
| `rateMbps` | Non-negative egress rate in megabits per second; `0` disables the rate limit. |
| `queueLimit` | Maximum packets in the netem queue, from `1` to `4294967295`; supplying it alone enables netem. |
| `scope` | `p2p` (default) or `all` outgoing traffic. |
| `delayDistribution` | Samples one base delay per Peer using the interval distribution schema; mutually exclusive with `delay`. A positive `min` is required when jitter or reordering is enabled. |
| `jitterDistribution` | `normal` (default), `uniform` (v2 behavior), `pareto`, or `paretonormal`. |
| `reorderCorrelationPercent` | Reordering correlation from `0` to `100`, corresponding to v2's `reorder.chance`; a positive value requires positive `reorderPercent`. |
| `tbf` | Token bucket with positive finite `rateMbps` and `burstKbit` (kilobits), and `latency` from `1us` to `4294967295us`; mutually exclusive with positive netem `rateMbps`. |

See the [v2 reproduction audit and mapping](v2-reproduction.md) and [churn example](../examples/v2-churn.yaml). They cover placement (`balanced`, per-node `random`, batch `single-agent`, or explicit `agentId`), `onError: continue` for churn, `payloadEncoding: raw` for exact PubSub data length, and `topic: '*'` for all topics. Network scope and payload encoding retain their existing defaults. Worker bootstrap now uses seeded first-success selection, and the transport stack is explicitly TCP/Noise/Yamux.

Peers with network conditions require Linux `NET_ADMIN` and host-kernel `sch_netem` support. The Docker runtime adds `NET_ADMIN` only to those Peers. Missing kernel support or a failed `tc` command fails node startup explicitly.

[`examples/network-conditions.yaml`](../examples/network-conditions.yaml) creates two bootstrap nodes and four constrained workers, waits for initialization, publishes sample messages, and stops all nodes. Run it from the dashboard or submit it with:

Replace `control-node:8080` with the Controller address printed by `access`. First follow [API authentication](api.md#authentication) to create `KPL_COOKIE_JAR`.

```bash
curl -X POST http://control-node:8080/api/v1/experiments \
  -H 'Content-Type: application/yaml' \
  -b "${KPL_COOKIE_JAR:?Log in first}" -H 'X-KPL-Request: dashboard' \
  --data-binary @examples/network-conditions.yaml
```

## Scenarios and jobs

The recommended scenario format is version 2 YAML. It preserves the important v2 execution controls while adding explicit job tracking and readiness barriers. The old line-oriented `.kpl` DSL is not parsed directly; translate its commands into phases.

| Action | Purpose |
|---|---|
| `join` | Creates nodes using available Agent capacity. |
| `wait-ready` | Waits until a target percentage of a group/type is ready, optionally after named jobs or a `minCount` floor. |
| `publish` | Selects ready nodes from a group to publish messages. `deliveryWindow` sets the per-message receipt deadline (default `10s`, positive, at most `1h`). |
| `leave` | Selects and stops nodes from a group. |
| `wait` / `sleep` | Waits for a fixed duration. `sleep` is a v2-compatible alias. |
| `wait-jobs` | Waits for selected background job IDs, or all jobs when `jobs` is empty. |
| `log` | Writes a scenario message to the Controller log. |
| `stop-all` / `reset` | Cancels and drains every background job, then uses the run-generation fence to stop the experiment's current and older Peer generations. `reset` is an alias. |

For `join`, `count` is the exact number of create operations. For `publish` and `leave`, it is a maximum capped by the number of eligible candidate nodes; publish candidates must have PubSub and publishing enabled and must have joined the requested topic. `repeat` repeats the entire phase. `parallel` selects sequential or concurrent replica execution, while `parallelism` optionally caps concurrent operations. `await` defaults to `true`; setting it to `false` starts a tracked background job named by `job`, allowing later phases to run while churn or publishing continues. A `wait-jobs` phase can join selected jobs by name.

### Phase fields and defaults

| Fields | Applies to | Meaning and default |
|---|---|---|
| `name`, `repeat` | All actions | Display name defaults to `<action>-<phase number>`; omitted or zero `repeat` resolves to `1`. |
| `job`, `await` | `join`, `publish`, `leave` | `await: false` creates a background job; omitted `job` becomes `phase-<phase number>`. Job IDs must be unique until `stop-all` resets tracking. |
| `group`, `count` | `join`, `publish`, `leave` | Required group and positive operation count. Each publish repetition selects distinct eligible publishers up to `count`. |
| `type` | `join`, `publish`, `leave`, `wait-ready` | Chooses the join preset or filters existing nodes by resolved type. |
| `role`, `profile`, `node` | `join` | Role is `boot` or `worker` (default); profile and inline node settings customize the preset. |
| `placement`, `agentId` | `join` | `balanced` (default) chooses by utilization, `random` chooses per node, and `single-agent` chooses one Agent per batch; explicit `agentId` pins placement. Admission waits for available capacity. |
| `parallel`, `parallelism` | `join`, `publish`, `leave` | Default is sequential. Omitted/zero parallelism allows the whole batch to run concurrently when parallel is enabled. |
| `interval`, `lifetime` | Paced operations; lifetime only on `join` | See timing rules below. Peer lifetime starts after successful container creation and includes configuration copy, start, and bootstrap. It is independent of background job completion. |
| `topic`, `payloadSize`, `payloadEncoding` | `publish` | Topic defaults to the publisher's first configured topic; `'*'` publishes to all its configured topics. Non-positive `payloadSize` resolves to `32`. Default `envelope` adds JSON/base64 metadata; `raw` makes PubSub data exactly `payloadSize` bytes. |
| `deliveryWindow`, `onError` | `publish`; `onError` also on `leave` | Window defaults to `10s`. `onError: fail` is the default; `continue` records individual operation failures and continues churn. A publish with no eligible candidate is a no-op under `continue`. |
| `group`, `type`, `readyRatio`, `minCount`, `jobs`, `timeout` | `wait-ready` | Empty group/type matches all current-generation nodes. Ratio defaults to `1`; timeout defaults to `1m` and covers both job waiting and readiness. `minCount` is a cohort-size floor. |
| `jobs`, `timeout` | `wait-jobs` | Empty jobs means all tracked jobs; timeout defaults to `5m`. |
| `duration`; `message` | `wait` / `sleep`; `log` | Positive wait duration; Controller log message. |

Background-job behavior at the natural end of the phase list is controlled by top-level `onExit`. Its default, `cancel`, cancels remaining jobs and then waits for them to stop. `onExit: drain` instead waits for them to complete naturally. A naturally successful single run applies this job policy but leaves Peer containers running unless the scenario contains an explicit `stop-all`. A Dashboard/API batch submitted with `repetitions > 1` automatically fences and cleans up Peers after every iteration, including the last, so they cannot overlap the next run. This is separate from a YAML phase's `repeat` field.

`jobShutdownTimeout` defaults to `3m`. When a user or API request cancels a scenario, or when any scenario phase or background job fails, the Controller cancels outstanding jobs and waits for their termination within this bound, then asks every Agent to generation-fence and clean up Peer containers through the current generation. An explicit `stop-all` uses the same bounded job shutdown, resets job tracking, and fences the current run generation. The Agent records the monotonically increasing fence before stopping matching containers: a late create at generation N either committed before the fence and is included in cleanup, or is rejected because its generation is at or below the fence. After `stop-all` succeeds, the scenario advances to generation N+1, so later phases may create new nodes under the same run ID and may reuse job IDs.

`wait-ready` evaluates the complete matching cohort in the current run generation, including failed, stopping, and stopped nodes; nodes from previous generations are ignored. A failed cohort member prevents the barrier from succeeding, and a node reported as ready contributes to the ready count only while its Agent is online. Because the cohort still contains only nodes observed so far, `wait-ready` after an `await: false` join must specify either `jobs: [job-id]` or `minCount`. `jobs` waits for the selected producer jobs to finish before checking readiness; `minCount` leaves them running but prevents a partially created group from satisfying the ratio too early.

| `parallel` | `await` | Behavior |
|---|---|---|
| `false` | `true` | Paced replicas execute sequentially; the next phase waits. |
| `true` | `true` | Replicas execute concurrently; the next phase waits for the batch. |
| `false` | `false` | A paced sequential job runs in the background. |
| `true` | `false` | A concurrent job runs in the background. |

For v2 compatibility, a `publish` phase with no `interval` has a special default: with `parallel: true`, every operation gets a `1s` phase-start offset; sequential publish uses zero delay. This rule is independent of `await`.

Sequential `join`, `publish`, and `leave` run the first operation immediately, then schedule each operation at the batch start plus the sum of preceding sampled intervals. Request/admission time consumes the interval instead of extending it. If dispatch falls behind, remaining operations execute in order without extra waiting until they catch up; arrivals are not skipped. Cancellation and errors still stop dispatch, and Agent capacity still limits admission. Parallel `join` and `leave` ignore `interval`. Parallel `publish` uses each sampled interval as that operation's independent offset from the batch start, before acquiring a concurrency slot; offsets are not cumulative and capacity can delay dispatch further.

For sequential joins, the Controller logs `churn join schedule delayed` when dispatch is at least one second late, at most once every ten seconds per job execution. The log includes the run, group, operation number, scheduled time, and delay. This exposes sustained capacity or API bottlenecks; the schedule cannot guarantee Docker startup or readiness times under overload. Agent heartbeats run independently of event forwarding, so a slow telemetry request does not pause lease renewal.

`wait` durations and readiness/job timeouts must be positive. An omitted join `lifetime` means no automatic leave, while an explicitly sampled `0s` lifetime stops the new node immediately, matching v2.

```yaml
phases:
  - name: background churn
    action: join
    job: churn
    await: false
    parallel: false
    group: churners
    role: worker
    type: light
    count: 100
    interval: {model: exponential, mean: 250ms}
    lifetime: {model: pareto, xm: 45s, alpha: 2.5, max: 3m}

  - action: wait
    duration: 10s

  - action: wait-ready
    group: churners
    jobs: [churn]
    readyRatio: 1
    timeout: 5m
```

Intervals and lifetimes support these distributions. Duration-valued fields use Go duration strings, and optional `min`/`max` duration bounds clamp the result.

| Model | Parameters and sampling semantics |
|---|---|
| `fixed` | `value` is the exact duration. |
| `exponential` | `mean` is the mean duration of a continuous exponential sample. |
| `normal` | `mean` and `sigma` are duration-valued mean and standard deviation. Negative samples are clamped to zero before optional bounds are applied. |
| `pareto` | `xm` is the scale duration and `alpha` is the dimensionless shape. |
| `poisson` | v2-compatible behavior: `mean` is a duration, converted to seconds as the Poisson mean; the sampled result is quantized to an integer number of seconds. |
| `gamma` | `alpha` is the shape. Supply exactly one of v2-compatible `beta` (a rate per second) or `scale` (a duration); the two forms are mutually exclusive. |
| `lognormal` | `mu` is the log-space mean and sigma is dimensionless: use exactly one of a numeric string such as `sigma: "0.5"` or the numeric `logSigma` field. The sample is `exp(mu + sigma*Z)` seconds for standard normal `Z`. |

A non-zero scenario seed reproduces sampled delays, distribution samples, and random selection/order when the initial state, eligible candidate set, and readiness/job barriers are the same. Peer identity is deterministic per `(run ID, per-node seed)`: the same pair yields the same Peer ID, while different run IDs intentionally yield different Peer IDs and avoid cross-run identity collisions. Thus the same scenario seed reproduces sampling and ordering across runs, but not Peer IDs when their run IDs differ; `seed: 0` intentionally chooses a time-based seed. Start with [`examples/smoke.yaml`](../examples/smoke.yaml), then see [`examples/mixed-workers.yaml`](../examples/mixed-workers.yaml) for custom profiles, heterogeneous worker types, bounded parallel batches, and background paced jobs.

## Execution and retention limits

The Docker runtime isolates each Peer and supports per-node P2P egress conditions. `wait-ready` confirms Peer initialization and API readiness; it does not verify mesh convergence. Periodic topic discovery repairs transport candidates under churn, while GossipSub heartbeat and GRAFT processing still need time to converge. Add a settling phase when the experiment needs it. Scenario seeds reproduce application sampling and ordering under the documented conditions, but do not promise identical kernel packet impairment or network timing. HopWave is not supported.

Stopped-node history is currently retained in memory and included in Agent heartbeats and Controller snapshots. Long-running, high-volume churn still needs a bounded retention policy and a separate paginated history API to prevent control-plane state and payloads from growing indefinitely.

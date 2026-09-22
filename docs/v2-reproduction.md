# Reproducing v2 scenario and network experiments

English | [Korean](v2-reproduction.kr.md)

This review uses the actual Controller/Peer code in `kpl-v2` and `exp/exp-2603_churn-02.sh` as its reference. Where documentation and implementation differ, the implementation takes precedence. The settings below reproduce the main experimental conditions; they do not reproduce the exact packet order or topology of a historical run.

These compatibility details belong to v3. For the project-wide version history and research context, see the [Hub](https://github.com/k-p2p-lab/hub).

## Container isolation and placement

Both versions run one Peer per container. v2 uses Swarm Services; v3 uses Docker containers created by an Agent. v3 Peers receive no host port bindings, host filesystem mounts, or Docker socket. Queueing disciplines (qdiscs) are installed only in each Peer's network namespace, and `NET_ADMIN` is granted only to Peers that need it. Neither v2 nor this configuration imposes CPU or memory limits, so container isolation does not isolate experiments from host load.

For a parallel join, v2 randomly selects one Worker host and sends ServiceCreate requests sequentially. The corresponding v3 settings are `parallel: true`, `parallelism: 1`, and `placement: single-agent`. A sequential join in v2 selects a host again for each node, corresponding to `parallel: false` and `placement: random`. Set `agentId` to pin placement to a specific Agent. The default `placement: balanced` policy distributes load.

Swarm runs **one Agent on each selected node**, with Peer containers connected through a shared attachable overlay. To experiment with inter-host latency, select at least two Agent nodes using the [Swarm workflow](swarm.md). Agent capacity waits, Docker CLI overhead, and actual machine load can cause the observed start rate to differ from the target join rate. v2's automatic Swarm recovery also differs from v3's recording of terminal states.

See the [Swarm deployment configuration and scalability review](swarm.md) for multiple servers. Swarm places a global Agent on each selected server, while the Controller distributes Peers. Restarting an Agent task cleans up its Peers; it does not reproduce v2's Peer Service rescheduling. The implementation accounts for Peer advertised addresses, direct access to each Agent, capacity reservations until deletion completes, and monitoring of all Agents.

If an experiment is canceled during creation, the Agent still waits for Docker create to return within its time limit, obtains the container ID, and removes the container. This avoids leaving a container that the daemon creates after a prematurely terminated CLI invocation. If Docker reports that removal is in progress, the Agent checks whether the container actually disappears. The default `jobShutdownTimeout` is `3m`, applied separately to job shutdown and Peer cleanup. Swarm sets the Controller's stop grace period to `10m` and each Agent's to `4m`; see the [shutdown procedure](swarm.md#failures-updates-and-shutdown). A Docker admission deadline or daemon failure is reported as an error; on restart, the Agent uses ownership labels to clean up leftover containers.

## Network condition mapping

| v2 setting or behavior | v3 setting |
|---|---|
| Apply netem to all egress on `eth0` | `network.scope: all` (the default `p2p` scope affects only P2P TCP traffic) |
| Draw one value per Peer from `delay.distribution` | `network.delayDistribution` |
| `delay.deviation` in milliseconds, for packet jitter | `jitter: 5ms`, `jitterDistribution: uniform` |
| `loss`, `corrupt` | `lossPercent`, `corruptPercent` |
| `reorder.percentage`, `reorder.chance` | `reorderPercent`, `reorderCorrelationPercent` |
| `tbf.rate`, `tbf.burst`, `tbf.latency` | `tbf.rateMbps`, `tbf.burstKbit`, `tbf.latency` |

v2's `chance` is actually the reordering **correlation** in the generated tc command. Standalone `rateMbps` applies netem packet serialization delay and is not equivalent to TBF. TBF cannot be combined with a positive `rateMbps`. TBF latency must be within tc's supported range of `1us` to `4294967295us`; fractional microseconds are rounded up.

```yaml
network:
  scope: all
  delayDistribution:
    model: normal
    mean: 50ms
    sigma: 10ms
    min: 1ms
  jitter: 5ms
  jitterDistribution: uniform
  lossPercent: 1
  reorderPercent: 2
  reorderCorrelationPercent: 25
  tbf:
    rateMbps: 10
    burstKbit: 64
    latency: 100ms
```

`delayDistribution` is sampled once using the Peer seed, independently of per-packet jitter. It is mutually exclusive with `delay`. A positive `min` is required when jitter or reordering is enabled. v2 truncated samples in seconds to integer milliseconds; v3 retains Go duration precision. v2's time-based randomness and gamma implementation bugs are not reproduced.

`scope: all` can delay or drop control API responses, bootstrap lookups, and telemetry. Failures of `wait-ready` or publish HTTP requests under high loss are part of those experimental conditions. Both versions shape egress traffic. If delay is configured on two Peers, both egress delays contribute to the round trip. Kernel offload, TCP retransmissions, and queue state mean that application message loss does not necessarily equal the configured packet loss.

## Join, leave, and publish scheduling

| Meaning | Mapping or caveat |
|---|---|
| Sequential interval | v2 waits after completion; v3 uses cumulative batch-start deadlines to avoid admission/RPC drift during long churn. Late operations catch up in order, without skipping arrivals. No wait follows the final operation. |
| Parallel join/leave | Ignore interval |
| Parallel publish | Independent initial delay for each publisher; defaults to 1 second when interval is omitted |
| Publish/leave replicas | Capped at the number of candidates, with no duplicate selections |
| v2's default await=false | Specify `await: false` explicitly in YAML (v3 defaults to true) |
| v2 asynchronous for-loop | Expand into separate job phases; `repeat` executes sequential repetitions within one job |
| Publish target disappears during churn | Use `onError: continue` to record that failure and continue |
| Clean up all Peers after a successful single run | Add an explicit final `stop-all`; repeated runs (`repetitions > 1`) also clean up automatically between iterations |

`onError: continue` applies only to publish/leave and also permits zero candidates. Individual operation failures are recorded as `phase-operation-failed` events. User cancellation and phase deadlines still propagate; an individual HTTP request timeout is treated as an operation failure. The default `fail` policy stops the experiment on failure, preserving the existing v3 behavior.

A Docker Peer's lifetime starts when **Docker create succeeds**, corresponding to v2's ServiceCreate return. Configuration copying, container startup, and bootstrap time all count toward that lifetime. It is not the time spent online after `ready`. Node metadata distinguishes these stages with `lifetime`, `lifetimeBasis`, `containerCreatedAt`, `containerStartedAt`, `readyAt`, `stopRequestedAt`, and `stoppedAt`. Each timestamp records when the Agent observed the state; `startedAt` is when it accepted the creation request. Lifetime expiry forces the Peer to leave. v2 Peers also had no application-level graceful shutdown procedure.

[`examples/v2-churn.yaml`](../examples/v2-churn.yaml) adapts the original churn scenario to a smaller scale and adds network conditions. It does not establish equivalent performance for the original scale of 10,000 join requests and hundreds of concurrent Peers.

## Initial connections and message size

Peers explicitly use TCP/Noise/Yamux, as in v2. Workers shuffle bootstrap candidates using their seed and finish initial connection setup after the first successful connection. The overall bootstrap timeout also covers list lookup and dialing. After PubSub startup, v3 additionally polls the Controller's same-run, exact-topic discovery registry every three seconds to supplement transport connections. Candidate ranking, connection budgets, and failure retries are implemented in [`internal/peer/discovery.go`](../internal/peer/discovery.go); GossipSub chooses the actual mesh. This extra discovery path differs from v2 and can change connectivity and churn recovery even when DHT/GossipSub parameters match.

v2's `size` is the length of the random bytes passed to PubSub. With `payloadEncoding: raw` in v3, `payloadSize` is exactly that length. It is not the packet size including libp2p framing, signatures, or TCP/IP headers. The default `envelope` encoding is larger because it adds JSON and base64 for propagation latency measurement. `topic: '*'` publishes a message separately to every publishable topic held by each selected node.

Raw publications, deliveries, and duplicate events use `pubsub-` followed by the hexadecimal native PubSub message ID. The current default ID combines the origin and sequence, so two publications with identical raw bytes remain distinct. Explicit content-based message-ID policies can merge identical content; see [message-ID configuration](protocol-options.md#pubsub-activation-and-common-controls). No application timestamp is inserted into the message, so raw deliveries provide no latency value and are excluded from mean/P95 calculations. Use dedicated topics and networks for runs with raw traffic: the envelope run ID filter cannot be applied to raw bytes. See [`internal/peer/publish.go`](../internal/peer/publish.go).

## Observability and remaining differences

- `wait-ready` indicates initialization and API readiness; it does not guarantee mesh convergence. The example includes a separate stabilization wait.
- The [dashboard topology](topology.md) has separate transport, Kademlia routing-table, and topic-specific GossipSub mesh layers from current Peer status. `TopicPeers` is `len(pubsub.ListPeers(topic))`: locally known topic peers, not mesh degree or proof of remote application subscription sessions. For historical analysis, use the stored `graft`/`prune`, `add_peer`/`remove_peer`, and `join`/`leave` events together, allowing for telemetry loss. New `observations.jsonl` records include group summaries and sampled nodes/edges. Saved protocol graphs merge topic relationships and do not preserve every intermediate routing/mesh transition. See [topology](topology.md#api-and-retained-data) for the retained format.
- v3 control metrics correspond to v2's physical IHAVE/IWANT counters through the per-type RPC count and to its logical counters through the message-ID reference count. v3 also retains protobuf entry counts, IDONTWANT, local pre-send drops, PRUNE peer-exchange records, and every type in a mixed RPC. v2 omitted those cases and discarded IHAVE topic information. Existing plain `graft`/`prune` events remain local mesh transitions; compare wire traffic with the separate `send_*`, `recv_*`, and `drop_*` control events. See the [control traffic definition](experiment-metrics.md#gossipsub-control-traffic).
- Interval, lifetime, and base delay samples are reproducible from their seeds. Peer IDs derived in part from the run ID, network timing, and kernel packet randomness are not identical across runs. Node metadata records `seed`, `networkRequested`, and the effective `network`; the Agent's Peer configuration files also retain effective settings.
- [Additional research analysis](experiment-metrics.md#saved-result-research-metrics) provides v2 chart families under a separate definition. Its populations, windows and aggregation differ from main session metrics; Eager/Lazy is estimated from GRAFT/IHAVE/IWANT and message IDs. Unavailable sender-queue causes and missing historical edges are not fabricated. See [v2 analysis coverage](v2-analysis-coverage.md) for chart and import support.
- The default connection cap of 55 and key Worker DHT/GossipSub parameters match. However, source for v2's custom PubSub fork is absent from the supplied directory, so equivalence inside that fork cannot be verified. v3 uses the official library, and HopWave is outside the supported scope.
- Dashboard message metrics accumulate independently of the recent-event buffer, but reset when the Controller restarts and remain subject to telemetry loss. [Saved-result analysis](visualization.md) and ZIP metrics reconstruct the recorded run after restart. Apply the [explicit churn cohort definitions](experiment-metrics.md) and consult the [complete v2 analysis audit](v2-analysis-coverage.md); matching scenario settings do not establish numerical or visualization parity.

Key v2 files reviewed: `kpl-controller/internal/handler/event.go`, `internal/docker/docker.go`, `internal/distribution/distribution.go`, `cmd/main.go`, `kpl-peer-app/internal/host/{host,tc}.go`, `internal/dht/dht.go`, and `internal/api/publish.go`.

## Historical validation record

The following retained record reports validation on Docker Desktop Linux on 2026-09-04. Its original run archives are not included in this repository; treat the numbers as historical observations, not verification of the current checkout. For current regression commands and the distinction between package tests and kernel integration, see [development](development.md#container-and-browser-regression-checks).

- The full `go test -buildvcs=false ./... -timeout 60s` suite and validation of every example YAML file passed.
- Integration run `run-20260904T021417Z-581b`: confirmed four containers with distinct network namespaces and no host mounts or port bindings.
- Installed both P2P-only netem→TBF and all-egress netem→TBF in the real kernel. Confirmed TBF at `10Mbit`, burst `8KB` (64 kbit), and latency `100ms`.
- Peers using the same Worker profile received independently sampled base delays of `57.008524ms`, `50.467284ms`, and `47.92387ms`. Confirmed loss `1%`, reordering `2%`, correlation `25%`, and packet drop counters in the actual qdiscs.
- Published six raw messages across two topics from three Workers. Every PubSub data payload was exactly 32 bytes. Confirmed 24 delivery events, including self-delivery, and a reach ratio of 1. Every raw latency value was recorded as unmeasured.
- Final churn run `run-20260904T022448Z-0f9d`: 16 Peers joined over about 86 seconds, with three publications and eight deliveries. One publication failed because its selected Peer had left, but the failure was recorded as `phase-operation-failed`, and the experiment continued to normal completion. All Peers reached stopped state, with zero remaining managed containers.

This validation establishes configuration application and real communication at a small scale. It does not measure maximum TBF throughput, overlays across multiple physical hosts, or churn performance with hundreds or thousands of Peers.

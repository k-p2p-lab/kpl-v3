# P2P bandwidth measurement

English | [Korean](bandwidth.kr.md)

[Documentation index](README.md) · [Repository](../README.md)

v3 measures **libp2p stream usage**, not physical link capacity or available capacity. Rebuild and apply Controller, Agent and Peer together, then start a new experiment. Old Agents reject the new typed event field. Historical payload sizes cannot reconstruct this traffic.

## Scope and calculation

A synchronous `metrics.Reporter`, installed through libp2p `BandwidthReporter`, records the actual byte counts returned by stream reads/writes. The pinned dependency reports these counts through its `swarm_stream.go` hooks. Global hooks update totals; stream hooks update protocol/remote attribution without double counting. Unlike the upstream asynchronous flow meter, the counters immediately include short transfers without waiting for a sweep.

Included: serialized GossipSub data/signatures/RPCs, forwarding, duplicate copies, control traffic, Kademlia, Identify, ping and stream protocol negotiation. Excluded: HTTP management/telemetry, Noise/Yamux overhead, TCP/IP headers/retransmissions and Docker overlay/VXLAN. A stream write is not proof of remote application delivery. Negotiation can be attributed to the chosen protocol on one endpoint and to an empty protocol on the other. Attribution can briefly lag global totals during an operation.

For each process session and direction:

```text
sent bit/s     = 8 × (sentBytes_now - sentBytes_previous) / elapsed_seconds
received bit/s = 8 × (receivedBytes_now - receivedBytes_previous) / elapsed_seconds
```

Counters start at host creation. Sampling starts after the initial Controller clock synchronization attempt, then every five seconds, with a final post-close sample emitted before telemetry shutdown/drain. The first sample uses an implicit zero at host creation. Rates use the source monotonic clock; aligning samples from different Peers still depends on timestamp synchronization. Collection continues if initial synchronization fails. Events include `clockBasis` and `clockUncertaintyMs` when synchronized, so cross-Peer alignment can be interpreted alongside clock quality.

## Persistence and quality

`type: "bandwidth"` events in `runs/<id>/events.jsonl` contain `bandwidth: {elapsedNs, receivedBytes, sentBytes, protocols: [{protocol, receivedBytes, sentBytes}], final}` and the usual `runId`, `agentId`, `nodeId`, `sessionId`, `sequence` and `timestamp` fields. For example:

```json
{"bandwidth":{"elapsedNs":5000000000,"receivedBytes":1000,"sentBytes":2000,"protocols":[{"protocol":"/meshsub/1.2.0","receivedBytes":1000,"sentBytes":2000}],"final":false}}
```

Latest cumulative values are indexed by `(nodeId, sessionId)`. Retries are deduplicated. Missing middle samples recover bytes at the next sample, at the cost of a longer interval average. Older snapshots are counted as superseded; malformed or regressing counters and samples that overflow the signed 64-bit whole-run totals are rejected. A missing source timestamp is not replaced with receipt time. A reset requires a new session.

`metrics.bandwidth` adds `scope: "libp2p-stream-v1"`, total and per-protocol bytes, `sessions`, `finalizedSessions`, `samples`, `rejectedSamples`, `supersededSamples`, and `latestAt`. No sample means N/A; measured zero stays zero. Finalization counts only sessions that reported at least once: even all reporting sessions finalized does not prove every Peer reported. The normal telemetry queue reserves slots for the final drop notice, stop checkpoint and bandwidth sample. Force termination or transport failure can still lose a final sample and leave an unknown tail. Send and receive can count opposite endpoints of the same transfer; their sum is not unique network traffic.

Original events and `metrics.json` are included in result ZIPs. Saved-result analysis reconstructs full recorded counters after Controller restart. Analysis API responses also include bandwidth.

## Dashboard and API

The **P2P B/W · Send / Receive** cards are part of the Dashboard's single **Metrics** row. Their main values show the current run's send/receive rates; expand each card for cumulative transfer and its explanation. See [Metrics controls](dashboard.md#built-in-dashboard-visualization) for mouse, keyboard and touch navigation. Rates sum each Peer's latest source interval average, with adaptive bit/s, kbit/s, Mbit/s and higher units; totals use B, KiB, MiB and higher units. They follow the run named by **Run metrics**, independently of topology topic/layer filters.

**B/W measurement** shows collection quality in the same row; expand it for fresh active sessions, finalized sessions, the last sample time and rejected samples. It displays `Partial` when only some active sessions are fresh, `Rate unavailable` when their rate is unknown, and `N/A` without valid samples. Measured zero remains zero. The 15-second freshness rule and zero rate after normal finalization match Grafana, while cumulative totals remain available. The [Dashboard stream](api.md#dashboard-sse) refreshes time-dependent values even without trace events, so expired rates update. Live snapshots include this aggregate as `metrics.bandwidth.currentRates`; saved-result aggregates do not acquire wall-clock-dependent current rates.

**Saved results → Images** provides total and per-protocol send/receive throughput (kbit/s) and cumulative transfer (KiB) as PNG/CSV, with a ZIP of all charts. Original cumulative counters also remain in the result API/ZIP and Grafana. See [visualization](visualization.md) for controls and comparisons.

The analysis API adds `bandwidthTimeline` and `bandwidthBinSeconds`. Bins contain `at`, fractional send/receive byte allocations, per-protocol allocations, and `peerSeconds` (sum of allocated source observation durations, not unique nodes or completeness). Five-second bins adaptively merge to at most 360. An interval's bytes are distributed uniformly across overlapping bins; dividing by the full bin width gives the displayed average, including partial edge bins. Integrating the chart preserves bytes to floating-point precision. Missing intervals remain gaps. Missing Peer reports make the aggregate partial; peaks inside a reporting interval cannot be recovered.

## Prometheus and Grafana

Common labels: `run_id`, `agent_id`. Directions are `send` and `receive`. Counters and fresh rates are summed across sessions before export so Peer churn does not create new Prometheus series. Node/session detail remains in the raw logs and saved analysis.

| Metric | Type / extra labels |
|---|---|
| `kpl_p2p_stream_bytes_total` | Counter / direction |
| `kpl_p2p_protocol_stream_bytes_total` | Counter / direction, protocol |
| `kpl_p2p_stream_bits_per_second` | Gauge / direction |
| `kpl_p2p_protocol_stream_bits_per_second` | Gauge / direction, protocol |
| `kpl_p2p_bandwidth_sample_timestamp_seconds` | Gauge / latest source timestamp across the group |
| `kpl_p2p_bandwidth_sessions` | Gauge / session count by `state` (`open`, `final`) |

Grafana's **P2P stream bandwidth** panels apply Run/Agent filters, not Topic: one RPC/stream can carry several topics and topicless controls. Empty protocol means negotiation/unassigned. Rate gauges represent source interval averages; do not apply `rate()` again. Non-final sessions are excluded from aggregate rates when the source timestamp lies more than 15 seconds before or after the Controller clock; this also excludes implausibly future-dated samples. Finalized sessions contribute current zero; a group with no fresh or finalized sessions omits its rate. Counters retain bytes from departed sessions. `kpl_message_bytes_total` remains a separate application-payload metric.

Live gauges/counters reflect sessions seen during the current Controller lifetime. Historical logs are not automatically loaded into Prometheus memory on restart; use saved-result analysis/ZIP for reconstruction, and the Prometheus server's retained history for earlier scrapes. A new cumulative sample recovers that session's previous bytes. A short experiment may finish between scrapes: final counters remain observable, while the stored analysis preserves its sampled throughput.

[Visualization](visualization.md) · [Complete v2 coverage audit](v2-analysis-coverage.md)

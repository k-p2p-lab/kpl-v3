# Publish While Workers Churn on Swarm

English | [Korean](swarm-churn-publish.kr.md)

Use [`examples/swarm-churn-publish.yaml`](../examples/swarm-churn-publish.yaml) to keep workers joining and leaving while repeatedly publishing from randomly selected live workers. It extends the small [Swarm smoke test](swarm.md#first-deployment-experiment-and-download) with ongoing churn and repeated observations. It is a starting configuration, not a measured capacity guarantee or an exact replay of a v2 experiment.

## Prepare and Run

First complete the [Swarm deployment workflow](swarm.md). Start with **at least two online Agents and 40 total available slots**, such as two otherwise idle Agents configured for capacity 20 each. In **Agent status**, count only `online` rows: **Peers** is occupied / capacity. The summary's **Available slots** can include offline Agents. Keep other experiments stopped for a useful comparison.

Use the same account and Docker context as deployment. The commands below use sudo; omit it consistently if your deployment account already has Docker access.

```sh
sudo sh scripts/swarm.sh status
sudo sh scripts/swarm.sh access
sudo sh scripts/swarm.sh credentials
sudo sh scripts/swarm.sh scenario examples/swarm-churn-publish.yaml
```

Open the Controller URL from `access`. Choose **Run experiment**, replace the entire **YAML scenario** field with the scenario output, log in with the account from `credentials` if needed, and click **Run**. `scenario` only prints YAML; the web action starts the experiment. The default web form and the helper's default `scenario` output are different examples.

Set **Runs** beside **Run** to repeat the entire scenario 1–100 times. Iterations run sequentially and get separate results; **Stop batch** or a failed iteration cancels the remainder. The explicit seed is reused. See [repetition and metric definitions](experiment-metrics.md).

Workers use Linux `NET_ADMIN` and the host kernel's netem support. At startup, each Peer has a five-second budget for up to seven Controller health samples and continues Ready even if they all fail. Unsynchronized Peers retry every five seconds, synchronized Peers refresh every 30 seconds, and a last successful sample loses trusted status after two minutes without refresh. Keep chrony/NTP enabled during this long run. The example's network settings apply to outbound P2P TCP traffic; control, clock sampling, discovery, and telemetry HTTP traffic are outside that configured scope.

## What Happens

| Stage | Behavior |
|---|---|
| Bootstrap | Create two stable `boot` Peers and wait for their readiness. They provide DHT bootstrap discovery without joining GossipSub or acting as stable publishers. |
| Churn | Start background job `worker-churn`: up to 10,000 sequential worker joins, balanced across available Agents, with exponential mean 4-second intervals. |
| Worker lifetime | Each worker has Pareto lifetime `xm: 60s`, `alpha: 2.5`, giving a theoretical mean of 100 seconds. The clock starts when Docker creation is acknowledged, including startup time. Expiration requests removal; actual cleanup can finish later. |
| Warm-up | Wait 3 minutes while churn continues. |
| Publishing | Run 30 sequential rounds. Each round randomly selects up to 10 eligible workers without replacement, then publishes sequentially with exponential mean 1-second gaps. |
| Round gaps | Wait 10 seconds between rounds, for 29 gaps in total. This explicit wait also prevents an empty candidate set from consuming all rounds immediately. |
| Collection and cleanup | After the final round, collect for 30 seconds while churn continues, then `stop-all` cancels the remaining join job and cleans up Peers. |

The theoretical steady-state worker count is `100s / 4s = 25`, plus two boot Peers. This is an expectation, **not a ceiling**. Lifetimes vary, Docker creation and startup take time, and joins wait when all eligible Agents are full. These effects change the actual ready population and arrival rate. The 40-slot starting recommendation leaves some headroom but does not establish that a particular server can sustain this workload.

`count: 10000` is a **total join budget**, not 10,000 simultaneous workers. The final `stop-all` usually cancels the producer well before that budget is exhausted. This example has no explicit per-worker `leave` phase; sampled lifetimes drive departures.

Only the stable boot group has a readiness barrier. A worker `wait-ready` barrier over a continually growing churn group would also include stopped members from the current generation. The 3-minute warm-up avoids that unsuitable barrier, but does not guarantee a particular worker count or GossipSub mesh convergence. Worker mesh settings are `d: 9`, `dLow: 7`, and `dHigh: 11`.

DHT bootstrapping does not itself connect these workers into a GossipSub overlay because the built-in boot Peers have PubSub disabled. Once a worker starts PubSub, it immediately queries the Controller's same-run, exact-topic discovery registry and schedules further rounds every three seconds. In [`internal/peer/discovery.go`](../internal/peer/discovery.go), rendezvous hashing ranks ready candidates, preferring subscribers. The dial budget is the deficit between `DHigh` and the current topic peer count, further limited by the transport connection cap. Failed dials advance to later candidates and enter a 15-second retry delay; slow rounds can delay the next poll. GossipSub still selects the actual GRAFT mesh; a registry candidate or transport connection is not automatically a mesh member.

A full 10-node round has nine sampled gaps, averaging about 9 seconds before request overhead. The nominal waits total `180 + 30×9 + 29×10 + 30 = 770 seconds`, or **about 12 minutes 50 seconds**, plus bootstrap, API, and cleanup time. It is not a deadline: empty or small rounds shorten it, and slow requests lengthen it.

## Who Publishes and What Is Measured

At each round's start, the Controller builds a fresh set of `ready`, publish-capable full workers on online Agents in the worker group, subscribed to `kpl/swarm-churn`. It shuffles this set and selects up to 10 distinct nodes. A node can be selected again in another round, and workers that joined later can participate in later rounds.

A selected worker can expire before its turn. `onError: continue` records individual failures as `phase-operation-failed` and continues; no eligible workers makes that round a no-op. Cancellation, scenario deadlines, persistence errors, and other fatal failures can still end the run. The maximum is **300 selected publish operations**, not 300 guaranteed successful messages.

Each publication uses `payloadSize: 32`, `payloadEncoding: envelope`, `deliveryWindow: 10s`, and topic `kpl/swarm-churn`. The 32 bytes describe the random payload. The envelope adds metadata, so the PubSub data and network traffic exceed 32 bytes. The envelope enables propagation latency measurement.

The worker network profile applies `scope: p2p`, 25ms delay, 2ms jitter, and 0.5% configured packet loss. Boot Peers have no added network impairment. **Packet loss is not application delivery failure**: TCP can retransmit and one publication can reach many subscribers. Overall delivery event counts also include the publisher's local receipt; the session-window ratios exclude it.

## Observe and Save Results

In the Dashboard, watch ready Peers, Agent occupancy, experiment/job state, and events. In Grafana, open **KP2PLab Experiment Analysis**, choose your `swarm-churn-random-publish` run in **Run**, and filter **Agent** and **Topic**. After completion, set the time range to the experiment window.

| Observe | Data |
|---|---|
| Changing population and occupied capacity | Live node inventory, `kpl_nodes`, `kpl_agent_active_nodes` |
| Publication, delivery, and duplicate event counts | `kpl_events_total`; use `graft`/`prune` events to observe mesh changes |
| Stable conditional delivery, known-starting-cohort delivery, and coverage bounds | `kpl_window_delivery_ratio`, `kpl_window_initial_delivery_ratio`, `kpl_window_stable_coverage`, `kpl_window_stable_coverage_upper_bound`; only the Run filter applies |
| Pending and uncertain measurement | `kpl_window_pending_publications`, `kpl_window_unknown_pairs`, `kpl_window_publication_availability_unknown_pairs`, `kpl_window_continuity_unknown_pairs`, `kpl_window_measurement_incomplete` |
| Observed extra copies per successful stable remote pair | `kpl_window_duplicates_per_reached_pair`; only the Run filter applies |
| Envelope latency and PubSub data volume | `kpl_window_propagation_latency_seconds`, `kpl_message_bytes_total` |
| Configured network conditions | `kpl_network_configured_*`: settings, not observed loss or RTT |
| Failed requests and reported telemetry drops | `kpl_operation_failures_total`, `kpl_telemetry_dropped_events_total` |

In this example, `session-window-v1` publications use actual publication time and the configured 10-second deadline. `measurement_start` records a subscribed application session, and same-session checkpoints every two seconds or a stop prove its observed continuity. The primary ratio uses only sessions proven subscribed throughout the window. A session stopping before the deadline is excluded from both primary counts even if it already received; later joiners are excluded. Network disconnection or mesh removal does not remove a subscriber.

Keep the known-starting-cohort ratio and stable coverage beside the primary ratio: ten known initial subscribers, two departures (one after receiving), and seven successes among eight stable subscribers give stable delivery **7/8**, starting-cohort delivery **8/10**, and coverage **80%**. The starting-cohort ratio includes the departure's early success. If a known starting pair has neither proven departure nor proof of continuity through the deadline, it widens the coverage range from `stable / known-starting` to `(stable + continuity-unknown) / known-starting`. Missing receipts with source sequence gaps similarly widen delivery bounds; these bounds are not confidence intervals. Messages younger than ten seconds are pending. Publication-time availability-unknown candidates stay outside the known cohort and appear in observation quality. Starting delivery and coverage become N/A only when that known cohort is empty, not merely because an incomplete/unknown warning exists.

PubSub `join`/`leave` events are **not Peer creation/departure records**. The measurement events record instrumented subscription sessions, not a complete Docker lifecycle or physical uptime history. Forced stops can leave unknown tails, and wholly invisible telemetry cannot be detected from the received log. A trusted clock estimate records its uncertainty: a bounded negative estimate may count as an on-time receipt while remaining outside latency statistics. Failed refreshes stop trusted metadata after the two-minute validity window, and later success restores it. Chrony/NTP is still recommended. ZIP `metrics.json` reconstructs these definitions from the same saved event boundary. Old dispatch cohorts remain legacy and cannot supply continuous-session evidence. See the [definitions and collection limits](experiment-metrics.md).

After the run ends, confirm Peer cleanup and, when no other runs are active, zero Agent occupancy. A `completed` run can have `canceledJobs: 1`: `stop-all` intentionally cancels the unfinished join job. That counter alone is not a failure.

Use **Download results** on the run or in **Saved results**. The ZIP includes the submitted scenario, run metadata, and full saved event log at the export boundary, independently of the 300-event recent buffer. It includes `metrics.json` and collected subscription-session events, but no separate complete Docker lifecycle snapshot, message payload dump, PCAP, or Prometheus/Grafana database. Running downloads are snapshots, and missing telemetry cannot be recovered by downloading. [Archive contents and retention](monitoring.md#download-experiment-results)

Download before `sudo sh scripts/swarm.sh remove` takes the web services offline. The data volumes and external Peer network remain. A retained `interrupted` result after restart is not resumed and does not prove Peer cleanup.

## Adjust the Experiment

| Setting | Effect |
|---|---|
| Worker join `count` | Changes the total creation budget. Keep it high enough that joins continue through the last collection period. |
| Worker `interval.mean` | A smaller value raises attempted arrivals; Docker and capacity waits still add time. |
| Worker lifetime `xm` / `alpha` | Changes session lengths and expected population. For `alpha > 1`, mean lifetime is `alpha × xm / (alpha - 1)`. |
| Agent capacity | Changes admission limits, not reserved CPU/RAM. Measure host load before raising it. |
| Warm-up `duration` | Changes how long churn runs before the first publication. It is a timed wait, not a convergence test. |
| Publish `count` / interval | Changes the maximum publishers per round and spacing between their requests. |
| Publish `deliveryWindow` | Fixed on-time receipt window, default 10s and maximum 1h. Changing it also changes which sessions remain eligible; compare the same value and keep the final collection wait longer than it. |
| Round pairs / 10-second waits | Changes observation duration. Keep explicit waits so empty rounds still advance time. |
| Worker network profile | Changes P2P delay/jitter/loss. Keep the two bootstrap Peers stable for this experiment design. |
| `payloadEncoding` | `envelope` provides latency metadata. `raw` gives exactly 32 bytes of PubSub data here, but no propagation latency samples. |

The YAML uses anchors to reuse a publish phase and its wait phase. These are standard YAML references expanded during parsing, **not a scenario-engine loop feature**. To change the round count, change the repeated publish/wait pairs; editing the anchored publish phase changes every alias that refers to it. Preserve the final collection wait and `stop-all`.

A fixed seed makes random sampling reproducible when the eligible populations and execution conditions match. It does not make Docker timing, departures before a request, or results identical across different runs and machines.

## Relationship to the v2 Churn Experiments

The reviewed v2 `exp/exp-2603_churn-02.sh` script kept 10 boot nodes stable, scheduled up to 10,000 worker joins, waited, published one batch from up to 10 randomly selected workers using 32-byte raw data on all their topics, then collected for 30 seconds while churn continued.

This example deliberately uses two boot nodes, a smaller expected worker population, **30 newly selected publication rounds**, one explicit topic, added P2P network conditions, and **envelopes for latency measurement**. Its balanced Agent placement also differs from v2's per-join random worker-server selection. It preserves the churn-and-observe idea while making repeated observations practical on a small Swarm. The session-window metric definition is an independent v3 experimental choice, not a v2 reproduction contract. It does not establish equivalent results or reconstruct missing v2 configuration files. See the [v2 reproduction notes](v2-reproduction.md) for the compatibility boundaries.

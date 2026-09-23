# Monitoring, Access Logs, and Connection Diagnosis

English | [Korean](monitoring.kr.md)

[Documentation index](README.md) · [Repository](../README.md)

Deploy the stack using the [Swarm deployment guide](swarm.md). It runs the Controller, one Agent per selected node, Prometheus, and Grafana. The data source and **KP2PLab Experiment Analysis** dashboard are provisioned automatically. Use `sh scripts/swarm.sh access` to find the published control-node addresses; the examples below use `control-node` as a placeholder for that host.

The dashboard is written in English, and the Swarm stack sets `GF_USERS_DEFAULT_LANGUAGE=en-US` for the Grafana UI; user or organization preferences can override that UI default. See [Grafana language preferences](https://grafana.com/docs/grafana/latest/administration/organization-preferences/#change-grafana-language).

Grafana initializes its SQLite database on first startup, which may take several minutes depending on disk performance. Follow initialization with `sh scripts/swarm.sh logs grafana`. Subsequent starts reuse the existing database.

The local SQLite database uses WAL mode. Both the database and WAL files are retained in the same Grafana named volume. See the [Grafana database settings](https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana/#wal).

| Interface | Default address |
|---|---|
| KPL Dashboard | http://control-node:8080 |
| Grafana experiment analysis | http://control-node:3000/d/kpl-experiments |
| Prometheus queries and target status | http://control-node:9090 |
| Controller metrics endpoint | http://control-node:8080/metrics |

The Swarm stack disables anonymous Grafana access and requires `GRAFANA_ADMIN_PASSWORD`. Its setup helper stores credentials in the manager's configuration; `sh scripts/swarm.sh credentials` prints the configured login. On startup, this stack synchronizes the original Grafana administrator password to the configured value while preserving the existing username and data.

Swarm publishes the Prometheus and Grafana ports on the control node. It also publishes each Agent's dedicated metrics listener on its own node at `KPL_AGENT_METRICS_PORT` (default `9091`). Use `scripts/swarm.sh configure` to set `PROMETHEUS_PORT`, `GRAFANA_PORT`, or the Agent metrics port, then redeploy to apply the changes.

The Dashboard header links to Prometheus and Grafana in new tabs. It preserves the current Dashboard scheme and host and substitutes the configured published ports. Direct access and SSH tunnels work when those browser-facing ports match the configured values. If a proxy changes the scheme/path or local forwarding uses different ports, open the actual monitoring addresses separately.

For Dashboard controls use [Dashboard](dashboard.md); for run recovery use [experiments](experiments.md); for source exports and backups use [saved results](results.md).

## Run and analyze an experiment

1. Use **Run experiment** in the Dashboard to run [`examples/monitoring.yaml`](../examples/monitoring.yaml). This small experiment includes both envelope and raw publications.
   Set **Runs** beside **Run** to 1–100 for sequential iterations. Each gets a separate result; failure or **Stop batch** cancels the remainder. See [execution and repetition](experiments.md).
2. Select **Run** (`run_id`), **Agent**, and **Topic** in Grafana. Selecting multiple runs aggregates their traffic and latency samples; network configuration and bandwidth time series identify each run in their legends. Session delivery panels apply only Run, and bandwidth/control panels apply Run and Agent without Topic.
3. After an experiment finishes, set the time range to its execution window to view the recorded series. The default refresh interval is 5 seconds.

Prometheus scrapes `/metrics` on the Controller and Agents every 5 seconds. The Controller returns the metrics URLs of registered Agents through HTTP service discovery; Prometheus then reaches each Agent's host-mode port directly instead of a service VIP. It does not scrape Peers directly or add exporters to individual Peer containers. The Controller aggregates the existing Peer telemetry stream, so telemetry loss under `scope: all` still affects Controller-derived Peer metrics. Monitoring services use a separate Docker network; Peers receive no additional networks or permissions.

## Metric definitions

| Metric | Meaning |
|---|---|
| `kpl_events_total` | Cumulative events received by the Controller, labeled by `run_id`, `agent_id`, `event_type`, and `topic` |
| `kpl_message_bytes_total` | Sum of `fields.wireBytes` in publish/deliver events: PubSub data including envelope JSON/base64 when used, excluding libp2p framing and TCP/IP headers |
| `kpl_p2p_stream_bytes_total`, `kpl_p2p_protocol_stream_bytes_total` | Measured cumulative libp2p stream bytes per run/Agent/direction, overall and by negotiated protocol |
| `kpl_p2p_stream_bits_per_second`, `kpl_p2p_protocol_stream_bits_per_second` | Source interval-average bit/s gauges; query directly, without `rate()` |
| `kpl_p2p_bandwidth_sample_timestamp_seconds`, `kpl_p2p_bandwidth_sessions` | Latest source sample time and counts of sessions with/without a normal post-close sample; see [bandwidth quality and limits](bandwidth.md) |
| `kpl_gossipsub_control_rpcs_total` | RPC envelopes containing each GossipSub control type, separated by `send`, `recv`, and local pre-send `drop` |
| `kpl_gossipsub_control_entries_total` | Repeated protobuf control entries carried in those RPCs |
| `kpl_gossipsub_control_message_ids_total` | Non-unique message-ID reference occurrences in IHAVE, IWANT, and IDONTWANT entries |
| `kpl_gossipsub_control_peer_exchange_records_total` | Peer-exchange records carried by PRUNE entries |
| `kpl_window_stable_pairs`, `kpl_window_reached_pairs` | Mature message/receiver-session pairs proven subscribed for the whole delivery window, and their on-time successes; labeled by `run_id` |
| `kpl_window_unknown_pairs`, `kpl_window_missed_pairs`, `kpl_window_late_pairs` | Unknown receipt and confirmed miss among stable pairs; late counts overlap one of those outcomes when no on-time receipt exists |
| `kpl_window_delivery_ratio` (`bound`: `lower` / `upper`) | Logical bounds on stable conditional delivery, absent with no stable pairs; not confidence intervals |
| `kpl_window_initial_pairs`, `kpl_window_initial_reached_pairs`, `kpl_window_initial_unknown_pairs` | Known starting-cohort pairs, on-time successes including departed sessions, and unknown receipts |
| `kpl_window_initial_delivery_ratio` (`bound`: `lower` / `upper`) | Logical delivery bounds within the known starting cohort; absent only when that cohort is empty |
| `kpl_window_stable_coverage`, `kpl_window_stable_coverage_upper_bound` | Lower and upper coverage bounds: stable / known-starting, and (stable + continuity-unknown) / known-starting; absent only when the known starting cohort is empty |
| `kpl_window_departed_pairs`, `kpl_window_continuity_unknown_pairs` | Known starting pairs confirmed departed before the deadline, and pairs with neither proven departure nor proven continuity through the deadline |
| `kpl_window_publication_availability_unknown_pairs`, `kpl_window_availability_unknown_pairs` | Candidate pairs lacking proof of availability at publication, and the aggregate of publication-time and continuity uncertainty |
| `kpl_window_pending_publications`, `kpl_window_finalized_publications` | Messages before their deadline and messages whose window has elapsed; late telemetry can still revise finalized results |
| `kpl_window_measurement_incomplete`, `kpl_window_legacy_publications` | Known unscoped measurement streams (0/1), and publications using historical definitions; a zero flag does not detect completely invisible telemetry |
| `kpl_window_propagation_latency_seconds` | First on-time stable remote envelope delivery histogram, labeled by `run_id`, receiving `agent_id`, and `topic`; raw/local/late/departed/unknown/invalid samples excluded |
| `kpl_window_duplicate_copies`, `kpl_window_duplicates_per_reached_pair` | Observed extra PubSub copies within the window at successful stable remote pairs, and copies per successful pair |
| `kpl_operation_failures_total` | Publish/leave failures recorded under `onError: continue` |
| `kpl_telemetry_dropped_events_total` | Reported telemetry loss; neither P2P packet loss nor proof that unreported loss is zero |
| `kpl_nodes` | Peer counts by experiment, Agent, group, role, type, and state |
| `kpl_agent_*` | Agent online status, capacity, and latest heartbeat observed by the Controller |
| `kpl_experiment_*` | Experiment state, phase, and job state |
| `kpl_network_configured_*` | Effective configurations of starting/ready Peers, aggregated by group; delay includes mean/min/max, while jitter/loss report the mean |
| `kpl_local_*` | Each Agent's local Peer states, capacity, pending cleanup, and telemetry queue length |
| `go_*`, `process_*` | Runtime, CPU, and memory metrics for the scraped Controller/Agent processes; these do not represent total Peer container resource usage |

`kpl_network_configured_loss_ratio` is the configured packet loss ratio, not an observed loss rate. Configured delay is also distinct from measured RTT. The `graft`/`prune`/`remove_peer` events help analyze PubSub mesh changes; they are neither a TCP connection graph nor a complete mesh snapshot.

Control counters use only `run_id`, `agent_id`, `direction`, and `control_type` labels. The Topic filter does not apply because IWANT and IDONTWANT contain no topic and one RPC may contain several topics. `send` records outbound queue admission rather than remote receipt, `recv` precedes later router policy checks, and `drop` is a local queue/size rejection rather than `netem` loss. Read RPC, entry, message-ID-reference, and PRUNE peer-exchange counts as separate units. Exact per-RPC topic counts and the per-Agent totals in `metrics.json` remain in the result ZIP. See [experiment metric definitions](experiment-metrics.md#gossipsub-control-traffic).

For current relationships, the Dashboard's [interactive topology](topology.md) uses Peer status snapshots to display transport, Kademlia routing-table, and GossipSub mesh layers independently. The Controller also saves sampled protocol graphs in `observations.jsonl`; these merge topic edges within each protocol and do not retain every intermediate transition or individual score pair. Prometheus has no corresponding complete graph history.

Grafana session panels use only the Run filter and aggregate receiver-pair counts, not averages of percentages. Bounds describe observed cohorts; they do not establish an unseen population. Starting-cohort delivery and stable coverage remain available despite publication-time availability warnings or `measurementIncomplete`; they are N/A only when the selected runs contain no known starting pair. For each aggregation, known starting = stable + departed + continuity-unknown. The legacy-publications panel identifies historical data excluded from these results. New panels use `kpl_window_*` only; historical `kpl_delivery_*` and `kpl_propagation_latency_seconds` keep their old meanings and must not be combined.

Overall `deliver` counts include local delivery and cannot be divided by publication counts to obtain reachability. TCP retransmissions can turn packet loss into delay rather than message loss. See the [metric definitions and formulas](experiment-metrics.md). Late batches can correct window gauges and histogram buckets; query them directly, not with `rate`/`increase`. Grafana latency shows cumulative whole-run quantiles, not quantiles restricted to the selected time range.

Cumulative counters are independent of the web interface's 300-event recent buffer. They reset when the Controller process restarts, and historical `events.jsonl` files are not replayed automatically. Time series already stored in Prometheus remain available, and `rate`/`increase` handle observed counter resets. They cannot recover events that disappeared before a scrape or telemetry that failed to arrive. Because `increase` estimates interval growth from scrape samples, it does not always exactly match integer cumulative event counts.

## Web access and authentication logs

The Controller automatically writes one JSON object per line under `<data-dir>/logs`. Local runs use `data/logs`; Swarm uses `/var/lib/kpl/data/logs` in the persistent `controller-data` volume.

| File | Records |
|---|---|
| `access.jsonl` | Dashboard/static-file/API requests, redirects, failures, and SSE connection open/close |
| `auth.jsonl` | Successful and failed logins, logout attempts, missing/expired-session access and same-origin rejections |

Each record includes UTC `timestamp`, `event`, a server-generated `requestId`, `remoteIp`, HTTP `method` and `path`, response `status`, body `bytes`, `durationMs`, `userAgent`, `authentication` (`anonymous`, `session`, or `internal`), and `user` when known. Authentication records include `outcome` and a reason such as `invalid_credentials`, `rate_limited`, `invalid_request`, `origin_denied`, or `login_required`. A failed login records the submitted username when the JSON request is valid. Access and authentication entries share the request ID. SSE writes `sse_open` immediately when the response starts and `sse_close` when it ends.

Passwords, session cookies, Authorization headers, request/response bodies, Referer and URL query strings are excluded. Untrusted text is length-limited and JSON-escaped. `remoteIp` comes from the actual connection, without trusting client-supplied `Forwarded` or `X-Forwarded-For`; behind a reverse proxy this is the proxy's address. Successful authenticated Agent/Peer traffic and successful health/metrics/Prometheus-target checks are omitted to avoid per-event disk logging during experiments. Their errors and unauthorized attempts are still logged.

Each file rotates before exceeding **10 MiB**, retaining the current file plus **five backups** (`.1` newest through `.5` oldest), up to approximately **120 MiB combined**. Startup appends to existing logs. The directory uses mode `0700` and files `0600`. Startup rejects an unusable log path; a later write error is reported in the Controller's ordinary service log without breaking HTTP requests, and subsequent writes retry. Records are appended without an in-memory queue; they are not individually fsynced. Include `logs` in Controller-volume backups when retaining access history. These files are not served by the Dashboard or included in experiment ZIPs.

For a local Controller, follow both files with:

```sh
tail -F data/logs/access.jsonl data/logs/auth.jsonl
```

For Swarm, use the manager helper to print the latest 100 lines as raw JSONL:

```sh
sh scripts/swarm.sh logs access
sh scripts/swarm.sh logs auth
sh scripts/swarm.sh logs access --tail 500
sh scripts/swarm.sh logs auth --tail all > auth.jsonl
```

`--tail` accepts a positive integer or `all`. These commands read the current file only; rotated `.1` through `.5` backups remain in the Controller volume. Output is a snapshot and can be piped to tools such as `jq`. Existing `logs controller`, `logs agent`, `logs prometheus`, and `logs grafana` continue to read timestamped service logs and also accept `--tail`.

The helper finds the running Controller task through the current manager Docker context, then reads its file with `docker exec`. By default the selected Docker daemon must host that task. If the Controller runs on another node (including a worker), create a Docker context for that node and pass it **only for file reads**:

```sh
# Replace user and control-node with your SSH login and Controller host.
docker context create kpl-control --docker "host=ssh://user@control-node"
sh scripts/swarm.sh logs access --context kpl-control
sh scripts/swarm.sh logs auth --context kpl-control --tail 500
```

Keep the caller's Docker context connected to a manager; `--context` on these two commands selects the daemon for the node check and file read, while task discovery stays on the manager. The helper verifies that the selected daemon is the task's actual node. It requires a running Controller with web logging enabled and reports missing files, unavailable tasks or a wrong node as errors. It does not start containers or change the stack. For continuous following or rotated backups, access the files directly on the Controller node.

## Dashboard stream traffic

A visible Dashboard uses `/api/v1/stream?view=dashboard`: an initial `snapshot`, then `snapshot_delta` updates or idle `heartbeat` events. Hidden pages disconnect and synchronize on return. The cumulative bytes of a long-lived stream are not a static page size. See [Dashboard SSE](api.md#dashboard-sse) for payloads, liveness timeouts, retry behavior, and legacy clients.

For excessive traffic or prolonged **Reconnecting**:

1. After an image update, reload the page and check that the browser Network panel uses the dashboard view above.
2. Compare `sse_close.bytes` with `durationMs` in the access log; inspect `sse_open`/`sse_close` and `/api/v1/auth/session` response status and duration together.
3. Check Controller/service load and reachability. A login error differs from a slow session probe; a browser disconnect does not establish an experiment failure. Consult saved run state before choosing [recovery](experiments.md).

The earlier trace-heavy fixture and churn benchmarks are retained in [performance evidence](churn-performance.md), with their validation limits.

## Retention and operation

Prometheus time series are stored in the `prometheus-data` named volume, and Grafana settings in `grafana-data`. Both survive ordinary container recreation. Prometheus retention is set to 15 days or 5 GB, whichever limit is reached first. The 5 GB setting is not a hard ceiling on disk usage: WAL, head data, and compaction require additional space. See the [Prometheus storage documentation](https://prometheus.io/docs/prometheus/latest/storage/).

Swarm configs are immutable: Prometheus and dashboard configuration changes use versioned config references and require a stack redeployment. The provisioned originals are managed as files. To save a separate dashboard, sign in as an administrator and work with a copy. See the [Grafana provisioning documentation](https://grafana.com/docs/grafana/latest/administration/provisioning/).

```bash
# Check scrape target status.
curl http://control-node:9090/api/v1/targets

# Check service status and logs from the manager.
sh scripts/swarm.sh status
sh scripts/swarm.sh logs prometheus
sh scripts/swarm.sh logs grafana
```

The Controller target also uses a browser-reachable address: `GET /api/v1/prometheus/controller-targets` returns the control node's Swarm address and published `KPL_HTTP_PORT`. The deployment helper derives `KPL_CONTROLLER_METRICS_URL` and `KPL_PROMETHEUS_EXTERNAL_URL` on each deployment, including custom ports and IPv6. Prometheus uses the latter as [`--web.external-url`](https://prometheus.io/docs/prometheus/latest/command-line/prometheus/) for its own links. Internal DNS remains in discovery requests and the Grafana datasource. Prometheus containers must be able to reach the control node's published HTTP port. An unset Controller metrics URL returns an empty target list.

The Swarm stack discovers registered targets from `GET /api/v1/prometheus/agent-targets`. Use `sh scripts/swarm.sh access` to inspect the advertised URLs, ensure the configured port is free on every selected Agent node, and permit TCP traffic from the control node. If operators open an Agent metrics link directly, permit their browser's trusted management network as well; block untrusted sources. `up{job="kpl-agent"}` distinguishes successful scrapes from registered targets that are unreachable through a firewall or an incorrect Swarm `NodeAddr`.

Image versions are pinned to Prometheus `v3.13.2` and Grafana `13.2.1`. When upgrading, consult the official [Prometheus downloads](https://prometheus.io/download/) and [Grafana Docker installation guide](https://grafana.com/docs/grafana/latest/setup-grafana/installation/docker/), then revalidate the configuration and dashboards.


## Execution validation

Validate a new run on Linux and retain its ZIP, code revision or image digest, and Prometheus time range. The repository does not include the original ZIPs behind earlier monitoring run examples, so those historical counts are not an auditable baseline for the current session-window implementation.

For [`examples/monitoring.yaml`](../examples/monitoring.yaml), check:

| Check | Source-based expectation |
|---|---|
| Successful publications | 60 envelope + 6 raw = 66 if every scheduled publish succeeds and its telemetry is collected |
| Worker network configuration | Three workers configured with 50 ms delay, 5 ms jitter, and 1% loss |
| Delivery denominator | Reconstructed from same-run/topic session evidence for each publication's window; not the overall `deliver` count |
| Latency samples | Only stable, on-time remote envelope pairs; raw and publisher-local receipts are excluded |
| Observation quality | Retain pending, unknown, incomplete, and reported telemetry-drop values; zero reported drops alone does not prove a complete log |
| Cleanup | The final `stop-all` requests Peer removal; verify Agent status and remaining Peer containers on each Linux host |

Compare `metrics.json` with its exact `events.jsonl` prefix and `export.json.exportedAt`. Counter comparisons also require the same Controller lifetime and collection boundary. Heartbeats initialize zero baselines for `add_peer` and `remove_peer`, but an event before the first scrape can still be absent from an `increase` estimate. Delivery, duplicate, mesh-event, and latency totals depend on execution and observation; they are not fixed acceptance counts.

The metric implementation and regression cases are linked from [experiment metrics](experiment-metrics.md#implementation-and-related-guides). Use the [development guide](development.md) for Linux validation commands.

## Built-in Dashboard visualization

This section moved to [dashboard](dashboard.md).

### Find saved results

See [saved-result browsing](results.md#find-saved-results).

### Show and hide panels

See [Dashboard panels](dashboard.md#show-and-hide-panels).

## Download experiment results

This section moved to [results](results.md#download-experiment-results).

## Analysis and image retention

This section moved to [results](results.md#analysis-and-image-retention).

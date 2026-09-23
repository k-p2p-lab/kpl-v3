# Analyze experiments with Prometheus and Grafana

English | [Korean](monitoring.kr.md)

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

The Dashboard requests the compact SSE view described in [Dashboard SSE](api.md#dashboard-sse): one complete initial view, then changed records/sections only. It sends the 40 event summaries actually displayed instead of repeatedly transferring the 300-event raw trace buffer. The browser disconnects when hidden and synchronizes again when visible. A long-running SSE request accumulating bytes is expected, but full raw trace arrays should no longer dominate normal Dashboard traffic.

After updating the Controller image and reloading the page, the browser's Network panel should show `/api/v1/stream?view=dashboard`, with `snapshot` followed by `snapshot_delta` events. The access log's `sse_close.bytes` reports the cumulative response bytes and `durationMs` the connection duration. A synthetic trace-heavy 20-second regression fixture (21 updates) decreased JSON payload from 85,767,024 to 170,144 bytes; this is a test measurement, not a bandwidth guarantee for arbitrary cluster sizes. Original full-snapshot clients without the query parameter still receive the legacy payload.

If a connection stalls, the Dashboard automatically closes it after 30 seconds without an initial snapshot or 45 seconds without data/heartbeat. Its session check has an 8-second timeout and retries back off to at most 30 seconds. An idle connection should receive a small `heartbeat` event roughly every 15 seconds; a healthy HTTP/2 stream no longer resets at the former 10-second write deadline. For continuing server delays, compare `/api/v1/auth/session` access-log durations with SSE open/close records to distinguish an unavailable server from a failed connection.

## Built-in Dashboard visualization

**Metrics** places all 11 cluster, delivery, bandwidth and observation cards in one horizontal row. Scroll sideways or use the **Previous metrics / Next metrics** arrow buttons. Hover over a card to expand that card horizontally and reveal its detailed values and explanation; only one card expands at a time. Long details scroll vertically within the expanded card.

- Click or tap a card to open its details, and select it again to close them. On touch screens, swipe horizontally to move through the row.
- Keyboard focus opens a card. Use Left/Right to move between cards and Home/End for the first/last card. Enter/Space toggles details; Escape or a click/tap outside the row closes them.
- **How delivery is measured** remains directly below the row. Its explanation and expanded state persist through live updates.

**Online Agents** and **Ready Peers** describe the whole cluster; message and bandwidth cards follow the run shown by **Run metrics**. **Observation quality** shows receipt, continuity and start unknown counts separately as `Receipt / Continuity / Start`; these overlapping categories are not summed. Expand it for the full labels and outcome counts.

When a run finishes, fails or is canceled, its carousel measurements reset to the initial N/A/zero values. All cards remain available; the next running run supplies new values. Cluster counts stay live, and final measurements remain in **Saved results**.

Within a repeated-run group in **Saved results**, rows are ordered by numeric **Run n of M** (1, 2, …, 10), including queued runs and retries. The order stays stable when a queued run starts; previous attempts remain in their separate table and entries without a valid iteration appear last. Batch groups keep their archive position. The results API keeps newest-started-first ordering, with batch/iteration/ID tie breaks for equal start times such as queued runs.

**Available slots** totals the free Peer capacity reported by online Agents; offline Agents contribute no available slots. Capacity is an admission count, not a CPU or memory reservation. See [Peer placement and capacity](swarm.md#distribution-and-capacity) before increasing it.

**Saved results → Images** submits server background analysis and provides overview, message and repeated-run charts as PNG/CSV/ZIP. It uses saved records independently of Prometheus retention. See [result images](visualization.md) for operation and [experiment metrics](experiment-metrics.md#saved-result-research-metrics) for definitions.

### Show and hide panels

Use the header's **Collapse / Expand** button on **Network overview**, **Experiment progress**, **Agent status**, **Recent network events**, or **Saved results**. Narrow screens show an arrow icon. The buttons support click, tap, and keyboard Enter/Space. All five panels start open; this browser remembers your choices for the Dashboard origin after reload.

**Network overview** and **Experiment progress** remain stacked at full width. On wide screens, collapsing either **Agent status** or **Recent network events** puts its compact header above the other panel, which fills the available width. Reopening both restores the Agent/events columns. Collapsing both leaves two compact headers side by side; mobile layouts remain vertical.

Experiments, background analysis and live data updates continue while a panel is hidden. Collapsing **Network overview** also stops its layout animation; reopening displays the latest state and preserves your **Pause motion** choice. These controls change only panel visibility.

## Run and analyze an experiment

1. Use **Run experiment** in the Dashboard to run [`examples/monitoring.yaml`](../examples/monitoring.yaml). This small experiment includes both envelope and raw publications.
   Set **Runs** beside **Run** to 1–100 for sequential iterations. Each gets a separate result; failure or **Stop batch** cancels the remainder. See [repetition and metric definitions](experiment-metrics.md).
2. Select **Run** (`run_id`), **Agent**, and **Topic** in Grafana. Selecting multiple runs aggregates their traffic and latency samples; network configuration and bandwidth time series identify each run in their legends. Session delivery panels apply only Run, and bandwidth/control panels apply Run and Agent without Topic.
3. After an experiment finishes, set the time range to its execution window to view the recorded series. The default refresh interval is 5 seconds.

Prometheus scrapes `/metrics` on the Controller and Agents every 5 seconds. The Controller returns the metrics URLs of registered Agents through HTTP service discovery; Prometheus then reaches each Agent's host-mode port directly instead of a service VIP. It does not scrape Peers directly or add exporters to individual Peer containers. The Controller aggregates the existing Peer telemetry stream, so telemetry loss under `scope: all` still affects Controller-derived Peer metrics. Monitoring services use a separate Docker network; Peers receive no additional networks or permissions.

## Download experiment results

In the Dashboard, choose **Download results** on an experiment or in **Saved results**. For repeated runs, expand the [series header](visualization.md#batch-mean-for-repetitions-of-one-experiment) to access each run's download, analysis and deletion controls. The saved list reads the Controller's data directory, so results remain accessible after a Controller restart. Use **Refresh** after restoring files from a backup. Running experiments offer **Download snapshot**. Sizes immediately show **Source**, the sum of uncompressed original files at the last list refresh. Running and queued results show **Live source**; refresh the list to update it. This value can differ from the downloaded ZIP size.

Each ZIP contains:

| File | Contents |
|---|---|
| `scenario.yaml` | Exact scenario submitted for the run |
| `experiment.json` | Original saved experiment metadata, state, seed, and job counters |
| `events.jsonl` | All event records saved at the export boundary; one JSON object per line, or an empty file when no events have been recorded |
| `observations.jsonl` | Group state/degree/clustering/scores and available protocol node/edge samples recorded about every five seconds during a run; older results can lack the file or edges |
| `metrics.json` | Session-window delivery bounds, starting-cohort results, coverage, pending/unknown counts, first remote latency, observed duplicates, control breakdown and recorded bandwidth totals/quality rebuilt from the same event-log prefix; historical definitions stay legacy |
| `export.json` | Export time, run state, active/partial flags, and the captured source file sizes |

The export captures file sizes under the Controller's persistence lock and streams the ZIP after releasing that lock. Events appended later are excluded, so a slow download does not hold up telemetry writes. A completed run can still receive delayed telemetry: download again after collection has settled if you need those later records. `partial: false` indicates a terminal recorded run state, not a guarantee that no telemetry was lost. The archive does not contain message payloads, PCAP files, or the Prometheus/Grafana databases.

The list field `sourceBytes` sums the sizes of currently present regular source files: `scenario.yaml`, `experiment.json`, `events.jsonl`, and `observations.jsonl`. It uses file metadata without reading log contents, reconstructing metrics, or compressing data, so large results remain quick to size. Generated analysis caches/images, the export-time `metrics.json` and `export.json`, and filesystem allocation overhead are excluded. Unreadable experiment metadata can still have a source size. If the source files cannot be statted safely, the field is omitted and the UI shows **Source · —**.

The dashboard makes no automatic size HEAD requests and runs no ZIP-size expiry timers. An explicitly requested `HEAD /api/v1/experiments/RUN_ID/download` still measures the exact ZIP `Content-Length` with a streaming byte counter; actual GET downloads use that measurement too. Archives are not retained wholesale in memory or on disk. Concurrent measurements for one run are shared, with at most two different runs measured at once. For API compatibility, the list can still return an already prepared, valid `downloadBytes` value, but the UI does not use it.

When no publication is pending, an unchanged file/state version keeps the measured byte count while each HEAD or download records a fresh, full-precision export time; the stored `export.json` entry uses a fixed-width timestamp so its encoded length stays constant. Stable list rows omit `downloadSizeMaxAgeMs`, and stable HEAD responses omit `X-KPL-Result-Size-Max-Age-Ms`. A result with pending publications briefly reuses both its byte count and export boundary. Its list row includes `downloadSizeMaxAgeMs`, and HEAD includes `X-KPL-Result-Size-Max-Age-Ms`; each value is a positive number of milliseconds calculated from the server's remaining cache lifetime after subtracting a one-second response safety grace. This relative lifetime avoids depending on synchronized client and server clocks. The first list that observes a prepared value extends the short lifetime once, then reports the max age from that extended boundary so an immediate GET still matches the displayed size; repeated list refreshes do not keep sliding the deadline. If the server has no lifetime left beyond the safety grace, the list omits both `downloadBytes` and `downloadSizeMaxAgeMs`, while HEAD measures a new boundary before responding.

The first HEAD after Controller startup or a source/state change must read the captured files and rebuild metrics; pending results repeat that work after their short cache expires. HTTP request cancellation is checked while waiting for a measurement slot, reading source logs, and aggregating the reconstructed metrics; canceled requests stop without producing a server-error response. Large saved event logs can therefore take longer to measure. This work occurs only for explicitly requested HEADs and actual downloads; displaying source sizes does not require it.

After a restart, a saved run that still says `running` or `queued` is displayed as `interrupted`; its original metadata is preserved in the ZIP. This is a display status, not evidence that its Peers have stopped. Saved results do not automatically restore live experiment state, resume execution, or replay live counters. ZIP metrics are rebuilt from the retained log. An unreadable metadata file is displayed as `unreadable`; inspect the Controller logs and stored files before retrying.

**Saved results → Retry from run N (count)** restarts saved unfinished iterations, including the failed/interrupted iteration itself. Completed runs keep their results. For a failed single experiment, use **Retry experiment**. The Controller first removes old Peers, then starts each selected iteration from its first phase with the original scenario, iteration number and seed. If cleanup fails, the new runs remain canceled and can be retried after resolving the cause. Old logs remain under **Previous attempts** and are excluded from batch progress and mean analysis. **Continue remaining** retains its earlier behavior: skip attempted runs and execute only the never-started remainder.

**Saved results → Delete** permanently deletes the selected run's scenario, metadata, events, and live metric index after confirmation. Running/queued experiments, members of an active batch, and results being downloaded are protected. The deletion API is `DELETE /api/v1/results/{id}` with the login session. It does not stop Peers; previously scraped Prometheus/Grafana history remains. Deletion markers prevent late telemetry from recreating a deleted result; preserve them with Controller data in backups and migrations. Reconstructing ZIP metrics uses memory proportional to distinct event IDs and message/receiver pairs; the raw file copy itself is streamed. See the [REST API guide](api.md) for status codes and download headers.

Explicit ZIP size requests (`HEAD`) and list inspections do not block deletion. Actual ZIP downloads (`GET`) protect their captured result until the request finishes. The delete request has a 30-second browser timeout; on timeout the Controller may still finish, so refresh or retry the same result. A slow follow-up list refresh does not keep the dialog controls disabled.

The series header's **Delete group** confirms the saved-run count and deletes the entire batch through `DELETE /api/v1/result-batches/{batchId}`. Unlike individual-source deletion, this also removes the separate `batch-analyses/{batchId}` mean files and cancels pending analyses. All members are checked for activity/downloads before deletion starts. A storage error can leave partial progress; refresh and retry after resolving the error. Existing Prometheus/Grafana history remains.

Saved-result lists and downloads require login. After [API login](api.md#authentication), reuse the cookie jar:

```bash
curl --fail -b "${KPL_COOKIE_JAR:?Log in first}" http://control-node:8080/api/v1/results
curl --fail -b "${KPL_COOKIE_JAR:?Log in first}" --output run-results.zip \
  http://control-node:8080/api/v1/experiments/RUN_ID/download
```

Replace `RUN_ID` with an ID from the saved list. In Swarm, use the control node's address. With `KPL_STACK_NAME=kpl`, original files reside in the `kpl_controller-data` volume on that node. It is mounted at `/var/lib/kpl/data` in the Controller, with run files under `runs/<run-id>`. The manager's `swarm.sh remove` command preserves this volume. There is no automatic raw-event retention limit or cross-node replication; manage disk space and backups separately.

Exports contain collected telemetry, including subscription-session start/checkpoint/stop evidence and recorded Agent termination confirmations. Peer retries, Agent queue backpressure, and graceful drains reduce loss but have finite bounds; forced termination does not drain the Peer. Source sequence gaps make missing receipts unknown. A download cannot recover missing events or reveal completely invisible sessions. `/api/v1/events` still returns only the latest 300 events, and the web event view displays the newest 40 from that buffer, while the ZIP reads the full saved log.

## Analysis and image retention

A run's raw export and its computed analysis have different boundaries. **Download results / Download snapshot** captures the source files when exporting. **Download analysis JSON** uses the boundary recorded by the completed analysis job; logs appended later do not enter that artifact until a fresh analysis runs. Compare `export.json.exportedAt` with analysis `asOf`/job `snapshotAt` when checking totals.

| File / artifact | Location and contents |
|---|---|
| `analysis-job.json` | Stored beside the run files after analysis is requested; current attempt, state, progress and source boundary |
| `analysis-result.json` | Completed server analysis: main metrics, research messages/populations/evidence, derived graph and bandwidth timelines, distributions and fits |
| `analysis-summary.json` | Completed compact comparison response; keeps aggregates/distributions/fits and message count while omitting per-message paths and source timelines |
| PNG / chart CSV / chart-definition JSON | Generated by the browser from the overview, message or comparison view; expand a title to download individually. **Download all PNG + CSV (ZIP)** includes all graph protocol variants regardless of the displayed protocol or collapsed titles |
| Imported v2 files / comparison selection | Kept in the browser view; not uploaded to the Controller or saved as a server comparison job |

The three server analysis files are not included in **Download results** ZIPs. Use the analysis download endpoints to retain computed JSON, and save chart downloads separately. A comparison ZIP records the generated charts, not the full source archives or imported files needed to rerun the analysis. Keep those sources and the selected Series/Case/parameters with the software revision.

Completed analysis survives Controller restart in the data volume. A new attempt replaces the current cache; download it first if an older analysis boundary must be retained. Accepted server calculations continue after closing the browser; image conversion and comparison calculations run when the view is reopened/recreated. A browser download is not a server-rendered image archive. Failed/interrupted/canceled jobs can be retried, as defined in the [background API](api.md#background-analysis). Deleting an eligible saved run removes these analysis files with its source records.

Repetition **Analyze batch mean** jobs are stored separately in `<data-dir>/batch-analyses/{batchId}/job.json` and `result.json`. Their artifacts contain equal-run means, sample SDs, valid counts and compact inputs for reproducing overview charts. Individual-source deletion leaves an existing batch snapshot on disk; reanalysis after membership changes replaces the current batch files. Download analysis JSON or the mean PNG/CSV ZIP, and include both `runs` and `batch-analyses` in server backups. See [batch means](visualization.md#batch-mean-for-repetitions-of-one-experiment) for eligibility, exclusions and sample-count semantics.

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

`session-window-v1` uses actual publication time and `publish.deliveryWindow` (default 10s, positive and at most 1h). Session evidence must prove subscription throughout that window for the primary conditional ratio. Departures before the deadline are excluded even after early success; they remain in the starting-cohort ratio. Later joins and publisher-local receipts are excluded from both. Disconnection or mesh changes do not remove a subscriber. Show stable bounds, starting-cohort bounds, coverage, pending messages, and uncertainty together. A missing sequence means unknown, not a confirmed miss; an absent availability proof is not a confirmed departure.

Grafana session panels use only the Run filter and aggregate receiver-pair counts, not averages of percentages. Bounds describe observed cohorts; they do not establish an unseen population. Starting-cohort delivery and stable coverage remain available despite publication-time availability warnings or `measurementIncomplete`; they are N/A only when the selected runs contain no known starting pair. For each aggregation, known starting = stable + departed + continuity-unknown. The legacy-publications panel identifies historical data excluded from these results. New panels use `kpl_window_*` only; historical `kpl_delivery_*` and `kpl_propagation_latency_seconds` keep their old meanings and must not be combined.

Overall `deliver` counts include local delivery and cannot be divided by publication counts to obtain reachability. TCP retransmissions can turn packet loss into delay rather than message loss. See the [metric definitions and formulas](experiment-metrics.md). Late batches can correct window gauges and histogram buckets; query them directly, not with `rate`/`increase`. Grafana latency shows cumulative whole-run quantiles, not quantiles restricted to the selected time range.

Cumulative counters are independent of the web interface's 300-event recent buffer. They reset when the Controller process restarts, and historical `events.jsonl` files are not replayed automatically. Time series already stored in Prometheus remain available, and `rate`/`increase` handle observed counter resets. They cannot recover events that disappeared before a scrape or telemetry that failed to arrive. Because `increase` estimates interval growth from scrape samples, it does not always exactly match integer cumulative event counts.

Raw deliveries contribute to delivery counts and bytes but not to the latency histogram. Do not interpret intervals without latency samples as 0 ms. At startup, each Peer has a five-second budget for up to seven Controller health samples, spacing fast failures by 250ms and using the minimum-RTT midpoint. Failure does not block Ready: an unsynchronized Peer retries every five seconds, while a synchronized Peer refreshes every 30 seconds. A sample expires after two minutes without success; the last offset remains for timestamp continuity, but trusted clock metadata stops until recovery. A bounded negative estimate can still prove on-time delivery, but it remains outside the latency histogram. Keep chrony/NTP enabled on all hosts. Checkpoint evidence describes instrumented application sessions rather than physical uptime.

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

The Dashboard coalesces telemetry bursts into at most four renders per second and retains unchanged text, lists, and topology elements. Live metric updates preserve the open card and the horizontal scroll position while refreshing its values.

The Swarm stack discovers registered targets from `GET /api/v1/prometheus/agent-targets`. Use `sh scripts/swarm.sh access` to inspect the advertised URLs, ensure the configured port is free on every selected Agent node, and permit TCP traffic from the control node. If operators open an Agent metrics link directly, permit their browser's trusted management network as well; block untrusted sources. `up{job="kpl-agent"}` distinguishes successful scrapes from registered targets that are unreachable through a firewall or an incorrect Swarm `NodeAddr`.

Image versions are pinned to Prometheus `v3.13.2` and Grafana `13.2.1`. When upgrading, consult the official [Prometheus downloads](https://prometheus.io/download/) and [Grafana Docker installation guide](https://grafana.com/docs/grafana/latest/setup-grafana/installation/docker/), then revalidate the configuration and dashboards.

[Bandwidth measurement](bandwidth.md) · [v2 analysis coverage audit](v2-analysis-coverage.md)

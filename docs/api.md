# REST API

English | [Korean](api.kr.md)

The dashboard and operational APIs require a login session. See [authentication](#authentication) for public monitoring exceptions and internal service authentication.

## Controller endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/auth/login` | Log in with JSON credentials and receive a session cookie |
| `GET` | `/api/v1/auth/session` | Current username and session expiry |
| `POST` | `/api/v1/auth/logout` | Revoke the current session and its SSE streams |
| `GET` | `/metrics` | Prometheus exposition for Controller and experiment metrics |
| `GET` | `/api/v1/health` | Controller health and current UTC time used for Peer clock sampling |
| `GET` | `/api/v1/ui-config` | Published Prometheus and Grafana ports used by Dashboard navigation |
| `GET` | `/api/v1/prometheus/controller-targets` | Public Controller metrics endpoint as a Prometheus HTTP service-discovery group |
| `GET` | `/api/v1/prometheus/agent-targets` | Prometheus HTTP service-discovery groups for online Agents with advertised metrics URLs |
| `GET` | `/api/v1/snapshot` | Full dashboard snapshot, including node `peerScores` |
| `GET` | `/api/v1/agents` | Agent state |
| `GET` | `/api/v1/nodes` | Peer state, including inspected `peerScores` |
| `GET` | `/api/v1/network` | Peers with `peerScores`, connection edges, and propagation metrics |
| `GET` | `/api/v1/bootstrap?runId={runId}` | Ready bootstrap peers belonging only to the required run ID |
| `GET` | `/api/v1/discovery?runId={runId}&topic={topic}&requesterNodeId={nodeId}` | Ready, online PubSub transport candidates from the same run and exact topic |
| `GET` | `/api/v1/events` | Recent trace events |
| `GET` | `/api/v1/stream` | Real-time snapshot SSE stream, including `peerScores` |
| `GET` | `/api/v1/experiments` | Experiment state plus `activeJobs`, `completedJobs`, `failedJobs`, and `canceledJobs` counters |
| `GET` / `POST` | `/api/v1/scenarios` | List scenario summaries or save validated `{name, yaml}` |
| `POST` | `/api/v1/scenarios/validate` | Validate raw YAML without saving or running; requires login |
| `GET` / `PUT` / `DELETE` | `/api/v1/scenarios/{id}` | Load, update, or delete one saved scenario |
| `GET` | `/api/v1/results` | Saved experiment results, including runs from previous Controller sessions |
| `DELETE` | `/api/v1/results/{id}` | Delete an inactive saved result; active batches and downloads are protected |
| `DELETE` | `/api/v1/result-batches/{batchId}` | Delete all saved runs in one group and its mean analysis; preflight all members for activity/downloads |
| `POST` | `/api/v1/result-batches/{batchId}/resume` | Continue only never-started runs after a failed/interrupted attempt; preserves the batch and iteration IDs. Returns `202` with the first queued run, `409` if busy or ineligible, and `404` for an unknown batch. Uses the login session and `X-KPL-Request: dashboard`. |
| `GET` | `/api/v1/experiments/{id}/analysis` | Chart distributions, timelines and metrics from saved events and observations |
| `GET` / `POST` | `/api/v1/analysis-jobs/{id}` | Inspect / submit background analysis. Duplicate requests reuse work; `?refresh=1` requests a new snapshot |
| `GET` / `HEAD` | `/api/v1/analysis-jobs/{id}/result?jobId={jobId}` | Download persisted analysis JSON. Unfinished or mismatched attempts return `409` |
| `GET` / `HEAD` | `/api/v1/analysis-jobs/{id}/summary?jobId={jobId}` | Compact comparison artifact with aggregates, distributions and fits; omits per-message paths and source timelines |
| `GET` / `POST` | `/api/v1/batch-analysis-jobs/{batchId}` | Inspect / submit equal-run batch analysis; `?refresh=1` regenerates from current logs |
| `GET` / `HEAD` | `/api/v1/batch-analysis-jobs/{batchId}/result?jobId={jobId}` | Download persisted means and compact per-run overview inputs |
| `GET` / `HEAD` | `/api/v1/experiments/{id}/download` | Download the scenario, metadata, events, optional observations and derived metrics as ZIP, or measure its size without a response body |
| `POST` | `/api/v1/experiments` | Run YAML once, or JSON `{scenario, repetitions}` for 1–100 sequential runs |
| `POST` | `/api/v1/experiments/{id}/stop` | Cancel a running experiment, then perform bounded job shutdown and generation-fenced Peer cleanup |

The `runId` query parameter on `/api/v1/bootstrap` is required. The registry returns only ready `boot` nodes with usable identity and address data from that run, so concurrent experiments cannot discover one another's bootstrap peers. `/api/v1/discovery` requires all three shown query parameters, excludes the requester, and returns configured topic participants rather than observed delivery or mesh outcomes. `/api/v1/prometheus/agent-targets` is a read-only operational endpoint used by the supplied Swarm Prometheus configuration; it omits offline Agents and Agents without a valid metrics URL.

The bootstrap response is an array of `{nodeId, peerId, addresses}` records (or `null` when empty). Unlike discovery, bootstrap does not filter on Agent online status. Discovery returns an array, empty when no candidates match, with an additional `subscribed` flag. It returns the full eligible candidate set; the requesting Peer applies rendezvous ranking, connection budgets, and retries as described in [topology](topology.md#bootstrap-and-topic-discovery).

`/api/v1/prometheus/controller-targets` advertises the Controller `--metrics-url` (`KPL_CONTROLLER_METRICS_URL`) and returns `[]` when it is unset. It is a public, read-only endpoint; the URL must be HTTP(S), end in `/metrics`, and have no credentials, query, fragment, or loopback/unspecified address. The Swarm helper supplies the control node address and `KPL_HTTP_PORT` automatically.

## Submit, stop, and observe runs

`POST /api/v1/scenarios/validate` accepts a raw YAML body (`Content-Type: application/yaml`), limited to 1 MiB. A valid scenario returns `200` with `{valid: true, name, phases}`. Empty input, YAML syntax errors, unknown fields, invalid settings, and multiple YAML documents return `400` with `{error: "…"}`; parser diagnostics retain line numbers when available. Bodies over the limit return `413`. This endpoint uses the same parser as save/run, creates no records or jobs, and reuses the login session. Validation checks configuration, not Agent capacity or runtime connectivity.

`POST /api/v1/experiments` returns `202` with the first run's experiment object. Use `Content-Type: application/json` for `{scenario, repetitions}`; `scenario` is a YAML string and omitted `repetitions` defaults to `1`. Other content types are treated as raw YAML. Raw YAML and the decoded JSON `scenario` string are limited to 1 MiB. The JSON request envelope allows `6 * 1 MiB + 64 KiB` for escaping. An oversized request envelope returns `413`; a decoded YAML string over its limit or invalid scenario/repetition returns `400`.

Repeated submissions reserve a separate run ID and result record for every iteration, sharing `batchId`, `iteration`, and `repetitions`. Runs execute sequentially; a failed or canceled iteration cancels the queued remainder. Stopping any member while its repetition batch is still active cancels that batch. For `repetitions > 1`, every iteration, including the last, fences and removes its Peers before finalization. Only a naturally successful single run may retain Peers when its YAML omits `stop-all`.

Live experiment objects in `GET /api/v1/experiments`, dashboard snapshots/SSE, and the initial submission response may include `timing`: `estimatedFinishAt` (UTC), non-negative `remainingSeconds`, `basis` (`scenario` or `observed-runs`), `observedRuns`, and `overdue`. Active repeated runs also include `batchEstimatedFinishAt`, `batchRemainingSeconds`, and `batchOverdue` (optional zero/false fields). Queued finish times include earlier runs in the same batch. Completed/failed/canceled runs and estimates with insufficient timing information omit `timing`; it is not persisted in result manifests. See [estimated finish times](scenario-library.md#estimated-finish-times) for calculation and uncertainty.

The stop endpoint returns `202` once cancellation is requested, before cleanup completes. Observe `/api/v1/experiments` or the snapshot until the final state is recorded. A run with no remaining cancellation handle returns `404`. SSE coalesces updates to at most one full snapshot per second, shares encoding across clients, and sends an initial `event: snapshot`, subsequent full snapshots on state updates, and full snapshots every 15 seconds even without events; it does not provide event-ID replay. `/api/v1/events` contains at most the 300 most recent events across live Controller state.

From the repository root, replace the host with the Controller address from `access` and first follow [authentication](#authentication) to create `KPL_COOKIE_JAR`:

```bash
curl -X POST http://control-node:8080/api/v1/experiments \
  -H 'Content-Type: application/yaml' \
  -b "${KPL_COOKIE_JAR:?Log in first}" -H 'X-KPL-Request: dashboard' \
  --data-binary @examples/smoke.yaml
```

The scenario library endpoints store reusable editor inputs independently of experiment results. The list omits YAML, while individual GET, POST, and PUT responses include it. See the [scenario library guide](scenario-library.md) for the UI workflow, payloads, validation limits, and storage location.

Raw events, including typed bandwidth samples, are stored at `<data-dir>/runs/<run-id>/events.jsonl`; the exact input is stored as `scenario.yaml`, experiment metadata as `experiment.json`, and collected graph samples and topology/score summaries as optional `observations.jsonl` in the same directory. The local Controller data directory defaults to `data`. In Swarm, the persistent `controller-data` volume is mounted at `/var/lib/kpl/data`, and each run's files are under `/var/lib/kpl/data/runs/<run-id>`.

Use **Download results** in the Dashboard to export a run as ZIP. **Saved results** also lists files retained from previous Controller sessions; **Refresh** reloads that list. Running experiments offer **Download snapshot**, which contains the records saved when the download starts. These exports include the full saved event log, independently of the 300-event recent buffer. See [result downloads](monitoring.md#download-experiment-results) for archive contents and collection limits.

`GET /api/v1/results` returns `sourceBytes`, the total uncompressed bytes of saved scenario, experiment metadata, event and observation source files at the last inspection, including running and queued results. It requires no log-content reads or ZIP generation and excludes analysis caches and generated images. The field is omitted only when source file sizes cannot be read safely. The dashboard uses this value without automatic ZIP size requests.

`DELETE /api/v1/results/{id}` returns `204` on deletion, `404` if the result is absent, and `409` while the run/batch is active or an actual `GET` download holds it. Explicit `HEAD` size calculations and list reads do not cause a download conflict.

`DELETE /api/v1/result-batches/{batchId}` uses the login session and exact batch ID. It checks every member before deleting any files, returns `409` if a run is active/finalizing or a ZIP download holds a member, and removes all saved member runs (including failed/canceled runs), their individual analyses, and the separate batch mean. Success returns `200` with `{"deletedIds":["run-id", "..."]}`; an absent group returns `404`. An orphan batch mean can also be removed. Storage failures return `500` and may leave a partially deleted group; refresh the list and resolve the storage error before retrying. The browser uses a 30-second request timeout; the Controller may finish after that timeout.

## Background analysis

Use `POST /api/v1/analysis-jobs/{id}` to analyze one saved run, then poll `GET` on that path. Both POST and GET/HEAD require a login session. An admitted queued/running job returns `202`; reuse of a current completed artifact returns `200`. At most 32 queued/running jobs are admitted across the Controller (`503` when full), and the shared analysis slot permits one computation at a time. Closing a client does not cancel admitted work.

The status `state` is `idle`, `queued`, `running`, `completed`, `failed`, `canceled`, or `interrupted`. `idle` means the saved run has no requested analysis. Failed/canceled/interrupted work can be resubmitted. Persisted unfinished work recovered after restart becomes `interrupted`; orderly cancellation can already have saved `canceled`. There is no separate job-cancel endpoint. Deleting an eligible saved run cancels its job and removes its artifacts.

| Status field | Meaning |
|---|---|
| `id`, `runId` | Analysis attempt identity and source run identity; a retry or refresh can create a new attempt |
| `version`, `analysisVersion` | Status/response format version `1`, computation version `3` for current jobs; independent of metric `definition` and log `rpcMetadataVersion` |
| `phase`, `processedBytes`, `totalBytes`, `progress` | Current stage and log-reading byte progress (0–100). Reading 100% does not complete graph/propagation calculations or persistence; wait for `state: completed` |
| `createdAt`, `startedAt`, `updatedAt`, `finishedAt` | Job timestamps; a phase not reached has no meaningful timestamp |
| `snapshotAt` | Source boundary captured after the worker obtains its analysis slot, rather than at POST admission |
| `error`, `resultUrl` | Failure detail or completed full-artifact URL with its attempt ID |

During `phase: saving`, an individual or batch job remains `running`; status reads, experiment event ingestion and stop requests can continue while the result files are saved. Before publishing completion, the server rechecks that the attempt is current and uncanceled and, for an individual analysis, that its source has not been deleted.

Requests reuse an existing queued/running job even with `?refresh=1`. A current completed job is reused unless refreshed; an outdated completed `analysisVersion` is regenerated on POST. GET status alone does not refresh it. **Images** performs that version check automatically. A fresh analysis uses a new recorded boundary; absent historical metadata remains absent.

After completion, `GET` or `HEAD` `/api/v1/analysis-jobs/{id}/result?jobId={jobId}` serves the full JSON; `/summary?jobId={jobId}` serves the comparison artifact. Supplying `jobId` guards against downloading a different attempt; omission selects the current completed attempt. An unfinished/mismatched attempt returns `409`, a missing run `404`, and malformed/unreadable stored data normally `422`. The compact artifact sets `observations`, `timeline`, `bandwidthTimeline` and `research.messages` to empty arrays while preserving `messageCount`, metrics, aggregates, distributions and fits. It cannot supply per-message paths or original timelines. Both responses carry `analysisId`, `analysisVersion`, and the source `asOf` boundary.

`GET /api/v1/experiments/{id}/analysis` remains a synchronous compatibility route with a two-minute request timeout. It computes a response without creating a persisted background job. Use the job API for long analysis and later downloads. The [metric guide](experiment-metrics.md#saved-result-research-metrics) owns research definitions; [monitoring](monitoring.md#analysis-and-image-retention) owns saved files, and [visualization](visualization.md) describes the browser workflow.

## Repetition batch analysis

`POST /api/v1/batch-analysis-jobs/{batchId}` analyzes completed runs from the exact submission batch. All batch members must finish execution/cleanup and at least two must be completed. Active batches return `409`; insufficient completed runs or inconsistent metadata return `422`; an unknown batch returns `404`. Authentication, duplicate admission, retry and restart behavior follow individual analysis. Individual and batch jobs share the 32-job admission bound; up to 100 repetitions are processed sequentially within one batch job.

Status identifies the batch with `batchId` (`runId` is empty) and adds selected `runIds`, `completedRuns`, `totalRuns`, `expectedRuns`, and a `membership` signature. `progress` weights each run equally; byte fields describe the current run. `snapshotAt` is the most recently captured run boundary; `runs[].asOf` records each source boundary in the artifact. Membership/state changes make status GET expose the previous completed cache as `idle`, and a new POST recomputes it. Log-only changes require `?refresh=1`. If batch membership changes during calculation or saving, the job fails instead of publishing a completed result with the old selection; retry to capture the current members.

The artifact has `version: 1`, `analysisVersion: 3`, `aggregation: "equal-run-mean-v1"`, batch/attempt identities, `expectedRuns`, `missingRuns`, `excluded`, `runs`, and `summary`. Summary entries under `metrics.*`, `research.*`, `bandwidth.sentBytes` and `bandwidth.receivedBytes` contain `average`, `deviation` (between-run sample SD), `median`, and `count` (valid runs). An undefined mean or single-run SD is `null`. Compact `runs` preserve graph metrics, observations, bandwidth, distributions, and message time series/receiver curves/origin counts in `research.overview`; raw message/node paths are omitted. See [batch means](visualization.md#batch-mean-for-repetitions-of-one-experiment) for chart alignment and distribution semantics.

Files are persisted as `<data-dir>/batch-analyses/{batchId}/job.json` and `result.json`. Closing a view or restarting the server does not remove a completed result. Download with an explicit `jobId` selects that completed snapshot, returning `409` if replaced by another attempt. Deleting an individual source does not remove the existing batch snapshot files. Reanalysis replaces the current files with the changed selection; download an older result before refreshing if it must be retained.

## Internal cluster endpoints

These REST/JSON endpoints serve component communication. Registration, heartbeats, and forwarded events are sent **to the Controller**; lifecycle commands are sent **to an Agent**.

| Caller → server | Method | Path | Purpose |
|---|---|---|---|
| Agent → Controller | `POST` | `/api/v1/agents/register` | Register the Agent instance |
| Agent → Controller | `POST` | `/api/v1/agents/heartbeat` | Report Agent and Peer snapshots |
| Agent → Controller | `POST` | `/api/v1/events/batch` | Forward up to 5000 events per batch |
| Controller → Agent | `GET` | `/api/v1/status` | Refresh Agent and Peer state |
| Controller → Agent | `POST` | `/api/v1/nodes` | Create a Peer from `CreateNodeRequest` |
| Controller → Agent | `DELETE` | `/api/v1/nodes` | Fence all retained runs, remove remaining Peers and drain queued telemetry before returning `204`; used during Controller shutdown |
| Controller → Agent | `DELETE` | `/api/v1/nodes/{nodeId}` | Request one Peer's shutdown |
| Controller → Agent | `POST` | `/api/v1/nodes/{nodeId}/publish` | Proxy a publish request |
| Peer → Agent | `POST` | `/api/v1/nodes/{nodeId}/status` | Report this Peer's latest status |
| Peer → Agent | `POST` | `/api/v1/telemetry` | Submit a telemetry batch |
| Agent → Peer | `GET` / `POST` | `/health` / `/publish` | Check readiness or publish through the Peer HTTP API |

Periodic Agent heartbeats use `partial: true` and split Peer records into JSON bodies of at most 10 MiB. If one Peer record alone exceeds the limit, the Agent reports an error naming that node and leaves the record unacknowledged. They include active/failed Peers and successfully stopped Peers awaiting Controller acknowledgement. Acknowledged successful exits are omitted from later periodic heartbeats and replayed after re-registration. Omission from a partial report does not stop a node or free its capacity. `GET /api/v1/status` and heartbeats with `partial` absent or false retain the full-inventory contract, including compact successful-exit records. The Agent keeps that full inventory in memory and exposes it on explicit status requests.

Telemetry requests are limited to 5000 events and a 10 MiB JSON body. Peer and Agent senders split batches by their encoded byte size, including escaping and the envelope; retries retain source order and event identities. A successful prefix is removed before sending the remaining events. Both receivers validate every event before admission: a single event that cannot fit in a 10 MiB encoded batch returns `413`, with none of that request admitted. The Agent checks after normalizing its identity. This also prevents direct Controller submissions from creating oversized event lines that archive analysis cannot read.

During orderly Agent shutdown or `DELETE /api/v1/nodes`, telemetry drain waits for Controller acknowledgement of both queued events and any batch already in flight, including final Peer events. A failed batch retains its order for retry; drain reports failure if acknowledgement cannot complete within its time limit.

JSON bodies must contain one value. Trailing whitespace is allowed within the body limit; a second JSON value, trailing garbage, or a body over the decoder limit returns `400`. Generic Controller/Agent JSON handlers use a 10 MiB limit; Peer `/publish` uses 1 MiB. Scenario request envelopes have the separate limits described above. A locally generated Peer event that cannot be JSON-encoded or fit in a batch is logged and counted in `telemetry_drop`, preserving its source sequence gap while later events continue. Network failures retain the pending batch for retry.

The Agent additionally exposes this Controller-driven cleanup endpoint:

| Method | Agent path | Description |
|---|---|---|
| `DELETE` | `/api/v1/runs/{runId}/nodes?generation=N` | Requires an unsigned `generation`; atomically raises the run fence through N, rejects later creates at generation N or below, stops existing nodes in those generations, and returns `202 Accepted` |

Internal endpoints may change independently of the operator API. Request and snapshot field definitions are in [`internal/model/model.go`](../internal/model/model.go), with bandwidth types in [`internal/model/bandwidth.go`](../internal/model/bandwidth.go); handlers are in [`internal/controller/api.go`](../internal/controller/api.go) and [`internal/agent/api.go`](../internal/agent/api.go).

## Detailed Peer logs

Updated Peers add metadata actually exposed by upstream libp2p to `events.jsonl`. Existing publish/deliver/duplicate events and main Metrics aggregation remain in use. `messageId` is the envelope application ID or the raw `pubsub-<hex native ID>`; `fields.pubsubMessageId` and detailed ID lists encode pubsub wire IDs as hex.

| Event / field | Meaning |
|---|---|
| `rpcMetadataVersion`, `rpcObservationId` on `send_*`, `recv_*`, `drop_*` | Format version `1` and identity of one local RPC callback. Each event retains its own `eventId` |
| `rpcMessageCount`, `rpcSubscriptionCount` | Complete data-message and subscription counts for that RPC |
| IHAVE `topicMessageIds` | Advertised IDs grouped by topic |
| IWANT/IDONTWANT `messageIds` | Requested/unwanted IDs; these protocol entries carry no topic |
| `messageIdsComplete`, `omittedMessageIds` | Completeness and omitted count; `messageIdCount` keeps the full total |
| PRUNE `peerExchangeIdsByTopic`, `peerExchangeIdsComplete`, `omittedPeerExchangeIds` | Actual trace peer-exchange IDs by topic and completeness |
| `rpc_metadata`: `direction`, `messages`, `subscriptions` | send/recv/drop, `{topic,pubsubMessageId}` records, `{topic,subscribe}` records. A separate event is emitted only when data/subscription metadata exists |
| `subscriptionsComplete`, `omittedSubscriptions` | Subscription-list completeness and omissions |
| `pubsub_reject`: `reason`, `pubsubMessageId` | Rejection reason and wire ID reported by libp2p |
| `timestampSource`, `sourceTimestamp` | Trace time provenance and original unadjusted time. Missing upstream time uses `peer-clock` without fabricating a source timestamp |
| `clockBasis`, `clockOffsetMs`, `clockUncertaintyMs` | Added when a valid Controller clock synchronization estimate exists |

Detailed IDs are limited to 8,192 entries and 512KiB of hex per category; subscription lists are capped at 8,192. RPC/entry/ID totals retain their full counts even when detailed lists are truncated. Existing `telemetry_drop` and source sequences expose event loss. Payload bodies, IWANT sender-queue causes and nonexistent global RPC IDs are not collected. Eager/Lazy follows the metadata estimation rules in [experiment metrics](experiment-metrics.md#eager-push-and-lazy-pull-estimates).

## Authentication

Set the same `KPL_USER` and `KPL_PASSWORD` on the Controller and every Agent. Both are required at startup. The dashboard first opens **Log in**; a successful login creates a random, server-side session identified by an HttpOnly, SameSite=Strict cookie. The cookie is Secure over HTTPS (including a trusted reverse proxy setting `X-Forwarded-Proto: https`). Sessions expire after 12 hours, are revoked by **Log out**, and do not survive a Controller restart. Logging out or expiry closes that session's SSE streams without stopping experiments or background analysis. There is one configured account with full dashboard permissions.

All dashboard/API reads, writes, SSE, scenario validation, analysis and downloads require a session. Unauthenticated API requests return `401`; the dashboard redirects to `/login`. Session-authenticated mutations also require `X-KPL-Request: dashboard` and reject cross-origin requests. The UI adds this header automatically; it never asks for another token and stores neither the password nor the session cookie in localStorage. Login accepts only JSON `{user, password}`; wrong credentials return `401`, and ten failed attempts from one connection IP within five minutes cause `429`. Sessions and failure records are bounded to 1,024 entries each.

Only the login assets, health/clock endpoint, `/metrics`, and the two Prometheus target-discovery endpoints remain public. Internal Agent registration/heartbeats/events and Peer bootstrap/discovery accept an automatically derived, domain-separated service key. It is generated from the account configuration and passed to Peers without exposing the login password to them. This key cannot access dashboard result APIs or start experiments. `KPL_API_TOKEN` and the `--token` CLI flag are retired; direct service clients must be updated together. Changing credentials requires redeploying Controller and Agents, with no active experiments or retained Peers using the old key. Grafana keeps its separate login.

For REST clients, replace the address below with `access` output and log in once. The cookie jar is private and can be reused by the examples above:

```bash
KPL_URL=http://control-node:8080
KPL_COOKIE_JAR=$(mktemp)
read -r -p 'Username: ' KPL_USER
read -r -s -p 'Password: ' KPL_PASSWORD; printf '\n'
export KPL_USER KPL_PASSWORD
python3 -c 'import json,os; print(json.dumps({"user":os.environ["KPL_USER"],"password":os.environ["KPL_PASSWORD"]}))' |
  curl --fail-with-body -c "$KPL_COOKIE_JAR" \
    -H 'Content-Type: application/json' --data-binary @- "$KPL_URL/api/v1/auth/login"
unset KPL_PASSWORD
curl --fail-with-body -b "$KPL_COOKIE_JAR" "$KPL_URL/api/v1/snapshot"
```

After completing your requests, log out and remove the cookie jar:

```bash
curl --fail-with-body -b "$KPL_COOKIE_JAR" -H 'X-KPL-Request: dashboard' \
  -X POST "$KPL_URL/api/v1/auth/logout"
rm -f "$KPL_COOKIE_JAR"
```

HTTP does not encrypt credentials or cookies. Use HTTPS or a trusted management network; an HTTPS proxy must overwrite the forwarded protocol header.

The same four scenario job counters are present in `/api/v1/snapshot` and SSE snapshots. The dashboard displays them on each run, so active, successful, failed, and canceled background work is visible without inspecting Controller logs.

See [visualization](visualization.md) for analysis responses, charts and export formats.

[Bandwidth measurement](bandwidth.md) · [v2 analysis coverage audit](v2-analysis-coverage.md)

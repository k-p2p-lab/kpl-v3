# Saved Results, Exports, and Retention

English | [Korean](results.kr.md)

[Documentation index](README.md) · [Repository](../README.md)

This guide owns saved source files, downloads, deletion, and backup boundaries. Use [execution and recovery](experiments.md) for continuation/retry, [visualization](visualization.md) for figures and comparisons, and [metrics](experiment-metrics.md) for formulas.

## Find saved results

**Saved results** supports searching by experiment name, batch ID, or run ID, together with **In progress**, **Completed**, and **Needs attention** status filters. A matching run keeps its whole batch visible, including previous attempts, so progress and batch actions remain available. Status filters use current attempts: **Completed** requires every current run in the group to be complete; **Needs attention** includes failed, interrupted, canceled, or unreadable results.

The displayed count is the number of visible saved runs. Filters stay selected while the result list refreshes; **Clear filters** restores the full list. On mobile, result rows become cards with labeled times and wrapping action buttons. The Agent table remains horizontally scrollable.

Within a repeated-run group in **Saved results**, rows are ordered by numeric **Run n of M** (1, 2, …, 10), including queued runs and retries. The order stays stable when a queued run starts; previous attempts remain in their separate table and entries without a valid iteration appear last. Batch groups keep their archive position.

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

**Source / Live source** excludes analysis caches, generated images, export-time derived files, and filesystem overhead. The Dashboard does not measure ZIP size automatically. [Download API details](api.md#saved-results-and-downloads) define `sourceBytes`, explicit HEAD requests, and cache headers.

Saved-result lists and downloads require login. After [API login](api.md#authentication), reuse the cookie jar:

```bash
curl --fail -b "${KPL_COOKIE_JAR:?Log in first}" http://control-node:8080/api/v1/results
curl --fail -b "${KPL_COOKIE_JAR:?Log in first}" --output run-results.zip \
  http://control-node:8080/api/v1/experiments/RUN_ID/download
```

Replace `RUN_ID` with an ID from the saved list. Use the control node address printed by `access`.

Exports contain collected telemetry, including subscription-session start/checkpoint/stop evidence and recorded Agent termination confirmations. Peer retries, Agent queue backpressure, and graceful drains reduce loss but have finite bounds; forced termination does not drain the Peer. Source sequence gaps make missing receipts unknown. A download cannot recover missing events or reveal completely invisible sessions. `/api/v1/events` still returns only the latest 300 events, and the web event view displays the newest 40 from that buffer, while the ZIP reads the full saved log.

## Delete saved results

**Saved results → Delete** permanently deletes the selected run's scenario, metadata, events, and live metric index after confirmation. Running/queued experiments, members of an active batch, and results being downloaded are protected. The deletion API is `DELETE /api/v1/results/{id}` with the login session. It does not stop Peers; previously scraped Prometheus/Grafana history remains. Deletion markers prevent late telemetry from recreating a deleted result; preserve them with Controller data in backups and migrations. See the [REST API guide](api.md) for status codes and download headers.

Explicit ZIP size requests (`HEAD`) and list inspections do not block deletion. Actual ZIP downloads (`GET`) protect their captured result until the request finishes. The delete request has a 30-second browser timeout; on timeout the Controller may still finish, so refresh or retry the same result. A slow follow-up list refresh does not keep the dialog controls disabled.

The series header's **Delete group** confirms the saved-run count and deletes the entire batch through `DELETE /api/v1/result-batches/{batchId}`. Unlike individual-source deletion, this also removes the separate `batch-analyses/{batchId}` mean files and cancels pending analyses. All members are checked for activity/downloads before deletion starts. A storage error can leave partial progress; refresh and retry after resolving the error. Existing Prometheus/Grafana history remains.

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

## Storage and backups

In Swarm with `KPL_STACK_NAME=kpl`, the `kpl_controller-data` volume on the control node is mounted at `/var/lib/kpl/data`. Run files are under `runs/<run-id>`. Ordinary service recreation and `swarm.sh remove` preserve the volume. There is no automatic raw-event retention limit or cross-node replication.

Preserve the entire Controller data directory, including hidden entries:

| Content | What it preserves |
| --- | --- |
| `runs/` | Scenarios submitted to runs, metadata, logs, observations, and per-run analysis |
| `scenarios/` | Reusable scenario library |
| `batch-analyses/` | Saved repetition means |
| `agent-capacities.json` | Per-Agent capacity overrides keyed by Agent ID |
| `.deleted-results/` | Deletion markers that prevent late telemetry recreating removed runs |
| `logs/` | Rotating access and authentication history |

A run ZIP preserves one experiment, not the entire Controller. Back up Prometheus and Grafana volumes separately for their history/settings; see [monitoring retention](monitoring.md#retention-and-operation). Restoring run files makes results available; it does not restore sessions, live counters, or execute unfinished runs automatically. Use [recovery](experiments.md) deliberately. See [deployment storage and restart](swarm.md#data-storage-and-restart) for volume location, write durability, and shutdown conditions.

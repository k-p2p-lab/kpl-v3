# Experiment result images

English | [Korean](visualization.kr.md)

[Documentation index](README.md) · [Repository](../README.md)

Choose **Saved results → Images** to submit background analysis. The server reads logs, calculates graph and propagation metrics, fits distributions, and saves the result. The browser renders white-background, 1,600px PNGs. **Graph images** starts with a collapsed list of chart titles; expand a title to see its image and **PNG ↓ / CSV ↓** links. **Download analysis JSON** downloads the full server artifact.

**Graph protocol** selects **GossipSub** (the default), **Kad**, or **Transport** for the graph metric images, keeping one title per metric. Each protocol has a separate image; switching within the same dialog preserves expanded titles. Common latency, propagation and bandwidth charts remain unchanged. **Download all PNG + CSV (ZIP)** includes all three protocols' separate PNGs, CSVs and chart definitions, regardless of the selected protocol or collapsed titles.

Accepted server jobs continue after closing the window/browser. At most 32 queued/running jobs are admitted, with one computation at a time. Completed artifacts survive Controller restart; interrupted jobs require Retry. Reopening a completed analysis reuses it. **Analyze latest snapshot** captures later logs. An outdated analysis version is regenerated on opening. Regeneration cannot recreate missing source evidence. Starting, retrying and refreshing jobs reuse the dashboard login session. PNG conversion and cross-run calculations happen in the browser while the view is open.

## Available images

- Existing v3 latency CDF/histogram, message activity, scores and peer lifecycle.
- Separate GossipSub, Kad and Transport graph metric images: node counts, degree, non-leaf denominator degree, diameter, shortest paths, clustering, five centralities, assortativity, modularity and connected-pair fraction.
- All-group GossipSub degree PDF/CDF and weighted Student-t fit; these retain their GossipSub input when Graph protocol changes.
- Propagation/duplicate cumulative curves, linear/log time, combined panels, hop distributions, per-message FRT/reachability/DRC and estimated eager/lazy and unclassified paths.
- Control RPCs, entries, message-ID references, mesh transitions and reciprocal logical edges.
- Total and per-protocol sent/received stream throughput and cumulative bytes. These measure libp2p stream use, not link capacity, IP/TCP framing, retransmissions or management traffic.

Open **Message images, repeated runs and v2 comparisons**, select a message, and choose **Message images** for its latency, hop, duplicate and first-reception path images. Missing evidence and undefined statistics appear as N/A; full node IDs and membership remain in analysis JSON.

## Batch mean for repetitions of one experiment

Submit **Run experiment → Runs** with two or more repetitions. **Saved results** groups the same `batchId` into one series, initially collapsed. Expand its header to access the individual **Run m of n** rows with their own **Images**, download and deletion controls. List refreshes preserve open groups; a new page load starts them collapsed again.

**Delete group** in the series header opens a confirmation with the saved-run count. It permanently removes every saved run in that batch, including failed/canceled runs and individual analyses, plus its batch mean analysis. The group can stay collapsed. Active batches and members being downloaded are protected; other groups with the same scenario name are unaffected.

After every member stops, choose **Analyze batch mean** in the series header, which remains accessible while the group is collapsed. The Controller selects completed runs with exactly the same `batchId`; an identical scenario name in another submission does not join the batch. At least two completed runs are required. Failed, canceled and interrupted runs are excluded, and missing/unreadable members are counted against the requested repetitions. The series header and completed view show inclusion counts. A completed run with zero observed receipts remains eligible.

The batch job processes each run's current saved logs separately, sharing the bounded worker queue with individual jobs. It never joins node/message identities across runs or replaces individual analysis artifacts. Close the window or browser and use the series header's analysis action again to reconnect or download the persisted result. **Analyze latest snapshot** regenerates the batch. A change in saved membership invalidates its current cache; a log-only change requires explicit refresh. Malformed logs fail the job rather than silently dropping a selected run. Runs with eligible main metrics using different measurement definitions also fail instead of averaging incompatible populations.

**Mean metrics and contributing run counts** lists each metric's arithmetic mean, between-run sample SD and valid run count. Every valid run has equal weight, regardless of its message/sample count. Undefined values are excluded, rather than replaced with zero. P95 is the mean of the individual run P95 values, not the percentile of pooled receipts. Total sent/received bytes and throughput use collected libp2p bandwidth evidence.

Overview images retain graph, score, activity, propagation, control and bandwidth views. Time series align on each run's start; line charts interpolate within observed segments and step charts hold within recorded intervals. Missing gaps and time beyond a series' last observation are excluded, so pointwise `n` can vary. Display grids contain at most 720 line/step points. CDFs average run distributions on a common axis; discrete distributions retain their whole support. Latency histograms use 30 common bins with uniform allocation within each source bin: counts are conserved, while within-bin locations are approximate. Fitted density curves are averaged without a pooled refit. Individual message trees remain in the run's **Images** view.

The PNG/CSV ZIP includes the averaged chart definitions, included/excluded run metadata and a `*-summary.csv` containing all summary means, sample SDs and counts. Chart CSV adds `n` for the contributing runs at each point. **Download analysis JSON** contains persisted means and compact per-run inputs that reproduce the overview; full message/node paths remain in individual analysis JSON. PNG conversion and curve averaging run in the browser after server computation completes.

## Comparing runs

1. **Load saved results**, select runs, and assign **Series** and **Case** labels. Identical labels define repeats; low-reachability runs are retained.
2. Choose Metric, X axis and Scatter color. Baseline series compares matching cases across groups. Reference case supplies arrow origins and fixed D parameters. Empty fields use the first series/case.
3. Enter parameters such as `i=1,f=0.5,p=90` or `dLow=4,d=8,dHigh=12` when needed. Case labels `i1-f0.5-p90` and `4-8-12` are also recognized. Missing parameters are never guessed; p is a percentage.
4. Select **Analyze selected results and generate comparison images**. Accepted analyses continue after closing; generate the comparison again to reuse completed artifacts.

Images cover repeat mean/sample-SD error bars; difference, `target/(target+baseline)` and division; arbitrary metric scatter; FRT–DRC with reach color and baseline arrows; two-/three-objective Pareto; weighted scores; i/f groups; lazy contribution estimates; all four v2 duplicate regression designs; univariate/multivariate quadratic Student-t parameter models; observed/fitted/predicted distributions; and raw/peak-normalized overlays. Enter `Dlow,D,Dhigh` rows under Degree predictions for model distributions.

One repeat has no sample SD. Error propagation assumes independent errors; same-input comparisons have zero uncertainty. Zero denominators are N/A. Regressions fit case means, report rank and in-sample R², and refuse unidentifiable coefficients. Score normalization depends on the selected set; a constant dimension contributes 0.5. Lazy estimates require a matched **lazy-off baseline** and are observational differences, not causal proof.

## v2 inputs and interpretation

Import v2 metric JSONL, propagation trees, duplicate-time maps, `x_case/y/yerr` or `x/y` JSON, numeric CSV, and v3 analysis JSON locally in the browser (32MiB/file). Select the appropriate Metric before importing v2 x/y summary data. Overlay files must share units. Peak normalization divides each curve and its supplied error bars by that curve's positive maximum; a curve with no positive maximum has no normalized values. It changes the original probability/count meaning. The fixed historical/ER reference button renders the data stored in v2's reference script, not selected-run measurements.

## Check definitions and collection evidence

Main Metrics and the additional `research` values use different populations, windows and aggregation even when their names match. Research reachability is conditioned on receipt observation times; raw messages can also have a research log-time estimate when timing evidence exists. See [saved-result research metrics](experiment-metrics.md#saved-result-research-metrics) for definitions, units, samples and graph normalization. Do not group different definitions as repeats under one Series/Case. Imported v2 values retain their original definitions and are not automatically converted.

Eager Push/Lazy Pull colors and contribution values are metadata estimates. Zero classified eager/lazy receipts can coexist with unknown receipts, so report the unclassified count. The [estimation rules](experiment-metrics.md#eager-push-and-lazy-pull-estimates) define message-ID/GRAFT/IHAVE/IWANT evidence and conflicts; [detailed Peer logs](api.md#detailed-peer-logs) define available fields and omission limits.

Reanalysis cannot recover message IDs, graph edges or session evidence absent from historical records. Deploy the current image to Controller/Agents and run new Peers to collect that version's logs, following [Swarm updates](swarm.md#failures-updates-and-shutdown). The current implementation uses upstream libp2p observations and needs no Python runtime or image-generation service.

## Analysis persistence and API

The [background analysis API](api.md#background-analysis) defines job state/version, source snapshot time and full/compact responses. The source boundary is captured after the job acquires its computation slot, not at queue admission. Completed work keeps that boundary; use **Analyze latest snapshot** to include later-arriving logs.

The server retains completed analysis JSON. The browser generates PNGs, CSVs and comparison ZIPs while the view is open. The **Download results** source ZIP, **Download analysis JSON**, and **Download all PNG + CSV (ZIP)** are different artifacts. See [analysis and image retention](results.md#analysis-and-image-retention) for contents and persistence.

[Experiment metrics](experiment-metrics.md) · [v2 coverage](v2-analysis-coverage.md) · [Bandwidth](bandwidth.md)

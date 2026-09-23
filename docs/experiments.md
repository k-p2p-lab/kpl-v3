# Run, Stop, and Recover Experiments

English | [Korean](experiments.kr.md)

[Documentation index](README.md) · [Repository](../README.md)

Use this guide after [Swarm deployment](swarm.md) and [Agent capacity setup](agents.md). A saved scenario is reusable input; a run is one execution; a batch groups repetitions; a retry is a new attempt at an unfinished iteration.

## Prepare and start

1. Open **Run experiment** in the [Dashboard](dashboard.md). Load a [saved scenario](scenario-library.md) or paste YAML; [scenario reference](scenario-reference.md) defines the fields.
2. Use **Validate** and inspect Agent availability. Validation checks the input, not runtime capacity or connectivity. [`examples/monitoring.yaml`](../examples/monitoring.yaml) is a small first experiment.
3. Set **Runs**, then choose **Run**. Follow **Experiment progress** and inspect the resulting records in [Saved results](results.md).

## Run the same scenario several times

In **Run experiment**, paste the YAML and set **Runs** beside **Run** to an integer from 1 to 100. The Controller queues all iterations and executes them sequentially, even if you close the browser. Each iteration gets a unique run ID, a separate result directory, and `batchId`, `iteration`, and `repetitions` in its metadata.

For more than one iteration, the Controller cancels or drains background jobs according to the scenario's exit policy, fences and removes that iteration's Peers, and refreshes Agent state before starting the next iteration. An execution or cleanup failure cancels all remaining iterations. **Stop batch** on any running or queued member cancels the active iteration and the remaining queue. Other independently submitted experiments can still run concurrently; keep them stopped when comparing repetitions.

The scenario YAML is unchanged in every iteration. An explicit nonzero `seed` is reused; zero or an omitted seed creates a new recorded seed per iteration. Reusing a seed repeats sampling inputs, but Docker timing, eligible populations, and network execution can still differ. Queues are not automatically resumed after a Controller restart. Retained `queued` or `running` records are displayed as `interrupted`.

For automation, see [run submission](api.md#submit-stop-and-observe-runs).

## Stop a run or batch

Use the active experiment’s stop control or **Stop batch** for a repetition group. Accepted cancellation displays **Stopping…** while jobs and Peers are cleaned up. Wait for the live state to settle; repeated clicks are not required. Stopping an active batch cancels its running and queued members. A Controller shutdown or crash has separate cleanup limits; see [failures and shutdown](swarm.md#failures-updates-and-shutdown).

## Recover saved work

After a restart, a saved run that still says `running` or `queued` is displayed as `interrupted`; its original metadata is preserved in the ZIP. This is a display status, not evidence that its Peers have stopped. Saved results do not automatically restore live experiment state, resume execution, or replay live counters. ZIP metrics are rebuilt from the retained log. An unreadable metadata file is displayed as `unreadable`; inspect the Controller logs and stored files before retrying.

If Run 4 of 10 failed after Runs 1–3 completed, choose the behavior deliberately:

| Action | What runs next | Previous attempts |
| --- | --- | --- |
| **Continue remaining** | Only never-started Runs 5–10 | Run 4 stays failed; no replacement to reach ten successes |
| **Retry from run 4** | Unfinished Runs 4–10, starting each at its first phase | Completed runs stay; retried iterations get new run IDs and retain earlier logs |

These are manual recovery operations, not phase checkpoints or automatic failover.

### Continue the remaining runs after a failure

In **Saved results**, use **Continue remaining (N)** on a stopped series to resume its never-started runs. Previously completed, failed, and otherwise attempted runs are preserved and are not repeated. The original batch ID, run IDs, iteration numbers, total repetitions, saved YAML, and per-run seeds are retained; for example, a failure at Run 2 of 5 continues with Runs 3–5. This does not add replacement runs to reach five successes. Batch means continue to include only completed runs.

Continuation also works from saved results after a Controller restart. It requires a failed run, or an interrupted run that had started, and retained unstarted members. The Controller retries cleanup of previous unsuccessful runs on its registered Agents before starting the remainder. If cleanup fails, no remaining run starts; its error is shown and **Continue remaining** can be retried. A later experiment failure stops the remainder again. Concurrent continuation requests, active batches, downloads, and running analysis jobs block admission. **Stop batch** also cancels a continuation during cleanup. Closing the browser does not cancel accepted work.

### Retry unfinished iterations

**Saved results → Retry from run N (count)** restarts saved unfinished iterations, including the failed/interrupted iteration itself. Completed runs keep their results. For a failed single experiment, use **Retry experiment**. The Controller first removes old Peers, then starts each selected iteration from its first phase with the original scenario, iteration number and seed. If cleanup fails, the new runs remain canceled and can be retried after resolving the cause. Old logs remain under **Previous attempts** and are excluded from batch progress and mean analysis. **Continue remaining** retains its earlier behavior: skip attempted runs and execute only the never-started remainder.

Both modes wait for prior Peer cleanup before new work starts. Resolve unreachable Agents or cleanup errors before retrying. Active execution, finalization, downloads, and active run/batch analysis can block recovery. See the [recovery API](api.md#submit-stop-and-observe-runs) for admission rules. Record which attempts enter a comparison following the [Hub reporting principles](https://github.com/k-p2p-lab/hub/blob/master/docs/RESEARCH.md).

## Progress and result order

**Experiment progress** shows running runs first, queued runs next, and terminal runs last. **Saved results** serves a different task: iterations within a batch stay in numeric **Run n of M** order, with previous attempts separate. See [result browsing](results.md) for filters, exports, and deletion.

## Estimated finish times

**Experiment progress** shows **Est. finish** and approximate remaining time for each running or queued Run. A summary above the cards shows **Group est. finish** for each active repeated series. Times use the browser's local time zone; remaining time is calculated by the Controller, including earlier Runs in the same group's queue. Separate groups execute independently.

The initial **Scenario estimate** combines waits, repeats and sampled intervals while accounting for overlapping background jobs, `wait-jobs`, `stop-all`, and `onExit`. Readiness barriers initially use their configured timeout allowance. Request counts use the configured maxima; actual eligible publishers/leavers may be fewer. Node startup, capacity waits, request processing and cleanup are not known in advance. As phases finish, their actual boundaries replace predictions. Completed successful Runs in the same active group supply average phase and total times, including cleanup, for later estimates; **Based on N completed runs** identifies this correction.

These are estimates, not deadlines. **Estimating…** means there is not enough timing information. If a Run exceeds its estimate, **Taking longer than estimated** appears and later queued Run/group estimates move forward. Stopping or finishing a Run removes its forecast. Estimates do not schedule an automatic stop and are not retained in saved results.

(function (root) {
  "use strict";

  const pending = run => run.active || ["queued", "running"].includes(run.state);
  const retryable = run => ["failed", "canceled", "interrupted"].includes(run.state);
  const currentRuns = runs => {
    const previous = new Set(runs.flatMap(run => run.previousRunIds || []));
    return runs.filter(run => !run.superseded && !previous.has(run.id));
  };

  function prepare(action, keys, results) {
    if (!["analysis", "retry"].includes(action)) throw new Error("Unknown saved result action.");
    if (!Array.isArray(keys) || !keys.length || keys.length > 2000) throw new Error("Select 1–2,000 saved result items.");
    const runs = new Map(), batches = new Map();
    for (const run of results || []) {
      if (!run || typeof run.id !== "string" || !run.id) continue;
      runs.set(run.id, run);
      if (run.batchId) {
        if (!batches.has(run.batchId)) batches.set(run.batchId, []);
        batches.get(run.batchId).push(run);
      }
    }
    const selected = [...new Set(keys)];
    for (const key of selected) {
      if (typeof key !== "string" || !(key.startsWith("run:") && runs.has(key.slice(4))
          || key.startsWith("batch:") && batches.has(key.slice(6)))) {
        throw new Error("Some selected results are no longer listed. Refresh saved results and select them again.");
      }
    }
    const coveredBatches = new Set(selected.filter(key => key.startsWith("batch:")).map(key => key.slice(6)));
    const units = selected.filter(key => !key.startsWith("run:") || !coveredBatches.has(runs.get(key.slice(4)).batchId));
    const entries = [], skipped = [], seen = new Set();
    const skip = (key, run, reason) => skipped.push({key, name: String(run?.name || run?.id || key), reason});
    for (const key of units) {
      const members = key.startsWith("batch:") ? batches.get(key.slice(6)) : [runs.get(key.slice(4))];
      if (action === "analysis") {
        for (const run of members) {
          if (seen.has(run.id)) continue;
          seen.add(run.id);
          if (run.state === "unreadable") {
            skip("run:" + run.id, run, "Saved metadata is unreadable.");
            continue;
          }
          entries.push({key: "run:" + run.id, id: run.id, name: String(run.name || run.id),
            url: "/api/v1/analysis-jobs/" + encodeURIComponent(run.id), runIds: [run.id], runCount: 1,
            previousAnalysisId: run.analysis?.id || "", previousAnalysisState: run.analysis?.state || "idle"});
        }
        continue;
      }
      const selectedRun = key.startsWith("run:") ? members[0] : null;
      const batchID = key.startsWith("batch:") ? key.slice(6) : selectedRun.batchId;
      if (!batchID) { skip(key, selectedRun, "This result has no retryable batch identity."); continue; }
      if (seen.has(batchID)) continue;
      const all = batches.get(batchID) || [], current = currentRuns(all);
      if (selectedRun && (!current.some(run => run.id === selectedRun.id)
          || !retryable(selectedRun))) {
        skip(key, selectedRun, "Only current failed, canceled or interrupted attempts can request a retry.");
        continue;
      }
      seen.add(batchID);
      const sample = current[0] || members[0];
      if (all.some(pending)) { skip(key, sample, "This batch is still queued or running."); continue; }
      const expected = Math.max(1, ...current.map(run => run.repetitions || 1));
      if (current.some(run => run.state === "unreadable" || !Number.isInteger(run.iteration)
          || run.iteration < 1 || run.iteration > expected) || new Set(current.map(run => run.iteration)).size !== current.length) {
        skip(key, sample, "Batch metadata is incomplete or unreadable."); continue;
      }
      if (!current.some(retryable)) {
        skip(key, sample, "This batch has no failed, canceled or interrupted attempt."); continue;
      }
      const unfinished = current.filter(retryable)
        .sort((a, b) => a.iteration - b.iteration);
      entries.push({key: "batch:" + batchID, id: batchID, name: String(sample?.name || batchID),
        url: "/api/v1/result-batches/" + encodeURIComponent(batchID) + "/retry",
        runIds: unfinished.map(run => run.id), runCount: unfinished.length,
        knownRunIds: [...new Set(all.flatMap(run => [run.id, ...(run.previousRunIds || [])]))]});
    }
    if (entries.length > 2000) throw new Error("Submit up to 2,000 runs or batches at a time. Select fewer results.");
    return Object.freeze({action, entries: Object.freeze(entries.map(entry => Object.freeze({...entry,
      runIds: Object.freeze([...entry.runIds]), ...(entry.knownRunIds ? {knownRunIds: Object.freeze([...entry.knownRunIds])} : {})}))),
      skipped: Object.freeze(skipped.map(entry => Object.freeze(entry)))});
  }

  function createAdmissions({api, timeoutMs = 30000}) {
    let active = false;
    const receipts = new WeakMap();
    async function execute(plan, {onProgress = () => {}, onAccepted = () => {}, signal} = {}) {
      if (active) throw new Error("A bulk submission is already in progress.");
      const saved = receipts.get(plan) || new Map();
      receipts.set(plan, saved);
      if ([...saved.values()].some(item => item.kind === "unknown")) throw new Error("Outcome unknown. Inspect the execution queue before creating a new selection.");
      active = true;
      const report = {accepted: [], skipped: [...plan.skipped], failed: [], unknown: [], remaining: []};
      for (const receipt of saved.values()) {
        if (receipt.kind === "accepted") report.accepted.push(receipt.value);
        if (receipt.kind === "skipped") report.skipped.push(receipt.value);
      }
      try {
        for (let index = 0; index < plan.entries.length; index++) {
          const entry = plan.entries[index];
          if (saved.has(entry.key)) continue;
          if (signal?.aborted) {
            report.remaining = plan.entries.slice(index).filter(next => !saved.has(next.key));
            break;
          }
          try { onProgress({index, total: plan.entries.length, name: entry.name}); } catch {}
          if (signal?.aborted) {
            report.remaining = plan.entries.slice(index).filter(next => !saved.has(next.key));
            break;
          }
          const controller = new AbortController();
          const abort = () => controller.abort();
          signal?.addEventListener("abort", abort, {once: true});
          const timer = setTimeout(() => controller.abort(), timeoutMs);
          try {
            const response = await api(entry.url, {method: "POST", signal: controller.signal,
              headers: {"Content-Type": "application/json"}, body: JSON.stringify(plan.action === "retry"
                ? {expectedRunIds: entry.runIds}
                : {expectedAnalysis: {id: entry.previousAnalysisId, state: entry.previousAnalysisState}})});
            if (!response || typeof response.id !== "string" || !response.id
                || (plan.action === "analysis" ? response.runId !== entry.id
                  || !["queued", "running", "completed"].includes(response.state)
                  : response.batchId !== entry.id || !["queued", "running"].includes(response.state) || entry.knownRunIds.includes(response.id))) {
              throw new Error("Unexpected submission response. The server may have accepted this work.");
            }
            const reused = plan.action === "analysis" && (response.reused === true || response.state === "completed"
              || Boolean(entry.previousAnalysisId && response.id === entry.previousAnalysisId));
            const value = {entry, response, reused};
            saved.set(entry.key, {kind: "accepted", value});
            report.accepted.push(value);
            try { onAccepted(value); } catch {}
          } catch (error) {
            const queueFull = plan.action === "analysis" && error?.status === 503 && /^analysis queue is full(?:;|$)/i.test(error.message || "");
            const ambiguous = ["batch-attempt-changed", "analysis-attempt-changed"].includes(error?.code) || error?.accepted === true || !Number.isInteger(error?.status) || error.status === 408
              || error.status >= 500 && error.accepted !== false && !queueFull;
            const message = ambiguous ? "Outcome unknown. Inspect the execution queue and refresh results before retrying. " + (error.message || "") : error.message || "Submission failed.";
            const value = {entry, message};
            if (ambiguous) {
              report.unknown.push(value);
              saved.set(entry.key, {kind: "unknown", value});
            } else if ([404, 409].includes(error.status)) {
              const skipped = {key: entry.key, name: entry.name, reason: message};
              report.skipped.push(skipped);
              saved.set(entry.key, {kind: "skipped", value: skipped});
              continue;
            } else report.failed.push(value);
            report.remaining = plan.entries.slice(index + 1).filter(next => !saved.has(next.key));
            break;
          } finally { clearTimeout(timer); signal?.removeEventListener("abort", abort); }
        }
        return report;
      } finally { active = false; }
    }
    return {execute};
  }

  const exported = {prepare, createAdmissions};
  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  else root.KPLResultBulkActions = exported;
})(globalThis);

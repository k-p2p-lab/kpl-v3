(function (root) {
  "use strict";

  function prepareDeletion(scope, keys, items, results = []) {
    if (!["scenarios", "results"].includes(scope) || !Array.isArray(keys)) throw new Error("Invalid deletion selection.");
    const unique = [...new Set(keys)];
    if (!unique.length || unique.length > 2000) throw new Error("Select 1–2,000 items to delete.");
    const known = new Map((items || []).map(item => [item.key, item]));
    const batches = new Map();
    for (const run of results) {
      if (!run.batchId) continue;
      if (!batches.has(run.batchId)) batches.set(run.batchId, []);
      batches.get(run.batchId).push(run.id);
    }
    const entries = unique.map(key => {
      const item = known.get(key);
      const prefix = typeof key === "string" ? key.split(":", 1)[0] : "";
      if (!item || !(scope === "scenarios" ? prefix === "scenario" : ["batch", "run"].includes(prefix))) {
        throw new Error("Some selected items are no longer listed. Refresh the list and select them again.");
      }
      const id = key.slice(prefix.length + 1);
      if (!id) throw new Error("Invalid deletion selection.");
      if (prefix === "run" && results.some(run => run.id === id && (run.missing || run.state === "missing"))) {
        throw new Error("This result data is already missing. Delete its batch to remove the saved retry inputs.");
      }
      const endpoint = prefix === "scenario" ? "scenarios" : prefix === "batch" ? "result-batches" : "results";
      const runIDs = prefix === "batch" ? batches.get(id) || [] : prefix === "run" ? [id] : [];
      return Object.freeze({key, id, name: String(item.name || id), kind: prefix,
        url: "/api/v1/" + endpoint + "/" + encodeURIComponent(id), runIDs: Object.freeze([...new Set(runIDs)])});
    });
    return Object.freeze({scope, entries: Object.freeze(entries)});
  }

  function prepareFailedDeletion(keys, items, results = []) {
    const runs = new Map((results || []).filter(run => run && typeof run.id === "string" && run.id)
      .map(run => [run.id, run]));
    // Expanded batch members have per-Run actions even though the library
    // checkboxes identify their batch as one selectable unit.
    const known = [...(items || []), ...[...runs.values()].map(run => ({key: "run:" + run.id, name: run.name || run.id}))];
    const selected = prepareDeletion("results", keys, known, results);
    const seen = new Set(), entries = [];
    for (const item of selected.entries) {
      for (const id of item.runIDs) {
        const run = runs.get(id);
        if (seen.has(id) || run?.state !== "failed") continue;
        seen.add(id);
        if (entries.length === 2000) throw new Error("Delete up to 2,000 failed runs at a time. Select fewer results.");
        entries.push(Object.freeze({key: "run:" + id, id, name: String(run.name || item.name || id), kind: "run",
          url: "/api/v1/results/" + encodeURIComponent(id) + "?expectedState=failed", expectedState: "failed",
          runIDs: Object.freeze([id])}));
      }
    }
    return Object.freeze({scope: "results", mode: "failed", entries: Object.freeze(entries)});
  }

  function createDeletions({api, timeoutMs = 30000}) {
    let active = false;
    async function execute(plan, {onDeleted = () => {}, onProgress = () => {}} = {}) {
      if (active) throw new Error("A deletion is already in progress.");
      active = true;
      const report = {deleted: [], deletedItems: [], failed: [], remaining: []};
      try {
        for (let index = 0; index < plan.entries.length; index++) {
          const entry = plan.entries[index];
          onProgress({index, total: plan.entries.length, name: entry.name});
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), timeoutMs);
          let runIDs = entry.runIDs;
          try {
            let response, missing = false;
            try { response = await api(entry.url, {method: "DELETE", signal: controller.signal}); }
            catch (error) { if (error.status === 404) missing = true; else throw error; }
            if (entry.kind === "batch" && !missing) {
              if (!Array.isArray(response?.deletedIds) || response.deletedIds.some(id => typeof id !== "string" || !id)) {
                throw new Error("Unexpected batch deletion response. Refresh the list before retrying.");
              }
              runIDs = [...new Set(response.deletedIds)];
            }
          } catch (error) {
            const message = error.name === "AbortError"
              ? "Deletion timed out and may still finish. Refresh or retry to confirm."
              : error.status === 409 ? plan.mode === "failed" || entry.expectedState === "failed"
                ? "This run is no longer failed or is busy. Refresh results before retrying."
                : "This item or its batch is busy. Wait for its current work to finish."
              : error.message;
            report.failed.push({entry, message});
            if (error.status !== 409) {
              report.remaining = plan.entries.slice(index + 1);
              break;
            }
            continue;
          } finally { clearTimeout(timeout); }
          report.deleted.push(entry.key);
          report.deletedItems.push({entry, runIDs});
          // The server already confirmed deletion. A rendering failure must
          // not reclassify it as a failed operation or trigger a second delete.
          try { onDeleted(entry, runIDs); } catch {}
        }
        return report;
      } finally { active = false; }
    }
    return {execute};
  }

  const exported = {prepareDeletion, prepareFailedDeletion, createDeletions};
  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  else root.KPLLibraryDeletions = exported;
})(typeof globalThis !== "undefined" ? globalThis : this);

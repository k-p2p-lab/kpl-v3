(function (root) {
  "use strict";
  function createDialog({document, api, onBusy = () => {}, onAdded = () => {}, onError = () => {}}) {
    const $ = selector => document.querySelector(selector);
    const dialog = $("#appendBatchDialog"), input = $("#appendBatchCount"), submit = $("#confirmAppendBatch"), cancel = $("#cancelAppendBatch");
    let batch = null, busy = false, stale = false;
    function render() {
      const count = Number(input.value), maximum = batch ? 100 - batch.expected : 0;
      const valid = Number.isInteger(count) && count >= 1 && count <= maximum;
      input.disabled = busy;
      submit.disabled = busy || stale || !valid;
      submit.textContent = busy ? "Adding runs…" : "Add and start";
      cancel.disabled = busy;
      $("#appendBatchSummary").textContent = !batch ? "" : valid
        ? `${batch.expected} completed + ${count} additional = ${batch.expected + count} total runs. The next run will be Run ${batch.expected + 1} of ${batch.expected + count}.`
        : `Enter a whole number from 1 to ${maximum}. A group can contain up to 100 runs.`;
    }
    function open(value) {
      if (!value || dialog.open || value.expected >= 100) return;
      batch = {id: value.id, name: value.name, expected: value.expected};
      busy = false; stale = false;
      input.value = String(Math.min(batch.expected, 100 - batch.expected));
      input.max = String(100 - batch.expected);
      $("#appendBatchName").textContent = batch.name || batch.id;
      $("#appendBatchID").textContent = batch.id;
      $("#appendBatchError").textContent = "";
      render(); dialog.showModal(); input.focus(); input.select();
    }
    async function save() {
      if (!batch || submit.disabled) return;
      const selected = batch, count = Number(input.value);
      const controller = new AbortController(), timer = setTimeout(() => controller.abort(), 30000);
      busy = true; $("#appendBatchError").textContent = ""; render(); onBusy(selected.id, true);
      try {
        const run = await api(`/api/v1/result-batches/${encodeURIComponent(selected.id)}/append`, {
          method: "POST", signal: controller.signal, headers: {"Content-Type": "application/json"},
          body: JSON.stringify({additionalRuns: count, expectedRepetitions: selected.expected}),
        });
        if (!run?.id || run.batchId !== selected.id || run.iteration !== selected.expected + 1 || run.repetitions !== selected.expected + count) {
          throw Object.assign(new Error("Unexpected response. Refresh results before adding more runs."), {status: 409});
        }
        busy = false; dialog.close(); onAdded(run, count);
      } catch (error) {
        stale = error.status === 409 || error.name === "AbortError";
        $("#appendBatchError").textContent = error.name === "AbortError"
          ? "The request timed out and may have completed. Close this window and refresh results before adding more runs."
          : `${error.message}${stale ? " Close this window and reopen Add runs after refreshing results." : ""}`;
        onError(error);
      } finally {
        clearTimeout(timer); busy = false; onBusy(selected.id, false); render();
      }
    }
    input.addEventListener("input", render);
    $("#appendBatchForm").addEventListener("submit", event => { event.preventDefault(); void save(); });
    cancel.addEventListener("click", () => { if (!busy) dialog.close(); });
    dialog.addEventListener("cancel", event => { if (busy) event.preventDefault(); });
    dialog.addEventListener("close", () => { batch = null; });
    return {open, save};
  }
  let ui;
  const exported = {createDialog, init: options => { ui = createDialog({document: root.document, ...options}); }, open: batch => ui?.open(batch)};
  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  else root.KPLBatchAppend = exported;
})(typeof globalThis !== "undefined" ? globalThis : this);

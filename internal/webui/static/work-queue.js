(function (root) {
  "use strict";
  const escape = value => String(value ?? "").replace(/[&<>"']/g, char => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[char]);
  const kinds = {experiment:"Experiment", analysis:"Run analysis", "batch-analysis":"Batch mean analysis"};

  function phaseLabel(item) {
    const labels = {"waiting-for-cleanup":"Waiting for Peer cleanup", "checking-sources":"Checking sources", starting:"Preparing analysis", reading:"Reading results", aggregating:"Calculating", saving:"Saving analysis"};
    if (item.state === "queued") return item.position > 0 ? item.position + " task" + (item.position === 1 ? "" : "s") + " ahead" : "Waiting to start";
    return labels[item.phase] || item.phase || (item.kind === "experiment" ? "Running experiment" : "Analyzing results");
  }

  function queueMarkup(items, pending = new Set()) {
    return items.map((item, index) => {
      const canceling = pending.has(item.id), analysis = item.kind !== "experiment";
      const progress = analysis && item.state === "running" && Number.isFinite(item.progress) ? Math.min(100, Math.max(0, item.progress)) : null;
      return '<li class="work-queue-item" data-work-item="' + escape(item.id) + '"><span class="work-queue-order" aria-label="Queue position ' + (index + 1) + '">' + (index + 1) + '</span><div class="work-queue-copy"><div class="work-queue-item-heading"><span class="work-queue-kind">' + kinds[item.kind] + '</span><span class="status-pill ' + item.state + '">' + (canceling ? "canceling" : item.state) + '</span></div><strong title="' + escape(item.name) + '">' + escape(item.name || item.runId || item.batchId || item.id) + '</strong><span class="work-queue-phase">' + escape(canceling ? "Cancellation requested · waiting for cleanup" : phaseLabel(item)) + (progress == null ? "" : ' · ' + Math.floor(progress) + '%') + '</span></div><div class="work-queue-actions">' + (analysis ? '<button class="secondary-button" type="button" data-work-open="' + escape(item.id) + '" aria-label="Open analysis for ' + escape(item.name) + '">View</button>' : '') + '<button class="secondary-button" type="button" data-work-cancel="' + escape(item.id) + '" aria-label="Cancel ' + escape(kinds[item.kind].toLowerCase() + ': ' + item.name) + '"' + (canceling ? ' disabled' : '') + '>Cancel</button></div></li>';
    }).join("");
  }

  function createWorkQueue(environment = root) {
    const document = environment.document;
    const timer = environment.setTimeout || setTimeout, clearTimer = environment.clearTimeout || clearTimeout;
    let options, initialized = false, items = [], lastMarkup = "";
    const pending = new Set();

    function render() {
      const section = document?.querySelector("#workQueue"), list = document?.querySelector("#workQueueList");
      if (!section || !list) return;
      section.hidden = items.length === 0;
      const count = document.querySelector("#workQueueCount");
      if (count) count.textContent = String(items.length);
      const html = queueMarkup(items, pending);
      if (html === lastMarkup) return;
      const active = document.activeElement;
      const focusID = list.contains?.(active) ? active?.dataset?.workCancel || active?.dataset?.workOpen : null;
      const focusAction = active?.dataset?.workCancel ? "workCancel" : "workOpen";
      list.innerHTML = html;
      lastMarkup = html;
      if (focusID) {
        const target = [...(list.querySelectorAll?.("[data-work-cancel], [data-work-open]") || [])]
          .find(element => element.dataset[focusAction] === focusID && !element.disabled);
        (target || (items.length ? list : document.querySelector("#refreshExperiments")))?.focus?.({preventScroll:true});
      }
    }

    function update(value) {
      const unique = new Map();
      for (const item of Array.isArray(value) ? value : []) {
        if (typeof item?.id !== "string" || !item.id || !Object.hasOwn(kinds, item.kind) || !["queued","running"].includes(item.state)) continue;
        if (!unique.has(item.id)) unique.set(item.id, {...item});
      }
      items = [...unique.values()];
      for (const id of pending) if (!unique.has(id)) pending.delete(id);
      render();
    }

    function notice(message, error = false) {
      const element = document?.querySelector("#workQueueStatus");
      if (!element) return;
      element.textContent = message;
      element.hidden = !message;
      element.setAttribute("role", error ? "alert" : "status");
    }

    async function cancel(id) {
      if (pending.has(id) || !items.some(item => item.id === id)) return;
      pending.add(id);
      notice("");
      render();
      const controller = new AbortController();
      const timeout = timer(() => controller.abort(), 30000);
      try {
        await options.api("/api/v1/work-queue/" + encodeURIComponent(id) + "/cancel", {method:"POST", signal:controller.signal});
        notice("Cancellation requested. The next task waits for cleanup to finish.");
      } catch (error) {
        pending.delete(id);
        notice(error.status === 404 ? "That task has already finished."
          : error.name === "AbortError" ? "The cancellation request timed out. Check the live queue before retrying."
          : error.message || "Could not cancel the task.", error.status !== 404);
        render();
      } finally { clearTimer(timeout); }
    }

    function init(configuration) {
      options = configuration;
      if (initialized) return;
      initialized = true;
      document?.addEventListener("click", event => {
        const control = event.target.closest?.("[data-work-cancel], [data-work-open]");
        if (!control || control.disabled) return;
        const item = items.find(item => item.id === (control.dataset.workCancel || control.dataset.workOpen));
        if (!item) return;
        event.preventDefault();
        if (control.dataset.workCancel) void cancel(item.id);
        else options.onOpen?.(item);
      });
    }
    return {init, update};
  }

  if (typeof module !== "undefined" && module.exports) module.exports = {createWorkQueue, queueMarkup, phaseLabel};
  else root.KPLWorkQueue = createWorkQueue();
})(typeof globalThis !== "undefined" ? globalThis : this);

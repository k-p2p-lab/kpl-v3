(function (root) {
  "use strict";
  const escape = value => String(value ?? "").replace(/[&<>"']/g, char => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[char]);
  const kinds = {experiment:"Experiment", analysis:"Run analysis", "batch-analysis":"Batch mean analysis", "host-reboot":"Host reboot"};

  function phaseLabel(item) {
    const labels = {"waiting-for-cleanup":"Waiting for Peer cleanup", "checking-sources":"Checking sources", starting:"Preparing analysis", reading:"Reading results", aggregating:"Calculating", saving:"Saving analysis", "checking-hosts":"Checking hosts", "requesting-reboot":"Requesting host reboot", "waiting-for-hosts":"Waiting for hosts to return"};
    if (item.state === "queued") return item.position > 0 ? item.position + " task" + (item.position === 1 ? "" : "s") + " ahead" : "Waiting to start";
    return labels[item.phase] || item.phase || (item.kind === "experiment" ? "Running experiment" : item.kind === "host-reboot" ? "Preparing host reboot" : "Analyzing results");
  }

  function queueMarkup(items, pending = new Set(), experimentDetails) {
    return items.map((item, index) => {
      const canceling = pending.has(item.id), analysis = ["analysis","batch-analysis"].includes(item.kind), viewable = item.kind !== "experiment";
      const progress = analysis && item.state === "running" && Number.isFinite(item.progress) ? Math.min(100, Math.max(0, item.progress)) : null;
      const details = item.kind === "experiment" ? experimentDetails?.(item, canceling) || {} : {};
      const cancelLabel = canceling ? "Stopping…" : item.kind === "experiment" && item.state === "running" ? (details.repeated ? "Stop batch" : "Stop") : "Cancel";
      return '<li class="work-queue-item" data-work-item="' + escape(item.id) + '"><header class="work-queue-card-heading"><span class="work-queue-order" aria-label="Queue position ' + (index + 1) + '">' + (index + 1) + '</span><div class="work-queue-copy"><div class="work-queue-item-heading"><span class="work-queue-kind">' + kinds[item.kind] + '</span><span class="status-pill ' + item.state + '">' + (canceling ? "canceling" : item.state) + '</span></div><strong title="' + escape(item.name) + '">' + escape(item.name || item.runId || item.batchId || item.id) + '</strong></div></header><div class="work-queue-body" data-work-details="' + escape(item.id) + '" tabindex="0" aria-label="Task details: ' + escape(item.name || item.id) + '"><span class="work-queue-phase">' + escape(canceling ? (item.kind === "host-reboot" ? "Stopping unsent requests and waiting" : "Cancellation requested · waiting for cleanup") : phaseLabel(item)) + (progress == null ? "" : ' · ' + Math.floor(progress) + '%') + '</span>' + (details.bodyHTML || '') + '</div><footer class="work-queue-actions">' + (details.actionsHTML || '') + (viewable ? '<button class="secondary-button" type="button" data-work-open="' + escape(item.id) + '" aria-label="Open ' + (item.kind === "host-reboot" ? 'host reboot status for ' : 'analysis for ') + escape(item.name) + '">View</button>' : '') + '<button class="secondary-button" type="button" data-work-cancel="' + escape(item.id) + '" aria-label="Cancel ' + escape(kinds[item.kind].toLowerCase() + ': ' + item.name) + '"' + (canceling || details.stopping ? ' disabled' : '') + '>' + (details.stopping ? "Stopping…" : cancelLabel) + '</button></footer></li>';

    }).join("");
  }

  function createWorkQueue(environment = root) {
    const document = environment.document;
    const timer = environment.setTimeout || setTimeout, clearTimer = environment.clearTimeout || clearTimeout;
    let options, initialized = false, items = [], lastMarkup = "", experimentDetails;
    const pending = new Set();

    function render() {
      const section = document?.querySelector("#workQueue"), list = document?.querySelector("#workQueueList");
      if (!section || !list) return;
      section.hidden = false;
      const empty = document.querySelector("#workQueueEmpty");
      if (empty) empty.hidden = items.length !== 0;
      const count = document.querySelector("#workQueueCount");
      if (count) count.textContent = String(items.length);
      const html = queueMarkup(items, pending, experimentDetails);
      updateNavigation();
      if (html === lastMarkup) return;
      const active = document.activeElement;
      const focusAction = active?.dataset?.workCancel ? "workCancel" : active?.dataset?.workOpen ? "workOpen" : active?.dataset?.resultDownload ? "resultDownload" : "workDetails";
      const focusedCard = active?.closest?.("[data-work-item]");
      const focusID = list.contains?.(active) ? active?.dataset?.[focusAction] || focusedCard?.dataset?.workItem : null;
      const scrollLeft = list.scrollLeft || 0;
      const bodyScroll = new Map([...(list.querySelectorAll?.("[data-work-item]") || [])].map(card => [card.dataset.workItem, card.querySelector?.(".work-queue-body")?.scrollTop || 0]));
      list.innerHTML = html;
      list.scrollLeft = scrollLeft;
      for (const card of list.querySelectorAll?.("[data-work-item]") || []) {
        const body = card.querySelector?.(".work-queue-body");
        if (body) body.scrollTop = bodyScroll.get(card.dataset.workItem) || 0;
      }
      lastMarkup = html;
      if (focusID) {
        const target = [...(list.querySelectorAll?.("[data-work-cancel], [data-work-open], [data-result-download], [data-work-details]") || [])]
          .find(element => element.dataset[focusAction] === focusID && !element.disabled);
        const fallback = [...(list.querySelectorAll?.("[data-work-item]") || [])].find(card => card.dataset.workItem === focusID)?.querySelector?.(".work-queue-body");
        (target || fallback || (items.length ? list : document.querySelector("#refreshExperiments")))?.focus?.({preventScroll:true});
      }
      updateNavigation();
    }

    function updateNavigation() {
      const list = document?.querySelector("#workQueueList");
      if (!list) return;
      const previous = document.querySelector("#workQueuePrevious"), next = document.querySelector("#workQueueNext");
      if (previous) previous.disabled = !items.length || (list.scrollLeft || 0) <= 1;
      if (next) next.disabled = !items.length || (list.scrollLeft || 0) + (list.clientWidth || 0) >= (list.scrollWidth || 0) - 1;
    }

    function slide(direction) {
      const list = document?.querySelector("#workQueueList");
      if (!list) return;
      list.scrollBy?.({left:direction * Math.max(1, (list.clientWidth || 0) * 0.85), behavior:"smooth"});
    }

    function update(value, details) {
      experimentDetails = details;
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
      const hostReboot = items.find(item => item.id === id)?.kind === "host-reboot";
      pending.add(id);
      notice("");
      render();
      const controller = new AbortController();
      const timeout = timer(() => controller.abort(), 30000);
      try {
        await options.api("/api/v1/work-queue/" + encodeURIComponent(id) + "/cancel", {method:"POST", signal:controller.signal});
        notice(hostReboot ? "Cancellation requested. Reboots already requested on a host cannot be canceled."
          : "Cancellation requested. The next task waits for cleanup to finish.");
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
      const list = document?.querySelector("#workQueueList");
      list?.addEventListener?.("scroll", updateNavigation, {passive:true});
      list?.addEventListener?.("keydown", event => {
        if (event.target !== list || !["ArrowLeft", "ArrowRight"].includes(event.key)) return;
        event.preventDefault();
        slide(event.key === "ArrowLeft" ? -1 : 1);
      });
      environment.addEventListener?.("resize", updateNavigation);
      if (environment.ResizeObserver && list) new environment.ResizeObserver(updateNavigation).observe(list);
      document?.querySelector("#workQueuePrevious")?.addEventListener?.("click", () => slide(-1));
      document?.querySelector("#workQueueNext")?.addEventListener?.("click", () => slide(1));
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

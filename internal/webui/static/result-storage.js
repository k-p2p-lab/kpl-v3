(function (root) {
  "use strict";
  function bytes(value) {
    if (!Number.isFinite(value) || value < 0) return "unknown";
    if (value < 1024) return `${value} B`;
    const unit = Math.min(4, Math.floor(Math.log(value) / Math.log(1024)));
    return `${(value / 1024 ** unit).toLocaleString("en-US", {maximumFractionDigits: 1})} ${["B", "KiB", "MiB", "GiB", "TiB"][unit]}`;
  }
  function reconnectFailureIsCurrent(status) {
    const checked = status.lastCheckedAt && !status.lastCheckedAt.startsWith("0001-");
    return status.reconnectState === "failed" && (!!status.error || !checked && !status.checking);
  }
  function storageErrorDetail(status) {
    if (["queued", "running"].includes(status.reconnectState)) return "";
    let detail = status.error || "";
    // Preserve request history during the next check, but retire it once a
    // completed ordinary check confirms that the archive has recovered.
    if (status.reconnectState === "failed" && (reconnectFailureIsCurrent(status) || status.checking) && status.reconnectError) {
      detail = `Last reconnect attempt failed: ${status.reconnectError}${detail && detail !== status.reconnectError ? ` Current status: ${detail}` : ""}`;
    }
    return String(detail).replace(/\s+/g, " ").trim();
  }
  function describe(status) {
    const localUnknown = status.localSpaceKnown === false || status.localSpaceKnown === undefined && status.errorKind === "local";
    const low = !localUnknown && status.minFreeBytes > 0 && status.availableBytes < status.minFreeBytes;
    // The server measures time since worker progress. A long transfer, a pause,
    // or a different clock on a mobile device must not imply a stalled NAS.
    const stalled = status.stalled === true && status.phase !== "paused";
    const phases = {
      preparing: "Preparing results for archiving…",
      discovering: "Discovering saved archives…",
      copying: "Copying results to archive…",
      verifying: "Verifying archived results…",
      publishing: "Finalizing archived results…",
      deleting: "Removing deleted archives…",
      paused: "Archiving paused while experiments are running.",
    };
    const reconnecting = ["queued", "running"].includes(status.reconnectState);
    const reconnectFailed = reconnectFailureIsCurrent(status);
    const errorMessages = {
      unavailable: "Archive unavailable; results remain local.",
      maintenance: "Some archive work is deferred; local results are retained.",
      local: localUnknown ? "Local result storage could not be inspected." : "Local result storage needs attention.",
    };
    const archive = reconnecting ? status.phase === "paused" ? "Archive reconnection is waiting for running experiments." : status.reconnectState === "queued" ? "Archive reconnection queued." : "Checking archived data before reconnecting…"
      : status.error ? errorMessages[status.errorKind] || "Archiving needs attention; results remain local."
      : reconnectFailed ? "Archive reconnection failed; local results are retained."
      : stalled ? "Archive progress is delayed; local recording remains available."
      : status.phase === "paused" ? phases.paused
      : status.checking ? phases[status.phase] || "Archive work in progress…"
      : !status.lastCheckedAt || status.lastCheckedAt.startsWith("0001-") ? "Discovering saved archives…" : "Archive connected.";
    const detail = storageErrorDetail(status);
    const shortDetail = detail.length > 240 ? `${detail.slice(0, 239)}…` : detail;
    return {text: `Local result space: ${bytes(localUnknown ? undefined : status.availableBytes)} free. ${low ? "Waiting for space before starting the next run. " : ""}${archive}${shortDetail ? ` ${shortDetail}` : ""}`, warning: low || !reconnecting && (!!status.error || reconnectFailed) || stalled};
  }
  function init({api, onStatus}) {
    const element = root.document.querySelector("#resultStorageStatus");
    if (!element) return;
    const reconnect = root.document.querySelector("#reconnectArchive");
    const prompt = root.document.querySelector("#archiveReconnectPrompt");
    const confirm = root.document.querySelector("#confirmArchiveReconnect");
    const cancel = root.document.querySelector("#cancelArchiveReconnect");
    let timer, controller, stopped = false, lastStatus, reconnectBusy = false, revision = 0;
    function closePrompt() {
      if (prompt) prompt.hidden = true;
      reconnect?.setAttribute("aria-expanded", "false");
    }
    function render(status) {
      const message = describe(status);
      element.textContent = message.text;
      element.dataset.warning = String(message.warning);
      const failed = reconnectFailureIsCurrent(status);
      element.title = storageErrorDetail(status) || (status.minFreeBytes ? `Minimum local free space before starting a run: ${bytes(status.minFreeBytes)}.` : "Local free-space admission guard is disabled.");
      const pending = reconnectBusy || ["queued", "running"].includes(status.reconnectState);
      if (reconnect) {
        reconnect.hidden = !(status.errorKind === "unavailable" && status.error || failed || pending);
        reconnect.disabled = pending;
        reconnect.textContent = pending ? "Reconnecting…" : "Reconnect archive";
        if (pending || reconnect.hidden) closePrompt();
      }
      if (confirm) confirm.disabled = pending;
    }
    async function refresh() {
      if (stopped || controller || reconnectBusy || root.document.hidden) return;
      controller = new AbortController();
      const request = controller;
      const requestRevision = revision;
      const timeout = setTimeout(() => request.abort(), 8000);
      try {
        const status = await api("/api/v1/result-storage", {signal: request.signal});
        if (requestRevision !== revision || stopped || root.document.hidden) return;
        lastStatus = status;
        render(status);
        if (!stopped && !root.document.hidden) onStatus?.(status);
      } catch (error) {
        if (!stopped && requestRevision === revision && error.status !== 401) {
          element.textContent = "Storage status is temporarily unavailable.";
          element.dataset.warning = "true";
        }
      } finally {
        clearTimeout(timeout); controller = null;
        if (!stopped && !reconnectBusy && !root.document.hidden) {
          clearTimeout(timer);
          if (requestRevision !== revision) void refresh();
          else timer = setTimeout(refresh, 15000);
        }
      }
    }
    reconnect?.addEventListener("click", () => {
      if (reconnect.disabled || reconnect.hidden || !prompt) return;
      prompt.hidden = false;
      reconnect.setAttribute("aria-expanded", "true");
      cancel?.focus();
    });
    cancel?.addEventListener("click", () => { closePrompt(); reconnect?.focus(); });
    confirm?.addEventListener("click", async () => {
      if (!lastStatus || reconnectBusy || prompt?.hidden || ["queued", "running"].includes(lastStatus.reconnectState)) return;
      reconnectBusy = true;
      revision++;
      clearTimeout(timer);
      controller?.abort();
      closePrompt();
      render(lastStatus);
      const request = new AbortController();
      const timeout = setTimeout(() => request.abort(), 8000);
      try {
        const response = await api("/api/v1/result-storage/reconnect", {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({confirmCurrentArchive: true, allowMissingRuns: true}), signal: request.signal});
        lastStatus = {...lastStatus, reconnectState: response.state, reconnectError: ""};
      } catch (error) {
        lastStatus = {...lastStatus, reconnectState: "failed", reconnectError: `Reconnect request could not be confirmed: ${error.message}`};
      } finally {
        clearTimeout(timeout);
        revision++;
        reconnectBusy = false;
        render(lastStatus);
        clearTimeout(timer);
        void refresh();
      }
    });
    root.document.addEventListener("visibilitychange", () => {
      clearTimeout(timer);
      if (root.document.hidden) controller?.abort(); else void refresh();
    });
    root.addEventListener("pagehide", () => { stopped = true; clearTimeout(timer); controller?.abort(); });
    root.addEventListener("pageshow", () => { stopped = false; void refresh(); });
    void refresh();
  }
  const exported = {describe, init};
  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  else root.KPLResultStorage = exported;
})(typeof globalThis !== "undefined" ? globalThis : this);

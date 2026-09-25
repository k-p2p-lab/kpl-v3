(function (root) {
  "use strict";
  function bytes(value) {
    if (!Number.isFinite(value) || value < 0) return "unknown";
    if (value < 1024) return `${value} B`;
    const unit = Math.min(4, Math.floor(Math.log(value) / Math.log(1024)));
    return `${(value / 1024 ** unit).toLocaleString("en-US", {maximumFractionDigits: 1})} ${["B", "KiB", "MiB", "GiB", "TiB"][unit]}`;
  }
  function describe(status, now = Date.now()) {
    const low = status.minFreeBytes > 0 && status.availableBytes < status.minFreeBytes;
    const stalled = !!status.checking && Number.isFinite(Date.parse(status.checkStartedAt)) && now - Date.parse(status.checkStartedAt) > 15000;
    const archive = status.error ? "Archive unavailable; results remain local."
      : stalled ? "Archive is taking longer to respond; local recording remains available."
      : !status.lastCheckedAt || status.lastCheckedAt.startsWith("0001-") ? "Discovering saved archives…" : "Archive connected.";
    return {text: `Local result space: ${bytes(status.availableBytes)} free. ${low ? "Waiting for space before starting the next run. " : ""}${archive}`, warning: low || !!status.error || stalled};
  }
  function init({api}) {
    const element = root.document.querySelector("#resultStorageStatus");
    if (!element) return;
    let timer, controller, stopped = false;
    async function refresh() {
      if (stopped || controller || root.document.hidden) return;
      controller = new AbortController();
      const timeout = setTimeout(() => controller?.abort(), 8000);
      try {
        const status = await api("/api/v1/result-storage", {signal: controller.signal});
        const message = describe(status);
        element.textContent = message.text;
        element.dataset.warning = String(message.warning);
        element.title = status.error || (status.minFreeBytes ? `Minimum local free space before starting a run: ${bytes(status.minFreeBytes)}.` : "Local free-space admission guard is disabled.");
      } catch (error) {
        if (!stopped && error.status !== 401) {
          element.textContent = "Storage status is temporarily unavailable.";
          element.dataset.warning = "true";
        }
      } finally {
        clearTimeout(timeout); controller = null;
        if (!stopped) timer = setTimeout(refresh, 15000);
      }
    }
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

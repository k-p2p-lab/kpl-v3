(function (root) {
  "use strict";

  const digestPattern = /^sha256:[a-f0-9]{64}$/;
  const text = value => typeof value === "string" && value.trim() ? value.trim() : "Unknown";
  const compact = (value, length) => value.length > length ? `${value.slice(0, length - 1)}…` : value;

  function describe(value) {
    const build = value?.build || {};
    const version = text(build.version), recordedRevision = text(build.revision);
    const revision = /^(unknown|development)$/i.test(recordedRevision) ? "Unknown" : recordedRevision;
    const image = text(value?.image);
    const digest = typeof value?.imageDigest === "string" && digestPattern.test(value.imageDigest)
      && image.endsWith(`@${value.imageDigest}`) ? value.imageDigest : "Unknown";
    const source = revision !== "Unknown" ? `${revision}${build.modified === true ? " +modified" : ""}` : revision;
    const label = version === "dev" && revision !== "Unknown" ? `dev (${revision.slice(0, 8)}${build.modified === true ? "+" : ""})` : compact(version, 28);
    return {
      summary: `Version · ${label} · ${digest === "Unknown" ? "Digest unknown" : digest.slice(0, 19)}`,
      version, revision: source, image, digest,
      binary: text(build.binaryHash), go: text(build.goVersion), time: text(build.buildTime),
    };
  }

  let api, pending, activeController, generation = 0, hasResult = false;

  function cancelPending() {
    ++generation;
    activeController?.abort();
    activeController = null;
    pending = null;
  }

  function render(value) {
    const details = describe(value);
    for (const [suffix, key] of [["Summary", "summary"], ["Build", "version"], ["Revision", "revision"], ["Time", "time"], ["Go", "go"], ["Binary", "binary"], ["Image", "image"], ["Digest", "digest"]]) {
      const element = root.document.querySelector(`#runtimeVersion${suffix}`);
      if (element && element.textContent !== details[key]) element.textContent = details[key];
    }
    const summary = root.document.querySelector("#runtimeVersionSummary");
    if (summary) summary.title = `Controller ${details.version}\nRevision: ${details.revision}\nDeployment image: ${details.image}\nImage digest: ${details.digest}`;
  }

  function refresh(options = {}) {
    if (!api || root.document.hidden) return pending;
    if (pending) {
      if (!options.force) return pending;
      cancelPending();
    }
    const current = ++generation;
    const controller = new AbortController();
    activeController = controller;
    const timer = setTimeout(() => controller.abort(), 10000);
    pending = Promise.resolve().then(async () => {
      const status = root.document.querySelector("#runtimeVersionStatus");
      try {
        const value = await api("/api/v1/version", {signal: controller.signal});
        if (current !== generation) return;
        render(value);
        hasResult = true;
        if (status) status.textContent = "";
      } catch {
        if (current !== generation) return;
        if (!hasResult) render(null);
        if (status) status.textContent = hasResult
          ? "Could not refresh runtime version. Showing the last confirmed information."
          : "Runtime version is unavailable. Reconnect to try again.";
      } finally {
        clearTimeout(timer);
        if (current === generation) {
          pending = null;
          activeController = null;
          // A refreshed label or status can change the open popover's bounds.
          root.KPLActionMenus?.refresh();
        }
      }
    });
    return pending;
  }

  function init(options) {
    if (api || !root.document?.querySelector("#runtimeVersion")) return;
    api = options.api;
    root.document.addEventListener("visibilitychange", () => {
      if (root.document.hidden) cancelPending();
      else void refresh({force:true});
    });
    void refresh();
  }

  const exported = {describe, init, refresh};
  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  else root.KPLRuntimeVersion = exported;
})(globalThis);

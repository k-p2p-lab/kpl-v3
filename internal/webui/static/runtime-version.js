(function (root) {
  "use strict";

  const digestPattern = /^sha256:[a-f0-9]{64}$/;
  const text = value => typeof value === "string" && value.trim() ? value.trim() : "Unknown";
  const compact = (value, length) => value.length > length ? `${value.slice(0, length - 1)}…` : value;

  function describe(value) {
    const build = value?.build || {};
    const version = text(build.version), revision = text(build.revision);
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

  let api, pending, hasResult = false;

  function render(value) {
    const details = describe(value);
    for (const [suffix, key] of [["Summary", "summary"], ["Build", "version"], ["Revision", "revision"], ["Time", "time"], ["Go", "go"], ["Binary", "binary"], ["Image", "image"], ["Digest", "digest"]]) {
      const element = root.document.querySelector(`#runtimeVersion${suffix}`);
      if (element && element.textContent !== details[key]) element.textContent = details[key];
    }
    const summary = root.document.querySelector("#runtimeVersionSummary");
    if (summary) summary.title = `Controller ${details.version}\nRevision: ${details.revision}\nDeployment image: ${details.image}\nImage digest: ${details.digest}`;
  }

  function refresh() {
    if (!api || root.document.hidden || pending) return pending;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 10000);
    pending = Promise.resolve().then(async () => {
      const status = root.document.querySelector("#runtimeVersionStatus");
      try {
        const value = await api("/api/v1/version", {signal: controller.signal});
        render(value);
        hasResult = true;
        if (status) status.textContent = "";
      } catch {
        if (!hasResult) render(null);
        if (status) status.textContent = hasResult
          ? "Could not refresh runtime version. Showing the last confirmed information."
          : "Runtime version is unavailable. Reconnect to try again.";
      } finally {
        clearTimeout(timer);
        pending = null;
      }
    });
    return pending;
  }

  function init(options) {
    if (api || !root.document?.querySelector("#runtimeVersion")) return;
    api = options.api;
    root.document.addEventListener("visibilitychange", () => { if (!root.document.hidden) void refresh(); });
    void refresh();
  }

  const exported = {describe, init, refresh};
  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  else root.KPLRuntimeVersion = exported;
})(globalThis);

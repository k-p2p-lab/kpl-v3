(function (root) {
  "use strict";

  function selectedIDs(scope, keys, scenarios, results) {
    const selected = new Set(keys), known = new Set(), ids = [];
    if (scope === "scenarios") {
      for (const item of scenarios || []) {
        const key = "scenario:" + item.id;
        if (selected.has(key)) { known.add(key); ids.push(item.id); }
      }
    } else if (scope === "results") {
      // A selected batch includes its full list, including failed and previous
      // attempts, even when search/status filters match only some members.
      for (const run of results || []) {
        const key = run.batchId ? "batch:" + run.batchId : "run:" + run.id;
        if (selected.has(key)) {
          known.add(key);
          if (!run.missing && run.state !== "missing") ids.push(run.id);
        }
      }
    } else throw new Error("Unknown download selection.");
    if (!selected.size || known.size !== selected.size) throw new Error("Some selected items are no longer listed. Refresh the list and select them again.");
    const unique = [...new Set(ids)];
    if (!unique.length) throw new Error("The selected result data is missing. Retry the missing runs to create new results before downloading.");
    if (unique.length > 2000) throw new Error("Download up to 2,000 scenarios or runs at a time. Select fewer items or batches.");
    return unique;
  }

  function saveBlob(blob, name, document, url) {
    const link = document.createElement("a"), href = url.createObjectURL(blob);
    try {
      link.href = href;
      link.download = name;
      link.hidden = true;
      document.body.appendChild(link);
      try { link.click(); } finally { link.remove(); }
    } catch (error) {
      url.revokeObjectURL(href);
      throw error;
    }
    setTimeout(() => url.revokeObjectURL(href), 60000).unref?.();
  }

  function createDownloads({api, getScenarios = () => [], getResults, document = root.document, url = root.URL, manifest = false}) {
    return async (scope, keys) => {
      if (manifest && scope !== "results") throw new Error("Extract manifests are available for saved results only.");
      const ids = selectedIDs(scope, keys, getScenarios(), getResults());
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 30000);
      let download;
      try {
        download = await api("/api/v1/downloads", {method: "POST", headers: {"Content-Type": "application/json"},
          body: JSON.stringify({kind: manifest ? "extract-manifest" : scope, ids}), signal: controller.signal,
          ...(manifest ? {responseType:"text"} : {})});
      } catch (error) {
        if (error.name === "AbortError") throw new Error("Preparing the download timed out. Try again.");
        throw error;
      } finally { clearTimeout(timer); }
      if (manifest) {
        saveBlob(new Blob([download], {type:"text/csv;charset=utf-8"}), "kpl_extract_manifest.csv", document, url);
        return;
      }
      if (!/^\/api\/v1\/downloads\/[A-Za-z0-9_-]+$/.test(download?.url || "")) throw new Error("The download response was invalid. Try again.");
      // Let the browser stream large archives to disk instead of collecting
      // every result in a JavaScript Blob or opening one tab per Run.
      const link = document.createElement("a");
      link.href = download.url;
      link.download = "";
      link.target = "_blank";
      link.rel = "noopener";
      link.hidden = true;
      document.body.appendChild(link);
      try { link.click(); } finally { link.remove(); }
    };
  }

  const exported = {selectedIDs, createDownloads, saveBlob};
  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  else root.KPLLibraryDownloads = exported;
})(globalThis);

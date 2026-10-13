(function (root) {
  "use strict";
  const node = typeof module !== "undefined" && module.exports;
  const selection = node ? require("./library-downloads.js") : root.KPLLibraryDownloads;
  const images = node ? require("./result-images.js") : root.KPLResultImages;
  const research = node ? require("./research-charts.js") : root.KPLResearch;
  const files = node ? require("./research-files.js") : root.KPLResearchFiles;
  const maxArchiveBytes = 128 * 1024 * 1024;
  const archiveTooLarge = () => new Error("CSV export exceeds 128 MiB. Select fewer results and download again.");
  const checkAbort = signal => { if (signal?.aborted) throw new DOMException("CSV download canceled.", "AbortError"); };

  function pause(signal, milliseconds) {
    checkAbort(signal);
    return new Promise((resolve, reject) => {
      const done = () => { signal?.removeEventListener("abort", abort); resolve(); };
      const timer = setTimeout(done, milliseconds);
      const abort = () => { clearTimeout(timer); signal.removeEventListener("abort", abort); reject(new DOMException("CSV download canceled.", "AbortError")); };
      signal?.addEventListener("abort", abort, {once:true});
    });
  }

  function createDownloads({api, getResults, document = root.document, url = root.URL, pollInterval = 2000,
    requestTimeout = 30000, byteLimit = maxArchiveBytes}) {
    async function request(path, options, signal) {
      checkAbort(signal);
      const controller = new AbortController();
      const abort = () => controller.abort();
      signal?.addEventListener("abort", abort, {once:true});
      const timer = setTimeout(abort, requestTimeout);
      try {
        const value = await api(path, {...options, signal:controller.signal});
        checkAbort(signal);
        return value;
      } catch (error) {
        checkAbort(signal);
        if (error.name === "AbortError" && !signal?.aborted)
          throw new Error("Loading analysis timed out. Download CSV ZIP again to reconnect; accepted analysis continues on the server.");
        throw error;
      } finally { clearTimeout(timer); signal?.removeEventListener("abort", abort); }
    }

    async function loadAnalysis(id, signal, progress) {
      const path = `/api/v1/analysis-jobs/${encodeURIComponent(id)}`;
      const validateJob = job => {
        if (job?.runId !== id || !["idle", "queued", "running", "completed", "failed", "interrupted", "canceled"].includes(job.state)
          || (images.pendingJob(job) || job.state === "completed" || job.state === "idle" && job.stale) && (typeof job.id !== "string" || !job.id))
          throw new Error("Unexpected analysis job response.");
      };
      let job = await request(path, {}, signal);
      validateJob(job);
      // Export the last completed snapshot even during verification or a new
      // analysis. CSV download never requests a forced reanalysis.
      const savedID = job.state === "completed" || job.state === "idle" && job.stale ? job.id : job.savedAnalysisId;
      let analysisID = savedID;
      if (!analysisID) {
        if (!images.pendingJob(job)) {
          job = await request(path, {method:"POST"}, signal);
          validateJob(job);
        }
        while (images.pendingJob(job)) {
          progress(job.state === "queued" ? "Analysis queued" : `Analyzing${job.phase ? " · " + job.phase : ""}`);
          await pause(signal, pollInterval);
          job = await request(path, {}, signal);
          validateJob(job);
        }
        if (job.state !== "completed") throw new Error(job.error || `Analysis ${job.state}. Download again to retry.`);
        analysisID = job.id;
      }
      if (typeof analysisID !== "string" || !analysisID) throw new Error("Analysis snapshot identity is missing.");
      progress("Loading completed analysis");
      const data = await request(`${path}/result?jobId=${encodeURIComponent(analysisID)}${savedID ? "&saved=1" : ""}`, {}, signal);
      images.validateResponse(data, id);
      if (data.analysisId !== analysisID) throw new Error("The saved analysis changed. Download again to capture the current snapshot.");
      return data;
    }

    async function runArchive(id, signal, progress, availableBytes) {
      const data = await loadAnalysis(id, signal, progress);
      const charts = images.buildCharts(data).map(research.formatChart), bundle = [], encoder = new TextEncoder();
      if (charts.length > 65535) throw new Error("This result has too many charts for one CSV ZIP.");
      let size = 22;
      for (const [index, chart] of charts.entries()) {
        checkAbort(signal);
        progress(`Preparing CSV ${index + 1}/${charts.length}`);
        const name = `${id}-${chart.id}.csv`, bytes = encoder.encode(files.chartCSV(chart));
        size += 76 + 2 * encoder.encode(name).length + bytes.length;
        if (size > availableBytes) throw archiveTooLarge();
        bundle.push({name, data:bytes});
        // Yield between charts so cancel and progress remain responsive.
        await pause(signal, 0);
      }
      const archive = files.zip(bundle);
      checkAbort(signal);
      const name = images.imageZIPFilename(data.result.name, id).replace(/-images\.zip$/, "-csv.zip");
      return {name, data:new Uint8Array(await archive.arrayBuffer())};
    }

    return async (scope, keys, {signal, onProgress = () => {}} = {}) => {
      if (scope !== "results") throw new Error("CSV ZIP export is available for saved results only.");
      const ids = selection.selectedIDs(scope, keys, [], getResults());
      // IDs are also filename components. The server accepts this same alphabet.
      if (ids.some(id => typeof id !== "string" || !/^[A-Za-z0-9_-]{1,128}$/.test(id))) throw new Error("Invalid result identity. Refresh the list and select again.");
      const bundle = [], encoder = new TextEncoder();
      let size = 22;
      const controller = new AbortController(), abort = () => controller.abort();
      signal?.addEventListener("abort", abort, {once:true});
      root.addEventListener?.("pagehide", abort);
      if (signal?.aborted) controller.abort();
      let objectURL;
      try {
        for (const [index, id] of ids.entries()) {
          checkAbort(controller.signal);
          const progress = message => onProgress(`CSV ZIP ${index + 1}/${ids.length} · ${id} · ${message}`);
          try {
            const archive = await runArchive(id, controller.signal, progress, byteLimit - size);
            size += 76 + 2 * encoder.encode(archive.name).length + archive.data.length;
            if (size > byteLimit) throw archiveTooLarge();
            bundle.push(archive);
          } catch (error) {
            if (error.name === "AbortError") throw error;
            throw new Error(`${id}: ${error.message}`);
          }
        }
        checkAbort(controller.signal);
        onProgress("Preparing selected CSV bundles…");
        objectURL = url.createObjectURL(files.zip(bundle));
        checkAbort(controller.signal);
        const link = document.createElement("a");
        link.href = objectURL;
        link.download = `results-csv-${new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z")}.zip`;
        link.hidden = true;
        document.body.appendChild(link);
        try { link.click(); } finally { link.remove(); }
        // Keep the URL alive until the browser has started reading its Blob.
        const completedURL = objectURL;
        setTimeout(() => url.revokeObjectURL(completedURL), 60000).unref?.();
        objectURL = null;
      } finally {
        if (objectURL) url.revokeObjectURL(objectURL);
        signal?.removeEventListener("abort", abort);
        root.removeEventListener?.("pagehide", abort);
      }
    };
  }
  const exported = {createDownloads};
  if (node) module.exports = exported;
  else root.KPLResultCSVDownloads = exported;
})(globalThis);

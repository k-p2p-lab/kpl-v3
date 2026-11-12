(function (root) {
  "use strict";
  const node = typeof module !== "undefined" && module.exports;
  const images = node ? require("./result-images.js") : root.KPLResultImages;
  const batch = node ? require("./batch-analysis.js") : root.KPLBatchAnalysis;
  const research = node ? require("./research-charts.js") : root.KPLResearch;
  const files = node ? require("./research-files.js") : root.KPLResearchFiles;
  const maxArchiveBytes = 128 * 1024 * 1024;
  const archiveTooLarge = label => new Error(`${label} export exceeds 128 MiB. Select fewer results and download again.`);
  const checkAbort = signal => { if (signal?.aborted) throw new DOMException("Analysis export canceled.", "AbortError"); };

  function batchSnapshotCSV(data) {
    const row = (scope, result, analysis = {}) => [scope, result.id, analysis.analysisId, analysis.analysisVersion,
      analysis.asOf, analysis.sourceHash || result.sourceHash, result.sourceRevision, result.state];
    const rows = [["scope", "id", "analysis_id", "analysis_version", "as_of", "source_hash", "source_revision", "state"],
      row("batch", {id:data.batchId}, data),
      ...data.runs.map(run => row("included-run", run.result, run)),
      ...(data.excluded || []).map(run => row("excluded-run", run))];
    const quote = value => {
      const text = String(value ?? "");
      const safe = /^[\s]*[=+\-@]/.test(text) ? "'" + text : text;
      return '"' + safe.replaceAll('"', '""') + '"';
    };
    return rows.map(row => row.map(quote).join(",")).join("\n") + "\n";
  }

  function pause(signal, milliseconds) {
    checkAbort(signal);
    return new Promise((resolve, reject) => {
      const done = () => { signal?.removeEventListener("abort", abort); resolve(); };
      const timer = setTimeout(done, milliseconds);
      const abort = () => { clearTimeout(timer); signal.removeEventListener("abort", abort); reject(new DOMException("Analysis export canceled.", "AbortError")); };
      signal?.addEventListener("abort", abort, {once:true});
    });
  }

  function createDownloads({api, getResults, document = root.document, url = root.URL, pollInterval = 2000,
    requestTimeout = 30000, byteLimit = maxArchiveBytes, format = "csv"}) {
    if (!["csv", "json"].includes(format)) throw new Error("Unknown analysis export format.");
    const label = format === "json" ? "JSON" : "CSV";
    const tooLarge = () => archiveTooLarge(label);
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
          throw new Error(`Loading analysis timed out. Choose Download → ${format === "json" ? "Analysis JSON ZIP" : "CSV ZIP"} again to reconnect; accepted analysis continues on the server.`);
        throw error;
      } finally { clearTimeout(timer); signal?.removeEventListener("abort", abort); }
    }

    function selectedAnalyses(keys, results) {
      // Both formats follow the selected result set: one integrated artifact
      // for a repetition batch, without analyzing each member/previous attempt.
      const selected = new Set(keys), known = new Set(), targets = new Map();
      // Single submissions also have a batchId. Only repetition series use a
      // batch mean; retain per-Run export for single runs and their retries.
      const repeated = new Set((results || []).filter(run => run.batchId && run.repetitions > 1).map(run => run.batchId));
      for (const run of results || []) {
        const key = run.batchId ? "batch:" + run.batchId : "run:" + run.id;
        if (!selected.has(key)) continue;
        known.add(key);
        const isBatch = Boolean(run.batchId && repeated.has(run.batchId)), id = isBatch ? run.batchId : run.id;
        const missing = run.missing || run.state === "missing";
        if (missing && !isBatch) continue;
        const targetKey = (isBatch ? "batch:" : "run:") + id;
        if (!targets.has(targetKey)) targets.set(targetKey, {id, isBatch, ...(missing ? {savedOnly:true} : {})});
        else if (!missing) delete targets.get(targetKey).savedOnly;
      }
      if (!selected.size || known.size !== selected.size)
        throw new Error("Some selected items are no longer listed. Refresh the list and select them again.");
      if (!targets.size) throw new Error("The selected result data is missing. Retry the missing runs before requesting analysis downloads.");
      if (targets.size > 2000) throw new Error("Download up to 2,000 result sets at a time. Select fewer items or batches.");
      return [...targets.values()];
    }

    async function loadAnalysis({id, isBatch, savedOnly}, signal, progress) {
      const path = `/api/v1/${isBatch ? "batch-analysis-jobs" : "analysis-jobs"}/${encodeURIComponent(id)}`;
      const validateJob = job => {
        if ((isBatch ? job?.batchId : job?.runId) !== id || !["idle", "queued", "running", "completed", "failed", "interrupted", "canceled"].includes(job.state)
          || (images.pendingJob(job) || job.state === "completed" || job.state === "idle" && job.stale) && (typeof job.id !== "string" || !job.id))
          throw new Error("Unexpected analysis job response.");
      };
      let job = await request(path, {}, signal);
      validateJob(job);
      // Export the last completed snapshot even during verification or a new
      // analysis. Selection downloads never request forced reanalysis.
      const savedID = job.state === "completed" || job.state === "idle" && job.stale ? job.id : job.savedAnalysisId;
      let analysisID = savedID;
      if (!analysisID) {
        if (savedOnly) throw new Error("This batch's result data is missing and no completed analysis snapshot remains. Retry the missing runs before requesting a new analysis.");
        if (!images.pendingJob(job)) {
          try {
            job = await request(path, {method:"POST", headers:{"Content-Type":"application/json"},
              body:JSON.stringify({expectedAnalysis:{id:job.id || "",state:job.state || "idle"}})}, signal);
          } catch (error) {
            if (error.code === "analysis-attempt-changed") {
              const failure = new Error("Analysis request outcome unknown. Refresh results and inspect the execution queue before retrying. " + error.message);
              failure.code = error.code;
              failure.status = error.status;
              throw failure;
            }
            throw error;
          }
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
      const source = await request(`${path}/result?jobId=${encodeURIComponent(analysisID)}${savedID ? "&saved=1" : ""}`, format === "json" ? {responseType:"text"} : {}, signal);
      let data = format === "json" ? JSON.parse(source) : source;
      if (isBatch) data = batch.validate(data, id); else images.validateResponse(data, id);
      if (data.analysisId !== analysisID) throw new Error("The saved analysis changed. Download again to capture the current snapshot.");
      return {data, source};
    }

    async function analysisArchive(target, signal, progress, availableBytes) {
      const {id, isBatch} = target;
      const {data, source} = await loadAnalysis(target, signal, progress);
      if (format === "json") {
        checkAbort(signal);
        progress("Preparing analysis JSON");
        if (source.length > availableBytes) throw tooLarge();
        // Preserve the original JSON text, including integers beyond JS precision.
        const bytes = new TextEncoder().encode(source);
        if (bytes.length > availableBytes) throw tooLarge();
        const name = images.imageZIPFilename(isBatch ? data.name : data.result.name, id)
          .replace(/-images\.zip$/, isBatch ? "-batch-analysis.json" : "-analysis.json");
        await pause(signal, 0);
        return {name, data:bytes};
      }
      const charts = (isBatch ? batch.build(data, images.buildCharts) : images.buildCharts(data)).map(research.formatChart);
      const bundle = [], encoder = new TextEncoder(), artifactID = isBatch ? `${id}-batch-mean` : id;
      if (charts.length + (isBatch ? 2 : 0) > 65535) throw new Error("This result has too many charts for one CSV ZIP.");
      let size = 22;
      const addCSV = (name, source) => {
        const bytes = encoder.encode(source);
        size += 76 + 2 * encoder.encode(name).length + bytes.length;
        if (size > availableBytes) throw tooLarge();
        bundle.push({name, data:bytes});
      };
      if (isBatch) {
        addCSV(`${artifactID}-summary.csv`, batch.summaryCSV(data.summary));
        addCSV(`${artifactID}-snapshot.csv`, batchSnapshotCSV(data));
      }
      for (const [index, chart] of charts.entries()) {
        checkAbort(signal);
        progress(`Preparing CSV ${index + 1}/${charts.length}`);
        addCSV(`${artifactID}-${chart.id}.csv`, files.chartCSV(chart));
        // Yield between charts so cancel and progress remain responsive.
        await pause(signal, 0);
      }
      const archive = files.zip(bundle);
      checkAbort(signal);
      const name = images.imageZIPFilename(isBatch ? data.name : data.result.name, artifactID).replace(/-images\.zip$/, "-csv.zip");
      return {name, data:new Uint8Array(await archive.arrayBuffer())};
    }

    return async (scope, keys, {signal, onProgress = () => {}} = {}) => {
      if (scope !== "results") throw new Error(`${label} ZIP export is available for saved results only.`);
      const targets = selectedAnalyses(keys, getResults());
      // IDs are also filename components. The server accepts this same alphabet.
      if (targets.some(({id}) => typeof id !== "string" || !/^[A-Za-z0-9_-]{1,128}$/.test(id))) throw new Error("Invalid result identity. Refresh the list and select again.");
      const bundle = [], encoder = new TextEncoder();
      let size = 22;
      const controller = new AbortController(), abort = () => controller.abort();
      signal?.addEventListener("abort", abort, {once:true});
      root.addEventListener?.("pagehide", abort);
      if (signal?.aborted) controller.abort();
      let objectURL;
      try {
        for (const [index, target] of targets.entries()) {
          checkAbort(controller.signal);
          const identity = target.isBatch ? `Batch ${target.id}` : target.id;
          const progress = message => onProgress(`${label} ZIP ${index + 1}/${targets.length} · ${identity} · ${message}`);
          try {
            const archive = await analysisArchive(target, controller.signal, progress, byteLimit - size);
            size += 76 + 2 * encoder.encode(archive.name).length + archive.data.length;
            if (size > byteLimit) throw tooLarge();
            bundle.push(archive);
          } catch (error) {
            if (error.name === "AbortError") throw error;
            throw new Error(`${identity}: ${error.message}`);
          }
        }
        checkAbort(controller.signal);
        onProgress(format === "json" ? "Preparing selected analysis JSON files…" : "Preparing selected CSV bundles…");
        objectURL = url.createObjectURL(files.zip(bundle));
        checkAbort(controller.signal);
        const link = document.createElement("a");
        link.href = objectURL;
        link.download = `results-${format === "json" ? "analysis" : "csv"}-${new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z")}.zip`;
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
  else root.KPLResultAnalysisDownloads = exported;
})(globalThis);

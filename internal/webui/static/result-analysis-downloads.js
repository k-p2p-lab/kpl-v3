(function (root) {
  "use strict";
  const node = typeof module !== "undefined" && module.exports;
  const images = node ? require("./result-images.js") : root.KPLResultImages;
  const batch = node ? require("./batch-analysis.js") : root.KPLBatchAnalysis;
  const research = node ? require("./research-charts.js") : root.KPLResearch;
  const files = node ? require("./research-files.js") : root.KPLResearchFiles;
  const downloads = node ? require("./library-downloads.js") : root.KPLLibraryDownloads;
  const maxArchiveBytes = 128 * 1024 * 1024;
  // Stored ZIP: local header + central header, two copies of the UTF-8
  // filename, and one end-of-central-directory record per archive.
  const encoder = new TextEncoder(), zipEnd = 22;
  const entryHeader = name => 76 + 2 * encoder.encode(name).length;
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

    async function* analysisFiles(target, signal, progress) {
      const {id, isBatch} = target;
      const {data, source} = await loadAnalysis(target, signal, progress);
      if (format === "json") {
        checkAbort(signal);
        progress("Preparing analysis JSON");
        // Preserve the original JSON text, including integers beyond JS precision.
        const name = images.imageZIPFilename(isBatch ? data.name : data.result.name, id)
          .replace(/-images\.zip$/, isBatch ? "-batch-analysis.json" : "-analysis.json");
        await pause(signal, 0);
        yield {name, blob:new Blob([source])};
        return;
      }
      const charts = (isBatch ? batch.build(data, images.buildCharts) : images.buildCharts(data)).map(research.formatChart);
      const artifactID = isBatch ? `${id}-batch-mean` : id;
      const name = images.imageZIPFilename(isBatch ? data.name : data.result.name, artifactID).replace(/-images\.zip$/, "-csv.zip");
      function* rows() {
        if (isBatch) {
          yield ["summary", batch.summaryCSV(data.summary)];
          yield ["snapshot", batchSnapshotCSV(data)];
        }
        for (const [index, chart] of charts.entries()) {
          progress(`Preparing CSV ${index + 1}/${charts.length}`);
          checkAbort(signal);
          yield [chart.id, files.chartCSV(chart)];
        }
      }
      const bundle = [];
      let nested = true, size = zipEnd + entryHeader(name) + zipEnd;
      for (const [suffix, text] of rows()) {
        checkAbort(signal);
        const entry = {name:`${artifactID}-${suffix}.csv`, data:encoder.encode(text)};
        size += entryHeader(entry.name) + entry.data.length;
        if (nested && (size > byteLimit || bundle.length === 65535)) {
          // Large CSV results remain usable chart by chart. The directory
          // keeps files associated with the same Run or batch in every part.
          nested = false;
          for (const item of bundle) yield {name:name.slice(0, -4) + "/" + item.name, blob:new Blob([item.data])};
          bundle.length = 0;
        }
        if (nested) bundle.push(entry);
        else yield {name:name.slice(0, -4) + "/" + entry.name, blob:new Blob([entry.data])};
        await pause(signal, 0);
      }
      checkAbort(signal);
      if (nested) yield {name, blob:files.zip(bundle)};
    }

    return async (scope, keys, {signal, onProgress = () => {}} = {}) => {
      if (scope !== "results") throw new Error(`${label} ZIP export is available for saved results only.`);
      const targets = selectedAnalyses(keys, getResults());
      // IDs are also filename components. The server accepts this same alphabet.
      if (targets.some(({id}) => typeof id !== "string" || !/^[A-Za-z0-9_-]{1,128}$/.test(id))) throw new Error("Invalid result identity. Refresh the list and select again.");
      const bundle = [];
      let size = zipEnd, parts = 0, fragmented = false;
      const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
      const base = `results-${format === "json" ? "analysis" : "csv"}-${stamp}`;
      const controller = new AbortController(), abort = () => controller.abort();
      signal?.addEventListener("abort", abort, {once:true});
      root.addEventListener?.("pagehide", abort);
      if (signal?.aborted) controller.abort();
      function sendPart(numbered) {
        onProgress(`Preparing selected ${format === "json" ? "analysis JSON files" : "CSV bundles"}${numbered ? " · ZIP " + (parts + 1) + " · Allow multiple downloads if prompted" : ""}…`);
        checkAbort(controller.signal);
        const suffix = numbered ? "-part-" + String(parts + 1).padStart(3, "0") : "";
        downloads.saveBlob(files.zip(bundle), base + suffix + ".zip", document, url);
        parts++;
        bundle.length = 0;
        size = zipEnd;
      }
      async function add(name, blob) {
        checkAbort(controller.signal);
        const bytes = entryHeader(name) + blob.size;
        if (bundle.length && (size + bytes > byteLimit || bundle.length === 65535)) sendPart(true);
        const data = new Uint8Array(await blob.arrayBuffer());
        checkAbort(controller.signal);
        bundle.push({name, data});
        size += bytes;
      }
      async function addFile({name, blob}) {
        if (zipEnd + entryHeader(name) + blob.size <= byteLimit) {
          await add(name, blob);
          return;
        }
        // Only a single oversized file requires byte reconstruction. Include
        // the expected count so an interrupted download is detectable.
        const number = n => String(n).padStart(6, "0");
        const fragment = (i, n) => `fragments/${name}/part-${number(i)}-of-${number(n)}`;
        const capacity = byteLimit - zipEnd - entryHeader(fragment(1, 1));
        const count = Math.ceil(blob.size / capacity);
        if (capacity < 1 || count > 999999) throw new Error("ZIP part size is too small for this filename.");
        fragmented = true;
        for (let i = 0; i < count; i++) {
          await add(fragment(i + 1, count), blob.slice(i * capacity, (i + 1) * capacity));
          await pause(controller.signal, 0);
        }
      }
      try {
        for (const [index, target] of targets.entries()) {
          checkAbort(controller.signal);
          const identity = target.isBatch ? `Batch ${target.id}` : target.id;
          const progress = message => onProgress(`${label} ZIP ${index + 1}/${targets.length} · ${identity} · ${message}`);
          try {
            for await (const file of analysisFiles(target, controller.signal, progress)) await addFile(file);
          } catch (error) {
            if (error.name === "AbortError") throw error;
            throw new Error(`${identity}: ${error.message}`);
          }
        }
        checkAbort(controller.signal);
        sendPart(parts > 0);
        return {parts, fragmented};
      } catch (error) {
        if (parts) {
          if (error.name !== "AbortError") throw new Error(`${error.message} Already requested ${parts} ZIP download(s). Export stopped before completion.`);
          error.partsRequested = parts;
        }
        throw error;
      } finally {
        signal?.removeEventListener("abort", abort);
        root.removeEventListener?.("pagehide", abort);
      }
    };
  }
  const exported = {createDownloads};
  if (node) module.exports = exported;
  else root.KPLResultAnalysisDownloads = exported;
})(globalThis);

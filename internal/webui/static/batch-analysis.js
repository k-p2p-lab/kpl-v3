/* Equal-run summaries and overview charts for one persisted repetition batch. */
(function (root) {
  "use strict";
  const R = typeof module !== "undefined" && module.exports ? require("./research-charts.js") : root.KPLResearch;
  const finite = value => typeof value === "number" && Number.isFinite(value);
  function validate(data, id) {
    if (data?.version !== 1 || data.aggregation !== "equal-run-mean-v1" || data.batchId !== id || !Array.isArray(data.runs) || data.runs.length < 2 || data.runs.length > 100 || !Array.isArray(data.excluded) || !data.summary)
      throw new Error("Invalid batch analysis response.");
    const ids = new Set();
    for (const run of data.runs) {
      if (!run.result?.id || ids.has(run.result.id) || run.result.batchId !== id || run.result.state !== "completed") throw new Error("Batch analysis contains an unrelated or duplicate run.");
      ids.add(run.result.id);
    }
  }
  function points(input) {
    const map = new Map();
    for (const p of input || []) if (finite(p.x)) map.set(p.x, { x: p.x, y: finite(p.y) ? p.y : null });
    return [...map.values()].sort((a, b) => a.x - b.x);
  }
  function valueAt(curve, x, mode) {
    if (!curve.length) return null;
    let lo = 0, hi = curve.length;
    while (lo < hi) { const mid = (lo + hi) >> 1; if (curve[mid].x <= x) lo = mid + 1; else hi = mid; }
    const left = curve[lo - 1], right = curve[lo];
    if (mode === "discrete") return left?.x === x ? left.y : 0;
    if (mode === "cdf") return left ? left.y : 0;
    if (!left || x > curve.at(-1).x) return null;
    if (left.x === x || mode === "step") return left.y;
    if (!right || !finite(left.y) || !finite(right.y)) return null;
    return left.y + (right.y - left.y) * (x - left.x) / (right.x - left.x);
  }
  function average(curves, mode) {
    curves = curves.map(points).filter(c => c.some(p => finite(p.y)));
    const xs = [...new Set(curves.flatMap(c => c.map(p => p.x)))].sort((a, b) => a - b);
    // Discrete probabilities/counts retain their whole support. Sampling them
    // would silently remove probability mass. Line/step previews are bounded.
    const stride = mode === "discrete" ? 1 : Math.max(1, Math.ceil((xs.length - 1) / 719));
    return xs.filter((_, i) => i % stride === 0 || i === xs.length - 1).map(x => {
      const stat = R.stats(curves.map(c => valueAt(c, x, mode)));
      return { x, y: stat.mean, error: stat.sd, n: stat.n, label: `n=${stat.n} runs` };
    });
  }
  function commonHistograms(runs) {
    const lists = runs.map(a => a.latencyHistogram || []), ranges = lists.filter(c => c.length).map(c => {
      const width = c.length > 1 ? c[1].x - c[0].x : 0;
      return [c[0].x - width / 2, c.at(-1).x + width / 2];
    });
    if (!ranges.length) return lists;
    const low = Math.min(...ranges.map(r => r[0])), high = Math.max(...ranges.map(r => r[1]));
    if (low === high) return lists;
    const count = 30, width = (high - low) / count;
    return lists.map(list => {
      if (!list.length) return [];
      const sourceWidth = list.length > 1 ? list[1].x - list[0].x : 0;
      const bins = Array.from({ length: count }, (_, i) => ({ x: low + (i + .5) * width, y: 0 }));
      for (const p of list) {
        if (!sourceWidth) { bins[Math.min(count - 1, Math.max(0, Math.floor((p.x - low) / width)))].y += p.y; continue; }
        const start = p.x - sourceWidth / 2, end = p.x + sourceWidth / 2;
        for (let i = 0; i < count; i++) bins[i].y += p.y * Math.max(0, Math.min(end, low + (i + 1) * width) - Math.max(start, low + i * width)) / sourceWidth;
      }
      return bins;
    });
  }
  const cumulativeIDs = new Set(["latency-cdf", "degree-cdf", "research-propagationCDF", "research-duplicateCDF", "research-hopCDF", "propagation-log-time", "eager-lazy-latency", "mean-receivers-time", "mean-receivers-hop"]);
  function build(data, buildRunCharts) {
    validate(data, data.batchId);
    const histograms = commonHistograms(data.runs.flatMap(run => [run, ...(run.research?.receiverGroups || [])])), chartMap = new Map();
    let histogramIndex = 0;
    const protocols = [...new Set(data.runs.flatMap(a => (a.bandwidthTimeline || []).flatMap(b => (b.protocols || []).map(p => p.protocol))))];
    for (const [index, original] of data.runs.entries()) {
      const a = { ...original, timeOrigin: original.result.startedAt, latencyHistogram: histograms[histogramIndex++], bandwidthTimeline: (original.bandwidthTimeline || []).map(b => ({ ...b, protocols: protocols.map(protocol => b.protocols?.find(p => p.protocol === protocol) || { protocol, sentBytes: 0, receivedBytes: 0 }) })) };
      if (original.research?.receiverGroups) a.research = { ...original.research, receiverGroups: original.research.receiverGroups.map(group => ({ ...group, latencyHistogram: histograms[histogramIndex++] })) };
      for (const chart of buildRunCharts(a)) {
        if (chart.panels || chart.tree || chart.id.endsWith("-increments")) continue;
        if (!chartMap.has(chart.id)) chartMap.set(chart.id, []);
        chartMap.get(chart.id).push(chart);
      }
    }
    const source = `${data.name || data.batchId} · Batch ${data.batchId} · ${data.runs.length}/${data.expectedRuns} completed runs`;
    const charts = [...chartMap.values()].map(copies => {
      const base = copies[0], names = [...new Set(copies.flatMap(c => c.series.map(s => s.name)))];
      const isCDF = cumulativeIDs.has(base.id), timeSeries = /Elapsed|elapsed/.test(base.xLabel || "");
      const mode = isCDF ? "cdf" : base.mode === "bar" ? "discrete" : base.mode === "step" ? "step" : "line";
      let note = "Equal weight per run; shaded ranges show ±1 between-run sample SD. Missing evidence is excluded; n in CSV is the contributing run count. One run has no sample SD.";
      if (copies.some(c => c.series.some(s => s.name.startsWith("Group: ") || s.name === "Unknown Group"))) note += " Receiver groups are matched by name across runs; absent groups are excluded, while measured zero receipts contribute zero. Each group uses its own eligible receiver denominator.";
      if (timeSeries) note += " Time is relative to each run start; lines interpolate within observed segments, steps hold within recorded intervals. No extrapolation beyond each series; at most 720 displayed time points.";
      if (base.id === "latency-distribution") note += " Common 30-bin histogram; source-bin counts are distributed uniformly over overlapping bins. Count mass is preserved; within-bin locations are approximate.";
      if (base.id === "degree-student-t") note += " Mean of individual fitted densities, not a refit of pooled samples.";
      if (/bandwidth|throughput/.test(base.id)) note += " Measured libp2p stream traffic, excluding IP/TCP framing, retransmissions and management traffic.";
      if (/eager|origin|messages-.*count/.test(base.id)) note += " Eager/lazy paths are GRAFT/IHAVE/IWANT metadata estimates; unclassified evidence remains unknown.";
      const series = names.map(name => ({ name, points: average(copies.map(c => c.series.find(s => s.name === name)?.points || []), mode) }));
      return { ...base, ...(base.groupTitle ? { groupTitle: `${base.groupTitle} · run mean` } : {}), title: `${base.title} · run mean`, source, note, series };
    });
    for (const kind of ["time", "hop"]) {
      const parent = charts.find(c => c.id === `mean-receivers-${kind}`);
      if (parent) {
        const originals = chartMap.get(parent.id);
        const series = parent.series.map(parentSeries => {
          const curves = originals.map(c => points(c.series.find(s => s.name === parentSeries.name)?.points || []));
          let previousX = null;
          const increments = parentSeries.points.map(p => {
            const stat = R.stats(curves.map(c => {
              if (!c.length) return null;
              return valueAt(c, p.x, "cdf") - (previousX === null ? 0 : valueAt(c, previousX, "cdf"));
            }));
            previousX = p.x;
            return { x: p.x, y: stat.mean, error: stat.sd, n: stat.n, label: `n=${stat.n} runs` };
          });
          return { name: parentSeries.name, points: increments };
        });
        charts.push({ ...parent, id: `${parent.id}-increments`, title: `Mean first-receiver increments by ${kind} · run mean`, yLabel: "New receivers / published message", mode: "bar", groupedBars: series.length > 1, series, note: "Per-run differences on the common displayed cumulative grid, then equal-run mean/sample SD. Counts per displayed interval, not a density per second. Group identity follows receiving peers." });
      }
    }
    for (const key of Object.keys(metricLabels)) {
      const stat = data.summary[key];
      if (!stat) continue;
      charts.push({ id: `summary-${key.replaceAll(".", "-")}`, category: summaryCategory(key), title: label(key), source, mode: "bar", xLabel: "Completed runs", yLabel: label(key), xTicks: ["Run mean"], series: [{ name: "Mean and sample SD", points: [{ x: 0, y: stat.average, error: stat.deviation, n: stat.count, label: `n=${stat.count} runs` }] }], note: "Equal run weight; between-run sample SD. Missing values are excluded. P95 is the mean of run P95 values, not a pooled percentile." });
    }
    const panels = charts.filter(c => ["research-propagationCDF", "research-duplicateCDF"].includes(c.id));
    if (panels.length === 2) charts.push({ id: "propagation-duplicate-panels", title: "Propagation and duplicate accumulation · run mean", panels, series: [], source });
    return charts.map(chart => R.formatChart({ ...chart, category: chart.category || chart.protocol || "gossipsub" }));
  }
  const metricLabels = { "metrics.averageLatencyMs": "Mean first-delivery latency (ms)", "metrics.p95LatencyMs": "Mean of run P95 latency (ms)", "metrics.reachability": "Continuous-session delivery ratio", "metrics.stableCoverage": "Stable coverage", "metrics.initialDeliveryRatio": "Initial delivery ratio", "metrics.averageDuplicates": "Mean duplicate copies", "bandwidth.sentBytes": "Sent stream bytes", "bandwidth.receivedBytes": "Received stream bytes" };
  function label(key) {
    if (key === "research.node_count") return "GossipSub Node Count";
    return R.metricTitle(metricLabels[key] || (key.startsWith("research.") ? R.labels[key.slice(9)] : null) || key.split(".").at(-1).replace(/([a-z])([A-Z])/g, "$1 $2").replaceAll("_", " "));
  }
  function summaryCategory(key) { return key.startsWith("bandwidth.") || key === "research.node_count" ? "common" : "gossipsub"; }
  function summaryGroups(summary) {
    return Object.keys(R.categoryLabels).map(category => ({ category,
      rows: Object.entries(summary).filter(([key]) => summaryCategory(key) === category).sort(([a], [b]) => label(a).localeCompare(label(b))),
    })).filter(group => group.rows.length);
  }
  function summaryCSV(summary) {
    const quote = value => '"' + String(value ?? "").replaceAll('"', '""') + '"';
    return [["metric", "label", "mean", "sample_sd", "n"], ...Object.entries(summary).sort(([a], [b]) => a.localeCompare(b)).map(([key, stat]) => [key, label(key), stat.average, stat.deviation, stat.count])].map(row => row.map(quote).join(",")).join("\n") + "\n";
  }
  function description(data) {
    const excluded = data.excluded.map(r => `Run ${r.iteration || r.id}: ${r.state}`).join("; ");
    return `${data.runs.length}/${data.expectedRuns} runs included · equal run weight · ${data.missingRuns} missing/unreadable${excluded ? ` · Excluded: ${excluded}` : ""}`;
  }
  function reliabilityMarkup(data, escape) {
    const r = data.reliability;
    if (!r) return "";
    const metrics = `${r.attempts} started attempts · ${r.failed} failed (${(100 * r.failedAttemptRate).toFixed(1)}%) · ${r.canceled} canceled · ${r.interrupted} interrupted · ${r.retries} retries`;
    const environments = (r.environments || []).map((group, i) => {
      const agents = (group.agents || []).map(a => escape(`${a.id}: ${a.peerImage || "image unknown"}, capacity ${a.capacity}`)).join("; ");
      return `<li>Environment ${i + 1}: ${group.runIds.length} runs · ${escape(group.controllerVersion || "Controller version unknown")} · ${agents || "No recorded Agents"}</li>`;
    }).join("");
    return `<p class="dialog-help">${escape(metrics)}. Failure rate uses all recorded started attempts, including retries. Never-started queued runs are excluded.</p>${(r.warnings || []).map(w => `<p class="result-integrity">${escape(w)}</p>`).join("")}<details><summary>Execution environments · ${(r.environments || []).length}</summary><ul class="result-environments">${environments}</ul><p class="dialog-help">Recorded versions, Agent instances, images and capacities do not capture every change in host load or network conditions.</p></details>`;
  }
  const exported = { reliabilityMarkup, validate, average, valueAt, commonHistograms, build, label, summaryGroups, summaryCSV, description };
  if (typeof module !== "undefined" && module.exports) module.exports = exported; else root.KPLBatchAnalysis = exported;
})(globalThis);

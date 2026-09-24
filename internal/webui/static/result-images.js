/* Per-result PNG previews and downloads; charts use the existing result metrics. */
(function (root) {
  "use strict";
  const research =
    typeof module !== "undefined" && module.exports
      ? require("./research-charts.js")
      : root.KPLResearch;
  const files =
    typeof module !== "undefined" && module.exports
      ? require("./research-files.js")
      : root.KPLResearchFiles;
  const batch = typeof module !== "undefined" && module.exports ? require("./batch-analysis.js") : root.KPLBatchAnalysis;
  const colors = [
    "#2563eb",
    "#c45c10",
    "#7c3aed",
    "#0f766e",
    "#dc2626",
    "#7c5b18",
  ];
  const finite = (value) => typeof value === "number" && Number.isFinite(value);
  const escape = (value) =>
    String(value ?? "").replace(
      /[&<>"']/g,
      (c) =>
        ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        })[c],
    );
  const number = (value) =>
    finite(value)
      ? new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(
          value,
        )
      : "N/A";
  const pointOK = (point) => point && finite(point.x) && finite(point.y);
  const timeLabel = (value) =>
    value &&
    !String(value).startsWith("0001-") &&
    Number.isFinite(Date.parse(value))
      ? new Date(value).toLocaleString()
      : "N/A";

  function metricValue(metrics, key) {
    const m = metrics || {};
    if (
      ["averageLatencyMs", "p95LatencyMs"].includes(key) &&
      !(m.latencySamples > 0)
    )
      return null;
    if (key === "averageDuplicates" && !(m.duplicateSamples > 0)) return null;
    if (
      ["reachability", "deliveryRatioUpperBound"].includes(key) &&
      !m.deliveryRatioAvailable
    )
      return null;
    if (
      ["initialDeliveryRatio", "initialDeliveryRatioUpperBound"].includes(
        key,
      ) &&
      !m.initialDeliveryRatioAvailable
    )
      return null;
    if (
      ["stableCoverage", "stableCoverageUpperBound"].includes(key) &&
      !m.stableCoverageAvailable
    )
      return null;
    // Legacy cohort metrics do not define an uncertainty upper bound.
    if (
      key === "deliveryRatioUpperBound" &&
      m.definition !== "session-window-v1"
    )
      return null;
    return finite(m[key]) ? m[key] : null;
  }

  function trafficPoints(analysis, key) {
    const width = analysis.binSeconds;
    if (!(width > 0)) return [];
    const bins = (analysis.timeline || [])
      .filter((bin) => Number.isFinite(Date.parse(bin.at)))
      .sort((a, b) => Date.parse(a.at) - Date.parse(b.at));
    if (!bins.length) return [];
    const origin = Date.parse(analysis.timeOrigin || bins[0].at);
    const points = [];
    let end = 0;
    for (const bin of bins) {
      const x = (Date.parse(bin.at) - origin) / 1000;
      if (x > end) points.push({ x: end, y: 0 }, { x, y: 0 });
      points.push({ x, y: finite(bin[key]) ? bin[key] / width : null });
      end = x + width;
    }
    const last = points[points.length - 1];
    points.push({ x: end, y: last.y });
    return points;
  }

  // The API allocates bytes uniformly over each measured interval. Divide by
  // the display bin width only for throughput; cumulative totals retain bytes.
  function bandwidthPoints(analysis, protocol, direction, cumulative = false) {
    const width = analysis.bandwidthBinSeconds;
    const bins = analysis.bandwidthTimeline || [];
    if (!(width > 0) || !bins.length) return [];
    const origin = Date.parse(analysis.timeOrigin || bins[0].at),
      key = direction === "send" ? "sentBytes" : "receivedBytes";
    let total = 0,
      end = 0;
    const points = [];
    for (const bin of bins) {
      const x = (Date.parse(bin.at) - origin) / 1000;
      if (!Number.isFinite(x)) continue;
      if (x > end) points.push({ x: end, y: null });
      const entry =
        protocol === "*"
          ? bin
          : bin.protocols?.find((p) => p.protocol === protocol);
      const bytes = entry?.[key] ?? 0;
      if (cumulative) {
        points.push({ x, y: total / 1024 });
        total += bytes;
        points.push({ x: x + width, y: total / 1024 });
      } else {
        points.push(
          { x, y: (bytes * 8) / width / 1000 },
          { x: x + width, y: (bytes * 8) / width / 1000 },
        );
      }
      end = x + width;
    }
    return points;
  }

  function observationPoints(analysis, group, read) {
    const observations = analysis.observations || [];
    if (!observations.length) return [];
    const origin = Date.parse(analysis.timeOrigin || observations[0].at);
    return observations.map((observation) => {
      const value = observation.groups?.find((item) => item.group === group);
      return {
        x: (Date.parse(observation.at) - origin) / 1000,
        y: value ? read(value) : null,
      };
    });
  }

  // Normalize before subtracting: two finite endpoints can have an infinite
  // difference. Keep constant axes and padding within the finite number range.
  function chartAxis(low, high, padding = 0) {
    if (low === high) {
      const step = Math.max(1, Math.abs(low) * 0.1);
      if (finite(high + step)) high += step;
      else low -= step;
    }
    const scale = Math.max(Math.abs(low), Math.abs(high));
    const min = low / scale;
    let max = high / scale;
    const padded = max + (max - min) * padding;
    if (finite(padded * scale)) max = padded;
    return {
      position: (value) => (value / scale - min) / (max - min),
      value: (fraction) => (min * (1 - fraction) + max * fraction) * scale,
    };
  }

  function chartSVG(chart) {
    const {
      title,
      xLabel,
      yLabel,
      series,
      mode = "line",
      yMax,
      zeroX = true,
      xTicks,
      source = "",
      note = "",
      logX = false,
    } = chart;
    if (chart.tree) return treeSVG(chart);
    if (chart.panels) {
      const panels = chart.panels.map((p) =>
        chartSVG({ ...p, source: chart.source }),
      );
      let offset = 0;
      const nested = panels
        .map((svg) => {
          const height = Number(svg.match(/height="(\d+)"/)[1]);
          const result = svg.replace("<svg ", `<svg x="0" y="${offset}" `);
          offset += height;
          return result;
        })
        .join("");
      return `<svg xmlns="http://www.w3.org/2000/svg" width="640" height="${offset}" viewBox="0 0 640 ${offset}">${nested}</svg>`;
    }
    const eligible = (p) => pointOK(p) && (!logX || p.x > 0);
    const valid = series.flatMap((s) => s.points.filter(eligible));
    const footer =
      (source + " · " + note).match(/.{1,90}(?:\s|$)|.{1,90}/g) || [];
    const height = 362 + series.length * 20 + footer.length * 15;
    const label = `<title>${escape(title)}</title>`;
    const begin = `<svg xmlns="http://www.w3.org/2000/svg" width="640" height="${height}" viewBox="0 0 640 ${height}" role="img" aria-label="${escape(title)}" style="background:#ffffff;font:12px system-ui,sans-serif">${label}<rect width="640" height="${height}" fill="#ffffff"/><text x="68" y="17" fill="#142638">${escape(title)}</text>`;
    const foot = footer
      .map(
        (line, i) =>
          `<text x="28" y="${355 + series.length * 20 + i * 15}" fill="#536575" font-size="10">${escape(line)}</text>`,
      )
      .join("");
    if (!valid.length)
      return (
        begin +
        '<text x="320" y="160" text-anchor="middle" fill="#536575">No eligible observations available</text>' +
        foot +
        "</svg>"
      );
    let xmin = zeroX && !logX ? 0 : valid[0].x,
      xmax = valid[0].x,
      ymin = 0,
      ymax = 0;
    for (const point of valid) {
      xmin = Math.min(xmin, point.x);
      xmax = Math.max(xmax, point.x);
      const error = finite(point.error) ? Math.abs(point.error) : 0;
      const lo = point.y - error,
        hi = point.y + error;
      ymin = Math.min(ymin, finite(lo) ? lo : point.y);
      ymax = Math.max(ymax, finite(hi) ? hi : point.y);
      if (finite(point.xError)) {
        const lo = point.x - Math.abs(point.xError),
          hi = point.x + Math.abs(point.xError);
        if (finite(lo) && (!logX || lo > 0)) xmin = Math.min(xmin, lo);
        if (finite(hi)) xmax = Math.max(xmax, hi);
      }
      if (point.from && eligible(point.from)) {
        xmin = Math.min(xmin, point.from.x);
        xmax = Math.max(xmax, point.from.x);
        ymin = Math.min(ymin, point.from.y);
        ymax = Math.max(ymax, point.from.y);
      }
    }
    if (xTicks) {
      xmin = -0.5;
      xmax = xTicks.length - 0.5;
    }
    const xAxis = chartAxis(
      logX ? Math.log10(xmin) : xmin,
      logX ? Math.log10(xmax) : xmax,
    );
    const yAxis = chartAxis(
      ymin,
      finite(yMax) ? yMax : ymax,
      finite(yMax) ? 0 : 0.08,
    );
    const x = (v) => 68 + xAxis.position(logX ? Math.log10(v) : v) * 548;
    const y = (v) => 266 - yAxis.position(v) * 238;
    const tick = (value) =>
      value !== 0 && (Math.abs(value) >= 1e9 || Math.abs(value) < 0.01)
        ? value.toExponential(2)
        : number(value);
    const f = (value) => value.toFixed(2);
    let svg = begin;
    for (let i = 0; i <= 4; i++) {
      const xv = logX ? 10 ** xAxis.value(i / 4) : xAxis.value(i / 4),
        yv = yAxis.value(i / 4);
      svg += `<path d="M68 ${f(266 - (238 * i) / 4)}H616" stroke="#dce4eb"/><text x="60" y="${f(270 - (238 * i) / 4)}" fill="#536575" text-anchor="end">${escape(tick(yv))}</text>`;
      if (!xTicks)
        svg += `<text x="${f(68 + (548 * i) / 4)}" y="286" fill="#536575" text-anchor="middle">${escape(tick(xv))}</text>`;
    }
    if (xTicks)
      xTicks.forEach((label, i) => {
        if (i % Math.max(1, Math.ceil(xTicks.length / 10)) !== 0) return;
        svg += `<text x="${f(x(i))}" y="286" fill="#536575" text-anchor="middle">${escape(String(label).length > 18 ? String(label).slice(0, 16) + "…" : label)}</text>`;
      });
    svg += `<text x="342" y="310" fill="#34485b" text-anchor="middle">${escape(xLabel)}</text><text transform="translate(16 150) rotate(-90)" fill="#34485b" text-anchor="middle">${escape(yLabel)}</text>`;
    series.forEach((s, index) => {
      const color = colors[index % colors.length];
      let d = "",
        previous = null;
      const width = Math.max(
        2,
        Math.min(xTicks ? 18 : 32, 480 / Math.max(s.points.length, 1)),
      );
      for (const p of s.points) {
        if (!eligible(p)) {
          previous = null;
          continue;
        }
        const description = `${p.label || s.name}: ${number(p.x)} ${xLabel}, ${number(p.y)} ${yLabel}`;
        const pointColor = finite(p.colorValue)
          ? `hsl(${Math.max(0, Math.min(1, p.colorValue)) * 120},65%,35%)`
          : color;
        if (finite(p.error) && finite(p.y - p.error) && finite(p.y + p.error))
          svg += `<path d="M${f(x(p.x))} ${f(y(p.y - p.error))}V${f(y(p.y + p.error))}M${f(x(p.x) - 4)} ${f(y(p.y - p.error))}h8M${f(x(p.x) - 4)} ${f(y(p.y + p.error))}h8" fill="none" stroke="${color}"/>`;
        if (
          finite(p.xError) &&
          eligible({ x: p.x - p.xError, y: p.y }) &&
          eligible({ x: p.x + p.xError, y: p.y })
        )
          svg += `<path d="M${f(x(p.x - p.xError))} ${f(y(p.y))}H${f(x(p.x + p.xError))}" stroke="${color}"/>`;
        if (p.from && eligible(p.from)) {
          const ax = x(p.from.x),
            ay = y(p.from.y),
            bx = x(p.x),
            by = y(p.y),
            angle = Math.atan2(by - ay, bx - ax);
          svg += `<path d="M${f(ax)} ${f(ay)}L${f(bx)} ${f(by)}M${f(bx - 8 * Math.cos(angle - 0.4))} ${f(by - 8 * Math.sin(angle - 0.4))}L${f(bx)} ${f(by)}L${f(bx - 8 * Math.cos(angle + 0.4))} ${f(by - 8 * Math.sin(angle + 0.4))}" fill="none" stroke="${color}" opacity=".5"/>`;
        }
        if (mode === "bar") {
          svg += `<rect x="${f(x(p.x) - width / 2)}" y="${f(Math.min(y(0), y(p.y)))}" width="${f(width)}" height="${f(Math.abs(y(0) - y(p.y)))}" fill="${color}"><title>${escape(description)}</title></rect>`;
        } else if (mode !== "scatter") {
          d += previous
            ? mode === "step"
              ? `H${f(x(p.x))}V${f(y(p.y))}`
              : `L${f(x(p.x))} ${f(y(p.y))}`
            : `M${f(x(p.x))} ${f(y(p.y))}`;
        }
        svg += `<circle cx="${f(x(p.x))}" cy="${f(y(p.y))}" r="${mode === "scatter" ? 6 : 2}" fill="${pointColor}"${mode === "scatter" ? ` tabindex="0" aria-label="${escape(description)}"` : ""}><title>${escape(description)}</title></circle>`;
        previous = p;
      }
      if (d)
        svg += `<path d="${d}" fill="none" stroke="${color}" stroke-width="2"/>`;
    });
    series.forEach((s, i) => {
      svg += `<rect x="68" y="${326 + i * 20}" width="9" height="9" fill="${colors[i % colors.length]}"/><text x="84" y="${335 + i * 20}" fill="#34485b"><title>${escape(s.name)}</title>${escape(s.name.length > 76 ? s.name.slice(0, 73) + "…" : s.name)}</text>`;
    });
    return svg + foot + "</svg>";
  }

  function buildCharts(a) {
    validateResponse(a, a?.result?.id);
    const m = a.metrics;
    const source = `${a.result.name || a.result.id} · ${a.result.id} · ${timeLabel(a.asOf)}`;
    const latencyNote = `${number(m.latencySamples)} eligible first receipts; mean ${number(metricValue(m, "averageLatencyMs"))} ms, P95 ${number(metricValue(m, "p95LatencyMs"))} ms. ${m.definition || "Unknown definition"}. Windows: ${(m.deliveryWindows || []).join(", ") || "N/A"}. ${m.measurementIncomplete ? "Incomplete evidence." : ""}`;
    const charts = [
      {
        id: "latency-cdf",
        title: "First-delivery latency",
        xLabel: "Latency (ms)",
        yLabel: "Eligible samples (%)",
        mode: "step",
        yMax: 100,
        series: [
          {
            name: "Cumulative distribution",
            points: a.latencyCDF.length
              ? [
                  { x: 0, y: 0 },
                  ...a.latencyCDF.map((p) => ({ x: p.x, y: p.y * 100 })),
                ]
              : [],
          },
        ],
        note: latencyNote + " This is a latency CDF, not network reachability.",
      },
      {
        id: "latency-distribution",
        title: "Latency distribution",
        xLabel: "Latency (ms)",
        yLabel: "Sample count",
        mode: "bar",
        series: [
          { name: "First-delivery samples", points: a.latencyHistogram },
        ],
        note: latencyNote,
      },
      {
        id: "message-activity",
        title: "Message activity",
        xLabel: "Elapsed time (s)",
        yLabel: "Observed events / s",
        mode: "step",
        series: [
          ["publish", "Published"],
          ["deliver", "Delivered"],
          ["duplicate", "Duplicate copies"],
        ].map(([key, name]) => ({ name, points: trafficPoints(a, key) })),
        note: `Recorded event counts, including local and late receipts; ${a.binSeconds}-second bins. These counts are not delivery ratios.`,
      },
    ];
    const controls = m.gossipsubControl || [];
    if (controls.length) {
      const types = ["ihave", "iwant", "idontwant", "graft", "prune"];
      charts.push({
        id: "gossipsub-control",
        title: "GossipSub control traffic",
        xLabel: "Control type",
        yLabel: "Observed RPC occurrences",
        mode: "bar",
        xTicks: types.map((t) => t.toUpperCase()),
        series: ["send", "recv", "drop"].map((direction, i) => ({
          name: direction,
          points: types.map((type, x) => ({
            x: x + (i - 1) * 0.23,
            y: controls
              .filter(
                (c) => c.direction === direction && c.controlType === type,
              )
              .reduce((sum, c) => sum + (finite(c.rpcs) ? c.rpcs : 0), 0),
          })),
        })),
        note: "One RPC may contain several control types. Send and receive count separate endpoint observations; drop is a local discard.",
      });
    }
    if (a.bandwidthTimeline?.length)
      charts.push({
        id: "p2p-throughput",
        title: "P2P stream throughput",
        xLabel: "Elapsed time (s)",
        yLabel: "kbit/s",
        mode: "step",
        series: [
          { name: "Sent", points: bandwidthPoints(a, "*", "send") },
          { name: "Received", points: bandwidthPoints(a, "*", "recv") },
        ],
        note: "All libp2p streams; excludes IP/TCP framing, retransmissions and management traffic. Gaps have no bandwidth evidence.",
      });
    const groups = [
      ...new Set(a.observations.flatMap((o) => o.groups.map((g) => g.group))),
    ]
      .filter(Boolean)
      .sort();
    if (!groups.length) groups.push("");
    const byGroup = (read) =>
      groups
        .map((group) => ({
          name: group || "All peers",
          points: observationPoints(a, group, read),
        }))
        .filter((s) => s.points.some(pointOK));
    const scores = byGroup((g) => g.scoreMean);
    if (scores.length)
      charts.push({
        id: "peer-scores",
        title: "Peer scores by group",
        xLabel: "Elapsed time (s)",
        yLabel: "Mean reported score",
        series: scores,
        note: "Mean of observer-to-peer scores reported by fresh peers in each group. Missing samples are gaps; observers can score the same peer differently.",
      });
    const degrees = byGroup(
      (g) => g.layers.find((l) => l.protocol === "gossipsub")?.averageDegree,
    );
    if (degrees.length)
      charts.push({
        id: "mesh-degree",
        title: "GossipSub mesh degree",
        xLabel: "Elapsed time (s)",
        yLabel: "Mean unique neighbors",
        series: degrees,
        note: "Fresh reported mesh relationships, deduplicated across topics. Each group's degree includes neighbors in other groups.",
      });
    if (research) charts.push(...research.buildCharts(a, { bandwidthPoints }));
    const pair = charts.filter((c) =>
      ["research-propagationCDF", "research-duplicateCDF"].includes(c.id),
    );
    if (pair.length === 2)
      charts.push({
        id: "propagation-duplicate-panels",
        title: "Propagation and duplicate accumulation",
        panels: pair,
        series: [],
      });
    return charts.map((chart) => research.formatChart({ ...chart, source, category: chart.category || chart.protocol || (chart.id === "p2p-throughput" ? "common" : "gossipsub") }));
  }

  function validateResponse(data, id) {
    const record = (value) =>
      value !== null && typeof value === "object" && !Array.isArray(value);
    const list = (value, check) => Array.isArray(value) && value.every(check);
    const optionalList = (value, check) => value == null || list(value, check);
    const timed = (value) =>
      record(value) &&
      typeof value.at === "string" &&
      Number.isFinite(Date.parse(value.at));
    if (
      !id ||
      data?.version !== 1 ||
      data.result?.id !== id ||
      !record(data.metrics) ||
      !list(data.latencyCDF, pointOK) ||
      !list(data.latencyHistogram, pointOK) ||
      !list(data.timeline, timed) ||
      !optionalList(
        data.metrics.deliveryWindows,
        (v) => typeof v === "string",
      ) ||
      !optionalList(
        data.metrics.gossipsubControl,
        (c) => record(c) && typeof c.controlType === "string",
      ) ||
      !optionalList(
        data.bandwidthTimeline,
        (b) => timed(b) && finite(b.sentBytes) && finite(b.receivedBytes),
      ) ||
      !list(
        data.observations,
        (o) =>
          timed(o) &&
          list(
            o.groups,
            (g) =>
              record(g) &&
              typeof g.group === "string" &&
              list(
                g.layers,
                (l) => record(l) && typeof l.protocol === "string",
              ),
          ),
      )
    ) {
      throw new Error("This result contains invalid chart data.");
    }
  }

  // Render the same PNG used by the preview and download. Data URLs work with
  // the Dashboard's existing self/data image policy, without loosening CSP.
  function toPNG(svg, doc, signal) {
    return new Promise((resolve, reject) => {
      const image = new root.Image();
      const finish = (error, value) => {
        clearTimeout(timer);
        image.onload = image.onerror = null;
        signal?.removeEventListener("abort", abort);
        error ? reject(error) : resolve(value);
      };
      const timer = setTimeout(
        () =>
          finish(
            new Error(
              "PNG conversion timed out. The saved analysis is still available; please retry.",
            ),
          ),
        30000,
      );
      const abort = () => {
        image.onload = image.onerror = null;
        image.src = "";
        finish(new DOMException("Image generation canceled", "AbortError"));
      };
      if (signal?.aborted) {
        abort();
        return;
      }
      signal?.addEventListener("abort", abort, { once: true });
      image.onerror = () =>
        finish(new Error("Unable to create a graph image. Please retry."));
      image.onload = () => {
        try {
          const canvas = doc.createElement("canvas");
          canvas.width = 1600;
          canvas.height = Math.round((1600 * image.height) / image.width);
          const context = canvas.getContext("2d");
          if (!context)
            throw new Error("This browser cannot create PNG images.");
          context.drawImage(image, 0, 0, canvas.width, canvas.height);
          finish(null, canvas.toDataURL("image/png"));
        } catch (error) {
          finish(error);
        }
      };
      image.src = "data:image/svg+xml;charset=utf-8," + encodeURIComponent(svg);
    });
  }

  function treeSVG(chart) {
    const message = chart.tree,
      nodes = [
        { id: message.publisher, hop: 0, source: "publisher" },
        ...(message.nodes || []),
      ];
    const maxHop = nodes.reduce(
        (m, n) => (finite(n.hop) ? Math.max(m, n.hop) : m),
        0,
      ),
      columns = new Map(),
      positions = new Map();
    for (const node of nodes) {
      const hop = finite(node.hop) ? node.hop : maxHop + 1;
      if (!columns.has(hop)) columns.set(hop, []);
      columns.get(hop).push(node);
    }
    for (const [hop, list] of columns)
      list.forEach((n, i) =>
        positions.set(n.id, {
          x: 40 + (560 * hop) / Math.max(maxHop + 1, 1),
          y: 70 + (370 * (i + 0.5)) / list.length,
        }),
      );
    let svg = `<svg xmlns="http://www.w3.org/2000/svg" width="640" height="520" viewBox="0 0 640 520"><rect width="640" height="520" fill="white"/><g font-family="system-ui,sans-serif" fill="#142638"><text x="24" y="26">${escape(chart.title)}</text><text x="24" y="49" font-size="10">${escape(message.id)}</text>`;
    for (const n of nodes) {
      const p = positions.get(n.id),
        parent = positions.get(n.parent);
      if (parent)
        svg += `<path d="M${parent.x} ${parent.y}L${p.x} ${p.y}" stroke="#a7b4c2"/>`;
    }
    for (const n of nodes) {
      const p = positions.get(n.id),
        color =
          {
            eager: "#2563eb",
            lazy: "#c45c10",
            unknown: "#64748b",
            publisher: "#111827",
          }[n.source] || "#64748b";
      svg += `<circle cx="${p.x}" cy="${p.y}" r="4" fill="${color}"><title>${escape(n.id)} · hop ${n.hop ?? "unknown"} · ${escape(n.source)}</title></circle>`;
      if (nodes.length <= 50)
        svg += `<text x="${p.x + 5}" y="${p.y - 5}" font-size="8">${escape(n.id.slice(0, 12))}</text>`;
    }
    svg += `<text x="24" y="477" font-size="10">Estimated source: blue eager, orange lazy, gray unclassified. Black: publisher. Last column: unresolved hop.</text><text x="24" y="498" font-size="9">${escape(String(chart.source || "").slice(0, 108))}</text></g></svg>`;
    return svg;
  }
  const pendingJob = (job) => job && ["queued", "running"].includes(job.state);
  function jobDescription(job) {
    if (job.state === "queued")
      return "Queued — waiting for an analysis worker.";
    if (job.state === "completed")
      return "Analysis complete. Preparing PNG downloads…";
    const phase =
      {
        starting: "Opening saved logs",
        "events.jsonl": "Reading event log",
        "observations.jsonl": "Reading topology and score history",
        aggregating: "Calculating metrics and chart data",
        saving: "Saving analysis result",
      }[job.phase] ||
      job.phase ||
      "Analyzing";
    const mb = (bytes) => number(bytes / 1048576);
    return `${phase} · ${mb(job.processedBytes)} / ${mb(job.totalBytes)} MiB read`;
  }

  const metricTypes = {
    population: "Nodes & Peers",
    topology: "Topology",
    delivery: "Delivery & Propagation",
    control: "Control Traffic",
    scores: "Peer Scores",
    bandwidth: "Bandwidth",
    comparison: "Comparisons & Models",
  };
  function metricType(chart) {
    const id = chart.id;
    if (["graph-node_count", "peer-lifecycle", "observers-reporting"].includes(id)) return "population";
    if (/bandwidth|throughput/.test(id)) return "bandwidth";
    if (id === "peer-scores" || id.startsWith("observers-")) return "scores";
    if (id.startsWith("control-") || ["gossipsub-control", "mesh-transitions"].includes(id)) return "control";
    if (id.startsWith("graph-") || id.startsWith("degree-") || id === "mesh-degree") return "topology";
    return chart.category || chart.protocol ? "delivery" : "comparison";
  }
  function filterMetricGroups(groups, { category = "common", query = "", type = "all", sort = "default" } = {}) {
    const normalize = value => String(value).toLocaleLowerCase("en-US").replace(/[_-]/g, " ");
    const terms = normalize(query).trim().split(/\s+/).filter(Boolean);
    const filtered = groups.flatMap((group, index) => {
      const chart = group.charts.find(chart => (chart.category || chart.protocol || "common") === category);
      if (!chart) return [];
      const kind = metricType(chart);
      const text = normalize([group.title, chart.title, chart.id, metricTypes[kind], chart.xLabel, chart.yLabel].join(" "));
      if ((type !== "all" && type !== kind) || !terms.every(term => text.includes(term))) return [];
      return [{ ...group, index, chart, type: kind }];
    });
    if (sort === "asc" || sort === "desc") filtered.sort((a, b) =>
      (sort === "asc" ? 1 : -1) * a.title.localeCompare(b.title, "en", { numeric: true, sensitivity: "base" }));
    return filtered;
  }

  function groupCharts(charts) {
    const groups = new Map();
    for (const chart of charts) {
      const id = chart.groupId || chart.id;
      if (!groups.has(id)) groups.set(id, { id, title: chart.groupTitle || chart.title, charts: [] });
      groups.get(id).charts.push(chart);
    }
    return [...groups.values()];
  }

  function createUI({
    api,
    document: doc = root.document,
    renderImage = (svg, signal) => toPNG(svg, doc, signal),
    onJob = () => {},
    pollInterval = 2000,
  }) {
    const $ = (id) => doc.querySelector(`#${id}`);
    const dialog = $("resultImagesDialog");
    let currentID = "",
      currentBatch = false,
      controller = null,
      revision = 0,
      currentData = null,
      objectURLs = [];
    let researchTools = null;
    let imageGroups = [], imageAssets = new Map(), selectedProtocol = "common", imageFailure = "";
    const protocolControls = $("resultImageProtocols");
    const imageList = $("resultImagesGrid");
    const expandedGroups = new Set();
    const searchInput = $("resultMetricSearch"), typeInput = $("resultMetricType"), sortInput = $("resultMetricSort");
    typeInput.innerHTML = '<option value="all">All Types</option>' + Object.entries(metricTypes).map(([id, title]) => `<option value="${id}">${escape(title)}</option>`).join("");
    function resetMetricFilters() {
      searchInput.value = "";
      typeInput.value = "all";
      sortInput.value = "default";
    }
    resetMetricFilters();
    function status(message, error = false) {
      $("resultImagesStatus").textContent = message;
      $("resultImagesStatus").classList.toggle("error", error);
      $("resultImagesStatus").setAttribute("role", error ? "alert" : "status");
      $("retryResultImages").hidden = !error;
    }
    // Detach the browser only: the accepted server job continues after closing.
    function clearImages() {
      imageList.innerHTML = "";
      imageGroups = [];
      expandedGroups.clear();
      $("resultMetricTools").hidden = true;
      imageAssets.clear();
      imageFailure = "";
      protocolControls.hidden = true;
      for (const url of objectURLs) root.URL.revokeObjectURL(url);
      objectURLs = [];
      if ($("downloadAllResultImages"))
        $("downloadAllResultImages").hidden = true;
    }
    function cancel() {
      revision++;
      controller?.abort();
      controller = null;
      researchTools?.cancel();
      clearImages();
    }
    function blobURL(blob) {
      const url = root.URL.createObjectURL(blob);
      objectURLs.push(url);
      return url;
    }
    function selectedChart(group) {
      return group.charts.find(chart => (chart.category || chart.protocol || "common") === selectedProtocol);
    }
    function updateImage(details) {
      const group = imageGroups[Number(details.dataset.imageIndex)];
      if (!group) return;
      const body = details.querySelector(".result-image-body");
      if (!details.open || details.hidden) {
        body.innerHTML = "";
        delete body.dataset.imageAsset;
        return;
      }
      const chart = selectedChart(group), asset = chart && imageAssets.get(chart.id);
      if (!asset) {
        delete body.dataset.imageAsset;
        const message = !chart ? "No image is available in this category."
          : imageFailure ? "This image could not be prepared. Use Retry to try again."
          : `Preparing ${chart.title}…`;
        body.innerHTML = `<p class="result-image-placeholder">${escape(message)}</p>`;
        return;
      }
      if (body.dataset.imageAsset === chart.id) return;
      body.innerHTML = `<figure><a href="${asset.pngURL}" download="${escape(asset.filename)}.png" aria-label="${escape(`Download ${chart.title} as PNG`)}"><img src="${asset.pngURL}" alt="${escape(chart.title)}" loading="lazy"></a><figcaption><span>${escape(chart.title)}</span><a href="${asset.pngURL}" download="${escape(asset.filename)}.png">PNG ↓</a>${asset.csvURL ? `<a href="${asset.csvURL}" download="${escape(asset.filename)}.csv">CSV ↓</a>` : ""}</figcaption></figure>`;
      body.dataset.imageAsset = chart.id;
    }
    function renderMetricList() {
      for (const details of imageList.querySelectorAll("details[data-image-index]")) {
        const id = imageGroups[Number(details.dataset.imageIndex)]?.id;
        if (id) { if (details.open) expandedGroups.add(id); else expandedGroups.delete(id); }
      }
      const filters = { category: selectedProtocol, query: searchInput.value, type: typeInput.value, sort: sortInput.value };
      const visible = filterMetricGroups(imageGroups, filters);
      const total = filterMetricGroups(imageGroups, { category: selectedProtocol }).length;
      $("resultMetricCount").textContent = `${visible.length} of ${total} metrics`;
      $("clearResultMetricFilters").hidden = !searchInput.value && typeInput.value === "all";
      const sections = Object.entries(metricTypes).map(([type, title]) => {
        const groups = visible.filter(group => group.type === type);
        if (!groups.length) return "";
        return `<section class="result-metric-group" aria-labelledby="metric-type-${type}"><h3 id="metric-type-${type}">${escape(title)}<span>${groups.length}</span></h3><div class="result-metric-items">${groups.map(group => `<details class="result-image" data-image-group="${escape(group.id)}" data-image-index="${group.index}"${expandedGroups.has(group.id) ? " open" : ""}>
          <summary><svg viewBox="0 0 20 20" aria-hidden="true"><path d="m7 5 5 5-5 5"/></svg><span>${escape(group.title)}</span></summary>
          <div class="result-image-body"></div>
        </details>`).join("")}</div></section>`;
      }).join("");
      imageList.innerHTML = sections || '<p class="result-metrics-empty">No matching metrics. Try another name or clear the filters.</p>';
      for (const details of imageList.querySelectorAll("details[open]")) updateImage(details);
    }
    function showImageList(charts) {
      imageGroups = groupCharts(charts);
      expandedGroups.clear();
      imageList.innerHTML = "";
      resetMetricFilters();
      const categories = new Set(charts.map(chart => chart.category || chart.protocol || "common"));
      if (!categories.has(selectedProtocol)) selectedProtocol = Object.keys(research.categoryLabels).find(category => categories.has(category)) || "common";
      protocolControls.hidden = categories.size < 2;
      $("resultMetricTools").hidden = false;
      for (const input of protocolControls.querySelectorAll('input[name="resultImageProtocol"]')) {
        input.checked = input.value === selectedProtocol;
        input.disabled = !categories.has(input.value);
      }
      renderMetricList();
    }
    // Disclosure state belongs to the metric, not its filtered DOM position.
    imageList.addEventListener("toggle", event => {
      const details = event.target;
      if (!details.matches("details[data-image-index]")) return;
      const id = imageGroups[Number(details.dataset.imageIndex)]?.id;
      if (id) { if (details.open) expandedGroups.add(id); else expandedGroups.delete(id); }
      updateImage(details);
    }, true);
    protocolControls.addEventListener("change", event => {
      const input = event.target;
      if (input.name !== "resultImageProtocol" || !input.checked || input.disabled || !Object.hasOwn(research.categoryLabels, input.value)) return;
      selectedProtocol = input.value;
      renderMetricList();
    });
    searchInput.addEventListener("input", renderMetricList);
    typeInput.addEventListener("change", renderMetricList);
    sortInput.addEventListener("change", renderMetricList);
    $("clearResultMetricFilters").addEventListener("click", () => {
      searchInput.value = "";
      typeInput.value = "all";
      renderMetricList();
      searchInput.focus();
    });
    async function prepareImages(charts, id, view, requestRevision, extra) {
      const bundle = [];
      charts = charts.map(research.formatChart);
      showImageList(charts);
      try {
        for (const [index, chart] of charts.entries()) {
          if (view.signal.aborted) throw new DOMException("Closed", "AbortError");
          status(`Preparing PNG ${index + 1} / ${charts.length} · ${chart.title}`);
          const png = await renderImage(chartSVG(chart), view.signal);
          if (revision !== requestRevision) return;
          if (!png.startsWith("data:image/png;base64,")) throw new Error("Unable to create a PNG image.");
          const filename = `${id}-${chart.id}`;
          const bytes = Uint8Array.from(root.atob(png.split(",")[1]), c => c.charCodeAt(0));
          const pngURL = png; // Keep previews compatible with the dashboard data: image policy.
          const csv = files?.chartCSV(chart);
          const csvURL = csv ? blobURL(new Blob([csv], { type: "text/csv;charset=utf-8" })) : null;
          imageAssets.set(chart.id, { filename, pngURL, csvURL });
          for (const details of imageList.querySelectorAll("details[open]")) {
            if (selectedChart(imageGroups[Number(details.dataset.imageIndex)])?.id === chart.id) updateImage(details);
          }
          if (files) bundle.push({ name: `${filename}.png`, data: bytes }, { name: `${filename}.csv`, data: csv });
        }
        if (files && bundle.length && $("downloadAllResultImages")) {
          if (extra?.summary) bundle.push({ name: `${id}-summary.csv`, data: batch.summaryCSV(extra.summary) });
          bundle.push({ name: `${id}-chart-definitions.json`, data: JSON.stringify({ charts, ...(extra || {}) }) });
          const link = $("downloadAllResultImages");
          link.href = blobURL(files.zip(bundle));
          link.download = `${id}-images.zip`;
          link.hidden = false;
        }
        status(`${imageGroups.length} images ready. Expand a title to preview and download PNG / CSV. N/A indicates missing evidence or undefined statistics.`);
      } catch (error) {
        if (revision === requestRevision) {
          imageFailure = error.message;
          for (const details of imageList.querySelectorAll("details[open]")) updateImage(details);
        }
        throw error;
      }
    }
    async function renderTools(charts, id, extra) {
      revision++;
      controller?.abort();
      clearImages();
      const requestRevision = revision,
        view = new AbortController();
      controller = view;
      dialog.setAttribute("aria-busy", "true");
      try {
        await prepareImages(charts, id, view, requestRevision, extra);
      } catch (e) {
        if (revision === requestRevision) status(e.message, true);
      } finally {
        if (revision === requestRevision) {
          controller = null;
          dialog.setAttribute("aria-busy", "false");
        }
      }
    }
    researchTools = root.KPLResearchTools?.create({
      document: doc,
      request,
      render: renderTools,
      status,
      onJob,
      getData: () => currentData,
      escape,
    });
    function delay(signal) {
      return new Promise((resolve, reject) => {
        const done = () => {
          signal.removeEventListener("abort", abort);
          resolve();
        };
        const timer = setTimeout(done, pollInterval);
        const abort = () => {
          clearTimeout(timer);
          signal.removeEventListener("abort", abort);
          reject(new DOMException("Closed", "AbortError"));
        };
        if (signal.aborted) abort();
        else signal.addEventListener("abort", abort, { once: true });
      });
    }
    async function request(path, options, signal) {
      const bounded = new AbortController();
      const abort = () => bounded.abort();
      signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) bounded.abort();
      const timer = setTimeout(abort, 30000);
      try {
        return await api(path, {
          ...options,
          cache: "no-store",
          signal: bounded.signal,
        });
      } finally {
        clearTimeout(timer);
        signal.removeEventListener("abort", abort);
      }
    }
    async function open(id, { refresh = false, retry = false, isBatch = false } = {}) {
      if (!id || (currentID === id && currentBatch === isBatch && controller)) return;
      if (!dialog.open || currentID !== id || currentBatch !== isBatch) selectedProtocol = "common";
      cancel();
      currentID = id;
      currentBatch = isBatch;
      if ($("batchAnalysisSummary")) $("batchAnalysisSummary").hidden = true;
      currentData = null;
      if ($("researchTools")) $("researchTools").hidden = true;
      const requestRevision = revision,
        view = new AbortController();
      controller = view;
      const path = `/api/v1/${isBatch ? "batch-analysis-jobs" : "analysis-jobs"}/${encodeURIComponent(id)}`;
      $("resultImagesName").textContent = id;
      $("resultImagesDate").textContent = "";
      $("resultImagesProgress").hidden = true;
      $("downloadResultAnalysis").hidden = true;
      $("refreshResultImages").hidden = true;
      status("Checking analysis job…");
      dialog.setAttribute("aria-busy", "true");
      if (!dialog.open) dialog.showModal();
      try {
        let job = await request(path, {}, view.signal);
        if (
          refresh ||
          job.state === "idle" ||
          (job.state === "completed" && (job.analysisVersion || 0) < 3) ||
          (retry && ["failed", "interrupted", "canceled"].includes(job.state))
        ) {
          job = await request(
            path + (refresh ? "?refresh=1" : ""),
            { method: "POST" },
            view.signal,
          );
        }
        while (true) {
          if (revision !== requestRevision) return;
          if (
            (isBatch ? job.batchId : job.runId) !== id ||
            ![
              "queued",
              "running",
              "completed",
              "failed",
              "interrupted",
              "canceled",
            ].includes(job.state)
          )
            throw new Error("Unexpected analysis job response.");
          onJob(job);
          $("resultImagesDate").textContent =
            `Requested ${timeLabel(job.createdAt)} · Snapshot ${timeLabel(job.snapshotAt)}`;
          status(isBatch && pendingJob(job) ? `${job.completedRuns || 0}/${job.totalRuns} runs complete · ${jobDescription(job)}` : jobDescription(job));
          const progress = $("resultImagesProgress");
          progress.hidden = !pendingJob(job);
          if (
            job.state === "running" &&
            (isBatch || ["events.jsonl", "observations.jsonl"].includes(job.phase) && job.totalBytes > 0)
          )
            progress.value = Math.max(0, Math.min(100, job.progress));
          else progress.removeAttribute("value");
          if (!pendingJob(job)) break;
          await delay(view.signal);
          job = await request(path, {}, view.signal);
        }
        if (job.state !== "completed")
          throw new Error(
            job.error || `Analysis ${job.state}. Retry to start again.`,
          );
        // Construct the same-origin URL locally; never trust a stored URL as a credential destination.
        const resultPath = `${path}/result?jobId=${encodeURIComponent(job.id)}`;
        const download = $("downloadResultAnalysis");
        download.href = resultPath;
        download.download = `${id}${isBatch ? "-batch" : ""}-analysis.json`;
        download.hidden = false;
        $("refreshResultImages").hidden = false;
        const data = await request(resultPath, {}, view.signal);
        if (isBatch) batch.validate(data, id); else validateResponse(data, id);
        if (data.analysisId !== job.id)
          throw new Error("The saved analysis changed. Reopen this result.");
        if (revision !== requestRevision) return;
        if (isBatch) {
          $("resultImagesName").textContent = `${data.name || id} · Batch mean`;
          $("resultImagesDate").textContent = `${batch.description(data)} · Computed ${timeLabel(data.asOf)}`;
          const summary = $("batchAnalysisSummary");
          if (summary) {
            summary.hidden = false;
            summary.innerHTML = `<summary>Mean metrics and contributing run counts</summary><p class="dialog-help">Equal run weight. Mean, between-run sample SD, and contributing runs (n). Missing evidence is excluded. P95 is the mean of each run's P95.</p><div class="table-wrap"><table><thead><tr><th>Metric</th><th>Mean</th><th>Sample SD</th><th>n / runs</th></tr></thead>${batch.summaryGroups(data.summary).map(group => `<tbody><tr class="metric-category-row"><th colspan="4" scope="rowgroup">${escape(research.categoryLabels[group.category])}</th></tr>${group.rows.map(([key, stat]) => `<tr><th scope="row">${escape(batch.label(key))}</th><td>${number(stat.average)}</td><td>${number(stat.deviation)}</td><td>${number(stat.count)} / ${data.runs.length}</td></tr>`).join("")}</tbody>`).join("")}</table></div>`;
          }
          await prepareImages(batch.build(data, buildCharts), `${id}-batch-mean`, view, requestRevision, { summary: data.summary, batchId: id, aggregation: data.aggregation, includedRunIds: data.runs.map(a => a.result.id), excluded: data.excluded, missingRuns: data.missingRuns });
        } else {
          $("resultImagesName").textContent = data.result.name || id;
          $("resultImagesDate").textContent = `${data.result.state} · Snapshot ${timeLabel(data.asOf)}`;
          currentData = data;
          researchTools?.setData(data);
          await prepareImages(buildCharts(data), id, view, requestRevision);
        }
      } catch (error) {
        if (revision === requestRevision) {
          status(
            error.name === "AbortError"
              ? "Status request timed out. The server job continues; retry to reconnect."
              : error.message,
            true,
          );
        }
      } finally {
        if (revision === requestRevision) {
          controller = null;
          dialog.setAttribute("aria-busy", "false");
        }
      }
    }
    $("closeResultImages").addEventListener("click", () => dialog.close());
    dialog.addEventListener("close", cancel);
    $("retryResultImages").addEventListener("click", () => {
      void open(currentID, { retry: true, isBatch: currentBatch });
    });
    $("refreshResultImages").addEventListener("click", () => {
      void open(currentID, { refresh: true, isBatch: currentBatch });
    });
    return {
      open,
      openBatch: (id) => open(id, { isBatch: true }),
      remove: (id) => {
        if (currentID === id) {
          cancel();
          currentID = "";
          dialog.close();
        }
      },
    };
  }
  let ui;
  const exported = {
    buildCharts,
    groupCharts,
    filterMetricGroups,
    metricTypes,
    chartSVG,
    metricValue,
    trafficPoints,
    bandwidthPoints,
    observationPoints,
    validateResponse,
    createUI,
    jobDescription,
    pendingJob,
    init: (options) => {
      ui = createUI(options);
    },
    open: (id) => ui?.open(id),
    openBatch: (id) => ui?.openBatch(id),
    remove: (id) => ui?.remove(id),
  };
  if (typeof module !== "undefined" && module.exports)
    module.exports = exported;
  else root.KPLResultImages = exported;
})(globalThis);

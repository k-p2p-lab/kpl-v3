(function (root) {
  "use strict";
  const R =
    typeof module !== "undefined" && module.exports
      ? require("./research-charts.js")
      : root.KPLResearch;
  const finite = (v) => typeof v === "number" && Number.isFinite(v),
    num = (v) =>
      (typeof v === "number" || typeof v === "string") &&
      String(v).trim() !== "" &&
      Number.isFinite(Number(v))
        ? Number(v)
        : null;
  function csvRows(text) {
    const rows = [];
    let row = [],
      cell = "",
      quoted = false;
    for (let i = 0; i < text.length; i++) {
      const c = text[i];
      if (c === '"') {
        if (quoted && text[i + 1] === '"') {
          cell += '"';
          i++;
        } else quoted = !quoted;
      } else if (!quoted && (c === "," || c === "\n" || c === "\r")) {
        row.push(cell);
        cell = "";
        if (c !== ",") {
          if (row.some((v) => v !== "")) rows.push(row);
          row = [];
          if (c === "\r" && text[i + 1] === "\n") i++;
        }
      } else cell += c;
    }
    if (quoted) throw new Error("Unclosed quoted CSV field.");
    if (cell || row.length) {
      row.push(cell);
      rows.push(row);
    }
    return rows;
  }
  function parseImport(text, name, key = "frt") {
    if (text.length > 32 * 1024 * 1024)
      throw new Error(
        "Import files up to 32 MiB each; use saved-result jobs for larger logs.",
      );
    const entries = [],
      curves = [],
      analysis = (id) => ({
        version: 1,
        result: { id, name: id, state: "imported" },
        metrics: {},
        research: {
          definition: "v2-imported-values",
          summary: {},
          messages: [],
          degreeDistribution: [],
        },
      });
    const add = (a, label) =>
      entries.push({ analysis: a, case: label, series: name, params: {} });
    if (name.toLowerCase().endsWith(".csv")) {
      const rows = csvRows(text),
        header = rows.shift() || [],
        normalized = header.map((v) => v.trim().toLowerCase());
      let xi = normalized.findIndex((v) =>
          [
            "x",
            "time",
            "time_s",
            "hop",
            "degree",
            "t",
            "seconds",
            "round",
          ].includes(v),
        ),
        yi = normalized.findIndex((v) =>
          [
            "y",
            "pdf",
            "cdf",
            "mean",
            "avg",
            "count",
            "cum_count",
            "mean_cumulative_count",
            "cum_receivers_mean",
            "probability",
            "value",
          ].includes(v),
        );
      if (xi < 0) xi = 0;
      if (yi < 0 || yi === xi) yi = xi === 0 ? 1 : 0;
      const labelIndex = normalized.findIndex((v) =>
          ["series", "label", "folder", "case", "file"].includes(v),
        ),
        grouped = new Map();
      for (const row of rows) {
        const label = labelIndex >= 0 ? row[labelIndex] : name;
        if (!grouped.has(label)) grouped.set(label, []);
        const x = num(row[xi]),
          y = num(row[yi]);
        if (x === null || y === null)
          throw new Error(`Non-numeric CSV value in ${name}.`);
        grouped.get(label).push({ x, y });
      }
      for (const [label, points] of grouped)
        curves.push({
          name: label,
          xLabel: header[xi] || "x",
          yLabel: header[yi] || "y",
          points: points.sort((a, b) => a.x - b.x),
        });
      return { entries, curves };
    }
    let objects;
    try {
      objects = [JSON.parse(text)];
    } catch {
      objects = text
        .split(/\r?\n/)
        .filter((s) => s.trim())
        .map((line, i) => {
          try {
            return JSON.parse(line);
          } catch {
            throw new Error(`Invalid JSON on line ${i + 1} of ${name}.`);
          }
        });
    }
    if (
      objects.length === 1 &&
      objects[0]?.version === 1 &&
      objects[0]?.research &&
      objects[0]?.result
    ) {
      add(objects[0], objects[0].result.name || name);
      return { entries, curves };
    }
    const xyRecords = objects.filter((object) =>
      object &&
      Array.isArray(object.y) &&
      (Array.isArray(object.x_case) || Array.isArray(object.x)),
    );
    if (xyRecords.length) {
      if (xyRecords.length !== objects.length)
        throw new Error("Do not mix x/y series with other JSON records in one file.");
      for (const [index, object] of xyRecords.entries()) {
        const sourceName = xyRecords.length === 1 ? name : `${name} / record ${index + 1}`;
        const xs = object.x_case || object.x;
        if (xs.length !== object.y.length)
          throw new Error("v2 x/y arrays have different lengths.");
        const points = [];
        for (let i = 0; i < xs.length; i++) {
          const y = num(object.y[i]),
            sd = num(object.yerr?.[i]);
          if (y === null)
            throw new Error("v2 y values must be finite numbers.");
          const a = analysis(`${sourceName}:${xs[i]}`);
          a.research.summary[key] = { average: y };
          a.importedSummaryStats = {
            [key]: { mean: y, sd: sd === null ? null : Math.abs(sd), n: null },
          };
          add(a, String(xs[i]));
          if (num(xs[i]) !== null) points.push({ x: num(xs[i]), y, error: sd });
        }
        if (points.length)
          curves.push({ name: sourceName, xLabel: "x", yLabel: key, points });
      }
      return { entries, curves };
    }
    const a = analysis(name),
      samples = new Map(),
      messages = [],
      addSample = (key, v) => {
        if (finite(v)) {
          if (!samples.has(key)) samples.set(key, []);
          samples.get(key).push(v);
        }
      };
    for (const object of objects) {
      if (!object || typeof object !== "object")
        throw new Error("Expected a v2 JSON object.");
      const legacyDegrees = [];
      const records =
        object.message_id && (object.root || object.tree)
          ? [[String(object.message_id), object.root || object.tree]]
          : Object.entries(object);
      for (const [id, record] of records) {
        if (!record || typeof record !== "object") continue;
        if (
          /dup/i.test(name) &&
          Object.keys(record).length &&
          Object.entries(record).every(
            ([x, y]) => num(x) !== null && finite(y) && y >= 0,
          )
        ) {
          let total = 0;
          const points = Object.entries(record)
            .map(([x, y]) => ({ x: num(x), y }))
            .sort((a, b) => a.x - b.x)
            .map((p) => ({ x: p.x, y: (total += p.y) }));
          curves.push({
            name: `${name} / ${id}`,
            xLabel: "Time since publication (s)",
            yLabel: "Cumulative duplicate copies",
            points,
          });
          continue;
        }
        const tree = record.root || record.tree || record;
        if (tree.id !== undefined && Array.isArray(tree.children)) {
          const nodes = [],
            queue = [{ node: tree, parent: "", hop: 0 }],
            byID = new Map();
          for (let i = 0; i < queue.length; i++) {
            const { node, parent, hop } = queue[i],
              seconds = num(node.time ?? node.t);
            if (seconds === null)
              throw new Error("Propagation node has no finite time.");
            const item = {
              id: String(node.id),
              parent,
              hop,
              seconds,
              source: "unknown",
            };
            if (!byID.has(item.id) || seconds < byID.get(item.id).seconds)
              byID.set(item.id, item);
            for (const child of node.children || [])
              queue.push({ node: child, parent: item.id, hop: hop + 1 });
          }
          const origin = num(tree.time ?? tree.t);
          for (const node of byID.values())
            if (node.id !== String(tree.id))
              nodes.push({ ...node, seconds: node.seconds - origin });
          messages.push({
            id,
            topic: "v2",
            publisher: String(tree.id),
            nodes,
            metrics: {},
            duplicateTimeline: [],
          });
          continue;
        }
        for (const [metric, stat] of Object.entries(record)) {
          if (stat && typeof stat === "object" && finite(stat.average))
            addSample(metric, stat.average);
        }
        const degree = record.degree;
        if (degree) {
          const total = num(degree.Total ?? degree.total),
            count = num(degree.Count ?? degree.count);
          if (finite(total) && count > 0) legacyDegrees.push(total / count);
        }
      }
      if (legacyDegrees.length)
        addSample("average_degree", R.stats(legacyDegrees).mean);
    }
    for (const [key, vs] of samples)
      a.research.summary[key] = { average: R.stats(vs).mean };
    for (const [key, stat] of Object.entries(a.research.summary))
      if (key.startsWith("degree_distribution-")) {
        const x = num(key.slice("degree_distribution-".length));
        if (x !== null)
          a.research.degreeDistribution.push({ x, y: stat.average });
      }
    a.research.degreeDistribution.sort((a, b) => a.x - b.x);
    if (messages.length) {
      a.research.messages = messages;
      a.research.messageCount = messages.length;
      const times = messages.flatMap((m) => m.nodes.map((n) => n.seconds)),
        hops = messages.flatMap((m) => m.nodes.map((n) => n.hop));
      a.research.propagationCDF = R.cumulative(times);
      a.research.hopPDF = R.pdf(hops);
      a.research.hopCDF = R.cumulative(hops);
      a.research.populationBasis =
        "Imported v2 trees: conditional on observed receivers; no membership denominator";
    }
    if (!samples.size && !messages.length && !curves.length)
      throw new Error(
        "No supported v2 metric, propagation tree, or x/y series found.",
      );
    if (samples.size || messages.length) add(a, name);
    return { entries, curves };
  }
  function curveCharts(curves) {
    return [false, true].map((peak) => ({
      id: `imported-curves-${peak ? "peak" : "raw"}`,
      title: peak
        ? "Imported curves · peak normalized"
        : "Imported curve overlay",
      xLabel: curves[0]?.xLabel || "x",
      yLabel: peak ? "Relative peak" : curves[0]?.yLabel || "y",
      series: curves.map((c) => {
        const max = c.points.reduce((m, p) => Math.max(m, p.y), 0);
        return {
          name: c.name,
          points: peak
            ? c.points.map((p) => ({
                ...p,
                y: max > 0 ? p.y / max : null,
                ...(finite(p.error)
                  ? { error: max > 0 ? Math.abs(p.error) / max : null }
                  : {}),
              }))
            : c.points,
        };
      }),
      note: peak
        ? "Each curve divided by its own positive maximum; original probability and count units are lost."
        : "Original imported points. Files with different units must be compared separately.",
    }));
  }
  function chartCSV(chart) {
    const quote = (v) => '"' + String(v ?? "").replace(/"/g, '""') + '"',
      rows = [
        ["panel", "series", "x", "y", "y_sd", "x_sd", "color_value", "label", "n"],
      ];
    for (const panel of chart.panels || [chart])
      for (const s of panel.series || [])
        for (const p of s.points || [])
          rows.push([
            panel.title,
            s.name,
            p.x,
            p.y,
            p.error,
            p.xError,
            p.colorValue,
            p.label,
            p.n,
          ]);
    if (chart.tree) {
      rows.push([
        "node_id",
        "parent",
        "time_s",
        "hop",
        "estimated_path_source",
        "estimated_link_source",
        "evidence",
      ]);
      for (const n of chart.tree.nodes || [])
        rows.push([
          n.id,
          n.parent,
          n.seconds,
          n.hop,
          n.source,
          n.linkEstimate,
          (n.evidence || []).join(" | "),
        ]);
    }
    return rows.map((row) => row.map(quote).join(",")).join("\r\n") + "\r\n";
  }
  function zip(files) {
    const encoder = new TextEncoder(),
      parts = [],
      central = [];
    let offset = 0;
    const crc = (data) => {
      let c = 0xffffffff;
      for (const byte of data) {
        c ^= byte;
        for (let j = 0; j < 8; j++) c = (c >>> 1) ^ (c & 1 ? 0xedb88320 : 0);
      }
      return (c ^ 0xffffffff) >>> 0;
    };
    const header = (length, values) => {
      const bytes = new Uint8Array(length),
        view = new DataView(bytes.buffer);
      for (const [at, n, size] of values)
        (size === 2 ? view.setUint16.bind(view) : view.setUint32.bind(view))(
          at,
          n,
          true,
        );
      return bytes;
    };
    for (const file of files) {
      const name = encoder.encode(file.name),
        data =
          typeof file.data === "string" ? encoder.encode(file.data) : file.data,
        checksum = crc(data),
        local = header(30, [
          [0, 0x04034b50, 4],
          [4, 20, 2],
          [6, 0x800, 2],
          [14, checksum, 4],
          [18, data.length, 4],
          [22, data.length, 4],
          [26, name.length, 2],
        ]);
      parts.push(local, name, data);
      const record = header(46, [
        [0, 0x02014b50, 4],
        [4, 20, 2],
        [6, 20, 2],
        [8, 0x800, 2],
        [16, checksum, 4],
        [20, data.length, 4],
        [24, data.length, 4],
        [28, name.length, 2],
        [42, offset, 4],
      ]);
      central.push(record, name);
      offset += local.length + name.length + data.length;
    }
    const centralSize = central.reduce((sum, b) => sum + b.length, 0),
      end = header(22, [
        [0, 0x06054b50, 4],
        [8, files.length, 2],
        [10, files.length, 2],
        [12, centralSize, 4],
        [16, offset, 4],
      ]);
    return new Blob([...parts, ...central, end], { type: "application/zip" });
  }
  const exported = { csvRows, parseImport, curveCharts, chartCSV, zip };
  if (typeof module !== "undefined" && module.exports)
    module.exports = exported;
  else root.KPLResearchFiles = exported;
})(globalThis);

const test = require("node:test"),
  assert = require("node:assert/strict");
const R = require("./static/research-charts.js"),
  C = require("./static/research-compare.js"),
  F = require("./static/research-files.js"),
  I = require("./static/result-images.js");
const close = (a, b, tol = 1e-8) =>
  assert.ok(Math.abs(a - b) <= tol, `${a} != ${b}`);
const analysis = (id, values) => ({
  version: 1,
  result: { id, name: id },
  metrics: {},
  research: {
    summary: Object.fromEntries(
      Object.entries(values).map(([key, average]) => [key, { average }]),
    ),
    messages: [],
    degreeDistribution: [],
    degreeFit: { df: 5, location: 6, scale: 2 },
  },
  observations: [],
  timeline: [],
  latencyCDF: [],
  latencyHistogram: [],
});
const entry = (series, label, values, params = {}) => ({
  series,
  case: label,
  params,
  analysis: analysis(series + label, values),
});
test("repeat statistics keep zero reachability, exact decimal cases, and unknown single-run SD", () => {
  const groups = C.aggregate([
    entry("a", "i1-f0.1-p100", { frt: 1, reachability: 0 }),
    entry("a", "i1-f0.1-p100", { frt: 3 }),
    entry("a", "i1-f0.9-p100", { frt: 8 }),
  ]);
  assert.equal(groups.length, 2);
  const stat = C.metric(groups[0], "frt");
  close(stat.mean, 2);
  close(stat.sd, Math.sqrt(2));
  assert.equal(C.metric(groups[1], "frt").sd, null);
  assert.equal(C.metric(groups[0], "reachability").mean, 0);
  assert.deepEqual(C.parameters("i2-f0.25-p90"), { i: 2, f: 0.25, p: 90 });
  assert.deepEqual(C.parameters("arbitrary name"), {});
  assert.throws(
    () =>
      C.aggregate([
        entry("a", "x", {}, { i: 1 }),
        entry("a", "x", {}, { i: 2 }),
      ]),
    /inconsistent/,
  );
});
test("comparison derivatives and zero denominators do not fabricate certainty", () => {
  const target = { mean: 6, sd: 2 },
    base = { mean: 2, sd: 1 };
  let result = C.comparison(target, base, "difference");
  close(result.y, 4);
  close(result.error, Math.sqrt(5));
  result = C.comparison(target, base, "ratio");
  close(result.y, 0.75);
  close(result.error, Math.hypot((2 / 64) * 2, 6 / 64));
  result = C.comparison(target, base, "division");
  close(result.y, 3);
  close(result.error, Math.hypot(1, 1.5));
  assert.equal(
    C.comparison({ mean: 0, sd: 0 }, { mean: 0, sd: 0 }, "ratio").y,
    null,
  );
  assert.equal(
    C.comparison(target, { mean: 2, sd: null }, "difference").error,
    null,
  );
  assert.deepEqual(C.comparison(base, base, "difference", true), {
    y: 0,
    error: 0,
  });
});
test("Pareto dominance includes reach as a third objective; constant scores are neutral", () => {
  const a = { frt: 1, drc: 1, reach: 0.2 },
    b = { frt: 2, drc: 2, reach: 1 },
    c = { frt: 3, drc: 3, reach: 0.1 };
  assert.deepEqual(C.pareto([a, b, c]), [a]);
  assert.deepEqual(C.pareto([a, b, c], true), [a, b]);
  assert.equal(C.scores([a])[0].score, 0.5);
  assert.throws(() => C.scores([a], [0, 0, 0]), /positive/);
});
test("full rank regressions recover coefficients and dependent designs remain unavailable", () => {
  const x = Array.from({ length: 16 }, (_, i) => [1, i, i * i]),
    ys = x.map(([one, v, sq]) => 2 - 3 * v + 0.5 * sq),
    fit = C.regress(x, ys);
  assert.equal(fit.ok, true);
  [2, -3, 0.5].forEach((v, i) => close(fit.coefficients[i], v));
  close(fit.r2, 1);
  assert.equal(
    C.regress(
      [
        [1, 2],
        [1, 2],
        [1, 2],
      ],
      [1, 2, 3],
    ).ok,
    false,
  );
  assert.equal(C.regress([[1, 2]], [3]).status, "insufficient observations");
  for (const model of C.duplicateModels) {
    const rows = Array.from({ length: 30 }, (_, i) =>
        model.row({ i: 1 + (i % 7), f: 0.1 + (i % 5) / 5, p: (i % 6) * 15 }),
      ),
      coefficients = model.names.map((_, i) => i + 1),
      y = rows.map((r) => r.reduce((s, v, i) => s + v * coefficients[i], 0)),
      fit = C.regress(rows, y);
    assert.equal(fit.ok, true);
    coefficients.forEach((v, i) => close(fit.coefficients[i], v, 1e-6));
  }
  close(C.studentPDF(0, 1, 0, 1), 1 / Math.PI);
  assert.equal(C.studentPDF(0, -1, 0, 1), null);
});
test("lazy metrics use matched deltas and preserve noisy negative estimates", () => {
  const on = C.aggregate([
      entry("on", "a", {
        node_count: 100,
        lazy_count: 20,
        reachability: 0.6,
        logical_recv_iwant: 40,
      }),
    ])[0],
    off = C.aggregate([entry("off", "a", { reachability: 0.7 })])[0];
  on.runs[0].research.messageCount = 2;
  const result = C.lazyMetrics(on, off);
  close(result.delta_reachability, -0.1);
  close(result.net_coverage, -10);
  close(result.overlap, 30);
  close(result.net_efficiency, -0.5);
  close(result.per_iwant, 1);
});
test("zero-time and missing hops are handled without division by zero or fake hop 0", () => {
  assert.deepEqual(R.cumulative([0, 0, null, 1]), [
    { x: 0, y: 2 / 3 },
    { x: 1, y: 1 },
  ]);
  assert.deepEqual(R.pdf([null, 1, 1, 2]), [
    { x: 1, y: 2 / 3 },
    { x: 2, y: 1 / 3 },
  ]);
  const average = R.averageCurves([[{ x: 0, y: 1 }], []]);
  assert.deepEqual(average, [{ x: 0, y: 0.5 }]);
});
test("v2 imports retain decimal labels, imported errors, degree ratios, tree hops and quoted CSV", () => {
  let data = F.parseImport(
    '{"x_case":["0.1","0.9"],"y":[1,2],"yerr":[0.2,0.3]}',
    "input.json",
    "frt",
  );
  assert.equal(data.entries.length, 2);
  close(C.metric(C.aggregate(data.entries)[0], "frt").sd, 0.2);
  data = F.parseImport(
    '{"m1":{"degree":{"Total":8,"Count":2}},"m2":{"degree":{"Total":6,"Count":3}}}',
    "metrics.jsonl",
  );
  close(data.entries[0].analysis.research.summary.average_degree.average, 3);
  data = F.parseImport(
    '{"m":{"id":"root","time":3,"children":[{"id":"a","time":4,"children":[]}]}}',
    "propagation.jsonl",
  );
  assert.equal(data.entries[0].analysis.research.messages[0].nodes[0].hop, 1);
  assert.equal(
    data.entries[0].analysis.research.messages[0].nodes[0].seconds,
    1,
  );
  data = F.parseImport(
    'series,time,cdf\n"case, one",0,0\n"case, one",1,1\n',
    "curves.csv",
  );
  assert.equal(data.curves[0].name, "case, one");
  assert.equal(data.curves[0].points[1].y, 1);
  assert.throws(() => F.parseImport("x,y\n1,wrong", "x.csv"), /Non-numeric/);
  assert.throws(() => F.parseImport("not JSON", "x.json"), /Invalid JSON/);
});
test("all chart families render finite escaped SVGs with unique download names", () => {
  const a = analysis("test<script>", {
      frt: 1,
      reachability: 0.8,
      drc_per_node_count: 2,
    }),
    charts = I.buildCharts(a);
  assert.ok(charts.some((c) => c.id === "graph-modularity-gossipsub"));
  assert.ok(charts.some((c) => c.id === "bandwidth-protocol-send-rate"));
  assert.equal(new Set(charts.map((c) => c.id)).size, charts.length);
  const entries = Array.from({ length: 15 }, (_, i) =>
    entry(
      i % 2 ? "on" : "off",
      `i${i + 1}-f${(i % 3) + 1}-p${(i % 4) * 25}`,
      {
        frt: i + 1,
        drc_per_node_count: 15 - i,
        reachability: 0.8,
        node_count: 50,
        lazy_count: 5,
      },
      { dLow: (i % 3) + 2, d: (i % 5) + 5, dHigh: (i % 7) + 10 },
    ),
  );
  const comparison = C.buildCharts(entries, {
      predictions: [{ dLow: 4, d: 8, dHigh: 12 }],
    }),
    all = [...charts, ...comparison, ...C.referenceCharts()];
  assert.ok(comparison.some((c) => c.id === "dup-model-interaction-01"));
  assert.ok(comparison.some((c) => c.id === "degree-multivariate-inverse_df"));
  assert.equal(new Set(comparison.map((c) => c.id)).size, comparison.length);
  for (const chart of all) {
    const svg = I.chartSVG(chart);
    assert.doesNotMatch(svg, /NaN|Infinity|<script>/);
    assert.match(svg, /<svg/);
  }
  const log = I.chartSVG({
    title: "log",
    xLabel: "s",
    yLabel: "ratio",
    logX: true,
    series: [
      {
        name: "x",
        points: [
          { x: 0, y: 0 },
          { x: 0.01, y: 0.5, error: 0.1 },
          { x: 1, y: 1 },
        ],
      },
    ],
  });
  assert.doesNotMatch(log, /NaN|Infinity/);
});
test("CSV and ZIP downloads preserve exact graph values and valid signatures", async () => {
  const csv = F.chartCSV({
    title: "title",
    series: [{ name: 'a"b', points: [{ x: 0.125, y: 1.25, error: 0.2 }] }],
  });
  assert.match(csv, /a""b/);
  assert.match(csv, /0.125/);
  const zip = F.zip([{ name: "one.csv", data: csv }]);
  const bytes = Buffer.from(await zip.arrayBuffer());
  assert.equal(bytes.readUInt32LE(), 0x04034b50);
  assert.equal(bytes.readUInt32LE(bytes.length - 22), 0x06054b50);
  assert.equal(bytes.readUInt16LE(bytes.length - 12), 1);
});
test("old analysis cache upgrades once without explicit refresh", async () => {
  const elements = new Map(),
    el = (id) => {
      if (!elements.has(id))
        elements.set(id, {
          innerHTML: "",
          listeners: {},
          querySelectorAll() { return []; },
          classList: { toggle() {} },
          setAttribute() {},
          removeAttribute() {},
          addEventListener(k, fn) {
            this.listeners[k] = fn;
          },
          showModal() {
            this.open = true;
          },
          insertAdjacentHTML(_, s) {
            this.innerHTML += s;
          },
          close() {},
        });
      return elements.get(id);
    };
  let posts = 0;
  const a = analysis("old", {});
  a.analysisId = "new";
  const ui = I.createUI({
    document: { querySelector: (s) => el(s.slice(1)) },
    renderImage: async () => "data:image/png;base64,iVBORw0KGgo=",
    api: async (path, opts) => {
      if (path.includes("/result?")) return a;
      if (opts.method === "POST") {
        posts++;
        return {
          version: 1,
          analysisVersion: 3,
          id: "new",
          runId: "old",
          state: "completed",
        };
      }
      return { version: 1, id: "previous", runId: "old", state: "completed" };
    },
  });
  await ui.open("old");
  assert.equal(posts, 1);
  assert.match(el("resultImagesStatus").textContent, /images ready/);
});

test("origin plots and CSV retain unknown estimates and per-link evidence", () => {
  const a = analysis("origins", {});
  a.research.messages = [
    {
      id: "m",
      topic: "t",
      publisher: "p",
      nodes: [
        {
          id: "a",
          parent: "p",
          hop: 1,
          seconds: 1,
          source: "eager",
          linkEstimate: "eager",
          evidence: ["GRAFT"],
        },
        {
          id: "b",
          parent: "p",
          hop: 1,
          seconds: 2,
          source: "unknown",
          linkEstimate: "unknown",
          evidence: ["push and pull evidence conflict"],
        },
      ],
      metrics: {},
    },
  ];
  const chart = R.buildCharts(a, I).find((c) => c.id === "origin-estimates");
  assert.deepEqual(
    chart.series[0].points.map((p) => p.y),
    [1, 0, 1],
  );
  assert.match(chart.note, /Message IDs are matched/);
  const tree = R.messageCharts(a, 0).find((c) => c.tree);
  const csv = F.chartCSV(tree);
  assert.match(csv, /estimated_link_source/);
  assert.match(csv, /push and pull evidence conflict/);
});

test('peak normalization scales imported standard deviations in the same units as their means', () => {
  const { curves } = F.parseImport('{"x":[1,2],"y":[10,20],"yerr":[1,4]}', 'errors.json');
  curves.push({ name: 'second', xLabel: 'x', yLabel: 'frt', points: [{ x: 1, y: 100, error: 10, xError: 2 }] });
  const original = JSON.stringify(curves);
  const [raw, normalized] = F.curveCharts(curves);
  assert.equal(raw.series[0].points[1].error, 4);
  assert.equal(normalized.series[0].points[0].y, .5);
  assert.equal(normalized.series[0].points[0].error, .05);
  assert.equal(normalized.series[0].points[1].y, 1);
  assert.equal(normalized.series[0].points[1].error, .2);
  assert.equal(normalized.series[1].points[0].error, .1);
  assert.equal(normalized.series[1].points[0].xError, 2);
  assert.equal(JSON.stringify(curves), original, 'normalizing changed the imported source data');
  const csv = F.csvRows(F.chartCSV(normalized));
  assert.equal(csv[1][4], '0.05');
  const unavailable = F.curveCharts([{ name: 'zero', points: [{ x: 0, y: 0, error: 1 }] }])[1];
  assert.equal(unavailable.series[0].points[0].y, null);
  assert.equal(unavailable.series[0].points[0].error, null);
});

test("graph metrics have separate protocol variants without mixing group series", () => {
  const a = analysis("layers", {});
  const protocols = ["gossipsub", "kademlia", "transport"];
  const protocolLabels = ["GossipSub", "Kademlia", "Transport"];
  a.observations = [
    {
      at: "2026-09-13T00:00:00Z",
      groups: ["", "workers"].map((group, groupIndex) => ({
        group,
        layers: protocols.map((protocol, index) => ({
          protocol,
          nodes: (index + 1) * 10 + groupIndex,
          averageDegree: (index + 1) * 2 + groupIndex,
          clustering: (index + 1) / 10,
          metrics: Object.fromEntries(R.graphKeys.map(key => [key, index + 1])),
        })),
      })),
    },
  ];
  const charts = I.buildCharts(a).filter(chart => chart.id.startsWith("graph-"));
  assert.equal(charts.length, (R.graphKeys.length - 1) * protocols.length + 1);
  assert.equal(new Set(charts.map(chart => chart.id)).size, charts.length);
  for (const key of R.graphKeys.filter(key => key !== "node_count")) {
    const variants = charts.filter(chart => chart.groupId === "graph-" + key);
    assert.deepEqual(variants.map(chart => chart.protocol), protocols);
    for (const [index, chart] of variants.entries()) {
      assert.equal(chart.id, "graph-" + key + "-" + protocols[index]);
      assert.equal(chart.groupTitle, R.labels[key]);
      assert.equal(chart.title, R.labels[key] + " · " + protocolLabels[index]);
      assert.ok(chart.series.every(series => series.name.startsWith(protocolLabels[index] + " · ")));
    }
  }
  const nodes = charts.find(chart => chart.id === "graph-node_count");
  assert.equal(nodes.category, "common");
  assert.equal(nodes.protocol, undefined);
  assert.deepEqual(nodes.series.map(series => series.points[0].y), [10, 11, 20, 21, 30, 31]);
  assert.match(nodes.note, /counts can differ/);
  for (const [index, protocol] of protocols.entries()) {
    const degree = charts.find(chart => chart.id === "graph-average_degree-" + protocol);
    assert.deepEqual(degree.series.map(series => series.name), [protocolLabels[index] + " · All Peers", protocolLabels[index] + " · workers"]);
    assert.deepEqual(degree.series.map(series => series.points[0].y), [(index + 1) * 2, (index + 1) * 2 + 1]);
    const diameter = charts.find(chart => chart.id === "graph-diameter-" + protocol);
    assert.equal(diameter.series.length, 1);
    assert.equal(diameter.series[0].name, protocolLabels[index] + " · All Peers");
  }
});

test("unobserved graph protocols remain unavailable instead of borrowing another layer", () => {
  const a = analysis("only-mesh", {});
  a.observations = [{
    at: "2026-09-13T00:00:00Z",
    groups: [{ group: "", layers: [{ protocol: "gossipsub", nodes: 10, averageDegree: 4 }] }],
  }];
  const charts = I.buildCharts(a);
  assert.equal(charts.find(chart => chart.id === "graph-average_degree-gossipsub").series[0].points[0].y, 4);
  for (const protocol of ["kademlia", "transport"]) {
    const chart = charts.find(chart => chart.id === "graph-average_degree-" + protocol);
    assert.deepEqual(chart.series, [{ name: R.categoryLabels[protocol] + " · All Peers", points: [{ x: 0, y: null }] }]);
    assert.match(I.chartSVG(chart), /No eligible observations available/);
  }
  // Stream protocol IDs have different attribution semantics from graph layers.
  const bandwidth = charts.filter(chart => chart.id.startsWith("bandwidth-protocol-"));
  assert.equal(bandwidth.length, 4);
  assert.ok(bandwidth.every(chart => chart.protocol === undefined && chart.groupId === undefined));
});


test("metric title case preserves protocol names, acronyms and measurement units", () => {
  assert.equal(R.metricTitle("GossipSub degree distribution · Student-t fit (ms)"), "GossipSub Degree Distribution · Student-t Fit (ms)");
  assert.equal(R.metricTitle("PageRank and FRT (s) · run mean"), "PageRank and FRT (s) · Run Mean");
  assert.equal(R.labels.node_count, "Node Count");
  assert.equal(R.labels.send_ihave, "Sent IHAVE RPCs");
});

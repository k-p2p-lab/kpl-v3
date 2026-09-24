/* v2 research views. Pure chart/statistics functions also run in the test runner. */
(function (root) {
  "use strict";
  const finite = (v) => typeof v === "number" && Number.isFinite(v);
  const categoryLabels = { common: "Common", gossipsub: "GossipSub", kademlia: "Kademlia", transport: "Transport" };
  // Display names use title case while units, acronyms and technical names keep
  // their spelling. Data keys, protocol IDs and user-provided series stay intact.
  function metricTitle(text) {
    const lower = new Set(["and", "as", "at", "by", "for", "from", "in", "of", "on", "or", "over", "per", "the", "to", "vs", "with", "without", "s", "ms", "v2", "v3"]);
    return String(text).replace(/\b[a-z][a-z0-9]*\b/g, (word, offset) =>
      (lower.has(word) && offset > 0 || word === "t" && String(text).slice(0, offset).endsWith("Student-"))
        ? word : word[0].toUpperCase() + word.slice(1));
  }
  function formatChart(chart) {
    return { ...chart, title: metricTitle(chart.title),
      ...(chart.groupTitle ? { groupTitle: metricTitle(chart.groupTitle) } : {}),
      ...(chart.panels ? { panels: chart.panels.map(formatChart) } : {}) };
  }
  const labels = {
    node_count: "Node count",
    average_degree: "Mean degree",
    average_degree_excluding_leaves: "Degree / non-leaf population",
    diameter: "Diameter",
    shortest_path_length: "Mean shortest path",
    clustering_coefficient: "Clustering coefficient",
    betweenness_centrality: "Betweenness centrality",
    page_rank: "PageRank",
    degree_centrality: "Degree centrality",
    closeness_centrality: "Closeness centrality",
    eigenvector_centrality: "Eigenvector centrality",
    assortativity: "Degree assortativity",
    modularity: "Modularity",
    connected_pair_fraction: "Connected pair fraction",
    frt: "First reception time (s)",
    eager_frt: "Estimated eager-path FRT (s)",
    reachability: "Observed membership reachability",
    eager_reachability: "Estimated eager reachability",
    drc: "Duplicate reception count",
    drc_per_node_count: "Duplicates / mean graph nodes",
    drc_per_target: "Duplicates / receiver population",
    eager_count: "Estimated eager receivers",
    lazy_count: "Estimated lazy receivers",
    unknown_count: "Unclassified receivers",
    send_ihave: "Sent IHAVE RPCs",
    send_iwant: "Sent IWANT RPCs",
    recv_ihave: "Received IHAVE RPCs",
    recv_iwant: "Received IWANT RPCs",
    send_graft: "GRAFT transitions",
    send_prune: "PRUNE transitions",
    logical_send_ihave: "Sent IHAVE IDs",
    logical_send_iwant: "Sent IWANT IDs",
    logical_recv_ihave: "Received IHAVE IDs",
    logical_recv_iwant: "Received IWANT IDs",
    logical_send_graft: "Reciprocal GRAFT edges",
    logical_send_prune: "Removed confirmed edges",
    bandwidth_sent: "Sent stream bytes",
    bandwidth_received: "Received stream bytes",
  };
  for (const key of Object.keys(labels)) labels[key] = metricTitle(labels[key]);
  const graphKeys = Object.keys(labels).slice(0, 14);
  const value = (a, key) => {
    if (key === "bandwidth_sent")
      return a.metrics?.bandwidth?.sentBytes ?? null;
    if (key === "bandwidth_received")
      return a.metrics?.bandwidth?.receivedBytes ?? null;
    if (key === "df" || key === "location" || key === "scale")
      return a.research?.degreeFit?.[key] ?? null;
    if (key === "inverse_df") {
      const v = value(a, "df");
      return finite(v) && v > 0 ? 1 / v : null;
    }
    if (key === "log_scale") {
      const v = value(a, "scale");
      return finite(v) && v > 0 ? Math.log(v) : null;
    }
    return a.research?.summary?.[key]?.average ?? null;
  };
  function stats(input) {
    const xs = input.filter(finite).sort((a, b) => a - b),
      n = xs.length;
    if (!n) return { mean: null, sd: null, n: 0 };
    const scale = xs.reduce(
      (largest, value) => Math.max(largest, Math.abs(value)),
      0,
    );
    const mean = scale
      ? xs.reduce((a, x, i) => a + (x / scale - a) / (i + 1), 0)
      : 0;
    const sd =
      n > 1
        ? Math.sqrt(
            xs.reduce((a, x) => a + (scale ? x / scale - mean : 0) ** 2, 0) /
              (n - 1),
          ) * scale
        : null;
    return { mean: mean * scale, sd: finite(sd) ? sd : null, n };
  }
  function cumulative(values, denominator) {
    if (denominator === undefined)
      denominator = values.filter((v) => finite(v) && v >= 0).length;
    const counts = new Map();
    for (const v of values)
      if (finite(v) && v >= 0) counts.set(v, (counts.get(v) || 0) + 1);
    let sum = 0;
    return denominator > 0
      ? [...counts]
          .sort((a, b) => a[0] - b[0])
          .map(([x, n]) => ({ x, y: (sum += n) / denominator }))
      : [];
  }
  function pdf(values) {
    const counts = new Map(),
      valid = values.filter((v) => finite(v) && v >= 0);
    for (const v of valid) counts.set(v, (counts.get(v) || 0) + 1);
    return [...counts]
      .sort((a, b) => a[0] - b[0])
      .map(([x, n]) => ({ x, y: n / valid.length }));
  }
  function sampled(points, limit = 360) {
    if (points.length <= limit) return points;
    const stride = Math.ceil(points.length / (limit - 1));
    return points.filter((_, i) => i % stride === 0 || i === points.length - 1);
  }
  function averageCurves(curves) {
    if (!curves.length) return [];
    const xs = [...new Set(curves.flatMap((c) => c.map((p) => p.x)))]
      .filter(finite)
      .sort((a, b) => a - b);
    const grid = sampled(xs),
      indexes = curves.map(() => 0);
    return grid.map((x) => ({
      x,
      y:
        curves.reduce((sum, c, i) => {
          while (indexes[i] < c.length && c[indexes[i]].x <= x) indexes[i]++;
          return sum + (indexes[i] ? c[indexes[i] - 1].y : 0);
        }, 0) / curves.length,
    }));
  }
  function graphPoints(a, protocol, group, key) {
    const origin = Date.parse(a.timeOrigin || a.observations?.[0]?.at);
    return (a.observations || []).map((o) => {
      const l = o.groups
        ?.find((g) => g.group === group)
        ?.layers?.find((l) => l.protocol === protocol);
      const y =
        key === "node_count"
          ? l?.nodes
          : key === "average_degree"
            ? l?.averageDegree
            : key === "clustering_coefficient"
              ? l?.clustering
              : l?.metrics?.[key];
      return { x: (Date.parse(o.at) - origin) / 1000, y: finite(y) ? y : null };
    });
  }
  function messageSeries(a, key) {
    if (a.research?.overview) return a.research.overview.messageSeries?.[key] || [];
    const rows = (a.research?.messages || [])
      .filter(
        (m) => finite(m.metrics?.[key]?.average) && finite(Date.parse(m.at)),
      )
      .sort((a, b) => Date.parse(a.at) - Date.parse(b.at));
    if (!rows.length) return [];
    const origin = Date.parse(a.timeOrigin || rows[0].at),
      chunk = Math.max(1, Math.ceil(rows.length / 360)),
      points = [];
    for (let i = 0; i < rows.length; i += chunk) {
      const group = rows.slice(i, i + chunk),
        s = stats(group.map((m) => m.metrics[key].average));
      points.push({
        x: (Date.parse(group[0].at) - origin) / 1000,
        y: s.mean,
        error: chunk === 1 ? group[0].metrics[key].deviation : s.sd,
        label: `${group.length} messages`,
      });
    }
    return points;
  }
  function controlPoints(a, key) {
    const bins = a.research?.controls || [],
      width = a.research?.controlBinSeconds || 5,
      origin = Date.parse(a.timeOrigin || bins[0]?.at),
      points = [];
    let end = 0;
    for (const b of bins) {
      const x = (Date.parse(b.at) - origin) / 1000;
      if (x > end) points.push({ x: end, y: 0 }, { x, y: 0 });
      points.push({ x, y: (b.values[key] || 0) / width });
      end = x + width;
    }
    if (points.length) points.push({ x: end, y: points.at(-1).y });
    return points;
  }
  function receiverGroupLabel(group) {
    return group.group ? `Group: ${group.group}` : "Unknown Group";
  }
  function deliveryCharts(a) {
    const r = a.research || {}, charts = [];
    const add = (id, title, xLabel, yLabel, series, extra = {}) =>
      charts.push({ id, title, xLabel, yLabel, series, category: "gossipsub", ...extra });
    for (const [key, title] of [
      ["propagationCDF", "Receivers over propagation time"],
      ["duplicateCDF", "Duplicate copies over propagation time"],
      ["hopPDF", "First-reception hop distribution"],
      ["hopCDF", "First-reception hop CDF"],
    ])
      add(
        `research-${key}`,
        title,
        key.startsWith("hop") ? "Hop" : "Time since publication (s)",
        key === "duplicateCDF" ? "Copies / receiver population" : "Fraction",
        [{ name: title, points: r[key] || [] }],
        {
          mode: key === "hopPDF" ? "bar" : "step",
          note: key.startsWith("hop")
            ? `Conditional on resolved first-reception paths. ${r.unresolvedParents || 0} unresolved parents are excluded and retained in JSON.`
            : `${r.populationBasis || "Membership evidence unavailable"}. Same message–node cohort in numerator and denominator; ${r.eligiblePopulation || 0} eligible pairs. Missing latency is not zero.`,
        },
      );
    add(
      "propagation-log-time",
      "Receivers and duplicates · logarithmic time",
      "Time since publication (s, log scale)",
      "Count / receiver population",
      [
        { name: "First receivers", points: r.propagationCDF || [] },
        { name: "Duplicates", points: r.duplicateCDF || [] },
      ],
      {
        logX: true,
        mode: "step",
        note: "Zero-time events remain in linear charts and JSON; log axes show strictly positive times.",
      },
    );
    add(
      "eager-lazy-latency",
      "Estimated eager / lazy first-reception latency",
      "Time since publication (s)",
      "Conditional CDF",
      [
        { name: "Estimated all-eager paths", points: r.eagerCDF || [] },
        {
          name: "Paths estimated to contain a lazy edge",
          points: r.lazyCDF || [],
        },
      ],
      {
        mode: "step",
        yMax: 1,
        note: `${r.unknownOrigins || 0} receivers have unknown origin. Heuristic: active GRAFT suggests eager; IHAVE followed by IWANT within 5 seconds suggests lazy. Detailed logs match IHAVE/IWANT to the delivered message ID; old count-only logs use time-window association. Conflicting or missing evidence is unclassified.`,
      },
    );
    for (const key of [
      "frt",
      "eager_frt",
      "reachability",
      "eager_reachability",
      "drc",
      "drc_per_node_count",
      "eager_count",
      "lazy_count",
      "unknown_count",
    ])
      add(
        `messages-${key}`,
        `${labels[key]} · messages`,
        "Publication elapsed time (s)",
        labels[key],
        [{ name: "Message metric", points: messageSeries(a, key) }],
        {
          note: "One point per message; large sets are grouped into at most 360 points. FRT shading shows ±1 population SD within a message; grouped points show ±1 between-message sample SD. Eager/lazy counts are estimates; unknown_count reports unclassified receipts.",
        },
      );
    const origins = r.overview?.originCounts || { eager: 0, lazy: 0, unknown: 0 };
    for (const message of r.messages || [])
      for (const node of message.nodes || [])
        origins[node.source in origins ? node.source : "unknown"]++;
    add(
      "origin-estimates",
      "Eager push / lazy pull estimates",
      "Estimated source",
      "First-receiver observations",
      [
        {
          name: "Metadata classification",
          points: ["eager", "lazy", "unknown"].map((key, x) => ({
            x,
            y: origins[key],
          })),
        },
      ],
      {
        mode: "bar",
        xTicks: ["Eager estimate", "Lazy estimate", "Unclassified"],
        note: `${r.originMethod || "No inference metadata"}; ${r.originWindowSeconds || 5}s lookback. GRAFT and IHAVE→IWANT suggest an origin. Message IDs are matched when available; old logs use a time-window association. Unknown observations remain visible.`,
      },
    );
    const messages = r.messages || [];
    const timeCurves = messages.map((m) =>
      cumulative(
        (m.nodes || []).map((n) => n.seconds),
        1,
      ),
    );
    const hopCurves = messages.map((m) =>
      cumulative(
        (m.nodes || []).map((n) => n.hop),
        1,
      ),
    );
    for (const [id, title, curves, xLabel] of [
      [
        "mean-receivers-time",
        "Mean receiver count over time",
        r.overview ? [r.overview.receiversTime] : timeCurves,
        "Time since publication (s)",
      ],
      ["mean-receivers-hop", "Mean receiver count over hop", r.overview ? [r.overview.receiversHop] : hopCurves, "Hop"],
    ])
      add(
        id,
        title,
        xLabel,
        "Receivers / published message",
        [{ name: "Equal message weight", points: averageCurves(curves) }],
        {
          mode: "step",
          note: "Includes messages with zero receipts. First receipt per node; publication is the time origin. Unresolved hops are excluded, not shifted to hop 0.",
        },
      );
    for (const [id, curves, xLabel] of [
      ["time", r.overview ? [r.overview.receiversTime] : timeCurves, "Time since publication (s)"],
      ["hop", r.overview ? [r.overview.receiversHop] : hopCurves, "Hop"],
    ]) {
      const cumulative = averageCurves(curves);
      let previous = 0;
      add(
        `mean-receivers-${id}-increments`,
        `Mean first-receiver increments by ${id}`,
        xLabel,
        "New receivers / published message",
        [
          {
            name: "Increment of mean receiver count",
            points: cumulative.map((p) => {
              const y = p.y - previous;
              previous = p.y;
              return { x: p.x, y };
            }),
          },
        ],
        {
          mode: "bar",
          note: "v2's PDF view is a difference of receiver counts, not a density per second. Time increments use the displayed common grid; hop increments use integer hop levels.",
        },
      );
    }
    return charts;
  }
  function buildCharts(a, helpers) {
    a = { ...a, timeOrigin: a.timeOrigin || a.result?.startedAt };
    const r = a.research || {},
      charts = [];
    const add = (id, title, xLabel, yLabel, series, extra = {}) =>
      charts.push({ id, title, xLabel, yLabel, series, category: "gossipsub", ...extra });
    const groups = [
      ...new Set(
        (a.observations || []).flatMap((o) => o.groups.map((g) => g.group)),
      ),
    ].sort();
    if (!groups.length) groups.push("");
    add("graph-node_count", labels.node_count, "Elapsed time (s)", labels.node_count,
      ["gossipsub", "kademlia", "transport"].flatMap(protocol => groups.map(group => ({
        name: `${categoryLabels[protocol]} · ${group || "All Peers"}`,
        points: graphPoints(a, protocol, group, "node_count"),
      }))), {
        category: "common",
        note: "Fresh participating nodes in each protocol graph, including isolates. GossipSub and Kademlia require overlay reports and enabled protocols, so their counts can differ from Transport. Missing observations remain gaps; counts are not added together.",
      });
    for (const key of graphKeys.filter(key => key !== "node_count")) {
      const globalOnly = [
        "diameter",
        "shortest_path_length",
        "assortativity",
        "modularity",
        "connected_pair_fraction",
        "average_degree_excluding_leaves",
      ].includes(key);
      for (const [protocol, protocolLabel] of [
        ["gossipsub", "GossipSub"],
        ["kademlia", "Kademlia"],
        ["transport", "Transport"],
      ]) {
        const series = (globalOnly ? [""] : groups).map((group) => ({
          name: `${protocolLabel} · ${group || "All Peers"}`,
          points: graphPoints(a, protocol, group, key),
        }));
        const populated = series.filter((s) => s.points.some((p) => finite(p.y)));
        add(
          `graph-${key}-${protocol}`,
          `${labels[key]} · ${protocolLabel}`,
          "Elapsed time (s)",
          labels[key],
          populated.length ? populated : [series[0]],
          {
            protocol,
            category: protocol,
            groupId: `graph-${key}`,
            groupTitle: labels[key],
            note:
              key === "average_degree_excluding_leaves"
                ? "v2 definition retained: sum of ALL degrees / count of nodes with degree > 1. Zero denominator is N/A."
                : "Fresh, undirected unique-neighbor graph. Includes isolates. Unreachable/self pairs excluded from distances; see connected pair fraction. Missing historical edges yield N/A; no topology is invented.",
          },
        );
      }
    }
    add(
      "degree-probability",
      "Degree distribution",
      "Degree",
      "Probability",
      [
        {
          name: "Snapshot-mean probability",
          points: r.degreeDistribution || [],
        },
      ],
      {
        mode: "bar",
        note: "Each saved snapshot has equal weight; isolated nodes remain at degree 0.",
      },
    );
    let degreeSum = 0;
    add(
      "degree-cdf",
      "Cumulative degree distribution",
      "Degree",
      "Probability",
      [
        {
          name: "Degree CDF",
          points: (r.degreeDistribution || []).map((p) => ({
            x: p.x,
            y: (degreeSum += p.y),
          })),
        },
      ],
      { mode: "step", yMax: 1 },
    );
    const fit = r.degreeFit;
    add(
      "degree-student-t",
      "Degree distribution · Student-t fit",
      "Degree",
      "Probability / continuous density",
      [
        { name: "Observed probability", points: r.degreeDistribution || [] },
        { name: "Fitted density", points: fit?.density || [] },
      ],
      {
        note: `${fit?.status || "No degree observations"}. ${fit?.method || ""}. df=${fit?.df ?? "N/A"}, location=${fit?.location ?? "N/A"}, scale=${fit?.scale ?? "N/A"}. Density is a continuous approximation to integer degrees.`,
      },
    );
    const delivery = deliveryCharts(a);
    const receiverGroups = r.receiverGroups || [];
    for (const chart of delivery) {
      chart.series = chart.series.map(series => ({ ...series, name: chart.series.length === 1 ? "All Peers" : `All Peers · ${series.name}` }));
      if (receiverGroups.length) {
        chart.groupedBars = chart.mode === "bar";
        chart.note += " Group series classify receiving peers; ratios use each group's own eligible population. Only messages with an eligible receiver or recorded receipt/duplicate in that group contribute. Cross-group paths retain their full hop count. Unknown Group means missing or conflicting group metadata.";
      }
    }
    for (const group of receiverGroups) {
      for (const chart of deliveryCharts({ ...a, research: group })) {
        const target = delivery.find(candidate => candidate.id === chart.id);
        const name = receiverGroupLabel(group);
        target.series.push(...chart.series.map(series => ({ ...series, name: chart.series.length === 1 ? name : `${name} · ${series.name}` })));
      }
    }
    charts.push(...delivery);
    for (const [id, title, keys] of [
      [
        "control-raw",
        "Control RPC rate",
        ["send_ihave", "send_iwant", "recv_ihave", "recv_iwant"],
      ],
      [
        "control-logical",
        "Advertised and requested message IDs",
        [
          "logical_send_ihave",
          "logical_send_iwant",
          "logical_recv_ihave",
          "logical_recv_iwant",
        ],
      ],
      [
        "mesh-transitions",
        "Mesh transitions",
        [
          "send_graft",
          "send_prune",
          "logical_send_graft",
          "logical_send_prune",
        ],
      ],
      [
        "control-entries",
        "Control entries",
        [
          "entries_send_ihave",
          "entries_send_iwant",
          "entries_recv_ihave",
          "entries_recv_iwant",
        ],
      ],
    ])
      add(
        id,
        title,
        "Elapsed time (s)",
        "Observations / s",
        keys.map((key) => ({
          name: labels[key] || key,
          points: controlPoints(a, key),
        })),
        {
          mode: "step",
          note: `${r.controlBinSeconds || 5}-second bins. RPCs, message-ID references, entries and reciprocal mesh transitions have different units; endpoint observations are not added as unique network messages.`,
        },
      );
    const controls = a.metrics?.gossipsubControl || [];
    for (const [field, title] of [
      ["messageIds", "Control message-ID totals"],
      ["entries", "Control entry totals"],
    ])
      add(
        `control-totals-${field}`,
        title,
        "Type",
        "Recorded count",
        ["send", "recv", "drop"].map((direction, i) => ({
          name: direction,
          points: ["ihave", "iwant", "idontwant", "graft", "prune"].map(
            (type, x) => ({
              x: x + (i - 1) * 0.23,
              y: controls
                .filter(
                  (c) => c.direction === direction && c.controlType === type,
                )
                .reduce((n, c) => n + (c[field] || 0), 0),
            }),
          ),
        })),
        {
          mode: "bar",
          xTicks: ["IHAVE", "IWANT", "IDONTWANT", "GRAFT", "PRUNE"],
        },
      );
    const origin = Date.parse(a.timeOrigin || a.observations?.[0]?.at),
      observed = (read) =>
        groups.map((group) => ({
          name: group || "All peers",
          points: (a.observations || []).map((o) => ({
            x: (Date.parse(o.at) - origin) / 1000,
            y: read(o.groups.find((g) => g.group === group) || {}),
          })),
        }));
    for (const [key, title] of [
      ["scoreMin", "Minimum reported score"],
      ["scoreMax", "Maximum reported score"],
      ["negativeScoreRatio", "Negative score fraction"],
      ["reporting", "Reporting peers"],
      ["scoreCount", "Score observations"],
    ])
      add(
        `observers-${key}`,
        title,
        "Elapsed time (s)",
        title,
        observed((g) => g[key]),
        {
          category: key === "reporting" ? "common" : "gossipsub",
          note: "Fresh reporting peers, grouped by observer. Missing reports remain gaps.",
        },
      );
    add(
      "peer-lifecycle",
      "Peer lifecycle",
      "Elapsed time (s)",
      "Peers",
      ["ready", "starting", "stopping", "failed"].map((key) => ({
        name: key,
        points: (a.observations || []).map((o) => ({
          x: (Date.parse(o.at) - origin) / 1000,
          y: o.groups.find((g) => g.group === "")?.[key],
        })),
      })),
      { mode: "step", category: "common" },
    );
    const bandwidth = a.metrics?.bandwidth,
      protocols = [
        ...new Set(
          (a.bandwidthTimeline || []).flatMap((b) =>
            (b.protocols || []).map((p) => p.protocol),
          ),
        ),
      ].sort();
    const bwNote = `libp2p stream bytes, not link capacity or IP/TCP traffic. ${bandwidth?.finalizedSessions ?? 0}/${bandwidth?.sessions ?? 0} sessions finalized; ${bandwidth?.rejectedSamples ?? 0} rejected samples. Missing intervals are gaps.`;
    add(
      "bandwidth-cumulative",
      "Cumulative P2P stream traffic",
      "Elapsed time (s)",
      "KiB",
      ["send", "recv"].map((direction) => ({
        name: direction,
        points: helpers.bandwidthPoints(a, "*", direction, true),
      })),
      { note: bwNote, category: "common" },
    );
    if (!protocols.length) protocols.push("No protocol observations");
    for (const direction of ["send", "recv"])
      for (const cumulative of [false, true])
        add(
          `bandwidth-protocol-${direction}-${cumulative ? "bytes" : "rate"}`,
          `${direction === "send" ? "Sent" : "Received"} traffic by protocol`,
          "Elapsed time (s)",
          cumulative ? "KiB" : "kbit/s",
          protocols.map((protocol) => ({
            name: protocol,
            points: helpers.bandwidthPoints(a, protocol, direction, cumulative),
          })),
          { mode: cumulative ? "line" : "step", note: bwNote, category: "common" },
        );
    return charts.map(formatChart);
  }
  function messageCharts(a, index) {
    const m = a.research?.messages?.[index];
    if (!m) return [];
    const nodes = m.nodes || [],
      name = `${m.topic} · ${m.id}`,
      source = `${a.result?.name || a.result?.id} · ${name}`;
    const series = (key, kind) => ({
      name: kind ? `Estimated ${kind}` : "All first receivers",
      points: cumulative(
        nodes.filter((n) => !kind || n.source === kind).map((n) => n[key]),
      ),
    });
    const charts = [
      {
        id: `message-${index}-time`,
        title: "Message first-reception CDF",
        xLabel: "Time since publication (s)",
        yLabel: "Conditional CDF",
        mode: "step",
        series: [
          series("seconds"),
          series("seconds", "eager"),
          series("seconds", "lazy"),
        ],
      },
      {
        id: `message-${index}-hop`,
        title: "Message hop distribution",
        xLabel: "Hop",
        yLabel: "Conditional probability",
        mode: "bar",
        series: [
          { name: "Resolved receivers", points: pdf(nodes.map((n) => n.hop)) },
        ],
      },
      {
        id: `message-${index}-duplicates`,
        title: "Message cumulative duplicates",
        xLabel: "Time since publication (s)",
        yLabel: "Copies",
        mode: "step",
        series: [
          {
            name: "Observed duplicate copies",
            points: m.duplicateTimeline || [],
          },
        ],
      },
      {
        id: `message-${index}-tree`,
        title: "Message first-reception paths",
        tree: m,
        series: [],
      },
    ];
    return charts.map((c) => ({
      ...formatChart(c),
      category: "gossipsub",
      source,
      note: `${nodes.length} unique non-publisher receivers; ${nodes.filter((n) => !finite(n.hop)).length} unresolved paths, ${nodes.filter((n) => n.source === "unknown").length} unclassified origins. Eager/lazy colors are metadata estimates, not direct source measurements. Full IDs and membership are in analysis JSON.`,
    }));
  }
  const exported = {
    categoryLabels,
    receiverGroupLabel,
    metricTitle,
    formatChart,
    labels,
    graphKeys,
    value,
    stats,
    cumulative,
    pdf,
    sampled,
    averageCurves,
    graphPoints,
    messageSeries,
    controlPoints,
    buildCharts,
    messageCharts,
  };
  if (typeof module !== "undefined" && module.exports)
    module.exports = exported;
  else root.KPLResearch = exported;
})(globalThis);

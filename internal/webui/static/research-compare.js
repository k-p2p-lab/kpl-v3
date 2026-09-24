/* Repeated-run and v2 model analysis. Inputs are explicit series/case labels. */
(function (root) {
  "use strict";
  const R =
    typeof module !== "undefined" && module.exports
      ? require("./research-charts.js")
      : root.KPLResearch;
  const finite = (v) => typeof v === "number" && Number.isFinite(v);
  const divide = (a, b) =>
    finite(a) && finite(b) && b !== 0 && finite(a / b) ? a / b : null;
  function parameters(label, explicit = {}) {
    const result = {};
    for (const [key, pattern] of [
      ["i", /(?:^|[-_ ])i(\d+(?:\.\d+)?)(?=$|[-_ ])/],
      ["f", /(?:^|[-_ ])f(\d+(?:\.\d+)?)(?=$|[-_ ])/],
      ["p", /(?:^|[-_ ])p(\d+(?:\.\d+)?)(?=$|[-_ ])/],
    ]) {
      const match = label.match(pattern);
      if (match) result[key] = Number(match[1]);
    }
    const degree = label.match(
      /^(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)$/,
    );
    if (degree)
      [result.dLow, result.d, result.dHigh] = degree.slice(1).map(Number);
    for (const key of ["i", "f", "p", "dLow", "d", "dHigh"])
      if (finite(explicit[key])) result[key] = explicit[key];
    return result;
  }
  function aggregate(entries) {
    const map = new Map();
    for (const e of entries) {
      const key = JSON.stringify([e.series, e.case]);
      if (!map.has(key))
        map.set(key, {
          series: e.series,
          case: e.case,
          params: parameters(e.case, e.params),
          runs: [],
        });
      const group = map.get(key);
      const params = parameters(e.case, e.params);
      if (JSON.stringify(group.params) !== JSON.stringify(params))
        throw new Error(`Repeated case ${e.case} has inconsistent parameters.`);
      group.runs.push(e.analysis);
    }
    return [...map.values()].sort(
      (a, b) =>
        a.series.localeCompare(b.series) ||
        a.case.localeCompare(b.case, undefined, { numeric: true }),
    );
  }
  const metric = (group, key) =>
    group.runs.length === 1 && group.runs[0].importedSummaryStats?.[key]
      ? group.runs[0].importedSummaryStats[key]
      : R.stats(group.runs.map((a) => R.value(a, key)));
  function comparison(target, base, mode, same = false) {
    if (!finite(target.mean) || !finite(base.mean))
      return { y: null, error: null };
    const t = target.mean,
      b = base.mean;
    if (same) {
      if (mode === "difference") return { y: 0, error: 0 };
      return b !== 0
        ? { y: mode === "ratio" ? 0.5 : 1, error: 0 }
        : { y: null, error: null };
    }
    let y, dt, db;
    if (mode === "difference") {
      y = t - b;
      dt = 1;
      db = -1;
    } else if (mode === "ratio") {
      const sum = t + b;
      if (sum === 0 || !finite(sum)) return { y: null, error: null };
      y = t / sum;
      dt = (1 - y) / sum;
      db = -y / sum;
    } else {
      if (b === 0) return { y: null, error: null };
      y = t / b;
      dt = 1 / b;
      db = -y / b;
    }
    const error =
      finite(target.sd) && finite(base.sd)
        ? Math.hypot(dt * target.sd, db * base.sd)
        : null;
    return { y: finite(y) ? y : null, error: finite(error) ? error : null };
  }
  function pareto(rows, includeReach = false) {
    return rows.filter(
      (a) =>
        !rows.some(
          (b) =>
            b !== a &&
            b.frt <= a.frt &&
            b.drc <= a.drc &&
            (!includeReach || b.reach >= a.reach) &&
            (b.frt < a.frt ||
              b.drc < a.drc ||
              (includeReach && b.reach > a.reach)),
        ),
    );
  }
  function scores(rows, weights = [0.25, 0.25, 0.5]) {
    if (
      weights.length !== 3 ||
      weights.some((w) => !finite(w) || w < 0) ||
      weights.reduce((a, b) => a + b, 0) <= 0
    )
      throw new Error(
        "Use three nonnegative score weights with a positive sum.",
      );
    const ranges = ["frt", "drc", "reach"].map((key) =>
      rows.reduce(
        ([lo, hi], r) => [Math.min(lo, r[key]), Math.max(hi, r[key])],
        [Infinity, -Infinity],
      ),
    );
    const norm = weights.reduce((a, b) => a + b, 0);
    return rows.map((row) => ({
      ...row,
      score: [row.frt, row.drc, row.reach].reduce((sum, v, i) => {
        const [lo, hi] = ranges[i];
        const benefit =
          hi === lo
            ? 0.5
            : i === 2
              ? (v - lo) / (hi - lo)
              : (hi - v) / (hi - lo);
        return sum + (benefit * weights[i]) / norm;
      }, 0),
    }));
  }
  function lazyMetrics(on, off) {
    const N = metric(on, "node_count").mean,
      L = metric(on, "lazy_count").mean,
      W = R.stats(
        on.runs.map((a) =>
          divide(
            R.value(a, "logical_recv_iwant"),
            a.research?.messages?.length || a.research?.messageCount || 0,
          ),
        ),
      ).mean,
      ron = metric(on, "reachability").mean,
      roff = metric(off, "reachability").mean;
    const delta = finite(ron) && finite(roff) ? ron - roff : null,
      net = finite(delta) && finite(N) ? delta * N : null,
      overlap = finite(L) && finite(net) ? L - net : null,
      eff = divide(net, L);
    return {
      delta_reachability: delta,
      net_coverage: net,
      overlap,
      net_efficiency: eff,
      overlap_ratio: finite(eff) ? 1 - eff : null,
      lazy_first_ratio: divide(L, N),
      net_coverage_ratio: delta,
      per_iwant: divide(L, W),
    };
  }
  // Scaled, pivoted modified Gram-Schmidt with a second orthogonalization pass.
  // Refuse rank-deficient models rather than claiming identifiable coefficients.
  function regress(rows, ys) {
    const n = rows.length,
      p = rows[0]?.length || 0,
      empty = {
        ok: false,
        rank: 0,
        n,
        p,
        coefficients: null,
        predicted: [],
        r2: null,
        status: "insufficient observations",
      };
    if (
      !p ||
      n < p ||
      ys.length !== n ||
      rows.some((r) => r.length !== p || r.some((v) => !finite(v))) ||
      ys.some((v) => !finite(v))
    )
      return empty;
    const scales = Array.from(
        { length: p },
        (_, j) => rows.reduce((m, r) => Math.max(m, Math.abs(r[j])), 0) || 1,
      ),
      yscale = ys.reduce((m, v) => Math.max(m, Math.abs(v)), 0) || 1;
    const columns = Array.from({ length: p }, (_, j) =>
        rows.map((r) => r[j] / scales[j]),
      ),
      order = Array.from({ length: p }, (_, i) => i),
      q = [],
      matrix = Array.from({ length: p }, () => Array(p).fill(0)),
      norm = (v) => v.reduce((sum, value) => Math.hypot(sum, value), 0),
      dot = (a, b) => a.reduce((s, v, i) => s + v * b[i], 0);
    const initial = Math.max(...columns.map(norm));
    let rank = 0;
    for (let k = 0; k < p; k++) {
      let pivot = k;
      for (let j = k + 1; j < p; j++)
        if (norm(columns[j]) > norm(columns[pivot])) pivot = j;
      [columns[k], columns[pivot]] = [columns[pivot], columns[k]];
      [order[k], order[pivot]] = [order[pivot], order[k]];
      for (let i = 0; i < k; i++)
        [matrix[i][k], matrix[i][pivot]] = [matrix[i][pivot], matrix[i][k]];
      const length = norm(columns[k]);
      if (length <= initial * 1e-10) break;
      matrix[k][k] = length;
      q.push(columns[k].map((v) => v / length));
      rank++;
      for (let j = k + 1; j < p; j++)
        for (let pass = 0; pass < 2; pass++) {
          const r = dot(q[k], columns[j]);
          matrix[k][j] += r;
          columns[j] = columns[j].map((v, i) => v - r * q[k][i]);
        }
    }
    if (rank < p)
      return {
        ...empty,
        rank,
        status: `rank deficient (${rank}/${p}); vary independent parameters`,
      };
    const beta = Array(p).fill(0),
      qty = q.map((v) =>
        dot(
          v,
          ys.map((y) => y / yscale),
        ),
      );
    for (let k = p - 1; k >= 0; k--)
      beta[k] =
        (qty[k] -
          beta.reduce((sum, b, j) => sum + (j > k ? matrix[k][j] * b : 0), 0)) /
        matrix[k][k];
    const coefficients = Array(p);
    for (let j = 0; j < p; j++)
      coefficients[order[j]] = (beta[j] * yscale) / scales[order[j]];
    const predicted = rows.map((r) => dot(r, coefficients));
    if (
      coefficients.some((v) => !finite(v)) ||
      predicted.some((v) => !finite(v))
    )
      return { ...empty, rank, status: "non-finite fit" };
    const ymean = R.stats(ys).mean / yscale,
      sse = ys.reduce(
        (sum, y, i) => sum + ((y - predicted[i]) / yscale) ** 2,
        0,
      ),
      sst = ys.reduce((sum, y) => sum + (y / yscale - ymean) ** 2, 0);
    return {
      ok: true,
      rank,
      n,
      p,
      coefficients,
      predicted,
      r2: sst > 1e-24 ? 1 - sse / sst : null,
      status: "full-rank least squares; in-sample R²",
    };
  }
  const duplicateModels = [
    {
      id: "inverse",
      names: ["intercept", "1/i", "f", "1-p/100"],
      row: ({ i, f, p }) => [1, 1 / i, f, 1 - p / 100],
    },
    {
      id: "inverse-square",
      names: ["intercept", "1/i²", "f", "1-p/100"],
      row: ({ i, f, p }) => [1, 1 / i ** 2, f, 1 - p / 100],
    },
    {
      id: "exponential-05",
      names: ["intercept", "exp(-0.5i)", "f", "1-p/100"],
      row: ({ i, f, p }) => [1, Math.exp(-0.5 * i), f, 1 - p / 100],
    },
    {
      id: "interaction-01",
      names: ["intercept", "exp(-0.1i)", "f", "1-p/100", "interaction"],
      row: ({ i, f, p }) => [
        1,
        Math.exp(-0.1 * i),
        f,
        1 - p / 100,
        Math.exp(-0.1 * i) * f * (1 - p / 100),
      ],
    },
  ];
  const quadratic = ({ dLow: a, d: b, dHigh: c }) => [
    1,
    a,
    b,
    c,
    a * a,
    b * b,
    c * c,
    a * b,
    a * c,
    b * c,
  ];
  const predict = (fit, row) =>
    fit.ok ? row.reduce((sum, v, i) => sum + v * fit.coefficients[i], 0) : null;
  function meanDistribution(curves) {
    const xs = [...new Set(curves.flatMap((c) => c.map((p) => p.x)))].sort(
      (a, b) => a - b,
    );
    const maps = curves.map((c) => new Map(c.map((p) => [p.x, p.y])));
    return xs.map((x) => ({
      x,
      y: maps.reduce((sum, m) => sum + (m.get(x) || 0), 0) / curves.length,
    }));
  }
  function buildCharts(entries, options = {}) {
    const groups = aggregate(entries),
      charts = [],
      seriesNames = [...new Set(groups.map((g) => g.series))],
      cases = [...new Set(groups.map((g) => g.case))],
      baseline = options.baselineSeries || seriesNames[0],
      reference = options.referenceCase || cases[0],
      chosen = options.metric || "frt",
      source = `Compared saved analyses: ${entries.map((e) => e.analysis.result?.id || e.case).join(", ")}`;
    const add = (id, title, xLabel, yLabel, series, extra = {}) =>
      charts.push({ id, title, xLabel, yLabel, series, source, ...extra });
    const selected = groups.filter(
      (g) => !finite(options.p) || g.params.p === options.p,
    );
    const xValue = (g) =>
      options.x && options.x !== "case"
        ? finite(g.params[options.x])
          ? g.params[options.x]
          : metric(g, options.x).mean
        : cases.indexOf(g.case);
    const repeatNote =
      "Mean and sample SD across independent run summaries. One run has no SD; no low-reachability run is silently removed. Exact case labels define repeats.";
    const available = [
      ...new Set([
        ...Object.keys(R.labels),
        ...groups.flatMap((g) =>
          g.runs.flatMap((a) => Object.keys(a.research?.summary || {})),
        ),
      ]),
    ];
    for (const key of available) {
      if (key !== chosen && !groups.some((g) => finite(metric(g, key).mean)))
        continue;
      add(
        `repeat-${key}`,
        `${R.labels[key] || key} · repeated cases`,
        options.x || "Case",
        R.labels[key] || key,
        seriesNames.map((series) => ({
          name: series,
          points: selected
            .filter((g) => g.series === series)
            .map((g) => {
              const s = metric(g, key);
              return {
                x: xValue(g),
                y: s.mean,
                error: s.sd,
                label: `${g.case}; n=${s.n}`,
              };
            })
            .sort((a, b) => a.x - b.x),
        })),
        {
          xTicks: !options.x || options.x === "case" ? cases : undefined,
          note: repeatNote,
        },
      );
    }
    const colorKey = options.color || "reachability",
      colorValues = selected
        .map((g) =>
          finite(g.params[colorKey])
            ? g.params[colorKey]
            : metric(g, colorKey).mean,
        )
        .filter(finite),
      colorMin = Math.min(...colorValues),
      colorMax = Math.max(...colorValues);
    add(
      "custom-scatter",
      `${chosen} vs ${options.x || "case"}`,
      options.x || "Case",
      R.labels[chosen] || chosen,
      seriesNames.map((series) => ({
        name: series,
        points: selected
          .filter((g) => g.series === series)
          .map((g) => {
            const stat = metric(g, chosen),
              color = finite(g.params[colorKey])
                ? g.params[colorKey]
                : metric(g, colorKey).mean;
            return {
              x: xValue(g),
              y: stat.mean,
              error: stat.sd,
              colorValue: finite(color)
                ? colorMax === colorMin
                  ? 0.5
                  : (color - colorMin) / (colorMax - colorMin)
                : null,
              label: `${g.case}; ${colorKey}=${color ?? "N/A"}`,
            };
          }),
      })),
      {
        mode: "scatter",
        zeroX: false,
        xTicks: !options.x || options.x === "case" ? cases : undefined,
        note: `Color: ${colorKey}, min ${finite(colorMin) ? colorMin : "N/A"} to max ${finite(colorMax) ? colorMax : "N/A"}. Between-run sample SD.`,
      },
    );
    for (const mode of ["difference", "ratio", "division"])
      add(
        `compare-${mode}`,
        `${chosen} · ${mode} to ${baseline}`,
        "Aligned case",
        mode === "difference"
          ? chosen
          : mode === "ratio"
            ? "target / (target + baseline)"
            : "target / baseline",
        seriesNames.map((series) => ({
          name: series,
          points: cases.map((key, x) => {
            const g = selected.find(
                (g) => g.series === series && g.case === key,
              ),
              b = selected.find((g) => g.series === baseline && g.case === key);
            return {
              x,
              ...(g && b
                ? comparison(
                    metric(g, chosen),
                    metric(b, chosen),
                    mode,
                    g === b,
                  )
                : { y: null, error: null }),
            };
          }),
        })),
        {
          xTicks: cases,
          note: "Only identical case labels are paired. Uncertainty uses first-order propagation with independent run errors. Same-input comparisons have zero uncertainty; zero denominators are N/A.",
        },
      );
    const rows = selected
      .map((g) => ({
        g,
        frt: metric(g, "frt").mean,
        drc: metric(g, "drc_per_node_count").mean,
        reach: metric(g, "reachability").mean,
      }))
      .filter((r) => finite(r.frt) && finite(r.drc) && finite(r.reach));
    const tradePoint = (r) => {
      const ref = rows.find(
        (b) => b.g.series === r.g.series && b.g.case === reference,
      );
      return {
        x: r.frt,
        y: r.drc,
        xError: metric(r.g, "frt").sd,
        error: metric(r.g, "drc_per_node_count").sd,
        colorValue: r.reach,
        label: `${r.g.series} / ${r.g.case}; reach=${r.reach}`,
        from: ref && ref !== r ? { x: ref.frt, y: ref.drc } : null,
      };
    };
    add(
      "tradeoff",
      "FRT–duplicate trade-off",
      "First reception time (s)",
      "Duplicates / mean graph nodes",
      seriesNames.map((name) => ({
        name,
        points: rows.filter((r) => r.g.series === name).map(tradePoint),
      })),
      {
        mode: "scatter",
        zeroX: false,
        note: `Color: red (reach 0) to green (reach 1). Arrows from case ${reference} within each series. Shaded ranges: ±1 between-run SD.`,
      },
    );
    for (const [id, three] of [
      ["pareto-2d", false],
      ["pareto-3-objective", true],
    ])
      add(
        id,
        three
          ? "Pareto frontier · delay, duplicates, reach"
          : "Pareto frontier · delay and duplicates",
        "First reception time (s)",
        "Duplicates / mean graph nodes",
        [
          { name: "All eligible cases", points: rows.map(tradePoint) },
          {
            name: "Non-dominated cases",
            points: pareto(rows, three)
              .sort((a, b) => a.frt - b.frt)
              .map(tradePoint),
          },
        ],
        {
          mode: "scatter",
          zeroX: false,
          note: three
            ? "Three-objective dominance: minimize FRT and duplicates; maximize reach. Reach is the color dimension."
            : "Minimize FRT and duplicates. Exact ties are retained.",
        },
      );
    const scored = scores(rows, options.weights || [0.25, 0.25, 0.5]);
    add(
      "tradeoff-score",
      "Normalized trade-off score",
      "Case",
      "Weighted benefit",
      seriesNames.map((name) => ({
        name,
        points: scored
          .filter((r) => r.g.series === name)
          .map((r) => ({ x: cases.indexOf(r.g.case), y: r.score })),
      })),
      {
        xTicks: cases,
        yMax: 1,
        note: `Min–max benefit over selected cases; weights FRT/DRC/reach ${(options.weights || [0.25, 0.25, 0.5]).join("/")}. A constant dimension contributes 0.5. Scores depend on the selected set.`,
      },
    );
    for (const param of ["i", "f"]) {
      const vals = [
        ...new Set(rows.map((r) => r.g.params[param]).filter(finite)),
      ];
      add(
        `tradeoff-by-${param}`,
        `FRT–duplicate trade-off by ${param}`,
        "First reception time (s)",
        "Duplicates / mean graph nodes",
        vals.map((v) => ({
          name: `${param}=${v}`,
          points: rows.filter((r) => r.g.params[param] === v).map(tradePoint),
        })),
        {
          mode: "scatter",
          zeroX: false,
          note: "Explicit i/f/p case parameters only. Reach is shown by color; no parameter is inferred from missing metadata.",
        },
      );
    }
    add(
      "clustering-eigenvector",
      "Clustering vs eigenvector centrality",
      "Clustering coefficient",
      "Mean eigenvector centrality",
      seriesNames.map((name) => ({
        name,
        points: selected
          .filter((g) => g.series === name)
          .map((g) => {
            const ref = selected.find(
              (b) => b.series === name && b.case === reference,
            );
            return {
              x: metric(g, "clustering_coefficient").mean,
              y: metric(g, "eigenvector_centrality").mean,
              label: g.case,
              from: ref
                ? {
                    x: metric(ref, "clustering_coefficient").mean,
                    y: metric(ref, "eigenvector_centrality").mean,
                  }
                : null,
            };
          }),
      })),
      {
        mode: "scatter",
        zeroX: false,
        note: `Arrows use explicit reference case ${reference}.`,
      },
    );
    const lazyKeys = [
      "delta_reachability",
      "net_coverage",
      "overlap",
      "net_efficiency",
      "overlap_ratio",
      "lazy_first_ratio",
      "net_coverage_ratio",
      "per_iwant",
    ];
    for (const key of lazyKeys)
      add(
        `lazy-${key}`,
        `Metadata-based lazy contribution · ${key}`,
        "Aligned case",
        key,
        seriesNames
          .filter((n) => n !== baseline)
          .map((series) => ({
            name: series,
            points: cases.map((label, x) => {
              const on = selected.find(
                  (g) => g.series === series && g.case === label,
                ),
                off = selected.find(
                  (g) => g.series === baseline && g.case === label,
                );
              return { x, y: on && off ? lazyMetrics(on, off)[key] : null };
            }),
          })),
        {
          xTicks: cases,
          note: `Baseline series ${baseline} must be an explicitly matched lazy-off experiment. Eager/lazy labels are metadata estimates. Δreach is observational, not causal proof; negative/over-one estimates remain visible. IWANT is per published message.`,
        },
      );
    for (const model of duplicateModels) {
      const rows = selected.filter(
          (g) =>
            g.params.i > 0 &&
            finite(g.params.f) &&
            g.params.p >= 0 &&
            g.params.p <= 100 &&
            finite(metric(g, "drc_per_node_count").mean),
        ),
        ys = rows.map((g) => metric(g, "drc_per_node_count").mean),
        fit = regress(
          rows.map((g) => model.row(g.params)),
          ys,
        );
      const note = `${fit.status}; n=${fit.n}, rank=${fit.rank}/${fit.p || model.names.length}, R²=${fit.r2 ?? "N/A"}. Fits case means once per case. No held-out validation.`;
      add(
        `dup-model-${model.id}`,
        `Duplicate regression · ${model.id}`,
        "Observed duplicates / node",
        "Fitted duplicates / node",
        [
          {
            name: "Observed vs fitted",
            points: fit.ok
              ? ys.map((x, i) => ({
                  x,
                  y: fit.predicted[i],
                  label: rows[i].case,
                }))
              : [],
          },
        ],
        { mode: "scatter", note },
      );
      add(
        `dup-coefficients-${model.id}`,
        `Regression coefficients · ${model.id}`,
        "Term",
        "Coefficient",
        [
          {
            name: "Least squares",
            points: (fit.coefficients || []).map((y, x) => ({ x, y })),
          },
        ],
        { mode: "bar", xTicks: model.names, note },
      );
    }
    const degreeRows = selected.filter((g) =>
      [g.params.dLow, g.params.d, g.params.dHigh].every(finite),
    );
    const targets = ["inverse_df", "location", "log_scale"],
      multi = {};
    for (const target of targets) {
      for (const param of ["dLow", "d", "dHigh"]) {
        const others = ["dLow", "d", "dHigh"].filter((k) => k !== param),
          ref = degreeRows.find((g) => g.case === reference),
          rows = degreeRows.filter(
            (g) =>
              finite(metric(g, target).mean) &&
              ref &&
              others.every((k) => g.params[k] === ref.params[k]),
          );
        const fit = regress(
            rows.map((g) => [1, g.params[param], g.params[param] ** 2]),
            rows.map((g) => metric(g, target).mean),
          ),
          xs = rows.map((g) => g.params[param]),
          grid = xs.length
            ? Array.from(
                { length: 80 },
                (_, i) =>
                  Math.min(...xs) +
                  ((Math.max(...xs) - Math.min(...xs)) * i) / 79,
              )
            : [];
        add(
          `degree-quadratic-${param}-${target}`,
          `Degree fit ${target} vs ${param}`,
          param,
          target,
          [
            {
              name: "Observed Student-t parameter",
              points: rows.map((g) => ({
                x: g.params[param],
                y: metric(g, target).mean,
              })),
            },
            {
              name: "Quadratic prediction",
              points: grid.map((x) => ({ x, y: predict(fit, [1, x, x * x]) })),
            },
          ],
          {
            note: `Other D parameters fixed to reference case ${reference}. ${fit.status}; R²=${fit.r2 ?? "N/A"}. Quadratic, as implemented in v2 (not cubic).`,
          },
        );
      }
      const rows = degreeRows.filter((g) => finite(metric(g, target).mean)),
        ys = rows.map((g) => metric(g, target).mean),
        fit = regress(
          rows.map((g) => quadratic(g.params)),
          ys,
        );
      multi[target] = fit;
      add(
        `degree-multivariate-${target}`,
        `Degree parameter model · ${target}`,
        "Observed parameter",
        "Predicted parameter",
        [
          {
            name: "Quadratic fit",
            points: fit.ok
              ? ys.map((x, i) => ({
                  x,
                  y: fit.predicted[i],
                  label: rows[i].case,
                }))
              : [],
          },
        ],
        {
          mode: "scatter",
          note: `Basis: 1,a,b,c,a²,b²,c²,ab,ac,bc. ${fit.status}; rank ${fit.rank}/10; R²=${fit.r2 ?? "N/A"}. In-sample predictions.`,
        },
      );
      add(
        `degree-multivariate-coefficients-${target}`,
        `Degree model coefficients · ${target}`,
        "Term",
        "Coefficient",
        [
          {
            name: "Quadratic coefficients",
            points: (fit.coefficients || []).map((y, x) => ({ x, y })),
          },
        ],
        {
          mode: "bar",
          xTicks: ["1", "a", "b", "c", "a²", "b²", "c²", "ab", "ac", "bc"],
          note: `a=Dlow, b=D, c=Dhigh. ${fit.status}`,
        },
      );
    }
    for (const [i, g] of degreeRows.entries()) {
      const row = quadratic(g.params),
        inv = predict(multi.inverse_df, row),
        loc = predict(multi.location, row),
        logScale = predict(multi.log_scale, row),
        scale = finite(logScale) ? Math.exp(logScale) : null,
        df = finite(inv) && inv > 0 ? 1 / inv : null;
      const observed = meanDistribution(
          g.runs.map((a) => a.research?.degreeDistribution || []),
        ),
        fitted = R.averageCurves(
          g.runs.map((a) => a.research?.degreeFit?.density || []),
        );
      const predicted = observed.map((p) => ({
        x: p.x,
        y: studentPDF(p.x, df, loc, scale),
      }));
      add(
        `degree-observed-fit-prediction-${i}`,
        `Degree distribution · ${g.series} / ${g.case}`,
        "Degree",
        "Probability / continuous density",
        [
          { name: "Observed degree probability", points: observed },
          { name: "Per-run Student-t density", points: fitted },
          { name: "Multivariate predicted density", points: predicted },
        ],
        {
          note: "Observed integer-degree probabilities and continuous model densities have different units. Invalid model parameters stay N/A. No forced uniform distribution or synthetic observations.",
        },
      );
    }
    if (options.predictions?.length)
      for (const [i, params] of options.predictions.entries()) {
        const inv = predict(multi.inverse_df, quadratic(params)),
          location = predict(multi.location, quadratic(params)),
          logScale = predict(multi.log_scale, quadratic(params)),
          scale = finite(logScale) ? Math.exp(logScale) : null,
          df = finite(inv) && inv > 0 ? 1 / inv : null;
        const degreeMax = Math.max(
            1,
            finite(params.dHigh) ? params.dHigh * 2 : 32,
          ),
          points =
            finite(df) && finite(location) && finite(scale) && scale > 0
              ? Array.from({ length: 241 }, (_, j) => {
                  const x = (degreeMax * j) / 240;
                  return { x, y: studentPDF(x, df, location, scale) };
                })
              : [];
        add(
          `degree-predicted-${i}`,
          `Predicted degree distribution · ${params.dLow}-${params.d}-${params.dHigh}`,
          "Degree",
          "Continuous density",
          [{ name: "Multivariate parameter prediction", points }],
          {
            note: `df=${df ?? "N/A"}, location=${location ?? "N/A"}, scale=${scale ?? "N/A"}. Invalid predicted parameters are N/A; extrapolation is not clamped into a valid-looking fit.`,
          },
        );
      }
    for (const key of [
      "degreeDistribution",
      "propagationCDF",
      "duplicateCDF",
      "hopPDF",
      "hopCDF",
      "eagerCDF",
      "lazyCDF",
    ])
      for (const normalize of [false, true])
        add(
          `overlay-${key}-${normalize ? "peak" : "raw"}`,
          `${key} · ${normalize ? "peak-normalized overlay" : "overlay"}`,
          key.toLowerCase().includes("hop")
            ? "Hop"
            : key === "degreeDistribution"
              ? "Degree"
              : "Time (s)",
          normalize ? "Relative peak (max = 1)" : "Original value",
          groups.map((g) => {
            const points = (
                key.endsWith("CDF") ? R.averageCurves : meanDistribution
              )(g.runs.map((a) => a.research?.[key] || [])),
              peak = points.reduce((m, p) => Math.max(m, p.y), 0);
            return {
              name: `${g.series} / ${g.case}`,
              points: normalize
                ? points.map((p) => ({ x: p.x, y: divide(p.y, peak) }))
                : points,
            };
          }),
          {
            mode: key.endsWith("CDF") ? "step" : "line",
            note: normalize
              ? "Each curve divided by its own peak; original counts and probability mass are not preserved. Zero curves remain N/A."
              : "Equal run weight on a common grid; CDFs use right-continuous steps. Original CSV points remain downloadable.",
          },
        );
    return charts;
  }
  // Lanczos log-gamma, for plotting validated Student-t model predictions.
  function logGamma(z) {
    const c = [
      676.5203681218851, -1259.1392167224028, 771.32342877765313,
      -176.61502916214059, 12.507343278686905, -0.13857109526572012,
      9.9843695780195716e-6, 1.5056327351493116e-7,
    ];
    if (z < 0.5)
      return (
        Math.log(Math.PI) - Math.log(Math.sin(Math.PI * z)) - logGamma(1 - z)
      );
    z -= 1;
    let x = 0.99999999999980993;
    for (let i = 0; i < c.length; i++) x += c[i] / (z + i + 1);
    const t = z + 7.5;
    return (
      0.5 * Math.log(2 * Math.PI) + (z + 0.5) * Math.log(t) - t + Math.log(x)
    );
  }
  function studentPDF(x, df, loc, scale) {
    if (![x, df, loc, scale].every(finite) || df <= 0 || scale <= 0)
      return null;
    const y = Math.exp(
      logGamma((df + 1) / 2) -
        logGamma(df / 2) -
        0.5 * Math.log(df * Math.PI) -
        Math.log(scale) -
        ((df + 1) / 2) * Math.log1p(((x - loc) / scale) ** 2 / df),
    );
    return finite(y) ? y : null;
  }
  function referenceCharts() {
    const data =
      typeof module !== "undefined" && module.exports
        ? require("./research-v2-reference.js")
        : root.KPLV2Reference;
    const er = data.er_csv
      .trim()
      .split("\n")
      .slice(1)
      .map((line) => line.split(",").map(Number));
    const source =
      "FIXED HISTORICAL REFERENCE · v2/kpl-viewer/old/frt_drc_graph.py · not a selected run";
    const delay = {
      id: "v2-reference-frt",
      title: "v2 historical reference · first reception time",
      xLabel: "Degree",
      yLabel: "FRT (s)",
      source,
      series: [
        {
          name: "v2 stored empirical values",
          points: data.emp_delay.degree.map((x, i) => ({
            x,
            y: data.emp_delay.mean[i],
            error: data.emp_delay.yerr[i],
          })),
        },
        {
          name: "v2 stored ER model",
          points: er.map((row) => ({ x: row[0], y: row[2], error: row[6] })),
        },
      ],
      note: "Original supplied yerr retained; empirical error type was not established by the source. ER shaded ranges are reported SE.",
    };
    const duplicate = {
      id: "v2-reference-duplicates",
      title: "v2 historical reference · duplicates per node",
      xLabel: "Degree",
      yLabel: "Duplicates / node",
      source,
      series: [
        {
          name: "v2 stored empirical values",
          points: data.emp_dup_total.degree.map((x, i) => {
            const A = data.emp_dup_total.dup_total[i],
              N = data.node_counts.nodes_mean[i];
            return {
              x,
              y: A / N,
              error: Math.hypot(
                data.emp_dup_total.dup_total_se[i] / N,
                (A * data.node_counts.nodes_se[i]) / N ** 2,
              ),
            };
          }),
        },
        {
          name: "v2 stored ER model",
          points: er.map((row) => ({ x: row[0], y: row[4], error: row[7] })),
        },
      ],
      note: "Ratio uncertainty assumes independent total-copy and node-count errors, as in the reference code.",
    };
    return [
      delay,
      duplicate,
      {
        id: "v2-reference-panels",
        title: "v2 fixed historical / ER reference",
        source,
        series: [],
        panels: [delay, duplicate],
      },
    ];
  }
  const exported = {
    meanDistribution,
    referenceCharts,
    parameters,
    aggregate,
    metric,
    comparison,
    pareto,
    scores,
    lazyMetrics,
    regress,
    duplicateModels,
    quadratic,
    predict,
    buildCharts,
    studentPDF,
  };
  if (typeof module !== "undefined" && module.exports)
    module.exports = exported;
  else root.KPLResearchCompare = exported;
})(globalThis);

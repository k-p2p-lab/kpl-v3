(function (root) {
  "use strict";
  function create({
    document: doc,
    request,
    render,
    status,
    onJob,
    getData,
    escape,
  }) {
    const $ = (id) => doc.querySelector(`#${id}`),
      panel = $("researchTools");
    if (!panel) return null;
    const R = root.KPLResearch,
      C = root.KPLResearchCompare,
      F = root.KPLResearchFiles;
    let work = null,
      imports = [],
      curves = [],
      saved = [];
    const optionsHTML = (values) =>
      values
        .map(
          (v) =>
            `<option value="${escape(v)}">${escape(R.labels[v] || v)}</option>`,
        )
        .join("");
    $("researchMetric").innerHTML = optionsHTML(Object.keys(R.labels));
    $("researchMetric").value = "frt";
    $("researchX").innerHTML = optionsHTML([
      "case",
      "i",
      "f",
      "p",
      "dLow",
      "d",
      "dHigh",
      ...Object.keys(R.labels),
    ]);
    $("researchColor").innerHTML = optionsHTML([
      "reachability",
      "i",
      "f",
      "p",
      "dLow",
      "d",
      "dHigh",
      ...Object.keys(R.labels).filter((k) => k !== "reachability"),
    ]);
    const stop = () => {
      work?.abort();
      work = null;
    };
    const pause = (signal) =>
      new Promise((resolve, reject) => {
        const finish = () => {
            signal.removeEventListener("abort", abort);
            resolve();
          },
          timer = setTimeout(finish, 2000),
          abort = () => {
            clearTimeout(timer);
            reject(new DOMException("Closed", "AbortError"));
          };
        if (signal.aborted) abort();
        else signal.addEventListener("abort", abort, { once: true });
      });
    async function action(fn) {
      stop();
      const view = new AbortController();
      work = view;
      try {
        await fn(view.signal);
      } catch (e) {
        if (!view.signal.aborted) {
          status(e.message, true);
          if (e.status === 401) $("resultImagesAuth").hidden = false;
        }
      } finally {
        if (work === view) work = null;
      }
    }
    function rows() {
      const entries = [
        ...saved.map((a) => ({
          id: a.id,
          name: a.name || a.id,
          case: a.name || a.id,
          series: "Experiment",
          analysis: null,
        })),
        ...imports.map((e, i) => ({
          ...e,
          id: `import-${i}`,
          name: e.analysis.result?.name || e.case,
        })),
      ];
      $("researchRuns").innerHTML = entries
        .map(
          (e, i) =>
            `<tr data-research-row="${i}"><td><input type="checkbox" aria-label="Select ${escape(e.name)}" ${e.id === getData()?.result?.id || e.analysis ? "checked" : ""}></td><td title="${escape(e.id)}">${escape(e.name)}</td><td><input data-field="series" aria-label="Series for ${escape(e.name)}" value="${escape(e.series)}"></td><td><input data-field="case" aria-label="Case for ${escape(e.name)}" value="${escape(e.case)}"></td><td><input data-field="params" aria-label="Parameters for ${escape(e.name)}" placeholder="i=1,f=1,p=100 or dLow=4,d=8,dHigh=12" value="${escape(
              Object.entries(e.params || {})
                .map(([k, v]) => `${k}=${v}`)
                .join(","),
            )}"></td></tr>`,
        )
        .join("");
      return entries;
    }
    let displayed = [];
    $("loadResearchRuns").addEventListener(
      "click",
      () =>
        void action(async (signal) => {
          status("Loading saved results…");
          const result = await request("/api/v1/results", {}, signal);
          if (!Array.isArray(result))
            throw new Error("Unexpected saved results response.");
          saved = result.filter(
            (r) => !["unreadable", "queued"].includes(r.state),
          );
          displayed = rows();
          status(
            "Select runs and assign exact series/case labels. Equal labels define repeats.",
          );
        }),
    );
    $("researchFiles").addEventListener(
      "change",
      (event) =>
        void action(async (signal) => {
          for (const file of event.target.files || []) {
            if (file.size > 32 * 1024 * 1024)
              throw new Error(`${file.name} exceeds 32 MiB.`);
            const text = await file.text();
            if (signal.aborted) return;
            const parsed = F.parseImport(
              text,
              file.name,
              $("researchMetric").value,
            );
            if (signal.aborted) return;
            imports.push(...parsed.entries);
            curves.push(...parsed.curves);
          }
          displayed = rows();
          status(
            `${imports.length} imported cases and ${curves.length} curves available. Files stay in this browser.`,
          );
        }),
    );
    $("clearResearchImports").addEventListener("click", () => {
      stop();
      imports = [];
      curves = [];
      $("researchFiles").value = "";
      displayed = rows();
      status("Imported data cleared.");
    });
    $("drawImportedCurves").addEventListener(
      "click",
      () =>
        void action(async () => {
          if (!curves.length)
            throw new Error("Choose CSV or numeric x/y JSON files first.");
          await render(F.curveCharts(curves), "imported-curves");
        }),
    );
    $("drawResearchCompare").addEventListener(
      "click",
      () =>
        void action(async (signal) => {
          const selected = [];
          for (const row of $("researchRuns").querySelectorAll(
            "tr[data-research-row]",
          )) {
            if (!row.querySelector('input[type="checkbox"]').checked) continue;
            const e = displayed[Number(row.dataset.researchRow)],
              series = row.querySelector('[data-field="series"]').value.trim(),
              label = row.querySelector('[data-field="case"]').value.trim(),
              params = {};
            if (!series || !label)
              throw new Error(
                "Every selected result needs series and case labels.",
              );
            for (const part of row
              .querySelector('[data-field="params"]')
              .value.split(",")
              .filter((v) => v.trim())) {
              const [key, value] = part.split("=").map((v) => v.trim());
              if (
                !["i", "f", "p", "dLow", "d", "dHigh"].includes(key) ||
                value === "" ||
                !Number.isFinite(Number(value))
              )
                throw new Error(`Invalid parameter ${part}.`);
              params[key] = Number(value);
            }
            selected.push({ ...e, series, case: label, params });
          }
          if (!selected.length)
            throw new Error("Select saved results or import v2 data first.");
          if (selected.filter((e) => !e.analysis).length > 32)
            throw new Error("Compare at most 32 saved runs at a time.");
          // Admit all selected jobs first. They outlive this browser view.
          for (const e of selected)
            if (!e.analysis) {
              const path = `/api/v1/analysis-jobs/${encodeURIComponent(e.id)}`;
              let job = await request(path, {}, signal);
              if (
                !["queued", "running"].includes(job.state) &&
                (job.state !== "completed" || (job.analysisVersion || 0) < 4)
              )
                job = await request(path, { method: "POST" }, signal);
              onJob(job);
              e.job = job;
            }
          for (const [index, e] of selected.entries())
            if (!e.analysis) {
              const path = `/api/v1/analysis-jobs/${encodeURIComponent(e.id)}`;
              let job = e.job;
              while (["queued", "running"].includes(job.state)) {
                status(
                  `Comparison ${index + 1}/${selected.length} · ${e.name} · ${root.KPLResultImages.jobDescription(job)}`,
                );
                await pause(signal);
                job = await request(path, {}, signal);
                onJob(job);
              }
              if (job.state !== "completed")
                throw new Error(
                  `${e.name}: ${job.error || job.state}. Generate comparison again to retry.`,
                );
              e.analysis = await request(
                `${path}/summary?jobId=${encodeURIComponent(job.id)}`,
                {},
                signal,
              );
              if (
                e.analysis.result?.id !== e.id ||
                e.analysis.analysisId !== job.id
              )
                throw new Error(
                  "A selected analysis changed. Generate comparison again.",
                );
            }
          const weights = $("researchWeights").value.split(",").map(Number),
            predictions = [];
          for (const line of $("researchPredictions")
            .value.split(/\r?\n/)
            .filter((l) => l.trim())) {
            const values = line.split(",").map(Number);
            if (values.length !== 3 || values.some((v) => !Number.isFinite(v)))
              throw new Error(
                "Predictions use Dlow,D,Dhigh, one row per parameter set.",
              );
            predictions.push({
              dLow: values[0],
              d: values[1],
              dHigh: values[2],
            });
          }
          const pText = $("researchP").value.trim(),
            p = pText === "" ? undefined : Number(pText);
          if (p !== undefined && (!Number.isFinite(p) || p < 0 || p > 100))
            throw new Error("p must be 0–100 or empty.");
          const opts = {
            metric: $("researchMetric").value,
            x: $("researchX").value,
            color: $("researchColor").value,
            baselineSeries: $("researchBaseline").value.trim(),
            referenceCase: $("researchReference").value.trim(),
            weights,
            p,
            predictions,
          };
          await render(C.buildCharts(selected, opts), "comparison", {
            entries: selected.map(
              ({ analysis, series, case: label, params }) => ({
                analysis,
                series,
                case: label,
                params,
              }),
            ),
            options: opts,
          });
        }),
    );
    $("drawResearchMessage").addEventListener(
      "click",
      () =>
        void action(async () => {
          const data = getData();
          if (!data) throw new Error("Complete this result analysis first.");
          await render(
            R.messageCharts(data, Number($("researchMessage").value)),
            `${data.result.id}-message`,
          );
        }),
    );
    $("drawResearchOverview").addEventListener(
      "click",
      () =>
        void action(async () => {
          const data = getData();
          if (data)
            await render(
              root.KPLResultImages.buildCharts(data),
              data.result.id,
            );
        }),
    );
    $("drawV2Reference").addEventListener(
      "click",
      () =>
        void action(async () => {
          await render(C.referenceCharts(), "v2-historical-reference");
        }),
    );
    return {
      cancel: stop,
      setData(data) {
        panel.hidden = false;
        const messages = data.research?.messages || [];
        $("researchMessage").innerHTML = messages
          .map(
            (m, i) =>
              `<option value="${i}">${escape(`${m.topic} / ${m.id}`)}</option>`,
          )
          .join("");
        $("drawResearchMessage").disabled = !messages.length;
      },
    };
  }
  root.KPLResearchTools = { create };
})(globalThis);

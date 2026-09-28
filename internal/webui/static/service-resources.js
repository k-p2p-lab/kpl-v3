(function(root) {
  "use strict";
  const escape = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[c]);
  const number = value => value.toLocaleString("en-US", {maximumFractionDigits:1});
  const bytes = value => { const i = value > 0 ? Math.min(4, Math.floor(Math.log(value)/Math.log(1024))) : 0; return `${number(value/1024**i)} ${["B","KiB","MiB","GiB","TiB"][i]}`; };
  const names = {controller:"Controller", prometheus:"Prometheus", grafana:"Grafana", "resource-monitor":"Resource monitor"};
  function describe(row, now = Date.now()) {
    const age = now - Date.parse(row.reportedAt), r = row.resources;
    let state = row.state;
    if (state !== "awaiting" && (!Number.isFinite(age) || age < -5000 || age > 30000)) state = "stale";
    const available = state === "running" && root.KPLAgentResources.valid({state:"online",resources:r}, now);
    const percent = available && Number.isInteger(r.cpuCapacityCores) && r.cpuCapacityCores > 0 ? 100 * r.cpuCores/r.cpuCapacityCores : null;
    return {name:names[row.service] || row.service, state,
      status:({running:"Running",not_running:"Not running",stale:"Collector unavailable",unknown:"Inventory unavailable",awaiting:"Awaiting collector"})[state] || "Unknown",
      cpu:Number.isFinite(percent) ? `${number(percent)}%` : "N/A", memory:available ? bytes(r.memoryWorkingSetBytes) : "N/A",
      coverage:available ? `${r.complete ? "" : "Partial · "}${r.measuredContainers}/${r.containers} containers` : state === "running" ? "Resource sample unavailable" : "No current measurement",
      title:available ? `CPU: 100% is this Docker host. Memory including cache: ${bytes(r.memoryUsageBytes)}. Sampled ${new Date(r.sampledAt).toISOString()}.` : row.error || r?.error || "Waiting for a fresh service resource sample.",
    };
  }
  function cards(rows, now) {
    return rows.map(row => {
      const d = describe(row, now);
      return `<article class="service-resource-card" data-service="${escape(row.service)}"><div class="service-resource-heading"><h4>${escape(d.name)}</h4><span class="service-resource-state" data-state="${escape(d.state)}">${escape(d.status)}</span></div><p class="service-resource-host" title="${escape(row.nodeId)}">${escape(row.nodeName || row.nodeId || "Host not reported")}</p><dl class="service-resource-values" title="${escape(d.title)}"><div><dt>CPU</dt><dd>${escape(d.cpu)}</dd></div><div><dt>Memory</dt><dd>${escape(d.memory)}</dd></div></dl><p class="service-resource-coverage">${escape(d.coverage)}</p></article>`;
    }).join("");
  }
  function init({api}) {
    const $ = id => root.document.querySelector(id);
    const container = $("#serviceResourceCards"), note = $("#serviceResourceStatus");
    if (!container || !note) return;
    let pending = false, next = 0, data = null, receivedAt = 0, lastHTML = "", failure = "";
    const visible = () => !root.document.hidden && !$("#agents")?.hidden;
    function render() {
      if (!data) { if (failure) note.textContent = failure; return; }
      const html = cards(data.services, Date.now());
      if (html !== lastHTML) { container.innerHTML = html; lastHTML = html; }
      const hint = $("#serviceResourceScrollHint");
      if (hint) hint.hidden = !(container.scrollHeight > container.clientHeight);
      const age = Math.max(0, Math.floor((Date.now()-receivedAt)/1000));
      const uptime = Math.max(0, Math.floor((data.controllerUptimeSeconds+age)/60));
      note.textContent = failure || `Controller uptime ${number(uptime)} min · Updated ${age}s ago · Included in interval measurements and CSV downloads.`;
    }
    async function refresh() {
      if (pending || !visible()) return;
      pending = true;
      const controller = new AbortController(), timer = root.setTimeout(() => controller.abort(), 8000);
      try {
        const response = await api("/api/v1/services/resources", {signal:controller.signal});
        if (!Array.isArray(response.services)) throw Error("Invalid service resource response.");
        data = response; receivedAt = Date.now(); failure = "";
      } catch (error) { failure = `Service status refresh failed. ${error.name === "AbortError" ? "Request timed out." : error.message}`; }
      finally { root.clearTimeout(timer); pending = false; next = Date.now()+10000; render(); }
    }
    root.document.addEventListener("visibilitychange", () => { if (visible()) {render(); if (Date.now() >= next) refresh();} });
    $("#tab-agents")?.addEventListener("click", () => {root.setTimeout(() => {render(); if (Date.now() >= next) refresh();}, 0);});
    $("#refreshAgents")?.addEventListener("click", refresh);
    root.setInterval(() => { if (!visible()) return; render(); if (Date.now() >= next) refresh(); }, 1000);
    refresh();
  }
  const exports = {describe, cards, init};
  if (typeof module !== "undefined" && module.exports) module.exports = exports;
  else root.KPLServiceResources = exports;
})(globalThis);

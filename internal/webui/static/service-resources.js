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
    return {name:names[row.service] || row.service, state, available, cpuPercent:percent,
      status:({running:"Running",not_running:"Not running",stale:"Collector unavailable",unknown:"Inventory unavailable",awaiting:"Awaiting collector"})[state] || "Unknown",
      cpu:Number.isFinite(percent) ? `${number(percent)}%` : "N/A", memory:available ? bytes(r.memoryWorkingSetBytes) : "N/A",
      coverage:available ? `${r.complete ? "" : "Partial · "}${r.measuredContainers}/${r.containers} containers` : state === "running" ? "Resource sample unavailable" : "No current measurement",
      title:available ? `CPU: 100% is this Docker host. Memory including cache: ${bytes(r.memoryUsageBytes)}. Sampled ${new Date(r.sampledAt).toISOString()}.` : row.error || r?.error || "Waiting for a fresh service resource sample.",
    };
  }
  function aggregate(rows, now = Date.now()) {
    const total = {total:rows.length, running:0, measured:0, cpuMeasured:0, cpuPercent:null, cpuCapacityCores:0, memoryWorkingSetBytes:0, memoryUsageBytes:0, containers:0, measuredContainers:0, partial:0, hosts:0};
    const cpuHosts = new Map(), reportedHosts = new Set();
    let cpuCores = 0;
    for (const row of rows) {
      const d = describe(row, now), r = row.resources;
      if (row.nodeId) reportedHosts.add(row.nodeId);
      if (d.state === "running") {
        total.running++;
        if (Number.isInteger(r?.containers) && r.containers > 0) total.containers += r.containers;
      }
      if (!d.available) continue;
      total.measured++;
      total.measuredContainers += r.measuredContainers;
      total.memoryWorkingSetBytes += r.memoryWorkingSetBytes;
      total.memoryUsageBytes += r.memoryUsageBytes;
      if (!r.complete) total.partial++;
      // Services can share a host; count its CPU capacity once in the denominator.
      // An unidentified host cannot safely contribute a CPU denominator.
      if (row.nodeId && Number.isFinite(d.cpuPercent)) {
        cpuHosts.set(row.nodeId, Math.max(cpuHosts.get(row.nodeId) || 0, r.cpuCapacityCores));
        cpuCores += r.cpuCores;
        total.cpuMeasured++;
      }
    }
    total.hosts = reportedHosts.size;
    total.cpuCapacityCores = [...cpuHosts.values()].reduce((sum, capacity) => sum + capacity, 0);
    if (total.cpuCapacityCores > 0) total.cpuPercent = 100 * cpuCores / total.cpuCapacityCores;
    return total;
  }
  function tableRows(rows, now = Date.now()) {
    const a = aggregate(rows, now);
    const partial = a.partial || a.measured < a.total || a.cpuMeasured < a.measured;
    const messages = [`${number(a.measured)} / ${number(a.total)} services measured`];
    if (a.partial) messages.push(`Partial · ${number(a.measuredContainers)}/${number(a.containers)} containers`);
    if (a.cpuMeasured !== a.measured) messages.push(`CPU coverage: ${number(a.cpuMeasured)} / ${number(a.total)} services measured`);
    const footer = partial ? root.KPLAgentResources.noticeRow(messages.join(" · "), {columns:6, total:true}) : "";
    const total = `<tr class="resource-total-row${footer ? " resource-has-notice" : ""}" data-resource-total="services">
      <th scope="row"><span class="resource-total-label">Total</span><span class="agent-capacity-note">${number(a.total)} services</span></th>
      <td>${number(a.running)} / ${number(a.total)} running</td>
      <td>${number(a.hosts)} hosts</td>
      <td title="CPU uses each measured host's capacity once; services with an unknown host or CPU capacity are excluded."><span class="agent-resource-value">${a.cpuPercent === null ? "N/A" : `${number(a.cpuPercent)}%`}</span></td>
      <td title="Measured working set; memory including cache: ${a.measured ? bytes(a.memoryUsageBytes) : "N/A"}."><span class="agent-resource-value">${a.measured ? bytes(a.memoryWorkingSetBytes) : "N/A"}</span></td>
      <td>${a.measured ? `${number(a.measuredContainers)}/${number(a.containers)} containers` : "—"}</td>
    </tr>${footer}`;
    return total + (rows.length ? rows.map(row => {
      const d = describe(row, now), messages = [];
      if (!d.available) messages.push(d.state === "running" ? d.coverage : `${d.status} · No current measurement`);
      else {
        if (!row.resources.complete) messages.push(d.coverage);
        if (!Number.isFinite(d.cpuPercent)) messages.push("Host CPU count unavailable");
      }
      const footer = root.KPLAgentResources.noticeRow(messages.join(" · "), {columns:6});
      return `<tr${footer ? ' class="resource-has-notice"' : ""} data-service="${escape(row.service)}"><th scope="row" class="service-resource-name">${escape(d.name)}</th><td><span class="service-resource-state" data-state="${escape(d.state)}">${escape(d.status)}</span></td><td class="service-resource-host" title="${escape(row.nodeId)}">${escape(row.nodeName || row.nodeId || "Host not reported")}</td><td class="agent-resource-value" title="${escape(d.title)}">${escape(d.cpu)}</td><td class="agent-resource-value" title="${escape(d.title)}">${escape(d.memory)}</td><td>${d.available ? `${number(row.resources.measuredContainers)}/${number(row.resources.containers)} containers` : "—"}</td></tr>${footer}`;
    }).join("") : '<tr><td colspan="6" class="empty-cell">No service resources reported.</td></tr>');
  }
  function init({api}) {
    const $ = id => root.document.querySelector(id);
    const container = $("#serviceResourceRows"), table = $("#serviceResourceTable"), note = $("#serviceResourceStatus");
    if (!container || !note) return;
    let pending = false, next = 0, data = null, receivedAt = 0, lastHTML = "", failure = "";
    const visible = () => !root.document.hidden && !$("#agents")?.hidden;
    function render() {
      if (!data) { if (failure) note.textContent = failure; return; }
      const html = tableRows(data.services, Date.now());
      if (html !== lastHTML) { container.innerHTML = html; lastHTML = html; }
      const hint = $("#serviceResourceScrollHint");
      if (hint && table) hint.hidden = !(table.scrollHeight > table.clientHeight || table.scrollWidth > table.clientWidth);
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
  const exports = {describe, aggregate, tableRows, init};
  if (typeof module !== "undefined" && module.exports) module.exports = exports;
  else root.KPLServiceResources = exports;
})(globalThis);

(function(root) {
  "use strict";
  const number = value => value.toLocaleString("en-US", {maximumFractionDigits: 1});
  const bytes = value => {
    if (value === 0) return "0 B";
    const unit = Math.min(4, Math.floor(Math.log(value) / Math.log(1024)));
    return `${number(value / 1024 ** unit)} ${["B", "KiB", "MiB", "GiB", "TiB"][unit]}`;
  };
  const escape = value => String(value ?? "").replace(/[&<>"']/g, char => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[char]);
  const scopeLabels = {agent_and_peers:"Agent + Peers", agent:"Agent only", peers:"Peers only"};
  function sample(agent, scope) {
    const r = agent.resources;
    if (scope === "agent_and_peers") return r;
    return r?.[scope] ? {...r[scope], sampledAt:r.sampledAt, cpuCapacityCores:r.cpuCapacityCores, error:r.error} : null;
  }
  function valid(agent, now = Date.now(), scope = "agent_and_peers") {
    const r = sample(agent, scope), age = now - Date.parse(r?.sampledAt);
    const empty = scope === "peers" && r?.containers === 0 && r.measuredContainers === 0 && r.complete && r.cpuCores === 0 && r.memoryUsageBytes === 0 && r.memoryWorkingSetBytes === 0;
    return !agent.disabled && agent.state === "online" && !!r && (empty || r.containers > 0 && r.measuredContainers > 0) &&
      Number.isInteger(r.containers) && Number.isInteger(r.measuredContainers) && r.measuredContainers <= r.containers &&
      (scope !== "agent" || r.containers === 1) && (!r.complete || r.containers === r.measuredContainers) && age >= -5000 && age <= 30000 &&
      Number.isFinite(r.cpuCores) && r.cpuCores >= 0 && Number.isFinite(r.memoryUsageBytes) && r.memoryUsageBytes >= 0 &&
      Number.isFinite(r.memoryWorkingSetBytes) && r.memoryWorkingSetBytes >= 0 && r.memoryWorkingSetBytes <= r.memoryUsageBytes;
  }
  function cpuPercent(r) {
    const percent = Number.isInteger(r.cpuCapacityCores) && r.cpuCapacityCores > 0 ? r.cpuCores / r.cpuCapacityCores * 100 : null;
    return Number.isFinite(percent) ? percent : null;
  }
  function describe(agent, now = Date.now(), scope = "agent_and_peers") {
    const r = sample(agent, scope);
    if (agent.disabled) return {cpu:"Excluded", memory:"Excluded", detail:"Agent disabled", memoryDetail:"Agent disabled", title:"Disabled Agents are excluded from hardware monitoring totals.", available:false};
    if (!valid(agent, now, scope)) {
      const age = now - Date.parse(r?.sampledAt);
      const status = agent.state !== "online" ? "Offline" : !r ? (scope === "agent_and_peers" ? "Awaiting sample" : "Breakdown unavailable") : age > 30000 || age < -5000 ? "Stale sample" : `Unavailable · ${r.measuredContainers || 0}/${r.containers || 0} containers`;
      return {cpu:"N/A", memory:"N/A", detail:status, memoryDetail:status, title:r?.error || status, available:false};
    }
    const percent = cpuPercent(r), partial = !r.complete;
    const coverage = `${partial ? "Partial · " : ""}${r.measuredContainers}/${r.containers} containers`;
    return {cpu:percent === null ? "N/A" : `${number(percent)}%`, memory:bytes(r.memoryWorkingSetBytes), detail:percent === null ? `Host CPU count unavailable · ${coverage}` : coverage,
      memoryDetail:partial ? `Working set · ${coverage}` : "Working set", partial,
      title:`${scopeLabels[scope]}; ${r.measuredContainers}/${r.containers} containers measured. ${percent === null ? "Host CPU count unavailable." : "CPU: 100% is the whole Docker host."} ${partial ? "Only measured containers are included; missing usage is not estimated. " : ""}Total memory including cache: ${bytes(r.memoryUsageBytes)}. Sampled ${new Date(r.sampledAt).toISOString()}.${r.error ? " " + r.error : ""}`, available:true};
  }
  function cell(agent, metric, now) {
    const d = describe(agent, now);
    if (agent.disabled || (!agent.resources?.agent && !agent.resources?.peers)) {
      return `<span class="agent-resource-value" title="${escape(d.title)}">${escape(d[metric])}</span>`;
    }
    return [["agent_and_peers", "Total"], ["agent", "Agent"], ["peers", "Peers"]].map(([scope, label]) => {
      const value = describe(agent, now, scope);
      return `<div class="agent-resource-scope" title="${escape(value.title)}"><span class="agent-resource-scope-label">${label}</span><span class="agent-resource-value">${escape(value[metric])}</span></div>`;
    }).join("");
  }
  function noticeRow(message, {columns = 11, total = false, warning = true} = {}) {
    if (!message) return "";
    return `<tr class="resource-notice-row${total ? " resource-total-row" : ""}${warning ? " resource-notice-warning" : ""}"><td colspan="${columns}"><div class="resource-row-notice">${escape(message)}</div></td></tr>`;
  }
  function groupedNotes(entries, scopeCount) {
    const groups = new Map();
    for (const [label, message] of entries) {
      if (!message) continue;
      if (!groups.has(message)) groups.set(message, []);
      groups.get(message).push(label);
    }
    return [...groups].map(([message, labels]) => labels.length === scopeCount ? message : `${labels.join(", ")}: ${message}`).join("; ");
  }
  function notice(agent, now = Date.now()) {
    if (agent.disabled) return noticeRow("Agent disabled · Excluded from hardware monitoring totals.", {warning:false});
    const scopes = agent.resources?.agent || agent.resources?.peers ? [["agent_and_peers", "Total"], ["agent", "Agent"], ["peers", "Peers"]] : [["agent_and_peers", "Total"]];
    const partial = [], unavailable = [];
    let missingCPU = false;
    for (const [scope, label] of scopes) {
      const d = describe(agent, now, scope), r = sample(agent, scope);
      if (!d.available) unavailable.push([label, d.detail]);
      if (d.partial) partial.push([label, `${number(r.measuredContainers)}/${number(r.containers)} containers`]);
      if (d.available && cpuPercent(r) === null) missingCPU = true;
    }
    const messages = [];
    if (partial.length) messages.push(`Partial measurement · ${groupedNotes(partial, scopes.length)}`);
    if (unavailable.length) messages.push(groupedNotes(unavailable, scopes.length));
    if (missingCPU) messages.push("Host CPU count unavailable");
    return noticeRow(messages.join(". "));
  }
  function aggregate(agents, now = Date.now(), scope = "agent_and_peers") {
    const result = {cpuPercent:null, cpuCores:0, cpuCapacityCores:0, cpuMeasured:0, memoryWorkingSetBytes:0, memoryUsageBytes:0, measured:0, total:agents.filter(a => !a.disabled).length, partial:0, containers:0, measuredContainers:0};
    let normalizedCores = 0;
    for (const a of agents) if (valid(a, now, scope)) {
      const r = sample(a, scope);
      result.cpuCores += r.cpuCores;
      result.memoryWorkingSetBytes += r.memoryWorkingSetBytes;
      result.memoryUsageBytes += r.memoryUsageBytes;
      result.containers += r.containers;
      result.measuredContainers += r.measuredContainers;
      if (!r.complete) result.partial++;
      if (cpuPercent(r) !== null) {
        normalizedCores += r.cpuCores;
        result.cpuCapacityCores += r.cpuCapacityCores;
        result.cpuMeasured++;
      }
      result.measured++;
    }
    if (result.cpuCapacityCores > 0) result.cpuPercent = normalizedCores / result.cpuCapacityCores * 100;
    return result;
  }
  function totalRow(agents, now = Date.now()) {
    const enabled = agents.filter(a => !a.disabled), online = enabled.filter(a => a.state === "online");
    const hosts = new Set(online.map(a => a.hostname).filter(Boolean));
    const sum = key => online.reduce((value, a) => value + (Number.isFinite(a[key]) && a[key] > 0 ? a[key] : 0), 0);
    const peers = sum("activeNodes"), capacity = sum("capacity"), usage = capacity > 0 ? peers / capacity * 100 : null;
    const scopes = [["agent_and_peers", "Total"], ["agent", "Agent"], ["peers", "Peers"]].map(([scope, label]) => ({scope, label, value:aggregate(agents, now, scope)}));
    function totalCell(metric) {
      return scopes.map(({scope, label, value:a}) => {
        const measured = metric === "cpu" ? a.cpuMeasured : a.measured;
        const value = metric === "cpu" ? (a.cpuPercent === null ? "N/A" : `${number(a.cpuPercent)}%`) : (a.measured ? bytes(a.memoryWorkingSetBytes) : "N/A");
        const coverage = `${number(measured)}/${number(a.total)} Agents measured${a.partial ? ` · ${number(a.measuredContainers)}/${number(a.containers)} containers` : ""}`;
        return `<div class="agent-resource-scope" title="${escape(`${scopeLabels[scope]}; ${coverage}. Disabled Agents and unavailable samples are excluded.`)}"><span class="agent-resource-scope-label">${label}</span><span class="agent-resource-value">${value}</span></div>`;
      }).join("");
    }
    const partial = [], unavailable = [], cpuUnavailable = [];
    for (const {label, value:a} of scopes) {
      if (a.partial) partial.push([label, `${number(a.measuredContainers)}/${number(a.containers)} containers`]);
      if (a.measured < a.total) unavailable.push([label, `${number(a.measured)}/${number(a.total)} Agents measured`]);
      if (a.cpuMeasured !== a.measured) cpuUnavailable.push([label, `${number(a.cpuMeasured)}/${number(a.total)} Agents measured`]);
    }
    const messages = [];
    if (partial.length) messages.push(`Partial measurement · ${groupedNotes(partial, scopes.length)}`);
    if (unavailable.length) messages.push(`Resource coverage · ${groupedNotes(unavailable, scopes.length)}`);
    if (cpuUnavailable.length) messages.push(`CPU coverage · ${groupedNotes(cpuUnavailable, scopes.length)}`);
    const footer = noticeRow(messages.join(". "), {total:true});
    return `<tr class="resource-total-row${footer ? " resource-has-notice" : ""}" data-resource-total="agents">
      <td class="agent-number">—</td>
      <th scope="row"><span class="resource-total-label">Total</span><span class="agent-capacity-note">${number(enabled.length)} enabled${agents.length > enabled.length ? ` · ${number(agents.length - enabled.length)} disabled` : ""}</span></th>
      <td>${number(online.length)} / ${number(enabled.length)} online</td>
      <td title="Reported hostnames of enabled, online Agents.">${hosts.size ? `${number(hosts.size)} hosts` : "—"}</td>
      <td>${number(peers)} / ${number(capacity)}<span class="agent-capacity-note">Online Agents</span></td>
      <td>${usage === null ? "N/A" : `<div class="usage"><div class="usage-track"><i style="width:${Math.min(100, usage)}%"></i></div><span>${number(usage)}%</span></div>`}</td>
      <td class="agent-resource-cell">${totalCell("cpu")}</td>
      <td class="agent-resource-cell">${totalCell("memory")}</td>
      <td>—</td><td>—</td><td class="agent-settings-cell">—</td>
    </tr>${footer}`;
  }
  function update(agents, now = Date.now()) {
    for (const [scope, id] of [["agent_and_peers","agentResourceSummary"],["agent","agentOnlyResourceSummary"],["peers","peersOnlyResourceSummary"]]) {
      const element = root.document?.querySelector("#" + id);
      if (!element) continue;
      const a = aggregate(agents, now, scope);
      const cpu = a.cpuPercent === null ? "N/A" : `${number(a.cpuPercent)}%`;
      element.textContent = `${scopeLabels[scope]} · CPU ${cpu} · Memory ${a.measured ? bytes(a.memoryWorkingSetBytes) : "N/A"} · ${a.measured}/${a.total} Agents measured${a.cpuMeasured !== a.measured ? ` · CPU ${a.cpuMeasured}/${a.total} Agents` : ""}${a.partial ? ` · Partial: ${a.measuredContainers}/${a.containers} containers` : ""}`;
    }
  }
  function init({api}) {
    const $ = id => root.document.querySelector(id);
    const buttons = [...root.document.querySelectorAll("[data-resource-export]")];
    const start = $("#startResourceMeasurement"), stop = $("#stopResourceMeasurement");
    const select = $("#resourceMeasurementSelect"), refresh = $("#refreshResourceMeasurements"), remove = $("#deleteResourceMeasurement");
    const endpoint = "/api/v1/agents/resources/measurements";
    const stamp = value => new Date(value).toLocaleString("en-US", {timeZone:"UTC", month:"short", day:"numeric", year:"numeric", hour:"2-digit", minute:"2-digit", second:"2-digit", hour12:false}) + " UTC";
    const elapsed = seconds => [Math.floor(seconds / 3600), Math.floor(seconds / 60) % 60, seconds % 60].map(v => String(v).padStart(2, "0")).join(":");
    let busy = false, changing = false, polling = false, loaded = false, revision = 0, nextPoll = 0;
    let measurements = [], active = null, serverTime = 0, receivedAt = 0, maxDuration = 86400;
    const selected = () => measurements.find(m => m.id === select?.value && m.endedAt);
    function controls() {
      for (const b of buttons) b.disabled = busy || (b.dataset.resourceExport.startsWith("measurement-") && !selected());
      if (start) start.disabled = !loaded || changing || !!active;
      if (stop) stop.disabled = changing || !active;
      if (refresh) refresh.disabled = changing || polling;
      if (remove) remove.disabled = !loaded || changing || busy || !selected();
    }
    function renderClock() {
      if (!start) return;
      const status = $("#resourceMeasurementStatus");
      if (active) {
        const seconds = Math.max(0, Math.min(maxDuration, Math.floor((serverTime + Date.now() - receivedAt - Date.parse(active.startedAt)) / 1000)));
        status.textContent = `Measuring · ${elapsed(seconds)} · Started ${stamp(active.startedAt)}`;
        status.dataset.active = "true";
      } else {
        status.textContent = loaded ? "Ready to measure" : "Loading measurement status…";
        status.dataset.active = "false";
      }
    }
    function renderSelection() {
      const m = selected();
      const duration = m ? Math.max(0, Date.parse(m.endedAt) - Date.parse(m.startedAt)) : 0;
      const durationText = duration > 0 && duration < 1000 ? "< 1 second" : elapsed(Math.floor(duration / 1000));
      if (select) $("#resourceMeasurementDetails").textContent = m ? `${stamp(m.startedAt)} → ${stamp(m.endedAt)} · ${durationText}${m.endReason === "time_limit" ? " · Stopped at the 24-hour limit" : ""}` : "Stop a measurement to download its samples and summary.";
      controls();
    }
    function accept(data, preferred) {
      measurements = data.measurements;
      active = measurements.find(m => !m.endedAt) || null;
      serverTime = Date.parse(data.generatedAt); receivedAt = Date.now();
      maxDuration = data.maxDurationSeconds; loaded = true;
      const previous = preferred || select.value;
      const complete = measurements.filter(m => m.endedAt);
      select.replaceChildren();
      for (const m of complete) {
        const option = root.document.createElement("option");
        option.value = m.id; option.textContent = `${stamp(m.startedAt)} · ${m.id.slice(0, 6)}`;
        select.append(option);
      }
      if (!complete.length) {
        const option = root.document.createElement("option"); option.value = ""; option.textContent = "No completed measurements"; select.append(option);
      }
      select.value = complete.some(m => m.id === previous) ? previous : (complete[0]?.id || "");
      select.disabled = complete.length === 0;
      renderClock(); renderSelection();
    }
    async function request(path, method = "GET") {
      const controller = new AbortController(), timer = setTimeout(() => controller.abort(), 10000);
      try { return await api(path, {method, signal:controller.signal}); }
      finally { clearTimeout(timer); }
    }
    async function sync() {
      if (!start || polling || changing) return;
      polling = true; const version = ++revision; controls();
      try {
        const data = await request(endpoint);
        if (version !== revision) return;
        accept(data); $("#resourceMeasurementError").hidden = true;
      } catch (error) {
        if (version !== revision) return;
        const status = $("#resourceMeasurementError"); status.hidden = false;
        status.textContent = "Measurement status unavailable. " + (error.name === "AbortError" ? "Request timed out; retry shortly." : error.message);
      } finally { polling = false; nextPoll = Date.now() + 10000; controls(); }
    }
    async function change(action) {
      if (changing || (action === "start" ? !loaded || active : !active)) return;
      const id = active?.id;
      changing = true; ++revision; controls();
      const status = $("#resourceMeasurementError"); status.hidden = true;
      try {
        const data = await request(action === "start" ? endpoint : `${endpoint}/${encodeURIComponent(id)}/stop`, "POST");
        accept(data, action === "stop" ? id : undefined);
      } catch (error) {
        loaded = false;
        status.hidden = false;
        status.textContent = (error.name === "AbortError" ? "Request timed out; the operation may have completed." : error.message) + " Refresh measurement status before retrying.";
      } finally { changing = false; nextPoll = Date.now() + 10000; controls(); }
    }
    async function deleteSelected() {
      const m = selected();
      if (!loaded || changing || busy || !m) return;
      changing = true; ++revision; controls();
      const status = $("#resourceMeasurementError"); status.hidden = true;
      try {
        accept(await request(`${endpoint}/${encodeURIComponent(m.id)}`, "DELETE"));
        status.hidden = false;
        status.textContent = "Interval deleted. Prometheus history is retained.";
      } catch (error) {
        loaded = false; status.hidden = false;
        status.textContent = (error.name === "AbortError" ? "Delete request timed out; it may have completed." : error.message) + " Refresh measurement status before retrying.";
      } finally { changing = false; nextPoll = Date.now() + 10000; controls(); }
    }
    if (start) {
      start.addEventListener("click", () => change("start"));
      stop.addEventListener("click", () => change("stop"));
      remove?.addEventListener("click", deleteSelected);
      refresh.addEventListener("click", sync);
      select.addEventListener("change", renderSelection);
      controls(); sync();
      // No additional SSE payload or polling from hidden/background dashboards.
      root.setInterval(() => {
        if (root.document.hidden || $("#agents")?.hidden) return;
        renderClock(); if (Date.now() >= nextPoll) sync();
      }, 1000);
    }
    for (const button of buttons) button.addEventListener("click", async () => {
      const kind = button.dataset.resourceExport, measurement = kind.startsWith("measurement-") ? selected() : null;
      if (busy || (kind.startsWith("measurement-") && !measurement)) return;
      busy = true; controls();
      const status = $("#agentResourceExportStatus");
      status.hidden = false; status.textContent = "Preparing resource CSV…";
      const controller = new AbortController(), timer = setTimeout(() => controller.abort(), 20000);
      try {
        const query = measurement ? `/measurements/${encodeURIComponent(measurement.id)}/export?kind=${kind.slice(12)}` : kind === "current" ? "" : `/history?range=${encodeURIComponent($("#agentResourceRange").value)}&kind=${kind}`;
        const response = await root.fetch(`/api/v1/agents/resources${query}${query ? "&" : "?"}format=csv`, {credentials:"same-origin", signal:controller.signal});
        if (response.status === 401) { await api("/api/v1/auth/session"); throw Error("Login required."); }
        if (!response.ok) { const error = await response.json().catch(() => ({})); throw Error(error.error || "Resource export failed."); }
        if (!response.headers.get("Content-Type")?.startsWith("text/csv")) throw Error("Unexpected resource export response.");
        const url = root.URL.createObjectURL(await response.blob());
        const a = root.document.createElement("a"); a.href = url; a.download = `kpl-resources-${kind}${measurement ? "-" + measurement.id : ""}.csv`;
        root.document.body.append(a); a.click(); a.remove(); setTimeout(() => root.URL.revokeObjectURL(url), 1000);
        status.textContent = kind === "current" ? "Current resource CSV downloaded." : `${measurement ? "Measurement" : kind === "summary" ? "Summary" : "History"} CSV downloaded. Gaps are excluded; averages use available Prometheus samples.`;
      } catch (error) { status.textContent = error.name === "AbortError" ? "Resource export timed out. Retry or select a shorter period." : error.message; }
      finally { clearTimeout(timer); busy = false; controls(); }
    });
  }
  const exports = {valid, describe, cell, notice, noticeRow, aggregate, totalRow, update, init};
  if (typeof module !== "undefined" && module.exports) module.exports = exports;
  else root.KPLAgentResources = exports;
})(globalThis);

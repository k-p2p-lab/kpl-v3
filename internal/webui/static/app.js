const $ = (selector) => document.querySelector(selector);
const state = {
  snapshot: null, stream: null, streamSnapshot: null, streamPageHidden: false, reconnectTimer: null, snapshotRenderTimer: null,
  streamWatchdogTimer: null, streamAuthAbort: null, streamFailures: 0,
  savedResults: null, resultsLoading: false, resultsError: "",
  resultsRefreshTimer: null, resultsRefreshPending: false, runStates: null,
  savedScenarios: null, scenariosLoading: false, scenariosError: "", scenarioActionError: "",
  selectedScenarioId: null, scenarioLoadingId: null, scenarioSaving: false,
  scenarioValidating: false, scenarioValidation: null, scenarioValidationVersion: 0,
  scenarioDeletingId: null, pendingScenarioDeleteId: null, scenarioLoadVersion: 0, scenarioSubmitting: false, scenarioEditorVersion: 0,
  agentNumbers: loadAgentNumbers(),
  pendingStops: new Set(), pendingResumes: new Set(), deletedResultIDs: new Set(),
  pendingDelete: null, deletingResultId: null, loginRedirecting: false,
  detailPanelObserver: null,
  topology: {
    layout: {},
    filters: { transport: false, kademlia: false, gossipsub: false, topic: "" },
    selected: null, hovered: null, graph: null,
    camera: { x: 0, y: 0, scale: 1 }, autoFit: true, drag: null,
    motion: { enabled: true, reduced: false, overridden: false, frame: null, lastFrame: 0 },
  },
};

const defaultScenario = `version: 1
name: gossip-smoke
seed: 42
phases:
  - name: boot nodes
    action: join
    group: boot
    role: boot
    count: 2
    interval:
      model: fixed
      value: 300ms
    node:
      topics: [kpl/default]

  - name: boot barrier
    action: wait-ready
    group: boot
    readyRatio: 1
    timeout: 45s

  - name: worker nodes
    action: join
    group: worker
    role: worker
    count: 12
    interval:
      model: exponential
      mean: 250ms
    lifetime:
      model: pareto
      xm: 45s
      alpha: 2.5
      max: 3m
    node:
      topics: [kpl/default]
      d: 6
      dLow: 4
      dHigh: 12
      dOut: 2
      dLazy: 6
      heartbeat: 1s

  - name: mesh barrier
    action: wait-ready
    group: worker
    readyRatio: 0.9
    timeout: 60s

  - name: publish sample
    action: publish
    group: worker
    count: 10
    payloadSize: 1024
    deliveryWindow: 10s
    interval:
      model: fixed
      value: 1s

  - name: settle
    action: wait
    duration: 15s

  - name: cleanup
    action: stop-all
`;

function escapeHTML(value) {
  return String(value ?? "").replace(/[&<>'"]/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[char]);
}

// Preserve text selection, focus and layout when a snapshot has no visible changes.
const renderedHTML = new WeakMap();

function setText(element, value) {
  const text = String(value ?? "");
  if (element.textContent !== text) element.textContent = text;
}

function setHTML(element, markup) {
  if (renderedHTML.get(element) === markup) return;
  element.innerHTML = markup;
  renderedHTML.set(element, markup);
}

const measurementHelp = "Each publication uses its configured deliveryWindow. Only mature publications and sessions subscribed throughout that window enter the main ratio. Ranges are logical bounds from missing evidence, not confidence intervals. Starting delivery and coverage are conditional on the known starting cohort. Sessions with unproved publication-time availability are outside that cohort; see observation quality for counts. Publications without the required measurement evidence are excluded.";

function loadAgentNumbers() {
  const numbers = new Map();
  const used = new Set();
  try {
    const saved = JSON.parse(localStorage.getItem("kpl-agent-numbers-v1") || "[]");
    if (!Array.isArray(saved)) return numbers;
    for (const entry of saved) {
      if (!Array.isArray(entry) || entry.length !== 2) continue;
      const [id, number] = entry;
      if (typeof id !== "string" || !id || !Number.isSafeInteger(number) || number < 1 || number >= 1000000 || numbers.has(id) || used.has(number)) continue;
      numbers.set(id, number);
      used.add(number);
    }
  } catch { /* Keep numbers stable in memory when browser storage is unavailable. */ }
  return numbers;
}

function rememberAgents(ids) {
  let next = 1;
  for (const number of state.agentNumbers.values()) next = Math.max(next, number + 1);
  let changed = false;
  for (const id of [...new Set(ids.filter((id) => typeof id === "string" && id))].sort()) {
    if (state.agentNumbers.has(id)) continue;
    state.agentNumbers.set(id, next++);
    changed = true;
  }
  if (changed) {
    try { localStorage.setItem("kpl-agent-numbers-v1", JSON.stringify([...state.agentNumbers])); }
    catch { /* The current page retains its mapping without persistent storage. */ }
  }
}

function agentNumber(id) {
  return state.agentNumbers.get(id) || "?";
}

function isTopologyPeer(node) {
  return node.state !== "stopping" && node.state !== "stopped" && node.state !== "failed";
}

function peerPopulation(nodes) {
  const counts = { total: 0, ready: 0, starting: 0, other: 0 };
  for (const node of nodes) {
    if (!isTopologyPeer(node)) continue;
    counts.total++;
    if (node.state === "ready") counts.ready++;
    else if (node.state === "starting") counts.starting++;
    else counts.other++;
  }
  return counts;
}

function peerPopulationLabel(counts) {
  return `${formatNumber(counts.total)} Peers · ${formatNumber(counts.ready)} ready · ${formatNumber(counts.starting)} starting`
    + (counts.other ? ` · ${formatNumber(counts.other)} other` : "");
}

function topologyData(nodes, edges) {
  const visibleNodes = nodes.filter(isTopologyPeer);
  const ids = new Set(visibleNodes.map((node) => node.id));
  return { nodes: visibleNodes, edges: edges.filter((edge) => ids.has(edge.source) && ids.has(edge.target)) };
}

function topologyProtocol(edge) {
  return edge.protocol === "kademlia" || edge.protocol === "gossipsub" ? edge.protocol : "transport";
}

function filterTopologyEdges(nodes, edges, filters) {
  const visible = topologyData(nodes, edges);
  const merged = new Map();
  for (const edge of visible.edges) {
    const protocol = topologyProtocol(edge);
    if (!filters[protocol] || edge.source === edge.target) continue;
    if (protocol === "gossipsub" && filters.topic && edge.topic !== filters.topic) continue;
    const [source, target] = [edge.source, edge.target].sort();
    const key = JSON.stringify([protocol, source, target]);
    if (!merged.has(key)) merged.set(key, { source, target, protocol, topics: new Set(), reportedBy: new Set(), topicReports: new Map() });
    const connection = merged.get(key);
    if (edge.topic) {
      connection.topics.add(edge.topic);
      if (!connection.topicReports.has(edge.topic)) connection.topicReports.set(edge.topic, new Set());
      for (const id of edge.reportedBy || []) connection.topicReports.get(edge.topic).add(id);
    }
    for (const id of edge.reportedBy || []) connection.reportedBy.add(id);
  }
  return [...merged.values()].map((edge) => ({ ...edge, topics: [...edge.topics].sort(), reportedBy: [...edge.reportedBy].sort(), topicReports: Object.fromEntries([...edge.topicReports].map(([topic, ids]) => [topic, [...ids].sort()])) }));
}

function topologyReportSummary(edge, selectedID) {
  const describe = (ids) => ids.includes(edge.source) && ids.includes(edge.target) ? "Both endpoints reported"
    : ids.includes(selectedID) ? "Selected Peer reported" : ids.length ? "Remote Peer reported" : "Reporter not available";
  if (edge.protocol === "gossipsub" && edge.topics.length) {
    return edge.topics.map((topic) => `${topic}: ${describe(edge.topicReports?.[topic] || [])}`).join("; ");
  }
  return describe(edge.reportedBy);
}

function observedTopologyTopics(nodes, edges) {
  const topics = new Set();
  for (const node of nodes) for (const topic of Object.keys(node.meshPeers || {})) topics.add(topic);
  for (const edge of edges) if (edge.protocol === "gossipsub" && edge.topic) topics.add(edge.topic);
  return [...topics].sort();
}

function fitTopologyCamera(bounds, width, height) {
  const scale = Math.min(1.8, Math.max(0.01, Math.min((width - 48) / bounds.width, (height - 48) / bounds.height)));
  return { x: (width - bounds.width * scale) / 2, y: (height - bounds.height * scale) / 2, scale };
}

function formatNumber(value, digits = 0) {
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: digits }).format(Number(value || 0));
}

function formatBytes(value) {
  let size = value;
  if (!Number.isFinite(size) || size <= 0) return "";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit++;
  }
  const digits = unit === 0 || size >= 100 ? 0 : size >= 10 ? 1 : 2;
  return `${formatNumber(size, digits)} ${units[unit]}`;
}

function ratioRange(available, lower, upper, unknown) {
  if (!available) return "N/A";
  const percentage = (value) => `${formatNumber(value * 100, 1)}%`;
  return unknown > 0 ? `${percentage(lower)}–${percentage(upper)}` : percentage(lower);
}

function formatBandwidthRate(value) {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) return "N/A";
  const units = ["bit/s", "kbit/s", "Mbit/s", "Gbit/s", "Tbit/s", "Pbit/s"];
  let unit = 0;
  while (value >= 1000 && unit < units.length - 1) { value /= 1000; unit++; }
  return `${formatNumber(value, value >= 100 ? 0 : value >= 10 ? 1 : 2)} ${units[unit]}`;
}

function bandwidthMetricView(bandwidth) {
  const valid = value => typeof value === "number" && Number.isFinite(value) && value >= 0;
  const measured = valid(bandwidth?.sessions) && bandwidth.sessions > 0;
  const rates = bandwidth?.currentRates;
  const available = measured && rates?.available === true && valid(rates.sentBitsPerSecond) && valid(rates.receivedBitsPerSecond);
  const finalized = measured ? bandwidth.finalizedSessions || 0 : 0;
  const active = measured ? Math.max(0, bandwidth.sessions - finalized) : 0;
  const reporting = rates?.reportingSessions || 0, stale = rates?.staleSessions || 0;
  const at = measured ? new Date(bandwidth.latestAt) : null;
  const bytes = value => measured && valid(value) ? (value === 0 ? "0 B" : formatBytes(value)) : "N/A";
  return {
    send: available ? formatBandwidthRate(rates.sentBitsPerSecond) : "N/A",
    receive: available ? formatBandwidthRate(rates.receivedBitsPerSecond) : "N/A",
    sent: `Total sent: ${bytes(bandwidth?.sentBytes)}`,
    received: `Total received: ${bytes(bandwidth?.receivedBytes)}`,
    quality: !measured ? "N/A" : !available ? "Rate unavailable" : stale > 0 ? "Partial" : active === 0 ? "Finalized" : "Live",
    sessions: measured ? `${formatNumber(reporting)} / ${formatNumber(active)} active sessions fresh · ${formatNumber(finalized)} / ${formatNumber(bandwidth.sessions)} finalized${stale > 0 ? ` · ${formatNumber(stale)} stale / clock-skewed` : ""}` : "No bandwidth samples",
    sample: `Last sample: ${at && Number.isFinite(at.getTime()) ? at.toLocaleTimeString("en-US") : "N/A"}`,
    rejected: `Rejected samples: ${formatNumber(bandwidth?.rejectedSamples || 0)}`,
  };
}

function renderBandwidthMetrics(bandwidth) {
  const view = bandwidthMetricView(bandwidth);
  for (const [id, key] of Object.entries({bandwidthSendMetric:"send", bandwidthReceiveMetric:"receive", bandwidthSendTotal:"sent", bandwidthReceiveTotal:"received", bandwidthQualityMetric:"quality", bandwidthSessionsMetric:"sessions", bandwidthSampleMetric:"sample", bandwidthRejectedMetric:"rejected"})) {
    setText($(`#${id}`), view[key]);
  }
}

function sessionMetrics(metrics) {
  return metrics?.definition === "session-window-v1" ? metrics : {};
}

function deliveryMetricView(metrics) {
  metrics = sessionMetrics(metrics);
  const n = (key) => formatNumber(metrics[key]);
  const compact = (key) => Number(metrics[key] || 0).toLocaleString("en-US", { notation: "compact", maximumFractionDigits: 1 });
  const coverageUpper = Number.isFinite(Number(metrics.stableCoverageUpperBound)) ? metrics.stableCoverageUpperBound : metrics.stableCoverage;
  return {
    label: "Continuous-session delivery",
    primary: ratioRange(metrics.deliveryRatioAvailable, metrics.reachability, metrics.deliveryRatioUpperBound, metrics.unknownDeliveries),
    primaryDetail: `On time: ${n("eligibleDeliveries")} / ${n("expectedDeliveries")} stable pairs`,
    progress: `Windows: ${(metrics.deliveryWindows || []).join(", ") || (metrics.definition ? "Awaiting publication" : "N/A")} · Mature: ${n("finalizedPublications")} · Pending: ${n("pendingPublications")}`,
    initial: ratioRange(metrics.initialDeliveryRatioAvailable, metrics.initialDeliveryRatio, metrics.initialDeliveryRatioUpperBound, metrics.initialUnknownDeliveries),
    initialDetail: `On time: ${n("initialEligibleDeliveries")} / ${n("initialExpectedDeliveries")} known starting pairs`,
    coverage: ratioRange(metrics.stableCoverageAvailable, metrics.stableCoverage, coverageUpper, metrics.continuityUnknownPairs),
    coverageDetail: `Stable: ${n("expectedDeliveries")} / ${n("initialExpectedDeliveries")} known starting pairs · Departed: ${n("departedPairs")}`,
    observationSummary: `${compact("unknownDeliveries")} / ${compact("continuityUnknownPairs")} / ${compact("publicationAvailabilityUnknownPairs")}`,
    observation: `${n("unknownDeliveries")} receipt unknown · ${n("continuityUnknownPairs")} continuity unknown · ${n("publicationAvailabilityUnknownPairs")} start unknown`,
    outcomes: `Known missed: ${n("missedDeliveries")} · Observed late: ${n("lateDeliveries")}`,
    note: measurementHelp,
  };
}

function relativeTime(value) {
  const time = new Date(value).getTime();
  const seconds = Math.max(0, Math.round((Date.now() - time) / 1000));
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return new Date(value).toLocaleString("en-US", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function redirectToLogin() {
  if (state.loginRedirecting) return;
  state.loginRedirecting = true;
  state.stream?.close();
  clearTimeout(state.reconnectTimer);
  if (state.streamWatchdogTimer != null) clearTimeout(state.streamWatchdogTimer);
  state.streamAuthAbort?.abort();
  globalThis.location.replace("/login");
}

async function logout() {
  const button = $("#logout");
  button.disabled = true;
  try {
    await api("/api/v1/auth/logout", { method: "POST" });
    redirectToLogin();
  } catch (error) {
    if (error.status !== 401) showToast(error.message);
  } finally {
    button.disabled = false;
  }
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (!["GET", "HEAD"].includes((options.method || "GET").toUpperCase())) headers.set("X-KPL-Request", "dashboard");
  const response = await fetch(path, { ...options, headers, credentials: "same-origin" });
  if (response.status === 401) redirectToLogin();
  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: response.statusText }));
    const failure = new Error(error.error || response.statusText);
    failure.status = response.status;
    throw failure;
  }
  if (response.status === 204) return null;
  return response.json();
}

const scenarioRequestTimeoutMs = 30000;

function scenarioTimeoutError(kind) {
  const message = kind === "run"
    ? "Request timed out. The experiment may have been submitted. Check Experiment progress before trying again."
    : kind === "mutation"
      ? "Request timed out. The server may have completed this operation. Refresh the saved scenario list before trying again."
      : "Request timed out.";
  const error = new Error(message);
  error.name = "TimeoutError";
  return error;
}

async function scenarioRequest(path, options = {}, kind = "read") {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), scenarioRequestTimeoutMs);
  try {
    return await api(path, { ...options, signal: controller.signal });
  } catch (error) {
    if (error.name === "AbortError") throw scenarioTimeoutError(kind);
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function setConnection(mode, label) {
  const element = $("#connectionState");
  const className = `connection-state ${mode}`;
  if (element.className !== className) element.className = className;
  setHTML(element, `<i></i>${escapeHTML(label)}`);
}

// Coalesce bursts of telemetry while keeping the latest snapshot available to controls.
function scheduleSnapshotRender(snapshot) {
  // Controls may optimistically edit runs; keep the stream's delta base intact.
  state.snapshot = { ...snapshot, experiments: (snapshot.experiments || []).map(run => ({ ...run })) };
  if (state.snapshotRenderTimer != null) return;
  state.snapshotRenderTimer = setTimeout(() => {
    state.snapshotRenderTimer = null;
    render(state.snapshot);
  }, 250);
}

function applySnapshotDelta(snapshot, patch) {
  if (!snapshot) throw new Error("Snapshot delta arrived before a full snapshot");
  const next = { ...snapshot };
  for (const key of ["generatedAt", "edges", "events", "metrics"]) {
    if (Object.hasOwn(patch, key)) next[key] = patch[key];
  }
  for (const key of ["agents", "nodes", "experiments"]) {
    if (!Object.hasOwn(patch, key)) continue;
    const change = patch[key];
    const items = new Map((snapshot[key] || []).map(item => [item.id, item]));
    for (const id of change.remove || []) items.delete(id);
    for (const item of change.upsert || []) items.set(item.id, item);
    if (change.order) {
      next[key] = change.order.map(id => {
        if (!items.has(id)) throw new Error("Incomplete snapshot delta");
        return items.get(id);
      });
    } else next[key] = [...items.values()];
  }
  return next;
}

function pauseStream() {
  const stream = state.stream;
  state.stream = null;
  state.streamSnapshot = null;
  stream?.close();
  clearTimeout(state.streamWatchdogTimer);
  state.streamWatchdogTimer = null;
  state.streamAuthAbort?.abort();
  state.streamAuthAbort = null;
  clearTimeout(state.reconnectTimer);
  state.reconnectTimer = null;
  clearTimeout(state.snapshotRenderTimer);
  state.snapshotRenderTimer = null;
}

const streamConnectTimeoutMs = 30000;
const streamIdleTimeoutMs = 45000;
const streamAuthTimeoutMs = 8000;

function connectStream() {
  if (state.loginRedirecting || state.streamPageHidden || document.hidden || state.stream) return;
  clearTimeout(state.reconnectTimer);
  state.reconnectTimer = null;
  state.streamSnapshot = null;
  const stream = new EventSource("/api/v1/stream?view=dashboard");
  state.stream = stream;
  let reconnecting = false;
  const reconnect = async () => {
    if (state.stream !== stream || reconnecting || state.loginRedirecting) return;
    if (document.hidden || state.streamPageHidden) { pauseStream(); return; }
    reconnecting = true;
    setConnection("offline", "Reconnecting");
    stream.close();
    clearTimeout(state.streamWatchdogTimer);
    state.streamWatchdogTimer = null;
    clearTimeout(state.reconnectTimer);
    // A slow/half-open session probe must not prevent the next SSE attempt.
    const controller = new AbortController();
    state.streamAuthAbort = controller;
    const timeout = setTimeout(() => controller.abort(), streamAuthTimeoutMs);
    try { await api("/api/v1/auth/session", { signal: controller.signal }); }
    catch (error) { if (error.status === 401) return; }
    finally {
      clearTimeout(timeout);
      if (state.streamAuthAbort === controller) state.streamAuthAbort = null;
    }
    if (state.stream === stream && !state.loginRedirecting) {
      state.stream = null;
      state.streamSnapshot = null;
      state.streamFailures = Math.min((state.streamFailures || 0) + 1, 5);
      const baseDelay = Math.min(30000, 2000 * 2 ** (state.streamFailures - 1));
      const delay = Math.min(30000, Math.round(baseDelay * (0.8 + Math.random() * 0.4)));
      if (!document.hidden && !state.streamPageHidden) state.reconnectTimer = setTimeout(connectStream, delay);
    }
  };
  const armWatchdog = (delay) => {
    clearTimeout(state.streamWatchdogTimer);
    state.streamWatchdogTimer = setTimeout(() => { void reconnect(); }, delay);
  };
  const markLive = () => {
    state.streamFailures = 0;
    armWatchdog(streamIdleTimeoutMs);
    setConnection("live", "Live");
  };
  const receive = (event, delta) => {
    if (state.stream !== stream || reconnecting) return;
    try {
      const data = JSON.parse(event.data);
      state.streamSnapshot = delta ? applySnapshotDelta(state.streamSnapshot, data) : data;
      scheduleSnapshotRender(state.streamSnapshot);
      markLive();
    } catch { void reconnect(); }
  };
  stream.addEventListener("snapshot", event => receive(event, false));
  stream.addEventListener("snapshot_delta", event => receive(event, true));
  stream.addEventListener("heartbeat", () => {
    if (state.stream === stream && !reconnecting && state.streamSnapshot) markLive();
  });
  stream.onerror = reconnect;
  armWatchdog(streamConnectTimeoutMs);
}

function setupStreamLifecycle() {
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      pauseStream();
      setConnection("offline", "Paused");
    } else connectStream();
  });
  window.addEventListener("pagehide", () => {
    state.streamPageHidden = true;
    pauseStream();
  });
  window.addEventListener("pageshow", () => {
    state.streamPageHidden = false;
    connectStream();
  });
  window.addEventListener("online", () => {
    // Mobile network changes can leave a socket looking open but unusable.
    pauseStream();
    state.streamFailures = 0;
    connectStream();
  });
}

function isPanelCollapsed(id) {
  return $("#panel-" + id + "-body")?.hidden === true;
}

function syncDetailPanelHeight() {
  const agents = $(".agents-panel");
  const events = $(".events-panel");
  if (!agents || !events) return;
  if (isPanelCollapsed("agents") || isPanelCollapsed("events") || window.matchMedia?.("(max-width: 1050px)").matches) {
    if (events.style.height) events.style.removeProperty("height");
    return;
  }
  const height = agents.getBoundingClientRect().height;
  if (height > 0 && events.style.height !== `${height}px`) events.style.height = `${height}px`;
}

function setupDetailPanelSizing() {
  syncDetailPanelHeight();
  if (typeof ResizeObserver === "undefined") return;
  state.detailPanelObserver?.disconnect();
  state.detailPanelObserver = new ResizeObserver(syncDetailPanelHeight);
  state.detailPanelObserver.observe($(".agents-panel"));
}

function setupDashboardPanelHandling() {
  document.addEventListener("dashboard:panel-toggle", (event) => {
    syncDetailPanelHeight();
    if (event.detail?.id !== "topology") return;
    if (isPanelCollapsed("topology")) {
      state.topology.hovered = null;
      state.topology.drag = null;
      stopTopologyMotion();
    } else if (state.snapshot) {
      renderTopology(state.snapshot.nodes || [], state.snapshot.edges || []);
    }
  });
}

function render(snapshot) {
  const agents = snapshot.agents || [];
  const nodes = snapshot.nodes || [];
  const edges = snapshot.edges || [];
  const metricRun = (snapshot.experiments || []).find((run) => run.id === snapshot.metrics?.runId);
  // Snapshots retain final summaries; the carousel shows only a running run.
  const metrics = metricRun?.state === "running" ? snapshot.metrics : {};
  const measurement = sessionMetrics(metrics);
  const metricIteration = metricRun?.repetitions > 1 ? ` · Run ${formatNumber(metricRun.iteration)} of ${formatNumber(metricRun.repetitions)}` : "";
  setText($("#messageMetricsScope"), metrics.runId
    ? `Run metrics: ${metricRun?.name ? `${metricRun.name} · ` : ""}${metrics.runId}${metricIteration}`
    : "Run metrics: No run selected");
  rememberAgents([...agents.map((agent) => agent.id), ...nodes.map((node) => node.agentId)]);
  const online = agents.filter((agent) => agent.state === "online").length;
  const capacity = agents.filter((agent) => agent.state === "online").reduce((sum, agent) => sum + Math.max(0, agent.capacity - agent.activeNodes), 0);
  const population = peerPopulation(nodes);
  setText($("#agentMetric"), `${online} / ${agents.length}`);
  setText($("#capacityMetric"), `Available slots: ${formatNumber(capacity)}`);
  setText($("#peerMetric"), formatNumber(population.ready));
  setText($("#peerPopulationMetric"), `Topology: ${formatNumber(population.total)} · Starting: ${formatNumber(population.starting)}`
    + (population.other ? ` · Other: ${formatNumber(population.other)}` : ""));
  setText($("#connectionMetric"), `Transport links: ${formatNumber(filterTopologyEdges(nodes, edges, { transport: true }).length)}`);
  setText($("#latencyMetric"), measurement.latencySamples > 0 ? `${formatNumber(measurement.p95LatencyMs, 1)} ms` : "N/A");
  setText($("#averageLatencyMetric"), measurement.latencySamples > 0 ? `Average: ${formatNumber(measurement.averageLatencyMs, 1)} ms · Samples: ${formatNumber(measurement.latencySamples)}` : "No eligible latency samples");
  const delivery = deliveryMetricView(metrics);
  setText($("#reachLabel"), delivery.label);
  setText($("#reachMetric"), delivery.primary);
  setText($("#deliveryMetric"), delivery.primaryDetail);
  setText($("#measurementProgress"), delivery.progress);
  setText($("#initialReachMetric"), delivery.initial);
  setText($("#initialDeliveryMetric"), delivery.initialDetail);
  setText($("#coverageMetric"), delivery.coverage);
  setText($("#coverageDetail"), delivery.coverageDetail);
  setText($("#observationMetric"), delivery.observation);
  setText($("#observationSummaryMetric"), delivery.observationSummary);
  setText($("#outcomesMetric"), delivery.outcomes);
  setText($("#measurementNote"), delivery.note);
  setText($("#eventTotalsMetric"), `Published: ${formatNumber(metrics.published)} · Delivered: ${formatNumber(metrics.delivered)}`);
  setText($("#duplicateMetric"), measurement.duplicateSamples > 0 ? formatNumber(measurement.averageDuplicates, 2) : "N/A");
  setText($("#duplicateSamplesMetric"), `Eligible duplicates: ${formatNumber(measurement.eligibleDuplicates)} · Delivered pairs: ${formatNumber(measurement.duplicateSamples)}`);
  setText($("#duplicateTotalMetric"), `All duplicate events: ${formatNumber(metrics.duplicates)}`);
  renderBandwidthMetrics(metrics.bandwidth);
  setText($("#updatedAt"), new Date(snapshot.generatedAt).toLocaleTimeString("en-US"));
  renderRuns(snapshot.experiments || []);
  renderAgents(agents);
  renderEvents(snapshot.events || []);
  syncDetailPanelHeight();
  renderTopology(nodes, edges);
}

function formatEstimateRemaining(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return "Estimating…";
  if (seconds < 60) return "Less than 1 min remaining";
  const minutes = Math.ceil(seconds / 60);
  const days = Math.floor(minutes / 1440);
  const hours = Math.floor(minutes % 1440 / 60);
  const rest = minutes % 60;
  return `About ${[days ? `${days}d` : "", hours ? `${hours}h` : "", rest ? `${rest}m` : ""].filter(Boolean).join(" ")} remaining`;
}

function estimateFinishMarkup(value) {
  const formatted = formatResultTime(value);
  return formatted === "—" ? "Estimating…" : `<time datetime="${escapeHTML(value)}">${escapeHTML(formatted)}</time>`;
}

function estimateBasisLabel(timing) {
  return timing?.basis === "observed-runs"
    ? `Based on ${formatNumber(timing.observedRuns)} completed ${timing.observedRuns === 1 ? "run" : "runs"}`
    : "Scenario estimate";
}

function runTimingMarkup(run, stopping) {
  if (!isPendingRun(run)) return "";
  const timing = run.timing;
  const remaining = stopping ? "Stopping…" : timing?.overdue ? "Taking longer than estimated" : formatEstimateRemaining(timing?.remainingSeconds);
  return `<div class="run-timing${timing?.overdue ? " overdue" : ""}" title="Estimates update as phases finish. Readiness timeouts are initial allowances; node startup, available capacity and cleanup can change the finish time.">
    <div><span>Est. finish</span><strong>${stopping ? "Stopping…" : estimateFinishMarkup(timing?.estimatedFinishAt)}</strong></div>
    <small>${escapeHTML(remaining)}${timing && !stopping ? ` · ${escapeHTML(estimateBasisLabel(timing))}` : ""}</small>
  </div>`;
}

function renderBatchEstimates(runs) {
  const groups = new Map();
  for (const run of runs) {
    if (!run.batchId || !(run.repetitions > 1)) continue;
    if (!groups.has(run.batchId)) groups.set(run.batchId, []);
    groups.get(run.batchId).push(run);
  }
  const summaries = [];
  for (const [id, members] of groups) {
    const pending = members.filter(isPendingRun);
    if (!pending.length) continue;
    const run = pending.find(member => member.state === "running") || pending[0];
    const timing = pending.find(member => member.timing?.batchEstimatedFinishAt)?.timing;
    const stopping = state.pendingStops.has(id);
    // After a Controller restart, completed iterations exist only in Saved
    // results. Merge by ID so live updates win without double-counting them.
    const known = new Map((state.savedResults || []).filter(member => member.batchId === id && !state.deletedResultIDs?.has(member.id)).map(member => [member.id, member]));
    for (const member of members) known.set(member.id, member);
    const completed = currentBatchRuns([...known.values()]).filter(member => member.state === "completed").length;
    const remaining = stopping ? "Stopping…" : timing?.batchOverdue ? "Delayed run · estimate may extend" : formatEstimateRemaining(timing?.batchRemainingSeconds);
    summaries.push(`<article class="run-batch-estimate${timing?.batchOverdue ? " overdue" : ""}" data-batch-estimate="${escapeHTML(id)}">
      <div class="run-batch-estimate-heading"><strong title="${escapeHTML(run.name)}">${escapeHTML(run.name)}</strong><span>${formatNumber(completed)} / ${formatNumber(run.repetitions)} completed</span></div>
      <div class="run-timing"><div><span>Group est. finish</span><strong>${stopping ? "Stopping…" : estimateFinishMarkup(timing?.batchEstimatedFinishAt)}</strong></div>
      <small>${escapeHTML(remaining)}${timing && !stopping ? ` · ${escapeHTML(estimateBasisLabel(timing))}` : ""}</small></div>
    </article>`);
  }
  setHTML($("#runBatchEstimates"), summaries.join(""));
  $("#runBatchEstimates").hidden = summaries.length === 0;
}

function renderRuns(runs) {
  runs = currentBatchRuns(runs.filter((run) => !state.deletedResultIDs?.has(run.id)));
  const activeGroups = new Set(runs.filter(isPendingRun).map(run => run.batchId || run.id));
  for (const key of state.pendingStops) {
    if (!activeGroups.has(key)) state.pendingStops.delete(key);
  }
  const runStates = JSON.stringify(runs.map((run) => [run.id, run.state]).sort((a, b) => String(a[0]).localeCompare(String(b[0]))));
  if (state.runStates !== null && state.runStates !== runStates) {
    clearTimeout(state.resultsRefreshTimer);
    state.resultsRefreshTimer = setTimeout(refreshSavedResults, 250);
  }
  state.runStates = runStates;
  setText($("#runCount"), runs.length);
  renderBatchEstimates(runs);
  if (!runs.length) {
    setHTML($("#runList"), '<div class="empty-copy">No experiments yet.</div>');
    return;
  }
  setHTML($("#runList"), runs.map((run) => {
    const progress = run.totalPhases ? Math.round((run.phase / run.totalPhases) * 100) : 0;
    const stopping = state.pendingStops.has(run.batchId || run.id);
    const stop = isPendingRun(run) ? `<button class="stop-button" data-stop-run="${escapeHTML(run.id)}" type="button" title="Stop this run and cancel the remaining queued runs in its batch." ${stopping ? "disabled" : ""}>${stopping ? "Stopping…" : run.repetitions > 1 ? "Stop batch" : "Stop"}</button>` : "";
    const sourceSize = runSourceSize(run);
    return `<article class="run-item">
      <div class="run-title"><strong title="${escapeHTML(run.name)}">${escapeHTML(run.name)}</strong><span class="status-pill ${escapeHTML(run.state)}">${escapeHTML(run.state)}</span></div>
      <div class="run-meta"><span>${escapeHTML(run.state === "queued" ? "Waiting to start" : run.phaseName || `seed ${run.seed}`)}</span>${stop}</div>
      ${run.repetitions > 1 ? `<div class="run-meta"><span>Run ${formatNumber(run.iteration)} of ${formatNumber(run.repetitions)}</span></div>` : ""}
      <div class="run-meta"><span>Jobs: ${formatNumber(run.activeJobs || 0)} active · ${formatNumber(run.completedJobs || 0)} completed · ${formatNumber(run.failedJobs || 0)} failed · ${formatNumber(run.canceledJobs || 0)} canceled</span></div>
      ${runTimingMarkup(run, stopping)}
      <div class="progress-track" aria-label="${progress}% complete"><i style="width:${Math.min(100, progress)}%"></i></div>
      ${run.error ? `<div class="run-meta"><span>${escapeHTML(run.error)}</span></div>` : ""}
      <div class="run-actions">${resultDownloadLink(run)}${sourceSize}</div>
    </article>`;
  }).join(""));
}

function isPendingRun(run) {
  return run.active || run.state === "running" || run.state === "queued";
}

async function requestRunStop(id) {
  const runs = state.snapshot?.experiments || [];
  const run = runs.find(item => item.id === id);
  if (!run) return;
  const key = run.batchId || run.id;
  if (state.pendingStops.has(key) || !runs.some(item => (item.batchId || item.id) === key && isPendingRun(item))) return;
  state.pendingStops.add(key);
  renderRuns(runs);
  try {
    await api(`/api/v1/experiments/${encodeURIComponent(id)}/stop`, { method: "POST" });
    // Acceptance precedes job/container cleanup. Keep all batch controls
    // disabled until an authoritative snapshot has no active members left.
    showToast(run.repetitions > 1 ? "Batch stop requested. Remaining queued runs will be canceled." : "Stop requested. Cleaning up Peers…");
  } catch (error) {
    state.pendingStops.delete(key);
    showToast(error.message);
  } finally {
    if (state.snapshot) renderRuns(state.snapshot.experiments || []);
  }
}

function resultLocked(run) {
  if (run.batchId && state.pendingResumes?.has(run.batchId)) return true;
  if (isPendingRun(run)) return true;
  return Boolean(run.batchId && [...(state.savedResults || []), ...(state.snapshot?.experiments || [])].some((other) => other.batchId === run.batchId && isPendingRun(other)));
}

function resultImagesButton(run) {
  const job = run.analysis;
  const label = job?.state === "queued" ? "Images · Queued" : job?.state === "running" ? (job.phase === "aggregating" ? "Images · Calculating" : job.phase === "saving" ? "Images · Saving" : `Images · ${Math.floor(job.progress || 0)}% read`) : job?.state === "completed" ? "Images · Ready" : ["failed", "interrupted"].includes(job?.state) ? "Images · Retry" : "Images";
  return `<button class="result-images-button" type="button" data-result-images="${escapeHTML(run.id)}" aria-label="${escapeHTML(`View graph images: ${run.name || run.id}`)}" ${run.state === "unreadable" || run.state === "queued" ? "disabled" : ""}>${label}</button>`;
}

function resultDownloadLink(run) {
  if (run.state === "unreadable") {
    return '<span class="download-unavailable" title="Saved metadata could not be read.">Unavailable</span>';
  }
  const active = isPendingRun(run);
  const label = active ? "Download snapshot" : "Download results";
  const path = `/api/v1/experiments/${encodeURIComponent(run.id)}/download`;
  const title = active ? "Download a ZIP snapshot of the scenario, metadata, and events recorded so far."
    : "Download the saved scenario, metadata, and events as a ZIP file.";
  return `<a class="download-link" data-result-download="${escapeHTML(run.id)}" href="${escapeHTML(path)}" download="${escapeHTML(`${run.id}.zip`)}" target="_blank" rel="noopener" title="${title}" aria-label="${escapeHTML(`${label}: ${run.name || run.id}`)}">${label}</a>`;
}

function resultSourceSize(run) {
  const bytes = run?.sourceBytes;
  const size = Number.isSafeInteger(bytes) && bytes >= 0 ? (bytes === 0 ? "0 B" : formatBytes(bytes)) : "";
  if (!size) return '<span class="download-size" title="Source file sizes could not be read at the last refresh.">Source · —</span>';
  const live = isPendingRun(run);
  return `<span class="download-size${live ? " live" : ""}" title="Uncompressed source files at the last refresh: scenario, experiment metadata, events and observations. Excludes generated analysis files.${live ? " This run is still recording." : ""}">${live ? "Live source " : "Source "}${escapeHTML(size)}</span>`;
}

function runSourceSize(run) {
  const saved = (state.savedResults || []).find((result) => result.id === run.id);
  return saved ? resultSourceSize({...saved, state: run.state, active: isPendingRun(run)}) : "";
}

function renderResultViews() {
  renderSavedResults();
  if (state.snapshot) renderRuns(state.snapshot.experiments || []);
}

function formatResultTime(value) {
  if (!value || String(value).startsWith("0001-")) return "—";
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return "—";
  return date.toLocaleString("en-US", { year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

function formatScenarioID(value) {
  return value.length > 10 ? `${value.slice(0, 10)}…` : value;
}

function validateSavedScenario(value, requireYAML = false) {
  if (!value || typeof value !== "object" || typeof value.id !== "string" || !value.id
      || typeof value.name !== "string" || (requireYAML && typeof value.yaml !== "string")) {
    throw new Error("Unexpected saved scenario response.");
  }
  return value;
}

function scenarioOperationBusy() {
  return state.scenariosLoading || state.scenarioSaving || state.scenarioSubmitting
    || Boolean(state.scenarioLoadingId) || Boolean(state.scenarioDeletingId);
}

function renderScenarioValidation() {
  const result = state.scenarioValidation;
  const button = $("#validateScenario");
  button.disabled = scenarioOperationBusy() || state.scenarioValidating;
  button.textContent = state.scenarioValidating ? "Validating…" : "Validate";
  const panel = $("#scenarioValidation");
  panel.hidden = !result;
  panel.classList.toggle("success", result?.kind === "success");
  panel.classList.toggle("error", result?.kind === "invalid" || result?.kind === "error");
  panel.setAttribute("role", result?.kind === "invalid" || result?.kind === "error" ? "alert" : "status");
  panel.setAttribute("aria-busy", String(state.scenarioValidating));
  $("#scenarioValidationTitle").textContent = result?.title || "";
  $("#scenarioValidationMessage").textContent = result?.message || "";
  $("#scenarioText").setAttribute("aria-invalid", String(result?.kind === "invalid"));
}

function resetScenarioValidation() {
  ++state.scenarioValidationVersion;
  state.scenarioValidating = false;
  state.scenarioValidation = null;
  renderScenarioValidation();
}

async function validateEditedScenario() {
  if (scenarioOperationBusy() || state.scenarioValidating) return;
  const yaml = $("#scenarioText").value;
  const version = ++state.scenarioValidationVersion;
  if (!yaml.trim()) {
    state.scenarioValidation = { kind: "invalid", title: "Scenario is invalid", message: "Enter a YAML scenario to validate." };
    renderScenarioValidation();
    $("#scenarioText").focus();
    return;
  }
  state.scenarioValidating = true;
  state.scenarioValidation = { kind: "pending", title: "Validating scenario…", message: "Checking YAML syntax and scenario settings." };
  renderScenarioValidation();
  try {
    const result = await scenarioRequest("/api/v1/scenarios/validate", {
      method: "POST",
      headers: { "Content-Type": "application/yaml" },
      body: yaml,
    });
    if (version !== state.scenarioValidationVersion || yaml !== $("#scenarioText").value) return;
    if (result?.valid !== true || typeof result.name !== "string" || !Number.isInteger(result.phases) || result.phases < 1) {
      throw new Error("Unexpected scenario validation response.");
    }
    state.scenarioValidation = {
      kind: "success", title: "Scenario is valid",
      message: `${result.name} · ${result.phases} ${result.phases === 1 ? "phase" : "phases"}\nYAML syntax and scenario settings passed validation.`,
    };
  } catch (error) {
    if (version !== state.scenarioValidationVersion || yaml !== $("#scenarioText").value) return;
    const invalid = error.status === 400 || error.status === 413;
    state.scenarioValidation = {
      kind: invalid ? "invalid" : "error",
      title: invalid ? "Scenario is invalid" : "Could not validate scenario",
      message: error.message,
    };
  } finally {
    if (version === state.scenarioValidationVersion) {
      state.scenarioValidating = false;
      // Ignore results for editor contents replaced outside an input event too.
      if (yaml !== $("#scenarioText").value) state.scenarioValidation = null;
      renderScenarioValidation();
    }
  }
}

function renderSavedScenarios() {
  renderScenarioValidation();
  const scenarios = state.savedScenarios || [];
  const status = $("#scenarioLibraryStatus");
  const busy = scenarioOperationBusy();
  $(".scenario-library").setAttribute("aria-busy", String(busy));
  $("#refreshScenarios").disabled = busy;
  $("#refreshScenarios").textContent = state.scenariosLoading ? "Refreshing…" : "Refresh";
  $("#newScenario").disabled = busy;
  $("#saveScenario").disabled = busy;
  $("#saveScenario").textContent = state.scenarioSaving ? "Saving…"
    : state.selectedScenarioId ? "Save changes" : "Save scenario";
  $("#saveScenarioCopy").hidden = !state.selectedScenarioId;
  $("#saveScenarioCopy").disabled = busy;
  $("#scenarioName").disabled = busy;
  $("#scenarioText").disabled = busy;
  $("#runRepetitions").disabled = busy;
  $("#runScenario").disabled = busy;
  $("#runScenario").textContent = state.scenarioSubmitting ? "Submitting…" : "Run";
  $("#scenarioLibraryError").textContent = state.scenarioActionError;
  status.classList.toggle("error", Boolean(state.scenariosError));
  status.setAttribute("role", state.scenariosError ? "alert" : "status");
  status.textContent = state.scenariosLoading ? "Loading saved scenarios…"
    : state.scenariosError ? `Could not load saved scenarios: ${state.scenariosError}${scenarios.length ? " Showing the last loaded list." : ""}`
    : state.savedScenarios === null ? "Open the editor to load saved scenarios."
    : scenarios.length ? "" : "No saved scenarios yet.";
  status.hidden = !status.textContent;

  const selected = scenarios.find((item) => item.id === state.selectedScenarioId);
  $("#scenarioEditingStatus").textContent = state.selectedScenarioId
    ? `Editing saved scenario · ${selected?.name || state.selectedScenarioId}`
    : "New unsaved scenario";
  $("#scenarioLibraryList").innerHTML = scenarios.map((item) => {
    const name = item.name || item.id;
    const isSelected = item.id === state.selectedScenarioId;
    const confirmingDelete = item.id === state.pendingScenarioDeleteId;
    const isDeleting = item.id === state.scenarioDeletingId;
    const isLoading = item.id === state.scenarioLoadingId;
    const updated = formatResultTime(item.updatedAt);
    const controls = confirmingDelete
      ? `<button class="secondary-button" type="button" data-cancel-scenario-delete="${escapeHTML(item.id)}" ${busy ? "disabled" : ""}>Cancel</button><button class="danger-button confirm-delete-scenario" type="button" data-confirm-scenario-delete="${escapeHTML(item.id)}" aria-label="${escapeHTML(`Confirm deletion of saved scenario: ${name}`)}" ${busy ? "disabled" : ""}>${isDeleting ? "Deleting…" : "Confirm delete"}</button>`
      : `<button class="secondary-button load-scenario-button" type="button" data-load-scenario="${escapeHTML(item.id)}" aria-label="${escapeHTML(`Load saved scenario: ${name}`)}" ${busy ? "disabled" : ""}>${isLoading ? "Loading…" : "Load"}</button><button class="secondary-button delete-scenario-button" type="button" data-delete-scenario="${escapeHTML(item.id)}" aria-label="${escapeHTML(`Delete saved scenario: ${name}`)}" ${busy ? "disabled" : ""}>Delete</button>`;
    return `<li class="scenario-library-item${isSelected ? " selected" : ""}"${isSelected ? ' aria-current="true"' : ""}>
      <div class="scenario-item-summary"><strong title="${escapeHTML(name)}">${escapeHTML(name)}</strong><small class="scenario-item-meta"><span>Updated ${escapeHTML(updated)}</span><span class="scenario-item-id" title="${escapeHTML(`Scenario ID: ${item.id}`)}"><span aria-hidden="true">ID ${escapeHTML(formatScenarioID(item.id))}</span><span class="visually-hidden">Scenario ID: ${escapeHTML(item.id)}</span></span></small></div>
      <div class="scenario-item-actions">${controls}</div>
    </li>`;
  }).join("");
}

async function refreshSavedScenarios() {
  if (scenarioOperationBusy()) return;
  state.scenariosLoading = true;
  state.scenariosError = "";
  state.scenarioActionError = "";
  renderSavedScenarios();
  try {
    const response = await scenarioRequest("/api/v1/scenarios", { cache: "no-store" });
    if (!Array.isArray(response)) throw new Error("Unexpected saved scenarios response.");
    const scenarios = response.map((item) => validateSavedScenario(item));
    state.savedScenarios = scenarios;
    if (state.pendingScenarioDeleteId && !scenarios.some((item) => item.id === state.pendingScenarioDeleteId)) {
      state.pendingScenarioDeleteId = null;
    }
    if (state.selectedScenarioId && !scenarios.some((item) => item.id === state.selectedScenarioId)) {
      state.selectedScenarioId = null;
      state.scenarioActionError = "The selected saved scenario no longer exists. The editor was retained as a new scenario.";
    }
  } catch (error) {
    state.scenariosError = error.message;
  } finally {
    state.scenariosLoading = false;
    renderSavedScenarios();
  }
}

async function loadSavedScenario(id) {
  if (scenarioOperationBusy() || !(state.savedScenarios || []).some((item) => item.id === id)) return;
  const version = ++state.scenarioLoadVersion;
  state.scenarioLoadingId = id;
  state.pendingScenarioDeleteId = null;
  state.scenarioActionError = "";
  renderSavedScenarios();
  try {
    const item = validateSavedScenario(await scenarioRequest(`/api/v1/scenarios/${encodeURIComponent(id)}`, { cache: "no-store" }), true);
    if (version !== state.scenarioLoadVersion) return;
    state.selectedScenarioId = item.id;
    $("#scenarioName").value = item.name;
    $("#scenarioText").value = item.yaml;
    resetScenarioValidation();
    $("#scenarioText").focus();
  } catch (error) {
    if (version === state.scenarioLoadVersion) state.scenarioActionError = `Could not load the saved scenario: ${error.message}`;
  } finally {
    if (version === state.scenarioLoadVersion) state.scenarioLoadingId = null;
    renderSavedScenarios();
  }
}

function startNewScenario() {
  if (scenarioOperationBusy()) return;
  ++state.scenarioLoadVersion;
  state.selectedScenarioId = null;
  state.pendingScenarioDeleteId = null;
  state.scenarioActionError = "";
  $("#scenarioName").value = "";
  const editor = $("#scenarioText");
  editor.value = defaultScenario;
  resetScenarioValidation();
  renderSavedScenarios();
  $("#scenarioName").focus();
}

function upsertSavedScenario(item) {
  const summary = { id: item.id, name: item.name, createdAt: item.createdAt, updatedAt: item.updatedAt };
  state.savedScenarios = [summary, ...(state.savedScenarios || []).filter((saved) => saved.id !== item.id)];
}

function focusScenarioListAction(attribute, id) {
  if (!id) return false;
  const target = [...document.querySelectorAll(`[${attribute}]`)].find((button) => button.getAttribute(attribute) === id);
  target?.focus();
  return Boolean(target);
}

async function saveEditedScenario(asNew = false) {
  if (scenarioOperationBusy()) return;
  const name = $("#scenarioName").value.trim();
  const yaml = $("#scenarioText").value;
  if (!name) {
    state.scenarioActionError = "Enter a name for the saved scenario.";
    renderSavedScenarios();
    $("#scenarioName").focus();
    return;
  }
  if (!yaml.trim()) {
    state.scenarioActionError = "Enter a YAML scenario to save.";
    renderSavedScenarios();
    $("#scenarioText").focus();
    return;
  }
  const updateID = !asNew && state.selectedScenarioId;
  state.scenarioSaving = true;
  state.pendingScenarioDeleteId = null;
  state.scenarioActionError = "";
  renderSavedScenarios();
  try {
    const path = updateID ? `/api/v1/scenarios/${encodeURIComponent(updateID)}` : "/api/v1/scenarios";
    const item = validateSavedScenario(await scenarioRequest(path, {
      method: updateID ? "PUT" : "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, yaml }),
    }, "mutation"), true);
    state.selectedScenarioId = item.id;
    $("#scenarioName").value = item.name;
    if ($("#scenarioText").value !== item.yaml) {
      $("#scenarioText").value = item.yaml;
      resetScenarioValidation();
    }
    upsertSavedScenario(item);
    showToast(`${updateID ? "Updated" : "Saved"} scenario: ${item.name}.`);
  } catch (error) {
    state.scenarioActionError = `${updateID ? "Could not update" : "Could not save"} the scenario: ${error.message}`;
  } finally {
    state.scenarioSaving = false;
    renderSavedScenarios();
  }
}

function requestScenarioDeletion(id) {
  if (scenarioOperationBusy() || !(state.savedScenarios || []).some((item) => item.id === id)) return;
  state.pendingScenarioDeleteId = id;
  state.scenarioActionError = "";
  renderSavedScenarios();
  focusScenarioListAction("data-confirm-scenario-delete", id);
}

function cancelScenarioDeletion(id) {
  if (state.scenarioDeletingId || state.pendingScenarioDeleteId !== id) return;
  state.pendingScenarioDeleteId = null;
  renderSavedScenarios();
  focusScenarioListAction("data-delete-scenario", id);
}

async function confirmScenarioDeletion(id) {
  if (scenarioOperationBusy() || state.pendingScenarioDeleteId !== id) return;
  const item = (state.savedScenarios || []).find((saved) => saved.id === id);
  if (!item) return;
  const index = state.savedScenarios.indexOf(item);
  const focusAfterDelete = state.savedScenarios[index + 1]?.id || state.savedScenarios[index - 1]?.id;
  let deleted = false;
  state.scenarioDeletingId = id;
  state.scenarioActionError = "";
  renderSavedScenarios();
  try {
    try {
      await scenarioRequest(`/api/v1/scenarios/${encodeURIComponent(id)}`, { method: "DELETE" }, "mutation");
    } catch (error) {
      if (error.status !== 404) throw error;
    }
    state.savedScenarios = (state.savedScenarios || []).filter((saved) => saved.id !== id);
    state.pendingScenarioDeleteId = null;
    if (state.selectedScenarioId === id) state.selectedScenarioId = null;
    deleted = true;
    showToast(`Deleted saved scenario: ${item.name || item.id}.`);
  } catch (error) {
    state.scenarioActionError = `Could not delete the saved scenario: ${error.message}`;
  } finally {
    state.scenarioDeletingId = null;
    renderSavedScenarios();
    if (deleted) {
      if (!focusScenarioListAction("data-delete-scenario", focusAfterDelete)) $("#newScenario").focus();
    } else {
      focusScenarioListAction("data-confirm-scenario-delete", id);
    }
  }
}

async function refreshSavedResults() {
  clearTimeout(state.resultsRefreshTimer);
  state.resultsRefreshTimer = null;
  if (state.resultsLoading) {
    state.resultsRefreshPending = true;
    return;
  }
  state.resultsLoading = true;
  state.resultsError = "";
  renderSavedResults();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);
  let refreshed = false;
  try {
    const results = await api("/api/v1/results", { cache: "no-store", signal: controller.signal });
    if (!Array.isArray(results)) throw new Error("Unexpected saved results response.");
    state.savedResults = results.filter((run) => !state.deletedResultIDs.has(run.id));
    refreshed = true;
  } catch (error) {
    state.resultsError = error.name === "AbortError" ? "Request timed out." : error.message;
  } finally {
    clearTimeout(timeout);
    state.resultsLoading = false;
    renderResultViews();
    if (state.resultsRefreshPending) {
      state.resultsRefreshPending = false;
      refreshSavedResults();
    } else if (refreshed && (state.savedResults || []).some(run => ["queued", "running"].includes(run.analysis?.state) || ["queued", "running"].includes(run.batchAnalysis?.state))) {
      state.resultsRefreshTimer = setTimeout(refreshSavedResults, 3000);
    }
  }
}

function currentBatchRuns(runs) {
  const previous = new Set(runs.flatMap(run => run.previousRunIds || []));
  return runs.filter(run => !previous.has(run.id));
}

function savedResultBatches(results, includeSingles = false) {
  const groups = new Map();
  for (const run of results) {
    if (!run.batchId) continue;
    if (!groups.has(run.batchId)) groups.set(run.batchId, { id: run.batchId, name: run.name || run.batchId, runs: [], expected: 1 });
    const batch = groups.get(run.batchId);
    batch.runs.push(run);
    batch.expected = Math.max(batch.expected, run.repetitions || 1);
  }
  return [...groups.values()].filter(batch => includeSingles || batch.expected > 1 || batch.runs.some(run => run.previousRunIds?.length)).map(batch => {
    const runs = currentBatchRuns(batch.runs), current = new Set(runs.map(run => run.id));
    return { ...batch, runs, previousRuns: batch.runs.filter(run => !current.has(run.id)),
      completed: runs.filter(run => run.state === "completed").length,
      active: batch.runs.some(isPendingRun),
      job: batch.runs.find(run => run.batchAnalysis)?.batchAnalysis,
    };
  });
}

function hasRunStarted(run) {
  return Boolean(run.startedAt && !String(run.startedAt).startsWith("0001-"));
}

function remainingBatchRuns(batch) {
  if (batch.active || !batch.runs.some(run => run.state === "failed" || run.state === "interrupted" && hasRunStarted(run))) return [];
  const lastAttempt = Math.max(0, ...batch.runs.filter(run => hasRunStarted(run) || ["completed", "failed"].includes(run.state)).map(run => run.iteration || 0));
  return batch.runs.filter(run => run.iteration > lastAttempt && !hasRunStarted(run) && ["canceled", "interrupted"].includes(run.state));
}

function retryBatchRuns(batch) {
  if (batch.active || batch.runs.some(run => !Number.isInteger(run.iteration) || run.iteration < 1 || run.iteration > batch.expected || run.state === "unreadable")) return [];
  if (new Set(batch.runs.map(run => run.iteration)).size !== batch.runs.length) return [];
  return batch.runs.filter(run => ["failed", "interrupted", "canceled"].includes(run.state)).sort((a, b) => a.iteration - b.iteration);
}

function retryBatchButton(batch, locked = false) {
  const runs = retryBatchRuns(batch);
  if (!runs.length) return "";
  const resuming = state.pendingResumes?.has(batch.id);
  const analyzing = ["queued", "running"].includes(batch.job?.state) || [...batch.runs, ...(batch.previousRuns || [])].some(run => ["queued", "running"].includes(run.analysis?.state));
  const label = batch.expected > 1 ? `Retry from run ${runs[0].iteration} (${runs.length})` : "Retry experiment";
  return `<button type="button" class="secondary-button batch-resume-button" data-retry-batch="${escapeHTML(batch.id)}" title="Clean up previous Peers, then restart unfinished runs from their first phase. Completed runs are kept. Previous attempts remain downloadable." aria-label="${escapeHTML(`${label}: ${batch.name}`)}" ${locked || analyzing || resuming || state.deletingResultId ? "disabled" : ""}>${resuming ? "Continuing…" : label}</button>`;
}

async function resumeSavedBatch(id, retry = false) {
  if (state.pendingResumes.has(id)) return;
  const batch = savedResultBatches(state.savedResults || [], true).find(batch => batch.id === id);
  const remaining = batch ? (retry ? retryBatchRuns(batch) : remainingBatchRuns(batch)) : [];
  if (!remaining.length) return;
  state.pendingResumes.add(id);
  renderSavedResults();
  try {
    const first = await api(`/api/v1/result-batches/${encodeURIComponent(id)}/${retry ? "retry" : "resume"}`, { method: "POST" });
    if (retry) {
      // The new ID belongs to the queued retry. Old attempts keep their state
      // and logs; do not optimistically turn those IDs back into running work.
      if (!first?.id || first.batchId !== id) throw new Error("Unexpected retry response. Refresh saved results to check the batch.");
      if (!(state.savedResults || []).some(run => run.id === first.id)) state.savedResults.push(first);
      if (state.snapshot) {
        state.snapshot.experiments ||= [];
        if (!state.snapshot.experiments.some(run => run.id === first.id)) state.snapshot.experiments.push(first);
      }
    } else {
      const ids = new Set(remaining.map(run => run.id));
      for (const run of [...(state.savedResults || []), ...(state.snapshot?.experiments || [])]) {
        if (ids.has(run.id)) { run.state = "queued"; run.error = ""; run.finishedAt = ""; }
      }
    }
    renderResultViews();
    showToast(retry ? `Queued ${remaining.length} unfinished ${remaining.length === 1 ? "run" : "runs"} from run ${remaining[0].iteration}. Previous Peers are cleaned up before execution.` : `Continuing ${remaining.length} remaining ${remaining.length === 1 ? "run" : "runs"}. Previous attempted runs are preserved.`);
    await refreshSavedResults();
  } catch (error) {
    showToast(error.message);
  } finally {
    state.pendingResumes.delete(id);
    renderSavedResults();
  }
}

function savedResultTable(runs, key, label = "Saved experiment results") {
  return `<div class="table-wrap" data-result-table="${escapeHTML(key)}"><table aria-label="${escapeHTML(label)}">
    <thead><tr><th scope="col">Experiment / ID</th><th scope="col">State</th><th scope="col">Started (local)</th><th scope="col">Finished (local)</th><th scope="col">Actions</th></tr></thead>
    <tbody>${runs.map(savedResultRow).join("")}</tbody>
  </table></div>`;
}

function savedResultBatch(batch) {
  const job = batch.job;
  const label = ["queued", "running"].includes(job?.state) ? `Batch mean · ${job.state === "queued" ? "Queued" : `${Math.floor(job.progress || 0)}%`}`
    : job?.state === "completed" ? "Batch mean · Ready" : ["failed", "interrupted"].includes(job?.state) ? "Batch mean · Retry" : "Analyze batch mean";
  const excluded = batch.runs.length - batch.completed;
  const missing = Math.max(0, batch.expected - batch.runs.length);
  const hint = batch.active ? "Available after all runs in this batch stop." : batch.completed < 2 ? "At least two completed runs are required." : "Analyze completed runs with equal weight; expand this series for individual Images.";
  const resuming = state.pendingResumes?.has(batch.id);
  const remaining = remainingBatchRuns(batch);
  const analyzing = ["queued", "running"].includes(job?.state) || [...batch.runs, ...(batch.previousRuns || [])].some(run => ["queued", "running"].includes(run.analysis?.state));
  const locked = resuming || batch.runs.some(resultLocked);
  return `<details class="saved-batch" data-result-batch="${escapeHTML(batch.id)}">
    <summary data-result-batch-toggle="${escapeHTML(batch.id)}">
      <svg class="saved-batch-chevron" viewBox="0 0 20 20" aria-hidden="true"><path d="m7 5 5 5-5 5"/></svg>
      <span class="saved-batch-heading"><strong>${escapeHTML(batch.name)}</strong><span class="result-id">Batch ${escapeHTML(batch.id)}</span><span class="result-id">${batch.runs.length} ${batch.runs.length === 1 ? "run" : "runs"} · ${batch.completed} / ${batch.expected} completed · ${batch.active ? "Batch still running" : `${excluded} excluded · ${missing} missing/unreadable`}</span></span>
      <span class="saved-batch-disclosure" aria-hidden="true"><span class="saved-batch-show">Show runs</span><span class="saved-batch-hide">Hide runs</span></span>
      <span class="saved-batch-actions">
        ${retryBatchButton(batch, locked)}
        ${remaining.length ? `<button type="button" class="secondary-button batch-resume-button" data-resume-batch="${escapeHTML(batch.id)}" title="Continue runs that never started. Previously attempted runs are preserved; failed runs are skipped." aria-label="${escapeHTML(`Continue ${remaining.length} remaining runs: ${batch.name}`)}" ${locked || analyzing || state.deletingResultId ? "disabled" : ""}>${resuming ? "Continuing…" : `Continue remaining (${remaining.length})`}</button>` : ""}
        <button type="button" class="secondary-button batch-images-button" data-batch-images="${escapeHTML(batch.id)}" title="${escapeHTML(hint)}" aria-label="${escapeHTML(`Analyze batch mean: ${batch.name}`)}" ${batch.active || resuming || batch.completed < 2 || state.deletingResultId ? "disabled" : ""}>${label}</button>
        <button type="button" class="secondary-button batch-delete-button" data-delete-batch="${escapeHTML(batch.id)}" title="${locked ? "Available after all runs in this group stop." : "Delete every saved run and the mean analysis in this group."}" aria-label="${escapeHTML(`Delete result group: ${batch.name}`)}" ${locked || state.deletingResultId ? "disabled" : ""}>${state.pendingDelete?.isBatch && state.deletingResultId === batch.id ? "Deleting…" : "Delete group"}</button>
      </span>
    </summary>
    ${savedResultTable(batch.runs, `batch:${batch.id}`, `Runs in ${batch.name} · ${batch.id}`)}
    ${batch.previousRuns?.length ? `<p class="results-help">Previous attempts · ${batch.previousRuns.length} saved ${batch.previousRuns.length === 1 ? "record" : "records"}, excluded from batch progress and mean.</p>${savedResultTable(batch.previousRuns, `previous:${batch.id}`, `Previous attempts in ${batch.name}`)}` : ""}
  </details>`;
}

function savedResultsMarkup(results) {
  const batches = new Map(savedResultBatches(results).map(batch => [batch.id, batch]));
  const shown = new Set(), sections = [];
  let individual = [], previousBatch = "";
  const flushIndividual = () => {
    if (!individual.length) return;
    sections.push(savedResultTable(individual, `individual:${previousBatch}`));
    individual = [];
  };
  // Keep the archive order; each batch occupies its first run's position.
  for (const run of results) {
    const batch = batches.get(run.batchId);
    if (!batch) { individual.push(run); continue; }
    if (shown.has(batch.id)) continue;
    flushIndividual();
    sections.push(savedResultBatch(batch));
    shown.add(batch.id);
    previousBatch = batch.id;
  }
  flushIndividual();
  return sections.join("");
}

function savedResultFocus(control) {
  const attribute = ["data-result-batch-toggle", "data-retry-batch", "data-resume-batch", "data-batch-images", "data-delete-batch", "data-result-images", "data-result-download", "data-delete-result"].find(name => control?.hasAttribute(name));
  return attribute ? { attribute, id: control.getAttribute(attribute), batch: control.closest("details[data-result-batch]")?.dataset.resultBatch } : null;
}

function restoreSavedResultFocus(focus) {
  if (!focus) return;
  const list = $("#savedResultsRows");
  const target = [...list.querySelectorAll(`[${focus.attribute}]`)].find(element => element.getAttribute(focus.attribute) === focus.id);
  const summary = [...list.querySelectorAll("[data-result-batch-toggle]")].find(element => element.dataset.resultBatchToggle === focus.batch);
  const candidates = [target, summary, $("#refreshResults"), $('[data-panel-toggle="results"]')];
  candidates.find(element => element && !element.disabled && element.getClientRects().length)?.focus({ preventScroll: true });
}

function rememberResultDialogFocus(dialog, control) {
  if (!$("#savedResultsRows").contains(control)) return;
  const focus = savedResultFocus(control);
  // A job update can replace the opener while its modal is displayed.
  $(dialog).addEventListener("close", () => restoreSavedResultFocus(focus), { once: true });
}

function updateSavedResultsList(markup) {
  const list = $("#savedResultsRows");
  if (renderedHTML.get(list) === markup) return;
  // Polling and analysis progress may change the markup while a series is open.
  // Restore its native disclosure, keyboard focus and horizontal table position
  // synchronously, before the browser paints the updated list.
  const open = new Set([...list.querySelectorAll("details[data-result-batch][open]")].map(details => details.dataset.resultBatch));
  const scroll = new Map([...list.querySelectorAll("[data-result-table]")].map(table => [table.dataset.resultTable, table.scrollLeft]));
  const focused = list.contains(document.activeElement) ? document.activeElement : null;
  const focus = savedResultFocus(focused);
  setHTML(list, markup);
  for (const details of list.querySelectorAll("details[data-result-batch]")) details.open = open.has(details.dataset.resultBatch);
  for (const table of list.querySelectorAll("[data-result-table]")) table.scrollLeft = scroll.get(table.dataset.resultTable) || 0;
  restoreSavedResultFocus(focus);
}

function renderSavedResults() {
  const results = state.savedResults || [];
  const status = $("#savedResultsStatus");
  const refresh = $("#refreshResults");
  refresh.disabled = state.resultsLoading;
  setText(refresh, "Refresh");
  refresh.setAttribute("aria-busy", String(state.resultsLoading));
  $("#savedResultsTable").setAttribute("aria-busy", String(state.resultsLoading));
  status.classList.toggle("error", Boolean(state.resultsError));
  status.setAttribute("role", state.resultsError ? "alert" : "status");
  setText(status, state.resultsLoading && state.savedResults === null ? "Loading saved results…"
    : state.resultsError ? `Could not load saved results: ${state.resultsError} Use Refresh to try again.${results.length ? " Showing the last loaded list." : ""}`
    : results.length ? "" : "No saved results yet.");
  status.hidden = !status.textContent;
  $("#savedResultsTable").hidden = results.length === 0;
  updateSavedResultsList(savedResultsMarkup(results));
}

function savedResultRow(run) {
  const singleBatch = run.batchId && run.repetitions === 1 ? savedResultBatches(state.savedResults || [], true).find(batch => batch.id === run.batchId) : null;
  const retry = singleBatch && !singleBatch.previousRuns.length ? retryBatchButton(singleBatch, resultLocked(run)) : "";
  const stateHint = run.state === "interrupted" ? "Saved by a previous Controller; this run was not resumed."
    : run.state === "unreadable" ? "Saved metadata could not be read." : run.state;
  return `<tr>
    <td class="result-name"><strong>${escapeHTML(run.name || run.id)}</strong><span class="result-id">${escapeHTML(run.id)}</span><span class="result-id result-meta">${run.repetitions > 1 ? `<span>Run ${formatNumber(run.iteration)} of ${formatNumber(run.repetitions)}</span>` : ""} · ${resultSourceSize(run)}</span></td>
    <td><span class="status-pill ${escapeHTML(run.state)}" title="${escapeHTML(stateHint)}">${escapeHTML(run.state)}</span></td>
    <td>${escapeHTML(formatResultTime(run.startedAt))}</td>
    <td>${escapeHTML(formatResultTime(run.finishedAt))}</td>
    <td><div class="result-actions">${retry}${resultImagesButton(run)}${resultDownloadLink(run)}<button class="delete-result-button" type="button" data-delete-result="${escapeHTML(run.id)}" aria-label="${escapeHTML(`Delete saved result: ${run.name || run.id}`)}" title="${resultLocked(run) ? "Available after this run and its batch have stopped." : "Delete this run's saved result."}" ${resultLocked(run) || state.deletingResultId ? "disabled" : ""}>${state.deletingResultId === run.id ? "Deleting…" : "Delete"}</button></div></td>
  </tr>`;
}

function requestResultDeletion(id, isBatch = false) {
  if (state.deletingResultId) return;
  const run = isBatch ? savedResultBatches(state.savedResults || []).find(batch => batch.id === id)
    : (state.savedResults || []).find((result) => result.id === id);
  if (!run || (isBatch ? run.runs.some(resultLocked) : resultLocked(run))) return;
  state.pendingDelete = isBatch ? { ...run, isBatch: true } : run;
  $("#deleteResultHeading").textContent = isBatch ? "Delete result group?" : "Delete saved result?";
  $("#deleteResultName").textContent = run.name || run.id;
  $("#deleteResultID").textContent = isBatch ? `Batch ${run.id} · ${run.runs.length} saved runs` : run.id;
  $("#deleteResultHelp").textContent = isBatch
    ? `Delete all ${run.runs.length} saved runs in this group, including their scenarios, metadata, logs, individual analyses, and the group mean analysis. This cannot be undone. Previously collected Prometheus and Grafana time series remain.`
    : "Delete this run's saved scenario, metadata, and event log. This cannot be undone. Previously collected Prometheus and Grafana time series remain.";
  $("#deleteResultError").textContent = "";
  $("#confirmDeleteResult").disabled = false;
  $("#confirmDeleteResult").textContent = isBatch ? "Delete group" : "Delete result";
  $("#cancelDeleteResult").disabled = false;
  $("#deleteResultDialog").showModal();
}

async function confirmResultDeletion() {
  const run = state.pendingDelete;
  if (!run || state.deletingResultId) return;
  const latest = run.isBatch ? (state.savedResults || []).filter(result => result.batchId === run.id)
    : [(state.savedResults || []).find((result) => result.id === run.id) || run];
  if (latest.some(resultLocked) || (run.isBatch && run.runs.some(resultLocked))) {
    $("#deleteResultError").textContent = "This run or its batch is active. Stop it before deleting its saved result.";
    return;
  }
  state.deletingResultId = run.id;
  $("#deleteResultError").textContent = "";
  $("#confirmDeleteResult").disabled = true;
  $("#confirmDeleteResult").textContent = "Deleting…";
  $("#cancelDeleteResult").disabled = true;
  renderSavedResults();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);
  try {
    let deletedIDs = run.isBatch ? [] : [run.id];
    try {
      const response = await api(`/api/v1/${run.isBatch ? "result-batches" : "results"}/${encodeURIComponent(run.id)}`, { method: "DELETE", signal: controller.signal });
      if (run.isBatch) {
        if (!Array.isArray(response?.deletedIds) || response.deletedIds.some(id => typeof id !== "string" || !id)) throw new Error("Unexpected group deletion response. Refresh the list to check.");
        deletedIDs = response.deletedIds;
      }
    } catch (error) {
      if (error.status !== 404) throw error;
    }
    for (const id of deletedIDs) {
      state.deletedResultIDs.add(id);
      globalThis.KPLResultImages?.remove(id);
    }
    if (run.isBatch) globalThis.KPLResultImages?.remove(run.id);
    state.savedResults = (state.savedResults || []).filter((result) => !state.deletedResultIDs.has(result.id));
    state.pendingDelete = null;
    $("#deleteResultDialog").close();
    showToast(`Deleted ${run.isBatch ? "result group" : "saved result"}: ${run.name || run.id}.`);
  } catch (error) {
    $("#deleteResultError").textContent = error.name === "AbortError"
      ? "Deletion timed out; it may still finish on the Controller. Refresh the list to check, or retry the deletion."
      : error.status === 409
      ? "This result is active, belongs to an active batch, or is being downloaded. Wait for it to finish, then try again."
      : `Could not delete the ${run.isBatch ? "result group" : "saved result"}: ${error.message}`;
  } finally {
    clearTimeout(timeout);
    state.deletingResultId = null;
    $("#confirmDeleteResult").disabled = false;
    $("#confirmDeleteResult").textContent = run.isBatch ? "Delete group" : "Delete result";
    $("#cancelDeleteResult").disabled = false;
    renderResultViews();
    // A slow list refresh must not keep the deletion dialog locked.
    void refreshSavedResults();
  }
}

function renderAgents(agents) {
  rememberAgents(agents.map((agent) => agent.id));
  if (!agents.length) {
    setHTML($("#agentRows"), '<tr><td colspan="8" class="empty-cell">No Agents registered.</td></tr>');
    return;
  }
  setHTML($("#agentRows"), [...agents].sort((a, b) => agentNumber(a.id) - agentNumber(b.id)).map((agent) => {
    const usage = agent.capacity ? Math.min(100, Math.round((agent.activeNodes / agent.capacity) * 100)) : 0;
    return `<tr>
      <td class="agent-number">${agentNumber(agent.id)}</td>
      <td><span class="agent-name">${escapeHTML(agent.name)}</span><span class="agent-id">${escapeHTML(agent.id)}</span></td>
      <td><span class="state-dot ${escapeHTML(agent.state)}">${escapeHTML(agent.state)}</span></td>
      <td>${escapeHTML(agent.hostname || "—")}</td>
      <td>${formatNumber(agent.activeNodes)} / ${formatNumber(agent.capacity)}</td>
      <td><div class="usage"><div class="usage-track"><i style="width:${usage}%"></i></div><span>${usage}%</span></div></td>
      <td>${escapeHTML(relativeTime(agent.lastSeen))}</td>
      <td>${agentMetricsLink(agent)}</td>
    </tr>`;
  }).join(""));
}

function safeAgentMetricsURL(value) {
  if (typeof value !== "string" || !value || value.trim() !== value) return "";
  try {
    const url = new URL(value);
    const port = url.port ? Number(url.port) : (url.protocol === "https:" ? 443 : 80);
    const host = url.hostname.replace(/^\[|\]$/g, "").replace(/\.$/, "").toLowerCase();
    const localHost = host === "localhost" || host === "localhost.localdomain" || host.endsWith(".localhost") || host === "0.0.0.0" || host === "::" || host === "::1" || host.startsWith("127.") || host.startsWith("::ffff:7f");
    if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || !url.hostname || localHost || url.pathname !== "/metrics" || url.search || url.hash || value.includes("#") || !Number.isInteger(port) || port < 1 || port > 65535) return "";
    return url.href;
  } catch {
    return "";
  }
}

function agentMetricsLink(agent) {
  const href = safeAgentMetricsURL(agent?.metricsUrl);
  if (!href) return "";
  const name = agent.name || agent.id || "Agent";
  return `<a class="agent-metrics-link" href="${escapeHTML(href)}" target="_blank" rel="noopener noreferrer" title="${escapeHTML(`Open ${name} metrics in a new tab`)}">Metrics<span class="visually-hidden"> for ${escapeHTML(name)} (opens in a new tab)</span></a>`;
}

function renderEvents(events) {
  const recent = events.slice(-40).reverse();
  if (!recent.length) {
    setHTML($("#eventList"), '<li class="empty-copy">No events collected.</li>');
    return;
  }
  setHTML($("#eventList"), recent.map((event) => {
    const latency = event.fields?.latencyAvailable === false ? "latency unavailable" : event.latencyMs > 0 ? `${formatNumber(event.latencyMs, 1)} ms` : "";
    const nodePrefix = event.runId ? `${event.runId}-` : "";
    const nodeLabel = nodePrefix && event.nodeId?.startsWith(nodePrefix) ? event.nodeId.slice(nodePrefix.length) : event.nodeId;
    const direction = event.fields?.direction;
    const peerArrow = direction === "send" ? "→" : direction === "drop" ? "⇢" : "←";
    const controlEntries = Number(event.fields?.controlEntries);
    const messageIDs = Number(event.fields?.messageIdCount);
    const peerExchange = Number(event.fields?.peerExchangeCount);
    const control = event.fields?.controlType ? [
      Number.isFinite(controlEntries) ? `${controlEntries} ${controlEntries === 1 ? "entry" : "entries"}` : "",
      Number.isFinite(messageIDs) && messageIDs > 0 ? `${messageIDs} message IDs` : "",
      Number.isFinite(peerExchange) && peerExchange > 0 ? `${peerExchange} PX records` : "",
    ].filter(Boolean).join(" · ") : "";
    const detail = [nodeLabel, event.remotePeerId ? `${peerArrow} ${event.remotePeerId.slice(0, 10)}` : "", control, latency].filter(Boolean).join(" · ");
    const runID = event.runId ? `<small class="event-run-id" title="${escapeHTML(`Experiment ID: ${event.runId}`)}">Experiment · ${escapeHTML(event.runId)}</small>` : "";
    return `<li class="event-item"><time datetime="${escapeHTML(event.timestamp)}">${new Date(event.timestamp).toLocaleTimeString("en-US", { hour12: false })}</time><span class="event-type" title="${escapeHTML(event.type)}">${escapeHTML(event.type)}</span><span class="event-summary" title="${escapeHTML(detail)}">${escapeHTML(detail)}</span>${runID}</li>`;
  }).join(""));
}

function svgElement(tag, attributes = {}, text) {
  const element = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, value);
  if (text !== undefined) element.textContent = text;
  return element;
}

function renderTopology(nodes, edges) {
  ({ nodes, edges } = topologyData(nodes, edges));
  const populationLabel = peerPopulationLabel(peerPopulation(nodes));
  const topology = state.topology;
  stopTopologyMotion();
  if (isPanelCollapsed("topology")) {
    const links = filterTopologyEdges(nodes, edges, topology.filters).length;
    setText($("#topologyLinkCount"), `${populationLabel} · ${formatNumber(links)} visible links`);
    return;
  }
  rememberAgents(nodes.map((node) => node.agentId));
  if (!nodes.some((node) => node.id === topology.selected)) topology.selected = null;
  if (!nodes.some((node) => node.id === topology.hovered)) topology.hovered = null;
  const topicSelect = $("#topologyTopic");
  const topics = observedTopologyTopics(nodes, edges);
  if (topology.filters.topic && !topics.includes(topology.filters.topic)) topics.push(topology.filters.topic);
  const topicOptions = ["", ...topics];
  if (JSON.stringify([...topicSelect.options].map((option) => option.value)) !== JSON.stringify(topicOptions)) {
    topicSelect.replaceChildren(...topicOptions.map((topic) => {
      const option = document.createElement("option");
      option.value = topic;
      option.textContent = topic || "All topics";
      return option;
    }));
  }
  topicSelect.value = topology.filters.topic;
  const connections = filterTopologyEdges(nodes, edges, topology.filters);
  const agents = [...(state.snapshot?.agents || [])].sort((a, b) => agentNumber(a.id) - agentNumber(b.id));
  const svg = $("#topology");
  const focusedNodeID = document.activeElement?.closest?.("[data-node-id]")?.dataset.nodeId;
  const width = svg.clientWidth || 1000, height = svg.clientHeight || 560;
  const layout = layoutTopology(nodes, agents, topology.layout, width);
  const signature = JSON.stringify([
    nodes.map((node) => [node.id, node.agentId]).sort(),
    connections.map((edge) => [edge.source, edge.target, edge.protocol, edge.topics]).sort(),
    layout.groups.map((group) => group.id), layout.bounds,
  ]);
  const changed = topology.signature !== signature;
  topology.signature = signature;
  const domSignature = JSON.stringify([
    signature, width, height,
    nodes.map((node) => [node.id, node.role, node.state, layout.positions.get(node.id).slot]).sort(),
    connections.map((edge) => [edge.source, edge.target, edge.protocol, edge.topics, edge.reportedBy, Object.entries(edge.topicReports).sort()]).sort(),
    layout.groups.map((group) => [group.id, group.count]),
  ]);
  const reuseElements = topology.domSignature === domSignature;
  topology.domSignature = domSignature;
  const previous = topology.graph;
  const graph = { nodes, edges: connections, ...layout, width, height,
    peerElements: reuseElements ? previous.peerElements : [],
    edgeElements: reuseElements ? previous.edgeElements : [] };
  topology.graph = graph;
  if (changed) {
    reheatTopologyLayout(graph);
    // Respect reduced motion without sacrificing a readable initial layout.
    // User-paused layouts retain their existing positions instead.
    if (topology.motion.reduced && !topology.motion.overridden) {
      for (let i = 0; i < 36; i++) stepTopologyLayout(graph, connections, { pinnedID: topology.selected });
    }
  }
  $("#topologyEmpty").hidden = nodes.length > 0;
  setText($("#topologyLinkCount"), `${populationLabel} · ${formatNumber(connections.length)} visible links`);
  const observed = nodes.filter((node) => node.overlayObservedAt && !node.overlayObservedAt.startsWith("0001-")).length;
  setText($("#topologyReportStatus"), nodes.length && observed < nodes.length
    ? `Overlay reports: ${observed}/${nodes.length} Peers. Waiting for reports from the remaining Peers; older Peers may report transport only.`
    : "Each slice is an Agent. Peers arrange around their visible connections; lines show relationships, not packet traffic.");
  if (!reuseElements) {
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    const world = svgElement("g", { id: "topologyWorld" });
    svg.replaceChildren(world);
    for (const [index, group] of layout.groups.entries()) {
      const background = svgElement("g", { class: "topology-agent", style: `--sector-hue: ${200 + (index % 4) * 17}` });
      background.append(svgElement("path", { d: topologySectorPath(group), class: "topology-agent-background" }));
      const label = svgElement("g", { class: "topology-agent-heading", transform: `translate(${group.labelX} ${group.labelY})` });
      label.append(svgElement("rect", { x: -21, y: -18, width: 42, height: 32, rx: 11, class: "topology-agent-badge" }));
      label.append(svgElement("text", { x: 0, y: 5, "text-anchor": "middle", class: "topology-label", "aria-label": `Agent ${agentNumber(group.id)}` }, agentNumber(group.id)));
      label.append(svgElement("text", { x: 0, y: 31, "text-anchor": "middle", class: "topology-agent-count" }, `${group.count} Peers`));
      label.append(svgElement("title", {}, `Agent ${agentNumber(group.id)} · ${group.id}`));
      background.append(label);
      world.append(background);
    }
    // Paint low-emphasis transport/routing relations below the GossipSub mesh.
    const order = { transport: 0, kademlia: 1, gossipsub: 2 };
    for (const edge of [...connections].sort((a, b) => order[a.protocol] - order[b.protocol])) {
      const path = svgElement("path", {
        class: `topology-edge ${edge.protocol}`, "data-source": edge.source, "data-target": edge.target,
      });
      const layer = edge.protocol === "gossipsub" ? "GossipSub mesh" : edge.protocol === "kademlia" ? "Kademlia routing" : "Transport";
      const reports = edge.protocol === "gossipsub" && edge.topics.length
        ? edge.topics.map((topic) => `${topic}: ${(edge.topicReports[topic] || []).join(", ") || "reporter not available"}`).join("\n")
        : edge.reportedBy.join(", ") || "reporter not available";
      path.append(svgElement("title", {}, `${layer}: ${edge.source} — ${edge.target}\nReported by: ${reports}`));
      world.append(path);
      graph.edgeElements.push({ element: path, edge });
    }
    if (layout.groups.length) {
      const { cx, cy } = layout.groups[0];
      const hub = svgElement("g", { class: "topology-hub", transform: `translate(${cx} ${cy})` });
      hub.append(svgElement("title", {}, populationLabel));
      hub.append(svgElement("circle", { r: 32 }));
      hub.append(svgElement("text", { "text-anchor": "middle", y: 2, class: "topology-hub-count" }, formatNumber(nodes.length)));
      hub.append(svgElement("text", { "text-anchor": "middle", y: 17, class: "topology-hub-label" }, "TOTAL"));
      world.append(hub);
    }
    for (const node of nodes) {
      const point = layout.positions.get(node.id);
      const mode = node.state === "starting" ? "starting" : node.state !== "ready" ? "issue" : node.role === "boot" ? "boot" : "worker";
      const peer = svgElement("g", {
        class: "topology-peer", "data-node-id": node.id, tabindex: 0, role: "button",
        "aria-label": `Peer ${point.slot}, Agent ${agentNumber(node.agentId)}: ${node.id}, ${node.state}`,
        "aria-pressed": String(topology.selected === node.id),
      });
      peer.append(svgElement("circle", { r: 17, class: "topology-hit-area" }));
      peer.append(svgElement("circle", { r: node.role === "boot" ? 8 : 6.5, class: `topology-node ${mode}` }));
      peer.append(svgElement("text", { x: 0, y: 20, "text-anchor": "middle", class: "topology-slot" }, point.slot));
      peer.append(svgElement("title", {}, `${node.id}\nAgent ${agentNumber(node.agentId)} · ${node.role} · ${node.state}\nSelect to inspect relationships and Peer details.`));
      world.append(peer);
      graph.peerElements.push({ element: peer, id: node.id });
    }
  }
  paintTopologyPositions();
  if (topology.autoFit) topology.camera = fitTopologyCamera(layout.bounds, width, height);
  applyTopologyCamera();
  highlightTopology();
  if (!reuseElements && focusedNodeID) {
    [...svg.querySelectorAll("[data-node-id]")].find((peer) => peer.dataset.nodeId === focusedNodeID)?.focus({ preventScroll: true });
  }
  updateTopologyMotionControl();
  startTopologyMotion();
}

function paintTopologyPositions() {
  const graph = state.topology.graph;
  if (!graph) return;
  for (const { element, id } of graph.peerElements) {
    const point = graph.positions.get(id);
    element.setAttribute("transform", `translate(${point.x} ${point.y})`);
  }
  for (const { element, edge } of graph.edgeElements) {
    const a = graph.positions.get(edge.source), b = graph.positions.get(edge.target);
    const dx = b.x - a.x, dy = b.y - a.y, distance = Math.max(1, Math.hypot(dx, dy));
    const bend = edge.protocol === "kademlia" ? -12 : edge.protocol === "gossipsub" ? 12 : 0;
    element.setAttribute("d", `M ${a.x} ${a.y} Q ${(a.x + b.x) / 2 - dy / distance * bend} ${(a.y + b.y) / 2 + dx / distance * bend} ${b.x} ${b.y}`);
  }
}

function stopTopologyMotion() {
  const motion = state.topology.motion;
  if (motion.frame !== null) cancelAnimationFrame(motion.frame);
  motion.frame = null;
}

function startTopologyMotion() {
  const topology = state.topology, motion = topology.motion;
  if (!motion.enabled || document.hidden || isPanelCollapsed("topology") || !topology.graph?.nodes.length || motion.frame !== null) return;
  motion.frame = requestAnimationFrame(animateTopology);
}

function animateTopology(timestamp) {
  const topology = state.topology, motion = topology.motion;
  motion.frame = null;
  if (!motion.enabled || document.hidden || isPanelCollapsed("topology") || !topology.graph) return;
  // Cap work at 30 frames/s and avoid a jump after a background-tab pause.
  if (topology.drag || timestamp - motion.lastFrame < 1000 / 30) { startTopologyMotion(); return; }
  motion.lastFrame = timestamp;
  const moving = stepTopologyLayout(topology.graph, topology.graph.edges, { pinnedID: topology.hovered || topology.selected });
  paintTopologyPositions();
  if (moving) startTopologyMotion();
}

function updateTopologyMotionControl() {
  const button = $("#topologyMotion"), motion = state.topology.motion;
  setText(button, motion.enabled ? "Pause motion" : "Resume motion");
  button.setAttribute("aria-pressed", String(!motion.enabled));
}

function applyTopologyCamera() {
  const { x, y, scale } = state.topology.camera;
  $("#topologyWorld")?.setAttribute("transform", `translate(${x} ${y}) scale(${scale})`);
  $("#topologyZoomLevel").textContent = `${Math.round(scale * 100)}%`;
}

function fitTopology() {
  const graph = state.topology.graph;
  if (!graph) return;
  state.topology.autoFit = true;
  state.topology.camera = fitTopologyCamera(graph.bounds, graph.width, graph.height);
  applyTopologyCamera();
}

function zoomTopology(factor, point) {
  const topology = state.topology, graph = topology.graph;
  if (!graph) return;
  const anchor = point || { x: graph.width / 2, y: graph.height / 2 };
  const camera = topology.camera;
  const scale = Math.max(0.01, Math.min(8, camera.scale * factor));
  const ratio = scale / camera.scale;
  topology.camera = { x: anchor.x - (anchor.x - camera.x) * ratio, y: anchor.y - (anchor.y - camera.y) * ratio, scale };
  topology.autoFit = false;
  applyTopologyCamera();
}

function highlightTopology() {
  const topology = state.topology, graph = topology.graph;
  if (!graph) return;
  const focused = topology.hovered || topology.selected;
  const neighbors = new Set([focused]);
  for (const edge of graph.edges) {
    if (edge.source === focused) neighbors.add(edge.target);
    if (edge.target === focused) neighbors.add(edge.source);
  }
  for (const edge of document.querySelectorAll(".topology-edge")) {
    const connected = edge.dataset.source === focused || edge.dataset.target === focused;
    edge.classList.toggle("is-active", Boolean(focused && connected));
    edge.classList.toggle("is-muted", Boolean(focused && !connected));
  }
  for (const peer of document.querySelectorAll(".topology-peer")) {
    peer.classList.toggle("is-selected", peer.dataset.nodeId === focused);
    peer.classList.toggle("is-muted", Boolean(focused && !neighbors.has(peer.dataset.nodeId)));
    peer.setAttribute("aria-pressed", String(peer.dataset.nodeId === topology.selected));
  }
  $("#clearTopologySelection").disabled = !topology.selected;
  renderTopologyDetails(graph.nodes.find((node) => node.id === focused));
}

function renderTopologyDetails(node) {
  const details = $("#topologyDetails");
  if (!node) {
    setHTML(details, '<p class="topology-detail-hint">Hover over a Peer to highlight its visible neighbors. Select it to keep the details open. Drag the background to pan; use + / − or Ctrl + scroll to zoom.</p>');
    return;
  }
  const graph = state.topology.graph;
  const edges = graph.edges.filter((edge) => edge.source === node.id || edge.target === node.id);
  const layers = { kademlia: "Kademlia routing", gossipsub: "GossipSub mesh", transport: "Transport" };
  const metadata = node.metadata || {};
  const scores = Object.values(node.peerScores || {}).filter(Number.isFinite);
  const scoreSummary = metadata.scoreEnabled === "false" ? "Disabled" : scores.length
    ? `${formatNumber(scores.length)} scores · Average: ${(scores.reduce((sum, score) => sum + score, 0) / scores.length).toFixed(2)}`
    : "0 scores · Average: N/A";
  const pubsub = metadata.pubsubEnabled === "false" ? "Off" : `${metadata.pubsubRouter || "pubsub"} / ${metadata.topicMode || "subscribe"}`;
  const dht = metadata.dhtEnabled === "false" ? "Off" : metadata.dhtMode || "server";
  const entries = [
    ["Node ID", node.id], ["Peer ID", node.peerId || "Not reported"],
    ["Placement", `Agent ${agentNumber(node.agentId)} · Peer ${graph.positions.get(node.id).slot} · ${node.group || node.role || "—"}`],
    ["State / profile", `${node.state} · ${node.type || node.role || "—"}${node.profile ? ` · ${node.profile}` : ""}`],
    ["Overlay observed", formatResultTime(node.overlayObservedAt)],
    ["Reported peers", `${(node.routingPeers || []).length} routing · ${Object.values(node.meshPeers || {}).reduce((sum, peers) => sum + (peers || []).length, 0)} mesh memberships · ${(node.connectedPeers || []).length} transport`],
    ["Peer scores", scoreSummary], ["PubSub router / topic mode", pubsub], ["DHT mode", dht],
    ["Topics", metadata.topics || "No topics reported"], ["Runtime", metadata.runtime || "Not reported"],
  ];
  if (metadata.containerId) entries.push(["Container ID", metadata.containerId]);
  let network = {};
  try { network = JSON.parse(metadata.network || "{}"); } catch { /* Legacy metadata can omit network settings. */ }
  if (!network || typeof network !== "object") network = {};
  const shaping = [network.delay && `delay ${network.delay}`, network.jitter && `jitter ${network.jitter}`, network.lossPercent != null && `loss ${network.lossPercent}%`, network.rateMbps > 0 && `${network.rateMbps} Mbps`].filter(Boolean).join(" · ");
  if (shaping) entries.push(["Configured network", shaping]);
  if (node.error) entries.push(["Error", node.error]);
  const relations = edges.map((edge) => {
    const other = graph.nodes.find((candidate) => candidate.id === (edge.source === node.id ? edge.target : edge.source));
    const slot = graph.positions.get(other.id).slot;
    const reports = topologyReportSummary(edge, node.id);
    return `<li><i class="link-key ${edge.protocol}" aria-hidden="true"></i><span><strong>${escapeHTML(layers[edge.protocol])}</strong> · Agent ${agentNumber(other.agentId)} / Peer ${slot}<small>${escapeHTML(other.id)} · ${escapeHTML(reports)}</small></span></li>`;
  }).join("");
  setHTML(details, `<dl class="topology-detail-grid">${entries.map(([label, value]) => `<div><dt>${escapeHTML(label)}</dt><dd>${escapeHTML(value)}</dd></div>`).join("")}</dl><div class="topology-neighbors"><h3>Visible relationships (${edges.length})</h3>${relations ? `<ul>${relations}</ul>` : '<p class="topology-detail-hint">No relationships match the current layer and topic filters.</p>'}</div>`);
}

function setupTopologyControls() {
  const motion = state.topology.motion;
  const preference = window.matchMedia?.("(prefers-reduced-motion: reduce)");
  const applyPreference = () => {
    motion.reduced = Boolean(preference?.matches);
    if (!motion.overridden) motion.enabled = !motion.reduced;
    updateTopologyMotionControl();
    if (!motion.enabled) stopTopologyMotion();
    else startTopologyMotion();
  };
  applyPreference();
  preference?.addEventListener("change", applyPreference);
  $("#topologyMotion").addEventListener("click", () => {
    motion.overridden = true;
    motion.enabled = !motion.enabled;
    updateTopologyMotionControl();
    if (!motion.enabled) stopTopologyMotion();
    else {
      if (state.topology.graph) reheatTopologyLayout(state.topology.graph, 0.3);
      startTopologyMotion();
    }
  });
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) stopTopologyMotion();
    else startTopologyMotion();
  });
  window.addEventListener("pagehide", stopTopologyMotion);
  for (const protocol of ["transport", "kademlia", "gossipsub"]) {
    $(`#show${protocol}`).addEventListener("change", (event) => {
      state.topology.filters[protocol] = event.target.checked;
      if (state.snapshot) renderTopology(state.snapshot.nodes || [], state.snapshot.edges || []);
    });
  }
  $("#topologyTopic").addEventListener("change", (event) => {
    state.topology.filters.topic = event.target.value;
    if (state.snapshot) renderTopology(state.snapshot.nodes || [], state.snapshot.edges || []);
  });
  $("#topologyZoomIn").addEventListener("click", () => zoomTopology(1.25));
  $("#topologyZoomOut").addEventListener("click", () => zoomTopology(0.8));
  $("#topologyFit").addEventListener("click", fitTopology);
  $("#clearTopologySelection").addEventListener("click", () => { state.topology.selected = null; state.topology.hovered = null; highlightTopology(); });
  const svg = $("#topology");
  const focusPeer = (event) => event.target.closest("[data-node-id]")?.dataset.nodeId || null;
  svg.addEventListener("pointerover", (event) => { state.topology.hovered = focusPeer(event); highlightTopology(); });
  svg.addEventListener("pointerleave", () => { state.topology.hovered = null; highlightTopology(); });
  svg.addEventListener("focusin", (event) => { state.topology.hovered = focusPeer(event); highlightTopology(); });
  svg.addEventListener("focusout", () => { state.topology.hovered = null; highlightTopology(); });
  const selectPeer = (id) => { state.topology.selected = state.topology.selected === id ? null : id; highlightTopology(); };
  svg.addEventListener("click", (event) => {
    if (state.topology.suppressClick) { state.topology.suppressClick = false; return; }
    const id = focusPeer(event);
    if (id) selectPeer(id);
  });
  svg.addEventListener("keydown", (event) => {
    const id = focusPeer(event);
    if (id && (event.key === "Enter" || event.key === " ")) { event.preventDefault(); selectPeer(id); }
    if (event.key === "Escape") { state.topology.selected = null; state.topology.hovered = null; highlightTopology(); }
  });
  svg.addEventListener("pointerdown", (event) => {
    state.topology.suppressClick = false;
    if (event.button !== 0 || focusPeer(event)) return;
    state.topology.drag = { id: event.pointerId, x: event.clientX, y: event.clientY, camera: { ...state.topology.camera } };
    svg.setPointerCapture(event.pointerId);
    svg.classList.add("is-panning");
  });
  svg.addEventListener("pointermove", (event) => {
    const drag = state.topology.drag;
    if (!drag || drag.id !== event.pointerId) return;
    const dx = event.clientX - drag.x, dy = event.clientY - drag.y;
    if (Math.hypot(dx, dy) > 3) state.topology.suppressClick = true;
    state.topology.camera = { ...drag.camera, x: drag.camera.x + dx, y: drag.camera.y + dy };
    state.topology.autoFit = false;
    applyTopologyCamera();
  });
  const finishPan = () => { state.topology.drag = null; svg.classList.remove("is-panning"); };
  svg.addEventListener("pointerup", finishPan);
  svg.addEventListener("pointercancel", finishPan);
  svg.addEventListener("lostpointercapture", finishPan);
  svg.addEventListener("wheel", (event) => {
    if (!event.ctrlKey && !event.metaKey) return;
    event.preventDefault();
    const rect = svg.getBoundingClientRect();
    zoomTopology(Math.exp(-event.deltaY * 0.005), { x: event.clientX - rect.left, y: event.clientY - rect.top });
  }, { passive: false });
}

async function submitScenarioRun() {
  if (scenarioOperationBusy()) return;
  const error = $("#scenarioError");
  const repetitions = Number($("#runRepetitions").value);
  if (!Number.isInteger(repetitions) || repetitions < 1 || repetitions > 100) {
    error.textContent = "Enter a whole number of runs from 1 to 100.";
    $("#runRepetitions").focus();
    return;
  }
  const editorVersion = state.scenarioEditorVersion;
  state.scenarioSubmitting = true;
  state.pendingScenarioDeleteId = null;
  state.scenarioActionError = "";
  error.textContent = "";
  renderSavedScenarios();
  try {
    const run = await scenarioRequest("/api/v1/experiments", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ scenario: $("#scenarioText").value, repetitions }),
    }, "run");
    if (state.scenarioEditorVersion === editorVersion && $("#scenarioDialog").open) $("#scenarioDialog").close();
    showToast(repetitions > 1 ? `Queued ${repetitions} runs: ${run.name}.` : `Submitted experiment: ${run.name}.`);
  } catch (caught) {
    error.textContent = caught.message;
  } finally {
    state.scenarioSubmitting = false;
    renderSavedScenarios();
  }
}

function openScenarioEditor() {
  state.scenarioEditorVersion++;
  $("#scenarioDialog").showModal();
  renderSavedScenarios();
  refreshSavedScenarios();
}

function closeScenarioEditor() {
  $("#scenarioDialog").close("cancel");
}

function handleScenarioNameKeydown(event) {
  if (event.key !== "Enter" || event.isComposing) return;
  event.preventDefault();
  if (!scenarioOperationBusy()) saveEditedScenario(false);
}

function showToast(message) {
  const toast = $("#toast");
  toast.textContent = message;
  toast.classList.add("show");
  setTimeout(() => toast.classList.remove("show"), 2600);
}

$("#scenarioText").value = defaultScenario;
try { localStorage.removeItem("kpl-api-token"); } catch { /* Legacy credential cleanup. */ }
$("#logout").addEventListener("click", logout);
window.addEventListener("pageshow", event => {
  if (event.persisted) void api("/api/v1/auth/session").catch(() => {});
});
$("#refreshResults").addEventListener("click", refreshSavedResults);
$("#openScenario").addEventListener("click", openScenarioEditor);
$("#refreshScenarios").addEventListener("click", refreshSavedScenarios);
$("#newScenario").addEventListener("click", startNewScenario);
$("#saveScenario").addEventListener("click", () => saveEditedScenario(false));
$("#saveScenarioCopy").addEventListener("click", () => saveEditedScenario(true));
$("#runScenario").addEventListener("click", submitScenarioRun);
$("#validateScenario").addEventListener("click", validateEditedScenario);
$("#scenarioText").addEventListener("input", resetScenarioValidation);
$("#scenarioName").addEventListener("keydown", handleScenarioNameKeydown);
$("#scenarioForm").addEventListener("submit", (event) => event.preventDefault());
for (const close of document.querySelectorAll("[data-scenario-close]")) close.addEventListener("click", closeScenarioEditor);

document.addEventListener("click", async (event) => {
  const retryButton = event.target.closest("[data-retry-batch]");
  if (retryButton) {
    event.preventDefault();
    if (!retryButton.disabled) await resumeSavedBatch(retryButton.dataset.retryBatch, true);
    return;
  }
  const resumeButton = event.target.closest("[data-resume-batch]");
  if (resumeButton) {
    event.preventDefault();
    if (!resumeButton.disabled) await resumeSavedBatch(resumeButton.dataset.resumeBatch);
    return;
  }
  const deleteBatchButton = event.target.closest("[data-delete-batch]");
  if (deleteBatchButton) {
    event.preventDefault();
    if (!deleteBatchButton.disabled) {
      rememberResultDialogFocus("#deleteResultDialog", deleteBatchButton);
      requestResultDeletion(deleteBatchButton.dataset.deleteBatch, true);
    }
    return;
  }
  const batchImagesButton = event.target.closest("[data-batch-images]");
  if (batchImagesButton) {
    event.preventDefault(); // Analyze without toggling the surrounding summary.
    if (!batchImagesButton.disabled) {
      rememberResultDialogFocus("#resultImagesDialog", batchImagesButton);
      globalThis.KPLResultImages?.openBatch(batchImagesButton.dataset.batchImages);
    }
    return;
  }
  const loadScenarioButton = event.target.closest("[data-load-scenario]");
  if (loadScenarioButton) {
    if (!loadScenarioButton.disabled) loadSavedScenario(loadScenarioButton.dataset.loadScenario);
    return;
  }
  const deleteScenarioButton = event.target.closest("[data-delete-scenario]");
  if (deleteScenarioButton) {
    if (!deleteScenarioButton.disabled) requestScenarioDeletion(deleteScenarioButton.dataset.deleteScenario);
    return;
  }
  const cancelScenarioDeleteButton = event.target.closest("[data-cancel-scenario-delete]");
  if (cancelScenarioDeleteButton) {
    if (!cancelScenarioDeleteButton.disabled) cancelScenarioDeletion(cancelScenarioDeleteButton.dataset.cancelScenarioDelete);
    return;
  }
  const confirmScenarioDeleteButton = event.target.closest("[data-confirm-scenario-delete]");
  if (confirmScenarioDeleteButton) {
    if (!confirmScenarioDeleteButton.disabled) confirmScenarioDeletion(confirmScenarioDeleteButton.dataset.confirmScenarioDelete);
    return;
  }
  const imagesButton = event.target.closest("[data-result-images]");
  if (imagesButton) {
    if (!imagesButton.disabled) {
      rememberResultDialogFocus("#resultImagesDialog", imagesButton);
      void globalThis.KPLResultImages?.open(imagesButton.dataset.resultImages);
    }
    return;
  }
  const deleteButton = event.target.closest("[data-delete-result]");
  if (deleteButton) {
    if (!deleteButton.disabled) {
      rememberResultDialogFocus("#deleteResultDialog", deleteButton);
      requestResultDeletion(deleteButton.dataset.deleteResult);
    }
    return;
  }
  const button = event.target.closest("[data-stop-run]");
  if (!button || button.disabled) return;
  await requestRunStop(button.dataset.stopRun);
});

$("#confirmDeleteResult").addEventListener("click", confirmResultDeletion);
$("#scenarioDialog").addEventListener("close", () => {
  if (!state.scenarioDeletingId) state.pendingScenarioDeleteId = null;
});
$("#deleteResultDialog").addEventListener("cancel", (event) => {
  if (state.deletingResultId) event.preventDefault();
});
$("#deleteResultDialog").addEventListener("close", () => {
  if (!state.deletingResultId) {
    state.pendingDelete = null;
  }
});

globalThis.KPLResultImages?.init({ api, onJob: job => {
  const run = (state.savedResults || []).find(run => run.id === job.runId);
  if (job.batchId) {
    for (const member of state.savedResults || []) if (member.batchId === job.batchId) member.batchAnalysis = job;
    renderSavedResults();
  } else if (run) { run.analysis = job; renderSavedResults(); }
  if (!state.resultsRefreshTimer && !state.resultsLoading) state.resultsRefreshTimer = setTimeout(refreshSavedResults, 3000);
} });
setupTopologyControls();
setupDashboardPanelHandling();
setupDetailPanelSizing();
renderSavedScenarios();
window.addEventListener("resize", () => {
  syncDetailPanelHeight();
  if (state.snapshot) renderTopology(state.snapshot.nodes || [], state.snapshot.edges || []);
});
refreshSavedResults();
setupStreamLifecycle();
connectStream();

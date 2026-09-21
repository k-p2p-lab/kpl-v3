/* Deterministic, DOM-independent sector geometry and bounded local force layout. */
const TOPOLOGY_NODE_PADDING = 28;
const TOPOLOGY_COLLISION_DISTANCE = 34;
const TOPOLOGY_HASH_CELL = 76;

function topologyHash(value) {
  let hash = 2166136261;
  for (const character of String(value)) hash = Math.imul(hash ^ character.charCodeAt(0), 16777619);
  return (hash >>> 0) / 4294967296;
}

function topologySectorLimits(group, radius) {
  const full = group.endAngle - group.startAngle >= Math.PI * 2 - 1e-8;
  const half = (group.endAngle - group.startAngle) / 2;
  const minimum = Math.max(group.innerRadius + TOPOLOGY_NODE_PADDING,
    full ? 0 : TOPOLOGY_NODE_PADDING / Math.max(0.001, Math.sin(Math.min(half, Math.PI / 2))) + 1);
  const maximum = Math.max(minimum + 1, group.outerRadius - TOPOLOGY_NODE_PADDING);
  const r = Math.max(minimum, Math.min(maximum, radius));
  return { full, minimum, maximum, radius: r, half: full ? Math.PI : Math.max(0, half - Math.asin(Math.min(1, TOPOLOGY_NODE_PADDING / r))) };
}

function constrainTopologyPosition(point, group) {
  let dx = point.x - group.cx, dy = point.y - group.cy;
  if (!Number.isFinite(dx) || !Number.isFinite(dy)) { dx = 0; dy = 0; }
  const radius = Math.hypot(dx, dy);
  const limits = topologySectorLimits(group, radius);
  const center = (group.startAngle + group.endAngle) / 2;
  const angle = Math.atan2(dy, dx);
  const relative = Math.atan2(Math.sin(angle - center), Math.cos(angle - center));
  const bounded = limits.full ? relative : Math.max(-limits.half, Math.min(limits.half, relative));
  if (Math.abs(radius - limits.radius) < 1e-9 && (limits.full || Math.abs(relative) <= limits.half + 1e-12) && Number.isFinite(point.x) && Number.isFinite(point.y)) return false;
  point.x = group.cx + Math.cos(center + bounded) * limits.radius;
  point.y = group.cy + Math.sin(center + bounded) * limits.radius;
  point.vx = Number.isFinite(point.vx) ? point.vx * 0.2 : 0;
  point.vy = Number.isFinite(point.vy) ? point.vy * 0.2 : 0;
  return true;
}

function seedTopologyPosition(group, slot, id) {
  const seed = topologyHash(id);
  const limits = topologySectorLimits(group, group.innerRadius);
  const fraction = (slot * 0.754877666 + seed * 0.17) % 1;
  const radius = Math.sqrt(limits.minimum ** 2 + fraction * (limits.maximum ** 2 - limits.minimum ** 2));
  const angleLimits = topologySectorLimits(group, radius);
  const angularFraction = (slot * 0.618033989 + seed * 0.13) % 1;
  const angle = (group.startAngle + group.endAngle) / 2 + (angularFraction * 2 - 1) * angleLimits.half * 0.94;
  return { x: group.cx + Math.cos(angle) * radius, y: group.cy + Math.sin(angle) * radius };
}

function layoutTopology(nodes, agents, memory, width) {
  // width affects the camera in the caller; geometry never changes on resize.
  const live = new Map(nodes.filter((node) => !["stopping", "stopped", "failed"].includes(node.state)).map((node) => [node.id, node]));
  const agentIDs = [...new Set(agents.map((agent) => agent.id))];
  for (const id of [...new Set([...live.values()].map((node) => node.agentId))].sort()) if (!agentIDs.includes(id)) agentIDs.push(id);
  const engine = memory.pizza || (memory.pizza = { positions: new Map(), slots: new Map(), radius: 0, alpha: 1, tick: 0, signature: "", edgeInput: null });
  const byAgent = new Map(agentIDs.map((id) => [id, []]));
  for (const node of live.values()) byAgent.get(node.agentId).push(node);
  let maximumOccupancy = 0;
  for (const groupNodes of byAgent.values()) maximumOccupancy = Math.max(maximumOccupancy, groupNodes.length);
  const count = agentIDs.length;
  const angularMinimum = count > 1 ? TOPOLOGY_NODE_PADDING / Math.sin(Math.PI / count) + 80 : 0;
  const neededRadius = Math.max(200, 62 + Math.sqrt(maximumOccupancy * Math.max(1, count)) * 22, angularMinimum);
  const oldCenter = engine.radius ? engine.radius + 60 : 0;
  // Reserve only observed occupancy. Keep the high-water radius during churn so
  // the whole drawing does not breathe as individual Peers join and leave.
  engine.radius = live.size ? Math.max(engine.radius, neededRadius) : neededRadius;
  const center = engine.radius + 60;
  const delta = oldCenter ? center - oldCenter : 0;
  const signature = JSON.stringify([agentIDs, [...live.values()].map((node) => [node.id, node.agentId]).sort((a, b) => String(a[0]).localeCompare(String(b[0]))), engine.radius]);
  const changed = signature !== engine.signature;
  const groups = agentIDs.map((id, index) => {
    const startAngle = -Math.PI / 2 + index * Math.PI * 2 / count;
    const endAngle = -Math.PI / 2 + (index + 1) * Math.PI * 2 / count;
    const middle = (startAngle + endAngle) / 2;
    return { id, index, cx: center, cy: center, startAngle, endAngle, innerRadius: 50, outerRadius: engine.radius,
      labelX: center + Math.cos(middle) * (engine.radius + 32), labelY: center + Math.sin(middle) * (engine.radius + 32),
      count: byAgent.get(id).length, nodePadding: TOPOLOGY_NODE_PADDING };
  });
  const groupByID = new Map(groups.map((group) => [group.id, group]));
  for (const [id, point] of engine.positions) {
    if (!live.has(id)) engine.positions.delete(id);
    else if (delta) { point.x += delta; point.y += delta; point.ax += delta; point.ay += delta; }
  }
  for (const [agentID, slots] of engine.slots) {
    for (const id of slots.keys()) if (live.get(id)?.agentId !== agentID) slots.delete(id);
  }
  for (const group of groups) {
    const slots = engine.slots.get(group.id) || new Map();
    engine.slots.set(group.id, slots);
    const used = new Set(slots.values());
    let freeSlot = 1;
    for (const node of byAgent.get(group.id).sort((a, b) => String(a.id).localeCompare(String(b.id)))) {
      if (!slots.has(node.id)) {
        while (used.has(freeSlot)) freeSlot++;
        slots.set(node.id, freeSlot); used.add(freeSlot);
      }
      const slot = slots.get(node.id);
      let point = engine.positions.get(node.id);
      if (!point || point.agentId !== group.id) {
        const initial = seedTopologyPosition(group, slot, node.id);
        point = { ...initial, ax: initial.x, ay: initial.y, vx: 0, vy: 0, slot, agentId: group.id };
        engine.positions.set(node.id, point);
      } else {
        point.slot = slot;
        if (constrainTopologyPosition(point, group)) { point.ax = point.x; point.ay = point.y; }
      }
    }
  }
  const layout = { positions: engine.positions, groups, bounds: { width: center * 2, height: center * 2 }, _engine: engine, _groupByID: groupByID, stats: { pairChecks: 0, candidateChecks: 0 } };
  engine.signature = signature;
  if (changed) { engine.edgeInput = null; reheatTopologyLayout(layout, 0.8); }
  return layout;
}

function reheatTopologyLayout(layout, alpha = 0.45) {
  const engine = layout._engine;
  engine.alpha = Math.max(engine.alpha || 0, Math.max(0.01, Math.min(1, alpha)));
  engine.tick = 0;
  engine.quietTicks = 0;
}

function topologyLayoutLinks(layout, edges) {
  const engine = layout._engine;
  if (engine.edgeInput === edges) return engine.links;
  const unique = new Map();
  for (const edge of edges) {
    if (edge.source === edge.target || !layout.positions.has(edge.source) || !layout.positions.has(edge.target)) continue;
    const key = JSON.stringify([edge.source, edge.target].sort());
    const weight = edge.protocol === "gossipsub" ? 1 : edge.protocol === "kademlia" ? 0.55 : 0.3;
    if (!unique.has(key) || weight > unique.get(key).weight) unique.set(key, { source: edge.source, target: edge.target, weight });
  }
  engine.links = [...unique.values()];
  engine.degrees = new Map();
  for (const edge of engine.links) for (const id of [edge.source, edge.target]) engine.degrees.set(id, (engine.degrees.get(id) || 0) + 1);
  engine.edgeInput = edges;
  return engine.links;
}

function stepTopologyLayout(layout, edges = [], { pinnedID } = {}) {
  const engine = layout._engine;
  const pinned = layout.positions.get(pinnedID);
  if (pinned) { pinned.vx = 0; pinned.vy = 0; constrainTopologyPosition(pinned, layout._groupByID.get(pinned.agentId)); }
  if (!engine || !layout.positions.size || engine.alpha <= 0) return false;
  const points = [...layout.positions.entries()];
  const alpha = engine.alpha;
  const links = topologyLayoutLinks(layout, edges);
  for (const [, point] of points) {
    if (!Number.isFinite(point.vx)) point.vx = 0;
    if (!Number.isFinite(point.vy)) point.vy = 0;
    point.vx += (point.ax - point.x) * 0.0015 * alpha;
    point.vy += (point.ay - point.y) * 0.0015 * alpha;
  }
  for (const edge of links) {
    const a = layout.positions.get(edge.source), b = layout.positions.get(edge.target);
    const dx = b.x - a.x, dy = b.y - a.y, distance = Math.max(1, Math.hypot(dx, dy));
    const target = a.agentId === b.agentId ? 68 : Math.max(100, engine.radius * 0.75);
    const force = Math.max(-2, Math.min(4, (distance - target) * 0.016 * alpha * edge.weight));
    const fa = force / Math.sqrt(engine.degrees.get(edge.source) || 1), fb = force / Math.sqrt(engine.degrees.get(edge.target) || 1);
    if (edge.source !== pinnedID) { a.vx += dx / distance * fa; a.vy += dy / distance * fa; }
    if (edge.target !== pinnedID) { b.vx -= dx / distance * fb; b.vy -= dy / distance * fb; }
  }
  const grid = new Map();
  for (let i = 0; i < points.length; i++) {
    const point = points[i][1], key = `${Math.floor(point.x / TOPOLOGY_HASH_CELL)},${Math.floor(point.y / TOPOLOGY_HASH_CELL)}`;
    if (!grid.has(key)) grid.set(key, []);
    grid.get(key).push(i);
  }
  let pairChecks = 0, candidateChecks = 0;
  for (let i = 0; i < points.length; i++) {
    const [id, a] = points[i], cellX = Math.floor(a.x / TOPOLOGY_HASH_CELL), cellY = Math.floor(a.y / TOPOLOGY_HASH_CELL);
    let budget = 96;
    for (let ox = -1; ox <= 1 && budget > 0; ox++) for (let oy = -1; oy <= 1 && budget > 0; oy++) {
      const bucket = grid.get(`${cellX + ox},${cellY + oy}`) || [];
      const start = bucket.length ? (i * 37) % bucket.length : 0;
      for (let k = 0; k < bucket.length && budget > 0; k++, budget--) {
        candidateChecks++;
        const j = bucket[(start + k) % bucket.length];
        if (j <= i) continue;
        pairChecks++;
        const [otherID, b] = points[j];
        let dx = b.x - a.x, dy = b.y - a.y, distance = Math.hypot(dx, dy);
        if (distance > TOPOLOGY_HASH_CELL) continue;
        if (distance < 0.001) {
          const angle = topologyHash(`${id}\0${otherID}`) * Math.PI * 2;
          dx = Math.cos(angle); dy = Math.sin(angle); distance = 1;
        }
        const overlap = Math.max(0, TOPOLOGY_COLLISION_DISTANCE - distance);
        const force = overlap * 0.25 + (1 - distance / TOPOLOGY_HASH_CELL) * 0.3 * alpha;
        const fx = dx / distance * force, fy = dy / distance * force;
        if (id !== pinnedID) { a.vx -= fx; a.vy -= fy; }
        if (otherID !== pinnedID) { b.vx += fx; b.vy += fy; }
      }
    }
  }
  let maximumSpeed = 0;
  for (const [id, point] of points) {
    if (id === pinnedID) { point.vx = 0; point.vy = 0; }
    else {
      point.vx *= 0.72; point.vy *= 0.72;
      const speed = Math.hypot(point.vx, point.vy);
      if (speed > 9) { point.vx *= 9 / speed; point.vy *= 9 / speed; }
      point.x += point.vx; point.y += point.vy;
    }
    constrainTopologyPosition(point, layout._groupByID.get(point.agentId));
    maximumSpeed = Math.max(maximumSpeed, Math.hypot(point.vx, point.vy));
  }
  layout.stats = { pairChecks, candidateChecks };
  engine.alpha *= 0.965;
  engine.tick++;
  engine.quietTicks = engine.alpha < 0.006 && maximumSpeed < 0.035 ? (engine.quietTicks || 0) + 1 : 0;
  if (engine.quietTicks >= 6 || engine.tick >= 600) {
    engine.alpha = 0;
    for (const [, point] of points) { point.vx = 0; point.vy = 0; }
    return false;
  }
  return true;
}

function topologySectorPath(group) {
  const { cx, cy, innerRadius: inner, outerRadius: outer, startAngle: start, endAngle: end } = group;
  if (end - start >= Math.PI * 2 - 1e-8) {
    const outside = `M ${cx + outer} ${cy} A ${outer} ${outer} 0 1 1 ${cx - outer} ${cy} A ${outer} ${outer} 0 1 1 ${cx + outer} ${cy}`;
    return inner ? `${outside} M ${cx + inner} ${cy} A ${inner} ${inner} 0 1 0 ${cx - inner} ${cy} A ${inner} ${inner} 0 1 0 ${cx + inner} ${cy} Z` : `${outside} Z`;
  }
  const point = (radius, angle) => `${cx + Math.cos(angle) * radius} ${cy + Math.sin(angle) * radius}`;
  const large = end - start > Math.PI ? 1 : 0;
  return `M ${point(outer, start)} A ${outer} ${outer} 0 ${large} 1 ${point(outer, end)} L ${point(inner, end)} A ${inner} ${inner} 0 ${large} 0 ${point(inner, start)} Z`;
}

if (typeof module !== "undefined" && module.exports) {
  module.exports = { layoutTopology, stepTopologyLayout, reheatTopologyLayout, topologySectorPath, constrainTopologyPosition, TOPOLOGY_NODE_PADDING };
}

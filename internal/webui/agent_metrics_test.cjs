// Run with: node --test internal/webui/agent_metrics_test.cjs
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, 'static/app.js'), 'utf8');
const markup = fs.readFileSync(path.join(__dirname, 'static/index.html'), 'utf8');
const functions = source.slice(source.indexOf('function escapeHTML('), source.indexOf('$("#scenarioText").value = defaultScenario;'));
const sandbox = { URL };
vm.createContext(sandbox);
vm.runInContext(functions, sandbox);

test('Agent table reserves a Metrics column and keeps the empty row aligned', () => {
  assert.match(markup, /<th>Last seen<\/th><th>Metrics<\/th>/);
  assert.match(markup, /id="agentRows"><tr><td colspan="9"/);
  assert.match(source, /colspan="9" class="empty-cell">No Agents registered\./);
});

test('Agent metrics links accept only absolute credential-free HTTP metrics URLs', () => {
  assert.equal(sandbox.safeAgentMetricsURL('http://10.20.0.8:18090/metrics'), 'http://10.20.0.8:18090/metrics');
  assert.equal(sandbox.safeAgentMetricsURL('https://[2001:db8::7]:18443/metrics'), 'https://[2001:db8::7]:18443/metrics');
  for (const value of [
    '', undefined, '/metrics', 'javascript:alert(1)', 'file:///metrics',
    'http://user:secret@example.test/metrics', 'http://example.test/status',
    'http://example.test/%6detrics', 'http://example.test/metrics?token=secret',
    'http://example.test/metrics#section', 'http://example.test/metrics#', 'http://[invalid/metrics',
    ' http://example.test/metrics', 'http://example.test:0/metrics',
    'http://localhost:9091/metrics', 'http://127.0.0.1:9091/metrics',
    'http://0.0.0.0:9091/metrics', 'http://[::1]:9091/metrics', 'http://[::]:9091/metrics',
  ]) {
    assert.equal(sandbox.safeAgentMetricsURL(value), '', `accepted unsafe metrics URL: ${value}`);
  }
});

test('Agent metrics links escape labels and open safely in a new tab', () => {
  const link = sandbox.agentMetricsLink({
    id: 'agent-a',
    name: '<img src=x onerror=alert(1)>',
    metricsUrl: 'https://metrics.example.test:18443/metrics',
  });
  assert.match(link, /href="https:\/\/metrics\.example\.test:18443\/metrics"/);
  assert.match(link, /target="_blank" rel="noopener noreferrer"/);
  assert.match(link, /&lt;img src=x onerror=alert\(1\)&gt;/);
  assert.doesNotMatch(link, /<img/);
  assert.match(link, /opens in a new tab/);
  assert.equal(sandbox.agentMetricsLink({ id: 'agent-a' }), '');
  assert.equal(sandbox.agentMetricsLink({ id: 'agent-a', metricsUrl: 'javascript:alert(1)' }), '');
});

test('available slots count only online Agents and clamp occupied capacity at zero', () => {
  const elements = new Map();
  const context = {};
  vm.createContext(context);
  vm.runInContext(functions, context);
  context.$ = selector => {
    if (!elements.has(selector)) elements.set(selector, { textContent: '' });
    return elements.get(selector);
  };
  for (const name of ['rememberAgents', 'renderRuns', 'renderAgents', 'renderEvents', 'renderTopology']) context[name] = () => {};
  context.filterTopologyEdges = () => [];
  const agents = [
    { id: 'available', state: 'online', capacity: 10, activeNodes: 4 },
    { id: 'full', state: 'online', capacity: 5, activeNodes: 8 },
    { id: 'offline', state: 'offline', capacity: 100, activeNodes: 1 },
  ];
  context.render({ generatedAt: '2026-09-12T00:00:00Z', agents });
  assert.equal(elements.get('#agentMetric').textContent, '2 / 3');
  assert.equal(elements.get('#capacityMetric').textContent, 'Available slots: 6');
  agents[0].state = 'offline';
  agents[1].state = 'offline';
  context.render({ generatedAt: '2026-09-12T00:00:01Z', agents });
  assert.equal(elements.get('#capacityMetric').textContent, 'Available slots: 0');
});

const test = require('node:test');
const assert = require('node:assert/strict');
const batch = require('./static/batch-analysis.js');
const images = require('./static/result-images.js');
const files = require('./static/research-files.js');
function run(id, start, latency, samples = 1) {
  return { version: 1, result: { id, batchId: 'batch', state: 'completed', startedAt: start, name: 'Repeated', repetitions: 2 }, asOf: start, metrics: { definition: 'session-window-v1', latencySamples: samples, averageLatencyMs: latency, p95LatencyMs: latency }, latencyCDF: [{ x: latency, y: 1 }], latencyHistogram: [{ x: latency, y: samples }], binSeconds: 1, timeline: [], observations: [], research: { messages: [], degreeDistribution: [], overview: { messageSeries: {}, originCounts: { eager: 0, lazy: 0, unknown: 0 }, receiversTime: [], receiversHop: [] } } };
}
function data(runs) { return { version: 1, batchId: 'batch', aggregation: 'equal-run-mean-v1', expectedRuns: runs.length, missingRuns: 0, excluded: [], summary: {}, runs }; }
const at = (start, seconds) => new Date(Date.parse(start) + seconds * 1000).toISOString();
const approx = (actual, expected) => assert.ok(Math.abs(actual - expected) < 1e-8, `${actual} != ${expected}`);

test('batch CDF gives runs equal weight independent of latency sample count', () => {
  const a = run('a', '2026-09-10T00:00:00Z', 10, 1), b = run('b', '2026-09-10T01:00:00Z', 30, 99);
  const charts = batch.build(data([a, b]), images.buildCharts);
  const cdf = charts.find(c => c.id === 'latency-cdf').series[0].points;
  const middle = cdf.find(p => p.x === 10);
  assert.equal(middle.y, 50); assert.equal(middle.n, 2); approx(middle.error, Math.sqrt(5000));
  assert.equal(cdf.at(-1).y, 100);
  const histogram = charts.find(c => c.id === 'latency-distribution').series[0].points;
  approx(histogram.reduce((sum, p) => sum + p.y, 0), 50);
  assert.equal(new Set(charts.map(c => c.id)).size, charts.length);
  assert.ok(charts.length > 50);
  assert.match(files.chartCSV(charts[0]), /"n"/);
});

test('run-start alignment interpolates observed time segments without extrapolating shorter runs', () => {
  const a = run('a', '2026-09-10T00:00:00Z', 10), b = run('b', '2026-09-10T03:00:00Z', 20);
  const observation = (start, second, score) => ({ at: at(start, second), groups: [{ group: '', layers: [], scoreMean: score }] });
  a.observations = [observation(a.result.startedAt, 5, 2), observation(a.result.startedAt, 15, 4)];
  b.observations = [observation(b.result.startedAt, 10, 6), observation(b.result.startedAt, 20, 8)];
  const values = batch.build(data([a, b]), images.buildCharts).find(c => c.id === 'peer-scores').series[0].points;
  assert.deepEqual(values.map(p => p.x), [5, 10, 15, 20]);
  assert.equal(values.find(p => p.x === 10).y, 4.5);
  assert.equal(values.find(p => p.x === 20).y, 8);
  assert.equal(values.find(p => p.x === 20).n, 1);
  assert.equal(values.find(p => p.x === 20).error, null);
});

test('batch bandwidth averages measured rates, retains gaps, and excludes absent collection', () => {
  const a = run('a', '2026-09-10T00:00:00Z', 10), b = run('b', '2026-09-10T02:00:00Z', 20), c = run('c', '2026-09-10T05:00:00Z', 30);
  for (const [r, bytes, protocol] of [[a, 1000, 'gossip'], [b, 3000, 'kad']]) {
    r.bandwidthBinSeconds = 1;
    r.bandwidthTimeline = [{ at: r.result.startedAt, sentBytes: bytes, receivedBytes: bytes * 2, protocols: [{ protocol, sentBytes: bytes, receivedBytes: bytes * 2 }] }];
  }
  const charts = batch.build(data([a, b, c]), images.buildCharts);
  const rate = charts.find(c => c.id === 'p2p-throughput').series[0].points[0];
  assert.equal(rate.y, 16); assert.equal(rate.n, 2);
  const protocol = charts.find(c => c.id === 'bandwidth-protocol-send-rate').series.find(s => s.name === 'gossip').points[0];
  assert.equal(protocol.y, 4); assert.equal(protocol.n, 2, 'an absent protocol within measured traffic is zero, not missing collection');
  const gap = batch.average([[{ x: 0, y: 2 }, { x: 1, y: null }, { x: 3, y: 6 }], [{ x: 2, y: 10 }]], 'step').find(p => p.x === 2);
  assert.equal(gap.y, 10); assert.equal(gap.n, 1);
});

test('compact batch inputs preserve message time curves and origin estimates', () => {
  const a = run('a', '2026-09-10T00:00:00Z', 10), b = run('b', '2026-09-10T01:00:00Z', 20);
  a.research.overview = { messageSeries: { frt: [{ x: 5, y: 1 }] }, originCounts: { eager: 2, lazy: 0, unknown: 1 }, receiversTime: [{ x: 1, y: 2 }], receiversHop: [{ x: 1, y: 2 }] };
  b.research.overview = { messageSeries: { frt: [{ x: 5, y: 3 }] }, originCounts: { eager: 4, lazy: 2, unknown: 1 }, receiversTime: [{ x: 2, y: 4 }], receiversHop: [{ x: 1, y: 4 }] };
  const charts = batch.build(data([a, b]), images.buildCharts);
  assert.equal(charts.find(c => c.id === 'messages-frt').series[0].points[0].y, 2);
  assert.equal(charts.find(c => c.id === 'origin-estimates').series[0].points[0].y, 3);
  const receivers = charts.find(c => c.id === 'mean-receivers-time').series[0].points;
  assert.deepEqual(receivers.map(p => p.y), [1, 3]);
  const increments = charts.find(c => c.id === 'mean-receivers-time-increments').series[0].points;
  assert.deepEqual(increments.map(p => p.y), [1, 2]);
});

test('histogram rebinning conserves source count mass for different bin widths', () => {
  const sources = [
    { latencyHistogram: [{ x: 1, y: 2 }, { x: 3, y: 4 }] },
    { latencyHistogram: [{ x: 2, y: 3 }, { x: 6, y: 7 }] },
    { latencyHistogram: [{ x: 10, y: 11 }] },
    { latencyHistogram: [] },
  ];
  const bins = batch.commonHistograms(sources);
  bins.forEach((points, index) => approx(points.reduce((s, p) => s + p.y, 0), sources[index].latencyHistogram.reduce((s, p) => s + p.y, 0)));
  assert.deepEqual(bins[0].map(p => p.x), bins[1].map(p => p.x));
});

test('discrete averaging retains probability mass beyond the line preview limit', () => {
  const curve = Array.from({ length: 1000 }, (_, x) => ({ x, y: .001 }));
  const averaged = batch.average([curve, curve], 'discrete');
  assert.equal(averaged.length, 1000);
  approx(averaged.reduce((s, p) => s + p.y, 0), 1);
});

test('batch validation rejects unrelated, duplicate, partial, and single-run inputs', () => {
  const a = run('a', '2026-09-10T00:00:00Z', 10), b = run('b', '2026-09-10T01:00:00Z', 20);
  assert.throws(() => batch.validate(data([a]), 'batch'));
  assert.throws(() => batch.validate(data([a, a]), 'batch'));
  assert.throws(() => batch.validate(data([a, { ...b, result: { ...b.result, batchId: 'other' } }]), 'batch'));
  assert.throws(() => batch.validate(data([a, { ...b, result: { ...b.result, state: 'running' } }]), 'batch'));
});

test('summary CSV preserves missing values, run counts and escaped metric names', () => {
  const csv = batch.summaryCSV({ 'metrics.averageLatencyMs': { average: 20, deviation: null, count: 1 }, 'research.name"test': { average: null, deviation: null, count: 0 } });
  const rows = files.csvRows(csv);
  assert.deepEqual(rows[0], ['metric', 'label', 'mean', 'sample_sd', 'n']);
  assert.deepEqual(rows[1].slice(2), ['20', '', '1']);
  assert.equal(rows[2][0], 'research.name"test');
});

test('batch graph variants preserve protocol groups and average only the same layer', () => {
  const a = run('a', '2026-09-13T00:00:00Z', 10);
  const b = run('b', '2026-09-13T01:00:00Z', 20);
  const c = run('c', '2026-09-13T02:00:00Z', 30);
  const observation = (source, degrees) => ({
    at: source.result.startedAt,
    groups: [{ group: '', layers: Object.entries(degrees).map(([protocol, averageDegree]) => ({ protocol, nodes: averageDegree * 10, averageDegree })) }],
  });
  a.observations = [observation(a, { gossipsub: 2, kademlia: 20, transport: 100 })];
  b.observations = [observation(b, { gossipsub: 6, kademlia: 40, transport: 300 })];
  c.observations = [observation(c, { gossipsub: 10 })];
  const charts = batch.build(data([a, b, c]), images.buildCharts);
  const nodes = charts.find(chart => chart.id === 'graph-node_count');
  assert.equal(nodes.category, 'common');
  for (const [index, expected] of [60, 300, 2000].entries()) approx(nodes.series[index].points[0].y, expected);
  assert.deepEqual(nodes.series.map(series => series.points[0].n), [3, 2, 2]);
  assert.equal(batch.label('research.node_count'), 'GossipSub Node Count');
  for (const [protocol, label, mean, count, error] of [
    ['gossipsub', 'GossipSub', 6, 3, 4],
    ['kademlia', 'Kademlia', 30, 2, Math.sqrt(200)],
    ['transport', 'Transport', 200, 2, Math.sqrt(20000)],
  ]) {
    const chart = charts.find(chart => chart.id === 'graph-average_degree-' + protocol);
    assert.equal(chart.protocol, protocol);
    assert.equal(chart.groupId, 'graph-average_degree');
    assert.equal(chart.groupTitle, 'Mean Degree · Run Mean');
    assert.equal(chart.title, 'Mean Degree · ' + label + ' · Run Mean');
    assert.equal(chart.series.length, 1);
    assert.equal(chart.series[0].name, label + ' · All Peers');
    const point = chart.series[0].points[0];
    assert.equal(point.x, 0);
    approx(point.y, mean);
    assert.equal(point.n, count, 'unobserved layers must not contribute zero-valued runs');
    approx(point.error, error);
    const exported = files.csvRows(files.chartCSV(chart));
    assert.equal(exported[1][exported[0].indexOf('series')], label + ' · All Peers');
  }
  assert.equal(new Set(charts.map(chart => chart.id)).size, charts.length);
});

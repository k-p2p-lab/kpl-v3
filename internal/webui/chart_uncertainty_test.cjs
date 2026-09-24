const test = require('node:test');
const assert = require('node:assert/strict');
const { chartSVG } = require('./static/result-images.js');
const { chartCSV } = require('./static/research-files.js');

const chart = (series, extra = {}) => ({ title: 'Group variability', xLabel: 'Time', yLabel: 'Mean', series, ...extra });
const shades = svg => svg.match(/<svg class="chart-uncertainty"[^>]*>([\s\S]*?)<\/svg>/)?.[1] || '';
const data = svg => svg.match(/<g class="chart-data">([\s\S]*?)<\/g>/)?.[1] || '';

test('all translucent ranges precede every mean line and preserve numeric exports', () => {
  const input = chart(['All Peers', 'Group: A', 'Group: B'].map((name, group) => ({
    name, points: [{ x: 0, y: group + 1, error: 4 }, { x: 1, y: group + 2, error: 5 }],
  })));
  const before = JSON.stringify(input), csv = chartCSV(input), svg = chartSVG(input);
  assert.equal((shades(svg).match(/opacity="0.14" stroke="none"/g) || []).length, 3);
  assert.equal((shades(svg).match(/<path /g) || []).length, 3);
  assert.equal((data(svg).match(/stroke-width="2"/g) || []).length, 3);
  assert.ok(svg.indexOf('class="chart-uncertainty"') < svg.indexOf('class="chart-data"'));
  assert.doesNotMatch(data(svg), /opacity=|h8/);
  for (const color of ['#2563eb', '#c45c10', '#7c3aed']) {
    assert.ok(shades(svg).includes(`fill="${color}"`));
    assert.ok(data(svg).includes(`stroke="${color}"`));
  }
  assert.equal(JSON.stringify(input), before);
  assert.equal(chartCSV(input), csv);
});

test('line uncertainty stops at missing errors or missing means', () => {
  const input = chart([{ name: 'Gaps', points: [
    { x: 0, y: 2, error: 1 }, { x: 1, y: 3, error: 1 },
    { x: 2, y: 4, error: null },
    { x: 3, y: 3, error: 1 }, { x: 4, y: 2, error: 1 },
    { x: 5, y: null, error: 1 },
    { x: 6, y: 2, error: 1 }, { x: 7, y: 3, error: 1 },
  ] }]);
  const svg = chartSVG(input);
  assert.equal((shades(svg).match(/<path /g) || []).length, 3);
  assert.equal((data(svg).match(/<path d="([^"]+)"/)[1].match(/M/g) || []).length, 2, 'missing error must not interrupt the valid mean');
});

test('step shading holds the same intervals on upper and lower boundaries', () => {
  const svg = chartSVG(chart([{ name: 'Steps', points: [{ x: 0, y: 2, error: .5 }, { x: 1, y: 4, error: .5 }] }], { mode: 'step' }));
  const boundary = shades(svg).match(/d="M([\d.]+) ([\d.]+)H([\d.]+)V([\d.]+)L([\d.]+) ([\d.]+)V([\d.]+)H([\d.]+)Z"/);
  assert.ok(boundary, 'lower edge must reverse the step corner order');
  const [x1, top1, x2, top2, x3, bottom2, bottom1, x4] = boundary.slice(1).map(Number);
  assert.equal(x1, x4); assert.equal(x2, x3);
  assert.ok(top1 < bottom1 && top2 < bottom2);
  assert.match(data(svg), /d="M[\d.]+ [\d.]+H[\d.]+V[\d.]+"/);
});

test('isolated, bar and scatter ranges stay shaded; log and clipped panels remain finite', () => {
  for (const mode of ['line', 'bar', 'scatter']) {
    const svg = chartSVG(chart([{ name: mode, points: [{ x: 1, y: .5, error: 2, xError: .2 }] }], { mode, yMax: 1, logX: true }));
    assert.match(shades(svg), /<rect /);
    assert.match(svg, /class="chart-uncertainty"[^>]*overflow="hidden"/);
    assert.doesNotMatch(svg, /NaN|Infinity/);
    assert.doesNotMatch(data(svg), /h8|opacity=/);
  }
  const svg = chartSVG(chart([{ name: 'Unknown', points: [{ x: 1, y: 1, error: null }, { x: 2, y: 2, error: null }] }]));
  assert.equal(shades(svg), '');
  const panel = chart([{ name: 'Panel', points: [{ x: 1, y: 2, error: 1 }] }]);
  const panels = chartSVG({ title: 'Panels', panels: [panel, panel], series: [] });
  assert.equal((panels.match(/class="chart-uncertainty"/g) || []).length, 2);
  assert.doesNotMatch(panels, /clip-path|NaN|Infinity/);
});

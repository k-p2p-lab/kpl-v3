// Run with: node --test internal/webui/topology_test.cjs
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, 'static/app.js'), 'utf8');
const layoutSource = fs.readFileSync(path.join(__dirname, 'static/topology-layout.js'), 'utf8');
const functions = source.slice(source.indexOf('function escapeHTML('), source.indexOf('$("#scenarioText").value = defaultScenario;'));
function context(extra = {}) {
  const sandbox = { Intl, Date, ...extra };
  vm.createContext(sandbox);
  vm.runInContext(layoutSource, sandbox);
  vm.runInContext(functions, sandbox);
  return sandbox;
}
const plain = (value) => JSON.parse(JSON.stringify(value));
const peer = (id, agentId = 'a', state = 'ready') => ({ id, agentId, state });
test('delivery display distinguishes conditional reach, missing observations and pending', () => {
  const api = context();
  const metrics = {definition:'session-window-v1',deliveryRatioAvailable:true,reachability:0.7,deliveryRatioUpperBound:0.9,unknownDeliveries:2,expectedDeliveries:7,eligibleDeliveries:5,pendingPublications:3,finalizedPublications:8,initialDeliveryRatioAvailable:true,initialDeliveryRatio:0.6,initialDeliveryRatioUpperBound:0.8,initialUnknownDeliveries:2,initialExpectedDeliveries:10,stableCoverageAvailable:true,stableCoverage:0.7,stableCoverageUpperBound:0.8,departedPairs:2,continuityUnknownPairs:1,publicationAvailabilityUnknownPairs:3,availabilityUnknownPairs:4};
  let view = api.deliveryMetricView(metrics);
  assert.equal(view.primary, '70%–90%');
  assert.equal(view.initial, '60%–80%');
  assert.equal(view.coverage, '70%–80%');
  assert.match(view.progress, /Pending: 3/);
  assert.match(view.note, /conditional on the known starting cohort/);
  assert.match(view.observation, /2 receipt unknown · 1 continuity unknown · 3 start unknown/);
  view = api.deliveryMetricView({...metrics, deliveryRatioAvailable:false});
  assert.equal(view.primary, 'N/A');
  view = api.deliveryMetricView({...metrics, initialDeliveryRatioAvailable:false, stableCoverageAvailable:false});
  assert.equal(view.initial, 'N/A');
  assert.equal(view.coverage, 'N/A');
});

test('initial markup and empty snapshots use the same current measurement cards', () => {
  const markup = fs.readFileSync(path.join(__dirname, 'static/index.html'), 'utf8');
  const api = context();
  const empty = api.deliveryMetricView({});
  const fields = {
    reachLabel: 'label', reachMetric: 'primary', deliveryMetric: 'primaryDetail',
    measurementProgress: 'progress', initialReachMetric: 'initial', initialDeliveryMetric: 'initialDetail',
    coverageMetric: 'coverage', coverageDetail: 'coverageDetail', observationMetric: 'observation',
    outcomesMetric: 'outcomes', measurementNote: 'note',
  };
  for (const [id, field] of Object.entries(fields)) {
    const text = markup.match(new RegExp(`id="${id}"[^>]*>([^<]*)<`))?.[1];
    assert.equal(text, empty[field], `${id} changes meaning on the first snapshot`);
  }
  assert.equal(empty.label, 'Continuous-session delivery');
  assert.equal(empty.primary, 'N/A');
  assert.doesNotMatch(markup.match(/<div[^>]*id="metricCarousel"[^>]*>/)[0], /\bhidden\b/);
  assert.doesNotMatch(markup, /Legacy|Historical definition|dispatch pairs/);
  for (const metrics of [undefined, null, {runId:'starting'}, {definition:'dispatch-cohort-v1', published:0}]) {
    assert.deepEqual(plain(api.deliveryMetricView(metrics)), plain(empty));
  }
});

test('historical and unknown measurements are not relabeled as current delivery values', () => {
  const api = context();
  for (const definition of ['dispatch-cohort-v1', 'unknown-format', undefined]) {
    const view = api.deliveryMetricView({
      definition, deliveryRatioAvailable:true, reachability:0.6,
      eligibleDeliveries:6, expectedDeliveries:10, finalizedPublications:2,
      initialDeliveryRatioAvailable:true, initialDeliveryRatio:0.6,
      stableCoverageAvailable:true, stableCoverage:0.8,
    });
    assert.equal(view.label, 'Continuous-session delivery');
    assert.equal(view.primary, 'N/A');
    assert.equal(view.initial, 'N/A');
    assert.equal(view.coverage, 'N/A');
    assert.equal(view.primaryDetail, 'On time: 0 / 0 stable pairs');
    assert.equal(view.note, api.deliveryMetricView({}).note);
    assert.doesNotMatch(view.progress, /Awaiting/);
    assert.doesNotMatch(JSON.stringify(view), /Legacy|Historical|dispatch pairs|60%|80%/);
  }
});

test('metric rendering stays consistent from idle through a run and back to measurement waiting', () => {
  const elements = new Map();
  const api = context({
    $: (selector) => {
      if (!elements.has(selector)) elements.set(selector, {textContent:''});
      return elements.get(selector);
    },
  });
  for (const name of ['rememberAgents', 'renderRuns', 'renderAgents', 'renderEvents', 'syncDetailPanelHeight', 'renderTopology']) {
    api[name] = () => {};
  }
  const render = (metrics) => api.render({
    generatedAt:'2026-09-07T00:00:00Z', metrics,
    experiments: metrics?.runId ? [{id:metrics.runId, state:'running'}] : [],
  });
  const text = (id) => elements.get(`#${id}`).textContent;
  const assertWaiting = () => {
    assert.equal(text('reachLabel'), 'Continuous-session delivery');
    for (const id of ['reachMetric', 'latencyMetric', 'duplicateMetric', 'initialReachMetric', 'coverageMetric']) {
      assert.equal(text(id), 'N/A', id);
    }
    assert.equal(text('averageLatencyMetric'), 'No eligible latency samples');
    assert.equal(text('duplicateSamplesMetric'), 'Eligible duplicates: 0 · Delivered pairs: 0');
  };
  render();
  assertWaiting();
  const observed = {
    runId:'run-one', deliveryRatioAvailable:true, reachability:0.6,
    eligibleDeliveries:6, expectedDeliveries:10,
    latencySamples:6, p95LatencyMs:25, averageLatencyMs:20,
    duplicateSamples:6, averageDuplicates:0.5, eligibleDuplicates:3,
    published:2, delivered:6, duplicates:3,
  };
  render({...observed, definition:'dispatch-cohort-v1'});
  assertWaiting();
  assert.equal(text('eventTotalsMetric'), 'Published: 2 · Delivered: 6');
  assert.equal(text('duplicateTotalMetric'), 'All duplicate events: 3');
  render({...observed, definition:'session-window-v1'});
  assert.equal(text('reachLabel'), 'Continuous-session delivery');
  assert.equal(text('reachMetric'), '60%');
  assert.equal(text('latencyMetric'), '25 ms');
  assert.equal(text('duplicateMetric'), '0.5');
  render({runId:'run-two'});
  assertWaiting();
  assert.equal(text('eventTotalsMetric'), 'Published: 0 · Delivered: 0');
});

test('the carousel resets finished runs without changing snapshots or cluster counts', () => {
  const elements = new Map();
  const api = context({$: selector => {
    if (!elements.has(selector)) elements.set(selector, {textContent:''});
    return elements.get(selector);
  }});
  for (const name of ['rememberAgents', 'renderRuns', 'renderAgents', 'renderEvents', 'syncDetailPanelHeight', 'renderTopology']) api[name] = () => {};
  const snapshot = {
    generatedAt:'2026-09-17T00:00:00Z',
    agents:[{id:'a', state:'online', capacity:10, activeNodes:2}],
    nodes:[peer('one'), peer('two')],
  };
  const texts = () => Object.fromEntries([...elements].map(([id, element]) => [id, element.textContent]));
  api.render(snapshot);
  const initial = texts();
  const metrics = {
    runId:'run-one', definition:'session-window-v1', published:5, delivered:8, duplicates:3,
    deliveryRatioAvailable:true, reachability:0.8, deliveryRatioUpperBound:0.9,
    expectedDeliveries:10, eligibleDeliveries:8, unknownDeliveries:1,
    deliveryWindows:['10s'], finalizedPublications:4, pendingPublications:1,
    latencySamples:8, p95LatencyMs:25, averageLatencyMs:20,
    duplicateSamples:8, averageDuplicates:0.375, eligibleDuplicates:3,
    initialDeliveryRatioAvailable:true, initialDeliveryRatio:0.7, initialDeliveryRatioUpperBound:0.8,
    initialExpectedDeliveries:12, initialEligibleDeliveries:8, initialUnknownDeliveries:1,
    stableCoverageAvailable:true, stableCoverage:0.8, stableCoverageUpperBound:0.9,
    departedPairs:1, continuityUnknownPairs:2, publicationAvailabilityUnknownPairs:3,
    missedDeliveries:1, lateDeliveries:2,
    bandwidth:{sessions:2, finalizedSessions:0, sentBytes:2048, receivedBytes:1024,
      rejectedSamples:3, latestAt:snapshot.generatedAt,
      currentRates:{available:true, sentBitsPerSecond:8000, receivedBitsPerSecond:16000, reportingSessions:1, staleSessions:1}},
  };
  const queued = {id:'run-two', batchId:'batch', state:'queued', iteration:2, repetitions:2};
  const running = {id:'run-one', batchId:'batch', state:'running', iteration:1, repetitions:2};
  for (const runState of ['completed', 'failed', 'canceled', 'interrupted', 'queued']) {
    api.render({...snapshot, metrics, experiments:[queued, running]});
    assert.equal(texts()['#latencyMetric'], '25 ms');
    assert.equal(texts()['#bandwidthSendMetric'], '8 kbit/s');
    assert.match(texts()['#messageMetricsScope'], /Run 1 of 2/);
    const finished = {...snapshot, metrics, experiments:[queued, {...running, state:runState}]};
    const before = JSON.stringify(finished);
    api.render(finished);
    assert.deepEqual(texts(), initial, `${runState} must reset all run metric fields`);
    api.render(finished);
    assert.deepEqual(texts(), initial, 'later snapshots must not restore finished metrics');
    assert.equal(JSON.stringify(finished), before, 'saved metrics and experiment states must remain intact');
  }
  api.render({...snapshot, metrics, experiments:[]});
  assert.deepEqual(texts(), initial, 'unmatched final metrics must not remain after a run is removed');
  api.render({...snapshot, metrics:{...metrics, runId:'run-two', p95LatencyMs:12},
    experiments:[{...running, state:'completed'}, {...queued, state:'running'}]});
  assert.equal(texts()['#latencyMetric'], '12 ms');
  assert.match(texts()['#messageMetricsScope'], /run-two · Run 2 of 2/);
  assert.equal(texts()['#agentMetric'], '1 / 1');
  assert.equal(texts()['#peerMetric'], '2');
  assert.equal(texts()['#capacityMetric'], 'Available slots: 8');
});

test('source sizes show original bytes for terminal, live and unreadable metadata', () => {
  const api = context();
  assert.match(api.resultSourceSize({id:'saved',state:'completed',sourceBytes:1536,downloadBytes:16}), />Source · 1.5 KiB<\/span>/);
  assert.match(api.resultSourceSize({id:'live',state:'running',sourceBytes:5*1024*1024}), />Live source · 5 MiB<\/span>/);
  assert.match(api.resultSourceSize({id:'queued',state:'queued',sourceBytes:1024}), />Live source · 1 KiB<\/span>/);
  assert.match(api.resultSourceSize({id:'bad-metadata',state:'unreadable',sourceBytes:100}), />Source · 100 B<\/span>/);
  assert.match(api.resultSourceSize({id:'zero',state:'completed',sourceBytes:0}), />Source · 0 B<\/span>/);
  for (const sourceBytes of [undefined,null,-1,'1536',NaN,Infinity,1.5]) {
    assert.match(api.resultSourceSize({id:'missing',state:'completed',sourceBytes,downloadBytes:1024}), />Source · —<\/span>/);
  }
});

test('experiment cards use saved source bytes with the current run state', () => {
  const state = {savedResults:[{id:'run',state:'running',active:true,sourceBytes:1536}]};
  const api = context({state});
  assert.match(api.runSourceSize({id:'run',state:'running'}), />Live source · 1.5 KiB<\/span>/);
  assert.match(api.runSourceSize({id:'run',state:'completed'}), />Source · 1.5 KiB<\/span>/);
  assert.equal(api.runSourceSize({id:'missing',state:'completed'}), '');
});

test('result refresh displays source bytes immediately without ZIP requests or expiry timers', async () => {
  const elements = new Map(), timers = new Map();
  let nextTimer=0, heads=0, requests=0;
  const element = id => {
    if (!elements.has(id)) elements.set(id, {querySelectorAll(){return [];},contains(){return false;},classList:{toggle(){}},setAttribute(){},textContent:'',innerHTML:'',hidden:false,disabled:false});
    return elements.get(id);
  };
  const run = {id:'done',name:'Completed run',state:'completed',phase:1,totalPhases:1,seed:7};
  const state = {savedResults:null,resultsLoading:false,resultsError:'',resultsRefreshTimer:null,resultsRefreshPending:false,
    deletedResultIDs:new Set(),snapshot:{experiments:[run]},runStates:null,pendingStops:new Set(),deletingResultId:null};
  const api = context({state,$:element,document:{activeElement:null},AbortController,
    setTimeout:(fn,delay)=>{const id=++nextTimer;timers.set(id,{fn,delay});return id;},clearTimeout:id=>timers.delete(id),
    fetch:()=>{heads++;throw new Error('unexpected ZIP request');}});
  api.api = async path => {assert.equal(path,'/api/v1/results');requests++;return [{...run,sourceBytes:requests*1536}];};
  await api.refreshSavedResults();
  assert.match(element('#runList').innerHTML, />Source · 1.5 KiB<\/span>/);
  assert.match(element('#savedResultsRows').innerHTML, />Source · 1.5 KiB<\/span>/);
  await api.refreshSavedResults();
  assert.match(element('#savedResultsRows').innerHTML, />Source · 3 KiB<\/span>/);
  assert.equal(requests,2);
  assert.equal(heads,0);
  assert.equal(timers.size,0,'idle results scheduled size polling');
});

test('relationship layers do not misclassify legacy transport and preserve the input history', () => {
  const api = context();
  const nodes = [peer('one'), peer('two'), peer('stopped', 'a', 'stopped'), peer('stopping', 'a', 'stopping'), peer('failed', 'a', 'failed')];
  const edges = [
    {source:'one',target:'two'},
    {source:'one',target:'two',protocol:'unknown'},
    {source:'one',target:'two',protocol:'kademlia',reportedBy:['one']},
    {source:'one',target:'stopped',protocol:'kademlia'},
    {source:'one',target:'stopping',protocol:'kademlia'},
    {source:'one',target:'missing',protocol:'kademlia'},
    {source:'one',target:'one',protocol:'kademlia'},
  ];
  const original = JSON.stringify({nodes,edges});
  const routing = api.filterTopologyEdges(nodes, edges, {kademlia:true});
  assert.deepEqual(plain(routing), [{source:'one',target:'two',protocol:'kademlia',topics:[],reportedBy:['one'],topicReports:{}}]);
  const transport = api.filterTopologyEdges(nodes, edges, {transport:true});
  assert.equal(transport.length,1);
  assert.equal(transport[0].protocol,'transport');
  assert.deepEqual(plain(api.topologyData(nodes,edges).nodes.map(n=>n.id)),['one','two','failed']);
  assert.equal(JSON.stringify({nodes,edges}),original);
});

test('GossipSub merges endpoint pairs and topics but keeps Kademlia and reporter direction distinct', () => {
  const api = context(), nodes = [peer('one'),peer('two')];
  const edges = [
    {source:'one',target:'two',protocol:'gossipsub',topic:'z',reportedBy:['one']},
    {source:'two',target:'one',protocol:'gossipsub',topic:'a',reportedBy:['two']},
    {source:'one',target:'two',protocol:'gossipsub',topic:'z',reportedBy:['one']},
    {source:'one',target:'two',protocol:'kademlia',reportedBy:['one']},
  ];
  const both = api.filterTopologyEdges(nodes,edges,{gossipsub:true,kademlia:true});
  assert.equal(both.length,2);
  assert.deepEqual(plain(both[0].topics),['a','z']);
  assert.deepEqual(plain(both[0].reportedBy),['one','two']);
  assert.deepEqual(plain(both[0].topicReports),{z:['one'],a:['two']});
  assert.equal(api.topologyReportSummary(both[0],'one'),'a: Remote Peer reported; z: Selected Peer reported');
  assert.ok(!api.topologyReportSummary(both[0],'one').includes('Both endpoints'));
  const filtered = api.filterTopologyEdges(nodes,edges,{gossipsub:true,kademlia:true,topic:'a'});
  assert.equal(filtered.length,2);
  assert.deepEqual(plain(filtered[0].topics),['a']);
  assert.deepEqual(plain(filtered[0].reportedBy),['two']);
  assert.equal(filtered[1].protocol,'kademlia');
  assert.equal(api.filterTopologyEdges(nodes,edges,{topic:'a'}).length,0);
});

test('topic choices include empty observed meshes and exclude unrelated transport topics', () => {
  const api=context();
  assert.deepEqual(plain(api.observedTopologyTopics([{meshPeers:{empty:[],alpha:['peer']}}],[
    {protocol:'gossipsub',topic:'beta'}, {protocol:'gossipsub',topic:'alpha'}, {topic:'transport-only'},
  ])),['alpha','beta','empty']);
});

test('fit camera includes every Agent and zoom preserves the chosen world anchor', () => {
  const topology={camera:{x:30,y:40,scale:0.5},graph:{width:1000,height:600},autoFit:true};
  const elements=new Map();
  const api=context({state:{topology},$:(id)=>{if(!elements.has(id))elements.set(id,{setAttribute(){}});return elements.get(id);}});
  const bounds={width:2400,height:1800};
  const fit=api.fitTopologyCamera(bounds,1000,600);
  assert.ok(fit.x>=24 && fit.y>=24);
  assert.ok(fit.x+bounds.width*fit.scale<=976.01);
  assert.ok(fit.y+bounds.height*fit.scale<=576.01);
  const point={x:200,y:150}, world={x:(point.x-30)/0.5,y:(point.y-40)/0.5};
  api.zoomTopology(1.25,point);
  assert.equal(topology.autoFit,false);
  assert.equal((point.x-topology.camera.x)/topology.camera.scale,world.x);
  assert.equal((point.y-topology.camera.y)/topology.camera.scale,world.y);
});

test('inspector escapes node/topic text and handles missing mesh values', () => {
  const first={...peer('<node>'),peerId:'<peer>',meshPeers:{empty:null},peerScores:{a:-1.25,b:2.75},metadata:{network:'null',pubsubRouter:'gossipsub',topicMode:'subscribe',dhtMode:'client',topics:'topic/a, <topic/b>',runtime:'docker',containerId:'container<1234>'}};
  const other=peer('other','b');
  const graph={nodes:[first,other],positions:new Map([['<node>',{slot:1}],['other',{slot:2}]]),edges:[{source:'<node>',target:'other',protocol:'gossipsub',topics:['<topic>'],reportedBy:['<node>'],topicReports:{'<topic>':['<node>']}}]};
  const output={};
  const api=context({state:{topology:{graph},agentNumbers:new Map([['a',1],['b',2]])},$:()=>output});
  api.renderTopologyDetails(first);
  assert.ok(output.innerHTML.includes('&lt;node&gt;'));
  assert.ok(output.innerHTML.includes('&lt;topic&gt;'));
  assert.ok(!output.innerHTML.includes('<node>'));
  assert.ok(output.innerHTML.includes('Selected Peer reported'));
  assert.ok(output.innerHTML.includes('2 scores · Average: 0.75'));
  assert.ok(output.innerHTML.includes('gossipsub / subscribe'));
  assert.ok(output.innerHTML.includes('<dt>DHT mode</dt><dd>client</dd>'));
  assert.ok(output.innerHTML.includes('topic/a, &lt;topic/b&gt;'));
  assert.ok(output.innerHTML.includes('<dt>Runtime</dt><dd>docker</dd>'));
  assert.ok(output.innerHTML.includes('container&lt;1234&gt;'));
  first.peerScores={};first.metadata={pubsubEnabled:'false',dhtEnabled:'false'};
  api.renderTopologyDetails(first);
  assert.ok(output.innerHTML.includes('0 scores · Average: N/A'));
  first.metadata.scoreEnabled='false';
  api.renderTopologyDetails(first);
  assert.ok(output.innerHTML.includes('<dt>Peer scores</dt><dd>Disabled</dd>'));
  assert.ok(output.innerHTML.includes('<dt>PubSub router / topic mode</dt><dd>Off</dd>'));
  assert.ok(output.innerHTML.includes('<dt>DHT mode</dt><dd>Off</dd>'));
});

test('recent events separate experiment IDs and shorten matching node prefixes', () => {
  const eventList = {};
  const api = context({$: (selector) => {
    assert.equal(selector, '#eventList');
    return eventList;
  }});
  api.renderEvents([{
    timestamp: '2026-09-05T03:04:05Z',
    type: 'measurement_checkpoint_with_a_long_name',
    runId: 'run-20260905T030000Z-a1b2c3d4',
    nodeId: 'run-20260905T030000Z-a1b2c3d4-workers-00001',
    remotePeerId: '<remote-peer-id>',
    latencyMs: 12.25,
  }]);
  assert.match(eventList.innerHTML, /class="event-type"[^>]*>measurement_checkpoint_with_a_long_name<\/span>/);
  assert.match(eventList.innerHTML, /class="event-summary"[^>]*>workers-00001 · ← &lt;remote-pe · 12\.3 ms<\/span>/);
  assert.match(eventList.innerHTML, /class="event-run-id"[^>]*>Experiment · run-20260905T030000Z-a1b2c3d4<\/small>/);
  assert.equal((eventList.innerHTML.match(/run-20260905T030000Z-a1b2c3d4/g) || []).length, 2, 'run ID should appear only in the small label and its title');
  assert.ok(!eventList.innerHTML.includes('<remote-peer-id>'));
});

test('recent events preserve node IDs when no matching experiment prefix exists', () => {
  const eventList = {};
  const api = context({$: () => eventList});
  api.renderEvents([{timestamp:'2026-09-05T03:04:05Z',type:'publish',runId:'run-a',nodeId:'standalone-node'}]);
  assert.match(eventList.innerHTML, />standalone-node<\/span>/);
  api.renderEvents([{timestamp:'2026-09-05T03:04:05Z',type:'publish',nodeId:'run-a-workers-00001'}]);
  assert.match(eventList.innerHTML, />run-a-workers-00001<\/span>/);
  assert.ok(!eventList.innerHTML.includes('event-run-id'));
});

test('recent events show aggregated GossipSub control units and direction', () => {
  const eventList = {};
  const api = context({$: () => eventList});
  api.renderEvents([{
    timestamp:'2026-09-05T03:04:05Z',type:'send_ihave',runId:'run-a',nodeId:'peer-a',remotePeerId:'remote-peer-id',
    fields:{direction:'send',controlType:'ihave',controlEntries:2,messageIdCount:17},
  }, {
    timestamp:'2026-09-05T03:04:06Z',type:'drop_prune',runId:'run-a',nodeId:'peer-a',remotePeerId:'remote-peer-id',
    fields:{direction:'drop',controlType:'prune',controlEntries:1,messageIdCount:0,peerExchangeCount:3},
  }]);
  assert.match(eventList.innerHTML, /remote-pee · 2 entries · 17 message IDs/);
  assert.match(eventList.innerHTML, /⇢ remote-pee · 1 entry · 3 PX records/);
  assert.match(eventList.innerHTML, /→ remote-pee/);
});

function uiFixture({reducedMotion=false}={}) {
  const ids=new Map();
  const frames={queue:new Map(),nextID:1,maximumQueued:0};
  frames.request=(callback)=>{const id=frames.nextID++;frames.queue.set(id,callback);frames.maximumQueued=Math.max(frames.maximumQueued,frames.queue.size);return id;};
  frames.cancel=(id)=>frames.queue.delete(id);
  frames.step=(timestamp)=>{const entry=frames.queue.entries().next().value;if(!entry)return false;frames.queue.delete(entry[0]);entry[1](timestamp);return true;};
  const eventTarget=()=>{
    const events=new Map();
    return {addEventListener(name,fn){if(!events.has(name))events.set(name,[]);events.get(name).push(fn);},emit(name,extra={}){for(const fn of events.get(name)||[])fn({type:name,...extra});}};
  };
  const preference={...eventTarget(),matches:reducedMotion};
  let document;
  class Element {
    constructor(tag='div') {
      this.tag=tag;this.children=[];this.dataset={};this.attributes={};this.events={};this.clientWidth=1280;this.clientHeight=600;
      const classes=new Set();
      this.classList={add:(name)=>classes.add(name),remove:(name)=>classes.delete(name),contains:(name)=>classes.has(name),toggle:(name,on)=>on?classes.add(name):classes.delete(name)};
    }
    setAttribute(key,value) {
      this.attributes[key]=String(value);
      if(key==='id')ids.set(String(value),this);
      if(key==='class')for(const name of String(value).split(' '))this.classList.add(name);
      if(key.startsWith('data-'))this.dataset[key.slice(5).replace(/-([a-z])/g,(_,letter)=>letter.toUpperCase())]=String(value);
    }
    getAttribute(key){return this.attributes[key]??null;}
    append(...children){for(const child of children){child.parent=this;this.children.push(child);}}
    replaceChildren(...children){this.children=[];this.append(...children);}
    get options(){return this.children;}
    addEventListener(name,fn){this.events[name]=fn;}
    emit(name,extra={}){return this.events[name]?.({target:this,button:0,...extra});}
    matches(selector){return selector==='[data-node-id]'?Boolean(this.dataset.nodeId):selector.startsWith('.')&&this.classList.contains(selector.slice(1));}
    closest(selector){return this.matches(selector)?this:this.parent?.closest(selector);}
    querySelectorAll(selector){return this.children.flatMap(child=>[...(child.matches(selector)?[child]:[]),...child.querySelectorAll(selector)]);}
    setPointerCapture(){}
    getBoundingClientRect(){return {left:0,top:0};}
    focus(){document.activeElement=this;}
  }
  const html=fs.readFileSync(path.join(__dirname,'static/index.html'),'utf8');
  for(const match of html.matchAll(/<([a-z]+)[^>]*\bid="([^"]+)"[^>]*>/g)){
    const element=new Element(match[1]);element.setAttribute('id',match[2]);element.checked=/\bchecked\b/.test(match[0]);
  }
  document={...eventTarget(),activeElement:null,hidden:false,get visibilityState(){return this.hidden?'hidden':'visible';},querySelector:(selector)=>ids.get(selector.slice(1)),querySelectorAll:(selector)=>ids.get('topology').querySelectorAll(selector),createElement:(tag)=>new Element(tag),createElementNS:(_,tag)=>new Element(tag)};
  const window={...eventTarget(),matchMedia:()=>preference};
  const sandbox={document,window,Intl,Date,requestAnimationFrame:frames.request,cancelAnimationFrame:frames.cancel,localStorage:{getItem:()=>null,setItem(){}}};
  vm.createContext(sandbox);
  vm.runInContext(layoutSource,sandbox);
  vm.runInContext(source.slice(0,source.indexOf('const defaultScenario ='))+functions,sandbox);
  const state=vm.runInContext('state',sandbox);
  const render=(nodes=[peer('one'),peer('two'),peer('three','b')],edges=[
    {source:'one',target:'two',protocol:'gossipsub',topic:'alpha'},
    {source:'two',target:'three',protocol:'kademlia'},
  ])=>{state.snapshot={nodes,edges,agents:[{id:'a'},{id:'b'}]};sandbox.renderTopology(nodes,edges);};
  return {ids,document,window,sandbox,state,frames,render,
    setHidden(value){document.hidden=value;document.emit('visibilitychange');},
    setReduced(value){preference.matches=value;preference.emit('change');},
  };
}

test('rendered graph controls preserve selection and camera while filtering and streaming churn', () => {
  const {ids,document,sandbox,state,frames}=uiFixture();
  const nodes=[{...peer('one'),peerScores:{a:1,b:3}},peer('two'),peer('three','b'),peer('stopped','a','stopped')];
  const edges=[
    {source:'one',target:'two',protocol:'kademlia',reportedBy:['one']},
    {source:'one',target:'two',protocol:'gossipsub',topic:'alpha',reportedBy:['one']},
    {source:'two',target:'three',protocol:'gossipsub',topic:'beta',reportedBy:['two']},
    {source:'one',target:'three'},
    {source:'one',target:'stopped',protocol:'gossipsub',topic:'alpha'},
  ];
  state.snapshot={nodes,edges,agents:[{id:'a',capacity:10000},{id:'b',capacity:10000}]};
  sandbox.setupTopologyControls();
  for (const protocol of ['transport','kademlia','gossipsub']) {
    assert.equal(ids.get(`show${protocol}`).checked,false);
    assert.equal(state.topology.filters[protocol],false);
  }
  for (const protocol of ['kademlia','gossipsub']) {
    ids.get(`show${protocol}`).checked=true;
    ids.get(`show${protocol}`).emit('change');
  }
  sandbox.renderTopology(nodes,edges);
  const svg=ids.get('topology');
  assert.equal(ids.get('topologyEmpty').hidden,true);
  assert.equal(svg.querySelectorAll('.topology-peer').length,3);
  assert.equal(svg.querySelectorAll('.topology-edge').length,3);
  assert.deepEqual(svg.querySelectorAll('.topology-label').map(label=>label.textContent),[1,2]);
  const first=svg.querySelectorAll('.topology-peer').find(node=>node.dataset.nodeId==='one');
  svg.emit('pointerover',{target:first});
  assert.equal(svg.querySelectorAll('.topology-edge').filter(edge=>edge.classList.contains('is-active')).length,2);
  assert.ok(ids.get('topologyDetails').innerHTML.includes('2 scores · Average: 2.00'));
  svg.emit('click',{target:first});
  svg.emit('pointerleave');
  assert.equal(state.topology.selected,'one');
  first.focus();
  ids.get('showgossipsub').checked=false;ids.get('showgossipsub').emit('change');
  assert.equal(svg.querySelectorAll('.topology-edge').length,1);
  assert.equal(document.activeElement.dataset.nodeId,'one');
  svg.emit('pointerdown',{pointerId:1,clientX:10,clientY:10});
  svg.emit('pointermove',{pointerId:1,clientX:40,clientY:60});
  svg.emit('pointerup');
  const camera=plain(state.topology.camera);
  sandbox.renderTopology([...nodes].reverse(),edges);
  frames.step(1000);
  assert.deepEqual(plain(state.topology.camera),camera);
  assert.equal(state.topology.selected,'one');
  assert.equal(state.topology.filters.gossipsub,false);
  assert.equal(document.activeElement.dataset.nodeId,'one');
  let prevented=false;
  svg.emit('wheel',{deltaY:20,ctrlKey:false,preventDefault(){prevented=true;}});
  assert.equal(prevented,false);
  svg.emit('wheel',{deltaY:20,ctrlKey:true,clientX:100,clientY:100,preventDefault(){prevented=true;}});
  assert.equal(prevented,true);
  assert.ok(state.topology.camera.scale<camera.scale);
  ids.get('topologyFit').emit('click');assert.equal(state.topology.autoFit,true);
  sandbox.renderTopology(nodes.map(node=>({...node,state:'stopped'})),edges);
  assert.equal(state.topology.selected,null);
  assert.equal(ids.get('topologyEmpty').hidden,false);
  assert.equal(svg.querySelectorAll('.topology-peer').length,0);
  assert.equal(frames.queue.size,0);
});

test('animation updates node coordinates and edge paths with at most one queued frame', () => {
  const {ids,sandbox,state,frames,render}=uiFixture();
  sandbox.setupTopologyControls();
  for (const protocol of ['kademlia','gossipsub']) {
    ids.get(`show${protocol}`).checked=true;
    ids.get(`show${protocol}`).emit('change');
  }
 render();
  const svg=ids.get('topology');
  const positions=()=>svg.querySelectorAll('.topology-peer').map(element=>element.getAttribute('transform'));
  const paths=()=>svg.querySelectorAll('.topology-edge').map(element=>element.getAttribute('d'));
  const before=positions(),beforePaths=paths(),camera=plain(state.topology.camera);
  assert.equal(frames.queue.size,1);
  sandbox.startTopologyMotion();sandbox.startTopologyMotion();
  assert.equal(frames.queue.size,1);
  frames.step(1000);
  assert.notDeepEqual(positions(),before);
  assert.notDeepEqual(paths(),beforePaths);
  assert.deepEqual(plain(state.topology.camera),camera);
  for(let i=1;i<=5;i++)frames.step(1000+i*40);
  assert.equal(frames.maximumQueued,1);
  assert.equal(frames.queue.size,1);
});

test('pause and resume cancel and restart motion without losing the graph or camera', () => {
  const {ids,sandbox,state,frames,render}=uiFixture();
  sandbox.setupTopologyControls();
  for (const protocol of ['kademlia','gossipsub']) {
    ids.get(`show${protocol}`).checked=true;
    ids.get(`show${protocol}`).emit('change');
  }
 render();frames.step(1000);
  const graph=state.topology.graph,camera=plain(state.topology.camera);
  const points=()=>plain([...graph.positions].map(([id,p])=>[id,p.x,p.y]));
  ids.get('topologyMotion').emit('click');
  const paused=points();
  assert.equal(state.topology.motion.enabled,false);
  assert.equal(ids.get('topologyMotion').textContent,'Resume motion');
  assert.equal(ids.get('topologyMotion').getAttribute('aria-pressed'),'true');
  assert.equal(frames.queue.size,0);
  assert.equal(frames.step(1040),false);
  sandbox.startTopologyMotion();assert.equal(frames.queue.size,0);
  assert.deepEqual(points(),paused);
  ids.get('topologyMotion').emit('click');
  assert.equal(ids.get('topologyMotion').textContent,'Pause motion');
  assert.equal(frames.queue.size,1);frames.step(1080);
  assert.notDeepEqual(points(),paused);
  assert.equal(state.topology.graph,graph);
  assert.deepEqual(plain(state.topology.camera),camera);
  assert.equal(frames.maximumQueued,1);
});

test('hidden tabs and page navigation stop queued work and visible tabs resume once', () => {
  const {sandbox,state,frames,render,setHidden,window}=uiFixture();
  sandbox.setupTopologyControls();render();frames.step(1000);
  setHidden(true);
  assert.equal(frames.queue.size,0);
  const snapshot=state.snapshot;
  render(snapshot.nodes,snapshot.edges);
  sandbox.startTopologyMotion();assert.equal(frames.queue.size,0);
  setHidden(false);setHidden(false);
  assert.equal(frames.queue.size,1);frames.step(2000);
  window.emit('pagehide');
  assert.equal(frames.queue.size,0);
  assert.equal(frames.maximumQueued,1);
});

test('stopped peers leave the force graph and no frames survive an empty snapshot', () => {
  const {ids,sandbox,state,frames,render,setHidden}=uiFixture();
  sandbox.setupTopologyControls();render();frames.step(1000);
  state.topology.selected='one';
  render(state.snapshot.nodes.map(node=>({...node,state:'stopped'})),state.snapshot.edges);
  assert.equal(state.topology.selected,null);
  assert.equal(state.topology.graph.positions.size,0);
  assert.equal(ids.get('topology').querySelectorAll('.topology-peer').length,0);
  assert.equal(ids.get('topology').querySelectorAll('.topology-edge').length,0);
  assert.equal(frames.queue.size,0);
  setHidden(true);setHidden(false);sandbox.startTopologyMotion();
  assert.equal(frames.queue.size,0);
  assert.equal(frames.maximumQueued,1);
});

test('identical streamed snapshots retain cooling and never reset settled engine alpha', () => {
  const {sandbox,state,frames,render}=uiFixture();
  sandbox.setupTopologyControls();render();frames.step(1000);
  const engine=state.topology.layout.pizza,alpha=engine.alpha,tick=engine.tick;
  const points=plain([...state.topology.graph.positions].map(([id,p])=>[id,p.x,p.y]));
  render([...state.snapshot.nodes].reverse(),[...state.snapshot.edges].reverse());
  assert.equal(state.topology.layout.pizza,engine);
  assert.equal(engine.alpha,alpha);assert.equal(engine.tick,tick);
  assert.deepEqual(plain([...state.topology.graph.positions].map(([id,p])=>[id,p.x,p.y])),points);
  frames.step(1040);assert.ok(engine.alpha<alpha);
  engine.alpha=0;engine.tick=600;
  render(state.snapshot.nodes.map(node=>({...node,lastSeen:'2026-09-05T00:00:00Z'})),state.snapshot.edges);
  assert.equal(engine.alpha,0);assert.equal(engine.tick,600);
  frames.step(1080);assert.equal(frames.queue.size,0);
  assert.equal(frames.maximumQueued,1);
});

test('reduced-motion preference defaults to still layout and preserves explicit user choice', () => {
  const {ids,sandbox,state,frames,render,setReduced}=uiFixture({reducedMotion:true});
  sandbox.setupTopologyControls();render();
  assert.equal(state.topology.motion.enabled,false);
  assert.equal(frames.queue.size,0);
  assert.equal(ids.get('topologyMotion').textContent,'Resume motion');
  setReduced(false);assert.equal(frames.queue.size,1);
  setReduced(true);assert.equal(frames.queue.size,0);
  ids.get('topologyMotion').emit('click');
  assert.equal(state.topology.motion.overridden,true);
  assert.equal(frames.queue.size,1);
  setReduced(false);setReduced(true);
  assert.equal(state.topology.motion.enabled,true);
  assert.equal(frames.queue.size,1);
  ids.get('topologyMotion').emit('click');setReduced(false);
  assert.equal(state.topology.motion.enabled,false);
  assert.equal(frames.queue.size,0);
});

test('measurement help stays fixed as windows mature and observation gaps change', () => {
  const api = context();
  const note = api.deliveryMetricView({}).note;
  for (const metrics of [
    {definition:'session-window-v1',pendingPublications:10},
    {definition:'session-window-v1',finalizedPublications:10,measurementIncomplete:true,publicationAvailabilityUnknownPairs:5},
    {definition:'session-window-v1',legacyPublications:4,unscopedPublications:2},
  ]) assert.equal(api.deliveryMetricView(metrics).note, note);
  const markup = fs.readFileSync(path.join(__dirname, 'static/index.html'), 'utf8');
  const help = markup.match(/<details[^>]*id="measurementHelp"[^>]*>[\s\S]*?<\/details>/)?.[0];
  assert.ok(help);
  assert.doesNotMatch(help.match(/<details[^>]*>/)[0], /\bopen\b/);
  assert.match(help, /<summary>How delivery is measured<\/summary>/);
});

test('identical live content does not replace text or markup', () => {
  let text = '', textWrites = 0, htmlWrites = 0;
  const element = {
    get textContent() { return text; },
    set textContent(value) { text = value; textWrites++; },
    set innerHTML(value) { htmlWrites++; },
  };
  const api = context({$:()=>element});
  api.setText(element, 12); api.setText(element, '12');
  assert.equal(textWrites, 1);
  api.setText(element, 13); assert.equal(textWrites, 2);
  api.setConnection('live', 'Live'); api.setConnection('live', 'Live');
  assert.equal(htmlWrites, 1);
  api.setConnection('offline', 'Reconnecting'); assert.equal(htmlWrites, 2);
  const event = {type:'publish', timestamp:'2026-09-07T00:00:00Z', nodeId:'one'};
  api.renderEvents([event]); api.renderEvents([{...event}]);
  assert.equal(htmlWrites, 3);
  api.renderEvents([{...event, type:'deliver'}]); assert.equal(htmlWrites, 4);
});

test('snapshot bursts render the latest state once per update interval', () => {
  const state = {snapshot:null, snapshotRenderTimer:null};
  const pending = [], rendered = [];
  const api = context({state, setTimeout:(fn, delay)=>{pending.push({fn,delay});return pending.length;}});
  api.render = snapshot => rendered.push(snapshot);
  for (let i=0; i<100; i++) api.scheduleSnapshotRender({sequence:i});
  assert.equal(state.snapshot.sequence,99);
  assert.equal(pending.length,1);
  assert.equal(pending[0].delay,250);
  pending[0].fn();
  assert.deepEqual(rendered.map(snapshot=>snapshot.sequence),[99]);
  api.scheduleSnapshotRender({sequence:100});
  assert.equal(pending.length,2);
  pending[1].fn();
  assert.deepEqual(rendered.map(snapshot=>snapshot.sequence),[99,100]);
});

test('telemetry updates preserve SVG identity and focus while refreshing scores and state', () => {
  const {ids,document,sandbox,state,render} = uiFixture();
  sandbox.setupTopologyControls();
  for (const protocol of ['kademlia','gossipsub']) {
    ids.get(`show${protocol}`).checked=true;
    ids.get(`show${protocol}`).emit('change');
  }
  render();
  const svg = ids.get('topology'), world = svg.children[0];
  const first = svg.querySelectorAll('.topology-peer').find(node=>node.dataset.nodeId==='one');
  first.focus(); state.topology.selected='one';
  const nodes = state.snapshot.nodes.map(node=>({...node, peerScores:{remote:8}, overlayObservedAt:'2026-09-07T00:00:00Z'}));
  const edges = state.snapshot.edges;
  render([...nodes].reverse(), [...edges].reverse());
  assert.equal(svg.children[0],world);
  assert.equal(document.activeElement,first);
  assert.ok(svg.querySelectorAll('.topology-peer').includes(first));
  assert.match(ids.get('topologyDetails').innerHTML, /Average: 8.00/);
  render(nodes.map(node=>({...node,state:node.id==='one'?'starting':node.state})),edges);
  assert.notEqual(svg.children[0],world);
  assert.equal(document.activeElement.dataset.nodeId,'one');
  assert.match(document.activeElement.getAttribute('aria-label'),/starting/);
  render(nodes, edges.map(edge=>({...edge,reportedBy:[edge.source]})));
  assert.match(svg.querySelectorAll('.topology-edge')[0].children[0].textContent,/Reported by:/);
  assert.match(ids.get('topologyDetails').innerHTML,/Selected Peer reported/);
});


test('fresh and reconnected snapshots hide completed exits and distinguish starting from issues', () => {
  const nodes=[peer('ended','a','stopped'),peer('leaving','a','stopping'),peer('joining','a','starting'),peer('failed','a','failed'),peer('healthy')];
  const check=({ids,render})=>{
    render(nodes,[]);
    const svg=ids.get('topology');
    assert.deepEqual(svg.querySelectorAll('.topology-peer').map(el=>el.dataset.nodeId).sort(),['failed','healthy','joining']);
    const nodeCircle=id=>svg.querySelectorAll('.topology-peer').find(el=>el.dataset.nodeId===id).querySelectorAll('.topology-node')[0];
    assert.ok(nodeCircle('joining').classList.contains('starting'));
    assert.equal(nodeCircle('joining').classList.contains('issue'),false);
    assert.ok(nodeCircle('failed').classList.contains('issue'));
    assert.ok(nodeCircle('healthy').classList.contains('worker'));
  };
  // A new page has never received exit events or previous snapshots.
  check(uiFixture());
  const reconnect=uiFixture();
  reconnect.render(nodes.map(node=>({...node,state:'starting'})),[]);
  check(reconnect);
});


test('collapsed topology stops rendering and motion, then reopens the latest snapshot without overriding pause', () => {
  const {ids,document,sandbox,state,frames,render}=uiFixture();
  sandbox.setupTopologyControls();
  sandbox.setupDashboardPanelHandling();
  render();
  state.topology.selected='one';
  state.topology.filters.transport=true;
  const svg=ids.get('topology'), graph=state.topology.graph;
  const body=ids.get('panel-topology-body');
  const world=svg.children[0];
  assert.equal(frames.queue.size,1);
  body.hidden=true;
  document.emit('dashboard:panel-toggle',{detail:{id:'topology',collapsed:true}});
  assert.equal(frames.queue.size,0);
  assert.equal(state.topology.motion.enabled,true);
  const nodes=[peer('one'),peer('two'),peer('new','b'),peer('latest','b')];
  render(nodes,[]);
  assert.equal(state.topology.graph,graph,'hidden panel recalculated its layout');
  assert.equal(svg.children[0],world,'hidden panel replaced its SVG');
  assert.match(ids.get('topologyLinkCount').textContent,/4 Peers/);
  document.emit('visibilitychange');
  sandbox.startTopologyMotion();
  sandbox.animateTopology(1000);
  assert.equal(frames.queue.size,0);
  body.hidden=false;
  svg.clientWidth=720;
  document.emit('dashboard:panel-toggle',{detail:{id:'topology',collapsed:false}});
  assert.equal(state.topology.graph.nodes.length,4);
  assert.equal(state.topology.graph.width,720);
  assert.equal(state.topology.selected,'one');
  assert.equal(state.topology.filters.transport,true);
  assert.equal(frames.queue.size,1);
  ids.get('topologyMotion').emit('click');
  body.hidden=true;
  document.emit('dashboard:panel-toggle',{detail:{id:'topology',collapsed:true}});
  body.hidden=false;
  document.emit('dashboard:panel-toggle',{detail:{id:'topology',collapsed:false}});
  assert.equal(state.topology.motion.enabled,false);
  assert.equal(frames.queue.size,0);
});

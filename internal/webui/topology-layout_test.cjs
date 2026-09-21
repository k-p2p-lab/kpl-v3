const test = require('node:test');
const assert = require('node:assert/strict');
const { layoutTopology, stepTopologyLayout, reheatTopologyLayout, topologySectorPath, TOPOLOGY_NODE_PADDING } = require('./static/topology-layout.js');

function fixture(counts) {
  const agents=counts.map((_,i)=>({id:`agent-${i+1}`,capacity:10000}));
  const nodes=counts.flatMap((count,i)=>Array.from({length:count},(_,j)=>({id:`n-${i}-${String(j).padStart(4,'0')}`,agentId:agents[i].id,state:'ready'})));
  return {agents,nodes};
}
function assertContained(layout) {
  const groups=new Map(layout.groups.map(group=>[group.id,group]));
  for(const [id,point] of layout.positions) {
    const group=groups.get(point.agentId);
    for(const value of [point.x,point.y,point.vx,point.vy]) assert.ok(Number.isFinite(value),`${id} has a non-finite coordinate`);
    const dx=point.x-group.cx,dy=point.y-group.cy,radius=Math.hypot(dx,dy);
    assert.ok(radius>=group.innerRadius+TOPOLOGY_NODE_PADDING-1e-7,`${id} crossed the inner boundary`);
    assert.ok(radius<=group.outerRadius-TOPOLOGY_NODE_PADDING+1e-7,`${id} crossed the outer boundary`);
    if(group.endAngle-group.startAngle<Math.PI*2-1e-8) {
      const angle=Math.atan2(dy,dx),center=(group.startAngle+group.endAngle)/2;
      const relative=Math.atan2(Math.sin(angle-center),Math.cos(angle-center));
      const half=(group.endAngle-group.startAngle)/2;
      assert.ok(Math.abs(relative)<=half+1e-7,`${id} escaped its Agent sector`);
      assert.ok(radius*Math.sin(half-Math.abs(relative))>=TOPOLOGY_NODE_PADDING-1e-7,`${id} violated radial padding`);
    }
    assert.ok(point.x>=0 && point.x<=layout.bounds.width);
    assert.ok(point.y>=0 && point.y<=layout.bounds.height);
  }
}
function settle(layout,edges=[]) {
  let pending=true,ticks=0;
  while(pending && ticks<650) {pending=stepTopologyLayout(layout,edges);ticks++;}
  assert.equal(pending,false,'layout did not converge within its bounded settling period');
  return ticks;
}

for(const counts of [[1],[28],[18,18],[4,4,4,4,4,4,4],[100,1,2,3,1,1,1],[72,72,72,72,72,72,72],[500]]) {
  test(`finite, padded sector confinement through settling: ${counts.join('/')}`,()=>{
    const {nodes,agents}=fixture(counts),layout=layoutTopology(nodes,agents,{},1280);
    assert.equal(layout.positions.size,nodes.length);
    assert.equal(layout.groups.length,agents.length);
    for(let i=0;i<layout.groups.length;i++) {
      const group=layout.groups[i];
      assert.equal(group.id,agents[i].id);
      assert.ok(Math.abs(group.endAngle-group.startAngle-Math.PI*2/agents.length)<1e-10);
      assert.ok(group.labelX>=0 && group.labelX<=layout.bounds.width);
      assert.ok(group.labelY>=0 && group.labelY+14<=layout.bounds.height);
      assert.ok(!/NaN|Infinity/.test(topologySectorPath(group)));
    }
    assertContained(layout);
    const edges=nodes.slice(1).map((node,i)=>({source:nodes[i].id,target:node.id,protocol:'gossipsub'}));
    for(let i=0;i<40;i++) {stepTopologyLayout(layout,edges);assertContained(layout);}
    settle(layout,edges);
    assertContained(layout);
    assert.ok(layout.stats.candidateChecks<=nodes.length*96);
  });
}

test('occupancy sets radius independently of capacity, and resizing does not reseed positions',()=>{
  const {nodes,agents}=fixture([1,1,1,0,0,0,0]),memory={};
  const first=layoutTopology(nodes,agents,memory,1280);
  const initial=[...first.positions].map(([id,p])=>[id,{...p}]);
  const second=layoutTopology([...nodes].reverse(),agents.map(agent=>({...agent,capacity:1})),memory,360);
  assert.deepEqual([...second.positions],initial);
  assert.ok(second.bounds.width<600,'configured capacity produced an oversized circle');
});

test('surviving positions and velocities persist; departure is immediate and slot numbers are reused',()=>{
  const {nodes,agents}=fixture([3,2]),memory={};
  let layout=layoutTopology(nodes,agents,memory,1000);
  stepTopologyLayout(layout,[]);
  const keep=nodes[1].id,removed=nodes[0].id,point=layout.positions.get(keep),before={...point},slot=layout.positions.get(removed).slot;
  const updated=[...nodes.slice(1),{id:'new-peer',agentId:agents[0].id,state:'ready'},{...nodes[0],state:'stopping'}];
  layout=layoutTopology(updated,agents,memory,1000);
  assert.equal(layout.positions.has(removed),false);
  assert.equal(layout.positions.get(keep),point);
  assert.deepEqual(layout.positions.get(keep),before);
  assert.equal(layout.positions.get('new-peer').slot,slot);
  assertContained(layout);
});

test('failed peers release layout positions and slots without disturbing surviving peers',()=>{
  const {nodes,agents}=fixture([3,2]),memory={};
  const before=layoutTopology(nodes,agents,memory,1000);
  const removed=nodes[0].id,keep=nodes[1].id,position=before.positions.get(keep),slot=before.positions.get(removed).slot;
  let layout=layoutTopology(nodes.map(node=>({...node,state:node.id===removed?'failed':node.state})),agents,memory,1000);
  assert.equal(layout.positions.has(removed),false);
  assert.equal(memory.pizza.slots.get(agents[0].id).has(removed),false);
  assert.equal(layout.positions.get(keep),position);
  layout=layoutTopology([...nodes.slice(1),{id:'replacement',agentId:agents[0].id,state:'starting'}],agents,memory,1000);
  assert.equal(layout.positions.get('replacement').slot,slot);
});

test('identical SSE snapshots stay settled and real changes or explicit reheating restart motion',()=>{
  const {nodes,agents}=fixture([8,8,8]),memory={};
  let layout=layoutTopology(nodes,agents,memory,1000);
  settle(layout);
  const positions=[...layout.positions].map(([id,p])=>[id,{...p}]);
  layout=layoutTopology([...nodes].reverse(),agents,memory,1000);
  assert.equal(stepTopologyLayout(layout,[]),false);
  assert.deepEqual([...layout.positions],positions);
  reheatTopologyLayout(layout);
  assert.equal(stepTopologyLayout(layout,[]),true);
  settle(layout);
  layout=layoutTopology([...nodes,{id:'joined',agentId:agents[0].id,state:'ready'}],agents,memory,1000);
  assert.equal(stepTopologyLayout(layout,[]),true);
});

test('spatial interactions are bounded for 500 coincident nodes and a pinned node remains fixed',()=>{
  const {nodes,agents}=fixture([500]),layout=layoutTopology(nodes,agents,{},1000),group=layout.groups[0];
  for(const point of layout.positions.values()){point.x=group.cx+120;point.y=group.cy;}
  const pinnedID=nodes[0].id,pinned={...layout.positions.get(pinnedID)};
  for(let i=0;i<3;i++) {
    stepTopologyLayout(layout,[],{pinnedID});
    assert.ok(layout.stats.candidateChecks<=500*96);
    assertContained(layout);
  }
  assert.equal(layout.positions.get(pinnedID).x,pinned.x);
  assert.equal(layout.positions.get(pinnedID).y,pinned.y);
  assert.equal(layout.positions.get(pinnedID).vx,0);
});

test('connection springs change geometry and duplicate protocol reports do not multiply attraction',()=>{
  const {nodes,agents}=fixture([20]);
  const a=layoutTopology(nodes,agents,{},1000),b=layoutTopology(nodes,agents,{},1000),c=layoutTopology(nodes,agents,{},1000);
  let pair,distance=0;
  for(const one of nodes)for(const two of nodes){const p=a.positions.get(one.id),q=a.positions.get(two.id),d=Math.hypot(p.x-q.x,p.y-q.y);if(d>distance){distance=d;pair={source:one.id,target:two.id,protocol:'gossipsub'};}}
  const duplicate=[pair,{...pair,source:pair.target,target:pair.source},{...pair,protocol:'kademlia'}];
  for(let i=0;i<80;i++){stepTopologyLayout(a,[pair]);stepTopologyLayout(b,[]);stepTopologyLayout(c,duplicate);}
  const length=layout=>{const p=layout.positions.get(pair.source),q=layout.positions.get(pair.target);return Math.hypot(p.x-q.x,p.y-q.y);};
  assert.ok(length(a)<length(b)-10,'connection springs did not attract related nodes');
  assert.deepEqual([...a.positions],[...c.positions]);
});

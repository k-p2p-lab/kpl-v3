const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function dashboard(hash = '#services') {
  const markup = fs.readFileSync(path.join(__dirname, 'static/index.html'), 'utf8');
  const ids = [...markup.matchAll(/data-dashboard-tab="([^"]+)"/g)].map(match => match[1]);
  const listeners = {}, events = [], focused = [], panels = new Map();
  const document = {activeElement:null, querySelectorAll:()=>tabs,
    getElementById:id=>panels.get(id), dispatchEvent:event=>events.push(event)};
  const tabs = ids.map(id => {
    panels.set(id, {id, hidden:true, contains:element=>element?.panel === id});
    return {dataset:{dashboardTab:id}, attrs:{'aria-controls':id}, listeners:{},
      getAttribute(key){return this.attrs[key]}, setAttribute(key,value){this.attrs[key]=value},
      addEventListener(type,fn){this.listeners[type]=fn}, scrollIntoView(){},
      focus(){document.activeElement=this;focused.push(id)}};
  });
  const window = {location:{hash}, history:{pushState(_state,_title,next){window.location.hash=next}},
    addEventListener(type,fn){listeners[type]=fn}};
  const sandbox = {document,window,CustomEvent:class {constructor(type,options){this.type=type;this.detail=options.detail}}};
  vm.runInNewContext(fs.readFileSync(path.join(__dirname,'static/dashboard-tabs.js'),'utf8'),sandbox);
  const selected = () => tabs.filter(tab=>tab.attrs['aria-selected']==='true').map(tab=>tab.dataset.dashboardTab);
  const key = (id,value) => tabs.find(tab=>tab.dataset.dashboardTab===id).listeners.keydown({key:value,preventDefault(){}});
  return {ids,tabs,panels,window,document,listeners,events,focused,selected,key,api:sandbox.KPLDashboardTabs};
}

test('all six dashboard tabs support direct links and exclusive panel visibility',()=>{
  const ui=dashboard();
  assert.deepEqual(ui.ids,['network','experiments','agents','services','events','results']);
  assert.deepEqual(ui.selected(),['services']);
  assert.equal(ui.panels.get('services').hidden,false);
  assert.ok([...ui.panels.values()].filter(panel=>panel.id!=='services').every(panel=>panel.hidden));
  ui.tabs.find(tab=>tab.dataset.dashboardTab==='agents').listeners.click();
  assert.equal(ui.window.location.hash,'#agents');assert.deepEqual(ui.selected(),['agents']);
  ui.window.location.hash='#services';ui.listeners.popstate();
  assert.deepEqual(ui.selected(),['services']);
  ui.window.location.hash='#unknown';ui.listeners.hashchange();
  assert.deepEqual(ui.selected(),['network']);
});

test('keyboard navigation reaches the new service tab and wraps across all six tabs',()=>{
  const ui=dashboard('#agents');
  ui.key('agents','ArrowRight');assert.deepEqual(ui.selected(),['services']);
  assert.equal(ui.window.location.hash,'#services');assert.equal(ui.focused.at(-1),'services');
  ui.key('services','End');assert.deepEqual(ui.selected(),['results']);
  ui.key('results','ArrowRight');assert.deepEqual(ui.selected(),['network']);
  ui.key('network','ArrowLeft');assert.deepEqual(ui.selected(),['results']);
  ui.key('results','Home');assert.deepEqual(ui.selected(),['network']);
  ui.document.activeElement={panel:'network'};ui.api.show('services');
  assert.deepEqual(ui.selected(),['services']);assert.equal(ui.focused.at(-1),'services');
  assert.deepEqual({...ui.events.at(-1).detail},{id:'services',previous:'network'});
});

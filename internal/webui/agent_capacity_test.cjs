const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname,'static/app.js'),'utf8');
function fixture() {
 const elements=new Map(),requests=[],toasts=[];
 const element=selector=>{
  if(!elements.has(selector))elements.set(selector,{textContent:'',innerHTML:'',value:'',disabled:false,open:false,focus(){this.focused=true;},showModal(){this.open=true;},close(){this.open=false;}});
  return elements.get(selector);
 };
 const api={Date,Intl,URL,document:{querySelector:element,querySelectorAll:()=>[]},localStorage:{getItem:()=>null}};
 vm.createContext(api);
 vm.runInContext(source.slice(0,source.indexOf('const defaultScenario ='))+source.slice(source.indexOf('function escapeHTML('),source.indexOf('$("#scenarioText").value = defaultScenario;')),api);
 const state=vm.runInContext('state',api);
 state.snapshot={agents:[{id:'agent/a',name:'Worker <A>',hostname:'node-a',state:'online',capacity:200,defaultCapacity:200,activeNodes:150}]};
 api.render=()=>{};
 api.showToast=message=>toasts.push(message);
 api.api=(url,options)=>new Promise((resolve,reject)=>requests.push({url,options,resolve,reject}));
 api.openAgentCapacity('agent/a');
 return {api,state,element,requests,toasts};
}
test('Agent settings show the default and escape identity in the table',()=>{
 const {api,state,element}=fixture();
 assert.equal(element('#agentCapacityDialog').open,true);
 assert.equal(element('#agentCapacityMode').value,'default');
 assert.equal(element('#agentCapacityValue').disabled,true);
 assert.match(element('#agentCapacityDefault').textContent,/200/);
 api.renderAgents(state.snapshot.agents);
 assert.match(element('#agentRows').innerHTML,/Worker &lt;A&gt;/);
 assert.match(element('#agentRows').innerHTML,/data-agent-capacity="agent\/a"/);
 api.closeAgentCapacity();
 api.openAgentCapacity('missing');
 assert.equal(element('#agentCapacityDialog').open,false);
});
test('save prevents duplicate submits, encodes Agent IDs and sends only its override',async()=>{
 const {api,state,element,requests}=fixture();
 element('#agentCapacityMode').value='custom';element('#agentCapacityValue').value='100';
 const saving=api.saveAgentCapacity();
 await api.saveAgentCapacity();api.closeAgentCapacity();
 assert.equal(requests.length,1);
 assert.equal(element('#agentCapacityDialog').open,true);
 assert.equal(element('#saveAgentCapacity').disabled,true);
 assert.equal(requests[0].url,'/api/v1/agents/agent%2Fa/capacity');
 assert.equal(requests[0].options.method,'PUT');
 assert.deepEqual(JSON.parse(requests[0].options.body),{capacity:100});
 requests[0].resolve({...state.snapshot.agents[0],capacity:100,capacityOverride:100,capacityPending:true});
 await saving;
 assert.equal(element('#agentCapacityDialog').open,false);
 assert.equal(state.snapshot.agents[0].defaultCapacity,200);
 assert.equal(state.snapshot.agents[0].capacity,100);
 assert.equal(state.agentSettingsSaving,false);
});
test('default reset sends null and offline changes explain deferred application',async()=>{
 const {api,state,element,requests,toasts}=fixture();
 state.snapshot.agents[0].state='offline';state.snapshot.agents[0].capacityOverride=100;
 api.openAgentCapacity('agent/a');
 assert.equal(element('#agentCapacityMode').value,'custom');
 element('#agentCapacityMode').value='default';api.updateAgentCapacityMode();
 const saving=api.saveAgentCapacity();
 assert.deepEqual(JSON.parse(requests[0].options.body),{capacity:null});
 requests[0].resolve({...state.snapshot.agents[0],capacityOverride:0,capacityPending:true});
 await saving;
 assert.match(toasts[0],/reconnects/);
});
test('invalid input never saves and server errors keep edits available for retry',async()=>{
 const {api,state,element,requests}=fixture();
 element('#agentCapacityMode').value='custom';
 for(const value of ['','0','-1','1.5','bad','9007199254740992']){
  element('#agentCapacityValue').value=value;await api.saveAgentCapacity();
  assert.match(element('#agentCapacityError').textContent,/positive whole number/);
 }
 assert.equal(requests.length,0);
 element('#agentCapacityValue').value='100';
 const saving=api.saveAgentCapacity();requests[0].reject(new Error('Unable to save settings'));await saving;
 assert.equal(element('#agentCapacityDialog').open,true);
 assert.equal(element('#agentCapacityValue').value,'100');
 assert.equal(element('#agentCapacityValue').disabled,false);
 assert.equal(state.snapshot.agents[0].capacity,200);
 assert.match(element('#agentCapacityError').textContent,/Unable to save settings/);
});

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const app = fs.readFileSync(path.join(__dirname, 'static/app.js'), 'utf8');
const render = app.slice(app.indexOf('function renderSavedScenarios()'), app.indexOf('async function refreshSavedScenarios()'));

function element(attributes = {}) {
  const classes = new Set();
  return {
    attributes: {...attributes}, disabled: false, hidden: false, textContent: '',
    setAttribute(key, value) { this.attributes[key] = String(value); },
    getAttribute(key) { return this.attributes[key] ?? null; },
    removeAttribute(key) { delete this.attributes[key]; },
    classList: {toggle(key, on) { on ? classes.add(key) : classes.delete(key); }, contains(key) { return classes.has(key); }},
    focus(options) { this.focusOptions = options; },
  };
}

function fixture() {
  const state = {savedScenarios: [{id:'a',name:'Alpha',updatedAt:'first'}, {id:'b',name:'Beta',updatedAt:'second'}], selectedScenarioId:null, pendingScenarioDeleteId:null};
  const controls = new Map();
  const get = key => { if (!controls.has(key)) controls.set(key, element()); return controls.get(key); };
  const selected = new Set(), badges = new Map();
  const library = get('#scenarioLibraryList');
  library.rows = []; library.scrollTop = 217;
  library.querySelectorAll = selector => selector === '[data-scenario-row]' ? library.rows : selector === '[data-library-select-key]' ? library.rows.map(r => r.controls.input).filter(Boolean) : [];
  const visible = () => state.savedScenarios.filter(item => !state.visible || state.visible.includes(item.id));
  const document = {activeElement: null};
  let replacements = 0;
  const sandbox = {
    state, document, $:get,
    renderScenarioValidation(){}, renderScenarioImportReview(){}, renderScenarioRunControl(){},
    scenarioOperationBusy:()=>Boolean(state.busy), filteredSavedScenarios:visible,
    formatResultTime:value=>value, formatScenarioID:value=>value, formatScenarioListDate:value=>value,
    escapeHTML:value=>String(value).replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),
    KPLLibraryGroups:{
      isFiltered:()=>false,
      badgeMarkup:key=>badges.get(key)||'',
      selectionMarkup:key=>state.noSelection ? '' : '<input data-library-select-key="'+key+'">',
      refreshUI(){ for(const row of library.rows) if(row.controls.input){row.controls.input.checked=selected.has(row.getAttribute('data-scenario-row')); row.controls.input.disabled=Boolean(state.busy);} },
    },
    setHTML(target,html){
      assert.equal(target,library); replacements++; library.scrollTop=0;
      library.rows=visible().map(item=>{
        const row=element({'data-scenario-row':item.id}); row.name=item.name;
        row.controls={load:element(),status:element(),delete:element(),input:state.noSelection?null:element({'data-library-select-key':'scenario:'+item.id})};
        if(state.pendingScenarioDeleteId===item.id){row.controls.confirm=element();row.controls.cancel=element();}
        row.querySelector=selector=>({'[data-load-scenario]':row.controls.load,'.scenario-item-state':row.controls.status,'[data-delete-scenario]':row.controls.delete,'[data-confirm-scenario-delete]':row.controls.confirm,'[data-cancel-scenario-delete]':row.controls.cancel}[selector]||null);
        row.classList.toggle('selected',item.id===state.selectedScenarioId);
        if(item.id===state.selectedScenarioId)row.setAttribute('aria-current','true');
        row.controls.load.disabled=Boolean(state.busy);
        row.controls.delete.disabled=Boolean(state.busy)||state.pendingScenarioDeleteId===item.id;
        row.controls.status.textContent=state.scenarioLoadingId===item.id?'Loading…':item.id===state.selectedScenarioId?'Editing':'Load →';
        return row;
      });
      target.markup=html;
    },
  };
  vm.createContext(sandbox); vm.runInContext(render,sandbox);
  return {state,library,selected,badges,document,render:()=>sandbox.renderSavedScenarios(),replacements:()=>replacements};
}

test('loading and editing a saved scenario preserve each row node and list scroll',()=>{
  const f=fixture();f.render();const original=f.library.rows.slice(),writes=f.replacements();
  for(const [loading,selected,busy] of [['b',null,true],[null,'b',false],[null,'a',false]]){
    f.state.scenarioLoadingId=loading;f.state.selectedScenarioId=selected;f.state.busy=busy;f.render();
    assert.equal(f.replacements(),writes,'selection/loading is a state patch, not replacement');
    f.library.rows.forEach((row,i)=>assert.equal(row,original[i]));
    assert.equal(f.library.scrollTop,217);
    for(const row of original){const id=row.getAttribute('data-scenario-row');assert.equal(row.controls.load.disabled,busy);assert.equal(row.classList.contains('selected'),id===selected);assert.equal(row.getAttribute('aria-current'),id===selected?'true':null);assert.equal(row.controls.status.textContent,id===loading?'Loading…':id===selected?'Editing':'Load →');}
  }
});

test('checkbox refresh and focus restoration retain row identity without scrolling',()=>{
  const f=fixture();f.render();const row=f.library.rows[1],input=row.controls.input,writes=f.replacements();
  f.document.activeElement=input;f.selected.add('b');f.render();
  assert.equal(f.library.rows[1],row);assert.equal(f.replacements(),writes);assert.equal(input.checked,true);assert.equal(input.focusOptions.preventScroll,true);assert.equal(f.library.scrollTop,217);
  f.selected.clear();f.render();assert.equal(input.checked,false);assert.equal(f.library.rows[1],row);
});

test('real metadata, group and filter changes still update scenario content',()=>{
  const f=fixture();f.render();let writes=f.replacements();
  f.state.savedScenarios[0].name='Renamed';f.render();assert.equal(f.replacements(),++writes);assert.equal(f.library.rows[0].name,'Renamed');
  f.badges.set('scenario:a','<span>New group</span>');f.render();assert.equal(f.replacements(),++writes);assert.match(f.library.markup,/New group/);
  f.state.visible=['b'];f.render();assert.equal(f.replacements(),++writes);assert.deepEqual(f.library.rows.map(r=>r.getAttribute('data-scenario-row')),['b']);
});

test('delete confirmation state can update in place while preserving scroll',()=>{
  const f=fixture();f.render();f.state.pendingScenarioDeleteId='a';f.render();const row=f.library.rows[0],writes=f.replacements();
  f.state.scenarioDeletingId='a';f.state.busy=true;f.render();
  assert.equal(f.replacements(),writes);assert.equal(f.library.rows[0],row);assert.equal(row.controls.confirm.textContent,'Deleting…');assert.equal(row.controls.confirm.disabled,true);assert.equal(row.controls.cancel.disabled,true);assert.equal(f.library.scrollTop,217);
});

test('queue help closes before the modal and restores focus without scrolling',()=>{
  const summary=element(),help={open:true,querySelector:selector=>selector==='summary'?summary:null};
  const sandbox={$:selector=>selector==='.scenario-run-help'?help:null};
  vm.createContext(sandbox);
  vm.runInContext(app.slice(app.indexOf('function closeScenarioHelp('),app.indexOf('function searchSavedScenarios(')),sandbox);
  assert.equal(sandbox.closeScenarioHelp(true),true);assert.equal(help.open,false);assert.equal(summary.focusOptions.preventScroll,true);
  assert.equal(sandbox.closeScenarioHelp(true),false);
});

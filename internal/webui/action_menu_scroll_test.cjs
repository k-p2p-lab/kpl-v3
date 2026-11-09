const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function menuHarness(followScroll) {
  const listeners = new Map(), classes = new Set(), attributes = new Map();
  let triggerRect = {top:390, bottom:420, right:900}, measurements = 0, focused = null;
  const panelAttributes = new Map();
  const panel = {
    style:{}, scrollTop:0, scrollLeft:0,
    contains:target => target === panel || target === checkbox,
    hasAttribute:name => panelAttributes.has(name),
    setAttribute:(name,value) => panelAttributes.set(name,value),
    getBoundingClientRect:() => { measurements++; return {width:240,height:200}; },
  };
  const trigger = {
    getBoundingClientRect:() => triggerRect,
    setAttribute:(name,value) => attributes.set(name,value),
    closest:selector => selector === '[data-action-menu-toggle]' ? trigger : selector === '.action-menu' ? menu : null,
    focus:() => { focused = trigger; }, isConnected:true,
  };
  const checkbox = {closest:selector => selector === '.action-menu' ? menu : null};
  const menu = {
    isConnected:true, tagName:'SPAN',
    classList:{contains:name => classes.has(name), add:name => classes.add(name), remove:name => classes.delete(name)},
    contains:target => target === trigger || target === panel || target === checkbox,
    hasAttribute:name => name === 'data-action-menu-follow-scroll' && followScroll,
    querySelector:selector => selector.includes('summary') ? trigger : selector.includes('.action-menu-items') ? panel : null,
  };
  const document = {
    documentElement:{clientWidth:1200}, activeElement:checkbox,
    querySelectorAll:() => [menu],
    addEventListener:(type,handler) => listeners.set(type,handler),
  };
  const window = {innerHeight:900, addEventListener(){}};
  vm.runInNewContext(fs.readFileSync(path.join(__dirname,'static/action-menus.js'),'utf8'),{document,window,queueMicrotask});
  const dispatch = (type,target,extra={}) => listeners.get(type)({target,preventDefault(){},stopPropagation(){},...extra});
  dispatch('click',trigger);
  return {
    dispatch, document, panel, checkbox, trigger, attributes,
    open:() => classes.has('is-open'), measurements:() => measurements, focused:() => focused,
    moveTrigger:rect => { triggerRect = rect; },
  };
}

test('group menu follows its trigger when filtering makes the browser adjust page scroll', () => {
  const h = menuHarness(true);
  assert.equal(h.open(),true);
  assert.equal(h.panel.style.top,'428px');
  h.moveTrigger({top:300,bottom:330,right:900});
  h.dispatch('scroll',h.document);
  assert.equal(h.open(),true);
  assert.equal(h.attributes.get('aria-expanded'),'true');
  assert.equal(h.panel.style.top,'338px','the open panel follows the trigger after layout changes');
});

test('scrolling group options keeps the panel open without disturbing its offset', () => {
  const h = menuHarness(true), before = h.measurements();
  h.panel.scrollTop = 75;
  h.dispatch('scroll',h.panel);
  assert.equal(h.open(),true);
  assert.equal(h.panel.scrollTop,75);
  assert.equal(h.measurements(),before);
});

test('ordinary action menus still close when their containing page scrolls', () => {
  const h = menuHarness(false);
  h.dispatch('scroll',h.document);
  assert.equal(h.open(),false);
  assert.equal(h.attributes.get('aria-expanded'),'false');
});

for (const dismissal of ['outside click','Escape','tab change']) {
  test('persistent group menu still closes on ' + dismissal, () => {
    const h = menuHarness(true);
    if (dismissal === 'outside click') h.dispatch('click',{closest:() => null});
    else if (dismissal === 'Escape') h.dispatch('keydown',h.checkbox,{key:'Escape'});
    else h.dispatch('dashboard:tab-change',h.document);
    assert.equal(h.open(),false);
    assert.equal(h.attributes.get('aria-expanded'),'false');
    if (dismissal === 'Escape') assert.equal(h.focused(),h.trigger);
  });
}

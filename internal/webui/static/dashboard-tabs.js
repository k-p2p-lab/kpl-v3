(() => {
  const tabs = [...document.querySelectorAll('[data-dashboard-tab]')];
  if (!tabs.length) return;
  const panels = tabs.map(tab => document.getElementById(tab.getAttribute('aria-controls')));
  let active = null;

  function select(id, { focus = false, history = false } = {}) {
    const tab = tabs.find(item => item.dataset.dashboardTab === id);
    if (!tab) return;
    const moveFocus = focus || panels.some(panel => panel.id !== id && panel.contains(document.activeElement));
    for (const item of tabs) {
      const selected = item === tab;
      item.setAttribute('aria-selected', String(selected));
      item.tabIndex = selected ? 0 : -1;
    }
    for (const panel of panels) panel.hidden = panel.id !== id;
    if (history && window.location.hash !== `#${id}`) {
      window.history.pushState(null, '', `#${id}`);
    }
    const previous = active;
    active = id;
    if (moveFocus) {
      tab.focus({ preventScroll: true });
      tab.scrollIntoView({ block: 'nearest', inline: 'nearest', behavior: 'instant' });
    }
    if (previous !== id) document.dispatchEvent(new CustomEvent('dashboard:tab-change', { detail: { id, previous } }));
  }

  for (const [index, tab] of tabs.entries()) {
    tab.addEventListener('click', () => select(tab.dataset.dashboardTab, { history: true }));
    tab.addEventListener('keydown', event => {
      let next;
      if (event.key === 'ArrowRight') next = (index + 1) % tabs.length;
      else if (event.key === 'ArrowLeft') next = (index + tabs.length - 1) % tabs.length;
      else if (event.key === 'Home') next = 0;
      else if (event.key === 'End') next = tabs.length - 1;
      else return;
      event.preventDefault();
      select(tabs[next].dataset.dashboardTab, { focus: true, history: true });
    });
  }
  function restore() {
    const id = window.location.hash.slice(1);
    select(tabs.some(tab => tab.dataset.dashboardTab === id) ? id : tabs[0].dataset.dashboardTab);
  }
  window.addEventListener('hashchange', restore);
  window.addEventListener('popstate', restore);
  globalThis.KPLDashboardTabs = { show: id => select(id, { focus: true, history: true }) };
  restore();
})();

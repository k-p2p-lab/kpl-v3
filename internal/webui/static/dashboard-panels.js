(() => {
  const panels = [...document.querySelectorAll('.panel[data-panel]')];
  if (!panels.length) return;
  const storageKey = 'kpl-dashboard-panels-v1';
  const known = new Set(panels.map(panel => panel.dataset.panel));
  let collapsed = new Set();
  try {
    const saved = JSON.parse(localStorage.getItem(storageKey) || '[]');
    if (Array.isArray(saved)) collapsed = new Set(saved.filter(id => known.has(id)));
  } catch { /* Keep the dashboard usable without browser storage. */ }
  const pairs = [...document.querySelectorAll('[data-panel-pair]')].map(group => ({
    group, panels: panels.filter(panel => panel.parentElement === group),
  }));
  const toggleFor = panel => panel.querySelector('[data-panel-toggle]');
  const bodyFor = panel => document.getElementById(toggleFor(panel).getAttribute('aria-controls'));

  function arrangePairs() {
    const focused = document.activeElement;
    for (const pair of pairs) {
      const closed = pair.panels.filter(panel => collapsed.has(panel.dataset.panel));
      pair.group.dataset.panelLayout = closed.length === 0 ? 'split' : closed.length === pair.panels.length ? 'closed' : 'single';
      const order = closed.length === 1 ? [...closed, ...pair.panels.filter(panel => !closed.includes(panel))] : pair.panels;
      // Match DOM and keyboard order to the compact header above the open pane.
      order.forEach((panel, index) => {
        if (pair.group.children[index] !== panel) pair.group.insertBefore(panel, pair.group.children[index] || null);
      });
    }
    if (focused?.isConnected && document.activeElement !== focused) focused.focus({ preventScroll: true });
  }
  function update(panel, close) {
    const toggle = toggleFor(panel), body = bodyFor(panel);
    const controls = [...panel.querySelectorAll('[data-panel-expanded-only]')];
    if (close && (body.contains(document.activeElement) || controls.some(control => control.contains(document.activeElement)))) toggle.focus({ preventScroll: true });
    body.hidden = close;
    panel.classList.toggle('is-collapsed', close);
    toggle.setAttribute('aria-expanded', String(!close));
    const action = close ? 'Expand' : 'Collapse';
    toggle.setAttribute('aria-label', `${action} ${panel.querySelector('h2').textContent.trim()}`);
    toggle.querySelector('.panel-toggle-label').textContent = action;
    for (const control of controls) control.hidden = close;
  }
  for (const panel of panels) {
    update(panel, collapsed.has(panel.dataset.panel));
    toggleFor(panel).addEventListener('click', () => {
      const id = panel.dataset.panel, close = !collapsed.has(id);
      if (close) collapsed.add(id); else collapsed.delete(id);
      update(panel, close);
      arrangePairs();
      try { localStorage.setItem(storageKey, JSON.stringify([...collapsed])); }
      catch { /* The current page still retains its layout preference. */ }
      document.dispatchEvent(new CustomEvent('dashboard:panel-toggle', { detail: { id, collapsed: close } }));
      // A paired panel can move above its neighbor; keep its toggle reachable
      // below the sticky navigation without jumping when it is already visible.
      const toggle = toggleFor(panel), bounds = toggle.getBoundingClientRect();
      const top = (document.querySelector('.topbar')?.getBoundingClientRect().bottom || 0) + 8;
      if (bounds.top < top) window.scrollBy({ top: bounds.top - top, behavior: 'instant' });
      else if (bounds.bottom > window.innerHeight) toggle.scrollIntoView({ block: 'nearest', behavior: 'instant' });
    });
  }
  for (const link of document.querySelectorAll('[data-panel-link]')) {
    link.addEventListener('click', () => {
      const panel = panels.find(panel => panel.dataset.panel === link.dataset.panelLink);
      if (panel && collapsed.has(panel.dataset.panel)) toggleFor(panel).click();
    });
  }
  arrangePairs();
})();

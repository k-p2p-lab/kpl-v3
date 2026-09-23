(() => {
  const track = document.querySelector('#metricCarousel');
  if (!track) return;
  const cards = [...track.querySelectorAll('.metric-card')];
  const previous = document.querySelector('#metricsPrevious');
  const next = document.querySelector('#metricsNext');
  const reduced = window.matchMedia('(prefers-reduced-motion: reduce)');
  let active = null, origin = null, dismissed = null;
  let pointer = null, moved = false, lastX = null, lastY = null;
  let revealTimer = null, scrollFrame = null;

  const button = card => card.querySelector('.metric-toggle');
  const detail = card => card.querySelector('.metric-detail');
  const behavior = () => reduced.matches ? 'instant' : 'smooth';
  const updateNavigation = () => {
    previous.disabled = track.scrollLeft <= 2;
    next.disabled = track.scrollLeft + track.clientWidth >= track.scrollWidth - 2;
  };
  const reveal = card => {
    if (active !== card) return;
    const viewport = track.getBoundingClientRect();
    const bounds = card.getBoundingClientRect();
    const delta = bounds.left < viewport.left + 4 ? bounds.left - viewport.left - 4
      : bounds.right > viewport.right - 4 ? bounds.right - viewport.right + 4 : 0;
    if (delta) track.scrollBy({ left: delta, behavior: origin === 'hover' ? 'instant' : behavior() });
    updateNavigation();
  };
  function expand(card, source) {
    if (card === active) return;
    clearTimeout(revealTimer);
    if (active) {
      active.classList.remove('is-expanded');
      button(active).setAttribute('aria-expanded', 'false');
      detail(active).hidden = true;
    }
    active = card;
    origin = source;
    if (card) {
      card.classList.add('is-expanded');
      button(card).setAttribute('aria-expanded', 'true');
      detail(card).hidden = false;
      // Reveal after the widths settle, without scrolling the page vertically.
      revealTimer = setTimeout(() => reveal(card), reduced.matches ? 0 : 260);
    }
    updateNavigation();
  }
  function dismiss() {
    dismissed = active;
    // Keep focus on the disclosure when hiding its scrollable detail region.
    if (active && detail(active).contains(document.activeElement)) button(active).focus({ preventScroll: true });
    expand(null, null);
  }

  // Layout and scrolling can generate pointerenter without the mouse moving.
  // Only real pointer motion chooses a different card, avoiding hover loops.
  track.addEventListener('pointermove', event => {
    if (event.pointerType === 'touch' || event.buttons || (event.clientX === lastX && event.clientY === lastY)) return;
    lastX = event.clientX;
    lastY = event.clientY;
    const card = event.target.closest('.metric-card');
    if (dismissed && dismissed !== card) dismissed = null;
    if (card && card !== dismissed) expand(card, 'hover');
  });
  track.addEventListener('pointerleave', event => {
    if (event.pointerType === 'touch') return;
    dismissed = null;
    lastX = lastY = null;
    if (origin === 'hover' && !active?.contains(document.activeElement)) expand(null, null);
  });

  track.addEventListener('pointerdown', event => {
    pointer = { id: event.pointerId, x: event.clientX, y: event.clientY, scroll: track.scrollLeft };
    moved = false;
  });
  track.addEventListener('pointermove', event => {
    if (pointer?.id === event.pointerId && Math.hypot(event.clientX - pointer.x, event.clientY - pointer.y) > 8) moved = true;
  });
  window.addEventListener('pointerup', () => { pointer = null; });
  window.addEventListener('pointercancel', () => { pointer = null; moved = true; });
  track.addEventListener('click', event => {
    const toggle = event.target.closest('.metric-toggle');
    if (!toggle) return;
    // A swipe can end with a synthesized click; it must only move the row.
    if (event.detail !== 0 && moved) { moved = false; return; }
    const card = toggle.closest('.metric-card');
    if (card === active && origin !== 'hover') dismiss();
    else {
      dismissed = null;
      if (card === active) origin = 'press';
      else expand(card, 'press');
    }
  });
  track.addEventListener('focusin', event => {
    const toggle = event.target.closest('.metric-toggle');
    if (toggle && !pointer && toggle.matches(':focus-visible')) {
      dismissed = null;
      expand(toggle.closest('.metric-card'), 'focus');
    }
  });
  track.addEventListener('focusout', event => {
    // Shared metrics stay open while moving between workspace tabs.
    if (!track.contains(event.relatedTarget) && !event.relatedTarget?.closest('[data-dashboard-tab]')) dismiss();
  });
  track.addEventListener('keydown', event => {
    if (event.key === 'Escape') {
      event.preventDefault();
      dismiss();
      return;
    }
    const toggle = event.target.closest('.metric-toggle');
    if (!toggle || !['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    const index = cards.indexOf(toggle.closest('.metric-card'));
    const target = event.key === 'Home' ? 0 : event.key === 'End' ? cards.length - 1
      : Math.max(0, Math.min(cards.length - 1, index + (event.key === 'ArrowRight' ? 1 : -1)));
    dismissed = null;
    button(cards[target]).focus({ preventScroll: true });
    expand(cards[target], 'focus');
  });
  document.addEventListener('pointerdown', event => {
    // Collapsing a mobile card on pointerdown would move the tab before click.
    if (!track.contains(event.target) && !event.target.closest('[data-dashboard-tab]')) dismiss();
  });
  for (const [control, direction] of [[previous, -1], [next, 1]]) {
    control.addEventListener('click', () => {
      expand(null, null);
      track.scrollBy({ left: direction * track.clientWidth * 0.7, behavior: behavior() });
    });
  }
  track.addEventListener('scroll', () => {
    if (pointer && Math.abs(track.scrollLeft - pointer.scroll) > 8) moved = true;
    if (scrollFrame != null) return;
    scrollFrame = requestAnimationFrame(() => { scrollFrame = null; updateNavigation(); });
  }, { passive: true });
  track.addEventListener('transitionend', updateNavigation);
  const resize = () => {
    track.style.setProperty('--metric-expanded', `${Math.min(480, track.clientWidth - 8)}px`);
    updateNavigation();
    if (active) reveal(active);
  };
  if (typeof ResizeObserver !== 'undefined') new ResizeObserver(resize).observe(track);
  else window.addEventListener('resize', resize);
  resize();
})();

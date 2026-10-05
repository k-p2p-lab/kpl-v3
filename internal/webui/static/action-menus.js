(() => {
  let active = null;
  let nextID = 0;

  const triggerFor = menu => menu.querySelector(":scope > summary, :scope > [data-action-menu-toggle]");
  const panelFor = menu => menu.querySelector(":scope > .action-menu-items");
  const isOpen = menu => menu.open || menu.classList.contains("is-open");
  const itemsFor = panel => [...panel.querySelectorAll("button:not(:disabled), a[href], select:not(:disabled), input:not(:disabled)")]
    .filter(item => item.getClientRects().length);

  function position(menu) {
    const panel = panelFor(menu), trigger = triggerFor(menu);
    if (!panel || !trigger || !menu.isConnected) return;
    const rect = trigger.getBoundingClientRect();
    const gap = 8, width = document.documentElement.clientWidth, height = window.innerHeight;
    panel.style.maxWidth = Math.max(0, width - gap * 2) + "px";
    panel.style.maxHeight = Math.max(0, height - gap * 2) + "px";
    const bounds = panel.getBoundingClientRect();
    panel.style.left = Math.max(gap, Math.min(rect.right - bounds.width, width - bounds.width - gap)) + "px";
    const below = height - rect.bottom - gap * 2, above = rect.top - gap * 2;
    const placeAbove = bounds.height > below && above > below;
    panel.style.maxHeight = Math.max(0, placeAbove ? above : below) + "px";
    panel.style.top = (placeAbove ? Math.max(gap, rect.top - Math.min(bounds.height, above) - gap) : rect.bottom + gap) + "px";
  }

  function close(menu, focus = false) {
    if (!menu) return;
    const panel = panelFor(menu), trigger = triggerFor(menu);
    if (typeof panel?.hidePopover === "function" && panel.matches(":popover-open")) panel.hidePopover();
    menu.classList.remove("is-open");
    if (menu.tagName === "DETAILS") menu.open = false;
    trigger?.setAttribute("aria-expanded", "false");
    if (active === menu) active = null;
    if (focus && trigger?.isConnected) trigger.focus({ preventScroll: true });
  }

  function open(menu) {
    const panel = panelFor(menu), trigger = triggerFor(menu);
    if (!panel || !trigger) return;
    if (active && active !== menu) close(active);
    active = menu;
    menu.classList.add("is-open");
    if (menu.tagName === "DETAILS") menu.open = true;
    if (!panel.id) panel.id = "action-menu-panel-" + ++nextID;
    trigger.setAttribute("aria-controls", panel.id);
    trigger.setAttribute("aria-expanded", "true");
    // Top-layer popovers avoid clipping inside tables and modal scroll areas.
    // Older browsers retain the fixed-position panel as a fallback.
    if (typeof panel.showPopover === "function") {
      panel.setAttribute("popover", "manual");
      if (!panel.matches(":popover-open")) panel.showPopover();
    }
    position(menu);
  }

  function refresh() {
    if (active && !active.isConnected) close(active);
    for (const menu of document.querySelectorAll(".action-menu")) {
      if (isOpen(menu)) open(menu);
      else triggerFor(menu)?.setAttribute("aria-expanded", "false");
    }
  }

  document.addEventListener("toggle", event => {
    if (!event.target.matches("details.action-menu")) return;
    if (event.target.open) open(event.target);
    else close(event.target);
  }, true);
  document.addEventListener("click", event => {
    const toggle = event.target.closest("[data-action-menu-toggle]");
    if (toggle && toggle.closest(".action-menu")) {
      event.preventDefault();
      const menu = toggle.closest(".action-menu");
      if (isOpen(menu)) close(menu);
      else open(menu);
      return;
    }
    if (!active) return;
    if (!active.contains(event.target)) close(active);
    else if (event.target.closest(".action-menu-items button:not(:disabled), .action-menu-items a[href]")) {
      const menu = active;
      close(menu);
      // Dialog actions set their own focus; downloads and inline actions return
      // to their trigger after closing the now-hidden panel.
      queueMicrotask(() => {
        if (menu.isConnected && !document.querySelector("dialog[open]") &&
            (document.activeElement === document.body || menu.contains(document.activeElement))) {
          triggerFor(menu)?.focus({ preventScroll: true });
        }
      });
    }
  });
  document.addEventListener("keydown", event => {
    const menu = event.target.closest(".action-menu");
    if (event.key === "Escape" && active) {
      event.preventDefault();
      event.stopPropagation();
      close(active, true);
    } else if (menu && ["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
      event.preventDefault();
      event.stopPropagation();
      open(menu);
      const items = itemsFor(panelFor(menu));
      if (!items.length) return;
      const index = items.indexOf(document.activeElement);
      const next = event.key === "Home" ? 0 : event.key === "End" ? items.length - 1 :
        event.key === "ArrowDown" ? (index + 1) % items.length : (index <= 0 ? items.length : index) - 1;
      items[next].focus();
    }
  }, true);
  document.addEventListener("focusin", event => {
    if (active && !active.contains(event.target)) close(active);
  });
  document.addEventListener("dashboard:tab-change", () => close(active));
  document.addEventListener("scroll", event => {
    if (active && !panelFor(active)?.contains(event.target)) close(active);
  }, true);
  window.addEventListener("resize", () => { if (active) position(active); });
  window.KPLActionMenus = { refresh };
  refresh();
})();

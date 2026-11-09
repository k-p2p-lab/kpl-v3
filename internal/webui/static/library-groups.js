(function (root) {
  "use strict";

  const endpoint = "/api/v1/library-groups", maxSelection = 2000;
  const escape = value => String(value ?? "").replace(/[&<>"']/g, char => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"})[char]);
  const scopeFor = key => String(key).startsWith("scenario:") ? "scenarios" : "results";

  function createLibraryGroups(environment = root) {
    const document = environment.document;
    let options, initialized = false, snapshot = {revision: "0", groups: [], memberships: {}}, loaded = false;
    let filters = new Set(["*"]), selectionScope = "", selected = new Set(), targetGroup = "";
    let readTask = null, mutationTask = null, epoch = 0, busy = false, needsRefresh = false;
    let errorText = "", noticeText = "", dialog = null, returnFocus = null, returnScope = "", editing = "", confirming = false;
    let analysisDownloadController = null;
    const markupCache = new WeakMap();

    function setMarkup(element, html) {
      if (markupCache.get(element) === html) return false;
      element.innerHTML = html;
      markupCache.set(element, html);
      return true;
    }

    function items() {
      const unique = new Map();
      for (const item of options?.getItems?.() || []) {
        if (typeof item.key !== "string" || !/^(scenario|batch|run):.+/.test(item.key)) continue;
        if (!unique.has(item.key)) unique.set(item.key, {...item, scope: scopeFor(item.key)});
      }
      return [...unique.values()];
    }

    function groupFor(key) { return snapshot.memberships[key] || ""; }
    function getGroupName(id) { return snapshot.groups.find(group => group.id === id)?.name || ""; }
    function matches(key) { return filters.has("*") || filters.has(groupFor(key)); }
    function importGroup() { return loaded && filters.size === 1 && !filters.has("*") ? [...filters][0] : ""; }
    function locked() { return busy || Boolean(options?.isExternallyBusy?.()); }
    function ready() { return loaded && !locked() && !needsRefresh; }
    function changed() { options?.onChanged?.(); }

    function validateSnapshot(value) {
      if (!value || typeof value.revision !== "string" || !Array.isArray(value.groups) ||
          !value.memberships || typeof value.memberships !== "object" || Array.isArray(value.memberships)) {
        throw new Error("The group response was invalid. Refresh groups to try again.");
      }
      const ids = new Set();
      const groups = value.groups.map(group => {
        if (!group || typeof group.id !== "string" || !group.id || typeof group.name !== "string" || ids.has(group.id)) {
          throw new Error("The group response was invalid. Refresh groups to try again.");
        }
        ids.add(group.id);
        return {id: group.id, name: group.name};
      });
      const memberships = Object.create(null);
      for (const [key, id] of Object.entries(value.memberships)) {
        if (typeof id !== "string" || !ids.has(id)) throw new Error("The group response was invalid. Refresh groups to try again.");
        memberships[key] = id;
      }
      return {revision: value.revision, groups, memberships};
    }

    function adopt(value) {
      const next = validateSnapshot(value);
      snapshot = next;
      loaded = true;
      needsRefresh = false;
      const removed = [...filters].filter(id => id !== "*" && id && !getGroupName(id));
      for (const id of removed) filters.delete(id);
      if (removed.length && !filters.size) filters.add("");
      if (targetGroup && !getGroupName(targetGroup)) targetGroup = "";
      if (editing && !getGroupName(editing)) { editing = ""; confirming = false; }
      refreshUI();
      changed();
    }

    async function request(path, requestOptions = {}) {
      if (!options?.api) throw new Error("Group tools are not ready.");
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 30000);
      try {
        return await options.api(path, {...requestOptions, signal: controller.signal});
      } catch (error) {
        if (error.name === "AbortError") {
          throw new Error(requestOptions.method
            ? "The request timed out and may have completed. Refresh groups before trying again."
            : "Loading groups timed out. Refresh groups to try again.");
        }
        throw error;
      } finally { clearTimeout(timer); }
    }

    function loadSnapshot() {
      if (readTask) return readTask;
      const requestEpoch = epoch;
      readTask = (async () => {
        try {
          const value = await request(endpoint);
          if (requestEpoch === epoch) {
            errorText = "";
            adopt(value);
          }
          return snapshot;
        } catch (error) {
          if (requestEpoch === epoch) {
            errorText = error.message || "Could not load groups.";
            refreshUI();
          }
          throw error;
        } finally { readTask = null; }
      })();
      return readTask;
    }

    function refresh({fresh = false} = {}) {
      if (fresh) {
        // An external write (such as submitting a saved scenario) may have
        // changed memberships after an existing GET began. Ignore that older
        // snapshot and issue a read after pending local operations finish.
        epoch++;
        return (async () => {
          if (readTask) await readTask.catch(() => {});
          if (mutationTask) await mutationTask.catch(() => {});
          return loadSnapshot();
        })();
      }
      if (mutationTask) return mutationTask.then(() => snapshot);
      return loadSnapshot();
    }

    function mutate(path, method, body, successMessage) {
      if (!ready()) return Promise.reject(new Error(busy ? "A group update is still in progress." : "Refresh groups before making changes."));
      busy = true;
      options?.onSelectionChanged?.();
      errorText = "";
      noticeText = "";
      epoch++;
      refreshUI();
      mutationTask = (async () => {
        try {
          const value = await request(path, {
            method, headers: {"Content-Type": "application/json"},
            body: JSON.stringify({...body, revision: snapshot.revision}),
          });
          noticeText = successMessage;
          adopt(value);
          return snapshot;
        } catch (error) {
          if (error.status === 409) {
            // A read started before this write must not replace the conflict refresh.
            if (readTask) await readTask.catch(() => {});
            try {
              await loadSnapshot();
              error = new Error("Groups changed or that name is already in use. Review the current groups and try again.");
            } catch (_) {
              needsRefresh = true;
              error = new Error("Groups changed, but reloading failed. Refresh groups before trying again.");
            }
          } else if (!error.status || error.status >= 500) {
            needsRefresh = true;
          }
          errorText = error.message || "Could not update groups.";
          throw error;
        } finally {
          busy = false;
          options?.onSelectionChanged?.();
          mutationTask = null;
          refreshUI();
        }
      })();
      return mutationTask;
    }

    function assign(keys, groupId = "") {
      const unique = [...new Set(keys || [])];
      if (!unique.length) return Promise.resolve(snapshot);
      if (unique.length > maxSelection) return Promise.reject(new Error("Move up to 2,000 items at a time."));
      if (groupId && !getGroupName(groupId)) return Promise.reject(new Error("That group no longer exists. Choose another group."));
      return mutate(endpoint + "/members", "PUT", {groupId, keys: unique}, "Moved " + unique.length + (unique.length === 1 ? " item." : " items."));
    }

    function reorder(ids, successMessage = "Group order saved.") {
      const remaining = new Set(snapshot.groups.map(group => group.id));
      if (!Array.isArray(ids) || ids.length !== remaining.size || ids.some(id => !remaining.delete(id)) || remaining.size) {
        return Promise.reject(new Error("Group order must contain every current group exactly once."));
      }
      return mutate(endpoint + "/order", "PUT", {ids: [...ids]}, successMessage);
    }

    function moveGroup(id, delta) {
      const index = snapshot.groups.findIndex(group => group.id === id);
      if (index < 0) return Promise.reject(new Error("That group no longer exists. Refresh groups to try again."));
      const target = index + delta;
      if (target < 0 || target >= snapshot.groups.length) return Promise.resolve(snapshot);
      const ids = snapshot.groups.map(group => group.id);
      [ids[index], ids[target]] = [ids[target], ids[index]];
      return reorder(ids, "Moved " + snapshot.groups[index].name + " to position " + (target + 1) + " of " + ids.length + ". Order saved.");
    }

    function focusOrderControl(id, action) {
      if (!dialog?.open) return;
      const row = [...(dialog.querySelectorAll("[data-library-order-group]") || [])].find(item => item.dataset.libraryOrderGroup === id);
      const controls = [row?.querySelector('[data-library-action="' + action + '"]'), row?.querySelector('[data-library-action="edit"]'), dialog.querySelector('[data-library-action="refresh"]'), dialog.querySelector("#libraryGroupName")];
      controls.find(control => control && !control.disabled)?.focus({preventScroll: true});
      row?.scrollIntoView?.({block: "nearest", inline: "nearest"});
    }

    function setFilter(id) {
      if (locked()) return;
      if (id !== "*" && id && !getGroupName(id)) return;
      if (filters.size === 1 && filters.has(id)) return;
      filters = new Set([id]);
      filterChanged();
    }

    function filterChanged() {
      selected.clear();
      selectionScope = "";
      errorText = "";
      noticeText = "";
      refreshUI();
      changed();
    }

    function checkFilter(id, checked) {
      if (!loaded || locked()) return;
      if (id === "*") filters = checked ? new Set(["*"]) : new Set();
      else {
        const ids = ["", ...snapshot.groups.map(group => group.id)];
        if (!ids.includes(id)) return;
        if (filters.has("*")) filters = new Set(ids);
        if (checked) filters.add(id); else filters.delete(id);
        if (ids.every(value => filters.has(value))) filters = new Set(["*"]);
      }
      filterChanged();
    }

    function visibleKeys(scope, allItems = items()) {
      const valid = new Set(allItems.filter(item => item.scope === scope && matches(item.key)).map(item => item.key));
      return [...new Set(options?.getVisibleKeys?.(scope) || [...valid])].filter(key => valid.has(key));
    }

    function checkVisible(scope, checked) {
      if (!ready() || !["scenarios", "results"].includes(scope)) return;
      const keys = visibleKeys(scope);
      const next = selectionScope === scope ? new Set(selected) : new Set();
      for (const key of keys) { if (checked) next.add(key); else next.delete(key); }
      if (next.size > maxSelection) throw new Error("Select up to 2,000 items at a time. Narrow the list or select fewer items.");
      if (selectionScope !== scope) targetGroup = importGroup();
      selected = next;
      selectionScope = selected.size ? scope : "";
      refreshUI();
      options?.onSelectionChanged?.();
    }

    function selectionMarkup(key, label) {
      return '<label class="library-item-select" data-library-select title="Select item"><input type="checkbox" data-library-select-key="' + escape(key) + '" aria-label="Select ' + escape(label) + '"' + (selected.has(key) ? " checked" : "") + (ready() ? "" : " disabled") + '></label>';
    }

    function badgeMarkup(key) {
      const name = getGroupName(groupFor(key));
      return name ? '<span class="library-group-badge" title="Group: ' + escape(name) + '">' + escape(name) + "</span>" : "";
    }

    function groupOptions(value) {
      const option = (id, name) => '<option value="' + escape(id) + '"' + (value === id ? " selected" : "") + ">" + escape(name) + "</option>";
      return option("", "Ungrouped") + snapshot.groups.map(group => option(group.id, group.name)).join("");
    }

    function filterMarkup(scope, counts) {
      const all = filters.has("*");
      const summary = all ? "All groups" : filters.size === 1 ? (getGroupName([...filters][0]) || "Ungrouped") : filters.size ? filters.size + " groups" : "No groups";
      const checkbox = (id, name) => '<label class="library-filter-option"><input type="checkbox" data-library-group-filter="' + escape(id) + '" aria-label="Show ' + escape(name) + '"' + (all || filters.has(id) ? " checked" : "") + (id === "*" && !all && filters.size ? ' data-library-mixed="true"' : "") + (loaded && !locked() ? "" : " disabled") + '><span>' + escape(name) + '</span><span class="library-filter-count">' + (counts.get(id) || 0) + '</span></label>';
      return '<span class="action-menu library-filter-menu" data-library-filter-menu data-action-menu="library-filter:' + scope + '"><button class="secondary-button" type="button" data-action-menu-toggle="library-filter:' + scope + '" aria-expanded="false" aria-label="Filter ' + scope + ' by group"' + (loaded && !locked() ? "" : " disabled") + '>Groups <span class="library-filter-summary">' + escape(summary) + '</span></button><span class="action-menu-items library-filter-options" role="group" aria-label="Filter ' + scope + ' by group">' + checkbox("*", "All groups") + checkbox("", "Ungrouped") + snapshot.groups.map(group => checkbox(group.id, group.name)).join("") + '</span></span>';
    }

    function renderBars(allItems) {
      const restoreControls = [];
      for (const scope of ["scenarios", "results"]) {
        const container = document?.querySelector(scope === "scenarios" ? "#scenarioGroupTools" : "#resultGroupTools");
        if (!container) continue;
        const counts = new Map([["*", 0]]);
        for (const item of allItems.filter(item => item.scope === scope)) {
          const id = groupFor(item.key);
          counts.set(id, (counts.get(id) || 0) + 1);
          counts.set("*", counts.get("*") + 1);
        }
        const selecting = selectionScope === scope;
        const selectedCount = selecting ? selected.size : 0;
        const keys = visibleKeys(scope, allItems), checkedCount = selecting ? keys.filter(key => selected.has(key)).length : 0;
        const allChecked = keys.length > 0 && checkedCount === keys.length, mixed = checkedCount > 0 && !allChecked;
        const disabled = selectedCount && ready() ? "" : " disabled";
        const canDownload = selectedCount && ready();
        const retryHint = !selectedCount ? "Select result checkboxes first." : !loaded ? "Wait for groups to load."
          : needsRefresh ? "Refresh groups to confirm their current state." : locked() ? "Wait for the current library operation to finish."
          : "Retry failed, canceled or interrupted runs; completed runs are kept.";
        const moveControls = '<label class="visually-hidden" for="libraryMove-' + scope + '">Move selected items to group</label><select id="libraryMove-' + scope + '" data-library-move-target' + disabled + '>' + groupOptions(selecting ? targetGroup : importGroup()) + '</select><button class="secondary-button" type="button" data-library-action="move"' + disabled + '>Move</button>';
        const downloadControls = (scope === "scenarios" || options?.download || options?.onDeleteSelected || scope === "results" && (options?.downloadCSV || options?.downloadJSON || options?.onAnalyzeSelected || options?.onRetrySelected || options?.onDeleteFailedSelected))
          ? '<div class="library-selection-downloads"><span class="action-menu" data-action-menu="library-download:' + scope + '">'
            + '<button class="secondary-button" type="button" data-action-menu-toggle="library-download:' + scope + '" aria-expanded="false"' + (canDownload ? "" : " disabled") + '>Actions<span class="visually-hidden"> for selected ' + scope + '</span></button><span class="action-menu-items">'
            + (scope === "scenarios" ? '<span class="library-move-menu">' + moveControls + '</span>' : "")
            + (scope === "results" && options?.onAnalyzeSelected ? '<button type="button" data-library-action="analyze-selected" title="Queue analysis for eligible selected saved runs"' + (canDownload ? "" : " disabled") + '>Start analysis</button>' : "")
            + (scope === "results" && options?.onRetrySelected ? '<button type="button" data-library-action="retry-selected" title="' + escape(retryHint) + '"' + (canDownload ? "" : " disabled") + '>Retry unfinished runs</button>' : "")
            + (scope === "results" && options?.onDeleteFailedSelected ? '<button type="button" data-library-action="delete-failed-selected"' + (canDownload ? "" : " disabled") + '>Delete failed runs…</button>' : "")
            + (options?.download ? '<button type="button" data-library-action="download" title="Download selected ' + scope + ' in one ZIP"' + (canDownload ? "" : " disabled") + '>' + (scope === "results" ? "Results ZIP" : "Scenarios ZIP") + '</button>' : "")
            + (scope === "results" && options?.downloadCSV ? '<button type="button" data-library-action="download-csv" title="Download one ZIP of per-Run CSV ZIPs, without images"' + (canDownload ? "" : " disabled") + '>CSV ZIP</button>' : "")
            + (scope === "results" && options?.downloadJSON ? '<button type="button" data-library-action="download-json" title="Download batch mean JSON for repetition batches and per-Run analysis JSON in one ZIP"' + (canDownload ? "" : " disabled") + '>Analysis JSON ZIP</button>' : "")
            + (options?.onDeleteSelected ? '<button class="library-delete-action" type="button" data-library-action="delete-selected"' + (canDownload ? "" : " disabled") + '>Delete selected…</button>' : "")
            + '</span></span>'
            + (scope === "results" && analysisDownloadController ? '<button class="secondary-button" type="button" data-library-action="cancel-download">Cancel</button>' : "") + '</div>' : "";
        const html = '<div class="library-group-toolbar"><label class="library-select-all" data-library-select><input type="checkbox" data-library-select-visible data-library-scope="' + scope + '" aria-label="Select visible ' + scope + '"' + (allChecked ? " checked" : "") + (mixed ? ' data-library-mixed="true"' : "") + (!keys.length || !ready() ? " disabled" : "") + '><span>Select visible</span><span class="library-filter-count">' + keys.length + '</span></label><div class="library-group-actions">' + filterMarkup(scope, counts) + '<button class="secondary-button" type="button" data-library-action="manage" aria-label="Manage shared groups">Manage</button></div></div>' +
          '<div class="library-group-selection"><div class="library-selection-count"><span role="status" aria-live="polite" title="' + selectedCount + ' selected' + (selectedCount > checkedCount ? ' · ' + (selectedCount - checkedCount) + ' hidden' : '') + '">' + selectedCount + ' selected' + (selectedCount > checkedCount ? ' · ' + (selectedCount - checkedCount) + ' hidden' : '') + '</span><button class="library-text-button" type="button" data-library-action="clear"' + (!selectedCount || locked() ? " disabled" : "") + '>Clear</button></div>' + (scope === "scenarios" ? "" : moveControls) + downloadControls + '</div>' +
          (errorText ? '<p class="library-group-status" role="alert">' + escape(errorText) + ' <button class="library-text-button" type="button" data-library-action="refresh"' + (locked() ? " disabled" : "") + '>Refresh groups</button></p>' : noticeText ? '<p class="library-group-status" role="status">' + escape(noticeText) + "</p>" : !loaded ? '<p class="library-group-status" role="status">Loading groups…</p>' : "");
        const active = document.activeElement;
        const focusKind = container.contains(active) ? active?.hasAttribute("data-library-select-visible") ? "[data-library-select-visible]" : active?.hasAttribute("data-library-move-target") ? "[data-library-move-target]" : active?.dataset?.libraryAction ? '[data-library-action="' + active.dataset.libraryAction + '"]' : active?.hasAttribute("data-action-menu-toggle") ? '[data-action-menu-toggle="' + active.dataset.actionMenuToggle + '"]' : "" : "";
        const filterFocus = container.contains(active) ? active?.getAttribute?.("data-library-group-filter") : null;
        const filterMenu = container.querySelector("[data-library-filter-menu]");
        const filterOpen = filterMenu?.classList?.contains("is-open");
        const filterScroll = filterMenu?.querySelector(".action-menu-items")?.scrollTop || 0;
        if (setMarkup(container, html)) {
          if (filterOpen) {
            const menu = container.querySelector("[data-library-filter-menu]");
            menu?.classList.add("is-open");
          }
          restoreControls.push(() => {
            if (focusKind) {
              const target = container.querySelector(focusKind);
              (target && !target.disabled ? target : container.querySelector("[data-library-select-visible]:not(:disabled)"))?.focus({preventScroll: true});
            }
            if (filterFocus != null) [...(container.querySelectorAll?.("[data-library-group-filter]") || [])].find(input => input.getAttribute("data-library-group-filter") === filterFocus)?.focus({preventScroll: true});
            const panel = container.querySelector("[data-library-filter-menu] .action-menu-items");
            if (filterOpen && panel) panel.scrollTop = filterScroll;
          });
        }
        const master = container.querySelector("[data-library-select-visible]");
        if (master) { master.checked = allChecked; master.indeterminate = mixed; }
        for (const input of container.querySelectorAll?.("[data-library-group-filter]") || []) {
          const id = input.dataset.libraryGroupFilter;
          input.checked = filters.has("*") || filters.has(id);
          input.indeterminate = id === "*" && !filters.has("*") && filters.size > 0;
        }
      }
      environment.KPLActionMenus?.refresh();
      // Position rebuilt popovers before restoring their scroll offsets: an
      // unconstrained panel has no scroll range and would clamp the offset to 0.
      for (const restore of restoreControls) restore();
    }

    function ensureDialog() {
      if (dialog || !document?.body) return;
      dialog = document.createElement("dialog");
      dialog.id = "libraryGroupDialog";
      dialog.className = "library-group-dialog";
      dialog.setAttribute("aria-labelledby", "libraryGroupTitle");
      dialog.innerHTML = '<div class="library-group-dialog-shell"><header><div><h2 id="libraryGroupTitle">Groups</h2><p class="helper">Shared by scenarios and saved results.</p></div><button class="icon-button" type="button" data-library-action="close" aria-label="Close groups"><svg viewBox="0 0 16 16" aria-hidden="true" focusable="false"><path d="M3 3l10 10M13 3L3 13"/></svg></button></header><form class="library-group-create" data-library-create><label for="libraryGroupName">New group</label><input id="libraryGroupName" name="name" maxlength="256" placeholder="Group name" autocomplete="off" required><button class="primary-button" type="submit">Create</button></form><p class="helper library-group-order-help">Use the arrows to change group order. Changes are saved immediately for scenarios and results; All groups and Ungrouped stay first.</p><div class="library-group-list"></div><div class="library-group-editor"></div><p class="library-group-dialog-status" aria-live="polite"></p><footer><button class="secondary-button" type="button" data-library-action="refresh">Refresh</button><button class="primary-button" type="button" data-library-action="close">Done</button></footer></div>';
      document.body.append(dialog);
      dialog.addEventListener("close", () => {
        editing = "";
        confirming = false;
        const focusTarget = returnFocus?.isConnected ? returnFocus : document.querySelector(returnScope + ' [data-library-action="manage"]');
        focusTarget?.focus({preventScroll: true});
        returnFocus = null;
        returnScope = "";
      });
    }

    function renderDialog(allItems) {
      if (!dialog?.open) return;
      const groups = dialog.querySelector(".library-group-list");
      const counts = new Map();
      for (const [key, id] of Object.entries(snapshot.memberships)) {
        const count = counts.get(id) || {scenarios: 0, results: 0};
        count[scopeFor(key)]++;
        counts.set(id, count);
      }
      const html = snapshot.groups.length ? snapshot.groups.map((group, index) => {
        const count = counts.get(group.id) || {scenarios: 0, results: 0};
        const move = (action, direction, arrow, edge) => '<button class="secondary-button library-group-order-button" type="button" data-library-action="' + action + '" data-library-group="' + escape(group.id) + '" title="Move ' + direction + '" aria-label="' + escape("Move " + group.name + " " + direction) + '"' + (ready() && !edge ? "" : " disabled") + '>' + arrow + '</button>';
        return '<div class="library-group-list-item" data-library-order-group="' + escape(group.id) + '"><div class="library-group-item-info"><strong>' + escape(group.name) + '</strong><span>' + count.scenarios + ' scenarios · ' + count.results + ' result sets</span></div><div class="library-group-row-actions">' + move("move-up", "up", "↑", index === 0) + move("move-down", "down", "↓", index === snapshot.groups.length - 1) + '<button class="secondary-button" type="button" data-library-action="edit" data-library-group="' + escape(group.id) + '"' + (ready() ? "" : " disabled") + '>Edit</button></div></div>';
      }).join("") : '<p class="helper">' + (loaded ? "No groups yet. Create a group to organize scenarios and results." : "Loading groups…") + "</p>";
      setMarkup(groups, html);
      const editor = dialog.querySelector(".library-group-editor");
      // Keep an unsaved name while requests and list refreshes update the dialog.
      const draft = editor.dataset.group === editing ? editor.querySelector("input")?.value : null;
      const name = getGroupName(editing);
      const editorHTML = !name ? "" : confirming
        ? '<section class="library-group-confirm"><h3>Delete ' + escape(name) + '?</h3><p>Only this group will be deleted. Its scenarios and results will remain saved and become Ungrouped.</p><div class="library-group-editor-actions"><button class="secondary-button" type="button" data-library-action="cancel-delete"' + (locked() ? " disabled" : "") + '>Cancel</button><button class="danger-button" type="button" data-library-action="confirm-delete"' + (ready() ? "" : " disabled") + '>Delete group</button></div></section>'
        : '<form data-library-rename><label for="libraryRenameGroup">Group name</label><input id="libraryRenameGroup" name="name" value="' + escape(draft ?? name) + '" maxlength="256" autocomplete="off" required><div class="library-group-editor-actions"><button class="secondary-button" type="button" data-library-action="cancel-edit"' + (locked() ? " disabled" : "") + '>Cancel</button><button class="secondary-button" type="submit"' + (ready() ? "" : " disabled") + '>Rename</button><button class="danger-button" type="button" data-library-action="delete"' + (ready() ? "" : " disabled") + '>Delete group</button></div></form>';
      const focusedInput = editor.contains(document.activeElement) && document.activeElement?.tagName === "INPUT";
      const selection = focusedInput ? [document.activeElement.selectionStart, document.activeElement.selectionEnd] : null;
      setMarkup(editor, editorHTML);
      editor.dataset.group = editing;
      if (focusedInput && editor.querySelector("input")) {
        const input = editor.querySelector("input");
        input.focus({preventScroll: true});
        if (selection) input.setSelectionRange(...selection);
      }
      const status = dialog.querySelector(".library-group-dialog-status");
      status.textContent = errorText || noticeText;
      status.dataset.error = String(Boolean(errorText));
      for (const control of dialog.querySelectorAll("[data-library-create] input, [data-library-create] button")) control.disabled = !ready();
      for (const control of dialog.querySelectorAll('[data-library-action="refresh"]')) control.disabled = locked();
    }

    function refreshUI() {
      const allItems = items();
      const available = new Set(allItems.map(item => item.key));
      const hadSelection = selected.size > 0;
      for (const key of selected) if (!available.has(key)) selected.delete(key);
      if (hadSelection && !selected.size) selectionScope = "";
      renderBars(allItems);
      renderDialog(allItems);
      for (const input of document?.querySelectorAll?.("[data-library-select-key]") || []) {
        input.disabled = !ready();
        input.checked = selected.has(input.dataset.librarySelectKey);
      }
    }

    function showError(error) {
      errorText = error.message || "Could not update groups.";
      refreshUI();
    }

    async function click(event) {
      const button = event.target.closest?.("[data-library-action]");
      if (!button || button.disabled) return;
      event.preventDefault();
      const action = button.dataset.libraryAction;
      try {
        if (action === "cancel-download") {
          analysisDownloadController?.abort();
          return;
        }
        if (locked() && ["clear", "download", "download-csv", "download-json", "delete-selected", "delete-failed-selected", "analyze-selected", "retry-selected", "move"].includes(action)) return;
        if (action === "manage") {
          ensureDialog();
          if (!dialog) return;
          returnFocus = button;
          returnScope = button.closest("#scenarioGroupTools") ? "#scenarioGroupTools" : "#resultGroupTools";
          dialog.showModal();
          renderDialog(items());
          if (!loaded || needsRefresh) await refresh();
        } else if (action === "close") {
          dialog?.close();
        } else if (action === "refresh") {
          noticeText = "";
          await refresh();
        } else if (action === "clear") {
          selected.clear();
          selectionScope = "";
          refreshUI();
          options?.onSelectionChanged?.();
        } else if (action === "move-up" || action === "move-down") {
          if (!ready()) return;
          try { await moveGroup(button.dataset.libraryGroup, action === "move-up" ? -1 : 1); }
          finally { focusOrderControl(button.dataset.libraryGroup, action); }
        } else if (action === "delete-selected") {
          if (selected.size && ready()) options?.onDeleteSelected?.(selectionScope, [...selected]);
        } else if (action === "delete-failed-selected") {
          if (selectionScope === "results" && selected.size && ready()) options?.onDeleteFailedSelected?.([...selected]);
        } else if (action === "analyze-selected" || action === "retry-selected") {
          const operation = action === "analyze-selected" ? options?.onAnalyzeSelected : options?.onRetrySelected;
          if (selectionScope !== "results" || !selected.size || !ready() || !operation) return;
          const keys = [...selected];
          busy = true;
          errorText = "";
          noticeText = action === "analyze-selected" ? "Queueing selected analyses…" : "Queueing unfinished runs…";
          options?.onSelectionChanged?.();
          refreshUI();
          try {
            const report = await operation(keys);
            noticeText = typeof report === "string" ? report : "Selected work was submitted. Check the execution queue.";
          } finally {
            busy = false;
            options?.onSelectionChanged?.();
            refreshUI();
            document?.querySelector("#resultGroupTools")?.querySelector('[data-action-menu-toggle="library-download:results"]')?.focus({preventScroll:true});
          }
        } else if (action === "download" || action === "download-csv" || action === "download-json") {
          const csv = action === "download-csv";
          const json = action === "download-json", analysis = csv || json;
          const download = json ? options?.downloadJSON : csv ? options?.downloadCSV : options?.download;
          if (!selected.size || !ready() || !download || analysis && selectionScope !== "results") return;
          const keys = [...selected], scope = selectionScope;
          if (analysis) analysisDownloadController = new AbortController();
          busy = true;
          options?.onSelectionChanged?.();
          errorText = "";
          noticeText = "Preparing download…";
          refreshUI();
          if (analysis) document?.querySelector("#resultGroupTools")
            ?.querySelector('[data-library-action="cancel-download"]')?.focus({preventScroll:true});
          try {
            await download(scope, keys, analysis ? {signal:analysisDownloadController.signal, onProgress:message => {
              noticeText = message;
              renderBars(items());
            }} : undefined);
            noticeText = "Download requested. Check your browser's downloads.";
          } catch (error) {
            if (analysis && error.name === "AbortError") noticeText = `${json ? "JSON" : "CSV"} download canceled. Accepted analysis continues on the server.`;
            else throw error;
          } finally {
            analysisDownloadController = null;
            busy = false;
            options?.onSelectionChanged?.();
            refreshUI();
            document?.querySelector(scope === "results" ? "#resultGroupTools" : "#scenarioGroupTools")
              ?.querySelector('[data-action-menu-toggle="library-download:' + scope + '"]')?.focus({preventScroll:true});
          }
        } else if (action === "move") {
          await assign([...selected], targetGroup);
          selected.clear();
          selectionScope = "";
          refreshUI();
          changed();
        } else if (action === "edit") {
          editing = button.dataset.libraryGroup;
          confirming = false;
          renderDialog(items());
          dialog.querySelector("#libraryRenameGroup")?.focus();
        } else if (action === "cancel-edit") {
          editing = "";
          confirming = false;
          renderDialog(items());
        } else if (action === "delete" || action === "cancel-delete") {
          confirming = action === "delete";
          renderDialog(items());
          dialog.querySelector('[data-library-action="' + (confirming ? "cancel-delete" : "delete") + '"]')?.focus();
        } else if (action === "confirm-delete") {
          await mutate(endpoint + "/" + encodeURIComponent(editing), "DELETE", {}, "Group deleted. Its scenarios and results are now Ungrouped.");
          editing = "";
          confirming = false;
          renderDialog(items());
          dialog.querySelector("#libraryGroupName")?.focus();
        }
      } catch (error) { showError(error); }
    }

    async function submit(event) {
      const create = event.target.matches?.("[data-library-create]");
      if (!create && !event.target.matches?.("[data-library-rename]")) return;
      event.preventDefault();
      const input = event.target.querySelector('input[name="name"]'), name = input.value.trim();
      try {
        if (!name || [...name].length > 128 || /[\u0000-\u001f\u007f-\u009f]/.test(name)) throw new Error("Use a group name with 1–128 characters and no control characters.");
        await mutate(create ? endpoint : endpoint + "/" + encodeURIComponent(editing), create ? "POST" : "PATCH", {name}, create ? "Group created." : "Group renamed.");
        if (create) input.value = "";
        else { editing = ""; confirming = false; }
        renderDialog(items());
        dialog.querySelector("#libraryGroupName")?.focus();
      } catch (error) { showError(error); }
    }

    function init(configuration) {
      options = configuration;
      if (!initialized) {
        initialized = true;
        document?.addEventListener("click", event => {
          if (event.target.closest?.("[data-library-select]") && event.target.closest?.("summary")) event.stopPropagation();
        }, true);
        document?.addEventListener("click", event => { void click(event); });
        document?.addEventListener("submit", event => { void submit(event); });
        document?.addEventListener("change", event => {
          const element = event.target;
          if (element.matches?.("[data-library-group-filter]")) {
            if (!loaded || locked()) { refreshUI(); return; }
            checkFilter(element.dataset.libraryGroupFilter, element.checked);
          } else if (element.matches?.("[data-library-select-visible]")) {
            if (!ready()) { refreshUI(); return; }
            try { checkVisible(element.dataset.libraryScope, element.checked); }
            catch (error) { element.checked = false; showError(error); }
          }
          else if (element.matches?.("[data-library-move-target]")) targetGroup = element.value;
          else if (element.matches?.("[data-library-select-key]")) {
            const key = element.dataset.librarySelectKey;
            if (!ready() || !items().some(item => item.key === key) || !matches(key)) { element.checked = selected.has(key); return; }
            if (!element.checked && selectionScope !== scopeFor(key)) return;
            if (selectionScope !== scopeFor(key)) { selected.clear(); selectionScope = scopeFor(key); targetGroup = importGroup(); }
            if (element.checked) {
              if (selected.size >= maxSelection) { element.checked = false; showError(new Error("Select up to 2,000 items at a time.")); return; }
              selected.add(key);
            } else selected.delete(key);
            if (!selected.size) selectionScope = "";
            refreshUI();
            options?.onSelectionChanged?.();
          }
        });
      }
      refreshUI();
      return refresh().catch(() => {});
    }

    return {init, refresh, refreshUI, matches, selectionMarkup, badgeMarkup, getGroupName, assign, reorder, setFilter,
      forgetSelection: keys => { for (const key of keys) selected.delete(key); if (!selected.size) selectionScope = ""; refreshUI(); changed(); },
      getSelection: scope => selectionScope === scope ? [...selected] : null, isBusy: locked,
      isFiltered: () => !filters.has("*"), getImportGroup: importGroup};
  }

  if (typeof module !== "undefined" && module.exports) module.exports = {createLibraryGroups};
  else root.KPLLibraryGroups = createLibraryGroups();
})(typeof globalThis !== "undefined" ? globalThis : this);

(function (root) {
  "use strict";

  const endpoint = "/api/v1/library-groups", maxSelection = 2000;
  const escape = value => String(value ?? "").replace(/[&<>"']/g, char => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"})[char]);
  const scopeFor = key => String(key).startsWith("scenario:") ? "scenarios" : "results";

  function createLibraryGroups(environment = root) {
    const document = environment.document;
    let options, initialized = false, snapshot = {revision: "0", groups: [], memberships: {}}, loaded = false;
    let filter = "*", selectionScope = "", selected = new Set(), targetGroup = "";
    let readTask = null, mutationTask = null, epoch = 0, busy = false, needsRefresh = false;
    let errorText = "", noticeText = "", dialog = null, returnFocus = null, returnScope = "", editing = "", confirming = false;
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
    function matches(key) { return filter === "*" || groupFor(key) === filter; }
    function ready() { return loaded && !busy && !needsRefresh; }
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
      if (filter !== "*" && filter && !getGroupName(filter)) filter = "";
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

    function refresh() {
      if (mutationTask) return mutationTask.then(() => snapshot);
      return loadSnapshot();
    }

    function mutate(path, method, body, successMessage) {
      if (!ready()) return Promise.reject(new Error(busy ? "A group update is still in progress." : "Refresh groups before making changes."));
      busy = true;
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
      return mutate(endpoint + "/members", "PUT", {groupId, keys: unique}, "Items moved to " + (getGroupName(groupId) || "Ungrouped") + ".");
    }

    function setFilter(id) {
      if (id !== "*" && id && !getGroupName(id)) return;
      if (filter === id) return;
      filter = id;
      selected.clear();
      errorText = "";
      noticeText = "";
      refreshUI();
      changed();
    }

    function selectionMarkup(key, label) {
      if (selectionScope !== scopeFor(key)) return "";
      return '<label class="library-item-select" data-library-select title="Select item"><input type="checkbox" data-library-select-key="' + escape(key) + '" aria-label="Select ' + escape(label) + '"' + (selected.has(key) ? " checked" : "") + (busy ? " disabled" : "") + '></label>';
    }

    function badgeMarkup(key) {
      const name = getGroupName(groupFor(key));
      return name ? '<span class="library-group-badge" title="Group: ' + escape(name) + '">' + escape(name) + "</span>" : "";
    }

    function groupOptions(value, {all = false, counts = null} = {}) {
      const option = (id, name) => '<option value="' + escape(id) + '"' + (value === id ? " selected" : "") + ">" + escape(name) + (counts ? " (" + (counts.get(id) || 0) + ")" : "") + "</option>";
      return (all ? option("*", "All groups") : "") + option("", "Ungrouped") + snapshot.groups.map(group => option(group.id, group.name)).join("");
    }

    function renderBars(allItems) {
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
        const disabled = ready() ? "" : " disabled";
        const html = '<div class="library-group-toolbar"><label class="library-group-filter"><span>Group</span><select data-library-filter aria-label="Filter ' + scope + ' by group"' + (loaded && !busy ? "" : " disabled") + '>' + groupOptions(filter, {all: true, counts}) + '</select></label><button class="secondary-button" type="button" data-library-action="manage">Groups</button><button class="secondary-button" type="button" data-library-action="select" data-library-scope="' + scope + '" aria-pressed="' + selecting + '"' + disabled + '>' + (selecting ? "Done" : "Select") + '</button></div>' +
          (selecting ? '<div class="library-group-selection"><div class="library-selection-count"><span aria-live="polite">' + selected.size + ' selected</span><button class="library-text-button" type="button" data-library-action="select-visible" data-library-scope="' + scope + '"' + disabled + '>Select visible</button><button class="library-text-button" type="button" data-library-action="clear"' + (selected.size && !busy ? "" : " disabled") + '>Clear</button></div><label class="visually-hidden" for="libraryMove-' + scope + '">Move selected items to group</label><select id="libraryMove-' + scope + '" data-library-move-target' + disabled + '>' + groupOptions(targetGroup) + '</select><button class="secondary-button" type="button" data-library-action="move"' + (selected.size && ready() ? "" : " disabled") + '>Move</button></div>' : "") +
          (errorText ? '<p class="library-group-status" role="alert">' + escape(errorText) + ' <button class="library-text-button" type="button" data-library-action="refresh"' + (busy ? " disabled" : "") + '>Refresh groups</button></p>' : noticeText ? '<p class="library-group-status" role="status">' + escape(noticeText) + "</p>" : !loaded ? '<p class="library-group-status" role="status">Loading groups…</p>' : "");
        const active = document.activeElement;
        const focusKind = container.contains(active) ? active?.hasAttribute("data-library-filter") ? "[data-library-filter]" : active?.hasAttribute("data-library-move-target") ? "[data-library-move-target]" : active?.dataset?.libraryAction ? '[data-library-action="' + active.dataset.libraryAction + '"]' : "" : "";
        if (setMarkup(container, html)) {
          if (focusKind) container.querySelector(focusKind)?.focus({preventScroll: true});
        }
      }
    }

    function ensureDialog() {
      if (dialog || !document?.body) return;
      dialog = document.createElement("dialog");
      dialog.id = "libraryGroupDialog";
      dialog.className = "library-group-dialog";
      dialog.setAttribute("aria-labelledby", "libraryGroupTitle");
      dialog.innerHTML = '<div class="library-group-dialog-shell"><header><div><h2 id="libraryGroupTitle">Groups</h2><p class="helper">Shared by scenarios and saved results.</p></div><button class="icon-button" type="button" data-library-action="close" aria-label="Close groups">×</button></header><form class="library-group-create" data-library-create><label for="libraryGroupName">New group</label><input id="libraryGroupName" name="name" maxlength="256" placeholder="Group name" autocomplete="off" required><button class="primary-button" type="submit">Create</button></form><div class="library-group-list"></div><div class="library-group-editor"></div><p class="library-group-dialog-status" aria-live="polite"></p><footer><button class="secondary-button" type="button" data-library-action="refresh">Refresh</button><button class="primary-button" type="button" data-library-action="close">Done</button></footer></div>';
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
      const html = snapshot.groups.length ? snapshot.groups.map(group => {
        const count = counts.get(group.id) || {scenarios: 0, results: 0};
        return '<div class="library-group-list-item"><div><strong>' + escape(group.name) + '</strong><span>' + count.scenarios + ' scenarios · ' + count.results + ' result sets</span></div><button class="secondary-button" type="button" data-library-action="edit" data-library-group="' + escape(group.id) + '"' + (ready() ? "" : " disabled") + '>Edit</button></div>';
      }).join("") : '<p class="helper">' + (loaded ? "No groups yet. Create a group to organize scenarios and results." : "Loading groups…") + "</p>";
      setMarkup(groups, html);
      const editor = dialog.querySelector(".library-group-editor");
      // Keep an unsaved name while requests and list refreshes update the dialog.
      const draft = editor.dataset.group === editing ? editor.querySelector("input")?.value : null;
      const name = getGroupName(editing);
      const editorHTML = !name ? "" : confirming
        ? '<section class="library-group-confirm"><h3>Delete ' + escape(name) + '?</h3><p>Only this group will be deleted. Its scenarios and results will remain saved and become Ungrouped.</p><div class="library-group-editor-actions"><button class="secondary-button" type="button" data-library-action="cancel-delete"' + (busy ? " disabled" : "") + '>Cancel</button><button class="danger-button" type="button" data-library-action="confirm-delete"' + (ready() ? "" : " disabled") + '>Delete group</button></div></section>'
        : '<form data-library-rename><label for="libraryRenameGroup">Group name</label><input id="libraryRenameGroup" name="name" value="' + escape(draft ?? name) + '" maxlength="256" autocomplete="off" required><div class="library-group-editor-actions"><button class="secondary-button" type="button" data-library-action="cancel-edit"' + (busy ? " disabled" : "") + '>Cancel</button><button class="secondary-button" type="submit"' + (ready() ? "" : " disabled") + '>Rename</button><button class="danger-button" type="button" data-library-action="delete"' + (ready() ? "" : " disabled") + '>Delete group</button></div></form>';
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
      for (const control of dialog.querySelectorAll('[data-library-action="refresh"]')) control.disabled = busy;
    }

    function refreshUI() {
      const allItems = items();
      const available = new Set(allItems.map(item => item.key));
      for (const key of selected) if (!available.has(key)) selected.delete(key);
      renderBars(allItems);
      renderDialog(allItems);
      for (const input of document?.querySelectorAll?.("[data-library-select-key]") || []) {
        input.disabled = busy;
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
        } else if (action === "select") {
          selectionScope = selectionScope === button.dataset.libraryScope ? "" : button.dataset.libraryScope;
          selected.clear();
          targetGroup = filter === "*" ? "" : filter;
          refreshUI();
          changed();
        } else if (action === "clear") {
          selected.clear();
          refreshUI();
          changed();
        } else if (action === "select-visible") {
          const scope = button.dataset.libraryScope;
          const valid = new Set(items().filter(item => item.scope === scope && matches(item.key)).map(item => item.key));
          const keys = [...new Set(options.getVisibleKeys?.(scope) || [...valid])].filter(key => valid.has(key));
          if (keys.length > maxSelection) throw new Error("Select up to 2,000 items at a time. Narrow the list or select fewer items.");
          selected = new Set(keys);
          refreshUI();
          changed();
        } else if (action === "move") {
          await assign([...selected], targetGroup);
          selected.clear();
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
          if (event.target.closest?.("[data-library-select]")) event.stopPropagation();
        }, true);
        document?.addEventListener("click", event => { void click(event); });
        document?.addEventListener("submit", event => { void submit(event); });
        document?.addEventListener("change", event => {
          const element = event.target;
          if (element.matches?.("[data-library-filter]")) setFilter(element.value);
          else if (element.matches?.("[data-library-move-target]")) targetGroup = element.value;
          else if (element.matches?.("[data-library-select-key]")) {
            const key = element.dataset.librarySelectKey;
            if (busy || selectionScope !== scopeFor(key)) { element.checked = selected.has(key); return; }
            if (element.checked) {
              if (selected.size >= maxSelection) { element.checked = false; showError(new Error("Select up to 2,000 items at a time.")); return; }
              selected.add(key);
            } else selected.delete(key);
            renderBars(items());
          }
        });
      }
      refreshUI();
      return refresh().catch(() => {});
    }

    return {init, refresh, refreshUI, matches, selectionMarkup, badgeMarkup, getGroupName, assign, setFilter,
      isFiltered: () => filter !== "*", getImportGroup: () => loaded && filter !== "*" ? filter : ""};
  }

  if (typeof module !== "undefined" && module.exports) module.exports = {createLibraryGroups};
  else root.KPLLibraryGroups = createLibraryGroups();
})(typeof globalThis !== "undefined" ? globalThis : this);

/* Persistent notes are loaded on demand; result lists contain only previews. */
(function (root) {
  "use strict";
  const limit = 16 * 1024;

  function createEditor({ document, api, onSaved = () => {}, formatTime = value => value }) {
    const $ = selector => document.querySelector(selector);
    const dialog = $("#resultNoteDialog");
    const input = $("#resultNoteText");
    const saveButton = $("#saveResultNote");
    const reloadButton = $("#reloadResultNote");
    const cancelButton = $("#cancelResultNote");
    let run = null, original = "", revision = "", loaded = false, busy = "", conflict = false;
    let request = null, generation = 0;
    const bytes = () => new TextEncoder().encode(input.value).length;
    const dirty = () => loaded && input.value !== original;

    function render() {
      input.disabled = !loaded || !!busy;
      saveButton.disabled = !loaded || !!busy || conflict || !dirty() || bytes() > limit;
      saveButton.textContent = busy === "save" ? "Saving…" : "Save note";
      cancelButton.disabled = busy === "save";
      reloadButton.disabled = !!busy;
      $("#resultNoteCount").textContent = `${bytes().toLocaleString("en-US")} / ${limit.toLocaleString("en-US")} bytes${bytes() > limit ? " — note is too long" : ""}`;
      input.setAttribute("aria-invalid", String(bytes() > limit));
    }

    async function perform(method, body) {
      const controller = new AbortController();
      request = controller;
      const timer = setTimeout(() => controller.abort(), 30000);
      try {
        return await api(`/api/v1/results/${encodeURIComponent(run.id)}/note`, {
          method, signal: controller.signal,
          ...(body ? { headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) } : {}),
        });
      } finally {
        clearTimeout(timer);
        if (request === controller) request = null;
      }
    }

    async function load(preserveDraft = false) {
      const current = generation;
      busy = "load";
      $("#resultNoteError").textContent = "";
      $("#resultNoteStatus").textContent = "Loading note…";
      reloadButton.hidden = true;
      render();
      try {
        const note = await perform("GET");
        if (current !== generation) return;
        original = note.text;
        revision = note.revision;
        loaded = true;
        conflict = false;
        if (preserveDraft) {
          $("#latestResultNote").value = note.text;
          $("#resultNoteConflict").hidden = false;
        } else {
          input.value = note.text;
          input.setSelectionRange(0, 0);
          input.scrollTop = 0;
        }
        $("#resultNoteStatus").textContent = preserveDraft
          ? "Your draft is unchanged. Review the latest saved note below, then save to replace it."
          : note.updatedAt ? `Last saved ${formatTime(note.updatedAt)}` : "No note yet. Changes are saved when you select Save note.";
      } catch (error) {
        if (current !== generation) return;
        $("#resultNoteStatus").textContent = "";
        $("#resultNoteError").textContent = error.name === "AbortError" ? "Loading timed out. Try again." : error.message;
        reloadButton.textContent = preserveDraft ? "Review latest note" : "Retry loading";
        reloadButton.hidden = false;
      } finally {
        if (current === generation) {
          busy = "";
          render();
          if (loaded) input.focus();
        }
      }
    }

    async function save() {
      if (saveButton.disabled || !run) return;
      const current = generation;
      busy = "save";
      $("#resultNoteError").textContent = "";
      render();
      try {
        const note = await perform("PUT", { text: input.value, revision });
        if (current !== generation) return;
        original = note.text;
        input.value = note.text;
        busy = "";
        dialog.close();
        onSaved(note);
      } catch (error) {
        if (current !== generation) return;
        conflict = error.status === 409 || error.name === "AbortError";
        $("#resultNoteError").textContent = error.name === "AbortError"
          ? "Saving timed out and may have completed. Review the latest note before saving again. Your draft is still here."
          : error.status === 409 ? "This note changed in another window. Review the latest note before saving again. Your draft is still here." : error.message;
        if (conflict) {
          reloadButton.textContent = "Review latest note";
          reloadButton.hidden = false;
        }
      } finally {
        if (current === generation) { busy = ""; render(); }
      }
    }

    async function open(result) {
      if (!result || dialog.open) return;
      generation++;
      run = result;
      original = "";
      revision = "";
      loaded = false;
      conflict = false;
      input.value = "";
      $("#resultNoteName").textContent = result.name || result.id;
      $("#resultNoteID").textContent = result.id;
      $("#resultNoteConflict").hidden = true;
      $("#latestResultNote").value = "";
      dialog.showModal();
      await load();
    }

    input.addEventListener("input", render);
    $("#resultNoteForm").addEventListener("submit", event => { event.preventDefault(); void save(); });
    reloadButton.addEventListener("click", () => { if (!busy) void load(loaded); });
    cancelButton.addEventListener("click", () => { if (busy !== "save") dialog.close(); });
    dialog.addEventListener("cancel", event => { if (busy === "save") event.preventDefault(); });
    dialog.addEventListener("close", () => { generation++; request?.abort(); request = null; run = null; loaded = false; busy = ""; });
    document.defaultView?.addEventListener("beforeunload", event => {
      if (dialog.open && dirty()) { event.preventDefault(); event.returnValue = ""; }
    });
    return { open, save };
  }

  let editor;
  const exported = { createEditor, init: options => { editor = createEditor({ document: root.document, ...options }); }, open: run => editor?.open(run) };
  if (typeof module !== "undefined" && module.exports) module.exports = exported;
  else root.KPLResultNotes = exported;
})(typeof globalThis !== "undefined" ? globalThis : this);

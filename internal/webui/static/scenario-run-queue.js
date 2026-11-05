(function (root) {
  "use strict";

  const storageKey = "kpl.pending-scenario-runs";

  function createQueue({request, storage, createKey = () => root.crypto?.randomUUID?.()
    || `request-${Date.now()}-${Math.random().toString(36).slice(2)}`}) {
    let pending = null, active = false;

    function save() {
      try { storage?.setItem(storageKey, JSON.stringify(pending)); } catch {}
    }

    function restore(signature, ids, repetitions) {
      let value;
      try { value = JSON.parse(storage?.getItem(storageKey)); } catch { return null; }
      if (value?.version !== 1 || value.signature !== signature || value.repetitions !== repetitions
          || !Array.isArray(value.entries) || value.entries.length !== ids.length
          || !Number.isInteger(value.next) || value.next < 0 || value.next > ids.length) return null;
      const remaining = new Set(ids);
      const keys = new Set();
      for (const entry of value.entries) {
        if (!entry || !remaining.delete(entry.id) || typeof entry.name !== "string"
            || typeof entry.key !== "string" || !entry.key || keys.has(entry.key)
            || entry.key.length > 128 || /[\r\n\0]/.test(entry.key)) return null;
        keys.add(entry.key);
      }
      if (value.payload != null) {
        let payload;
        try { payload = JSON.parse(value.payload); } catch { return null; }
        if (!payload || value.next >= ids.length || payload.scenarioId !== value.entries[value.next].id
            || payload.repetitions !== repetitions || typeof payload.scenario !== "string" || !payload.scenario.trim()) return null;
      }
      return value;
    }

    async function submit(scenarios, repetitions, onProgress = () => {}) {
      if (active) throw new Error("Scenario registration is already in progress.");
      if (!Array.isArray(scenarios) || !scenarios.length || scenarios.length > 2000
          || !Number.isInteger(repetitions) || repetitions < 1 || repetitions > 100) {
        throw new Error("Select 1–2,000 saved scenarios and 1–100 runs per scenario.");
      }
      const ids = scenarios.map(item => item?.id);
      if (ids.some(id => typeof id !== "string" || !id) || new Set(ids).size !== ids.length) {
        throw new Error("Refresh the saved scenario list and select the scenarios again.");
      }
      const signature = JSON.stringify({ids: [...ids].sort(), repetitions});
      if (pending?.signature !== signature) {
        pending = restore(signature, ids, repetitions) || {version: 1, signature, repetitions, next: 0,
          entries: scenarios.map(item => ({id: item.id, name: item.name || item.id, key: createKey()}))};
      }
      active = true;
      save();
      try {
        while (pending.next < pending.entries.length) {
          const entry = pending.entries[pending.next];
          onProgress({accepted: pending.next, total: pending.entries.length, name: entry.name});
          if (!pending.payload) {
            const scenario = await request("/api/v1/scenarios/" + encodeURIComponent(entry.id));
            if (scenario?.id !== entry.id || typeof scenario.yaml !== "string" || !scenario.yaml.trim()) {
              throw new Error("Unexpected saved scenario response.");
            }
            pending.payload = JSON.stringify({scenario: scenario.yaml, repetitions, scenarioId: entry.id});
            save();
          }
          const run = await request("/api/v1/experiments", {method: "POST",
            headers: {"Content-Type": "application/json", "Idempotency-Key": entry.key}, body: pending.payload}, "run");
          if (typeof run?.id !== "string" || !run.id) throw new Error("Unexpected experiment response. Retry with the same selection and Runs.");
          pending.next++;
          delete pending.payload;
          save();
        }
        const result = {scenarios: pending.entries.length, runs: pending.entries.length * repetitions};
        pending = null;
        try { storage?.removeItem(storageKey); } catch {}
        return result;
      } catch (error) {
        const entry = pending.entries[pending.next];
        throw new Error(`Confirmed ${pending.next}/${pending.entries.length} scenarios (${pending.next * repetitions} runs). Registration stopped at ${entry.name}: ${error.message} Keep the selection and Runs unchanged and choose Run selected to retry. Already queued runs will continue.`);
      } finally { active = false; }
    }

    return {submit};
  }

  if (typeof module !== "undefined" && module.exports) module.exports = {createQueue};
  else root.KPLScenarioRunQueue = {createQueue};
})(typeof globalThis !== "undefined" ? globalThis : this);

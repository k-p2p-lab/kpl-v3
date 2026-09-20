# Scenario Library

English | [Korean](scenario-library.kr.md)

The Dashboard stores reusable, validated YAML scenarios on the Controller. Library records are independent of experiment results: saving a scenario does not run it, running edited YAML does not save it automatically, and deleting either record does not delete the other.

## Use the library in the Dashboard

1. Open **Run experiment**. The **Saved scenarios** list is loaded from the current Controller.
2. Enter a descriptive **Saved scenario name** and edit the YAML.
3. Choose **Save scenario**. The saved item becomes the selected record.
4. Choose **Load** on a list item to replace the name and YAML in the editor.
5. Edit either the name or YAML and choose **Save changes** to update the selected record. Choose **Save as new** to keep the original and create another record.
6. Choose **Delete**, then **Confirm delete**, to remove a record. **Refresh** reloads the list from the Controller.

**New** resets the editor to the built-in example and clears the selected record. Loading another item or starting a new one replaces unsaved editor changes without a recovery copy. The **Run** button always executes the YAML currently in the editor and can run it 1–100 times whether it has been saved or not.

Names are trimmed, required, limited to 128 Unicode characters, and cannot contain control characters. YAML is required, limited to 1 MiB, and must pass normal scenario validation before it is stored. Names do not have to be unique; each record receives its own generated ID. The list is ordered by most recent update.

The library name and YAML's top-level `name` are independent: the former labels the saved editor input, while the latter names experiment runs. Saving validates the [scenario schema](scenario-reference.md); it does not reserve Agent capacity or test Docker, kernel support, or connectivity.

## Validate editor contents

Click **Validate** beside **YAML scenario** to check the current text with the same parser used for saving and running. The panel below the editor shows success with the scenario name and phase count, or the reason validation failed. YAML diagnostics include line numbers when available; configuration errors identify the relevant field or phase. Long errors can be scrolled and copied.

Validation reuses the login session, requires no saved scenario name, and does not save or start anything. YAML must contain a single document and fit within 1 MiB. Editing the YAML, choosing **New**, or loading another scenario clears the previous result; late responses for older text are ignored. Request/network failures are distinguished from invalid YAML. A successful check does not test Agent capacity, Docker support, or runtime connectivity.

## Estimated finish times

**Experiment progress** shows **Est. finish** and approximate remaining time for each running or queued Run. A summary above the cards shows **Group est. finish** for each active repeated series. Times use the browser's local time zone; remaining time is calculated by the Controller, including earlier Runs in the same group's queue. Separate groups execute independently.

The initial **Scenario estimate** combines waits, repeats and sampled intervals while accounting for overlapping background jobs, `wait-jobs`, `stop-all`, and `onExit`. Readiness barriers initially use their configured timeout allowance. Request counts use the configured maxima; actual eligible publishers/leavers may be fewer. Node startup, capacity waits, request processing and cleanup are not known in advance. As phases finish, their actual boundaries replace predictions. Completed successful Runs in the same active group supply average phase and total times, including cleanup, for later estimates; **Based on N completed runs** identifies this correction.

These are estimates, not deadlines. **Estimating…** means there is not enough timing information. If a Run exceeds its estimate, **Taking longer than estimated** appears and later queued Run/group estimates move forward. Stopping or finishing a Run removes its forecast. Estimates do not schedule an automatic stop and are not retained in saved results.

## REST API

All request and response bodies use JSON. List responses omit YAML so opening a large library remains inexpensive; fetch an individual record before editing it.

| Method | Path | Body | Success |
|---|---|---|---|
| `GET` | `/api/v1/scenarios` | — | Summary list: `id`, `name`, `createdAt`, `updatedAt` |
| `POST` | `/api/v1/scenarios` | `{ "name": "…", "yaml": "…" }` | `201` with the complete record |
| `GET` | `/api/v1/scenarios/{id}` | — | Complete record including `yaml` |
| `PUT` | `/api/v1/scenarios/{id}` | `{ "name": "…", "yaml": "…" }` | `200` with the updated record |
| `DELETE` | `/api/v1/scenarios/{id}` | — | `204` with no body |

Reads, validation, and mutations require a login session. Mutations also require `X-KPL-Request: dashboard`, which the UI supplies automatically. Invalid input, including decoded YAML over 1 MiB, returns `400`. The JSON request envelope allows `6 * 1 MiB + 64 KiB` to accommodate escaped characters; exceeding that envelope returns `413`. Unknown JSON fields and trailing JSON values are rejected. A missing or invalid ID returns `404`; storage errors return `500`. IDs contain 32 lowercase hexadecimal characters.

Replace `control-node:8080` with the Controller address printed by `access`. First follow [API authentication](api.md#authentication) to create `KPL_COOKIE_JAR`.

```sh
curl --fail http://control-node:8080/api/v1/scenarios

curl --fail -X POST http://control-node:8080/api/v1/scenarios \
  -H 'Content-Type: application/json' \
  -b "${KPL_COOKIE_JAR:?Log in first}" -H 'X-KPL-Request: dashboard' \
  --data-binary @- <<'JSON'
{"name":"Smoke baseline","yaml":"version: 2\nname: smoke-baseline\nphases:\n  - action: stop-all\n"}
JSON
```

## Storage and backup

Records are stored as individual JSON files under `<data-dir>/scenarios`. Swarm mounts the Controller's persistent data volume at `/var/lib/kpl/data`, so ordinary service restarts and `scripts/swarm.sh remove` preserve the library. Back up the entire Controller data directory to keep both the scenario library and experiment results. The library is local to that Controller and is not replicated between control nodes.

The Controller writes each record through a temporary file and atomic filesystem operation. It rejects malformed or unexpected record content when listing or loading the library rather than returning a partially trusted scenario.

Storage, payload limits, and validation are implemented in [`internal/controller/scenarios.go`](../internal/controller/scenarios.go); the editor workflow is in [`internal/webui/static/app.js`](../internal/webui/static/app.js).

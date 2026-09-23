# Dashboard Guide

English | [Korean](dashboard.kr.md)

[Documentation index](README.md) · [Repository](../README.md)

Open the Controller address from `sh scripts/swarm.sh access` and log in with the configured account. This guide covers live panels and controls. Use [experiment execution and recovery](experiments.md) to run work, [saved results](results.md) to preserve it, and [topology](topology.md) to interpret nodes and edges.

## Built-in Dashboard visualization

Use **Network / Experiments / Agents / Results** beneath the page heading to jump to a panel. A collapsed target opens automatically. Keyboard users can use **Skip to dashboard** to move past the header.

**Metrics** places all 11 cluster, delivery, bandwidth and observation cards in one horizontal row. Scroll sideways or use the **Previous metrics / Next metrics** arrow buttons. Hover over a card to expand that card horizontally and reveal its detailed values and explanation; only one card expands at a time. Long details scroll vertically within the expanded card. On narrow mobile screens, expanded details appear below the metric value to keep the text readable.

- Click or tap a card to open its details, and select it again to close them. On touch screens, swipe horizontally to move through the row.
- Keyboard focus opens a card. Use Left/Right to move between cards and Home/End for the first/last card. Enter/Space toggles details; Escape or a click/tap outside the row closes them.
- **How delivery is measured** remains directly below the row. Its explanation and expanded state persist through live updates.

**Online Agents** and **Ready Peers** describe the whole cluster; message and bandwidth cards follow the run shown by **Run metrics**. **Observation quality** shows receipt, continuity and start unknown counts separately as `Receipt / Continuity / Start`; these overlapping categories are not summed. Expand it for the full labels and outcome counts.

When a run finishes, fails or is canceled, its carousel measurements reset to the initial N/A/zero values. All cards remain available; the next running run supplies new values. Cluster counts stay live, and final measurements remain in **Saved results**.

**Available slots** totals the free Peer capacity reported by online Agents; offline Agents contribute no available slots. Capacity is an admission count, not a CPU or memory reservation. See [Peer placement and capacity](agents.md) before increasing it.

**Saved results → Images** submits server background analysis and provides overview, message and repeated-run charts as PNG/CSV/ZIP. It uses saved records independently of Prometheus retention. See [result images](visualization.md) for operation and [experiment metrics](experiment-metrics.md#saved-result-research-metrics) for definitions.

### Show and hide panels

Use the header's **Collapse / Expand** button on **Network overview**, **Experiment progress**, **Agent status**, **Recent network events**, or **Saved results**. Narrow screens show an arrow icon. The buttons support click, tap, and keyboard Enter/Space. All five panels start open; this browser remembers your choices for the Dashboard origin after reload.

**Network overview** and **Experiment progress** remain stacked at full width. On wide screens, collapsing either **Agent status** or **Recent network events** puts its compact header above the other panel, which fills the available width. Reopening both restores the Agent/events columns. Collapsing both leaves two compact headers side by side; mobile layouts remain vertical.

Experiments, background analysis and live data updates continue while a panel is hidden. Collapsing **Network overview** also stops its layout animation; reopening displays the latest state and preserves your **Pause motion** choice. These controls change only panel visibility.

## Login and live updates

A session expiry opens **Login required**; log in again to return to the Dashboard. Closing or hiding the page stops its live connection, while accepted experiments and analysis continue on the Controller. Returning to a visible page synchronizes the current state. **Reconnecting** describes the browser connection, not the experiment outcome.

Use [stream diagnosis](monitoring.md#dashboard-stream-traffic) for a connection that stays in recovery, [authentication](api.md#authentication) for session behavior, and [Dashboard SSE](api.md#dashboard-sse) for the wire format.

The Dashboard coalesces telemetry bursts into at most four renders per second and retains unchanged text, lists, and topology elements. Live metric updates preserve the open card and the horizontal scroll position while refreshing its values.

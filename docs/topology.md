# Inspect Transport, Kademlia, and GossipSub Topology

English | [Korean](topology.kr.md)

## Read the graph

The Dashboard shows a full-width interactive topology with equally sized, wedge-shaped sectors for Agents. Agent numbers match **Agent status → No.** Each Peer has a local display number inside its Agent's sector; select it to see the full node and Peer IDs. The number helps identify a Peer on screen and does not fix its position. Display numbers can be reused after a Peer departs; use the full IDs for persistent identity.

Peers settle gradually within their Agent's sector. Visible links apply spring-like forces, while repulsion and collision handling spread nearby Peers apart. Peers stay inside their assigned sector as they move. Changes to Peers or visible relationships restart the layout briefly; it stops when settled instead of adding continuous random movement. Placement is a display aid and does not change protocol behavior or experiment results.

All three layers start turned off. Use the independent checkboxes above the graph, in this order:

| Layer | Default | Line | Meaning |
|---|---|---|---|
| **Transport** | Off | Thin muted gray | An active libp2p transport connection reported by a Peer. |
| **Kademlia** | Off | Thin cyan dashed | A Peer lists the other Peer in its current DHT routing table. |
| **GossipSub mesh** | Off | Thicker orange solid | A Peer reports accepted GRAFT mesh membership for the selected topic(s). |

Turning off a layer hides its lines and updates the layout forces to use the remaining visible relationships; it does not disable that protocol or change the experiment. **GossipSub topic** filters mesh relationships while preserving the other layers. Changing these filters can reposition Peers within their sectors. In **All topics**, a pair's mesh relations are combined into one visual line, with the topic evidence retained in the details. The summary's **Transport links** counts unique transport pairs, not the sum of all three layers.

Hover over a Peer to emphasize its visible neighbors and dim unrelated lines. Select a Peer to retain its details, including Agent/Peer display numbers, full IDs, profile, network settings, and reporting endpoints. **Clear selection** or Escape clears the selection. Use **+ / −** or Ctrl/Command + scroll to zoom, drag the graph background to pan, and **Fit** to show the whole layout. Peers support keyboard focus and Enter/Space selection.

**Pause motion** keeps existing Peer positions while leaving filters, selection, zoom, and pan available. New Peers and changes to Agent sectors still appear immediately. **Resume motion** lets the layout settle again.

The **Network overview** header's **Collapse / Expand** button hides or shows the entire panel. A collapsed panel stops layout animation while live state continues to update. Reopening shows the latest topology and preserves the chosen motion setting. See [panel visibility](monitoring.md#show-and-hide-panels) for saved preferences and the other panel controls.

With the operating system or browser's reduced-motion preference enabled, animated settling is off by default. The initial layout and relationship changes use a bounded settling calculation and display the result without intermediate animation. You can explicitly choose **Resume motion** to enable animation.

Agent sectors have equal angles, and configured capacity does not reserve empty Peer positions. Dense experiments can still look crowded; use topic filtering, layer toggles, and zoom to inspect a selected Agent or Peer.

## What each line establishes

Transport connectivity, DHT knowledge, locally known PubSub topic peers (`TopicPeers` / PubSub `ListPeers`), and GRAFT mesh membership are different relationships. A topic-peer entry does not prove a remote application subscription: publish-only and relay participants are possible. The current graph derives each layer from its own Peer status fields, rather than reconstructing it from ADD_PEER events.

- **Kademlia** comes from `dht.RoutingTable().ListPeers()`. Routing-table membership does not prove an active transport connection or a packet sent at that moment.
- **GossipSub** is tracked at each Peer through synchronous router callbacks. Accepted `Graft` adds membership; `Prune`, topic `Leave`, and `RemovePeer` remove it. This state is independent of the telemetry queue, event delivery order at the Controller, and the 300-row recent-event buffer. `floodsub`, `randomsub`, and disabled PubSub have no GossipSub mesh layer. Direct/fanout forwarding paths outside the GRAFT mesh are not included.
- **Transport** comes from the current host connection list. ADD_PEER events and existing connections alone never create Kademlia or GossipSub edges.

An edge is an undirected display projection of one or both endpoints' reports. `reportedBy` identifies which endpoints actually reported it; Kademlia tables and transient mesh observations can be asymmetric. A PRUNE seen by one endpoint may leave the other endpoint's report visible briefly, until that endpoint reports its new state. DHT, transport, and mesh data are not sampled at a single atomic cluster-wide instant.

## Bootstrap and topic discovery

Bootstrap transport connections and GossipSub topic discovery are separate. Each Peer first queries `/api/v1/bootstrap` for ready `boot` Peers in its run. A worker tries a seeded shuffle until the first successful connection, including when its DHT is disabled; a boot node attempts the advertised peers and may initialize an empty network. It bootstraps Kademlia only when DHT is enabled. The built-in boot type does not run PubSub, so this connection alone cannot form a GossipSub mesh.

After PubSub starts, a Peer queries the Controller's `/api/v1/discovery` registry immediately, then on a three-second polling loop for each exact configured topic. The registry returns all ready, PubSub-enabled Peers from the same run and topic whose Agents are online. The Peer ranks them with rendezvous hashing, preferring subscribed receivers before publish-only or relay candidates. It subtracts its current PubSub `ListPeers(topic)` count from `DHigh` and attempts to fill the remaining deficit, within the shared hard connection-cap budget. Failed dials advance to another ranked candidate; failed Peer IDs have a 15-second retry backoff. Registry requests and each dial round have five-second bounds, so slow rounds can delay the next poll. This repairs candidate connectivity under churn without deliberately constructing an all-to-all graph; incoming and other protocol connections can still exceed the per-topic discovery target.

The registry supplies addresses and configured topic participation; it does not report or choose delivery outcomes. A discovered transport appears in the gray layer only after libp2p connects. GossipSub still decides which topic peers enter the mesh, and only an accepted GRAFT appears as an orange mesh edge. A DHT routing-table entry remains a separate cyan edge. Consequently, `DHigh` is a discovery target rather than a promise that the graph will show exactly that many transport or mesh neighbors.

## Freshness and lifecycle

Peers send status about every two seconds; Agents forward their current snapshots. Network, scheduling, and status failures add delay. Stopping, stopped, starting, and failed endpoints have no displayed relationship lines. Stopping, stopped, and failed Peer circles and their incident lines are hidden. Starting Peers appear in amber as **Starting**. Failed Peer diagnostics remain available through node inspection and saved experiment records.

After successful container cleanup, the Agent keeps a compact record with the Peer identity, terminal state, lifecycle timestamps, and topic labels. It releases old connections, routing/mesh memberships, scores, and bulky configuration metadata from memory. Full Agent status and node inspection still include the compact terminal record; active Peers and failed processes retain full diagnostics.

Periodic heartbeats send active/failed Peers and successful exits that the Controller has not yet acknowledged, splitting large reports into bounded JSON batches. After acknowledgement, a successful exit is omitted from subsequent periodic reports but remains in the full inventory. Re-registration resets these acknowledgements so retained history is reported again after a Controller restart. The Controller does not treat omission from a partial heartbeat as an exit; see the [heartbeat contract](api.md#internal-cluster-endpoints).

Controller snapshots reflect these exits even when no Dashboard is connected. Late heartbeats and create responses cannot revive a stopped or failed Peer; failed Peers can still progress through cleanup to stopped, and reopening the Dashboard uses this current snapshot.

Offline Agents and Peer status older than ten seconds are excluded from lines. To avoid comparing clocks on different servers, the Controller derives Peer-report age from two timestamps produced by the same Agent, then advances that age on its own clock after receiving the heartbeat. This is the age reported by the Agent plus time since receipt, not a bound on time spent in transit. Missing legacy timestamps cannot establish that age. `OverlayObservedAt` identifies a supported overlay snapshot; it is the Peer's clock and is not compared directly with browser/Controller time to declare staleness.

A fresh empty snapshot removes old routing/mesh memberships. Agents reject older crossed status reports and copy slices/maps before forwarding. A new snapshot therefore repairs the graph after dropped telemetry. These rules do not imply that every displayed relationship is currently transmitting or reachable.

Older Peers lacking overlay snapshots provide only Transport lines. The graph reports how many visible Peers have supplied overlay data; it does not relabel old transport connections as Kademlia. Deploy the updated image to Controller and Agents, then start new Peers to obtain all layers.

## API and retained data

`GET /api/v1/snapshot`, `GET /api/v1/network`, and the SSE snapshot carry the same typed edges. Relevant Node fields are `routingPeers` (Peer IDs), `meshPeers` (topic → Peer IDs), and `overlayObservedAt`. `/api/v1/discovery` is a live transport-candidate registry and is not a topology-history endpoint. An example edge is:

```json
{"source":"node-a","target":"node-b","protocol":"gossipsub","topic":"kpl/demo","reportedBy":["node-a"]}
```

`protocol` is `transport`, `kademlia`, or `gossipsub`. GossipSub edges are distinct per topic in the API. Duplicate reports are merged deterministically, and unknown endpoints, ambiguous Peer identities within a run, and cross-run relationships are excluded. IDs in the graph and API are Node IDs; reported neighbor lists inside Node objects use libp2p Peer IDs.

During a run, the Controller saves group state/degree/clustering/score summaries and protocol graph samples in `observations.jsonl` about every five seconds. Each `graphs` item has `protocol`, sorted Node IDs in `nodes`, aligned `groups`, and node-index pairs in `edges`. Edges are unique and undirected; GossipSub merges the same peer pair across topics. The live edge fields `topic` and `reportedBy` are not retained in this graph format. Changes between samples and individual observer-to-peer scores are not retained.

`events.jsonl` preserves collected GRAFT/PRUNE transitions and [detailed RPC metadata](api.md#detailed-peer-logs), subject to telemetry loss. Historical summaries without edges cannot recover new graph metrics. Use the [result ZIP](monitoring.md#download-experiment-results) for source records and [research metrics](experiment-metrics.md#research-graph-observations) for thinning and graph calculations. Layer visibility and layout do not change the main delivery definition.

## Development checks

Use `make test-linux` or `docker build --target test -t kpl-v3:test .` with an already available Linux Docker engine for Peer mesh/DHT snapshots, Agent ordering/copying, Controller edges/freshness, and embedded HTML checks. Run native `go test ./...` only on Linux; do not execute Go tests or test binaries on Windows. Run `node --test internal/webui/topology_test.cjs internal/webui/topology-layout_test.cjs` with a modern Node.js runtime for layer/topic filters, sector layout and settling, churn, camera behavior, and DOM interaction regressions. The JavaScript tests use built-in Node.js modules and require no npm packages. These checks do not replace a browser visual review or a real multi-server experiment. See the [development guide](development.md) for the full validation boundary.

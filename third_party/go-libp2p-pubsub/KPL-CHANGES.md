# KPL PubSub patches

Based on `github.com/libp2p/go-libp2p-pubsub v0.13.1`. Original licenses, source and tests are retained. `KPL-UPSTREAM-SHA256.json` records every pristine upstream file checksum; differences identify the patches below.

## Score inspection

The `score.go` patch adds an optional `TimedPeerScoreInspectFn` callback with the snapshot timestamp. The extended inspector includes topic `InMesh`, `MeshMessageDeliveriesActive` and `MeshFailurePenalty`, captured under the existing score lock. Callbacks remain asynchronous. Scoring arithmetic and decay are unchanged.

These fields support exact P1, P3 and P3b observations: active mesh delivery penalties can remain after pruning, and capped totals cannot recover the sticky penalty by subtraction. KPL computes weighted components from these snapshots and immutable parameters. The timestamp covers empty snapshots and prevents older callbacks from replacing newer measurements.

## HopWave

The scholarly reference is [Sungwook Lee and Hongtaek Ju, HopWave, APNOMS 2026](https://github.com/k-p2p-lab/kpl-v3/wiki/Publications#hopwave-reference), **not yet published**.

The implementation source used for this port is [kmu-comnet/go-libp2p-pubsub, hop-wave](https://github.com/kmu-comnet/go-libp2p-pubsub/tree/7f75a9dbee3d5e1e3a15a9d30fa8a428590af76c), revision `7f75a9dbee3d5e1e3a15a9d30fa8a428590af76c`. Its metadata changes also appear on master at `6d217e5a0eaed10816d8213626428e17575ec5f0`. `KPL-HOPWAVE-SHA256.json` records hashes of the eight changed source files at the hop-wave revision, relative to that fork's upstream v0.14.2 base. These are source provenance hashes, not checksums of this adapted copy. The four protobuf files at that base match v0.13.1; unrelated v0.14.2 changes and dependency upgrades are not included.

- `pb.Message` adds optional propagation type (field 7) and hop counter (field 8), with matching delivery/duplicate trace metadata. Mutable fields are excluded from message signatures; payload, topic, author and sequence remain signed.
- `WithHopwave()` enables metadata alone: publication starts at zero, outgoing eager pushes and IWANT replies add one to the cached arrival count. Metadata is disabled by default.
- `WithHopwavePublish(true)` additionally enables the reference branch's periodic forwarding. `HopwaveInterval` resets the outgoing counter to zero for a full wave. Other counter values select a shuffled `floor(recipient count × HopwaveFactor)` subset, with at least one recipient when the factor is positive. The interval counts hops, not time; the cyclic counter is not an absolute path length.
- Selection applies to all eligible publication recipients, including direct peers and flood-publish targets, after sender/author exclusions and existing protocol/score/IDONTWANT rules. IWANT responses retain normal eligibility/retransmission limits and bypass fractional selection.
- `HopwaveFactor` must be finite in `[0, 1]`; `HopwaveInterval` must be in `[1, 2147483647]`. Defaults `1`/`1` preserve full forwarding even when explicitly enabled; the KPL example configures `0.5`/`3`. Scoring and mesh maintenance retain their existing behavior.
- Outgoing metadata uses a shallow message copy with new metadata pointers. Subscribers, cached arrivals, trace events and queued pushes remain unchanged. Repeated pull replies neither accumulate the counter nor contaminate queued eager labels. Normal forwarding resets the propagation label to eager.
- With HopWave enabled, default and per-topic message ID callbacks receive the message without mutable propagation/hop fields. This preserves one ID across eager/lazy forwarding, deduplication and IWANT cache lookup even for callbacks that serialize the entire protobuf. Other fields are preserved. Disabled HopWave retains the original callback input; mixed settings require an ID function based only on stable fields.
- Missing, negative and overflowing counters remain unknown. Unknown counters use full publication forwarding. Wire fields are unauthenticated reports and require compatible instrumentation/settings along the path for interpretation. Protocol IDs remain GossipSub's, as in the fork; unpatched signed-message verifiers do not support the extension.

Modified upstream implementation files are `score.go`, `pubsub.go`, `topic.go`, `gossipsub.go`, `comm.go`, `midgen.go`, `sign.go`, `trace.go` and the four protobuf schema/binding files. KPL adds `hopwave.go`, `mesh_freeze.go`, `mesh_plan.go`, score/HopWave/lifecycle/mesh regression tests and the two provenance manifests. The upstream license files are retained unchanged.

`kpl_hopwave_audit_test.go` covers mesh/fanout/flood recipient parity with disabled, metadata-only and full-forwarding options, hop interval boundaries, lazy recovery, and custom message IDs. `kpl_score_audit_test.go` checks legacy inspector behavior, asynchronous callback ownership and unchanged score/retention arithmetic.

## Resource lifetime

- Failed default GossipSub construction closes its owned address book. PubSub starts the backoff and seen-message cache workers only after option, signature and discovery setup succeeds.
- A failed or closed address-book event subscription leaves the book available until PubSub cancellation, then closes it exactly once. A closed event stream waits for cancellation without polling. These paths repair resource handling inherited from v0.13.1.
- Initial heartbeat and direct-peer connection delays stop on context cancellation. Initial direct-peer queue sends also stop on cancellation.
- Periodic direct-peer reconnects use the bounded connector queue without spawning blocked senders; peers that do not fit are retried on a later eligible heartbeat.

`kpl_lifecycle_test.go` covers failed construction, canceled timers and queue waits, and reconnects after queue capacity becomes available. `kpl_lifecycle_audit_test.go` also covers caller-owned Host/router reuse, successful shutdown, repeated queue progress and failed/closed event subscriptions.

## Mesh freeze

This KPL extension is independent of HopWave and disabled by default. `WithMeshFreeze()` permits a later `PubSub.FreezeMesh(ctx)` call; construction still forms and maintains a normal GossipSub mesh. Unsupported routers and instances without the option return explicit errors. Freeze is one-way and idempotent, and returns success only after the router event loop applies the request. Cancellation before application leaves the mesh unchanged; cancellation racing application can be resolved by retrying or inspecting the snapshot.

- Freeze pins every current topic's mesh peer IDs, including an empty mesh. Heartbeat degree repair, score pruning, outbound repair and opportunistic grafting stop. Incoming GRAFT and PRUNE cannot change membership and do not trigger PRUNE or PX replies.
- Later local joins cannot introduce new topic meshes or peer IDs. Leaving or closing a topic stops local participation, and rejoining an original topic can use its pinned peers again. Joining clears publication-only fanout and its timestamp as upstream does, without promoting fanout peers into the mesh. New subscriptions and relays retain IHAVE/IWANT and heartbeat gossip through an empty mesh; they do not acquire publication-only fanout merely because the topic was absent at freeze time.
- Disconnects, remote unsubscriptions and blacklisting still release transport and active score/connection-manager state. Their pinned IDs remain until PubSub shutdown, with no automatic replacement. Disconnect preserves the upstream score-retention decision and its delivery penalty ordering; only actual unsubscribe/leave operations perform separate prune accounting. A same-ID reconnect/resubscription can restore an existing link if the negotiated protocol still supports mesh routing.
- `PubSub.MeshFreezeSnapshot(ctx)` returns caller-owned `Mesh` and `Active` maps plus `Frozen` and a monotonically increasing `ObservedAt`. `Mesh` is the logical membership; `Active` additionally requires local subscription/relay participation, a connected mesh-capable PubSub stream and the remote subscription. A pinned ID is not evidence of a live physical link. Internal score/tag bookkeeping follows participation changes without synthesizing GRAFT/PRUNE trace events or applying duplicate mesh-failure penalties.
- Retry controls and queued GRAFT/PRUNE are discarded without mutating shared RPCs or losing publications and gossip in the same RPC. Stream writers also suppress old mesh controls after freezing. Controls already dequeued by a writer or transmitted before application cannot be recalled; this is a local operation and does not atomically freeze remote peers.
- Heartbeats continue cache expiry, counters, gossip and fanout maintenance. IHAVE/IWANT, direct peers, flood publication, validation, scoring and optional HopWave forwarding remain active under their normal rules. Freeze fixes mesh membership; it does not constrain all GossipSub publication recipients to that mesh.
- An accepted message whose validation completes after local unsubscription still follows the upstream cache and forwarding path. Freezing does not discard that in-flight data or change ordinary fanout maintenance timing.

`kpl_mesh_freeze_test.go` covers authorization, acknowledged application, cancellation before/after enqueue, all membership mutation paths, cache expiry, control queue ownership, score retention, real TCP forwarding, remote unsubscribe, disconnect and same-peer reconnect. This extension changes no protobuf schema or protocol ID. Both SHA-256 manifests remain pristine source-provenance records and must not be regenerated from the patched tree.

`kpl_mesh_freeze_parity_test.go` exercises ordinary and opted-in-but-unfrozen routing, control retries, cache maintenance, TCP delivery and other routers. `kpl_mesh_freeze_semantics_test.go` covers joined-topic versus fanout semantics and lazy recovery for new topics, with and without HopWave. `kpl_mesh_freeze_resource_test.go` covers upstream positive/negative score retention on disconnect and protocol capabilities when pinned peers reconnect.

## Integration and upgrades

### Explicit mesh plans

`mesh_plan.go` adds `PubSub.ValidateMeshPlan(ctx, topic, peers)` and `PubSub.SetMeshAndFreeze(ctx, topic, peers)` behind the same `WithMeshFreeze()` capability. Validation requires a participating local topic and distinct, valid, nonlocal mesh-capable neighbors with connected PubSub streams and remote topic subscriptions. Direct peers, blacklisted peers and rejected peer filters are excluded. Invalid inputs and temporary readiness failures are distinguishable through `ErrMeshPlanInvalid` and `ErrMeshPlanNotReady`.

Application checks all inputs before mutation, installs exactly one topic's requested neighbors, then freezes all current topic meshes in the same router event-loop callback. Other topics keep their existing members. It uses the requested membership regardless of automatic degree/score/backoff selection. Score and connection-manager accounting change only for added/removed neighbors; publication fanout is cleared for that topic, and the existing freeze control purge is reused. No wire GRAFT/PRUNE exchange or corresponding trace event is fabricated.

Already frozen identical membership succeeds even after a link disappears; any different topic/membership returns `ErrMeshFreezeConflict`. Cancellation before application changes nothing; cancellation racing application may require snapshot inspection or retry. The calls are local and cannot establish an atomic distributed graph. KPL's scenario Controller prepares every target before dispatching installation. `kpl_mesh_plan_test.go` covers validation, cancellation, ownership, accounting, queues, other-topic preservation, retries and actual TCP forwarding. Generation models and control contracts are in the [scenario reference](https://github.com/k-p2p-lab/kpl-v3/wiki/Scenario-Reference#topology-formation).

### Application and maintenance

The root `go.mod` selects this local replacement; Docker copies it before dependency download. KPL exposes routing through `gossipsub.hopwave` and `gossipsub.params.hopwaveFactor` / `hopwaveInterval`. Operator and log contracts live in the wiki's [Protocol Options](https://github.com/k-p2p-lab/kpl-v3/wiki/Protocol-Options#hopwave).

When upgrading PubSub, update the complete upstream copy and pristine manifest, then reapply the score inspection, HopWave, mesh-freeze/plan and resource-lifetime patches. Preserve licenses and reference provenance, regenerate protobuf bindings if schemas change, and run upstream score/signature tests, KPL component/retention/cap tests, mesh plan/freeze lifecycle/control tests, and HopWave forwarding/pull/signature tests under the race detector. KPL's HopWave and mesh network tests explicitly use TCP, Noise and Yamux, matching the application transport.

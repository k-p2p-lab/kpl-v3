# Third-Party Notices

This file identifies third-party source included directly in K-P2PLab and selected linked dependencies. Original license files and notices are retained with included source or copied below. All Go dependencies and their versions are declared in [go.mod](go.mod) and [go.sum](go.sum).

## go-libp2p-pubsub

K-P2PLab includes a modified copy of go-libp2p-pubsub for GossipSub score observation, optional HopWave forwarding, opt-in mesh freezing, and runtime routing parameter updates.

- Upstream project: [libp2p/go-libp2p-pubsub](https://github.com/libp2p/go-libp2p-pubsub)
- Upstream version: [v0.13.1](https://github.com/libp2p/go-libp2p-pubsub/tree/v0.13.1)
- Upstream commit: [`68726389f2c07d451f93a210146472e2e11ac32f`](https://github.com/libp2p/go-libp2p-pubsub/commit/68726389f2c07d451f93a210146472e2e11ac32f)
- Included source: [third_party/go-libp2p-pubsub](third_party/go-libp2p-pubsub)
- HopWave paper: [Sungwook Lee and Hongtaek Ju, HopWave, APNOMS 2026](https://github.com/k-p2p-lab/kpl-v3/wiki/Publications#hopwave-reference); not yet published
- HopWave implementation source: [kmu-comnet/go-libp2p-pubsub, hop-wave revision `7f75a9d`](https://github.com/kmu-comnet/go-libp2p-pubsub/tree/7f75a9dbee3d5e1e3a15a9d30fa8a428590af76c), selectively adapted from its v0.14.2 base while retaining KPL's v0.13.1 baseline

### Licenses

The upstream project describes its licensing as MIT / Apache License 2.0. Its [LICENSE](third_party/go-libp2p-pubsub/LICENSE) also records the transition from MIT-only licensing: unless otherwise noted, contributions made before May 6, 2019 by contributors outside the referenced signoff list remain MIT-only.

The original license files are included:

- [MIT license](third_party/go-libp2p-pubsub/LICENSE-MIT)
- [Apache License 2.0 notice](third_party/go-libp2p-pubsub/LICENSE-APACHE)
- [Licensing transition notice](third_party/go-libp2p-pubsub/LICENSE)

### K-P2PLab modifications

The [score.go](third_party/go-libp2p-pubsub/score.go) patch provides:

- Added `TimedPeerScoreInspectFn`, which supplies the observation timestamp with the score snapshots, including empty snapshots.
- Added `InMesh`, `MeshMessageDeliveriesActive`, and `MeshFailurePenalty` to topic score snapshots so K-P2PLab can measure P1, P3, and P3b accurately.

The observation fields and timestamp are captured under the existing score lock; inspection callbacks remain asynchronous. Scoring arithmetic and decay are unchanged.

The HopWave adaptation adds optional unsigned propagation/hop metadata and periodic full/fractional forwarding to GossipSub. It changes `pubsub.go`, `topic.go`, `gossipsub.go`, `midgen.go`, `sign.go`, `trace.go` and the RPC/trace protobuf schemas and generated bindings, and adds [hopwave.go](third_party/go-libp2p-pubsub/hopwave.go). Outgoing copies preserve cached/subscriber metadata. When HopWave is enabled, message ID callbacks exclude mutable metadata to preserve deduplication and lazy recovery. HopWave is disabled by default; existing GossipSub protocol IDs and default routing remain.

The copy also bounds periodic direct-peer reconnect work, cancels initial timers and connection queue waits on shutdown, and releases resources when default GossipSub construction fails. Address-book cleanup also survives failed/closed event subscriptions without polling a closed stream. These changes and their [lifecycle tests](third_party/go-libp2p-pubsub/kpl_lifecycle_test.go) affect `gossipsub.go` and `pubsub.go`.

The optional mesh freeze patch adds `WithMeshFreeze()` and `PubSub.FreezeMesh(ctx)` to pin GossipSub mesh membership after a runtime command. It suppresses automatic and incoming GRAFT/PRUNE membership changes while retaining transport cleanup and ordinary message processing. The capability is disabled by default; its implementation and regression tests are recorded in the patch notes below.

The related [explicit mesh plan extension](third_party/go-libp2p-pubsub/mesh_plan.go) adds `ValidateMeshPlan` and `SetMeshAndFreeze` under the same opt-in capability. It validates a supplied neighbor set, installs one topic's membership and freezes all local topic meshes in one event-loop operation, preserving existing score/connection accounting without synthetic wire controls. Graph generation itself lives in KPL's `internal/topology` package. This extension changes no protobuf schema, protocol ID or dependency version.

The [runtime parameter extension](third_party/go-libp2p-pubsub/runtime_params.go) adds acknowledged, validated updates of degree, gossip factor and HopWave forwarding parameters on the router event loop. It preserves mesh freezing and construction-time timers, queues, caches and scoring. The separate file changes no defaults, dependencies, protobufs or protocol IDs.

K-P2PLab also adds [inspection](third_party/go-libp2p-pubsub/kpl_score_snapshot_test.go) and [HopWave](third_party/go-libp2p-pubsub/kpl_hopwave_test.go) regression tests, [patch and upgrade notes](third_party/go-libp2p-pubsub/KPL-CHANGES.md), a [SHA-256 manifest of the pristine upstream files](third_party/go-libp2p-pubsub/KPL-UPSTREAM-SHA256.json), and a [manifest of HopWave reference sources](third_party/go-libp2p-pubsub/KPL-HOPWAVE-SHA256.json). Original license files are unchanged; the reference fork retains the same licenses. The root [go.mod](go.mod) selects this local copy through a `replace` directive.

## bbolt

K-P2PLab uses the unmodified bbolt Go module for the Agent's local retired-Peer history store.

- Upstream project: [etcd-io/bbolt](https://github.com/etcd-io/bbolt)
- Version: [v1.4.3](https://github.com/etcd-io/bbolt/tree/v1.4.3), selected by [go.mod](go.mod)
- License: MIT, copyright (c) 2013 Ben Johnson
- Original license: [third_party/licenses/bbolt-LICENSE](third_party/licenses/bbolt-LICENSE)

The module is downloaded through Go modules; its source is not vendored in this repository.

# Third-Party Notices

This file identifies third-party source included directly in K-P2PLab. Original license files and notices are retained with that source. Other Go dependencies and their versions are declared in [go.mod](go.mod) and [go.sum](go.sum).

## go-libp2p-pubsub

K-P2PLab includes a modified copy of go-libp2p-pubsub for GossipSub score observation.

- Upstream project: [libp2p/go-libp2p-pubsub](https://github.com/libp2p/go-libp2p-pubsub)
- Upstream version: [v0.13.1](https://github.com/libp2p/go-libp2p-pubsub/tree/v0.13.1)
- Upstream commit: [`68726389f2c07d451f93a210146472e2e11ac32f`](https://github.com/libp2p/go-libp2p-pubsub/commit/68726389f2c07d451f93a210146472e2e11ac32f)
- Included source: [third_party/go-libp2p-pubsub](third_party/go-libp2p-pubsub)

### Licenses

The upstream project describes its licensing as MIT / Apache License 2.0. Its [LICENSE](third_party/go-libp2p-pubsub/LICENSE) also records the transition from MIT-only licensing: unless otherwise noted, contributions made before May 6, 2019 by contributors outside the referenced signoff list remain MIT-only.

The original license files are included:

- [MIT license](third_party/go-libp2p-pubsub/LICENSE-MIT)
- [Apache License 2.0 notice](third_party/go-libp2p-pubsub/LICENSE-APACHE)
- [Licensing transition notice](third_party/go-libp2p-pubsub/LICENSE)

### K-P2PLab modifications

Only the upstream [score.go](third_party/go-libp2p-pubsub/score.go) source file is modified:

- Added `TimedPeerScoreInspectFn`, which supplies the observation timestamp with the score snapshots, including empty snapshots.
- Added `InMesh`, `MeshMessageDeliveriesActive`, and `MeshFailurePenalty` to topic score snapshots so K-P2PLab can measure P1, P3, and P3b accurately.

The observation fields and timestamp are captured under the existing score lock; inspection callbacks remain asynchronous. Scoring arithmetic, decay, routing, wire protocols, and defaults are unchanged.

K-P2PLab also adds an [inspection regression test](third_party/go-libp2p-pubsub/kpl_score_snapshot_test.go), [patch and upgrade notes](third_party/go-libp2p-pubsub/KPL-CHANGES.md), and a [SHA-256 manifest of the original upstream files](third_party/go-libp2p-pubsub/KPL-UPSTREAM-SHA256.json). The root [go.mod](go.mod) selects this local copy through a `replace` directive.

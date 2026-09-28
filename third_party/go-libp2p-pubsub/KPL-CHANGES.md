# KPL score observation patch

Based on github.com/libp2p/go-libp2p-pubsub v0.13.1. Original licenses, source and tests are retained. KPL-UPSTREAM-SHA256.json records every original file checksum.

Only score.go is changed: an optional TimedPeerScoreInspectFn callback supplies the snapshot timestamp, and the extended inspector includes the topic InMesh, MeshMessageDeliveriesActive and MeshFailurePenalty state, captured under the existing score lock. The callback remains asynchronous. Scoring arithmetic, decay, routing, wire protocols and all defaults are unchanged.

These fields are needed for exact P1, P3 and P3b observations: active mesh delivery penalties may remain after pruning, and capped totals cannot recover the missing sticky penalty by subtraction. KPL computes weighted components from these snapshots and its immutable parameters. The timestamp also covers empty snapshots and prevents an older callback from replacing newer measurements.

The root go.mod uses this local replacement; Docker copies it before dependency download. When upgrading libp2p-pubsub, update this complete copy and checksum manifest, reapply only this observation patch, and run the upstream score tests plus KPL's component/retention/cap tests. See the repository wiki's Protocol Options for operator documentation.

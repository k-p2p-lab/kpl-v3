# KPL PubSub patches

Based on `github.com/libp2p/go-libp2p-pubsub v0.13.1`. Original licenses, source and tests are retained. `KPL-UPSTREAM-SHA256.json` records every pristine upstream file checksum; differences identify the patches below.

## Score inspection

The `score.go` patch adds an optional `TimedPeerScoreInspectFn` callback with the snapshot timestamp. The extended inspector includes topic `InMesh`, `MeshMessageDeliveriesActive` and `MeshFailurePenalty`, captured under the existing score lock. Callbacks remain asynchronous. Scoring arithmetic and decay are unchanged.

These fields support exact P1, P3 and P3b observations: active mesh delivery penalties can remain after pruning, and capped totals cannot recover the sticky penalty by subtraction. KPL computes weighted components from these snapshots and immutable parameters. The timestamp covers empty snapshots and prevents older callbacks from replacing newer measurements.

## HopWave

The scholarly reference is [Sungwook Lee and Hongtaek Ju, HopWave, APNOMS 2026](https://github.com/k-p2p-lab/kpl-v3/wiki/Publications#hopwave-reference), **not yet published**.

The implementation source used for this port is [kmu-comnet/go-libp2p-pubsub, hop-wave](https://github.com/kmu-comnet/go-libp2p-pubsub/tree/7f75a9dbee3d5e1e3a15a9d30fa8a428590af76c), revision `7f75a9dbee3d5e1e3a15a9d30fa8a428590af76c`. Its metadata changes also appear on master at `6d217e5a0eaed10816d8213626428e17575ec5f0`. `KPL-HOPWAVE-SHA256.json` records hashes of the eight changed source files at the hop-wave revision, relative to that fork's upstream v0.14.2 base. These are source provenance hashes, not checksums of this adapted copy. The four protobuf files at that base match v0.13.1; unrelated v0.14.2 changes and dependency upgrades are not included.

- `pb.Message` adds optional propagation type (field 7) and hop counter (field 8), with matching delivery/duplicate trace metadata. Mutable fields are excluded from message signatures; payload, topic, author and sequence remain signed.
- `WithHopWave()` enables metadata alone: publication starts at zero, outgoing eager pushes and IWANT replies add one to the cached arrival count. Metadata is disabled by default.
- `WithHopWavePublish(true)` additionally enables the reference branch's periodic forwarding. `HopWaveInterval` resets the outgoing counter to zero for a full wave. Other counter values select a shuffled `floor(recipient count × HopWaveFactor)` subset, with at least one recipient when the factor is positive. The interval counts hops, not time; the cyclic counter is not an absolute path length.
- Selection applies to all eligible publication recipients, including direct peers and flood-publish targets, after sender/author exclusions and existing protocol/score/IDONTWANT rules. IWANT responses retain normal eligibility/retransmission limits and bypass fractional selection.
- `HopWaveFactor` must be finite in `[0, 1]`; `HopWaveInterval` must be in `[1, 2147483647]`. Defaults `1`/`1` preserve full forwarding even when explicitly enabled; the KPL example configures `0.5`/`3`. Scoring and mesh maintenance retain their existing behavior.
- Outgoing metadata uses a shallow message copy with new metadata pointers. Subscribers, cached arrivals, trace events and queued pushes remain unchanged. Repeated pull replies neither accumulate the counter nor contaminate queued eager labels. Normal forwarding resets the propagation label to eager.
- Missing, negative and overflowing counters remain unknown. Unknown counters use full publication forwarding. Wire fields are unauthenticated reports and require compatible instrumentation/settings along the path for interpretation. Protocol IDs remain GossipSub's, as in the fork; unpatched signed-message verifiers do not support the extension.

Modified upstream files are `score.go`, `pubsub.go`, `topic.go`, `gossipsub.go`, `sign.go`, `trace.go` and the four protobuf schema/binding files. KPL adds `hopwave.go`, score/HopWave regression tests and the two provenance manifests. The upstream license files are retained unchanged.

## Integration and upgrades

The root `go.mod` selects this local replacement; Docker copies it before dependency download. KPL exposes routing through `gossipsub.hopWave` and `gossipsub.params.hopWaveFactor` / `hopWaveInterval`. Operator and log contracts live in the wiki's [Protocol Options](https://github.com/k-p2p-lab/kpl-v3/wiki/Protocol-Options#hopwave).

When upgrading PubSub, update the complete upstream copy and pristine manifest, then reapply both score inspection and HopWave patches. Preserve licenses and reference provenance, regenerate protobuf bindings if schemas change, and run upstream score/signature tests, KPL component/retention/cap tests, and HopWave forwarding/pull/signature tests under the race detector. KPL's HopWave network tests explicitly use TCP, Noise and Yamux, matching the application transport.

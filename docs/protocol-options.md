# Protocol configuration reference

English | [Korean](protocol-options.kr.md)

[Documentation index](README.md) · [Repository](../README.md)

This reference covers the declarative node configuration for the pinned `go-libp2p-pubsub v0.13.1` and `go-libp2p-kad-dht v0.31.0` dependencies. Use these blocks under a scenario profile or a join phase's `node`; see [scenario configuration](scenario-reference.md) and the [protocol example](../examples/protocol-options.yaml). Experiments run on [Swarm](swarm.md). Validate YAML before deployment with `kpl validate --scenario FILE`.

Duration fields use Go strings such as `250ms`, `1s`, and `5m`. Numeric tuning pointers preserve explicit `0`, and boolean pointers preserve explicit `false`, wherever those values are valid. Lists and maps replace inherited lists and maps. Optional object blocks such as `score`, `peerGater`, and `discovery` replace the inherited object as a whole: repeat the settings you want to retain. A `score` block without `enabled: true` remains disabled, including after replacing an enabled profile score.

## PubSub activation and common controls

All fields in this section are under `gossipsub`. Omitted optional library settings retain the pinned library defaults. Built-in node presets can supply different mesh settings, as listed below.

| Field | Behavior |
|---|---|
| `enabled` | Enable PubSub. Defaults to true; the `boot` preset disables it unless overridden. |
| `router` | `gossipsub` (default), `floodsub`, or `randomsub`. |
| `topics` | Joined topic names; default `[kpl/default]`. Topic-specific configuration uses these exact strings. |
| `topicMode` | `subscribe` (default), `relay`, or `publish`. Relay forwards without an application subscription; publish joins without subscribing. |
| `subscribe` | Compatibility switch. `false` selects publish mode when `topicMode` is absent. |
| `allowPublish` | Allow application publication requests; default true. |
| `randomDegree`, `randomNetworkSize` | Positive RandomSub degree target and estimated network size. Each Peer container has its own process-global RandomSub degree. |
| `floodPublish`, `peerExchange` | GossipSub flood publishing and PRUNE peer exchange. Both preserve explicit false. |
| `maxMessageSize` | Maximum serialized PubSub RPC size in bytes; default 1 MiB. A zero limit is passed through, not treated as unlimited. |
| `peerOutboundQueueSize` | Positive per-peer outbound RPC queue size; library default 32. |
| `validateQueueSize` | Positive incoming validation queue size; library default 32. |
| `validateThrottle` | Concurrent asynchronous validation throttle; library default 8192, nonnegative. Zero permits no asynchronous validations. |
| `validateWorkers` | Positive validation-worker count; defaults to the number of CPUs reported to the process. |
| `subscriptionBufferSize` | Nonnegative application subscription buffer size. Topic overrides take precedence. |
| `signaturePolicy` | `strict-sign` (library default), `strict-no-sign`, `lax-sign`, `lax-no-sign`. Strict policies enforce the selected signing convention on received messages; lax policies relax incoming signature requirements. |
| `seenMessagesTTL` | PubSub seen-message cache lifetime; library default `2m`. This differs from the score delivery-record TTL. |
| `seenMessagesStrategy` | `first-seen` (library default) or `last-seen` cache expiry strategy. |
| `messageId` | `default` uses author and sequence number; `sha256` hashes message data; `topic-sha256` hashes a length-prefixed topic followed by data. Content-identical raw messages can therefore collapse into the same PubSub message. |
| `scoreInspectInterval` | Score snapshot period, default `1s` only when scoring is explicitly enabled. `0s` disables inspection. Setting this field alone does not enable scoring. |

GossipSub mesh parameters, active scoring, flood publishing, peer exchange, direct peers, and PeerGater apply to the GossipSub router. FloodSub can use custom protocol IDs without mesh features; RandomSub does not support a custom protocol list. Shared filters, validation, message IDs, publication, and discovery policies apply through the common PubSub API.

## All 32 GossipSub mesh parameters

These fields live under `gossipsub.params`. Defaults below are KPL's base defaults before a node preset or profile override.

| Field | Default | Meaning |
|---|---|---|
| `d` | 6 | Target mesh degree. |
| `dLow` | 5 | Mesh repair threshold. |
| `dHigh` | 12 | Mesh pruning threshold. |
| `dScore` | 4 | High-scoring peers retained during pruning. |
| `dOut` | 2 | Minimum outbound mesh connections. |
| `dLazy` | 6 | Minimum gossip recipient count. |
| `historyLength` | 5 | Message-cache history slots. |
| `historyGossip` | 3 | Recent history slots advertised through gossip. |
| `gossipFactor` | 0.25 | Fraction of eligible peers selected for gossip. |
| `gossipRetransmission` | 3 | Per-message retransmission limit in response to IWANT. |
| `heartbeatInitialDelay` | `100ms` | Delay before the first heartbeat. |
| `heartbeatInterval` | `1s` | Mesh maintenance interval. |
| `slowHeartbeatWarning` | 0.1 | Fraction of heartbeat interval used for slow-heartbeat warnings. |
| `fanoutTTL` | `1m` | Retention of inactive fanout state. |
| `prunePeers` | 16 | Peer-exchange entries offered in PRUNE. |
| `pruneBackoff` | `1m` | Backoff after pruning a mesh peer. |
| `unsubscribeBackoff` | `10s` | PRUNE backoff after unsubscribe. |
| `connectors` | 8 | Concurrent connection workers for peers obtained through PX. |
| `maxPendingConnections` | 128 | Pending PX connection queue limit. |
| `connectionTimeout` | `30s` | Timeout for those connections. |
| `directConnectTicks` | 300 | Heartbeats between direct-peer reconnect checks. |
| `directConnectInitialDelay` | `1s` | Initial direct-peer connection delay. |
| `opportunisticGraftTicks` | 60 | Heartbeats between opportunistic graft checks. |
| `opportunisticGraftPeers` | 2 | Peers selected by an opportunistic graft. |
| `graftFloodThreshold` | `10s` | Interval used to penalize early repeated GRAFT requests. |
| `maxIHaveLength` | 5000 | Maximum IDs included in IHAVE and accepted/requested from a peer per heartbeat. |
| `maxIHaveMessages` | 10 | IHAVE handling limit per peer per heartbeat. |
| `maxIDontWantLength` | 10 | Maximum IDONTWANT message-ID count included or accepted. |
| `maxIDontWantMessages` | 1000 | IDONTWANT handling limit per peer per heartbeat. |
| `iWantFollowupTime` | `3s` | Time to fulfill an IWANT promise before a behavioral penalty. |
| `iDontWantMessageThreshold` | 1024 | Message-size threshold in bytes for generating IDONTWANT. |
| `iDontWantMessageTTL` | 3 | Heartbeats retaining IDONTWANT state. |

Degree constraints include `dLow <= d <= dHigh`, `0 <= dScore <= d`, `dOut < dLow`, and `dOut <= d/2`. History requires `0 <= historyGossip <= historyLength`; `d`, `dHigh`, and `historyLength` are positive. Heartbeat interval and both tick counters are positive; other counts and durations are nonnegative. Gossip factor is finite and in `[0,1]`; slow-heartbeat warning is finite and nonnegative. Zero values can suppress activity and are not automatically replaced by defaults.

When PubSub is enabled on `boot`, its preset sets `dScore: 3`. Worker presets inherit `dLow: 5`, `dScore: 3`, `maxIHaveLength: 5500`, and `heartbeatInitialDelay: 1s` from v2. The [typed schema](../internal/model/gossipsub_config.go) and [default resolver](../internal/model/config.go) define the effective values.

## Peer scoring: explicitly disabled by default

Set **`gossipsub.score.enabled: true`** to install PeerScore. Omitting `score`, omitting `score.enabled`, or setting it to false disables scoring and inspection. An inactive block may preserve incomplete tuning; malformed durations, non-finite numbers, invalid CIDRs, invalid Peer IDs, and empty topic keys are still rejected. Active configurations additionally validate all relationships required by the router.

| `gossipsub.score` field | Meaning and active validation |
|---|---|
| `enabled` | Explicit opt-in; defaults to false. |
| `skipAtomicValidation` | Allow untouched score groups to remain disabled. Default false keeps the library's complete-group validation. |
| `topicScoreCap` | Nonnegative cap on the aggregate positive topic contribution; zero means uncapped. |
| `appSpecificWeight` | Finite P5 weight; either sign is allowed. Nonzero requires `appSpecificScore`. |
| `appSpecificScore.default` | Default unweighted P5 score, zero when omitted. |
| `appSpecificScore.peers` | Map from libp2p Peer ID to unweighted P5 score. An explicit zero overrides the default. Equivalent duplicate Peer IDs are rejected. |
| `ipColocationFactorWeight` | Nonpositive P6 weight; zero disables the penalty. |
| `ipColocationFactorThreshold` | At least 1 when the P6 weight is nonzero. |
| `ipColocationFactorWhitelist` | CIDR strings exempt from the colocation penalty. |
| `behaviourPenaltyWeight` | Nonpositive P7 weight; zero disables it. |
| `behaviourPenaltyThreshold` | Nonnegative penalty-counter threshold. |
| `behaviourPenaltyDecay` | Counter decay factor in `(0,1)` when P7 is enabled. |
| `decayInterval` | Counter-maintenance interval, at least `1s`. |
| `decayToZero` | Counter cutoff in `(0,1)`. |
| `retainScore` | Nonnegative disconnected-peer score retention; zero disables retention. |
| `seenMessageTTL` | Nonnegative score delivery-record retention; zero uses the library `2m` default. Distinct from `gossipsub.seenMessagesTTL`. |
| `thresholds` | Threshold block below. |
| `topics` | Map of exact joined topic names to complete TopicScore blocks below. Active score keys must name joined topics. |

P5 is calculated as the selected value multiplied by `appSpecificWeight`. The peer-specific map takes precedence over the default, and values remain fixed for that Peer instance. These are libp2p Peer IDs, not scenario node IDs, groups, or profile names. Every value and its weighted result must be finite. The callback reads an immutable in-memory map and does not make network requests.

With active selective validation, omitting the entire `decayInterval`/`decayToZero` group fills `1s`/`0.01`. Omitting the entire disabled time-in-mesh group fills a harmless `timeInMeshQuantum: 1s`. Explicit `0s` is rejected for these ticker/divisor settings. These defaults prevent crashes that the pinned library's selective validator otherwise permits; they do not turn on any scoring weight.

```yaml
gossipsub:
  topics: [kpl/default]
  score:
    enabled: true
    skipAtomicValidation: true
    appSpecificWeight: 1
    appSpecificScore:
      default: 2
    topics:
      kpl/default:
        skipAtomicValidation: true
        topicWeight: 1
        timeInMeshWeight: 0.01
        timeInMeshQuantum: 1s
        timeInMeshCap: 100
```

### All PeerScore thresholds

Under `gossipsub.score.thresholds`:

| Field | Constraint and behavior |
|---|---|
| `skipAtomicValidation` | Permit untouched threshold groups; default false. |
| `gossipThreshold` | Nonpositive score threshold for gossip propagation. |
| `publishThreshold` | At most `gossipThreshold`; suppresses flood/fanout publication below it. |
| `graylistThreshold` | At most `publishThreshold`; suppresses message processing below it. |
| `acceptPXThreshold` | Nonnegative score required to accept peer exchange. |
| `opportunisticGraftThreshold` | Nonnegative median mesh-score threshold for opportunistic grafting. |

### All TopicScore parameters

Under `gossipsub.score.topics.<joined-topic>`:

| Field | Meaning |
|---|---|
| `skipAtomicValidation` | Permit untouched score groups; default false. |
| `topicWeight` | Nonnegative multiplier for the complete topic contribution. |
| `timeInMeshWeight` | Nonnegative P1 weight; zero disables it. |
| `timeInMeshQuantum` | Positive mesh-time divisor. |
| `timeInMeshCap` | Positive cap when P1 is enabled. |
| `firstMessageDeliveriesWeight` | Nonnegative P2 weight. |
| `firstMessageDeliveriesDecay` | Factor in `(0,1)` when P2 is enabled. |
| `firstMessageDeliveriesCap` | Positive counter cap when P2 is enabled. |
| `meshMessageDeliveriesWeight` | Nonpositive P3 delivery-deficit weight. |
| `meshMessageDeliveriesDecay` | Factor in `(0,1)` when P3 is enabled. |
| `meshMessageDeliveriesThreshold` | Positive delivery target when P3 is enabled. |
| `meshMessageDeliveriesCap` | Positive counter cap when P3 is enabled. |
| `meshMessageDeliveriesActivation` | At least `1s` when P3 is enabled. |
| `meshMessageDeliveriesWindow` | Nonnegative near-first delivery accounting window. |
| `meshFailurePenaltyWeight` | Nonpositive P3b sticky mesh-failure weight. |
| `meshFailurePenaltyDecay` | Factor in `(0,1)` when P3b is enabled. |
| `invalidMessageDeliveriesWeight` | Nonpositive P4 invalid-delivery weight. |
| `invalidMessageDeliveriesDecay` | Factor in `(0,1)` for a configured P4 group, including zero weight in atomic validation. |

All floating-point score values must be finite, even for inactive weight terms. A missing topic score block leaves that topic unscored. With complete-group validation, supply a valid time-in-mesh quantum and invalid-delivery decay even when their weights are zero. The topology UI displays **Peer scores: Disabled** when scoring is off. Active score snapshots are available as `peerScores` through node/network/snapshot APIs and SSE; they are observations of the router, not fixed expected experiment outcomes.

## PeerGater: all 11 parameters

A `gossipsub.peerGater` block installs the validation-load gate; omit the block to leave it off. It is independent of PeerScore. Defaults come from the pinned library's `DefaultPeerGaterParams`.

| Field | Default / constraint |
|---|---|
| `threshold` | 0.33; finite and positive throttled/validated-message ratio threshold. |
| `globalDecay` | `ScoreParameterDecay(2m)`; finite factor in `(0,1)`. |
| `sourceDecay` | `ScoreParameterDecay(1h)`; finite factor in `(0,1)` for per-IP statistics. |
| `decayInterval` | `1s`; at least `1s`. |
| `decayToZero` | 0.01; finite factor in `(0,1)`. |
| `retainStats` | `6h`; nonnegative, zero disables retention. |
| `quiet` | `1m`; at least `1s` without throttling before the gate turns off. |
| `duplicateWeight` | 0.125; finite and positive. |
| `ignoreWeight` | 1; finite and at least 1. |
| `rejectWeight` | 16; finite and at least 1. |
| `topicDeliveryWeights` | Topic-to-weight map; finite positive values. |

## Filters, topics, and protocol selection

| Path under `gossipsub` | Configuration and behavior |
|---|---|
| `topicOptions.<topic>` | `messageId`, `subscriptionBufferSize`, and `validator`. Keys must name joined topics. Topic message ID and buffer settings override the global settings; buffers require subscribe mode. |
| `peerFilter` | Peer-ID lists `allow`, `deny`, and `topics.<topic>.allow/deny`. Global and topic allow restrictions both apply; either deny list wins. Empty allow lists impose no allow restriction. |
| `directPeers` | Transport multiaddresses ending in `/p2p/<Peer ID>` for GossipSub direct peers. The addresses must be reachable from Peer containers; configure both ends of a direct-peer relationship symmetrically. |
| `subscriptionFilter` | Topic `allowlist` or regular-expression `pattern`, plus optional positive `maxSubscriptions` per incoming subscription RPC. The configured local topics must pass the filter. |
| `blacklist` | Initial Peer-ID list `peers`; omitted `ttl` makes entries permanent for the Peer instance, while a positive TTL expires them lazily. |
| `protocols` | Ordered list of `{id, features}`. IDs are unique slash-prefixed strings. Features are `mesh`, `px`, `idontwant`, or exclusive `none`; PX/IDONTWANT require mesh. |
| `protocolMatch` | `exact` (default) or `prefix`. Prefix matching requires an explicit protocol list; the longest matching prefix supplies capabilities. |

An omitted feature list uses the library's feature mapping for a known protocol ID. A custom ID with no declared features has no mesh features. Peers in the same experiment must use compatible wire protocols and message ID policies.

### Message validators and RPC inspector

`defaultValidators` is a list of common validators; `topicOptions.<topic>.validator` adds that topic's validator. All supplied validators participate in validation. Each validator supports:

| Field | Meaning |
|---|---|
| `result` | Result after checks pass: `accept` (default), `reject`, or `ignore`. |
| `failureResult` | Failed checks return `reject` (default) or `ignore`. Rejection contributes to invalid-message scoring; ignoring does not mean delivery. |
| `minBytes`, `maxBytes` | Nonnegative bounds on PubSub message data, with minimum no greater than maximum. Envelope data includes its encoding overhead. |
| `pattern` | Go regular expression matched against message data. |
| `format` | `any` (default), `json`, or KPL `envelope`; envelope validation checks required ID, run, publisher, and positive timestamp fields. |
| `delay` | Nonnegative synthetic validation delay; canceled work returns ignore. |
| `timeout` | Nonnegative validator timeout; `0s` uses the library's no-timeout behavior. |
| `concurrency` | Positive per-validator concurrency limit; library default 1024. |
| `inline` | Execute synchronously in the validation path; default false. |
| `basicSeqno` | Default false. True uses the library's basic sequence-number validator with an in-memory nonce store; requires the default/strict-sign policy. Old/repeated sequences are ignored; malformed sequences use `failureResult`. |
| `useValidatorData` | Allow process-local `ValidatorData.result` to choose accept/reject/ignore after content checks. |

`rpcInspector` inspects incoming RPC envelopes. Optional nonnegative `maxMessages`, `maxSubscriptions`, `maxControlEntries`, `maxMessageIds`, and `maxBytes` reject envelopes exceeding their limits; zero is an actual zero limit. `rejectPeers` rejects named sender Peer IDs. Control entries count IHAVE, IWANT, GRAFT, PRUNE, and IDONTWANT protobuf entries; message-ID counts sum IHAVE/IWANT/IDONTWANT references. Byte limits use protobuf serialized RPC size.

### Discovery, authors, and publication

An omitted `discovery` block retains KPL's capacity-aware Controller discovery loop. Explicit `discovery.mode: controller` (also the mode of `{}`) uses Controller candidates through libp2p's discovery pipeline; `routing` uses Kademlia and prefixes the namespace with the run ID; `none` disables topic discovery. Routing discovery requires Kademlia with providers and provider storage enabled. These options do not remove the separate initial bootstrap connection procedure.

`discovery.ttl` is a positive advertisement TTL and `limit` is a positive requested result limit. For Controller discovery, TTL controls renewal of the no-op advertisement callback; Controller liveness still follows Agent reports. An optional `connector` configures exponential connection backoff:

| Connector field | Default when the connector block exists |
|---|---|
| `cacheSize` | 100; positive. |
| `dialTimeout` | `2m`; positive. |
| `minBackoff`, `maxBackoff` | `10s`, `1h`; positive and minimum no greater than maximum. |
| `timeUnit` | `1s`; positive. |
| `base` | 5; finite and greater than 1. |
| `offset` | `0s`; nonnegative. |
| `jitter` | `full` or `none`; default full. |
| `seed` | 1; integer seed for backoff jitter. |

Without a connector block, libp2p supplies its connector defaults. Routing discovery is affected by DHT admission, storage, provider, and refresh settings.

`author` selects the PubSub author independently of the Peer transport identity. Fields are `peerId`, base64 libp2p-marshaled `privateKey`, or `noAuthor`. A supplied Peer ID must match the private key; a signed author supplied only by ID needs its key in the host peerstore. `noAuthor: true` conflicts with the other fields, requires an unsigned signature policy, and requires a content-based message ID on every topic. Explicit private keys are part of saved scenario inputs and downloadable archives; use experiment keys.

`publish` configures each application publication:

| Field | Behavior |
|---|---|
| `readinessMinPeers` | Nonnegative minimum `pubsub.ListPeers(topic)` count, checked before generating payloads and timestamps. This is a known remote topic-peer count, not an upstream `MinTopicSize` mesh predicate or convergence guarantee. |
| `readinessTimeout` | Positive optional bound up to `10s`; requires `readinessMinPeers` and remains within the overall publish-request budget. |
| `local` | Default false. True requests a local publication, bypasses remote readiness, and excludes that publication from remote session-delivery metrics. |
| `validatorData` | Process-local map; supported `result` values are accept/reject/ignore and require a validator with `useValidatorData`. It is not serialized onto the P2P wire. |
| `author` | Per-publication author override using the same identity fields; requires a private key and a signing policy, and cannot use `noAuthor` or be combined with `local: true`. |

## Kademlia options

All fields below are under `kademlia`; they are applied when Kademlia is enabled. The library's datastore and provider interfaces are exposed through the named policies below.

| Field or group | Meaning |
|---|---|
| `enabled`, `mode` | Enabled by default; mode is `server` by default, or `client`, `auto`, `auto-server`. |
| `protocolPrefix` | Default `/k-p2p-lab/v3`; prefix for the DHT protocol. |
| `protocolId` | Exact V1 protocol override; mutually exclusive with prefix and extension. |
| `protocolExtension` | Extension appended to the prefix. |
| `bucketSize` | Positive bucket capacity; KPL default 20. |
| `concurrency` | Positive lookup parallelism. |
| `resiliency` | Nonnegative lookup resiliency. |
| `lookupCheckConcurrency` | Positive concurrent routing-table admission checks. |
| `routingTableLatencyTolerance` | Nonnegative peer latency admission threshold. The pinned upstream v0.31.0 accepts this option but constructs its routing table with a fixed `1m` threshold, so configured overrides have no effect. The field remains for configuration compatibility. |
| `routingTableRefreshPeriod` | Positive automatic routing-table refresh period. |
| `routingTableRefreshTimeout` | Positive timeout for refresh queries. |
| `maxRecordAge` | Nonnegative maximum value-record age. |
| `disableAutoRefresh`, `disableProviders`, `disableValues` | Explicit switches for these DHT facilities. |
| `optimisticProvide`, `optimisticProvideJobsPoolSize` | Enable optimistic providing and set its positive background-job pool size. |
| `bootstrapTimeout`, `bootstrapRetryInterval` | KPL's initial transport bootstrap budget and retry interval, default `60s` and `1s`; both positive. |
| `bootstrapSource`, `bootstrapPeers` | DHT bootstrap source: `none`, `static`, or `controller`. Static peers require transport `/p2p/<Peer ID>` multiaddresses; the controller source reads run-scoped bootstrap peers. This controls DHT recovery bootstrap separately from KPL's initial transport bootstrap. |
| `queryFilter`, `routingTableFilter`, `addressFilter` | Each contains `policy`, `allowCIDRs`, and `denyCIDRs`; details below. |
| `routingTableDiversity` | `enabled`, positive `maxPerCpl`, and positive `maxForTable`. An existing block defaults enabled; omitted limits use the pinned library's diversity defaults. |
| `datastore` | `memory` (default behavior) or `null`. Memory is per Peer and is lost when its container stops; null discards records. |
| `validator` | `mode: default` retains built-in `pk`/`ipns` validators and can add namespaces; `mode: namespaced` supplies the complete namespace map. `namespaces.<name>` selects `public-key`, `ipns`, `bytes`, or `reject`. |
| `providerStore` | `mode: manager` (default behavior) or `null`. Manager accepts positive `cleanupInterval` and `cacheSize`; null discards provider records and cannot use those tunings. |
| `requestHook` | `none` (default) or `log` for inbound DHT request INFO logs. |

Filter policies are `all` (empty/default), `public`, `private`, and `non-loopback`. Query/routing-table public/private policies use the corresponding library filters; notably, the library's private query policy accepts any nonempty address set. Use CIDR constraints for exact experiment ranges. Address filtering applies per address. A deny CIDR on any address rejects a whole query/routing-table candidate; allow lists require at least one matching IP. `non-loopback` requires a non-loopback global-unicast IP, including private unicast addresses.

An omitted DHT bootstrap source retains the library option default; it does not replace KPL's initial Controller bootstrap. An explicit empty static list or `none` supplies no DHT recovery bootstrap peers. The `public-key` policy is restricted to the `pk` namespace, and `ipns` to the `ipns` namespace. The `bytes` record validator accepts opaque values and selects the lexicographically greatest candidate; `reject` rejects all records. These policies configure protocol behavior and do not add scenario DHT Put/Get/Provide actions.

The standard `/ipfs` namespace, including exact `protocolId: /ipfs/kad/1.0.0`, keeps upstream constraints: bucket size 20, values/providers enabled, and only standard `pk`/`ipns` validators. A custom experiment prefix or exact protocol ID allows different policies. For a custom exact ID, KPL also sets an internal validation prefix so upstream default-namespace checks do not reject its tuning; the configured exact wire ID remains unchanged. Public-address filtering can exclude Swarm's private overlay addresses; choose filters that match the intended experiment network.

## Source boundaries

The schema is split across [common node configuration](../internal/model/config.go), [GossipSub parameters](../internal/model/gossipsub_config.go), [PubSub policies](../internal/model/gossipsub_policies.go), [scoring](../internal/model/score_config.go), and [Kademlia policies](../internal/model/kademlia_config.go). Runtime adapters live in matching `internal/peer` files.


Scalar struct fields are covered by the typed schema. Library options that accept Go functions or interfaces use the named policies described here. Arbitrary score functions, validators, stores, discovery providers, feature tests, tracer implementations, request hooks, and Kademlia `WithCustomMessageSender` implementations require a code extension; YAML does not execute Go or load arbitrary implementations. KPL retains its telemetry tracers and Peer network isolation so protocol options remain observable within the experiment system.

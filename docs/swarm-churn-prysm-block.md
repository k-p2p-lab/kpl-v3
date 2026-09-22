# Three delay cohorts with Prysm beacon-block scoring

English | [Korean](swarm-churn-prysm-block.kr.md)

[`examples/swarm-churn-prysm-block.yaml`](../examples/swarm-churn-prysm-block.yaml) extends the supplied 10-boot, 5-minute warm-up, 4-KiB experiment with three concurrent churn jobs. All workers share `kpl/prysm/beacon_block`; publisher cohorts rotate while all three cohorts receive messages.

## Cohorts and scheduling

| Group/profile | Outbound P2P delay | Jitter | Packet loss | Total join budget | Mean join interval |
|---|---:|---:|---:|---:|---:|
| `delay-low` | 25ms | 2ms | 0.5% | 3,334 | 0.3s |
| `delay-medium` | 100ms | 2ms | 0.5% | 3,333 | 0.3s |
| `delay-high` | 250ms | 2ms | 0.5% | 3,333 | 0.3s |

Only delay differs between worker profiles. Balanced placement spreads each cohort across available Agents, so cohorts are not physical hosts. Network shaping covers outbound P2P TCP, including DHT, and excludes Controller HTTP/telemetry. Configured egress delays are not RTTs, and packet loss is not application delivery loss.

Pareto lifetime `xm: 120s`, `alpha: 2.5` has mean **200s**. The three sequential producers use exponential mean `0.3s` gaps, preserving an ideal aggregate rate of **10 joins/s** on the cumulative arrival schedule. Request processing consumes the sampled intervals; capacity stalls can still delay arrivals, which catch up in order without being skipped. The stationary expectation is **667 workers/cohort, about 2,000 workers plus 10 boots**. This is not a concurrency cap or measured capacity guarantee. Lifetime includes startup after successful container creation.

**40 free slots cannot sustain this rate.** Provision headroom and measure host load, or change the shared `&churn` interval: mean `10s` gives an ideal 60 workers plus 10 boots; mean `30s` gives 20 workers plus 10 boots. Aliases inherit this setting. Capacity waiting can distort arrival rates and cohort balance. The three budgets sum to 10,000; they are not 10,000 each.

1. Start ten stable boot peers with PubSub disabled and wait for their readiness.
2. Start `churn-low`, `churn-medium`, and `churn-high` with `await: false`. Each job is sequential internally (`parallel: false`); the three executions overlap without a barrier synchronizing their first joins.
3. Warm up for 5 minutes. Workers use the common Controller topic-discovery registry and GossipSub overlay. Do not wait for readiness of historical churn cohorts, which include expired nodes.
4. Run 30 publish rounds, rotating **low → medium → high** ten times. Each round randomly selects up to ten distinct eligible publishers within its cohort. Each cohort gets at most 100 operations; churn can reduce successful publications. `group` filters publishers, not subscribers.
5. Preserve 4,096-byte envelope payloads, `1s` delivery windows, exponential mean `1s` intra-round gaps, and 29 explicit `10s` inter-round waits. Empty cohorts still get the explicit wait.
6. Collect for another 30 seconds, then `stop-all` cancels remaining jobs and removes this run's peers. Churn continues while its finite join budgets last.

With ten publishers every round, nominal waiting time is `300 + 30×9 + 29×10 + 30 = 890s`, **14m50s**, plus bootstrap, requests, and cleanup. Slow requests can extend the run and exhaust join budgets earlier. Publisher rotation replaces the supplied pooled random selection with equal per-cohort round budgets. See [scheduling and measurement details](swarm-churn-publish.md).

## Prysm baseline

Parameters come from **Prysm v7.1.8**, commit `51b5a75ebbadf05af22bd2601b5baf7a9e99b66d`: [`peerScoringParams`, `defaultBlockTopicParams`, and timing helpers](https://github.com/OffchainLabs/prysm/blob/51b5a75ebbadf05af22bd2601b5baf7a9e99b66d/beacon-chain/p2p/gossip_scoring_params.go), evaluated with [mainnet's 12-second slots and 32-slot epochs](https://github.com/OffchainLabs/prysm/blob/51b5a75ebbadf05af22bd2601b5baf7a9e99b66d/config/params/mainnet_config.go). Only configuration values are added; no Prysm dependency or source fork is installed.

| Component | Value |
|---|---|
| Topic weight | `0.8` |
| P1 mesh time | Weight `10/300`, quantum `12s`, cap `300` |
| P2 first deliveries | Weight `1`, cap `23`, decay `0.9928302477768374` |
| P3 / P3b mesh penalties | **Both weights `0`**, matching Prysm's `meshDeliveryIsScored = false` |
| P4 invalid deliveries | Weight `-140.4475`, decay `0.9971259067705325` |
| P5 application | Constant `0`, weight `1` |
| P6 IP colocation | Weight `-35.11`, threshold `10`, no whitelist |
| P7 behaviour | Weight `-15.92`, threshold `6`, decay `0.9857119009006162` |
| Global | Topic cap `32.72`, decay every `12s` to `0.01`, retain `10h40m` |
| Thresholds | Gossip `-4000`, publish `-8000`, graylist `-16000`, accept PX `100`, opportunistic graft `5` |

Per-slot decay for `E` epochs is `0.01^(1/(32×E))`: P2 uses 20 epochs, P4 50, P7 10. The inactive mesh terms retain 5-epoch decay, threshold `16`, cap `160`, activation `25m36s`, and window `2s`. Keep both mesh weights zero for this baseline. Inspection runs every `1s`, independently of the `12s` decay interval.

This is **KPL synthetic traffic with Prysm scoring**, using KPL discovery, Kademlia, envelopes, and the requested mesh degree `6/5/12` with `dScore: 3`. It does not reproduce Ethereum SSZ/snappy blocks, fork digests, consensus validation, or mainnet block timing. Ordinary transaction gossip belongs to Ethereum's [execution network](https://ethereum.org/developers/docs/networking-layer/); the selected baseline is the consensus `beacon_block` topic.

Lower delay may improve P2 first-delivery rewards; P1 also depends on mesh age. Measure that hypothesis rather than assuming a ranking. With P3/P3b disabled, slow delivery alone does not cause a delivery-deficit penalty. Valid synthetic traffic does not exercise Ethereum-specific P4 failures. Shared apparent IPs or protocol behaviour penalties can affect scores independently of delay. Short-lived peers rarely reach the one-hour P1 cap. `retainScore` is router policy for disconnected peers, not historical logging or persistence across new Peer IDs.

## Run and collect

Complete [Swarm deployment](swarm.md), validate, then paste the printed YAML into the Dashboard and log in with the configured dashboard account and start the run:

```sh
go run ./cmd/kpl validate --scenario examples/swarm-churn-prysm-block.yaml
sh scripts/swarm.sh scenario examples/swarm-churn-prysm-block.yaml
```

A topology node's score count/mean describes its observations: `peerScores[remotePeerId]` is **the observer's opinion of the remote peer**, not the observer's own reputation. Compare observer-group × evaluated-group scores, mesh membership, age, and ready populations.

The Controller automatically saves group score and topology summaries about every five seconds in `observations.jsonl`, included in the result ZIP and shown by [Saved results → Images](visualization.md). These are observer-group summaries; they do not retain individual score pairs, evaluated-group breakdowns or full mesh history, and Prometheus does not expose score time series. For observer-group × evaluated-group analysis, run this Bash collector from the repository during warm-up and stop it with Ctrl-C after the final collection window. Replace the Controller URL and actual run ID. It requires `curl` and `jq` and uses the public read-only snapshot API. Each interval includes request/processing time plus 5 seconds.

```bash
KPL_CONTROLLER_URL=http://control-node:8080
KPL_RUN_ID=REPLACE_WITH_RUN_ID
set -o pipefail
while curl --fail --silent --show-error --max-time 30 \
    "$KPL_CONTROLLER_URL/api/v1/snapshot" |
  jq -ce --arg run "$KPL_RUN_ID" -f scripts/score-cohorts.jq \
    >> "$KPL_RUN_ID-score-groups.jsonl"
do
  sleep 5
done
```

[`score-cohorts.jq`](../scripts/score-cohorts.jq) keeps ready peers on online Agents and transport-connected scored pairs in that run, excluding retained scores of disconnected peers. It emits directed group means/min/max, negative-pair counts, mesh-pair counts, and starting/ready populations. Unlike the automatic summaries, which include all scores from fresh reporting observers, this collector restricts scores to currently transport-connected pairs. Its means can therefore differ. Pair counts are observations, not unique evaluated peers; missing rows mean no qualifying scores, not zero. Means weight directed pairs equally. Status can be stale or repeated; `generatedAt` is API generation time, not the exact score-inspection time. Inspect node `lastSeen` and Agent health when diagnosing stale data. For raw per-peer data, save a snapshot too:

```sh
curl --fail --output "$KPL_RUN_ID-snapshot.json" \
  "$KPL_CONTROLLER_URL/api/v1/snapshot"
jq --arg run "$KPL_RUN_ID" -f scripts/score-cohorts.jq \
  "$KPL_RUN_ID-snapshot.json"
```

Grafana **KP2PLab Experiment Analysis** provides run-level delivery/coverage, latency, and graft/prune traffic. `kpl_nodes` and `kpl_network_configured_*` have group labels; existing delivery/latency metrics do not. Balanced placement means Agent filters cannot substitute for cohorts. A `1s` delivery deadline can miss multi-hop or TCP-retransmitted messages without invalid-message penalties. Check unknown pairs, pending publications, and telemetry drops. Export the result ZIP separately for complete collected events and session-window metrics; the score JSONL remains a separate artifact.

For a scoring-off control, copy the YAML, change its `name`, and set the shared `score.enabled` to `false`; all three profiles inherit it. Keep delays, churn, mesh, publishing, and seed fixed and run it separately. Compare repeated runs with actual populations and publication counts: a seed does not reproduce container timing or protocol outcomes. See [metrics](experiment-metrics.md) and [protocol options](protocol-options.md).

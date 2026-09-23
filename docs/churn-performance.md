# Controller Performance Investigation and Benchmarks

English | [Korean](churn-performance.kr.md)

[Documentation index](README.md) · [Repository](../README.md)

This is implementation evidence, not a capacity guarantee. For operating an existing deployment, start with [Agent capacity](agents.md) and [monitoring](monitoring.md). The investigation below records earlier measurements; it has not been rerun merely by reorganizing this documentation.

The 2026-09-22 investigation observed 65,227 retained Peer records. Recent five-minute Prometheus averages showed about 7.17 CPU cores and approximately 526 MB/s of Controller memory allocation. Allocation rate is distinct from live memory size; it also includes the cost of creating and reclaiming short-lived objects.

## Investigated paths and implementation changes

- `/api/v1/bootstrap` previously generated a full snapshot to obtain addresses, copying/sorting all historical Peers and calculating metrics/topology. It now reads ready boot addresses for that run from the active-node index.
- Discovery and occupancy checks for partial heartbeats also use the active-node index. Partial heartbeat processing no longer scans the entire history for omitted nodes.
- Dashboard SSE excludes terminal nodes before snapshot construction, avoiding copies and sorting that would later be discarded. Full inventories and saved results retain history.
- Agent, experiment, and recent-event REST endpoints copy only the requested lists. Agent registration builds Agent data without a full Peer snapshot.
- Prometheus reuses propagation-latency histograms for unchanged completed experiments. New events, delayed evidence, and delivery-window expiry still update summaries and histograms. Metric definitions and saved-result calculations remain in effect.
- Agent/Controller event-size validation inspects and discards each encoded event's length instead of allocating the complete batch again. JSON escaping, the exact 10 MiB boundary, and rejection of a batch containing an invalid later event are retained.

## Reproducible benchmarks

Recorded on Intel Xeon E5-2620 v3 with `GOMAXPROCS=2` and identical inputs. Lookup fixtures contain 65,000 terminal Peers and 100 active Peers. Event validation uses 250 events with 4 KiB strings. MB and KB mean 1,000,000 and 1,000 bytes.

| Path | Before: time/op | After: time/op | Before: allocation/op | After: allocation/op |
| --- | ---: | ---: | ---: | ---: |
| Bootstrap address lookup | 196.56 ms | 0.111 ms | 149.22 MB | 28.86 KB |
| Dashboard frame creation | 242.60 ms | 1.132 ms | 171.83 MB | 320.91 KB |
| Event-size validation | 4.33 ms | 1.70 ms | 7.05 MB | 12.26 KB |

After the change, active-node lookup allocated similar amounts with no terminal history and with 65,000 records. Production CPU also depends on active population, topology, event rates, and Docker load. These are local path benchmarks; overall CPU and arrival rates require another experiment after deployment.

```bash
GOMAXPROCS=2 go test ./internal/controller ./internal/model \
  -run '^$' \
  -bench 'Benchmark(BootstrapChurnHistory|DashboardChurnHistory|ValidateTelemetryBatch)$' \
  -benchtime=300ms -benchmem
```

Applying these changes requires rebuilding and redeploying Controller and Agent images; deleting history is not a prerequisite. Compare the following metrics and Controller `churn join schedule delayed` logs using the same scenario and active Peer population:

```promql
rate(process_cpu_seconds_total{job="kpl-controller"}[5m])
rate(go_memstats_alloc_bytes_total{job="kpl-controller"}[5m])
sum by (state) (kpl_nodes)
kpl_local_telemetry_queue_events
```

## SSE payload evidence

An earlier synthetic trace-heavy 20-second fixture (21 updates) reduced JSON payload from 85,767,024 to 170,144 bytes when using the compact Dashboard stream. This is a regression-fixture measurement, not a mobile-network measurement or a bandwidth guarantee for arbitrary cluster sizes. [Dashboard SSE](api.md#dashboard-sse) owns the current format; [connection diagnosis](monitoring.md#dashboard-stream-traffic) describes operational checks.

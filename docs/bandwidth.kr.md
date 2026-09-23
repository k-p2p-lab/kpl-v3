# P2P Bandwidth 측정

[English](bandwidth.md) | 한국어

[문서 안내](README.kr.md) · [저장소](../README.kr.md)

v3는 **실제로 사용한 libp2p 스트림 대역폭**을 측정합니다. 물리 회선의 최대/가용 용량을 측정하는 부하 시험은 아닙니다. 모든 v3 구성요소(Controller, Agent, Peer 이미지)를 다시 빌드하고 적용한 뒤 시작한 실험부터 기록됩니다. Agent의 엄격한 이벤트 디코더도 새 필드를 알아야 하므로 Peer만 교체하지 마십시오. 기존 실험의 payload 크기에서 과거 P2P 트래픽을 역산하지 않습니다.

## 측정 범위와 계산

Peer의 libp2p `BandwidthReporter`에 동기식 정수 카운터를 연결했습니다. 현재 의존성의 `swarm_stream.go`가 각 스트림 read/write의 실제 반환 바이트 수를 global hook과 protocol/remote hook에 보고합니다. global hook은 총량을, stream hook은 귀속만 갱신하므로 같은 바이트를 두 번 더하지 않습니다. libp2p 기본 flow meter의 비동기 갱신 시점을 기다리지 않아 짧은 전송의 마지막 바이트도 즉시 조회할 수 있습니다.

| 포함 | 제외 |
|---|---|
| GossipSub 데이터, 서명·protobuf/RPC 인코딩, 전달/중복 복사, control 메시지 | HTTP API·telemetry·bootstrap 레지스트리 등 관리 트래픽 |
| Kademlia, Identify, ping 등 실제 libp2p 스트림 프로토콜 | TCP/IP 헤더·재전송, Noise 암호화 오버헤드, Yamux framing, Docker overlay/VXLAN |
| 스트림 프로토콜 협상 바이트 | NIC 전체 사용량, 회선 최대 용량, 사용 가능한 여유 용량 |

프로토콜은 `/meshsub/1.2.0` 등의 **실제 negotiated ID**입니다. 비어 있는 ID는 미지정/협상 중 바이트입니다. 협상 단계에서 송신 측에는 선택된 프로토콜, 수신 측에는 빈 ID로 귀속될 수 있습니다. 진행 중인 read/write의 총량 갱신과 프로토콜 귀속 사이에는 짧은 차이도 생길 수 있습니다. 송신 성공은 libp2p 스트림에 쓴 바이트이며, 상대 애플리케이션의 최종 전달 성공을 증명하지 않습니다.

libp2p host를 구성할 때 누적값은 0으로 시작하며 다음과 같이 전송률을 계산합니다.

```text
sent bit/s     = 8 × (sentBytes_now - sentBytes_previous) / elapsed_seconds
received bit/s = 8 × (receivedBytes_now - receivedBytes_previous) / elapsed_seconds
```

경과시간은 Peer의 **단조 시계**입니다. Controller 시계 동기화가 바뀌어도 비율 계산의 분모가 바뀌지 않습니다. 첫 표본은 host 생성 시점의 0에서 계산합니다. 초기 Controller 시계 동기화 시도 후 첫 샘플, 이후 5초마다 샘플, 정상 host 종료 후 마지막 `final: true` 샘플을 telemetry 종료 flush 전에 생성합니다. 처음 동기화가 실패해도 수집은 계속합니다. 여러 Peer의 시계열 배치는 이벤트 시각을 사용하므로 시계 미동기화·보정 오차의 영향을 받을 수 있습니다. 이벤트의 `clockBasis`/`clockUncertaintyMs`를 함께 확인할 수 있습니다.

## 저장과 품질

`runs/<id>/events.jsonl`의 `type: "bandwidth"` 이벤트는 기존 `runId`, `agentId`, `nodeId`, `sessionId`, `sequence`, `timestamp`와 다음 typed 필드를 포함합니다.

```json
{"bandwidth":{"elapsedNs":5000000000,"receivedBytes":1000,"sentBytes":2000,"protocols":[{"protocol":"/meshsub/1.2.0","receivedBytes":1000,"sentBytes":2000}],"final":false}}
```

- `(nodeId, sessionId)`마다 최신 누적값을 보관합니다. 같은 이벤트의 재전송은 기존 식별자로 제거합니다. 재시작은 새 세션입니다.
- 중간 샘플이 빠지면 다음 누적값이 바이트 총량을 복구합니다. 그 구간의 전송률은 더 긴 기간의 평균이며 사라진 순간 피크를 복원하지 않습니다.
- 늦게 도착한 이전 표본은 `supersededSamples`에 집계합니다. 같은 세션에서 역행하는 카운터, 잘못된 프로토콜 카운터, 누락된 세션/시각, 전체 실행의 signed 64-bit 누적량을 넘치는 표본은 `rejectedSamples`에 집계하고 계산에서 제외합니다. 누락된 소스 시각을 Controller 수신 시각으로 대체하지 않습니다.
- `finalizedSessions / sessions`는 **한 번이라도 보고한 세션** 중 종료 샘플까지 받은 비율입니다. 일반 telemetry 큐에는 마지막 유실 알림·종료 체크포인트·대역폭 표본을 위한 자리를 예약합니다. 강제 종료나 통신 장애로 마지막 샘플이 유실될 수는 있습니다. 종료 샘플이 없는 꼬리 구간은 미지수입니다. 전혀 보고하지 않은 Peer는 이 비율의 분모에도 없으므로 100%가 전체 실험의 완전성을 증명하지 않습니다.
- 수집된 0은 0으로, 표본이 없는 이전 결과는 N/A로 표시합니다. 송신과 수신은 같은 전송의 양 끝을 셀 수 있으므로 합계를 고유 네트워크 트래픽으로 해석하지 않습니다. 설정한 payload 바이트를 세는 기존 `kpl_message_bytes_total`은 별개의 지표입니다.

`metrics.bandwidth`에는 `scope: "libp2p-stream-v1"`, 전체 및 프로토콜별 송수신 누적 바이트, sessions/finalizedSessions/samples/rejectedSamples/supersededSamples/latestAt가 있습니다. 과거 결과에는 필드가 없으며, 유효 표본이 없으면 수치를 사용하지 않습니다. 결과 ZIP의 원본 이벤트와 `metrics.json`, 분석 API에 포함됩니다. 저장 결과 분석과 ZIP은 Controller 재시작 후에도 전체 파일을 재생하여 복원합니다.

## 내장 Dashboard

Dashboard의 **P2P B/W · Send / Receive** 카드는 같은 **Metrics** 행에 포함됩니다. 대표 값은 현재 실행의 송수신 전송률이며, 각 카드를 펼치면 누적 전송량과 설명을 확인합니다. 마우스·키보드·터치 조작은 [Metrics 사용법](dashboard.kr.md#dashboard-내장-시각화)을 참고하십시오. 전송률은 각 Peer의 마지막 소스 측정 구간 평균을 합산하며, 단위는 bit/s·kbit/s·Mbit/s 등으로 자동 표시합니다. 누적량은 B·KiB·MiB 등으로 표시합니다. 토폴로지의 topic/layer 필터와 독립적이며 상단 **Run metrics**에 표시된 실행을 따릅니다.

**B/W measurement**는 같은 행에서 수집 품질을 표시하며, 펼치면 신선한 활성 세션 수, 종료 표본 수, 마지막 표본 시각과 거부 표본 수를 보여 줍니다. 일부 활성 세션만 신선하면 `Partial`, 전송률을 알 수 없으면 `Rate unavailable`, 유효 표본이 없으면 `N/A`입니다. 실제 측정한 0은 0으로 표시합니다. 15초 신선도 기준과 정상 종료 후 0 전송률은 Grafana와 같으며 누적량은 유지합니다. [대시보드 스트림](api.kr.md#dashboard-sse)은 trace 이벤트가 없어도 시간에 따라 달라지는 값을 확인해 만료된 전송률을 갱신합니다. 라이브 스냅샷의 `metrics.bandwidth.currentRates`에 이 집계가 포함되며, 저장 결과 집계에는 현재 시각에 따른 전송률을 추가하지 않습니다.

**Saved results → Images**는 전체·프로토콜별 송신·수신 전송률(kbit/s)과 누적 전송량(KiB)을 PNG·CSV로 제공하며 전체 차트를 ZIP으로 다운로드할 수 있습니다. 원본 누적 카운터는 결과 API·ZIP과 Grafana에도 있습니다. 차트 조작과 비교 방법은 [시각화](visualization.kr.md)를 참고하십시오.

API는 기존 분석 응답에 `bandwidthTimeline`, `bandwidthBinSeconds`를 추가합니다. 각 구간은 `at`, 송수신 바이트, 프로토콜별 바이트, `peerSeconds`(그 구간에 배분된 수집 기간의 Peer·초 합계)를 가집니다. `peerSeconds`는 중복 없는 노드 수나 측정 완전성 비율이 아닙니다. 기본 5초, 최대 360구간으로 병합합니다. 각 구간 바이트는 시간 겹침 비율 때문에 소수일 수 있으나 전체 합계는 정수 누적 카운터와 일치합니다(부동소수점 반올림 제외). 기록이 없는 구간에는 선을 잇지 않습니다. 일부 Peer만 보고한 구간의 값은 보고된 Peer들의 사용량입니다.


## Prometheus / Grafana

| 지표 | 종류 / 추가 라벨 |
|---|---|
| `kpl_p2p_stream_bytes_total` | Counter / `direction` (`send`, `receive`) |
| `kpl_p2p_protocol_stream_bytes_total` | Counter / `direction`, `protocol` |
| `kpl_p2p_stream_bits_per_second` | Gauge / `direction` |
| `kpl_p2p_protocol_stream_bits_per_second` | Gauge / `direction`, `protocol` |
| `kpl_p2p_bandwidth_sample_timestamp_seconds` | Gauge / 그룹의 가장 최근 소스 표본 시각 |
| `kpl_p2p_bandwidth_sessions` | Gauge / `state` (`open`, `final`)별 세션 수 |

공통 라벨은 `run_id`, `agent_id`입니다. 내보내기 전에 세션별 누적량과 유효 전송률을 합산하여 Peer 교체가 Prometheus 시계열을 계속 늘리지 않도록 합니다. 노드·세션 상세 정보는 원본 로그와 저장 분석에 유지됩니다. topic 귀속은 제공하지 않습니다. 하나의 GossipSub RPC/스트림에 여러 topic과 topic 없는 control 데이터가 섞일 수 있기 때문입니다.

Grafana의 **P2P stream bandwidth** 영역은 전체/프로토콜별 사용률, 누적량, 종료 표본 대기 세션 수를 표시합니다. Run/Agent 필터를 사용하며 Topic 필터는 적용하지 않습니다. 전송률 Gauge는 마지막 **소스 수집 간격**의 평균이므로 `rate()`를 다시 씌우지 않습니다. 비종료 소스 표본의 시각이 Controller 시각보다 15초 넘게 과거이거나 미래이면 집계 전송률에서 해당 세션을 제외합니다. 종료 표본을 받은 세션은 현재 사용률 0으로 합산합니다. 유효한 표본이나 종료된 세션이 하나도 없는 그룹은 전송률을 생략하며, 누적 카운터에는 종료된 세션의 바이트도 유지합니다. 여러 실행 선택 시 범례로 실행을 구분합니다.

실시간 메모리 지표는 현재 Controller 실행 중 받은 세션에 대한 값입니다. Controller 재시작 시 과거 모든 로그를 자동으로 Prometheus 메모리에 적재하지 않습니다. 기존 Prometheus 저장 이력은 그 서버의 보존 정책을 따르며, 저장 결과 화면/ZIP이 재시작 후 전체 실험을 복원하는 경로입니다. 새 누적 샘플이 들어오면 해당 Peer 세션의 이전 바이트도 즉시 복구됩니다. 짧은 실험이 Prometheus scrape 사이에 끝나도 최종 누적량은 이후 scrape에 잡힐 수 있지만, 중간 전송률 파형은 저장 결과 분석에서 확인해야 합니다.

[저장 결과 시각화](visualization.kr.md) · [v2 전체 분석 대조](v2-analysis-coverage.kr.md)

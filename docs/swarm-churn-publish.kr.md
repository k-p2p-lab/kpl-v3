# Swarm에서 Worker Churn 중 반복 발행

[English](swarm-churn-publish.md) | 한국어

[문서 안내](README.kr.md) · [저장소](../README.kr.md)

[`examples/swarm-churn-publish.yaml`](../examples/swarm-churn-publish.yaml)은 worker가 계속 들어오고 나가는 동안 현재 살아 있는 worker를 무작위로 골라 반복 발행하는 예제입니다. 작은 [Swarm smoke 실험](swarm.kr.md#첫-배포부터-실험과-다운로드까지)에 지속적인 churn과 반복 관측을 더합니다. 실측 용량을 보장하거나 v2 실험을 그대로 재생하는 구성이 아닌 시작용 설정입니다.

## 준비와 실행

먼저 [Swarm 배포 절차](swarm.kr.md)를 완료하십시오. **online Agent 두 개 이상과 전체 여유 슬롯 40개 이상**으로 시작합니다. 다른 실험이 없는 capacity 20의 Agent 두 개가 한 예입니다. **Agent status**에서 `online` 행만 세고 **Peers**의 점유량 / 용량을 확인하십시오. 요약의 **Available slots**에는 offline Agent도 포함될 수 있습니다. 비교가 쉽도록 다른 실험은 종료해 두십시오.

배포할 때와 같은 사용자·Docker context를 사용합니다. 아래 명령은 sudo를 사용하며, 배포 계정에 이미 Docker 권한이 있다면 일관되게 생략합니다.

```sh
sudo sh scripts/swarm.sh status
sudo sh scripts/swarm.sh access
sudo sh scripts/swarm.sh credentials
sudo sh scripts/swarm.sh scenario examples/swarm-churn-publish.yaml
```

`access`의 Controller URL을 여십시오. **Run experiment**에서 **YAML scenario** 전체를 출력된 내용으로 교체하고, 대시보드에 로그인한 상태에서 **Run**을 누릅니다. `scenario`는 YAML만 출력하며 웹 동작이 실험을 시작합니다. 웹 폼의 기본값과 helper의 기본 `scenario` 출력은 다른 예제입니다.

실행 창 **Run** 옆 **Runs**를 1~100으로 설정하면 전체 시나리오를 순차 반복하고 회차별 결과를 남깁니다. **Stop batch** 또는 회차 실패는 남은 반복을 취소하며 명시된 seed는 재사용합니다. [반복 실행과 지표 정의](experiment-metrics.kr.md)를 참고하십시오.

worker는 Linux `NET_ADMIN`과 호스트 커널의 netem 지원을 사용합니다. 각 Peer는 시작 시 최대 5초 동안 Controller health 표본을 최대 7개 수집하며 모두 실패해도 Ready까지 진행합니다. 미동기화 Peer는 5초마다 다시 시도하고 동기화 Peer는 30초마다 갱신하며, 마지막 성공 표본은 갱신 없이 2분이 지나면 신뢰 상태를 잃습니다. 장시간 실행 중에는 chrony/NTP를 계속 사용하십시오. 예제의 네트워크 설정은 송신 P2P TCP 트래픽에 적용하며 제어·시계 측정·discovery·telemetry HTTP는 해당 범위에서 제외합니다.

## 실행 흐름

| 단계 | 동작 |
|---|---|
| Bootstrap | 안정적으로 유지할 `boot` Peer 두 개를 생성하고 readiness를 기다립니다. DHT bootstrap만 제공하며 GossipSub나 고정 발행자로 참여하지 않습니다. |
| Churn | background job `worker-churn`을 시작합니다. 최대 10,000번의 worker join을 가용 Agent에 balanced로 순차 배치하며 간격은 평균 4초의 지수분포입니다. |
| Worker 수명 | Pareto `xm: 60s`, `alpha: 2.5`를 사용해 이론 평균수명이 100초입니다. Docker 생성 승인 시점부터 시작하므로 startup 시간도 포함합니다. 만료는 제거 요청이며 실제 정리는 이후 완료될 수 있습니다. |
| Warm-up | churn을 계속하면서 3분 기다립니다. |
| 발행 | 순차적으로 30 round를 실행합니다. 매 round마다 조건에 맞는 worker 최대 10개를 중복 없이 무작위 선택하고 평균 1초의 지수분포 간격으로 순차 발행합니다. |
| Round 사이 | 각 round 사이에 10초씩, 총 29번 기다립니다. 후보가 없어도 이 명시적 대기 덕분에 모든 round가 즉시 소진되지 않습니다. |
| 수집과 정리 | 마지막 round 뒤에도 churn을 유지하며 30초 더 수집합니다. 이어 `stop-all`이 남은 join job을 취소하고 Peer를 정리합니다. |

이론적 정상 상태의 worker 수는 `100s / 4s = 25`이며 boot 두 개가 별도입니다. 이는 기대값이고 **상한이 아닙니다**. 수명은 변동하고 Docker 생성·시작에 시간이 걸리며 배치 가능한 Agent가 모두 가득 차면 join 접수가 기다립니다. 이 때문에 실제 ready 수와 도착률은 달라집니다. 40슬롯 권장은 여유를 둔 시작점일 뿐 특정 서버가 이 부하를 감당한다는 실측 근거는 아닙니다.

`count: 10000`은 **총 join 예산**이며 동시 worker 10,000개가 아닙니다. 보통 이 예산을 다 쓰기 전에 마지막 `stop-all`이 producer를 취소합니다. 이 예제에는 개별 worker의 명시적 `leave` phase가 없고 샘플링한 수명이 퇴장을 발생시킵니다.

readiness barrier는 안정적인 boot 그룹에만 적용합니다. 계속 커지는 churn 그룹에 `wait-ready`를 적용하면 현재 generation에서 이미 종료된 구성원도 포함됩니다. 3분 warm-up은 이 부적절한 barrier를 피하기 위한 대기이며 특정 worker 수나 GossipSub mesh 수렴을 보장하지 않습니다. worker의 mesh 설정은 `d: 9`, `dLow: 7`, `dHigh: 11`입니다.

내장 boot Peer는 PubSub가 비활성화되어 있으므로 DHT bootstrap만으로 worker가 GossipSub overlay에 연결되지는 않습니다. Worker가 PubSub를 시작하면 동일 run·정확한 topic의 Controller discovery registry를 즉시 조회하고 이후 3초 간격으로 다음 round를 예약합니다. [`internal/peer/discovery.go`](../internal/peer/discovery.go)는 subscriber를 우선하면서 rendezvous hash로 ready 후보의 순위를 정합니다. dial 예산은 `DHigh`에서 현재 topic peer 수를 뺀 부족분이며 transport 연결 상한으로 한 번 더 제한합니다. dial 실패 시 다음 순위 후보를 시도하고 실패 후보에는 15초 재시도 유예를 적용합니다. 느린 round는 다음 조회를 늦출 수 있습니다. 실제 GRAFT mesh는 계속 GossipSub가 선택하므로 registry 후보나 transport 연결이 곧 mesh 구성원인 것은 아닙니다.

10개 노드를 모두 선택한 round에는 평균 합계 9초인 간격 아홉 개가 있으며 첫 요청부터 마지막 요청의 예약 시각까지 적용됩니다. 요청 처리 시간은 이 간격 안에 포함되고, 초과 지연과 마지막 요청의 처리 시간이 round를 늘릴 수 있습니다. 기본 대기 스케줄은 `180 + 30×9 + 29×10 + 30 = 770초`, 즉 **약 12분 50초**이며 bootstrap·API·정리 시간이 추가됩니다. 고정 종료 시각은 아닙니다. 후보가 적거나 없는 round는 짧아지고 느린 요청은 길어집니다.

## 발행 대상과 측정 의미

각 round 시작 시 Controller는 worker 그룹에서 Agent가 online이고, 상태가 `ready`이며, 발행이 가능한 full worker 중 `kpl/swarm-churn`에 참여한 노드를 다시 조회합니다. 이 집합을 shuffle하여 최대 10개를 중복 없이 선택합니다. 같은 노드가 다른 round에서 다시 선택될 수 있으며 나중에 들어온 worker도 이후 round에 참여할 수 있습니다.

선택한 worker가 자기 차례 전에 수명을 다할 수 있습니다. `onError: continue`는 개별 실패를 `phase-operation-failed`로 기록한 뒤 계속 진행합니다. 후보가 없으면 그 round는 no-op입니다. 취소·시나리오 deadline·저장 오류 등 치명적인 실패는 여전히 실행을 끝낼 수 있습니다. 최대치는 **선택된 발행 작업 300개**이며 성공 메시지 300개를 보장하지 않습니다.

각 발행은 `payloadSize: 32`, `payloadEncoding: envelope`, `deliveryWindow: 10s`, topic `kpl/swarm-churn`을 사용합니다. 32바이트는 무작위 payload의 길이입니다. envelope가 메타데이터를 추가하므로 PubSub 데이터와 네트워크 트래픽은 32바이트보다 큽니다. envelope 덕분에 전파 지연을 측정할 수 있습니다.

worker network는 `scope: p2p`, 지연 25ms, jitter 2ms, 설정 packet loss 0.5%입니다. boot에는 추가 네트워크 제약이 없습니다. **packet loss는 애플리케이션 미도달과 다릅니다.** TCP가 재전송할 수 있고 메시지 하나가 여러 subscriber에게 도달합니다. 전체 수신 이벤트에는 발행자 로컬 수신도 포함되지만 세션 기간 도달률에서는 제외합니다.

## 관측과 결과 저장

대시보드에서는 ready Peer 수, Agent 점유량, 실험·job 상태와 이벤트를 확인합니다. Grafana의 **KP2PLab Experiment Analysis**에서 **Run**에 실행한 `swarm-churn-random-publish`를 선택하고 **Agent**, **Topic**으로 필터링하십시오. 종료 후에는 시간 범위를 실험 구간으로 맞춥니다.

| 관측 항목 | 데이터 |
|---|---|
| 변화하는 노드 수와 점유 용량 | 실시간 node inventory, `kpl_nodes`, `kpl_agent_active_nodes` |
| 발행·수신·중복 이벤트 수 | `kpl_events_total`. mesh 변화는 `graft`/`prune` 이벤트로 관측 |
| 안정 대상 조건부 도달률·확인된 발행 시점 대상 도달률·coverage 범위 | `kpl_window_delivery_ratio`, `kpl_window_initial_delivery_ratio`, `kpl_window_stable_coverage`, `kpl_window_stable_coverage_upper_bound`. Run 필터만 적용 |
| Pending과 관측 불명 | `kpl_window_pending_publications`, `kpl_window_unknown_pairs`, `kpl_window_publication_availability_unknown_pairs`, `kpl_window_continuity_unknown_pairs`, `kpl_window_measurement_incomplete` |
| 안정 대상의 원격 성공 수신당 관측 추가 복사본 | `kpl_window_duplicates_per_reached_pair`. Run 필터만 적용 |
| Envelope 지연과 PubSub 데이터 양 | `kpl_window_propagation_latency_seconds`, `kpl_message_bytes_total` |
| 설정한 네트워크 조건 | `kpl_network_configured_*`. 실제 손실률이나 RTT가 아닌 설정값 |
| 실패 요청과 보고된 telemetry drop | `kpl_operation_failures_total`, `kpl_telemetry_dropped_events_total` |

이 예제의 `session-window-v1` 발행은 실제 발행 시각과 설정된 10초 마감을 사용합니다. `measurement_start`는 구독 애플리케이션 세션을 기록하고 동일 세션의 2초 주기 checkpoint 또는 stop으로 관측된 지속을 확인합니다. 주 도달률은 기간 전체에 걸쳐 구독을 증명한 세션만 사용합니다. 마감 전 stop 세션은 이미 수신했어도 주 지표의 분자·분모에서 모두 제외하고, 늦은 join도 제외합니다. 네트워크 단절이나 mesh 제거로 구독자를 제외하지 않습니다.

확인된 발행 시점 대상 도달률과 안정 coverage를 함께 보십시오. 확인된 최초 10명 중 2명이 이탈하고 그중 1명은 미리 수신했으며 안정 구독자 8명 중 7명이 성공하면 안정 도달률은 **7/8**, 발행 시점 대상 도달률은 **8/10**, coverage는 **80%**입니다. 발행 시점 대상 도달률에는 이탈자의 조기 성공도 포함합니다. 확인된 발행 시점 쌍의 이탈과 마감까지 지속 어느 쪽도 증명하지 못하면 coverage 범위는 `안정/확인된 발행 시점 대상`부터 `(안정 + 지속 여부 불명)/확인된 발행 시점 대상`까지 넓어집니다. 원본 sequence가 누락된 미수신도 도달률 범위를 넓힙니다. 이 범위는 신뢰구간이 아닙니다. 10초가 지나지 않은 메시지는 pending입니다. 발행 시점 가용성 불명 후보는 확인된 집단 밖에 두고 관측 품질로 표시합니다. 확인된 집단이 비었을 때만 발행 시점 도달률과 coverage가 N/A이며 incomplete/unknown 경고만으로 숨기지는 않습니다.

PubSub `join`/`leave`는 **Peer 생성·종료 기록이 아닙니다**. 측정 이벤트는 계측된 구독 세션이며 전체 Docker lifecycle이나 물리적 uptime 이력이 아닙니다. 강제 종료는 불명 꼬리 구간을 남길 수 있고 아예 보이지 않은 telemetry는 수신 로그만으로 검출하지 못합니다. 신뢰 상태의 시계 추정은 uncertainty를 기록하며, 범위상 기한 내인 음수 추정은 수신 성공으로 인정해도 지연 통계에서는 제외합니다. 갱신 실패가 2분 유효 기간을 넘으면 신뢰 metadata를 중단하고 이후 성공하면 다시 제공합니다. chrony/NTP도 계속 권장합니다. ZIP `metrics.json`은 같은 이벤트 경계에서 재계산합니다. 과거 dispatch 대상은 legacy로 남기며 지속 구독 증거를 대신하지 못합니다. [지표 정의와 수집 한계](experiment-metrics.kr.md)를 참고하십시오.

실험 종료 후 Peer 정리와, 다른 실행이 없다면 Agent 점유량 0을 확인합니다. `stop-all`이 끝나지 않은 join job을 의도적으로 취소하므로 `completed` 실행에도 `canceledJobs: 1`이 있을 수 있습니다. 이 값만으로 실패를 뜻하지는 않습니다.

실험 항목 또는 **Saved results**의 **Download results**를 사용하십시오. ZIP에는 최근 300개 버퍼와 별개로 제출한 시나리오·실험 메타데이터·내보내기 경계까지 저장된 전체 이벤트 로그가 들어갑니다. `metrics.json`과 수집된 구독 세션 이벤트도 포함하지만 별도의 완전한 Docker lifecycle snapshot, 메시지 payload dump, PCAP, Prometheus/Grafana 데이터베이스는 포함하지 않습니다. 실행 중 다운로드는 snapshot이며 다운로드로 누락된 telemetry를 복구할 수는 없습니다. [파일 구성과 보존 정책](results.kr.md#실험-결과-다운로드)

`sudo sh scripts/swarm.sh remove`로 웹 서비스를 내리기 전에 다운로드하십시오. 데이터 volume과 외부 Peer network는 남습니다. 재시작 후 보이는 `interrupted` 결과는 자동 재개하지 않으며 Peer 정리 완료를 증명하지 않습니다.

## 조정할 값

| 설정 | 효과 |
|---|---|
| Worker join의 `count` | 총 생성 예산을 바꿉니다. 마지막 수집 구간까지 join이 이어지도록 충분히 크게 유지합니다. |
| Worker `interval.mean` | 작게 하면 예약 도착 빈도가 높아집니다. 간격은 누적 예약 시각을 사용하고 늦어진 접수는 순서대로 따라잡지만, 지속적인 Docker·용량 병목은 실제 도착률을 제한합니다. |
| Worker lifetime의 `xm` / `alpha` | 수명과 기대 노드 수를 바꿉니다. `alpha > 1`이면 평균수명은 `alpha × xm / (alpha - 1)`입니다. |
| Agent capacity | 접수 한도이며 예약 CPU/RAM이 아닙니다. 높이기 전에 호스트 부하를 측정합니다. |
| Warm-up `duration` | 첫 발행 전 churn 시간을 바꿉니다. 수렴 검사가 아닌 시간 대기입니다. |
| Publish `count` / interval | round당 최대 발행 노드 수와 요청 사이 간격을 바꿉니다. |
| Publish `deliveryWindow` | 기본 10초·최대 1시간인 고정 수신 기간입니다. 끝까지 남는 대상도 바뀌므로 같은 값으로 비교하고 마지막 수집 대기는 이보다 길게 유지합니다. |
| Round 쌍 / 10초 대기 | 관측 기간을 바꿉니다. 후보가 없어도 시간이 지나도록 명시적 대기를 유지합니다. |
| Worker network profile | P2P 지연·jitter·loss를 바꿉니다. 이 실험에서는 bootstrap 두 개를 안정적으로 유지합니다. |
| `payloadEncoding` | `envelope`는 지연 메타데이터를 제공합니다. `raw`는 여기서 PubSub 데이터를 정확히 32바이트로 만들지만 전파 지연 표본이 없습니다. |

YAML은 anchor로 발행 phase와 대기 phase를 재사용합니다. 이는 파싱 시 펼쳐지는 표준 YAML 참조이며 **시나리오 엔진의 반복문 기능이 아닙니다**. round 수를 바꾸려면 반복된 publish/wait 쌍을 조정하고, anchor가 있는 발행 phase를 수정하면 이를 참조하는 모든 alias에 적용됩니다. 마지막 30초 수집 대기와 `stop-all`은 유지하십시오.

고정 seed는 후보 집합과 실행 조건이 같을 때 난수 샘플링을 재현합니다. Docker 처리 시간, 요청 전 노드 퇴장, 서로 다른 실행·장비의 결과까지 같게 만들지는 않습니다.

## v2 Churn 실험과의 관계

검토한 v2 `exp/exp-2603_churn-02.sh` 스크립트는 boot 10개를 유지하고 worker join을 최대 10,000회 예약한 뒤 대기했으며, 무작위 worker 최대 10개에서 전체 topic으로 32바이트 raw 데이터를 **한 batch** 발행했습니다. 이후 churn을 유지하며 30초 수집했습니다.

이 예제는 의도적으로 boot 두 개, 더 작은 기대 worker 수, **매번 다시 선택하는 발행 30 round**, 명시적 topic 하나, 추가 P2P 네트워크 조건, **지연 측정용 envelope**를 사용합니다. balanced Agent 배치도 v2의 join마다 worker 서버를 무작위 선택하는 방식과 다릅니다. 세션 기간 지표는 v2 재현 계약과 독립적인 v3 실험 설계입니다. churn 중 관측이라는 목적을 유지하면서 작은 Swarm에서 반복 관측할 수 있도록 한 것이며, 결과의 동등성을 보장하거나 누락된 v2 설정 파일을 복원하지 않습니다. [v2 재현 참고](v2-reproduction.kr.md)에서 호환 범위를 확인하십시오.

# 세 지연 그룹의 Churn과 Prysm beacon_block 스코어링

[English](swarm-churn-prysm-block.md) | 한국어

[`examples/swarm-churn-prysm-block.yaml`](../examples/swarm-churn-prysm-block.yaml)은 제시한 boot 10개, warm-up 5분, 4KiB 실험을 확장하여 세 그룹에서 동시에 churn을 실행합니다. 모든 worker가 `kpl/prysm/beacon_block` 토픽을 공유하며 발행 그룹을 돌아가면서 선택합니다.

## 그룹과 실행 순서

| 그룹/profile | 송신 P2P 지연 | Jitter | 패킷 손실 | 총 join 예산 | 평균 join 간격 |
|---|---:|---:|---:|---:|---:|
| `delay-low` | 25ms | 2ms | 0.5% | 3,334 | 0.3s |
| `delay-medium` | 100ms | 2ms | 0.5% | 3,333 | 0.3s |
| `delay-high` | 250ms | 2ms | 0.5% | 3,333 | 0.3s |

Worker profile 사이에서는 지연만 다릅니다. Balanced 배치로 각 그룹을 가용 Agent에 분산하므로 그룹이 특정 물리 서버를 뜻하지 않습니다. `scope: p2p`는 DHT를 포함한 송신 P2P TCP에 적용하고 Controller HTTP·telemetry는 제외합니다. 표의 지연은 송신 측 설정값으로 RTT가 아니며, 패킷 손실률도 애플리케이션 미도달률과 다릅니다.

공통 수명은 Pareto `xm: 120s`, `alpha: 2.5`로 평균 **200초**입니다. 순차 producer 세 개가 평균 `0.3s` 지수분포 간격으로 실행되어 생성 처리·용량 대기 전 전체 속도는 원본처럼 **초당 약 10개**입니다. 이론적 정상 상태는 **그룹당 약 667개, 전체 worker 약 2,000개 + boot 10개**입니다. 동시 실행 상한이나 실측 용량 보장이 아닙니다. 수명은 컨테이너 생성 성공부터 시작하여 startup 시간도 포함합니다.

**여유 슬롯 40개로는 이 속도를 유지할 수 없습니다.** 기대 모집단보다 여유 있게 용량을 준비하고 호스트 부하를 확인하거나, 공통 `&churn` 간격을 조정하십시오. 평균 `10s`이면 worker 기대값 60개 + boot 10개, `30s`이면 worker 20개 + boot 10개입니다. 나머지 job도 이 값을 상속합니다. 용량 대기는 실제 도착률과 그룹 비율을 바꿀 수 있습니다. 10,000회는 세 그룹을 합한 예산입니다.

1. PubSub를 끈 안정적인 boot 10개를 생성하고 readiness를 기다립니다.
2. `churn-low`, `churn-medium`, `churn-high`를 `await: false`로 시작합니다. 각 job 내부는 `parallel: false`로 간격을 유지합니다. 세 실행은 겹치지만 첫 join을 같은 시각에 맞추는 barrier는 아닙니다.
3. 공통 topic discovery와 GossipSub overlay에 참여하며 5분간 warm-up합니다. 이미 종료된 노드도 포함하는 누적 churn 그룹에는 readiness barrier를 적용하지 않습니다.
4. **저지연 → 중지연 → 고지연**을 10회 순환하여 총 30 round를 실행합니다. 매 round는 해당 그룹의 조건에 맞는 publisher를 중복 없이 최대 10개 선택합니다. 그룹당 최대 100회이며 실제 성공은 churn으로 줄어들 수 있습니다. `group`은 발행자만 제한하고 다른 그룹의 구독자를 제외하지 않습니다.
5. 원본처럼 4,096바이트 envelope, 수신 기한 `1s`, round 내부 평균 `1s` 지수분포 간격을 사용합니다. Round 사이에는 총 29번의 명시적 `10s` 대기를 두어 후보가 없어도 시간을 보냅니다.
6. 마지막 30초 동안 더 수집한 뒤 `stop-all`로 남은 job을 취소하고 이 run의 Peer를 정리합니다. Churn은 유한한 join 예산이 남아 있는 동안 계속됩니다.

매번 10개를 선택할 때 기본 시간은 `300 + 30×9 + 29×10 + 30 = 890초`, **약 14분 50초**이며 bootstrap·요청·정리 시간이 추가됩니다. 요청이 느리면 실행이 길어져 join 예산이 먼저 소진될 수도 있습니다. 원본의 전체 모집단 무작위 발행자 선택을 그룹별 동일 round 예산으로 바꿨습니다. [스케줄링·측정 상세](swarm-churn-publish.kr.md)를 참고하십시오.

## Prysm 스코어 기준

기준은 **Prysm v7.1.8**, commit `51b5a75ebbadf05af22bd2601b5baf7a9e99b66d`의 [`peerScoringParams`, `defaultBlockTopicParams`, 시간 계산 함수](https://github.com/OffchainLabs/prysm/blob/51b5a75ebbadf05af22bd2601b5baf7a9e99b66d/beacon-chain/p2p/gossip_scoring_params.go)입니다. [Mainnet 상수](https://github.com/OffchainLabs/prysm/blob/51b5a75ebbadf05af22bd2601b5baf7a9e99b66d/config/params/mainnet_config.go)인 slot 12초, epoch 32 slot로 값을 계산했습니다. YAML에 수치만 기록하며 Prysm 의존성이나 서드파티 소스 수정은 추가하지 않습니다.

| 항목 | 설정 |
|---|---|
| 토픽 가중치 | `0.8` |
| P1 mesh 체류 시간 | 가중치 `10/300`, quantum `12s`, cap `300` |
| P2 최초 전달 | 가중치 `1`, cap `23`, decay `0.9928302477768374` |
| P3 / P3b mesh 벌점 | **가중치 모두 `0`**. Prysm의 `meshDeliveryIsScored = false` 반영 |
| P4 잘못된 메시지 전달 | 가중치 `-140.4475`, decay `0.9971259067705325` |
| P5 애플리케이션 | 고정 `0`, 가중치 `1` |
| P6 IP 집중 | 가중치 `-35.11`, threshold `10`, whitelist 없음 |
| P7 행동 벌점 | 가중치 `-15.92`, threshold `6`, decay `0.9857119009006162` |
| 전역 | 토픽 cap `32.72`, decay 주기 `12s`, floor `0.01`, retain `10h40m` |
| Threshold | Gossip `-4000`, publish `-8000`, graylist `-16000`, accept PX `100`, opportunistic graft `5` |

`E` epoch에 대한 slot당 decay는 `0.01^(1/(32×E))`입니다. P2는 20 epoch, P4는 50, P7은 10입니다. 비활성 mesh 항목도 5 epoch decay, threshold `16`, cap `160`, activation `25m36s`, window `2s`로 기록했습니다. 이 기준 실험에서는 두 mesh 가중치를 모두 0으로 유지합니다. 점수 inspection은 `1s`이며 decay의 `12s`와 별개입니다.

이 실험은 **Prysm 스코어 파라미터를 적용한 KPL 합성 트래픽**입니다. Discovery·Kademlia·envelope는 KPL 구현이며 mesh degree도 요청한 `6/5/12`, `dScore: 3`을 사용합니다. Ethereum SSZ/snappy 블록, fork digest, 합의 검증이나 mainnet 블록 발행 일정을 재현하지 않습니다. 일반 트랜잭션 gossip은 [실행 계층 네트워크](https://ethereum.org/developers/docs/networking-layer/)의 역할이며 선택한 기준은 합의 계층의 `beacon_block`입니다.

지연이 낮으면 P2 최초 전달 보상을 더 받을 수 있다는 가설을 관측합니다. P1은 mesh 체류 시간에도 영향을 받으므로 지연순 점수 순위를 보장하지 않습니다. P3/P3b가 꺼져 있어 느린 전달만으로 전달 부족 벌점이 생기지는 않습니다. 정상 합성 메시지는 Ethereum 고유의 P4 검증 실패도 만들지 않습니다. 동일한 외부 IP로 보이는 Peer나 프로토콜 행동 벌점은 지연과 별개로 점수를 바꿀 수 있습니다. 짧은 churn 수명에서는 한 시간짜리 P1 cap까지 도달하는 경우가 드뭅니다. `retainScore`는 연결 해제 Peer에 대한 router 정책이며 점수 이력 저장이나 새로운 Peer ID로의 승계가 아닙니다.

## 실행과 수집

[Swarm 배포](swarm.kr.md)를 마친 뒤 아래 명령으로 검증하고, 출력한 YAML을 Dashboard에 붙여 넣어 설정된 대시보드 계정으로 로그인한 뒤 실행합니다.

```sh
go run ./cmd/kpl validate --scenario examples/swarm-churn-prysm-block.yaml
sh scripts/swarm.sh scenario examples/swarm-churn-prysm-block.yaml
```

토폴로지에서 노드를 선택하면 관측한 점수 개수와 평균을 볼 수 있습니다. `peerScores[remotePeerId]`는 **관측 노드가 상대 Peer에 부여한 점수**이며 관측 노드 자신의 평판이 아닙니다. 평가자 그룹 × 평가 대상 그룹의 점수와 mesh 포함 여부, 노드 나이, ready 모집단을 함께 비교합니다.

Controller는 그룹별 점수·토폴로지 요약을 약 5초마다 `observations.jsonl`에 자동 저장하며 결과 ZIP과 [Saved results → Images](visualization.kr.md)에서 제공합니다. 평가자 그룹별 요약이므로 개별 점수 쌍, 평가 대상 그룹별 내역, 전체 mesh 이력은 남기지 않으며 Prometheus에도 점수 시계열은 없습니다. 평가자 그룹 × 평가 대상 그룹 분석에는 저장소에서 다음 Bash 수집기를 warm-up 중 시작하고 마지막 수집 구간 뒤 Ctrl-C로 종료하십시오. Controller URL과 실제 run ID를 교체합니다. `curl`, `jq`가 필요하며 공개 읽기 전용 snapshot API를 사용합니다. 요청·처리 시간에 더해 5초마다 기록합니다.

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

[`score-cohorts.jq`](../scripts/score-cohorts.jq)는 같은 run에서 online Agent의 ready Peer와 현재 transport 연결이 있는 점수 쌍만 선택하여 연결 해제 후 보존된 점수를 제외합니다. 평가 방향별 그룹 평균·최소·최대, 음수 평가 쌍 수, mesh 쌍 수, starting/ready 모집단을 출력합니다. 자동 요약은 신선하게 보고한 평가자의 점수 전체를 사용하지만 이 수집기는 현재 transport 연결이 있는 쌍만 사용하므로 평균이 다를 수 있습니다. `pairs`는 평가 쌍 수이며 고유 평가 대상 노드 수가 아닙니다. 행이 없으면 해당 관측이 없는 것이며 점수 0을 뜻하지 않습니다. 평균에서는 평가 쌍마다 같은 가중치를 사용합니다. 상태 보고가 오래됐거나 반복될 수 있고, `generatedAt`은 API 생성 시각이며 정확한 score inspection 시각은 아닙니다. 이상이 있으면 노드의 `lastSeen`과 Agent 상태도 확인합니다. 개별 Peer의 원본 데이터가 필요하면 snapshot도 저장하십시오.

```sh
curl --fail --output "$KPL_RUN_ID-snapshot.json" \
  "$KPL_CONTROLLER_URL/api/v1/snapshot"
jq --arg run "$KPL_RUN_ID" -f scripts/score-cohorts.jq \
  "$KPL_RUN_ID-snapshot.json"
```

Grafana의 **KP2PLab Experiment Analysis**에서 run 단위 도달률·coverage·지연·graft/prune 트래픽을 함께 봅니다. `kpl_nodes`, `kpl_network_configured_*`에는 그룹 label이 있지만 기존 도달률·지연 지표에는 없습니다. Balanced 배치이므로 Agent 필터로 그룹을 대신할 수도 없습니다. `1s` 수신 기한에는 다중 hop·TCP 재전송으로 늦게 도착한 메시지가 빠질 수 있고 이것이 invalid-message 벌점을 뜻하지는 않습니다. Unknown, pending, telemetry drop도 확인합니다. 전체 수집 이벤트와 세션 기간 지표를 위해 결과 ZIP도 별도로 내려받습니다. 점수 JSONL은 별도 수집 파일입니다.

스코어링을 끈 대조 실험은 YAML을 복사하여 `name`과 공통 `score.enabled: false`만 바꾸면 세 profile 모두에 적용됩니다. 지연·churn·mesh·발행·seed를 유지하고 별도 run으로 실행합니다. 반복 실행과 실제 모집단·발행 수를 함께 비교하십시오. 같은 seed가 컨테이너 타이밍과 프로토콜 결과까지 같게 만들지는 않습니다. [지표 정의](experiment-metrics.kr.md)와 [프로토콜 설정](protocol-options.kr.md)을 참고하십시오.

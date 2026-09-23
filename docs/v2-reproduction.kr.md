# v2 시나리오·네트워크 실험 재현 범위

[English](v2-reproduction.md) | 한국어

[문서 안내](README.kr.md) · [저장소](../README.kr.md)

검토 기준은 `kpl-v2`의 실제 Controller/Peer 코드와 `exp/exp-2603_churn-02.sh`입니다. 설명 문서와 실행 코드가 다를 때는 실행 코드를 기준으로 삼았습니다. 아래 설정으로 실험의 주요 조건을 재현할 수 있으나, 과거 실행의 패킷 순서나 토폴로지가 동일해지는 것은 아닙니다.

이 호환성 세부 사항은 v3에서 관리합니다. 프로젝트 공통 버전 이력과 연구 배경은 [Hub](https://github.com/k-p2p-lab/hub/blob/master/README.kr.md)를 참고하십시오.

## 컨테이너 격리와 배치

양쪽 모두 피어 하나를 컨테이너 하나로 실행합니다. v2는 Swarm Service, v3는 Agent가 생성하는 Docker 컨테이너입니다. v3 Peer에는 호스트 포트, 호스트 파일시스템, Docker socket을 공유하지 않습니다. 각 Peer의 network namespace에만 qdisc를 설치하며, 필요한 Peer에만 `NET_ADMIN`을 부여합니다. CPU·메모리 제한은 v2에도 없었으며, 호스트 부하까지 격리된다는 의미는 아닙니다.

v2의 parallel join은 한 Worker 호스트를 무작위 선택한 뒤 ServiceCreate 요청을 순차 전송합니다. v3에서 이에 대응하는 설정은 `parallel: true`, `parallelism: 1`, `placement: single-agent`입니다. v2의 sequential join은 노드마다 호스트를 다시 선택하므로 `parallel: false`, `placement: random`을 사용합니다. `agentId`를 지정하면 특정 Agent에 고정할 수 있습니다. 기본 `placement: balanced`는 부하 분산 정책입니다.

Swarm은 **선택한 노드마다 Agent 하나**를 실행하며 Peer 컨테이너는 공통 attachable overlay로 연결됩니다. 물리 호스트 간 지연 실험에는 [Swarm 절차](swarm.kr.md)로 Agent 노드를 최소 두 개 선택하십시오. Agent capacity 대기, Docker CLI 비용, 실제 머신 부하에 따라 목표 join rate와 실제 시작 rate는 달라집니다. v2의 Swarm 자동 복구와 v3의 종료 상태 기록도 다릅니다.

Swarm 다중 서버용 [배포 구성과 확장성 검토](swarm.kr.md)를 추가했습니다. Swarm은 global Agent를 서버별로 배치하고 Controller가 Peer를 분배합니다. Agent task 재시작은 담당 Peer를 정리하며, v2의 Peer Service 재스케줄링을 그대로 재현하지는 않습니다. Peer 광고 주소·Agent별 직접 접근·삭제 완료까지의 capacity 보존·전체 Agent 모니터링을 반영했습니다.

생성 도중 실험을 취소하더라도 Docker create의 응답을 제한시간 내 기다려 컨테이너 ID를 확보한 뒤 제거합니다. CLI만 먼저 종료하여 daemon에 늦게 생성되는 컨테이너를 남기는 경합을 방지합니다. Docker가 삭제 진행 중이라고 응답하면 실제 소멸 여부를 확인합니다. 기본 `jobShutdownTimeout`은 `3m`이며 job 종료와 Peer 정리에 각각 적용됩니다. Swarm은 Controller 종료 유예를 `10m`, 각 Agent 종료 유예를 `4m`로 설정합니다. [종료 절차](swarm.kr.md#장애업데이트종료)를 참고하십시오. Docker admission 자체의 deadline 초과나 daemon 장애는 오류로 보고하며, 이후 재시작 시 소유 label로 잔여 컨테이너를 정리합니다.

## 네트워크 조건 대응

| v2 설정/동작 | v3 설정 |
|---|---|
| `eth0` 전체 송신에 netem 적용 | `network.scope: all` (기본 `p2p`는 P2P TCP만 적용) |
| `delay.distribution`에서 피어당 값 하나 추출 | `network.delayDistribution` |
| `delay.deviation` 밀리초, 패킷 jitter | `jitter: 5ms`, `jitterDistribution: uniform` |
| `loss`, `corrupt` | `lossPercent`, `corruptPercent` |
| `reorder.percentage`, `reorder.chance` | `reorderPercent`, `reorderCorrelationPercent` |
| `tbf.rate`, `tbf.burst`, `tbf.latency` | `tbf.rateMbps`, `tbf.burstKbit`, `tbf.latency` |

v2의 `chance`는 실제 tc 명령에서는 재정렬 **상관계수**입니다. `rateMbps` 단독은 netem의 패킷 직렬화 지연이며 TBF와 동등하지 않습니다. TBF와 양수 `rateMbps`는 함께 설정할 수 없습니다. TBF latency는 tc가 지원하는 `1us`~`4294967295us` 범위이며 소수 마이크로초는 올림합니다.

```yaml
network:
  scope: all
  delayDistribution:
    model: normal
    mean: 50ms
    sigma: 10ms
    min: 1ms
  jitter: 5ms
  jitterDistribution: uniform
  lossPercent: 1
  reorderPercent: 2
  reorderCorrelationPercent: 25
  tbf:
    rateMbps: 10
    burstKbit: 64
    latency: 100ms
```

`delayDistribution`는 Peer seed로 한 번 샘플링합니다. 패킷마다 바뀌는 jitter와 별개입니다. `delay`와는 배타적이며, jitter/reorder가 활성화되면 양수 `min`을 지정해야 합니다. v2는 초 단위 샘플을 정수 ms로 잘랐지만 v3는 Go duration 정밀도를 유지합니다. v2의 시간 기반 난수 및 gamma 구현 오류까지 복제하지는 않습니다.

`scope: all`은 제어 API 응답·bootstrap 조회·telemetry도 지연시키거나 유실시킵니다. 큰 손실률에서 `wait-ready`나 발행 HTTP 요청이 실패할 수 있는 것도 실험 조건의 일부입니다. 양쪽 모두 송신 방향 shaping입니다. 두 Peer에 delay를 설정하면 왕복 경로에는 두 송신 지연이 더해집니다. 커널 offload, TCP 재전송, 큐 상태 때문에 application 메시지 손실률이 packet loss 설정값과 같지는 않습니다.

## 참여·이탈·발행 스케줄

| 의미 | 대응/주의점 |
|---|---|
| 순차 interval | v2는 완료 후 대기. v3는 장기 churn에서 접수·요청 지연이 누적되지 않도록 batch 시작 기준 누적 예약 시각을 사용. 늦어진 작업은 누락 없이 순서대로 따라잡으며 마지막 작업 뒤 대기는 없음. |
| parallel join/leave | interval 무시 |
| parallel publish | 발행자마다 독립적인 시작 지연, interval 생략 시 1초 |
| publish/leave replicas | 후보 수로 제한, 중복 선택 없음 |
| v2 기본 await=false | YAML에서는 `await: false`를 명시 (v3 기본 true) |
| v2 async for-loop | 서로 다른 job phase로 펼침; `repeat`는 한 job의 순차 반복 |
| churn 중 발행 대상 소멸 | `onError: continue`로 해당 실패만 기록하고 계속 |
| 단일 실행의 정상 완료 후 전체 Peer 정리 | 마지막 `stop-all`을 명시. 반복 실행(`repetitions > 1`)은 회차 사이에도 자동 정리 |

`onError: continue`는 publish/leave에만 적용하며, 후보 0개도 허용합니다. 작업별 실패는 `phase-operation-failed` 이벤트에 남습니다. 사용자 취소와 phase deadline은 계속 전파되며, 개별 HTTP 요청 timeout은 해당 작업의 실패로 처리합니다. 기본 `fail`은 기존 v3처럼 실패 시 실험을 중단합니다.

Docker Peer lifetime은 v2의 ServiceCreate 반환에 대응하는 **Docker create 성공 시점**부터 시작합니다. config copy, 컨테이너 시작, bootstrap에 걸리는 시간은 lifetime에 포함됩니다. `ready`부터의 온라인 수명이 아닙니다. 노드 metadata의 `lifetime`, `lifetimeBasis`, `containerCreatedAt`, `containerStartedAt`, `readyAt`, `stopRequestedAt`, `stoppedAt`으로 구분할 수 있습니다. 각 시각은 Agent가 해당 상태를 관측한 시각이며 `startedAt`은 생성 요청 수락 시각입니다. 종료는 강제 이탈이며, v2 Peer에도 애플리케이션 수준의 정상 종료 절차가 없었습니다.

[`examples/v2-churn.yaml`](../examples/v2-churn.yaml)은 기존 churn 시나리오의 구조를 작은 규모로 옮기고 네트워크 조건을 추가한 예제입니다. 원래 10,000개 join 요청과 수백 개 동시 Peer 규모의 성능 재현 결과를 의미하지 않습니다.

## 초기 연결과 메시지 크기

Peer는 v2처럼 TCP/Noise/Yamux를 명시적으로 사용합니다. Worker는 bootstrap 후보를 seed로 섞고 첫 연결 성공에서 초기 접속을 끝냅니다. bootstrap 전체 timeout은 목록 조회와 dial에도 적용됩니다. PubSub 시작 후 v3는 Controller의 동일 run·정확한 topic discovery registry도 3초 간격으로 조회하여 transport 연결을 보충합니다. 후보 순위·연결 예산·실패 재시도는 [`internal/peer/discovery.go`](../internal/peer/discovery.go)에 구현되어 있으며 실제 mesh는 GossipSub가 선택합니다. 이 추가 discovery 경로는 v2와 다르므로 DHT/GossipSub 파라미터가 같더라도 연결 구조와 churn 복구가 달라질 수 있습니다.

v2의 `size`는 PubSub에 전달되는 무작위 바이트 길이입니다. v3에서 `payloadEncoding: raw`를 사용하면 `payloadSize`가 정확히 그 길이입니다. libp2p framing, 서명, TCP/IP 헤더까지 포함한 패킷 크기를 뜻하지는 않습니다. 기본 `envelope`는 전파 지연 계측을 위한 JSON과 base64 때문에 더 큽니다. `topic: '*'`는 선택한 노드가 가진 모든 발행 가능한 topic에 각각 메시지를 발행합니다.

raw 발행·수신·중복 이벤트는 `pubsub-` 뒤에 native PubSub 메시지 ID를 16진수로 붙여 연결합니다. 현재 기본 ID는 origin과 sequence를 결합하므로 raw 바이트가 같은 두 발행도 구분됩니다. 내용 기반 메시지 ID를 명시하면 같은 내용이 합쳐질 수 있으므로 [메시지 ID 설정](protocol-options.kr.md#pubsub-활성화와-공통-설정)을 참고하십시오. 메시지에 애플리케이션 시각을 추가하지 않으므로 raw 수신에는 지연값을 제공하지 않으며 평균/P95 계산에서 제외합니다. raw 트래픽을 사용하는 run은 전용 topic과 네트워크로 분리하십시오. envelope의 run ID 필터를 raw 바이트에는 적용할 수 없습니다. [`internal/peer/publish.go`](../internal/peer/publish.go)를 참고하십시오.

## 관측과 남은 차이

- `wait-ready`는 초기화/API 준비를 뜻하며 mesh 수렴을 보장하지 않습니다. 예제는 별도의 안정화 대기를 둡니다.
- [dashboard 토폴로지](topology.kr.md)는 현재 Peer 상태를 바탕으로 transport, Kademlia 라우팅 테이블, topic별 GossipSub mesh를 별도로 표시합니다. `TopicPeers`는 `len(pubsub.ListPeers(topic))`이며 로컬에서 알고 있는 topic peer 수입니다. mesh 차수나 원격 애플리케이션 구독 세션의 증거가 아닙니다. 과거 분석에는 저장된 `graft`/`prune`, `add_peer`/`remove_peer`, `join`/`leave` 이벤트를 함께 사용하고 telemetry 누락을 고려해야 합니다. 새 결과의 `observations.jsonl`에는 그룹별 요약과 표본 노드·간선도 있습니다. 저장 프로토콜 그래프는 여러 토픽의 관계를 합치며 개별 시점의 모든 routing·mesh 전이를 보존하지 않습니다. 저장 형식은 [토폴로지](topology.kr.md#api와-보존-데이터)를 참고하십시오.
- v3의 타입별 RPC 수는 v2의 physical IHAVE/IWANT counter, 메시지 ID 참조 수는 logical counter에 대응합니다. v3는 protobuf entry 수, IDONTWANT, 로컬 송신 전 drop, PRUNE peer-exchange record, 혼합 RPC 안의 모든 타입도 보존합니다. v2는 이 경우들을 누락하고 IHAVE topic 정보도 버렸습니다. 기존 단순 `graft`/`prune` 이벤트는 로컬 mesh 전이이므로 wire traffic은 별도 `send_*`, `recv_*`, `drop_*` 제어 이벤트와 비교하십시오. [제어 트래픽 정의](experiment-metrics.kr.md#gossipsub-제어-트래픽)를 참고하십시오.
- interval/lifetime/base delay 샘플은 seed로 재현할 수 있으나 run ID를 포함한 Peer ID, 네트워크 타이밍, 커널 패킷 난수는 동일하지 않습니다. 노드 metadata에 `seed`, `networkRequested`, 실제 `network`를 남기며 Agent의 Peer config 파일에도 실효 설정을 저장합니다.
- [추가 연구 분석](experiment-metrics.kr.md#저장-결과-연구-지표)은 v2 그림 계열을 별도 정의로 제공합니다. 메인 세션 지표와 모집단·기간·집계가 다르고 Eager/Lazy는 GRAFT·IHAVE·IWANT·메시지 ID의 추정입니다. 실제 발신 큐 원인과 누락된 과거 간선을 만들어 수집하지 않습니다. 지원 그림과 가져오기 형식은 [v2 분석 대응](v2-analysis-coverage.kr.md)을 참고하십시오.
- 기본 연결 상한 55와 주요 worker DHT/GossipSub 파라미터는 일치합니다. 그러나 v2 custom PubSub fork 경로의 소스가 제공된 디렉터리에 없어 fork 내부까지 동등성을 검증할 수 없습니다. v3는 공식 라이브러리이며 HopWave는 지원 범위 밖입니다.
- dashboard 메시지 지표는 최근 이벤트 버퍼와 독립적으로 누적되지만 Controller 재시작 시 초기화되며 telemetry 누락의 영향을 받습니다. [저장 결과 분석](visualization.kr.md)과 ZIP 지표는 재시작 후에도 기록된 실행을 재계산합니다. [churn 대상 집합 정의](experiment-metrics.kr.md)와 [v2 전체 분석 대조표](v2-analysis-coverage.kr.md)를 함께 확인하십시오. 시나리오 설정이 대응하더라도 수치나 시각화 동등성을 보장하지 않습니다.

검토한 핵심 v2 파일: `kpl-controller/internal/handler/event.go`, `internal/docker/docker.go`, `internal/distribution/distribution.go`, `cmd/main.go`, `kpl-peer-app/internal/host/{host,tc}.go`, `internal/dht/dht.go`, `internal/api/publish.go`.

## 과거 검증 기록

아래 보존된 기록은 2026-09-04 Docker Desktop Linux 검증을 보고합니다. 원본 실행 아카이브는 이 저장소에 포함되어 있지 않으므로 수치는 현재 checkout의 검증 결과가 아닌 과거 관측으로 보십시오. 현재 회귀 명령과 패키지 테스트·커널 통합 검증의 구분은 [개발 가이드](development.kr.md#컨테이너와-브라우저-회귀-검사)를 참고하십시오.

- 전체 `go test -buildvcs=false ./... -timeout 60s` 및 모든 예제 YAML 검증 통과.
- 통합 run `run-20260904T021417Z-581b`: 컨테이너 4개의 서로 다른 network namespace, host mount/port binding 없음 확인.
- P2P 전용 netem→TBF와 전체 송신 netem→TBF 모두 실제 커널에 설치. TBF `10Mbit`, burst `8KB`(64kbit), latency `100ms` 확인.
- 동일 worker 프로파일의 피어별 기본 지연은 `57.008524ms`, `50.467284ms`, `47.92387ms`로 각각 추출. 실제 qdisc에서 loss `1%`, reorder `2%`, correlation `25%` 및 packet drop 카운터 확인.
- Worker 3개에서 topic 두 개에 총 6개 raw 메시지 발행. 모든 PubSub data는 정확히 32바이트, 자기 수신을 포함한 24개 수신 이벤트와 도달률 1 확인. raw 지연은 전부 미측정으로 기록.
- 최종 churn run `run-20260904T022448Z-0f9d`: 약 86초 동안 16개 Peer 참여, 발행 3건·수신 8건. 선택된 Peer의 이탈로 발행 1건이 실패했지만 `phase-operation-failed`로 기록하고 계속 진행하여 정상 완료. 모든 Peer stopped 및 잔여 관리 컨테이너 0개 확인.

이 검증은 설정 적용과 작은 규모의 실제 통신을 확인한 것입니다. TBF 최대 처리량, 다중 물리 호스트 overlay, 수백·수천 Peer의 churn 부하 성능까지 측정한 결과는 아닙니다.

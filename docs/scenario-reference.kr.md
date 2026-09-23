# 시나리오 설정 레퍼런스

[English](scenario-reference.md) | 한국어

이 문서는 버전 2 YAML 형식, 피어 프로파일, 프로토콜 설정, 네트워크 조건과 백그라운드 잡을 설명합니다. 대시보드에서 재사용할 YAML을 저장하는 방법은 [시나리오 라이브러리](scenario-library.kr.md)를 참고하십시오.

## 최상위 필드

시나리오 형식 버전은 K-P2PLab release 버전과 독립적입니다. v3 실행 파일에는 `version: 2` YAML을 권장합니다. Parser는 `version: 1`도 허용하며 `version`을 생략하거나 `0`으로 설정하면 `1`로 처리합니다. 두 버전 모두 현재의 동일한 검증과 runner를 사용합니다. 알 수 없는 YAML 필드 이름은 거부합니다.

| 필드 | 필수 여부 또는 기본값 | 의미 |
|---|---|---|
| `version` | 권장: `2` | 애플리케이션 release가 아닌 scenario schema 버전입니다. |
| `name` | 필수, 공백만으로 구성 불가 | Run 이름이며 저장 라이브러리 항목 이름과 독립적입니다. |
| `seed` | `0` | 0이 아닌 seed는 애플리케이션 sampling을 제어하고 0은 시각 기반 seed를 선택합니다. |
| `onExit` | `cancel` | 자연 완료 시 background job에 적용하는 `cancel` 또는 `drain` 정책입니다. |
| `jobShutdownTimeout` | `3m`, 양수 | Job 종료 제한과 별도로 Peer cleanup 제한에 적용하며 전체 run timeout은 아닙니다. |
| `profiles` | 빈 map | 재사용할 노드별 configuration overlay입니다. |
| `phases` | 최소 하나 | 순서대로 실행하는 action이며 추적되는 background 작업을 시작할 수 있습니다. |

실행 schema와 검증은 [`internal/scenario/scenario.go`](../internal/scenario/scenario.go), scheduling은 [`internal/controller/runner.go`](../internal/controller/runner.go)에 있습니다.

## 노드 역할, 종류와 프로파일

`role`은 Kademlia bootstrap discovery를 제어합니다. `boot` 노드는 Controller가 bootstrap peer로 제공하지만 `worker` 노드는 제공하지 않습니다. Topic transport discovery는 별도이며 동일 run·정확한 topic의 ready PubSub 참가자를 대상으로 합니다. `type`은 노드의 libp2p, Kademlia, PubSub 동작을 제어합니다. 이 개념들은 서로 독립적이므로 bootstrap 역할을 오용하지 않고 여러 종류의 worker를 한 실험에서 사용할 수 있습니다. `type`과 `profile`을 모두 생략하면 `role: boot`는 `boot` preset을, 그 밖의 role은 `full`을 선택합니다.

| 내장 type | 동작 |
|---|---|
| `boot` | v2와 마찬가지로 기본 상태에서는 Kademlia만 실행하는 bootstrap 노드입니다. |
| `full` / `worker` | Kademlia server와 표준 GossipSub에 모두 참여하며 v2의 hard connection cap `55`를 사용합니다. |
| `light` | Kademlia client이며 기본 연결 제한이 더 작습니다. |
| `publisher` | 토픽에 구독하지 않고 발행만 수행합니다. |
| `subscriber` / `observer` | 구독 전용이며 발행 요청을 거부합니다. |
| `relay` | `Topic.Relay()`로 application subscription이나 application publish 없이 topic 트래픽을 전달합니다. |
| `flood` | FloodSub를 사용합니다. |
| `random` | 최소 degree와 추정 네트워크 크기를 별도로 조정하는 RandomSub를 사용합니다. |
| `dht-only` | PubSub 없이 Kademlia만 실행합니다. |
| `gossip-only` | Kademlia 없이 PubSub만 실행합니다. |
| `non-gossip` / `mesh-only` | lazy gossip을 끈 GossipSub mesh를 사용합니다. |

재사용할 노드 설정은 최상위 `profiles` 맵에 정의합니다. join phase의 설정 결합 순서는 내장 `type`, 이름이 지정된 `profile`, phase 안의 `node` override 순서입니다. Phase의 `type`을 생략하면 지정 profile의 `type`으로 preset을 선택하고, 이것도 없으면 role에 따른 기본값을 사용합니다. Pointer 기반 boolean과 숫자 제어값은 명시적인 `false`와 `0`도 보존하므로 `historyGossip: 0`처럼 lazy gossip을 끄는 실험도 표현할 수 있습니다. Legacy flat 숫자 필드에서는 0이 생략을 뜻하므로 명시적으로 0으로 override하려면 중첩 제어값을 사용하십시오.

내장 `boot` type은 `gossipsub.enabled: false`를 설정합니다. bootstrap 노드도 PubSub에 참여시킬 때는 profile이나 phase의 `node` 블록에서 이를 명시적으로 `true`로 설정하십시오. 활성화한 boot 노드에는 아래에서 설명하는 해당 router에 맞는 parameter, scoring, inspection 옵션을 사용할 수 있습니다. [`examples/mixed-workers.yaml`](../examples/mixed-workers.yaml)이 이 opt-in 구성을 보여 줍니다. PubSub가 활성화된 Peer는 시작 직후 동일 run·정확한 topic의 Controller registry를 조회하고 이후 3초 loop로 polling합니다. 각 Peer가 조건에 맞는 전체 후보를 rendezvous hash로 정렬하고 전체 connection budget과 retry backoff 범위에서 현재 topic Peer 수와 `DHigh`의 차이를 채웁니다. 주기와 대체 후보 동작은 [bootstrap과 topic discovery](topology.kr.md#bootstrap과-topic-discovery)를 참고하십시오. 실제 GRAFT mesh는 GossipSub가 구성하므로 DHT bootstrap, transport 후보, mesh 구성원은 서로 구분됩니다.

```yaml
version: 2
name: mixed-workers
seed: 42
onExit: cancel
jobShutdownTimeout: 3m

profiles:
  tuned-mesh:
    type: full
    libp2p:
      connectionLimit: 128
      connectionManager:
        lowWater: 64
        highWater: 96
        gracePeriod: 30s
    kademlia:
      mode: server
      protocolPrefix: /k-p2p-lab/v3
      bucketSize: 20
      concurrency: 10
    gossipsub:
      router: gossipsub
      topics: [kpl/default]
      floodPublish: false
      peerExchange: true
      params:
        d: 8
        dLow: 6
        dHigh: 12
        dOut: 3
        dLazy: 8
        heartbeatInterval: 500ms

phases:
  - action: join
    group: tuned
    role: worker
    profile: tuned-mesh
    count: 100
    parallel: true
    parallelism: 16
```

### 프로토콜 설정

중첩된 node 설정에서 현재 고정된 버전의 libp2p 패키지가 지원하는 매개변수를 조정할 수 있습니다. 모든 duration 필드는 `250ms`, `30s`, `5m` 같은 Go duration 문자열을 사용합니다. [전체 프로토콜 레퍼런스](protocol-options.kr.md)에 mesh·score·PeerGater·filter·validation·discovery·publication·Kademlia의 모든 필드를 정리했습니다. [프로토콜 예제](../examples/protocol-options.yaml)도 참고하십시오.

| 영역 | 조정 가능한 필드 |
|---|---|
| `libp2p` | `userAgent`, `natPortMap`, `relay`, `relayService`, `connectionLimit`, `connectionManager.lowWater`, `connectionManager.highWater`, `connectionManager.gracePeriod`, `dialTimeout` |
| `kademlia` | `enabled`, `mode`, `protocolPrefix`, `protocolId`, `protocolExtension`, `bucketSize`, `concurrency`, `resiliency`, `lookupCheckConcurrency`, routing-table latency/refresh duration, `maxRecordAge`, provider/value/auto-refresh switch, optimistic-provide 설정, bootstrap timeout/retry interval, filter·diversity·datastore·validator·providerStore·bootstrapSource·requestHook 정책 |
| `gossipsub` | `enabled`, `router`, `topicMode`, `randomDegree`, `randomNetworkSize`, `subscribe`, `allowPublish`, `topics`, `floodPublish`, `peerExchange`, message/queue/validation limit, `signaturePolicy`, `seenMessagesTTL`, `subscriptionBufferSize`, `scoreInspectInterval`, message ID·topicOptions·filter·protocol·PeerGater·validator·RPC inspector·discovery·author·publish 정책 |
| `gossipsub.params` | 고정된 라이브러리의 모든 `GossipSubParams` 필드: mesh degree, history, gossip factor/retransmission, heartbeat, fanout, prune/backoff, connector, connection timeout, direct-connect 및 opportunistic-graft 제어값, IHAVE 제한, IDONTWANT 제한, IWANT follow-up time |
| `gossipsub.score` | 명시적 `enabled` switch, 선언형 P5를 포함한 전체 PeerScore weight와 decay 설정, score threshold, IP-colocation whitelist, mesh·최초 전달·실패·잘못된 메시지 항목을 모두 포함한 `topics.<topic>` score block |

GossipSub 전용 설정(`params`, 활성 score/inspection, `floodPublish`, `peerExchange`, `directPeers`, `peerGater`)은 `gossipsub` router에서만 적용됩니다. FloodSub와 RandomSub에서는 활성 scoring을 거부하지만 비활성 tuning 블록은 보존할 수 있습니다.

기본 mesh 설정은 `D=6`, `DLow=5`, `DHigh=12`, `DScore=4`, `DOut=2`, `DLazy=6`, history `5/3`, gossip factor `0.25`, heartbeat `1s`입니다. PubSub를 활성화한 `boot` preset은 `DScore=3`을 사용합니다. `full`/`worker`와 역할별 GossipSub worker preset은 v2에서 가져온 `DLow=5`, `DScore=3`, `maxIHaveLength=5500`, 초기 heartbeat `1s`를 상속합니다. 대부분의 worker preset은 v2의 hard connection limit `55`도 사용하며 `light`는 `32`를 사용합니다. Kademlia의 기본 bucket size는 `20`, protocol prefix는 `/k-p2p-lab/v3`입니다. 적용되는 기본값은 [`internal/model/config.go`](../internal/model/config.go), 분리된 typed schema는 [프로토콜 설정 구현 위치](protocol-options.kr.md#구현-경계)를 참고하십시오.

`libp2p.connectionLimit`는 전체 연결 수에 적용되는 resource manager hard cap일 뿐이며 soft connection manager를 암묵적으로 생성하지 않습니다. soft low-water/high-water 정리는 `libp2p.connectionManager` 블록을 명시했을 때만 설치되며 그 블록의 `lowWater`, `highWater`, `gracePeriod`를 사용합니다. 따라서 profile마다 hard cap, soft manager 또는 둘 다를 독립적으로 선택할 수 있습니다.

v2 설정을 이식할 때는 이름과 달리 prefix로 사용됐던 v2 `protocol_id`를 v3 `kademlia.protocolPrefix`로 옮기십시오. v3의 `kademlia.protocolId`는 Kademlia V1 wire protocol ID 전체를 정확히 override하며 `protocolPrefix`, `protocolExtension`과 함께 사용할 수 없으므로 v2 prefix 값을 이 필드에 복사하면 안 됩니다. 기본 prefix `/k-p2p-lab/v3`는 의도적으로 새로 분리한 protocol namespace입니다. v2 namespace 호환이 필요한 실험에서만 `protocolPrefix: /k-p2p-lab/kad-dht`를 사용하십시오.

RandomSub에서 `randomDegree`는 libp2p의 process-global `RandomSubD`를 통해 최소 연결/degree 목표를 설정합니다. KPL은 Peer마다 별도 컨테이너를 사용하므로 이 global 값은 Peer 하나에만 적용됩니다. `randomNetworkSize`는 `NewRandomSub`에 별도로 전달되는 추정 전체 네트워크 크기입니다.

스코어링은 **기본 비활성**입니다. `gossipsub.score.enabled: true`로만 활성화하며 score 블록이나 관측 주기만으로 켜지지 않습니다. 명시적으로 켜면 `scoreInspectInterval`의 기본값은 `1s`이며 `0s`는 관측을 끕니다. inspection이 실행될 때마다 노드의 `peerScores` 맵이 갱신됩니다. 이 값은 `/api/v1/nodes`, `/api/v1/network`, `/api/v1/snapshot`, SSE snapshot stream에서 제공되며 topology의 노드를 선택하면 세부 정보에 관측한 score 수와 평균이 표시됩니다.

`appSpecificScore`는 고정 `default`와 선택적인 libp2p Peer ID별 `peers` map으로 P5를 제공합니다. 명시적 Peer 값은 0을 포함하여 기본값보다 우선합니다. 활성 `appSpecificWeight`가 0이 아니면 이 정책이 필요하며 모든 값과 가중치를 적용한 P5 결과는 유한해야 합니다. 활성 selective validation은 생략한 decay group에 `1s`/`0.01`, 생략한 비활성 P1 quantum에 `1s`를 적용하고 명시적 0 ticker·제수 duration은 거부합니다. Profile에서 inline으로 `score`를 덮어쓰면 전체 블록을 교체하여 명시적 0·false를 보존합니다. [점수 설정](protocol-options.kr.md#peer-scoring-기본-비활성)을 참고하십시오.

### 노드별 네트워크 조건

Profile 또는 join 단계의 `node` 블록에 `network`를 추가할 수 있습니다. 기본 `scope: p2p`는 각 Peer 컨테이너 안에서 포트 `20000`의 송신 P2P TCP에만 적용합니다. v2처럼 제어 API·telemetry까지 포함한 전체 송신을 제한하려면 `scope: all`을 지정하십시오. `delay`는 편도 egress 추가 지연이며 왕복 지연의 목표값이 아닙니다. `rateMbps`와 `tbf.rateMbps`는 송신 제한을 설정하고 [Bandwidth 측정](bandwidth.kr.md)은 실제 libp2p 스트림 사용량을 별도로 기록합니다.

```yaml
node:
  network:
    delay: 100ms
    jitter: 10ms
    lossPercent: 1
    duplicatePercent: 0.1
    corruptPercent: 0.1
    reorderPercent: 1
    rateMbps: 10
    queueLimit: 1000
```

| 필드 | 의미 |
|---|---|
| `delay`, `jitter` | 추가 지연과 변동 폭을 음수가 아닌 Go duration으로 지정합니다. jitter에는 양수 delay가 필요하며 Linux netem에 따라 최대 `2147483647ns`입니다. |
| `lossPercent`, `duplicatePercent`, `corruptPercent`, `reorderPercent` | `0`~`100` 범위의 패킷 비율입니다. 재정렬에는 양수 delay가 필요합니다. |
| `rateMbps` | 음수가 아닌 송신 속도 제한(Mbps)입니다. `0`은 속도 제한을 해제합니다. |
| `queueLimit` | netem 큐의 최대 패킷 수이며 `1`~`4294967295`입니다. 이 필드만 지정해도 netem을 활성화합니다. |
| `scope` | `p2p`(기본) 또는 `all` 송신 트래픽입니다. |
| `delayDistribution` | interval과 같은 분포 형식으로 피어별 기본 지연을 한 번 추출합니다. `delay`와 배타적이며 jitter나 재정렬을 사용하면 양수 `min`이 필요합니다. |
| `jitterDistribution` | `normal`(기본), `uniform`(v2 방식), `pareto`, `paretonormal`입니다. |
| `reorderCorrelationPercent` | `0`~`100`의 재정렬 상관계수이며 v2의 `reorder.chance`에 대응합니다. 양수로 설정하려면 양수 `reorderPercent`가 필요합니다. |
| `tbf` | 양수 유한값인 `rateMbps`, `burstKbit`(kilobit)와 `1us`~`4294967295us`의 `latency`로 token bucket을 설정합니다. 양수 netem `rateMbps`와 배타적입니다. |

v2 실험을 옮기실 때는 [재현 검토와 설정 대응표](v2-reproduction.kr.md), [churn 예제](../examples/v2-churn.yaml)를 참고하십시오. 여기에는 배치(`balanced`, 노드별 `random`, batch별 `single-agent`, 명시적 `agentId`), churn을 위한 `onError: continue`, 정확한 PubSub 데이터 크기를 위한 `payloadEncoding: raw`, 전체 topic 발행을 위한 `topic: '*'`를 설명합니다. Network scope와 payload encoding은 기존 기본값을 유지합니다. Worker bootstrap은 seed 기반 첫 연결 성공 선택을 사용하며 transport stack은 TCP/Noise/Yamux로 명시합니다.

네트워크 조건을 적용하는 Peer에는 Linux `NET_ADMIN`과 호스트 커널의 `sch_netem` 지원이 필요합니다. Docker 런타임은 해당 Peer에만 `NET_ADMIN`을 추가합니다. 커널 지원이 없거나 `tc` 명령이 실패하면 노드 시작도 명시적인 오류로 실패합니다.

[`examples/network-conditions.yaml`](../examples/network-conditions.yaml)은 bootstrap 노드 두 개와 네트워크 조건이 적용된 worker 네 개를 생성하고, 초기화 대기와 메시지 발행 후 모든 노드를 종료합니다. 대시보드에서 실행하거나 다음과 같이 제출할 수 있습니다.

`control-node:8080`은 `access`가 표시한 Controller 주소로 바꾸고, 먼저 [API 인증](api.kr.md#인증)으로 `KPL_COOKIE_JAR`를 생성하십시오.

```bash
curl -X POST http://control-node:8080/api/v1/experiments \
  -H 'Content-Type: application/yaml' \
  -b "${KPL_COOKIE_JAR:?Log in first}" -H 'X-KPL-Request: dashboard' \
  --data-binary @examples/network-conditions.yaml
```

## 시나리오와 잡

권장 시나리오 형식은 version 2 YAML입니다. v2의 중요한 실행 제어를 유지하면서 명시적인 job 추적과 readiness barrier를 추가했습니다. 기존 줄 단위 `.kpl` DSL 자체를 직접 읽지는 않으므로 해당 명령을 phase로 변환해야 합니다.

| Action | 역할 |
|---|---|
| `join` | Agent 용량을 고려하여 노드를 생성합니다. |
| `wait-ready` | 필요하면 지정 job 완료 또는 `minCount` 하한을 먼저 확인한 뒤 그룹/type의 목표 준비율을 기다립니다. |
| `publish` | 준비된 노드를 그룹에서 선택해 메시지를 발행합니다. `deliveryWindow`는 메시지별 수신 기한입니다(기본 `10s`, 양수, 최대 `1h`). |
| `leave` | 지정 그룹에서 노드를 선택해 종료합니다. |
| `wait` / `sleep` | 지정 시간 동안 기다립니다. `sleep`은 v2 호환 alias입니다. |
| `wait-jobs` | `jobs`에 지정한 background job을 기다리며, 목록이 비어 있으면 모든 job을 기다립니다. |
| `log` | Controller 로그에 시나리오 메시지를 기록합니다. |
| `stop-all` / `reset` | 모든 background job을 취소하고 종료될 때까지 기다린 뒤 run generation fence를 사용해 해당 실험의 현재 및 이전 Peer generation을 종료합니다. `reset`은 alias입니다. |

`join`에서 `count`는 정확한 생성 작업 수입니다. `publish`와 `leave`에서는 조건에 맞는 후보 노드 수로 제한되는 최댓값이며, publish 후보는 PubSub와 발행이 활성화되어 있고 요청한 topic에 참여해야 합니다. `repeat`는 phase 전체를 반복합니다. `parallel`은 replica의 순차 또는 동시 실행을 선택하며 `parallelism`으로 최대 동시 작업 수를 제한할 수 있습니다. `await`의 기본값은 `true`입니다. `false`로 설정하면 `job` 이름으로 추적되는 background job이 시작되어 churn이나 publish가 진행되는 동안 다음 phase를 실행할 수 있습니다. 이후 `wait-jobs`에서 이름으로 선택해 기다릴 수 있습니다.

### Phase 필드와 기본값

| 필드 | 적용 action | 의미와 기본값 |
|---|---|---|
| `name`, `repeat` | 모든 action | 표시 이름은 기본적으로 `<action>-<phase number>`이며 `repeat`를 생략하거나 0으로 설정하면 `1`입니다. |
| `job`, `await` | `join`, `publish`, `leave` | `await: false`가 background job을 만들며 `job`을 생략하면 `phase-<phase number>`입니다. Job ID는 `stop-all`로 추적 상태를 초기화하기 전까지 고유해야 합니다. |
| `group`, `count` | `join`, `publish`, `leave` | 필수 group과 양수 작업 수입니다. Publish의 각 반복은 조건에 맞는 서로 다른 publisher를 최대 `count`개 선택합니다. |
| `type` | `join`, `publish`, `leave`, `wait-ready` | Join preset을 선택하거나 적용 완료된 type으로 기존 노드를 필터링합니다. |
| `role`, `profile`, `node` | `join` | Role은 `boot` 또는 기본값 `worker`이며 profile과 inline node 설정으로 preset을 조정합니다. |
| `placement`, `agentId` | `join` | 기본값 `balanced`는 다음 Peer를 포함한 예상 점유량/유효 Agent capacity(웹 예외값 포함)가 가장 낮은 곳을 선택하고 `random`은 노드마다, `single-agent`는 batch마다 Agent 하나를 선택합니다. 명시적 `agentId`는 배치를 고정하며 admission은 가용 용량을 기다립니다. |
| `parallel`, `parallelism` | `join`, `publish`, `leave` | 기본값은 순차 실행입니다. Parallel을 켜고 parallelism을 생략하거나 0으로 설정하면 batch 전체의 동시 실행을 허용합니다. 명시한 값은 실행 중인 작업뿐 아니라 worker goroutine 수도 제한하며, 대기 중인 publish의 지연 시간은 batch 시작 시점 기준을 유지합니다. |
| `interval`, `lifetime` | 간격이 있는 작업; lifetime은 `join`만 | 아래 시간 규칙을 따릅니다. Peer lifetime은 컨테이너 생성 성공 후 시작하므로 설정 복사, start, bootstrap을 포함하며 background job 완료와 독립적입니다. |
| `topic`, `payloadSize`, `payloadEncoding` | `publish` | Topic 기본값은 publisher의 첫 설정 topic이며 `'*'`는 설정한 모든 topic으로 발행합니다. 0 이하 `payloadSize`는 `32`로 처리하며 16 MiB를 초과하는 값은 시나리오 검증에서 거부합니다. 기본값 `envelope`는 JSON/base64 metadata를 추가하고 `raw`는 PubSub data를 정확히 `payloadSize` byte로 만듭니다. |
| `deliveryWindow`, `onError` | `publish`; `onError`는 `leave`에도 적용 | Window 기본값은 `10s`입니다. 기본값 `onError: fail` 대신 `continue`를 사용하면 개별 작업 실패를 기록하고 churn을 계속합니다. Publish 후보가 없을 때 `continue`는 아무 작업 없이 성공합니다. |
| `group`, `type`, `readyRatio`, `minCount`, `jobs`, `timeout` | `wait-ready` | 빈 group/type은 현재 generation의 모든 노드에 대응합니다. Ratio 기본값은 `1`이며 유한한 `(0, 1]` 값이어야 합니다. Timeout 기본값은 `1m`이며 job 대기와 readiness를 함께 제한합니다. `minCount`는 cohort 크기의 하한입니다. |
| `jobs`, `timeout` | `wait-jobs` | 빈 jobs는 추적하는 모든 job을 뜻하며 timeout 기본값은 `5m`입니다. |
| `duration`; `message` | `wait` / `sleep`; `log` | 양수 대기 시간과 Controller 로그 메시지입니다. |

phase 목록이 자연스럽게 끝났을 때 background job을 처리하는 방식은 최상위 `onExit`으로 결정합니다. 기본값 `cancel`은 남은 job을 취소한 다음 종료될 때까지 기다립니다. `onExit: drain`은 job이 자연스럽게 완료될 때까지 기다립니다. 단일 실행의 자연스러운 성공에는 이 job 정책만 적용하며, 명시적인 `stop-all` phase가 없으면 Peer 컨테이너는 계속 실행됩니다. Dashboard/API에서 `repetitions > 1`로 제출한 배치는 마지막 iteration을 포함한 매 iteration 종료 때 Peer를 자동으로 fence하고 정리하여 다음 실행과 겹치지 않게 합니다. 이는 YAML phase의 `repeat`와 별개의 기능입니다.

`jobShutdownTimeout`의 기본값은 `3m`입니다. 사용자 또는 API 요청이 scenario를 취소하거나 phase 또는 background job이 실패하면 Controller는 남은 job을 취소하고 이 제한 안에서 종료를 기다린 뒤, 모든 Agent에 현재 generation까지 generation fence를 설정하고 Peer를 정리하도록 요청합니다. 명시적인 `stop-all`도 같은 제한 시간 내 job 종료를 적용하고 job 추적 상태를 초기화한 뒤 현재 run generation을 fence로 설정합니다. Agent는 일치하는 컨테이너를 종료하기 전에 단조 증가하는 fence를 기록합니다. 따라서 generation N의 늦은 create는 fence보다 먼저 완료되어 cleanup에 포함되거나, generation이 fence 이하이므로 거부됩니다. `stop-all`이 성공하면 scenario는 generation N+1로 진행하므로 이후 phase에서 같은 run ID로 새 노드를 만들고 job ID도 다시 사용할 수 있습니다.

`wait-ready`는 실패, 종료 중, 종료된 노드를 포함하여 현재 run generation에서 조건에 맞는 전체 cohort를 검사하며 이전 generation의 노드는 무시합니다. cohort에 실패한 노드가 하나라도 있으면 barrier는 성공하지 않으며, 준비 상태로 보고된 노드도 해당 Agent가 online일 때만 ready 수에 포함됩니다. 이 cohort 역시 지금까지 관측된 노드로만 구성되므로 `await: false` join 다음의 `wait-ready`에는 `jobs: [job-id]` 또는 `minCount` 중 하나가 필요합니다. `jobs`는 지정 producer job이 완료된 뒤 readiness를 검사합니다. `minCount`는 producer를 계속 실행하면서 일부만 생성된 그룹이 준비율을 너무 일찍 만족하지 못하게 합니다.

| `parallel` | `await` | 동작 |
|---|---|---|
| `false` | `true` | 간격을 적용한 replica를 순차 실행하며 다음 phase가 기다립니다. |
| `true` | `true` | replica를 동시에 실행하며 다음 phase가 전체 batch를 기다립니다. |
| `false` | `false` | 간격을 적용한 순차 job이 background에서 실행됩니다. |
| `true` | `false` | 동시 실행 job이 background에서 실행됩니다. |

v2 호환을 위해 `interval`을 생략한 `publish` phase에는 특수 기본값을 적용합니다. `parallel: true`이면 각 작업에 phase 시작 기준 `1s` offset을 주며, 순차 publish의 delay는 `0`입니다. 이 규칙은 `await` 값과 무관합니다.

순차 `join`, `publish`, `leave`는 첫 작업을 즉시 실행하고, 이후 작업은 batch 시작 시각에 앞서 sample된 interval을 누적한 시각에 예약합니다. 요청 처리·접수 대기 시간은 interval 안에 포함되며 매번 추가되지 않습니다. 예약보다 늦어진 작업은 건너뛰지 않고 순서대로 실행하며 원래 일정에 따라잡을 때까지 추가로 쉬지 않습니다. 취소·오류는 여전히 실행을 중단하며 Agent 용량 제한도 적용됩니다. 병렬 `join`과 `leave`는 `interval`을 무시합니다. 병렬 `publish`는 각 sample interval을 batch 시작 기준의 독립적인 offset으로 사용하고 그 후 동시 실행 slot을 확보합니다. Offset은 누적하지 않으며 동시 실행 용량에 따라 dispatch가 더 늦어질 수 있습니다.

순차 join이 예약보다 1초 이상 늦어지면 Controller는 job 실행별 최대 10초에 한 번 `churn join schedule delayed`를 기록합니다. 실행 ID, 그룹, 작업 번호, 예약 시각, 지연 시간이 포함됩니다. 지속적인 용량·API 병목을 확인할 수 있지만 과부하 상황의 Docker 시작·ready 시각까지 보장하지는 않습니다. Agent heartbeat는 이벤트 전송과 독립적으로 실행하므로 느린 telemetry 요청이 생존 신호 전송을 막지 않습니다.

`wait` duration과 readiness/job timeout은 양수여야 합니다. join의 `lifetime`을 생략하면 자동 leave를 하지 않지만 명시적으로 sample된 `0s` lifetime은 v2와 같이 새 노드를 즉시 종료합니다.

```yaml
phases:
  - name: background churn
    action: join
    job: churn
    await: false
    parallel: false
    group: churners
    role: worker
    type: light
    count: 100
    interval: {model: exponential, mean: 250ms}
    lifetime: {model: pareto, xm: 45s, alpha: 2.5, max: 3m}

  - action: wait
    duration: 10s

  - action: wait-ready
    group: churners
    jobs: [churn]
    readyRatio: 1
    timeout: 5m
```

간격과 수명에는 다음 분포를 사용할 수 있습니다. duration 값은 Go duration 문자열을 사용하며 선택적인 `min`/`max` duration 범위로 결과를 제한할 수 있습니다.

| Model | 매개변수와 sampling 의미 |
|---|---|
| `fixed` | `value`가 정확한 duration입니다. |
| `exponential` | `mean`은 연속 지수분포 sample의 평균 duration입니다. |
| `normal` | `mean`과 `sigma`는 duration 단위의 평균과 표준편차입니다. 음수 sample은 0으로 제한한 뒤 선택적인 범위를 적용합니다. |
| `pareto` | `xm`은 scale duration이며 `alpha`는 무차원 shape입니다. |
| `poisson` | v2 호환 동작입니다. duration인 `mean`을 초 단위 Poisson 평균으로 변환하며 sample 결과는 정수 초로 양자화됩니다. |
| `gamma` | `alpha`는 shape입니다. v2 호환 `beta`(초당 rate) 또는 `scale`(duration) 중 정확히 하나만 사용해야 하며 두 형식은 상호배타적입니다. |
| `lognormal` | `mu`는 log-space 평균이고 sigma는 무차원입니다. `sigma: "0.5"` 같은 숫자 문자열 또는 숫자 필드 `logSigma` 중 정확히 하나를 사용합니다. 표준정규분포 `Z`에 대해 결과는 `exp(mu + sigma*Z)`초입니다. |

0이 아닌 scenario seed는 초기 상태, 조건에 맞는 후보 집합, readiness/job barrier가 같을 때 sample된 delay와 분포 sample, 무작위 선택 및 순서를 재현합니다. Peer identity는 `(run ID, 노드별 seed)` 조합마다 결정됩니다. 같은 조합은 같은 Peer ID를 만들지만, run ID가 다르면 실행 간 identity 충돌을 막기 위해 의도적으로 다른 Peer ID를 만듭니다. 따라서 같은 scenario seed는 실행 간 sampling과 순서를 재현하지만 run ID가 다르면 Peer ID까지 같아지지는 않습니다. `seed: 0`은 의도적으로 실행 시각 기반 seed를 선택합니다. 먼저 [`examples/smoke.yaml`](../examples/smoke.yaml)을 확인하고, custom profile, 이종 worker type, 제한된 병렬 batch, background paced job은 [`examples/mixed-workers.yaml`](../examples/mixed-workers.yaml)을 참고하십시오.

## 실행 및 보존 한계

Docker 런타임은 Peer마다 네트워크를 격리하고 노드별 P2P egress 조건을 지원합니다. `wait-ready`는 Peer 초기화와 API 준비 완료를 확인하며 mesh 수렴을 검증하지 않습니다. 주기적 topic discovery가 churn 중 transport 후보를 보강하지만 GossipSub heartbeat와 GRAFT 처리에는 여전히 수렴 시간이 필요합니다. 필요한 실험에는 별도 안정화 대기 단계를 추가하십시오. scenario seed는 앞서 설명한 조건에서 애플리케이션 sampling과 순서를 재현하지만, 커널의 packet impairment나 네트워크 타이밍까지 동일하게 재현하지는 않습니다. HopWave는 지원하지 않습니다.

종료된 노드의 이력은 메모리에 유지되며 Agent의 전체 등록 정보와 Controller의 전체 snapshot에 포함됩니다. 평상시 부분 heartbeat, discovery, dashboard SSE는 활성 노드와 전송할 변경 사항을 사용하여 전체 이력의 반복 처리를 피합니다. 보존 메모리와 전체 snapshot 크기까지 제한하는 것은 아니므로, 장기간 대규모 churn에는 제한된 보존 정책과 별도의 pagination 기반 history API가 추가로 필요합니다. 대규모 parallel phase에는 `parallelism`을 명시하십시오. 생략하면 여전히 batch 전체의 동시 실행을 허용합니다.

# 프로토콜 설정 레퍼런스

[English](protocol-options.md) | 한국어

[문서 안내](README.kr.md) · [저장소](../README.kr.md)

이 문서는 고정 의존성 `go-libp2p-pubsub v0.13.1`, `go-libp2p-kad-dht v0.31.0`에 대한 선언형 노드 설정을 설명합니다. 각 블록은 시나리오 profile 또는 join phase의 `node`에 작성합니다. [시나리오 설정](scenario-reference.kr.md)과 [프로토콜 예제](../examples/protocol-options.yaml)를 참고하십시오. 실험은 [Swarm](swarm.kr.md)에서 실행하며 배포 전에 `kpl validate --scenario FILE`로 YAML을 검증합니다.

Duration에는 `250ms`, `1s`, `5m` 같은 Go 문자열을 사용합니다. 포인터로 표현하는 숫자·boolean 설정은 유효한 범위에서 명시적인 `0`·`false`를 보존합니다. List와 map은 상속된 list·map 전체를 교체합니다. `score`, `peerGater`, `discovery` 같은 선택적 객체 블록도 상속된 객체 전체를 교체하므로 유지하려는 설정을 다시 작성해야 합니다. `enabled: true`가 없는 `score` 블록은 활성 profile score를 교체한 경우에도 비활성입니다.

## PubSub 활성화와 공통 설정

이 절의 필드는 모두 `gossipsub` 아래에 둡니다. 생략한 라이브러리 선택 설정은 고정 버전의 기본값을 유지합니다. 기본 노드 preset은 아래에서 설명하는 별도 mesh 설정을 제공할 수 있습니다.

| 필드 | 동작 |
|---|---|
| `enabled` | PubSub 활성화. 기본 true이며 `boot` preset은 명시적으로 재설정하지 않으면 비활성입니다. |
| `router` | `gossipsub`(기본), `floodsub`, `randomsub`. |
| `topics` | 참여 topic 이름. 기본 `[kpl/default]`이며 topic별 설정은 정확히 같은 문자열을 사용합니다. |
| `topicMode` | `subscribe`(기본), `relay`, `publish`. Relay는 애플리케이션 구독 없이 전달하며 publish는 참여하되 구독하지 않습니다. |
| `subscribe` | 호환 설정. `topicMode`가 없을 때 false는 publish 모드를 선택합니다. |
| `allowPublish` | 애플리케이션 발행 요청 허용. 기본 true. |
| `randomDegree`, `randomNetworkSize` | 양수인 RandomSub degree 목표와 추정 전체 네트워크 크기. 각 Peer 컨테이너에는 독립된 process-global degree가 적용됩니다. |
| `floodPublish`, `peerExchange` | GossipSub flood 발행과 PRUNE peer exchange. 명시적인 false를 보존합니다. |
| `maxMessageSize` | 직렬화된 PubSub RPC 최대 바이트 수. 기본 1 MiB이며 0도 실제 제한값으로 전달합니다. |
| `peerOutboundQueueSize` | 양수인 Peer별 송신 RPC 큐 크기. 라이브러리 기본 32. |
| `validateQueueSize` | 양수인 수신 검증 큐 크기. 라이브러리 기본 32. |
| `validateThrottle` | 동시 비동기 검증 한도. 라이브러리 기본 8192이며 0 이상입니다. 0은 비동기 검증을 허용하지 않습니다. |
| `validateWorkers` | 양수인 검증 worker 수. 기본값은 프로세스에서 보이는 CPU 수입니다. |
| `subscriptionBufferSize` | 0 이상인 애플리케이션 구독 버퍼 크기. Topic별 설정이 우선합니다. |
| `signaturePolicy` | `strict-sign`(라이브러리 기본), `strict-no-sign`, `lax-sign`, `lax-no-sign`. Strict 정책은 선택한 서명 규칙을 수신 메시지에도 적용하며 lax 정책은 수신 서명 요건을 완화합니다. |
| `seenMessagesTTL` | PubSub seen-message cache 보존 시간. 라이브러리 기본 `2m`이며 score 전달 기록 TTL과 구분합니다. |
| `seenMessagesStrategy` | `first-seen`(라이브러리 기본) 또는 `last-seen` cache 만료 전략. |
| `messageId` | `default`는 author·sequence, `sha256`은 메시지 데이터 hash, `topic-sha256`은 길이를 앞에 붙인 topic과 데이터의 hash를 사용합니다. 내용이 같은 raw 발행은 같은 PubSub 메시지로 합쳐질 수 있습니다. |
| `scoreInspectInterval` | 점수 snapshot 주기. 스코어링을 명시적으로 켰을 때만 기본 `1s`; `0s`는 관측을 끕니다. 이 필드만으로 스코어링을 켜지 않습니다. |

GossipSub mesh parameter, 활성 scoring, flood 발행, peer exchange, direct peer와 PeerGater는 GossipSub router에 적용됩니다. FloodSub는 mesh feature 없는 사용자 지정 protocol ID를 사용할 수 있으며 RandomSub는 사용자 지정 protocol 목록을 지원하지 않습니다. 공통 filter, validator, message ID, publication과 discovery 정책은 공통 PubSub API를 사용합니다.

## GossipSub mesh parameter 전체 32개

`gossipsub.params` 아래에 설정합니다. 아래 기본값은 노드 preset 또는 profile을 적용하기 전의 KPL 기본값입니다.

| 필드 | 기본값 | 의미 |
|---|---|---|
| `d` | 6 | 목표 mesh degree. |
| `dLow` | 5 | Mesh 보충 기준. |
| `dHigh` | 12 | Mesh pruning 기준. |
| `dScore` | 4 | Pruning할 때 유지할 고득점 Peer 수. |
| `dOut` | 2 | 최소 outbound mesh 연결 수. |
| `dLazy` | 6 | 최소 gossip 대상 수. |
| `historyLength` | 5 | 메시지 cache history slot 수. |
| `historyGossip` | 3 | Gossip으로 알릴 최근 history slot 수. |
| `gossipFactor` | 0.25 | 적격 Peer 중 gossip 대상으로 선택할 비율. |
| `gossipRetransmission` | 3 | IWANT 응답에서 메시지별 재전송 한도. |
| `heartbeatInitialDelay` | `100ms` | 첫 heartbeat 전 지연. |
| `heartbeatInterval` | `1s` | Mesh 유지보수 주기. |
| `slowHeartbeatWarning` | 0.1 | 느린 heartbeat 경고에 사용하는 heartbeat 주기의 비율. |
| `fanoutTTL` | `1m` | 비활성 fanout 상태 보존 시간. |
| `prunePeers` | 16 | PRUNE에 포함하는 peer exchange 항목 수. |
| `pruneBackoff` | `1m` | Mesh Peer pruning 후 backoff. |
| `unsubscribeBackoff` | `10s` | 구독 해제 뒤 PRUNE backoff. |
| `connectors` | 8 | PX로 얻은 Peer의 동시 연결 worker 수. |
| `maxPendingConnections` | 128 | PX 연결 대기 큐 한도. |
| `connectionTimeout` | `30s` | 해당 연결의 timeout. |
| `directConnectTicks` | 300 | Direct peer 재연결 검사 사이 heartbeat 수. |
| `directConnectInitialDelay` | `1s` | 최초 direct peer 연결 지연. |
| `opportunisticGraftTicks` | 60 | Opportunistic graft 검사 사이 heartbeat 수. |
| `opportunisticGraftPeers` | 2 | Opportunistic graft에서 선택할 Peer 수. |
| `graftFloodThreshold` | `10s` | 이른 반복 GRAFT에 penalty를 적용하는 기준 시간. |
| `maxIHaveLength` | 5000 | IHAVE에 넣거나 heartbeat당 Peer에서 수락·요청하는 최대 ID 수. |
| `maxIHaveMessages` | 10 | Peer별 heartbeat당 IHAVE 처리 한도. |
| `maxIDontWantLength` | 10 | IDONTWANT에 포함하거나 수락하는 최대 메시지 ID 수. |
| `maxIDontWantMessages` | 1000 | Peer별 heartbeat당 IDONTWANT 처리 한도. |
| `iWantFollowupTime` | `3s` | IWANT 약속을 이행하지 않아 행동 penalty를 부여하기 전 대기 시간. |
| `iDontWantMessageThreshold` | 1024 | IDONTWANT 생성 기준 메시지 바이트 수. |
| `iDontWantMessageTTL` | 3 | IDONTWANT 상태를 보존하는 heartbeat 수. |

Degree에는 `dLow <= d <= dHigh`, `0 <= dScore <= d`, `dOut < dLow`, `dOut <= d/2` 제약이 있습니다. History는 `0 <= historyGossip <= historyLength`이며 `d`, `dHigh`, `historyLength`는 양수입니다. Heartbeat interval과 두 tick counter도 양수이고 나머지 count와 duration은 0 이상입니다. Gossip factor는 유한한 `[0,1]`, slow-heartbeat warning은 유한한 0 이상 값입니다. 0은 동작을 억제할 수 있으며 자동으로 기본값으로 바꾸지 않습니다.

`boot`에서 PubSub를 켜면 preset의 `dScore: 3`을 사용합니다. Worker preset은 v2의 `dLow: 5`, `dScore: 3`, `maxIHaveLength: 5500`, `heartbeatInitialDelay: 1s`를 상속합니다. 실제 값은 [타입 schema](../internal/model/gossipsub_config.go)와 [기본값 처리](../internal/model/config.go)에서 확인할 수 있습니다.

## Peer scoring: 기본 비활성

**`gossipsub.score.enabled: true`**를 설정해야 PeerScore를 설치합니다. `score` 또는 `score.enabled`를 생략하거나 false로 설정하면 스코어링과 점수 관측을 실행하지 않습니다. 비활성 블록에는 미완성 tuning을 보존할 수 있지만 잘못된 duration, 비유한 수치, 잘못된 CIDR·Peer ID와 빈 topic key는 거부합니다. 활성 구성에서는 router가 요구하는 필드 간 관계까지 검증합니다.

| `gossipsub.score` 필드 | 의미와 활성 상태 검증 |
|---|---|
| `enabled` | 명시적 활성화. 기본 false. |
| `skipAtomicValidation` | 설정하지 않은 score group을 비활성으로 둘 수 있습니다. 기본 false는 라이브러리의 전체 group 검증을 유지합니다. |
| `topicScoreCap` | 양의 topic 기여 합계에 적용할 0 이상 상한. 0은 상한 없음. |
| `appSpecificWeight` | 유한한 P5 가중치. 양수·음수 모두 가능하며 0이 아니면 `appSpecificScore`가 필요합니다. |
| `appSpecificScore.default` | 기본 P5 원점수. 생략하면 0. |
| `appSpecificScore.peers` | libp2p Peer ID에서 P5 원점수로의 map. 명시적 0도 기본값보다 우선하며 같은 Peer ID의 중복 표기는 거부합니다. |
| `ipColocationFactorWeight` | 0 이하인 P6 가중치. 0은 penalty 비활성. |
| `ipColocationFactorThreshold` | P6 가중치가 0이 아니면 최소 1. |
| `ipColocationFactorWhitelist` | Colocation penalty에서 제외할 CIDR 문자열. |
| `behaviourPenaltyWeight` | 0 이하인 P7 가중치. 0은 비활성. |
| `behaviourPenaltyThreshold` | 0 이상인 penalty counter 기준. |
| `behaviourPenaltyDecay` | P7 활성 시 `(0,1)`인 counter decay factor. |
| `decayInterval` | Counter 유지보수 주기. 최소 `1s`. |
| `decayToZero` | `(0,1)`인 counter cutoff. |
| `retainScore` | 0 이상인 연결 해제 Peer의 점수 보존 시간. 0은 보존하지 않음. |
| `seenMessageTTL` | 0 이상인 score 전달 기록 보존 시간. 0은 라이브러리 기본 `2m`을 사용하며 `gossipsub.seenMessagesTTL`과 구분합니다. |
| `thresholds` | 아래 threshold 블록. |
| `topics` | 정확한 참여 topic 이름에서 아래 TopicScore 블록으로의 map. 활성 score key는 참여 topic을 가리켜야 합니다. |

P5는 선택한 원점수에 `appSpecificWeight`를 곱합니다. Peer별 map이 default보다 우선하며 각 Peer 인스턴스에서 값은 고정됩니다. Key는 시나리오 node ID·group·profile 이름이 아닌 libp2p Peer ID입니다. 모든 원점수와 가중치 적용 결과는 유한해야 합니다. Callback은 불변 메모리 map을 읽으며 네트워크 요청을 하지 않습니다.

활성 selective validation에서 `decayInterval`/`decayToZero` group 전체를 생략하면 `1s`/`0.01`을 적용합니다. 비활성 time-in-mesh group 전체를 생략하면 안전한 `timeInMeshQuantum: 1s`를 적용합니다. 이 ticker·나눗셈 설정의 명시적 `0s`는 거부합니다. 고정 라이브러리의 selective validator가 허용하는 panic 조건을 막기 위한 기본값이며 scoring 가중치를 켜지는 않습니다.

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

### PeerScore threshold 전체

`gossipsub.score.thresholds` 아래에 설정합니다.

| 필드 | 제약과 동작 |
|---|---|
| `skipAtomicValidation` | 설정하지 않은 threshold group 허용. 기본 false. |
| `gossipThreshold` | Gossip 전달의 0 이하 score 기준. |
| `publishThreshold` | `gossipThreshold` 이하. 이보다 낮으면 flood/fanout 발행을 억제합니다. |
| `graylistThreshold` | `publishThreshold` 이하. 이보다 낮으면 메시지 처리를 억제합니다. |
| `acceptPXThreshold` | Peer exchange를 수락하는 데 필요한 0 이상 score. |
| `opportunisticGraftThreshold` | Opportunistic graft를 위한 0 이상 mesh score 중앙값 기준. |

### TopicScore parameter 전체

`gossipsub.score.topics.<참여-topic>` 아래에 설정합니다.

| 필드 | 의미 |
|---|---|
| `skipAtomicValidation` | 설정하지 않은 score group 허용. 기본 false. |
| `topicWeight` | Topic 전체 기여의 0 이상 배수. |
| `timeInMeshWeight` | 0 이상 P1 가중치. 0은 비활성. |
| `timeInMeshQuantum` | 양수인 mesh 시간 제수. |
| `timeInMeshCap` | P1 활성 시 양수인 상한. |
| `firstMessageDeliveriesWeight` | 0 이상 P2 가중치. |
| `firstMessageDeliveriesDecay` | P2 활성 시 `(0,1)`인 factor. |
| `firstMessageDeliveriesCap` | P2 활성 시 양수인 counter 상한. |
| `meshMessageDeliveriesWeight` | 0 이하 P3 전달 부족 가중치. |
| `meshMessageDeliveriesDecay` | P3 활성 시 `(0,1)`인 factor. |
| `meshMessageDeliveriesThreshold` | P3 활성 시 양수인 전달 목표. |
| `meshMessageDeliveriesCap` | P3 활성 시 양수인 counter 상한. |
| `meshMessageDeliveriesActivation` | P3 활성 시 최소 `1s`. |
| `meshMessageDeliveriesWindow` | 0 이상인 near-first 전달 계측 기간. |
| `meshFailurePenaltyWeight` | 0 이하 P3b sticky mesh-failure 가중치. |
| `meshFailurePenaltyDecay` | P3b 활성 시 `(0,1)`인 factor. |
| `invalidMessageDeliveriesWeight` | 0 이하 P4 invalid-delivery 가중치. |
| `invalidMessageDeliveriesDecay` | 설정된 P4 group에서 `(0,1)`인 factor. Atomic validation에서는 가중치가 0이어도 필요합니다. |

비활성 가중치 항목을 포함해 모든 score 부동소수점 값은 유한해야 합니다. Topic score 블록을 생략하면 해당 topic은 점수를 계산하지 않습니다. 전체 group 검증에서는 가중치가 0이어도 유효한 time-in-mesh quantum과 invalid-delivery decay를 제공하십시오. 비활성이면 토폴로지 UI에 **Peer scores: Disabled**로 표시합니다. 활성 점수 snapshot은 node/network/snapshot API와 SSE의 `peerScores`에 있습니다. Router 관측값이며 실험 결과의 고정 기대값은 아닙니다.

## PeerGater parameter 전체 11개

`gossipsub.peerGater` 블록을 지정하면 검증 부하 gate를 설치합니다. 블록을 생략하면 비활성이며 PeerScore와 독립적입니다. 기본값은 고정 라이브러리의 `DefaultPeerGaterParams`를 사용합니다.

| 필드 | 기본값·제약 |
|---|---|
| `threshold` | 0.33. 유한한 양수인 throttled/validated 메시지 비율 기준. |
| `globalDecay` | `ScoreParameterDecay(2m)`. `(0,1)`인 유한한 factor. |
| `sourceDecay` | `ScoreParameterDecay(1h)`. IP별 통계의 `(0,1)`인 유한한 factor. |
| `decayInterval` | `1s`. 최소 `1s`. |
| `decayToZero` | 0.01. `(0,1)`인 유한한 factor. |
| `retainStats` | `6h`. 0 이상이며 0은 보존하지 않음. |
| `quiet` | `1m`. Throttle 없이 지낸 뒤 gate를 끄기까지 최소 `1s`. |
| `duplicateWeight` | 0.125. 유한한 양수. |
| `ignoreWeight` | 1. 유한한 1 이상 값. |
| `rejectWeight` | 16. 유한한 1 이상 값. |
| `topicDeliveryWeights` | Topic에서 weight로의 map. 유한한 양수 값. |

## Filter, topic과 protocol 선택

| `gossipsub` 아래 경로 | 설정과 동작 |
|---|---|
| `topicOptions.<topic>` | `messageId`, `subscriptionBufferSize`, `validator`. Key는 참여 topic이어야 합니다. Topic별 ID·buffer 설정이 전역 설정보다 우선하며 buffer에는 subscribe 모드가 필요합니다. |
| `peerFilter` | Peer-ID 목록 `allow`, `deny`, `topics.<topic>.allow/deny`. 전역·topic allow 제한을 모두 적용하며 어느 쪽 deny도 우선합니다. 빈 allow 목록은 allow 제한을 두지 않습니다. |
| `directPeers` | GossipSub direct peer의 `/p2p/<Peer ID>`로 끝나는 transport multiaddress. Peer 컨테이너에서 접근할 수 있어야 하며 direct-peer 관계의 양쪽에 대칭적으로 설정해야 합니다. |
| `subscriptionFilter` | Topic `allowlist` 또는 정규식 `pattern`, 선택적인 수신 구독 RPC당 양수 `maxSubscriptions`. 설정된 자신의 topic도 filter를 통과해야 합니다. |
| `blacklist` | 최초 Peer-ID 목록 `peers`. `ttl` 생략은 Peer 인스턴스 내 영구 보존, 양수 TTL은 조회 시 만료 처리합니다. |
| `protocols` | 순서가 있는 `{id, features}` 목록. ID는 중복 없는 `/` 시작 문자열입니다. Feature는 `mesh`, `px`, `idontwant` 또는 단독 `none`이며 PX/IDONTWANT에는 mesh가 필요합니다. |
| `protocolMatch` | `exact`(기본) 또는 `prefix`. Prefix에는 명시적 protocol 목록이 필요하며 가장 긴 일치 prefix의 capability를 적용합니다. |

알려진 protocol ID에서 feature 목록을 생략하면 라이브러리의 feature 대응을 사용합니다. Feature를 선언하지 않은 사용자 지정 ID에는 mesh feature가 없습니다. 같은 실험의 Peer는 호환되는 wire protocol과 message ID 정책을 사용해야 합니다.

### 메시지 validator와 RPC inspector

`defaultValidators`는 공통 validator 목록이며 `topicOptions.<topic>.validator`는 해당 topic의 validator를 추가합니다. 제공한 validator는 모두 검증에 참여합니다. 각 validator는 다음 필드를 지원합니다.

| 필드 | 의미 |
|---|---|
| `result` | 검사 성공 시 `accept`(기본), `reject`, `ignore`. |
| `failureResult` | 검사 실패 시 `reject`(기본) 또는 `ignore`. Reject는 invalid-message 점수에 반영되며 ignore는 전달 성공을 의미하지 않습니다. |
| `minBytes`, `maxBytes` | PubSub 메시지 데이터의 0 이상 바이트 범위. 최소는 최대 이하이며 envelope 데이터에는 인코딩 overhead가 포함됩니다. |
| `pattern` | 메시지 데이터에 적용하는 Go 정규식. |
| `format` | `any`(기본), `json`, KPL `envelope`. Envelope는 필수 ID·run·publisher와 양수 timestamp를 검사합니다. |
| `delay` | 0 이상인 인위적 검증 지연. 취소된 작업은 ignore를 반환합니다. |
| `timeout` | 0 이상인 validator timeout. `0s`는 라이브러리의 timeout 없음 동작입니다. |
| `concurrency` | 양수인 validator별 동시 실행 한도. 라이브러리 기본 1024. |
| `inline` | 검증 경로에서 동기 실행. 기본 false. |
| `basicSeqno` | 기본 false. True이면 메모리 nonce 저장소로 라이브러리의 기본 sequence validator를 사용합니다. 기본·strict-sign 정책이 필요하며 이전·반복 sequence는 ignore, 잘못된 sequence는 `failureResult`로 처리합니다. |
| `useValidatorData` | 내용 검사 후 프로세스 내부 `ValidatorData.result`로 accept/reject/ignore를 선택할 수 있습니다. |

`rpcInspector`는 수신 RPC envelope를 검사합니다. 선택적인 0 이상 `maxMessages`, `maxSubscriptions`, `maxControlEntries`, `maxMessageIds`, `maxBytes`를 넘으면 거부하며 0도 실제 0 제한입니다. `rejectPeers`는 지정한 Peer ID 발신자를 거부합니다. Control entry는 IHAVE·IWANT·GRAFT·PRUNE·IDONTWANT protobuf entry 수이며 message ID 수는 IHAVE·IWANT·IDONTWANT의 참조 합계입니다. 바이트 제한에는 protobuf 직렬화 RPC 크기를 사용합니다.

### Discovery, author와 publication

`discovery` 블록을 생략하면 기존 KPL의 용량을 고려하는 Controller discovery loop를 유지합니다. 명시적 `discovery.mode: controller`(`{}`도 동일)는 Controller 후보를 libp2p discovery pipeline으로 연결하고, `routing`은 Kademlia를 사용하며 namespace 앞에 run ID를 붙입니다. `none`은 topic discovery를 끕니다. Routing discovery에는 Kademlia와 provider 기능·저장소가 필요합니다. 이 설정은 별도 최초 bootstrap 연결 절차를 제거하지 않습니다.

`discovery.ttl`은 양수 advertisement TTL이며 `limit`은 요청할 양수 결과 한도입니다. Controller discovery의 TTL은 실제 광고를 새로 만들지 않는 callback의 갱신 주기이며 Controller 생존 판단은 계속 Agent 보고를 따릅니다. 선택적인 `connector`는 지수 connection backoff를 설정합니다.

| Connector 필드 | Connector 블록이 있을 때 기본값 |
|---|---|
| `cacheSize` | 100. 양수. |
| `dialTimeout` | `2m`. 양수. |
| `minBackoff`, `maxBackoff` | `10s`, `1h`. 양수이며 최소는 최대 이하. |
| `timeUnit` | `1s`. 양수. |
| `base` | 5. 유한한 1 초과 값. |
| `offset` | `0s`. 0 이상. |
| `jitter` | `full` 또는 `none`. 기본 full. |
| `seed` | 1. Backoff jitter의 정수 seed. |

Connector 블록이 없으면 libp2p 기본 connector를 사용합니다. Routing discovery는 DHT admission·storage·provider·refresh 설정의 영향을 받습니다.

`author`는 Peer transport identity와 별도로 PubSub author를 선택합니다. 필드는 `peerId`, base64 libp2p-marshaled `privateKey`, 또는 `noAuthor`입니다. Peer ID를 함께 제공하면 private key와 일치해야 하며 ID만으로 서명 author를 지정하면 host peerstore에 해당 key가 있어야 합니다. `noAuthor: true`는 다른 두 필드와 충돌하며 unsigned 서명 정책과 모든 topic의 content 기반 message ID가 필요합니다. 명시적 private key는 저장된 시나리오와 다운로드 archive에 포함되므로 실험용 key를 사용하십시오.

`publish`는 애플리케이션의 각 발행을 설정합니다.

| 필드 | 동작 |
|---|---|
| `readinessMinPeers` | Payload·timestamp 생성 전에 검사하는 0 이상 최소 `pubsub.ListPeers(topic)` 수. 알려진 원격 topic Peer 수이며 upstream `MinTopicSize`의 mesh 조건이나 수렴 보장이 아닙니다. |
| `readinessTimeout` | 최대 `10s`인 선택적 양수 timeout. `readinessMinPeers`가 필요하며 전체 발행 요청 예산 안에서 적용합니다. |
| `local` | 기본 false. True는 local publication을 요청하고 원격 readiness 검사를 건너뛰며 해당 발행을 원격 세션 전달 지표에서 제외합니다. |
| `validatorData` | 프로세스 내부 map. 지원하는 `result`는 accept/reject/ignore이며 `useValidatorData`인 validator가 필요합니다. P2P wire에 직렬화하지 않습니다. |
| `author` | 같은 identity 필드를 사용하는 발행별 author override. Private key와 signing 정책이 필수이며 `noAuthor` 또는 `local: true`와 함께 사용할 수 없습니다. |

## Kademlia 설정

아래 필드는 모두 `kademlia` 아래에 있으며 Kademlia 활성 시 적용합니다. 라이브러리 datastore·provider interface는 아래 이름 있는 정책으로 제공합니다.

| 필드·group | 의미 |
|---|---|
| `enabled`, `mode` | 기본 활성. Mode는 기본 `server` 또는 `client`, `auto`, `auto-server`. |
| `protocolPrefix` | 기본 `/k-p2p-lab/v3`. DHT protocol prefix. |
| `protocolId` | 정확한 V1 protocol override. Prefix·extension과 상호 배타적입니다. |
| `protocolExtension` | Prefix 뒤에 붙이는 extension. |
| `bucketSize` | 양수 bucket 용량. KPL 기본 20. |
| `concurrency` | 양수 lookup 병렬 수. |
| `resiliency` | 0 이상 lookup resiliency. |
| `lookupCheckConcurrency` | 양수인 동시 routing-table admission 검사 수. |
| `routingTableLatencyTolerance` | 0 이상 Peer 지연 admission 기준. 고정된 upstream v0.31.0은 이 옵션을 받지만 routing table을 고정 `1m` 기준으로 생성하므로 설정한 override는 적용되지 않습니다. 설정 호환성을 위해 필드는 유지합니다. |
| `routingTableRefreshPeriod` | 양수 자동 routing-table refresh 주기. |
| `routingTableRefreshTimeout` | 양수 refresh query timeout. |
| `maxRecordAge` | 0 이상 value-record 최대 age. |
| `disableAutoRefresh`, `disableProviders`, `disableValues` | 각 DHT 기능의 명시적 비활성화 switch. |
| `optimisticProvide`, `optimisticProvideJobsPoolSize` | Optimistic provide 활성화와 양수 백그라운드 job pool 크기. |
| `bootstrapTimeout`, `bootstrapRetryInterval` | KPL 최초 transport bootstrap 예산·재시도 주기. 기본 `60s`·`1s`, 모두 양수. |
| `bootstrapSource`, `bootstrapPeers` | DHT bootstrap 원천인 `none`, `static`, `controller`. Static에는 transport `/p2p/<Peer ID>` multiaddress가 필요하며 controller는 run 범위 bootstrap Peer를 읽습니다. KPL 최초 transport bootstrap과 별도로 DHT 복구 bootstrap을 설정합니다. |
| `queryFilter`, `routingTableFilter`, `addressFilter` | 각각 `policy`, `allowCIDRs`, `denyCIDRs`를 포함합니다. 아래 설명 참고. |
| `routingTableDiversity` | `enabled`, 양수 `maxPerCpl`, 양수 `maxForTable`. 블록이 있으면 기본 활성이고 생략한 한도는 고정 라이브러리 기본값을 사용합니다. |
| `datastore` | `memory`(기본 동작) 또는 `null`. Memory는 Peer별 저장소로 컨테이너 종료 시 소멸하며 null은 기록을 버립니다. |
| `validator` | `mode: default`는 기본 `pk`·`ipns`를 유지하고 namespace를 추가할 수 있습니다. `mode: namespaced`는 전체 namespace map을 지정합니다. `namespaces.<이름>`은 `public-key`, `ipns`, `bytes`, `reject` 중 선택합니다. |
| `providerStore` | `mode: manager`(기본 동작) 또는 `null`. Manager는 양수 `cleanupInterval`, `cacheSize`를 받으며 null은 provider 기록을 버리고 해당 tuning을 받지 않습니다. |
| `requestHook` | `none`(기본) 또는 수신 DHT 요청 INFO log를 남기는 `log`. |

Filter policy는 `all`(빈 값·기본), `public`, `private`, `non-loopback`입니다. Query/routing-table의 public/private는 해당 라이브러리 filter를 사용하며, 특히 라이브러리의 private query는 비어 있지 않은 주소 목록을 모두 허용합니다. 정확한 실험 주소 범위에는 CIDR 제한을 사용하십시오. Address filter는 주소별로 적용합니다. 어느 주소든 deny CIDR에 포함되면 query/routing-table 후보 전체를 거부하며 allow 목록은 일치하는 IP가 최소 하나 필요합니다. `non-loopback`은 private unicast를 포함해 loopback이 아닌 global-unicast IP를 요구합니다.

DHT bootstrap source를 생략하면 라이브러리 option 기본값을 유지하며 KPL 최초 Controller bootstrap을 대체하지 않습니다. 명시적으로 빈 static 목록 또는 `none`을 설정하면 DHT 복구 bootstrap Peer를 제공하지 않습니다. `public-key` 정책은 `pk` namespace에서만, `ipns`는 `ipns` namespace에서만 사용합니다. `bytes` record validator는 불투명 값을 수락하고 바이트 사전식 순서에서 가장 큰 후보를 선택하며 `reject`는 모든 record를 거부합니다. 이 정책은 프로토콜 동작을 설정하며 DHT Put/Get/Provide 시나리오 action을 추가하지는 않습니다.

정확한 `protocolId: /ipfs/kad/1.0.0`을 포함한 표준 `/ipfs` namespace에는 upstream 제약인 bucket size 20, value/provider 활성, 표준 `pk`·`ipns` validator만 적용합니다. 사용자 지정 실험 prefix 또는 정확한 protocol ID에서는 다른 정책을 사용할 수 있습니다. 사용자 지정 exact ID에서는 upstream 기본 namespace 검사가 tuning을 거부하지 않도록 내부 검증 prefix도 설정하며, 지정한 정확한 wire ID는 유지합니다. Public address filter는 Swarm의 private overlay 주소를 제외할 수 있으므로 실험 네트워크에 맞는 filter를 선택하십시오.

## 구현 경계

Schema는 [공통 노드 설정](../internal/model/config.go), [GossipSub parameter](../internal/model/gossipsub_config.go), [PubSub 정책](../internal/model/gossipsub_policies.go), [scoring](../internal/model/score_config.go), [Kademlia 정책](../internal/model/kademlia_config.go)으로 나뉩니다. Runtime adapter는 대응하는 `internal/peer` 파일에 있습니다.


Scalar struct 필드는 typed schema로 제공합니다. Go 함수나 interface를 받는 라이브러리 옵션에는 이 문서의 이름 있는 정책을 사용합니다. 임의 score 함수·validator·store·discovery provider·feature test·tracer·request hook 및 Kademlia `WithCustomMessageSender` 구현에는 코드 확장이 필요하며 YAML은 Go 코드를 실행하거나 임의 구현을 로드하지 않습니다. KPL telemetry tracer와 Peer 네트워크 격리는 유지하여 실험 시스템 안에서 protocol 동작을 관측합니다.

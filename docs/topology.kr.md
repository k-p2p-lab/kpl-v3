# Transport, Kademlia, GossipSub 토폴로지 확인

[English](topology.md) | 한국어

## 화면 사용법

대시보드는 전체 폭의 대화형 토폴로지를 같은 크기의 Agent별 부채꼴 영역으로 표시합니다. Agent 번호는 **Agent status → No.**와 연결됩니다. 각 Peer에는 Agent 내부 표시 번호가 붙으며 선택하면 전체 Node ID와 Peer ID를 확인할 수 있습니다. 이 번호는 화면에서 Peer를 구분하기 위한 것으로 위치를 고정하지 않습니다. Peer 이탈 후 표시 번호는 재사용될 수 있으므로 영구 식별에는 전체 ID를 사용하십시오.

Peer는 소속 Agent의 부채꼴 안에서 서서히 자리를 잡습니다. 표시된 연결에는 스프링과 같은 인력이 작용하고, 반발력과 충돌 처리가 가까운 Peer들을 분산시킵니다. 이동 중에도 소속 영역 밖으로 나가지 않습니다. Peer나 표시 관계가 바뀌면 배치 조정을 다시 시작하고, 안정되면 멈추므로 무작위로 계속 움직이지 않습니다. 이 배치는 화면 표시를 위한 것으로 프로토콜 동작이나 실험 결과를 바꾸지 않습니다.

세 레이어는 모두 끈 상태로 시작합니다. 그래프 위 체크박스는 다음 순서이며 각각 조작할 수 있습니다.

| 레이어 | 기본값 | 선 | 의미 |
|---|---|---|---|
| **Transport** | 끔 | 가는 회색 선 | Peer가 보고한 활성 libp2p transport 연결 |
| **Kademlia** | 끔 | 가는 청록 점선 | 현재 DHT 라우팅 테이블에 상대 Peer가 포함된 관계 |
| **GossipSub mesh** | 끔 | 굵은 주황 실선 | 선택한 topic에서 수락된 GRAFT mesh 관계 |

체크를 해제하면 해당 선을 숨기고 남은 표시 관계를 기준으로 배치의 힘을 다시 계산합니다. 프로토콜이나 실험 실행을 끄지는 않습니다. **GossipSub topic** 필터는 mesh에만 적용하며 다른 레이어는 유지합니다. 필터를 바꾸면 Peer가 각자의 부채꼴 안에서 재배치될 수 있습니다. **All topics**에서는 같은 쌍의 여러 topic 관계를 한 선으로 합치되 세부 정보에 topic별 근거를 유지합니다. 상단 **Transport links**는 세 레이어의 합이 아닌 고유 transport 연결 쌍 수입니다.

Peer에 마우스를 올리면 현재 표시 중인 이웃 연결을 강조하고 관련 없는 선을 흐리게 표시합니다. 선택하면 Agent·Peer 표시 번호, 전체 ID, profile, 네트워크 설정, 보고한 endpoint 정보를 유지합니다. **Clear selection** 또는 Escape로 선택을 해제합니다. **+ / −**, Ctrl/Command + 스크롤로 확대·축소하고, 배경을 드래그해 이동하며, **Fit**으로 전체 배치를 맞춥니다. 키보드 초점 이동과 Enter/Space 선택도 지원합니다.

**Pause motion**을 누르면 기존 Peer의 위치를 유지하면서 필터, 선택, 확대·축소와 화면 이동을 계속 사용할 수 있습니다. 새 Peer와 Agent 영역 변경은 즉시 반영합니다. **Resume motion**으로 배치 조정을 재개합니다.

**Network overview** 헤더의 **Collapse / Expand** 버튼은 패널 전체를 숨기거나 보여 줍니다. 접힌 동안에는 배치 애니메이션을 멈추고 실시간 상태 갱신은 계속합니다. 다시 펼치면 최신 토폴로지를 표시하며 기존 동작 설정을 유지합니다. 선택 저장과 다른 패널 조작은 [패널 접기와 펼치기](monitoring.kr.md#패널-접기와-펼치기)를 참고하십시오.

운영체제나 브라우저의 동작 줄이기 설정을 사용하면 기본적으로 안정화 애니메이션을 끕니다. 최초 배치와 관계 변경에는 제한된 정착 계산을 적용하고 중간 이동 과정 없이 결과를 표시합니다. **Resume motion**을 직접 선택하면 애니메이션을 켤 수 있습니다.

Agent 영역의 각도는 같으며 설정한 capacity만큼 빈 Peer 위치를 예약하지 않습니다. Peer가 많은 실험은 여전히 조밀하게 보일 수 있으므로 topic 필터와 레이어 체크박스를 사용하고 관심 Agent·Peer를 확대해 확인하십시오.

## 선의 정확한 의미

transport 연결, DHT가 아는 Peer, 로컬에서 알려진 PubSub topic Peer(`TopicPeers` / PubSub `ListPeers`), GRAFT mesh는 서로 다른 관계입니다. Topic Peer 항목만으로 원격 application subscription을 입증할 수는 없으며 publish-only나 relay 참가자도 가능합니다. 현재 그래프는 ADD_PEER 이벤트로 관계를 재구성하지 않고 각 레이어에 해당하는 Peer 상태 필드에서 관계를 가져옵니다.

- **Kademlia**는 `dht.RoutingTable().ListPeers()`에서 얻습니다. 라우팅 테이블 포함만으로 현재 transport 연결이나 패킷 전송을 입증하지는 않습니다.
- **GossipSub**는 Peer 내부의 동기 router callback으로 추적합니다. 수락된 `Graft`는 추가하고 `Prune`, topic `Leave`, `RemovePeer`는 제거합니다. telemetry 큐 유실, Controller 이벤트 도착 순서, 최근 300개 표시 한도와 독립적입니다. `floodsub`, `randomsub`, PubSub 비활성에는 GossipSub mesh가 없습니다. GRAFT mesh 밖의 direct/fanout 전달 경로는 포함하지 않습니다.
- **Transport**는 현재 host 연결 목록입니다. ADD_PEER 이벤트나 기존 연결만으로 Kademlia·GossipSub 선을 만들지 않습니다.

선은 한쪽 또는 양쪽 endpoint의 보고를 무방향으로 표시한 것입니다. `reportedBy`가 실제 보고자를 나타내며, Kademlia 테이블과 갱신 중 mesh는 비대칭일 수 있습니다. 한쪽에서 PRUNE가 관측돼도 상대의 다음 보고가 오기까지 반대쪽 관계가 잠시 남을 수 있습니다. DHT·transport·mesh를 클러스터 전체의 단일 원자적 시점에 읽는 것은 아닙니다.

## Bootstrap과 topic discovery

Bootstrap transport 연결과 GossipSub topic discovery는 서로 다른 절차입니다. 각 Peer는 먼저 `/api/v1/bootstrap`으로 동일 run의 ready `boot` Peer를 조회합니다. Worker는 DHT를 비활성화했더라도 seed 기반으로 섞은 순서에서 첫 연결 성공까지 시도합니다. Boot 노드는 제공된 Peer에 연결을 시도하며 비어 있는 네트워크로 시작할 수도 있습니다. Kademlia bootstrap은 DHT가 활성화된 경우에만 수행합니다. 내장 boot type은 PubSub를 실행하지 않으므로 이 연결만으로 GossipSub mesh가 생기지는 않습니다.

PubSub가 시작되면 Peer는 설정한 정확한 topic마다 Controller의 `/api/v1/discovery` registry를 즉시 조회하고 이후 3초 polling loop로 다시 조회합니다. Registry는 동일 run·topic에서 ready 상태이고 PubSub가 활성화되어 있으며 Agent가 online인 모든 Peer를 반환합니다. Peer가 rendezvous hash로 순서를 정하고 publish-only·relay 후보보다 실제 subscriber를 우선합니다. 현재 PubSub `ListPeers(topic)` 수를 `DHigh`에서 빼고, 전체 hard connection cap의 여유 범위 안에서 부족한 연결을 채웁니다. Dial이 실패하면 다음 순위 후보를 시도하고 실패한 Peer ID에는 15초 retry backoff를 적용합니다. Registry 요청과 각 dial round의 제한 시간은 5초이므로 느린 round는 다음 polling을 늦출 수 있습니다. 이 방식은 churn 중 후보 연결을 보강하며 의도적으로 all-to-all graph를 만들지는 않습니다. 수신 연결이나 다른 프로토콜 연결로 topic별 discovery 목표보다 많은 연결이 생길 수는 있습니다.

Registry는 주소와 설정된 topic 참여 정보만 제공하며 전달 결과를 조회하거나 선택하지 않습니다. 발견한 연결은 libp2p 연결이 완료된 뒤 회색 transport 레이어에 나타납니다. GossipSub가 어떤 topic Peer를 mesh에 넣을지 계속 결정하며, 수락된 GRAFT만 주황 mesh 선으로 표시합니다. DHT routing-table entry는 별도의 청록 선입니다. 따라서 `DHigh`는 discovery 목표이며 화면에 정확히 그 수의 transport 또는 mesh 이웃이 표시된다는 보장은 아닙니다.

## 갱신과 노드 수명

Peer는 약 2초마다 상태를 보고하고 Agent가 현재 snapshot을 전달합니다. 네트워크·스케줄링·보고 실패로 지연이 추가됩니다. stopping·stopped·starting·failed endpoint에는 관계 선을 표시하지 않습니다. stopping·stopped·failed 원과 연결선은 숨깁니다. starting Peer는 황색 **Starting**으로 표시합니다. 실패 피어의 진단 정보는 노드 조회와 저장 실험 기록에 유지합니다.

컨테이너 정리가 정상 완료되면 Agent는 Peer 식별자, 종료 상태, 수명 시각, 토픽 레이블을 축약 기록으로 보존하고 과거 연결·routing/mesh·스코어와 큰 설정 메타데이터는 메모리에서 해제합니다. Agent 전체 상태와 노드 조회에는 이 축약 종료 기록이 계속 포함되며, 실행 중인 Peer와 실패한 프로세스는 상세 정보를 유지합니다.

정기 heartbeat는 실행 중이거나 실패한 Peer와 Controller가 아직 수신 확인하지 않은 정상 종료 기록을 전송하며, 큰 보고는 제한된 크기의 JSON batch로 나눕니다. 정상 종료 기록은 수신 확인 뒤 정기 보고에서 생략하지만 전체 인벤토리에는 남습니다. 재등록은 이 수신 확인을 초기화하므로 Controller 재시작 후 보존 이력을 다시 전송합니다. Controller는 부분 heartbeat에서 빠진 노드를 종료로 해석하지 않습니다. [Heartbeat 계약](api.kr.md#내부-cluster-endpoint)을 참고하십시오.

Dashboard가 연결되지 않아도 Controller snapshot에 종료 상태가 반영됩니다. 늦은 heartbeat나 생성 응답은 stopped 또는 failed Peer를 다시 활성 상태로 되돌리지 못합니다. failed Peer의 정리 진행과 stopped 전환은 계속 반영하며, Dashboard를 다시 열면 현재 snapshot으로 그래프를 구성합니다.

오프라인 Agent와 10초 이상 오래된 Peer 보고는 선에서 제외합니다. 서로 다른 서버 시계를 직접 빼지 않도록, Controller는 동일 Agent가 기록한 두 시각으로 Peer 보고 나이를 계산한 뒤 수신 후 경과 시간을 자기 시계로 더합니다. 이는 Agent가 보고한 나이와 수신 후 시간이며 네트워크 전송 중 시간의 상한을 뜻하지 않습니다. 과거 형식에 시각 정보가 없으면 이 나이를 확정할 수 없습니다. `OverlayObservedAt`은 overlay 보고 지원 여부를 식별하는 Peer 시각으로, 브라우저·Controller 현재 시각과 직접 비교해 stale을 판정하지 않습니다.

새 빈 snapshot은 이전 routing·mesh 관계를 지웁니다. Agent는 순서가 뒤집힌 오래된 보고를 거부하고 slice/map을 복사해 전달합니다. 따라서 이벤트 일부가 유실돼도 새 상태 보고로 그래프를 복구합니다. 이 규칙이 모든 표시 관계의 현재 통신 가능성이나 전송 중 여부를 보증하는 것은 아닙니다.

overlay 보고가 없는 이전 Peer는 Transport 선만 제공합니다. 화면에서 overlay 정보를 보고한 Peer 수를 안내하며, 과거 transport 선을 Kademlia로 바꾸어 표시하지 않습니다. 갱신한 이미지를 Controller·Agent에 배포하고 새 Peer를 실행해야 모든 레이어를 받을 수 있습니다.

## API와 보존 데이터

`GET /api/v1/snapshot`, `GET /api/v1/network`, SSE snapshot에 동일한 typed edge가 포함됩니다. Node에는 `routingPeers`(Peer ID 목록), `meshPeers`(topic → Peer ID 목록), `overlayObservedAt`이 추가됩니다. `/api/v1/discovery`는 현재 transport 후보 registry이며 토폴로지 이력 endpoint가 아닙니다. 예시는 다음과 같습니다.

```json
{"source":"node-a","target":"node-b","protocol":"gossipsub","topic":"kpl/demo","reportedBy":["node-a"]}
```

`protocol`은 `transport`, `kademlia`, `gossipsub` 중 하나입니다. API는 GossipSub edge를 topic별로 구분합니다. 중복 보고는 일관된 순서로 합치고 알 수 없는 endpoint, 같은 run 안의 모호한 중복 Peer ID, 다른 run 관계는 제외합니다. 그래프/API edge endpoint는 Node ID이고 Node 내부 이웃 목록은 libp2p Peer ID입니다.

Controller는 실행 중 약 5초마다 `observations.jsonl`에 그룹 상태·차수·clustering·점수 요약과 프로토콜별 그래프 표본을 저장합니다. `graphs`의 각 항목은 `protocol`, 정렬된 Node ID 배열 `nodes`, 같은 인덱스의 `groups`, 노드 인덱스 쌍 `edges`입니다. 고유 무방향 간선을 저장하며 GossipSub는 여러 토픽의 같은 Peer 쌍을 합칩니다. 실시간 간선의 `topic`과 `reportedBy`는 이 저장 형식에 포함되지 않습니다. 표본 사이 변화 전부나 평가자별 개별 점수는 보존하지 않습니다.

`events.jsonl`에는 수집한 GRAFT/PRUNE 등의 전이와 [상세 RPC 메타정보](api.kr.md#상세-peer-로그)가 남지만 telemetry 유실의 영향을 받습니다. 간선이 없는 과거 요약은 새 그래프 지표를 복원할 수 없습니다. 원본은 [결과 ZIP](monitoring.kr.md#실험-결과-다운로드), 표본 축소·그래프 계산은 [연구 지표](experiment-metrics.kr.md#연구-그래프-관측)를 참고하십시오. 레이어 표시와 배치는 메인 도달 지표의 정의를 바꾸지 않습니다.

## 개발 검증

이미 사용 가능한 Linux Docker engine에서 `make test-linux` 또는 `docker build --target test -t kpl-v3:test .`로 Peer mesh·DHT snapshot, Agent 순서·복사, Controller edge·신선도, 내장 HTML을 검사합니다. 네이티브 `go test ./...`는 Linux에서만 실행하며 Windows에서는 Go 테스트나 테스트 binary를 실행하지 마십시오. 최신 Node.js 환경에서 `node --test internal/webui/topology_test.cjs internal/webui/topology-layout_test.cjs`로 레이어·topic 필터, 부채꼴 배치와 안정화, churn, 카메라와 DOM 조작 회귀를 검사합니다. JavaScript 테스트는 Node.js 내장 모듈만 사용하며 npm 패키지 설치가 필요 없습니다. 이 검사는 브라우저 화면 검토나 실제 다중 서버 실험을 대신하지 않습니다. 전체 검증 범위는 [개발 가이드](development.kr.md)를 참고하십시오.

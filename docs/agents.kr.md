# Agent 용량과 Peer 배치

[English](agents.md) | 한국어

[문서 안내](README.kr.md) · [저장소](../README.kr.md)

Agent를 실행할 호스트는 [Swarm 배포](swarm.kr.md)에서 선택합니다. 이 문서는 Peer 생성 허용량과 배치를 설명하며 요청·응답·적용 확인 필드는 [설정 API](api.kr.md#agent-용량-설정)가 정의합니다.

## 기본값과 Agent별 예외 설정

capacity는 Peer 개수의 admission 제한이며 CPU·메모리 예약이나 cgroup 한도가 아닙니다. Agent의 Swarm resource 제한을 바꾸어도 형제 컨테이너인 Peer에 전파되지 않습니다. 제공된 stack은 `KPL_AGENT_CAPACITY`를 모든 Agent의 시작 기본값으로 사용합니다. **Dashboard → Agent status → Configure**에서 **Custom for this Agent**를 선택하면 해당 Agent ID에만 예외값을 지정합니다. **CLI default**를 선택하면 예외값을 삭제합니다. 예를 들어 `KPL_AGENT_CAPACITY=200`을 유지한 채 특정 Agent만 `100`으로 설정하면 다른 Agent는 계속 `200`을 사용합니다. CPU/RAM/FD/conntrack 및 Docker daemon 부하를 측정해 capacity를 정하십시오.

Agent별 예외값은 `<controller-data-dir>/agent-capacities.json`에 저장하며 Controller 볼륨과 Agent ID가 같으면 Controller·Agent 재시작 뒤에도 유지합니다. Controller 백업에 이 파일을 포함하십시오. Agent ID가 바뀌면 별도 설정 대상입니다. 오프라인 Agent의 설정은 재접속할 때 적용합니다. Controller와 Agent 모두 이 기능을 지원하는 버전이어야 하며, 구버전 Agent는 UI의 Configure 버튼을 비활성화합니다.

서비스 재시작 없이 등록·heartbeat 응답으로 설정을 전달합니다. 표에는 CLI 기본값 또는 예외값과 적용 확인 전 **Applying…**을 표시합니다. 용량을 낮추면 새 배치 제한을 즉시 적용하고 기존 Peer는 유지합니다. 현재 점유량이 한도보다 크면 여유가 생길 때까지 새 join이 대기합니다. 증가는 Agent가 현재 설정을 적용했다고 확인한 뒤 사용할 수 있습니다. Agent 자체 생성 제한과 capacity 메트릭에도 변경값을 반영합니다.

## 배치 정책

| 설정 | 배치 동작 |
|---|---|
| `placement: balanced` (기본) | Peer 하나를 추가한 예상 점유량/유효 capacity가 가장 낮은 online Agent부터 배치. 동률은 현재 점유율, Agent ID 순서 |
| `placement: random` | 여유가 있는 online Agent 중 시드 기반 선택 |
| `placement: single-agent` | 한 join 실행을 선택한 Agent 하나에 고정. repeat는 회차마다 재선택. 가득 차면 다른 서버로 넘기지 않고 대기 |
| `agentId` | 지정한 Agent로 고정. 물리 서버 고정 실험은 `/api/v1/agents`의 ID 사용 |
| `parallelism` | `parallel: true`인 join에서 동시에 실행할 작업 수. 서버 수나 Swarm replica 수와 다름 |

Balanced 배치는 웹 예외값을 포함한 각 Agent의 유효 capacity에 비례합니다. 다른 부하가 없는 capacity 100과 50의 Agent에 Peer 75개를 예약하면 50개와 25개로 배분합니다. 점유량에는 모든 실험의 Peer, 생성 예약, 제거 중인 컨테이너를 포함합니다. 기존 Peer는 이동하지 않고 새 join과 churn으로 비워지는 슬롯을 통해 점유 비율을 맞춥니다. 명시적 `agentId`, `random`, `single-agent`는 지정한 배치 정책을 유지합니다.

| Agent | 유효 용량 | 75개 예약 후 Peer 수 | 점유율 |
| --- | ---: | ---: | ---: |
| A | 100 | 50 | 50% |
| B | 50 | 25 | 50% |

다른 점유가 없는 Agent에서 시작한 예제입니다. Peer 점유율이 같아도 CPU·메모리 사용률까지 같다는 뜻은 아닙니다.

## 생성 허용·정리·부하

동시 실험의 예약은 Controller에서 직렬화합니다. Agent는 생성 요청 수락부터 **삭제 완료 확인까지** 슬롯을 유지하므로 Docker 생성·시작 대기 시간도 포함하며, 삭제 실패도 점유량에 포함합니다. Controller는 DELETE 응답만으로 슬롯을 재사용하지 않습니다. 네트워크 단절로 보고가 10초 넘게 없으면 신규 배치에서 제외하며, 이전 Agent 프로세스나 순서가 뒤집힌 보고가 현재 노드·용량을 덮어쓰지 않도록 검사합니다. 같은 ID의 새 Agent는 이전 프로세스의 online 유효 시간이 끝난 뒤 등록됩니다.

Docker 생성 admission에는 45초의 예산을 적용합니다. 이후 설정 복사·시작·주소 확인은 새로 시작하는 45초 예산을 공유합니다. 원래 요청의 deadline이 더 짧으면 두 단계 모두 이를 따릅니다. admission 도중 취소는 늦게 생성된 컨테이너를 정리할 수 있도록 제한 시간 안에서 생성 결과를 확보할 때까지 보류하며, 시작 단계는 즉시 취소할 수 있습니다. [`internal/agent/docker.go`](../internal/agent/docker.go)를 참고하십시오. 혼잡한 서버에서 이 제한을 반복해서 초과하면 join `parallelism`과 capacity를 낮추고 디스크·데몬 부하를 점검하십시오.

전체 용량을 늘리기 전에 [overlay와 배포 제약](swarm.kr.md#분배와-용량)을 확인하십시오. 준비 상태·정리 큐·프로세스 부하는 [모니터링](monitoring.kr.md), 재현 가능한 개발 벤치마크는 [성능 조사](churn-performance.kr.md)를 참고하십시오.

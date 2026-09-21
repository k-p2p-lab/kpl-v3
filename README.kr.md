# K-P2PLab v3

[English](README.md) | 한국어

[K-P2PLab Hub](https://github.com/k-p2p-lab/hub/blob/master/README.kr.md)는 프로젝트 공통 개념과 연구 배경을 관리하며, 이 저장소는 실행 가능한 v3 구현, 배포 절차, 설정과 버전별 동작을 관리합니다.

K-P2PLab v3는 하나 이상의 Linux 호스트에 구성된 Docker Swarm에서 설정 가능한 libp2p Kademlia 및 PubSub 실험을 실행합니다. Controller는 시나리오 스케줄링과 웹 Dashboard 제공을 맡고, Agent는 로컬 Docker daemon으로 Peer 컨테이너를 생성하고 관리합니다. Docker Swarm은 선택한 호스트마다 Agent 하나를 실행하며 각 Peer에 독립된 컨테이너와 네트워크 네임스페이스를 제공합니다. Prometheus/Grafana는 수집된 telemetry를 보여 주고, Controller는 다운로드 가능한 실행 결과를 보존합니다.

코드, UI와 문서의 기본 언어는 영어이며 한국어 문서는 대응하는 `.kr.md` 파일로 유지합니다.

## 핵심 기능

- join, leave, 준비 장벽, publish, phase 반복, 백그라운드 잡과 seeded distribution을 지원하는 버전 1·2 YAML 시나리오
- Kademlia 설정과 GossipSub, FloodSub, RandomSub router 선택
- Peer별 delay, jitter, loss, duplication, corruption, reordering과 bandwidth 설정을 적용하는 격리 컨테이너
- 하나 이상의 노드에서 용량 기반 Peer 배치를 지원하는 Docker Swarm 배포
- Agent 영역과 topic 필터를 제공하는 실시간 Kademlia, GossipSub GRAFT 및 transport 토폴로지
- churn을 고려한 도달률, 지연, 중복, coverage와 관측 품질 지표
- 재사용 가능한 시나리오 라이브러리, 반복 실행, 결과 보존, ZIP 내보내기와 삭제
- 개별 run·반복 run 동일 가중 평균의 백그라운드 분석, v2 연구 비교·대역폭 시각화와 PNG/CSV/ZIP 다운로드
- 프로토콜별 libp2p 스트림 전송률·누적 바이트 실측과 수집 품질 표시

## 사전 요구사항

- 활성 Swarm에 참여한 rootful Docker Engine이 설치된 Linux. 기본 배포 구성은 userns-remap을 지원하지 않습니다.
- 네트워크 조건을 위한 `NET_ADMIN`과 커널 `sch_prio`, `sch_netem`, `cls_u32` 및 선택적인 `sch_tbf` 모듈
- Docker 밖에서 개발할 때만 Go 1.25 이상
- 활성 Swarm manager와 선택한 모든 노드가 접근하고 신뢰하는 이미지 registry

호스트 준비, 권한, 원격 접속, 저장소와 안전한 종료 방법은 [Swarm 배포 가이드](docs/swarm.kr.md)를 참고하십시오.

동일한 시나리오 seed는 distribution 입력을 반복하지만 실행 타이밍, 생성 payload, Peer identity와 프로토콜 결과까지 결정적으로 만들지는 않습니다.

## Swarm 빠른 시작

활성 manager의 저장소 디렉터리에서 아래 명령을 실행합니다. 이미지 주소는 모든 노드가 접근할 수 있는 registry로 바꾸십시오.

```sh
sh scripts/swarm.sh init KPL_IMAGE=registry.example.com/kpl-v3:v3 KPL_AGENT_CAPACITY=20 KPL_MIN_AGENTS=2
sh scripts/swarm.sh publish
sh scripts/swarm.sh deploy --workers
sh scripts/swarm.sh status
sh scripts/swarm.sh access
sh scripts/swarm.sh credentials
sh scripts/swarm.sh scenario
```

manager에도 Agent를 실행해야 한다면 `--workers` 대신 `--all`을 사용합니다. 단일 노드 Swarm에서는 `init` 시 `KPL_MIN_AGENTS=1`을 설정하고 `--all`로 배포하십시오. `access`가 출력한 Controller 주소를 열고 `credentials`의 `KPL_USER`·`KPL_PASSWORD`로 로그인한 뒤 `scenario` 출력을 붙여 넣으십시오. `access`는 선택된 각 Agent 노드의 metrics URL도 표시합니다. TCP `KPL_AGENT_METRICS_PORT`(기본 `9091`)는 control 노드에서 허용하고, 운영자가 해당 링크를 직접 열 때에는 운영자 브라우저가 속한 신뢰 관리망에서도 허용하십시오. helper는 배포 시점의 이미지 digest를 확인해 고정하므로 tag를 갱신할 때 SHA를 직접 수정할 필요가 없습니다. 운영 클러스터를 관리하거나 철거하기 전에 [전체 Swarm 절차](docs/swarm.kr.md)를 확인하십시오.

모니터링 예제는 [`examples/monitoring.yaml`](examples/monitoring.yaml)을 실행하십시오. [모니터링과 결과](docs/monitoring.kr.md)에서 Grafana의 run 선택과 이벤트 로그·파생 지표 다운로드 방법을 확인할 수 있습니다. Controller 재시작 후에도 결과 ZIP을 받을 수 있지만 이 파일에서 실행이나 실시간 counter를 복원하지는 않습니다. 비정상 task가 있어도 `sh scripts/swarm.sh remove`로 서비스를 직접 삭제할 수 있습니다. standalone Peer 정리 완료를 확인하지 않으므로 계획된 종료에서는 먼저 실험을 완료·취소하고 결과를 다운로드하십시오.

## 문서

목적, 연구, 설계 원칙과 개념 아키텍처는 [Hub](https://github.com/k-p2p-lab/hub/blob/master/README.kr.md)에서 시작하십시오. 아래 가이드는 현재 v3 구현과 한계를 설명합니다.

| 문서 | 내용 |
|---|---|
| [구현 아키텍처](docs/architecture.kr.md) | v3 컴포넌트, 제어 및 실험 경로, Docker 네트워크, 배치와 격리 경계 |
| [Swarm 배포](docs/swarm.kr.md) | Linux 요구사항, registry 설정, 노드 선택, 배포, 저장소, 업데이트와 철거 |
| [시나리오 설정](docs/scenario-reference.kr.md) | YAML action, profile, 프로토콜 설정, distribution과 네트워크 조건 |
| [시나리오 라이브러리](docs/scenario-library.kr.md) | 재사용할 시나리오의 저장, 이름 지정, 불러오기, 갱신과 삭제 |
| [REST API](docs/api.kr.md) | Controller endpoint, 인증, 결과와 내부 정리 API |
| [개발](docs/development.kr.md) | Go 빌드, 시나리오 검증, 테스트와 Swarm 개발 배포 |
| [실험 지표](docs/experiment-metrics.kr.md) | churn 상황의 도달률 분모, 지연, 중복과 한계 |
| [모니터링과 결과](docs/monitoring.kr.md) | Prometheus, Grafana, ZIP 내용, 보존 데이터와 삭제 |
| [저장 결과 시각화](docs/visualization.kr.md) | 결과별 이미지, 반복 실험 비교와 PNG/CSV/ZIP 다운로드 |
| [토폴로지](docs/topology.kr.md) | Agent 영역, 그래프 레이어, topic 필터와 조작 방법 |
| [Swarm churn 및 publish](docs/swarm-churn-publish.kr.md) | 다중 서버 연속 churn 실험 절차 |
| [Prysm 블록 스코어와 churn](docs/swarm-churn-prysm-block.kr.md) | 세 지연 그룹의 동시 churn과 스코어 관측 |
| [v2 재현](docs/v2-reproduction.kr.md) | K-P2PLab v2 호환 매핑과 의도적인 차이 |
| [프로토콜 설정](docs/protocol-options.kr.md) | PubSub·스코어·Kademlia·이름 있는 정책의 전체 설정 |
| [Bandwidth 측정](docs/bandwidth.kr.md) | 스트림 사용량, 전송률 계산, 프로토콜 귀속과 수집 한계 |
| [v2 전체 분석 대조](docs/v2-analysis-coverage.kr.md) | v2 분석·시각화 대응, 정의 차이와 필요한 관측 근거 |

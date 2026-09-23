# K-P2PLab v3

[English](README.md) | 한국어

K-P2PLab v3는 Linux 호스트의 Docker Swarm에서 libp2p Kademlia·PubSub 실험을 실행합니다. Controller가 실험을 스케줄링하고 Dashboard를 제공하며, Agent가 호스트 Docker daemon을 통해 격리된 Peer 컨테이너를 관리합니다.

이 저장소에는 구현 코드·배포 스크립트·실행 YAML 예제·테스트를 둡니다. 상세 문서는 **[K-P2PLab 위키](https://github.com/k-p2p-lab/v3/wiki/Home-KO)**에서 관리합니다.

| 작업 | 위키 안내 |
| --- | --- |
| 처음 시작 | [빠른 시작](https://github.com/k-p2p-lab/v3/wiki/Getting-Started-KO) · [전체 문서](https://github.com/k-p2p-lab/v3/wiki/Documentation-Index-KO) |
| 배포와 설정 | [Swarm 배포](https://github.com/k-p2p-lab/v3/wiki/Swarm-Deployment-KO) · [Agent 용량](https://github.com/k-p2p-lab/v3/wiki/Agent-Capacity-KO) |
| 실행과 복구 | [시나리오](https://github.com/k-p2p-lab/v3/wiki/Scenario-Library-KO) · [실행·복구](https://github.com/k-p2p-lab/v3/wiki/Experiments-KO) |
| 관측과 분석 | [대시보드](https://github.com/k-p2p-lab/v3/wiki/Dashboard-KO) · [모니터링](https://github.com/k-p2p-lab/v3/wiki/Monitoring-KO) · [결과](https://github.com/k-p2p-lab/v3/wiki/Results-KO) · [그림](https://github.com/k-p2p-lab/v3/wiki/Visualization-KO) |
| 연동과 개발 | [API](https://github.com/k-p2p-lab/v3/wiki/API-KO) · [아키텍처](https://github.com/k-p2p-lab/v3/wiki/Architecture-KO) · [개발](https://github.com/k-p2p-lab/v3/wiki/Development-KO) |

배포에는 Linux와 활성 Swarm의 rootful Docker가 필요합니다. 위키의 사전 요구사항을 확인하고 문서의 명령은 이 저장소 루트에서 실행하십시오. 코드·UI·기본 문서는 영어이며 한국어 위키 페이지는 `-KO` 접미사를 사용합니다.

[`examples/`](examples) · [`scripts/`](scripts) · [`stack.swarm.yaml`](stack.swarm.yaml) · [Project Hub](https://github.com/k-p2p-lab/hub)

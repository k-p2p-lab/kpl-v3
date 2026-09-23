# K-P2PLab v3 문서 안내

[English](README.md) | 한국어

[저장소와 빠른 시작](../README.kr.md) · [Hub 공통 개념](https://github.com/k-p2p-lab/hub/blob/master/docs/CONCEPTS.kr.md) · [문서 관리 기준](https://github.com/k-p2p-lab/hub/blob/master/docs/DOCUMENTATION.kr.md)

현재 구현체의 작업별 문서 지도입니다. 프로젝트 공통 개념·연구 설계·출판물은 Hub가, 명령·화면 사용 절차·스키마·수식·운영 제약은 이 저장소가 관리합니다.

## 읽는 순서

- **처음 배포:** [Swarm](swarm.kr.md) → [Agent 용량](agents.kr.md) → [실험 실행](experiments.kr.md) → [결과 보존](results.kr.md).
- **실험 설계:** [시나리오 라이브러리](scenario-library.kr.md) → [시나리오 참조](scenario-reference.kr.md) → [프로토콜 옵션](protocol-options.kr.md) → [실험 예제](#실험-예제).
- **실패·중단 복구:** [복구 방식](experiments.kr.md#저장된-실험-복구) → [Agent 정리와 용량](agents.kr.md) → [로그와 연결 진단](monitoring.kr.md).
- **해석과 보고:** [지표 정의](experiment-metrics.kr.md) → [대역폭 범위](bandwidth.kr.md) → [결과 이미지](visualization.kr.md) → [Hub 보고 원칙](https://github.com/k-p2p-lab/hub/blob/master/docs/RESEARCH.kr.md).
- **구현 변경:** [아키텍처](architecture.kr.md) → [개발](development.kr.md) → 아래의 해당 참조 문서.

## 배포와 운영

| 문서 | 담당 내용 |
| --- | --- |
| [Swarm 배포](swarm.kr.md) | 호스트 준비, CLI·자동완성, 네트워크, 갱신·종료, 저장소와 배포 검증 |
| [Agent 용량과 배치](agents.kr.md) | 시작 기본값, Agent별 예외, 비례 배치, 생성 예약과 정리 |
| [모니터링과 진단](monitoring.kr.md) | Prometheus/Grafana, 지표 노출, 접근·인증 로그와 SSE 진단 |

## 실험 사용 절차

| 문서 | 담당 내용 |
| --- | --- |
| [시나리오 라이브러리](scenario-library.kr.md) | 시나리오 입력 저장·검증·편집·재사용 |
| [실행과 복구](experiments.kr.md) | 실행·반복·정지·이어하기·재시도, 진행 정렬과 예상 종료 |
| [대시보드](dashboard.kr.md) | 실시간 패널, 지표 카드, 키보드·터치 조작과 연결 상태 |
| [토폴로지 뷰어](topology.kr.md) | 노드·간선 의미, 계층·필터, 최신성·생명주기와 그래프 관측 |
| [저장 결과](results.kr.md) | 원본·분석 파일 조회, 내보내기, 삭제, 보존과 백업 |
| [결과 이미지와 비교](visualization.kr.md) | 분석 작업, 반복 평균, 그림·비교와 PNG/CSV/JSON 내보내기 |

## 참조

| 문서 | 담당 내용 |
| --- | --- |
| [시나리오 스키마](scenario-reference.kr.md) | YAML 필드, phase·job, 분포, 네트워크 조건, 기본값과 한도 |
| [프로토콜 옵션](protocol-options.kr.md) | PubSub, 점수, 검증기, discovery와 Kademlia 설정 |
| [HTTP API와 이벤트 계약](api.kr.md) | 엔드포인트, 인증, SSE 형식, 로그 스키마, 다운로드 헤더와 분석 작업 |
| [실험 지표 정의](experiment-metrics.kr.md) | 모집단, 도달률 경계, 지연, 중복, 그래프 지표와 추정 |
| [대역폭 측정](bandwidth.kr.md) | 측정 계층, 전송률·바이트, 프로토콜 구분과 수집 품질 |

## 실험 예제

| 문서 | 담당 내용 |
| --- | --- |
| [연속 churn과 발행](swarm-churn-publish.kr.md) | 다중 호스트 실행, 누적 진입 일정, 기대 규모와 관측 |
| [세 지연 그룹의 Prysm 점수](swarm-churn-prysm-block.kr.md) | 합성 워크로드, 기준 파라미터, 그룹 일정과 해석 한계 |

## 개발과 호환성

| 문서 | 담당 내용 |
| --- | --- |
| [구현 아키텍처](architecture.kr.md) | 구성 요소, 통신, 네트워크, 격리와 소스 연결 |
| [개발 안내](development.kr.md) | 빌드·테스트·검증, 개발 이미지 배포와 문서 유지보수 |
| [성능 조사](churn-performance.kr.md) | 과거 churn·SSE 근거, 벤치마크 입력과 검증 범위 |
| [v2 재현 매핑](v2-reproduction.kr.md) | 실행·네트워크 대응, 의도적인 차이와 과거 검증 |
| [v2 분석 지원 범위](v2-analysis-coverage.kr.md) | 소스 목록, 분석·시각화 대응, 정의와 근거의 한계 |

[버전 비교 manifest](v2-analysis-coverage.json)는 v2 감사를 뒷받침합니다. 실행 YAML은 Hub가 아닌 [`examples/`](../examples)에 둡니다. 과거 결과와 벤치마크는 기록된 환경의 근거이며 현재 배포 검증은 아닙니다.

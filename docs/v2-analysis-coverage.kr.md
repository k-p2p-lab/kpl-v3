# v2 분석·시각화 대응

[English](v2-analysis-coverage.md) | 한국어

[문서 안내](README.kr.md) · [저장소](../README.kr.md)

제공된 v2 revision `74b71090410cac1315ae08f479a95c086feeaaf6`의 분석 출력 40개 필드/계열과 Go·Python·실행 스크립트 34개를 기준으로 합니다. 각 소스의 hash는 [목록](v2-analysis-coverage.json)에 있습니다. `python3 scripts/audit-v2-analysis.py --v2 ../v2`는 목록의 누락·변경을 확인하며 수치 동등성을 증명하는 검사는 아닙니다.

아래 시각화 계열을 v3의 **Saved results → Images** 및 비교 도구에서 제공합니다. 메인 Metrics는 기존 세션·수신 기간 정의를 유지하고, 추가 분석은 `research.definition = v2-corrected-observations-v1`로 구분합니다. **원본 자료와 통계 정의가 다른 경우 v2와 같은 수치를 보장하지 않습니다.** 사용할 수 없는 관측값은 N/A, 추정하지 못한 전파 출처는 unknown으로 남깁니다.

## 분석 출력

| v2 출력 계열 | v3 표현과 조건 |
|---|---|
| `node_count`, `average_degree`, `average_degree_excluding_leaves` | 프로토콜·그룹별 시계열과 요약. 비잎 분모 지표는 전체 차수 합 / degree > 1인 노드 수 |
| `diameter`, `shortest_path_length` | 저장 간선에서 계산. 도달 가능한 서로 다른 노드 쌍만 사용하고 connected-pair 비율을 함께 표시 |
| `clustering_coefficient`, `assortativity`, `modularity` | 평균 지역 clustering, 차수 상관, 결정적 seed의 Louvain modularity. 정의되지 않는 통계는 N/A |
| `betweenness_centrality`, `page_rank`, `degree_centrality`, `closeness_centrality`, `eigenvector_centrality` | 정규화 중심성의 전체·그룹 평균. 그래프 원자료가 있는 저장 표본에서 계산 |
| `degree_distribution-*` | 관측 snapshot별 확률을 평균한 차수 PDF/CDF와 가중 Student-t 적합 |
| `frt`, `reachability`, `drc` | 메시지별 최초 수신 지연·도달률·중복 수와 메시지 평균 요약. DRC/graph-node 및 DRC/target도 분리 |
| `eager_count`, `lazy_count`, `eager_frt`, `eager_reachability` | GRAFT·IHAVE·IWANT·메시지 ID에 근거한 추정치. 미분류 수를 별도 표시하고 eager FRT는 분류된 수신 중 시각 근거가 있는 표본만 사용 |
| `pub_map`, `propa_map`, `reach_map`, `eager_propa_map`, `eager_reach_map` | `research.messages`의 발행·수신·부모·hop·모집단·추정 근거와 전파/추정 eager 누적곡선. JSON·CSV 내보내기 |
| `dup_map`, `time` | 메시지별 중복 누적곡선 및 시각 축. 원시 중복 이벤트는 events.jsonl에 보존 |
| `send_ihave`, `recv_ihave`, `send_iwant`, `recv_iwant` | RPC 내 해당 control 종류의 관측 횟수, entry 수, message-ID 참조 수를 구분 |
| `logical_send_ihave`, `logical_recv_ihave`, `logical_send_iwant`, `logical_recv_iwant` | 광고·요청 ID 개수. 고유 ID 개수나 실제 전달 바이트와 다름 |
| `send_graft`, `send_prune`, `logical_send_graft`, `logical_send_prune` | 관측 mesh 전이와 reciprocal 논리 간선 생성·제거. wire RPC 횟수는 별도 `_rpc` 계열 |
| v3 추가 | IDONTWANT, drop control, 미분류 경로, lifecycle·점수 요약, 실제 libp2p 스트림 송수신 및 프로토콜별 B/W |

## Python·배치 시각화

| v2 소스 | v3 도구 |
|---|---|
| `basic_graph/*` | Series/Case 반복 평균·표본 SD, 임의 지표와 D 조건, 차수 확률 비교 |
| `compare_graph/*` | 동일 case 기준 차이, target/(target+baseline), target/baseline 및 오차 전파 |
| `trade-off_graph/*` | FRT–DRC 산점도, reach 색상, 기준 화살표, 2/3목적 Pareto, 가중 점수, i/f 그룹과 p 필터 |
| `time_vs_metric_plot.py` | 전파·중복 누적곡선, 선형/로그 시간 축, 결합 패널 |
| `calc_lazy_metric.py` | 같은 case의 lazy-off 기준을 이용한 기여·겹침·효율 등 8개 관측 추정 |
| `dup_regression.py` | v2 중복 회귀식 4개, 계수·rank·학습 자료 R² 및 관측/적합 비교 |
| `old/graphic.py` | v2 metric JSONL과 x/y 요약 가져오기, 임의 지표·축·색상의 산점도 |
| `old/propagation.py`, `old/prop_plot.py` | 메시지별 최초 수신 경로, 지연·hop 분포, 시간·hop별 평균 누적 수신 수와 증가량 |
| `old/prop_plot_graph.py` | 전파 CSV·JSON 곡선 overlay, 원래 크기/최대값 정규화 |
| `old/mean_degree_ratio.py` | Total/Count 자료 가져오기와 snapshot 평균 차수 확률 |
| `old/clustering_vs_eigenvector.py` | 두 지표 산점도와 기준 case 화살표 |
| `old/frt_drc_graph.py` | v2에 하드코딩된 역사적/ER 참고 그림 3개. 현재 실험 데이터와 분리된 버튼 |
| `old/fitting.py` | 가중 Student-t MLE, df/location/scale 변화, 단변량·다변량 이차 회귀, 지정 D 조건의 예측 분포 |
| `image_gen.sh`, `total_image_gen.sh`, `run*.sh` | 선택 결과의 서버 분석 접수, 전체 PNG/CSV/차트 정의 ZIP. 기존 Python 경로 설정이나 CLI를 실행하는 방식은 아님 |

Import는 파일당 32MiB까지 지원합니다. 제공 형식은 metric JSONL, 전파 tree JSON/JSONL, 중복 시각 map, x_case/y/yerr·x/y JSON, 숫자 CSV, v3 분석 JSON입니다. 임의의 v2 내부 자료 구조 전부를 자동 판별하거나 원래 디렉터리·파일명을 그대로 재현하지는 않습니다. 자세한 조작은 [시각화](visualization.kr.md)를 참고하십시오.

## 해석과 원자료

정확한 수식·집계·그래프 표본·Eager/Lazy 추정은 [저장 결과 연구 지표](experiment-metrics.kr.md#저장-결과-연구-지표)에서 관리합니다. v2와 비교할 때 다음 차이를 유지하십시오.

- 메인 Metrics의 세션 기간 정의와 v2 그림을 위한 `research` 정의는 별개입니다. 연구 reachability는 수신 시점에 조건부인 모집단을 사용하며, 메시지별 지표와 누적 곡선의 가중 방식도 다릅니다.
- v2 reciprocal GRAFT 그래프와 v3의 표본 이웃 그래프는 동일한 관측이 아닙니다. v3 저장 그래프는 토픽을 합친 무방향 간선이며 분석은 최대 1,440개 관측 표본을 사용합니다. 원본 간선이 없는 예전 요약으로 새 중심성·경로 지표를 복원할 수 없습니다.
- 추정된 eager/lazy, 알려지지 않은 출처, 모델 예측과 v2의 고정 역사적/ER 참고자료를 구분합니다. 새 로그의 메시지 ID도 실제 발신 큐 원인을 직접 증명하지 않습니다.
- 중앙값과 모집단 합집합을 바로잡은 연구 집계를 사용합니다. Student-t는 가상 표본 60,000개를 만들거나 실패 시 임의 파라미터를 넣지 않고 관측 확률에 직접 적합합니다. 따라서 기존 Python 수치·파일 구조·그림의 완전한 동일성을 주장하지 않습니다.

새 상세 ID와 그래프 간선은 해당 버전으로 배포한 이후 수집한 로그에만 존재합니다. 필드·생략 한도는 [API](api.kr.md#상세-peer-로그), 원본·분석·이미지 구분은 [보존 안내](results.kr.md#분석-파일과-이미지-보존), v3에 추가한 실제 스트림 B/W의 범위는 [대역폭 정의](bandwidth.kr.md)를 참고하십시오.

계산 검증은 [연구 지표 테스트](../internal/controller/analysis_research_test.go)와 [출처 추정 테스트](../internal/controller/analysis_origin_test.go), 로그는 [Peer tracer 테스트](../internal/peer/tracer_test.go), 가져오기·비교·그림은 [연구 UI 테스트](../internal/webui/research_test.cjs)와 [결과 이미지 테스트](../internal/webui/result_images_test.cjs)에 있습니다. 소스 목록 검사는 [개발 안내](development.kr.md#문서-관리)를 따릅니다.

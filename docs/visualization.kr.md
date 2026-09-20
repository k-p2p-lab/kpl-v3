# 실험 결과 이미지

[English](visualization.md) | 한국어

**Saved results → Images**에서 백그라운드 분석을 시작합니다. 서버가 로그 읽기·그래프 지표·전파 경로·분포 적합·저장 단계를 처리하고, 완료 데이터를 브라우저가 흰 배경의 가로 1,600px PNG로 만듭니다. **Graph images**는 기본으로 접힌 차트 제목 목록이며, 제목을 펼치면 이미지와 **PNG ↓ / CSV ↓** 링크를 표시합니다. **Download analysis JSON**은 서버에 저장된 전체 분석입니다.

**Graph protocol**에서 **GossipSub**(기본값), **Kad**, **Transport**를 선택하면 그래프 지표별 제목 한 행에서 해당 프로토콜의 별도 이미지를 확인합니다. 같은 창에서 프로토콜을 전환해도 펼친 제목은 유지합니다. 공통 지연·전파·대역폭 차트는 바뀌지 않습니다. **Download all PNG + CSV (ZIP)**에는 선택한 프로토콜이나 제목의 펼침 여부와 관계없이 세 프로토콜의 별도 PNG·CSV와 차트 정의를 모두 포함합니다.

창이나 브라우저를 닫아도 접수한 서버 분석은 계속됩니다. 같은 실행의 중복 요청은 기존 작업을 재사용하며, 최대 32개의 대기·실행 작업을 접수하며 한 번에 하나씩 계산합니다. 읽기 100% 이후에도 그래프 계산과 저장이 남을 수 있습니다. 완료 데이터는 Controller 재시작 후에도 보존됩니다. 진행 중 재시작한 작업은 `interrupted`가 되며 **Retry**로 다시 시작합니다. 네트워크 요청만 실패한 경우 Retry는 기존 작업에 연결합니다. PNG 변환과 비교 수식 계산은 창을 열었을 때 브라우저에서 수행합니다.

**Analyze latest snapshot**은 새 원본 스냅샷을 분석합니다. 분석기 버전이 바뀐 이전 캐시는 Images를 열 때 새 작업으로 갱신합니다. 원본 로그가 없는 정보는 캐시 갱신으로 복원되지 않습니다. 새 분석·재시도·갱신은 대시보드 로그인 세션을 사용하며 추가 인증 입력은 없습니다. 실행 삭제 시 분석 작업과 아티팩트도 삭제됩니다.

## 결과별 이미지

- 기존 v3 지연 CDF·히스토그램, 메시지 활동, Peer 점수, lifecycle.
- GossipSub·Kad·Transport별 노드 수, 평균 차수, v2의 비잎 분모 차수, 직경·평균 최단 경로, clustering, betweenness·PageRank·degree·closeness·eigenvector 중심성, assortativity, modularity, 연결된 노드 쌍의 비율을 별도 이미지로 표시.
- 전체 그룹 GossipSub 차수 확률·누적분포와 가중 Student-t 적합. Graph protocol을 바꾸어도 이 차트의 GossipSub 입력은 유지.
- 전파·중복 누적곡선, 선형·로그 시간 축과 복합 패널, hop 확률·누적분포, 메시지별 FRT·reachability·DRC, eager/lazy 추정과 미확인 경로.
- IHAVE/IWANT 등의 RPC 수, control entry 수, message-ID 참조 수, GRAFT/PRUNE 전이와 reciprocal 논리 간선 수.
- 전체·프로토콜별 송수신 kbit/s와 누적 KiB. 대역폭은 libp2p 스트림 사용량이며 회선 용량, IP/TCP 헤더·재전송·관리 API 트래픽을 포함하지 않습니다.

**Message images, repeated runs and v2 comparisons**를 펼쳐 메시지를 선택하고 **Message images**를 누르면 해당 메시지의 지연 CDF, hop 분포, 중복 누적곡선, 최초 수신 경로를 만듭니다. 전체 노드 ID·부모·시각·모집단은 분석 JSON에 있습니다. 근거 부족과 정의되지 않는 통계는 N/A 이미지로 표시합니다.

## 같은 실험의 반복 run 통합 평균

**Run experiment → Runs**를 2 이상으로 실행하면 **Saved results**에서 동일 `batchId`를 기본으로 접힌 시리즈 하나로 묶습니다. 헤더를 펼치면 개별 **Run m of n** 행의 **Images**·다운로드·삭제를 각각 사용할 수 있습니다. 목록 갱신은 펼침 상태를 유지하며 페이지를 새로 로드하면 다시 접힌 상태로 시작합니다.

시리즈 헤더의 **Delete group**을 누르면 저장된 run 수를 보여주는 확인창이 열립니다. 확인하면 실패·취소 run과 개별 분석을 포함한 해당 배치의 모든 저장 run 및 통합 평균 분석을 영구 삭제합니다. 그룹을 접은 상태에서도 사용할 수 있습니다. 활성 배치와 다운로드 중인 구성원은 보호하며, 시나리오 이름이 같은 다른 그룹에는 영향을 주지 않습니다.

배치의 모든 run이 정지한 뒤 시리즈 헤더의 **Analyze batch mean**을 누르면 동일 `batchId`의 완료 run을 통합합니다. 이 분석 버튼은 그룹을 접어도 사용할 수 있습니다. 시나리오 이름이 같아도 다른 실행 제출이면 합치지 않습니다. 완료 run이 최소 2개 필요하며 실패·취소·중단 run은 제외하고, 누락되거나 읽을 수 없는 run은 요청한 반복 수와의 차이로 표시합니다. 시리즈 헤더와 완료 화면에서 포함 수를 확인할 수 있습니다. 완료됐지만 관측 수신이 0건인 run도 대상에 남습니다.

서버는 각 run의 현재 저장 로그를 따로 분석하고 개별 분석과 제한된 작업 큐를 공유합니다. 서로 다른 run의 메시지·노드 ID를 합치거나 개별 분석 파일을 덮어쓰지 않습니다. 창이나 브라우저를 닫아도 작업은 계속되며 시리즈 헤더의 분석 버튼으로 다시 열어 진행 상황과 결과를 확인합니다. **Analyze latest snapshot**은 배치를 다시 계산합니다. 저장된 구성원이 바뀌면 기존 캐시가 무효화되며 로그 내용만 바뀐 경우에는 명시적으로 갱신합니다. 선택된 run의 로그가 잘못되면 해당 run을 조용히 빼지 않고 작업 실패로 표시합니다. 유효한 메인 지표의 측정 정의가 서로 다른 경우에도 다른 모집단을 섞어 평균하지 않고 실패로 알립니다.

**Mean metrics and contributing run counts**에서 지표별 산술평균, run 간 표본 표준편차와 유효 run 수를 확인합니다. 각 유효 run은 메시지·표본 수와 무관하게 같은 가중치를 갖습니다. 정의되지 않는 값은 0으로 바꾸지 않고 제외합니다. P95는 각 run의 P95를 평균한 값이며 전체 수신 표본을 합쳐 계산한 백분위가 아닙니다. 송수신 바이트와 처리율은 실제 수집된 libp2p 대역폭 근거를 사용합니다.

그래프·점수·메시지 활동·전파·control·대역폭 개요 이미지를 평균 차트로 제공합니다. 시계열은 run 시작 시점을 기준으로 정렬하고 선형 차트는 관측 구간 안에서 보간하며 계단형 차트는 기록 구간 안에서 값을 유지합니다. 관측 공백이나 마지막 관측 이후 구간은 제외하므로 점별 `n`은 달라질 수 있습니다. 선형·계단형 표시 격자는 최대 720점입니다. CDF는 공통 축에서 run별 분포를 평균하고 이산분포는 전체 지지집합을 유지합니다. 지연 히스토그램은 공통 30개 bin을 사용하며 원래 bin 내부를 균등 배분하므로 건수는 보존하지만 bin 내부 위치는 근사입니다. 적합 밀도는 run별 곡선을 평균하며 합친 표본으로 재적합하지 않습니다. 개별 메시지 트리는 각 run의 **Images**에서 확인합니다.

PNG/CSV ZIP에는 평균 차트 정의, 포함·제외 run 정보와 전체 요약 평균·표본 SD·유효 수를 담은 `*-summary.csv`가 들어갑니다. 차트 CSV의 `n`은 각 점에 기여한 run 수입니다. **Download analysis JSON**에는 서버에 보존한 평균과 개요 재현용 run별 경량 입력이 포함되며 전체 메시지·노드 경로는 개별 분석 JSON에 남습니다. PNG 변환과 곡선 평균화는 서버 계산 완료 후 브라우저에서 수행합니다.

## 반복 실험과 비교

1. **Load saved results**를 누르고 결과를 선택합니다. **Series**는 비교 집단, **Case**는 동일 조건입니다. 동일 Series/Case만 반복 실험으로 묶습니다. 실패한 전달이나 낮은 도달률 표본을 자동 제외하지 않습니다.
2. Metric·X axis·Scatter color를 고릅니다. Baseline series는 같은 Case끼리의 기준 집단, Reference case는 화살표와 D 파라미터 변화의 기준 조건입니다. 생략하면 첫 집단/조건을 사용합니다.
3. 모델이 필요하면 Parameters에 `i=1,f=0.5,p=90` 또는 `dLow=4,d=8,dHigh=12`를 입력합니다. `i1-f0.5-p90`, `4-8-12` 형태의 Case에서는 자동으로 읽습니다. 일반 실행 이름에서 값을 추측하지 않습니다. p는 백분율입니다.
4. **Analyze selected results and generate comparison images**를 누릅니다. 선택된 서버 분석은 창을 닫아도 계속됩니다. 다시 비교할 때 완료 캐시를 사용합니다.

생성 항목은 반복 평균·표본 SD 오차막대, 차이/`target/(target+baseline)`/비율, 범용 산점도, FRT–DRC와 도달률 색상, 기준점 화살표, 2목적/3목적 Pareto, 가중 점수, i/f 그룹 그림, lazy 기여 추정, v2의 중복 회귀식 4개, Student-t 파라미터의 단변량·다변량 이차 회귀, 관측·적합·예측 비교 및 곡선 overlay입니다. Degree predictions에는 `Dlow,D,Dhigh`를 한 줄씩 입력하여 모델 분포를 만듭니다.

반복 1회에는 표본 SD가 없습니다. 비교 오차는 독립 표본의 1차 오차 전파이고, 같은 입력 자체를 비교하면 오차가 0입니다. 회귀는 case 평균에 적합하며 독립 파라미터가 부족하면 계수를 만들지 않습니다. R²는 학습 자료 내 적합도입니다. 가중 점수는 선택 집합의 min–max 값에 의존하고, 변화 없는 차원은 0.5로 둡니다. Lazy 기여 추정에는 동일 조건의 **lazy-off 기준 집단**을 사용해야 합니다. 도달률 차이를 이용한 관측적 추정이며 인과 효과로 단정하지 않습니다.

## v2 자료와 정의

Import에서 v2 `metrics.jsonl`, 전파 tree JSON/JSONL, 중복 시간 map, `x_case/y/yerr`·`x/y` JSON, 숫자 CSV, v3 분석 JSON을 읽습니다. 파일당 최대 32MiB이며 브라우저 안에서 처리합니다. `x_case/y/yerr`를 읽기 전에 해당 Metric을 선택하십시오. CSV overlay는 단위가 같은 파일끼리 사용합니다. Peak-normalized overlay는 각 곡선과 제공된 오차 막대를 그 곡선의 양수 최댓값으로 함께 나눕니다. 양수 최댓값이 없는 곡선은 정규화 값을 표시하지 않습니다. 이 값은 원래 확률/횟수가 아닙니다. **v2 fixed historical / ER reference images**는 v2 코드에 들어 있던 고정 참고자료이며 선택한 실험의 관측값이 아닙니다.

## 지표와 수집 근거 확인

같은 이름의 메인 Metrics와 추가 `research` 값은 모집단·기간·집계가 다릅니다. 특히 연구 reachability는 수신 관측 시각에 조건부이고, raw 메시지도 시각 근거가 있으면 연구용 로그 지연 추정이 나올 수 있습니다. [저장 결과 연구 지표](experiment-metrics.kr.md#저장-결과-연구-지표)에서 정의·단위·표본·그래프 정규화를 확인하십시오. 서로 다른 정의의 결과를 동일 Series/Case 반복으로 묶지 마십시오. 가져온 v2 값은 원래 정의를 유지하며 자동 변환되지 않습니다.

Eager Push/Lazy Pull 색상과 기여 값은 메타정보 추정입니다. 분류된 eager/lazy가 0이어도 unknown 수신이 남을 수 있으므로 미분류 수를 함께 확인하십시오. [추정 규칙](experiment-metrics.kr.md#eager-push와-lazy-pull-추정)은 메시지 ID·GRAFT·IHAVE·IWANT의 사용과 근거 충돌을 설명합니다. [상세 Peer 로그](api.kr.md#상세-peer-로그)는 실제 수집 필드와 생략 한도를 정의합니다.

과거 기록에 없는 메시지 ID·그래프 간선·세션 증거는 재분석으로 복원되지 않습니다. 현재 이미지를 Controller·Agent에 배포하고 새 Peer로 실험하면 해당 버전의 로그를 수집합니다. [Swarm 업데이트](swarm.kr.md#장애업데이트종료)를 따르십시오. 현재 구현은 upstream libp2p의 관측값을 사용하며 별도 Python 실행 환경이나 이미지 생성 서비스는 필요하지 않습니다.

## 분석 결과 보존과 API

[백그라운드 분석 API](api.kr.md#백그라운드-분석)는 작업 상태·버전·원본 snapshot 시점과 전체/경량 응답을 정의합니다. 원본 경계는 대기열 접수 시점이 아니라 해당 작업이 계산 슬롯을 얻은 뒤 잡힙니다. 완료 작업은 이 경계를 유지하므로 나중에 도착한 로그를 포함하려면 **Analyze latest snapshot**을 사용하십시오.

서버는 완료 분석 JSON을 보존하고, PNG·CSV·비교 ZIP은 창을 열었을 때 브라우저에서 생성합니다. **Download results**의 원본 ZIP과 이 화면의 **Download analysis JSON**, **Download all PNG + CSV (ZIP)**은 서로 다른 파일입니다. 파일별 구성·보존 범위는 [분석 파일과 이미지 보존](monitoring.kr.md#분석-파일과-이미지-보존)을 참고하십시오.

[실험 지표](experiment-metrics.kr.md) · [v2 기능 대조](v2-analysis-coverage.kr.md) · [Bandwidth](bandwidth.kr.md)

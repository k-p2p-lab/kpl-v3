# 저장 결과·내보내기·보존

[English](results.md) | 한국어

[문서 안내](README.kr.md) · [저장소](../README.kr.md)

이 문서는 저장 원본, 다운로드, 삭제와 백업 경계를 설명합니다. 이어하기·재시도는 [실행·복구](experiments.kr.md), 그림과 비교는 [시각화](visualization.kr.md), 수식은 [지표 정의](experiment-metrics.kr.md)를 참고하십시오.

## 저장 결과 찾기

**Saved results**에서 실험 이름·배치 ID·run ID로 검색하고 **In progress**, **Completed**, **Needs attention** 상태 필터를 함께 사용할 수 있습니다. 일치하는 run이 있으면 이전 시도를 포함한 배치 전체를 표시하므로 진행률과 배치 작업을 그대로 이용할 수 있습니다. 상태 필터는 현재 시도를 기준으로 판정합니다. **Completed**는 그룹의 현재 run이 모두 완료된 경우, **Needs attention**은 실패·중단·취소되었거나 읽을 수 없는 결과가 있는 경우입니다.

결과 수는 화면에 표시되는 저장 run의 개수입니다. 목록을 갱신해도 검색·필터를 유지하며 **Clear filters**로 전체 목록을 복원합니다. 모바일에서는 결과 행을 시간 레이블과 줄바꿈되는 작업 버튼이 있는 카드로 표시합니다. Agent 표는 가로로 스크롤할 수 있습니다.

**Saved results**의 반복 실행 그룹 내부는 queued 및 재시도 항목을 포함해 **Run n of M**의 숫자 순서(1, 2, …, 10)로 정렬합니다. queued 항목이 실행을 시작해도 순서를 유지합니다. 이전 시도는 별도 표에 남으며, 유효한 회차 번호가 없는 항목은 마지막에 표시합니다. 배치 그룹 자체는 기존 결과 목록 위치를 유지합니다.

## 실험 결과 다운로드

대시보드의 실험 항목이나 **Saved results**에서 **Download results**를 선택합니다. 반복 run은 [시리즈 헤더](visualization.kr.md#같은-실험의-반복-run-통합-평균)를 펼쳐 각 run의 다운로드·분석·삭제를 사용합니다. 저장 목록은 Controller의 데이터 디렉터리를 읽으므로 Controller 재시작 후에도 결과에 접근할 수 있습니다. 백업 파일을 복원한 뒤에는 **Refresh**로 목록을 갱신합니다. 실행 중 실험에는 **Download snapshot**이 표시됩니다. 용량은 마지막 목록 조회 시점의 압축 전 원본 파일 합계인 **Source**로 바로 표시합니다. 실행·대기 중 결과도 **Live source**로 표시하며 새로고침하면 갱신됩니다. ZIP 다운로드 크기와는 다를 수 있습니다.

ZIP에는 다음 파일이 들어 있습니다.

| 파일 | 내용 |
|---|---|
| `scenario.yaml` | 실험 실행 시 제출한 시나리오 원문 |
| `experiment.json` | 저장된 실험 메타데이터·상태·seed·job 카운터 원문 |
| `events.jsonl` | 내보내기 기준 시점까지 저장된 전체 이벤트. 한 줄에 JSON 하나이며, 기록된 이벤트가 없으면 빈 파일 |
| `observations.jsonl` | 실행 중 약 5초마다 저장한 그룹 상태·차수·clustering·점수와 이용 가능한 프로토콜별 노드·간선 표본. 과거 결과에는 파일이나 간선이 없을 수 있음 |
| `metrics.json` | 동일 이벤트 로그 경계에서 재계산한 세션 기간 도달률 범위, 발행 시점 대상 결과, coverage, pending/unknown, 첫 원격 지연, 관측 중복, control 내역과 수집된 대역폭 누적량·품질. 과거 정의는 legacy 유지 |
| `export.json` | 내보내기 시각, 실험 상태, active/partial 여부와 원본 파일의 캡처된 크기 |

Controller의 저장 잠금 안에서 파일 크기를 확보한 뒤 잠금을 해제하고 ZIP을 전송합니다. 이후 추가된 이벤트는 제외되며 느린 다운로드가 telemetry 파일 쓰기를 붙잡지 않습니다. 완료된 실험에도 지연된 telemetry가 도착할 수 있으므로 나중에 추가된 기록까지 필요하면 수집이 안정된 뒤 다시 다운로드하십시오. `partial: false`는 기록된 실험 상태가 종료 상태라는 의미이며 telemetry 무손실을 보장하지 않습니다. 메시지 본문, PCAP, Prometheus/Grafana 데이터베이스는 ZIP에 포함하지 않습니다.

**Source / Live source**에는 분석 캐시, 생성 이미지, 내보낼 때 계산하는 파일과 파일시스템 부가 공간을 포함하지 않습니다. 대시보드는 ZIP 크기를 자동 계산하지 않습니다. `sourceBytes`, 명시적 HEAD 요청과 캐시 헤더는 [다운로드 API](api.kr.md#저장-결과와-다운로드)가 정의합니다.

저장 결과 목록과 다운로드에도 로그인이 필요합니다. [API 로그인](api.kr.md#인증) 후 쿠키 파일을 재사용하십시오.

```bash
curl --fail -b "${KPL_COOKIE_JAR:?Log in first}" http://control-node:8080/api/v1/results
curl --fail -b "${KPL_COOKIE_JAR:?Log in first}" --output run-results.zip \
  http://control-node:8080/api/v1/experiments/RUN_ID/download
```

`RUN_ID`는 저장 목록의 ID로 바꾸고 `access`가 출력한 control 노드 주소를 사용하십시오.

내보내는 내용은 구독 세션 start/checkpoint/stop과 기록된 Agent 종료 확인을 포함한 수집 telemetry입니다. Peer 재시도, Agent 큐 backpressure, 정상 종료 drain은 유실을 줄이지만 한도가 있으며 강제 종료는 Peer를 drain하지 않습니다. 원본 sequence 간격이 있으면 미수신은 unknown입니다. 다운로드로 누락을 복구하거나 아예 보이지 않은 세션을 발견할 수는 없습니다. `/api/v1/events`는 여전히 최근 300개만 반환하고 웹 이벤트 화면은 그중 최신 40개를 표시하지만 ZIP은 저장된 전체 로그를 읽습니다.

## 저장 결과 삭제

**Saved results → Delete**는 확인 후 선택한 run의 시나리오·메타데이터·이벤트와 실시간 지표 인덱스를 영구 삭제합니다. 실행·대기 중 실험, 활성 배치 구성원, 다운로드 중 결과는 보호합니다. API는 로그인 세션을 사용하는 `DELETE /api/v1/results/{id}`입니다. Peer를 종료하지 않으며 기존 Prometheus/Grafana 시계열은 유지합니다. 삭제 표식은 지연 telemetry의 결과 재생성을 막으므로 백업·이전 때 Controller 데이터와 함께 보존하십시오. 상태 코드와 다운로드 header는 [REST API 가이드](api.kr.md)를 참고하십시오.

직접 요청한 ZIP 크기 조회(`HEAD`)와 목록 조회는 삭제를 막지 않습니다. 실제 ZIP 다운로드(`GET`)는 요청이 끝날 때까지 결과를 보호합니다. 삭제 요청은 브라우저에서 30초 제한 시간을 적용하며, 시간 초과 후에도 Controller에서 완료될 수 있으므로 목록을 갱신하거나 같은 결과의 삭제를 재시도할 수 있습니다. 후속 목록 갱신이 느려도 삭제창 버튼은 다시 사용할 수 있습니다.

시리즈 헤더의 **Delete group**은 저장된 run 수를 확인한 뒤 `DELETE /api/v1/result-batches/{batchId}`로 배치 전체를 삭제합니다. 개별 원본 삭제와 달리 별도 `batch-analyses/{batchId}` 평균 파일까지 제거하며 진행 중인 분석도 취소합니다. 삭제 전 모든 구성원의 활성 상태·다운로드를 검사합니다. 저장 장치 오류로 일부만 삭제됐을 수 있으므로 오류를 해결한 뒤 목록을 갱신하고 재시도하십시오. 기존 Prometheus/Grafana 시계열은 유지합니다.

## 분석 파일과 이미지 보존

실행의 원본 내보내기와 계산된 분석은 경계가 다릅니다. **Download results / Download snapshot**은 내보낼 때 원본 파일 경계를 잡고, **Download analysis JSON**은 완료된 분석 작업의 경계를 사용합니다. 나중에 추가된 로그는 새 분석을 실행하기 전까지 기존 분석에 들어가지 않습니다. 합계를 대조할 때 `export.json.exportedAt`과 분석의 `asOf`·작업의 `snapshotAt`을 확인하십시오.

| 파일 / 산출물 | 위치와 내용 |
|---|---|
| `analysis-job.json` | 분석 요청 후 실행 파일 옆에 저장하는 현재 시도·상태·진행률·원본 경계 |
| `analysis-result.json` | 완료 서버 분석. 메인 지표, 연구 메시지·모집단·근거, 계산된 그래프·대역폭 시계열, 분포와 적합 결과 |
| `analysis-summary.json` | 완료 비교용 경량 응답. 메시지별 경로와 원본 시계열을 제외하고 집계·분포·적합·메시지 수 유지 |
| PNG / 차트 CSV / 차트 정의 JSON | 브라우저가 개요·메시지·비교 화면에서 생성. 제목을 펼쳐 개별 다운로드하며, **Download all PNG + CSV (ZIP)**에는 화면의 프로토콜 선택·제목 펼침 여부와 관계없이 모든 그래프 프로토콜의 PNG·CSV·차트 정의를 포함 |
| 가져온 v2 파일 / 비교 선택 | 브라우저 화면에서 유지하며 Controller에 업로드하거나 서버 비교 작업으로 저장하지 않음 |

위 서버 분석 파일 세 개는 **Download results** ZIP에 포함되지 않습니다. 계산 JSON은 분석 다운로드 endpoint로, 그림은 차트 다운로드로 각각 보존하십시오. 비교 ZIP은 생성한 차트이며 재분석에 필요한 전체 원본 기록이나 가져온 파일의 묶음이 아닙니다. 원본과 선택한 Series/Case·파라미터·소프트웨어 revision을 함께 남기십시오.

완료 분석은 데이터 볼륨에 보존되어 Controller 재시작 후에도 사용할 수 있습니다. 새 시도는 현재 캐시를 대체하므로 이전 분석 경계를 보존하려면 먼저 다운로드하십시오. 접수한 서버 계산은 브라우저를 닫아도 계속되고 이미지 변환·비교 계산은 화면을 다시 열거나 구성하여 수행합니다. 브라우저 다운로드는 서버가 미리 만든 이미지 보관소가 아닙니다. 실패·중단·취소 작업은 [백그라운드 API](api.kr.md#백그라운드-분석)에 따라 재시도합니다. 삭제 가능한 저장 실행을 지우면 분석 파일도 원본과 함께 제거됩니다.

반복 실험의 **Analyze batch mean** 작업은 `<data-dir>/batch-analyses/{batchId}/job.json`과 `result.json`에 별도로 보존합니다. 결과에는 run별 동일 가중 평균·표본 SD·유효 수와 평균 개요 재현용 경량 입력이 들어갑니다. 개별 원본을 삭제해도 기존 통합 스냅샷 파일은 남으며, 구성원이 바뀐 뒤 재분석하면 현재 통합 파일을 교체합니다. **Download analysis JSON** 또는 평균 PNG/CSV ZIP을 내려받고, 서버 백업에는 `runs`와 `batch-analyses`를 함께 포함하십시오. 통합 작업의 실패·제외 기준과 표본 수 해석은 [통합 평균 사용법](visualization.kr.md#같은-실험의-반복-run-통합-평균)에 정리되어 있습니다.

## 저장소와 백업

Swarm에서 `KPL_STACK_NAME=kpl`이면 control 노드의 `kpl_controller-data` volume을 `/var/lib/kpl/data`에 마운트합니다. 실행 파일은 `runs/<run-id>`에 있습니다. 일반 서비스 재생성과 `swarm.sh remove`는 volume을 유지합니다. 원본 이벤트의 자동 보존 한도나 다른 노드로의 복제는 없습니다.

숨김 항목을 포함한 Controller 데이터 디렉터리 전체를 보존하십시오.

| 내용 | 보존하는 항목 |
| --- | --- |
| `runs/` | 실행에 제출한 시나리오, 메타데이터, 로그, 관측과 개별 분석 |
| `scenarios/` | 재사용 시나리오 라이브러리 |
| `batch-analyses/` | 저장된 반복 평균 |
| `agent-capacities.json` | Agent ID별 용량 예외 |
| `.deleted-results/` | 늦은 telemetry가 삭제 결과를 재생성하지 못하게 하는 표식 |
| `logs/` | 회전하는 접근·인증 이력 |

실험 ZIP은 하나의 실험 기록이며 전체 Controller 백업이 아닙니다. Prometheus·Grafana 이력과 설정은 별도 volume을 백업합니다. [모니터링 보존](monitoring.kr.md#보존과-운영)을 참고하십시오. 실행 파일을 복원하면 결과를 조회할 수 있지만 세션·실시간 카운터를 복원하거나 미완료 실행을 자동 시작하지 않습니다. [복구 절차](experiments.kr.md)를 선택해 사용하십시오. Volume 위치, 쓰기 내구성과 종료 조건은 [배포 저장소와 재시작](swarm.kr.md#데이터-저장소와-재시작)을 참고하십시오.

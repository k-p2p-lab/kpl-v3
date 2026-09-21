# Prometheus와 Grafana로 실험 분석하기

[English](monitoring.md) | 한국어

[Swarm 배포 가이드](swarm.kr.md)에 따라 stack을 배포하십시오. Controller, 선택한 노드마다 Agent 하나, Prometheus와 Grafana를 실행하며 데이터 소스와 **KP2PLab Experiment Analysis** 대시보드는 자동 등록됩니다. `sh scripts/swarm.sh access`로 control 노드의 게시 주소를 확인하십시오. 아래 예제의 `control-node`는 해당 호스트 주소로 바꿉니다.

대시보드는 영어로 작성되어 있으며, Swarm stack은 `GF_USERS_DEFAULT_LANGUAGE=en-US`로 Grafana UI 기본 언어를 영어로 설정합니다. 사용자나 조직의 언어 설정이 있으면 해당 UI 기본값보다 우선합니다. [Grafana 언어 설정 문서](https://grafana.com/docs/grafana/latest/administration/organization-preferences/#change-grafana-language)

Grafana는 최초 실행 시 SQLite 데이터베이스를 초기화하므로 디스크 성능에 따라 준비까지 수 분이 걸릴 수 있습니다. `sh scripts/swarm.sh logs grafana`에서 초기화 진행 상황을 확인할 수 있으며, 이후 시작에서는 기존 데이터베이스를 사용합니다.

로컬 SQLite 데이터베이스에는 WAL 모드를 사용합니다. 데이터베이스와 WAL 파일은 같은 Grafana named volume에 보존됩니다. [Grafana 데이터베이스 설정](https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana/#wal)

| 화면 | 기본 주소 |
|---|---|
| KPL Dashboard | http://control-node:8080 |
| Grafana 실험 분석 | http://control-node:3000/d/kpl-experiments |
| Prometheus 쿼리/수집 상태 | http://control-node:9090 |
| Controller 지표 원문 | http://control-node:8080/metrics |

Swarm stack은 Grafana 익명 접속을 비활성화하고 `GRAFANA_ADMIN_PASSWORD`를 필수로 요구합니다. 설정 helper가 manager 설정에 자격 증명을 저장하며 `sh scripts/swarm.sh credentials`로 설정된 로그인 정보를 확인할 수 있습니다. 이 stack은 시작할 때 원래 Grafana 관리자 비밀번호를 설정값으로 동기화하며 기존 사용자명과 데이터는 유지합니다.

Swarm은 Prometheus/Grafana 포트를 control 노드에 게시합니다. 각 Agent의 전용 metrics listener도 해당 노드의 `KPL_AGENT_METRICS_PORT`(기본 `9091`)로 게시합니다. `scripts/swarm.sh configure`로 `PROMETHEUS_PORT`, `GRAFANA_PORT` 또는 Agent metrics 포트를 변경한 뒤 다시 배포해 적용하십시오.

대시보드 상단 메뉴는 Prometheus와 Grafana를 새 탭으로 엽니다. 현재 Dashboard의 scheme과 호스트를 유지하고 설정된 게시 포트로 바꿉니다. 브라우저가 사용하는 포트가 설정값과 같으면 직접 접속과 SSH 터널에서 동작합니다. Proxy가 scheme/path를 바꾸거나 로컬 forwarding에 다른 포트를 쓰면 실제 모니터링 주소를 별도로 여십시오.

## Dashboard 내장 시각화

**Metrics**는 클러스터·전달·대역폭·관측 지표 카드 11개를 가로 한 행에 표시합니다. 가로로 스크롤하거나 **Previous metrics / Next metrics** 화살표 버튼으로 이동하십시오. 카드에 마우스를 올리면 해당 카드 하나만 가로로 넓어지면서 상세 수치와 설명을 보여 줍니다. 긴 상세 내용은 펼친 카드 안에서 세로로 스크롤합니다.

- 카드를 클릭하거나 탭하면 상세를 열고, 다시 선택하면 닫습니다. 터치 화면에서는 가로로 밀어 행을 이동합니다.
- 키보드 초점을 받으면 상세가 열립니다. Left/Right로 이전·다음 카드, Home/End로 첫·마지막 카드로 이동합니다. Enter/Space는 상세를 열고 닫으며, Escape 또는 행 바깥 클릭·탭으로 닫습니다.
- **How delivery is measured**는 행 바로 아래에 유지합니다. 설명과 펼침 상태는 실시간 갱신 중에도 유지됩니다.

**Online Agents**와 **Ready Peers**는 전체 클러스터, 메시지·대역폭 카드는 **Run metrics**에 표시된 실행을 대상으로 합니다. **Observation quality**의 대표 값은 `Receipt / Continuity / Start` 순서의 각 unknown 수이며, 서로 겹칠 수 있는 범주를 합산하지 않습니다. 펼치면 전체 레이블과 outcome 수를 확인할 수 있습니다.

Run이 완료·실패·취소되면 해당 실험의 캐러셀 수치는 초기 N/A·0으로 돌아갑니다. 카드는 그대로 유지하며 다음 run이 시작되면 새 수치를 표시합니다. 클러스터 수치는 실시간 상태를 유지하고 최종 측정값은 **Saved results**에서 확인합니다.

**Available slots**는 online Agent가 보고한 여유 Peer 수를 합산하며 offline Agent는 포함하지 않습니다. Capacity는 CPU·메모리 예약이 아닌 생성 허용 개수입니다. 값을 늘리기 전 [Peer 배치와 capacity](swarm.kr.md#분배와-용량)를 확인하십시오.

**Saved results → Images**에서 서버 백그라운드 분석을 접수하고 개요·메시지·반복 비교 그림을 PNG/CSV/ZIP으로 다운로드합니다. 저장 기록을 사용하며 Prometheus 보존과 독립적입니다. 조작 방법은 [결과 이미지](visualization.kr.md), 계산 정의는 [실험 지표](experiment-metrics.kr.md#저장-결과-연구-지표)를 참고하십시오.

### 패널 접기와 펼치기

**Network overview**, **Experiment progress**, **Agent status**, **Recent network events**, **Saved results** 헤더의 **Collapse / Expand** 버튼으로 패널을 접고 펼칩니다. 좁은 화면에는 화살표 아이콘을 표시합니다. 클릭·탭과 키보드 Enter/Space를 지원합니다. 다섯 패널은 기본으로 열리며, 같은 브라우저의 Dashboard origin에 선택을 저장해 새로고침 뒤에도 유지합니다.

**Network overview**와 **Experiment progress**는 전체 폭의 세로 배치를 유지합니다. 넓은 화면에서 **Agent status** 또는 **Recent network events** 중 하나를 접으면 짧은 제목을 위에 두고 열린 패널을 아래에 전체 폭으로 표시합니다. 둘 다 펼치면 기존 Agent/events 열 배치를 복원합니다. 둘 다 접으면 짧은 제목 둘을 나란히 표시하며, 모바일에서는 세로 배치를 유지합니다.

패널을 숨겨도 실험·백그라운드 분석과 실시간 데이터 갱신은 계속됩니다. **Network overview**를 접으면 배치 애니메이션도 멈추고, 다시 펼치면 최신 상태를 표시하며 기존 **Pause motion** 선택을 유지합니다. 이 버튼은 패널 표시만 바꿉니다.

## 실행과 분석

1. 대시보드의 **Run experiment**에서 [`examples/monitoring.yaml`](../examples/monitoring.yaml)을 실행합니다. envelope 발행과 raw 발행을 함께 확인하는 작은 실험입니다.
   **Run** 옆 **Runs**를 1~100으로 지정하면 순차 반복합니다. 회차마다 별도 결과를 남기며 실패 또는 **Stop batch**는 나머지를 취소합니다. [반복 실행과 지표 정의](experiment-metrics.kr.md)를 참고하십시오.
2. Grafana에서 **Run**(run_id), **Agent**, **Topic**을 선택합니다. 여러 run을 고르면 선택한 실험의 트래픽·지연을 합산하며, 네트워크 설정·대역폭 시계열은 범례에서 run별로 구분합니다. 세션 도달률 패널은 Run만, 대역폭·control 패널은 Topic 없이 Run과 Agent를 적용합니다.
3. 실험이 끝난 뒤에도 시간 범위를 해당 실행 구간으로 지정하면 시계열을 볼 수 있습니다. 기본 새로고침은 5초입니다.

Prometheus는 Controller와 Agent의 `/metrics`를 5초마다 수집합니다. Controller가 등록된 Agent의 metrics URL을 HTTP service discovery로 반환하고, Prometheus가 service VIP 대신 각 Agent의 host-mode 포트에 직접 접근합니다. Peer를 직접 scrape하거나 각 컨테이너에 exporter를 추가하지 않습니다. Controller는 기존 Peer telemetry를 누적 집계하므로 `scope: all`에서 telemetry가 손실되면 Controller가 만드는 Peer 지표도 영향을 받습니다. 모니터링 서비스는 별도 Docker network에 있으며 Peer에는 추가 네트워크·권한을 부여하지 않습니다.

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

결과 목록의 `sourceBytes`는 `scenario.yaml`, `experiment.json`, `events.jsonl`, `observations.jsonl` 중 현재 존재하는 일반 파일의 크기를 합산한 바이트 수입니다. 로그 본문을 읽거나 분석·압축하지 않고 파일 통계만 조회하므로 큰 결과도 용량을 빠르게 표시합니다. 생성된 분석 캐시·이미지, 다운로드 시 생성하는 `metrics.json`·`export.json`, 파일시스템 할당 오버헤드는 제외합니다. 메타데이터가 손상되어도 원본 파일 크기를 조회할 수 있으면 용량을 표시합니다. 원본 파일 자체의 통계를 읽을 수 없으면 `sourceBytes`를 생략하고 화면에는 **Source · —**로 표시합니다.

대시보드는 용량 표시를 위한 `HEAD` 요청이나 ZIP 크기 재측정 타이머를 실행하지 않습니다. 직접 호출하는 `HEAD /api/v1/experiments/RUN_ID/download`는 기존처럼 ZIP encoder를 바이트 counter에 실행하여 정확한 `Content-Length`를 반환하며, 실제 GET 다운로드도 해당 측정을 사용합니다. ZIP은 메모리나 디스크에 통째로 보관하지 않고 스트리밍합니다. 같은 run의 동시 측정은 공유하고 서로 다른 run의 측정은 최대 두 개로 제한합니다. 기존 API 호환성을 위해 목록은 준비된 ZIP 크기가 유효하면 `downloadBytes`도 제공하지만 화면은 이를 사용하지 않습니다.

Pending publication이 없으면 파일과 상태가 같은 동안 측정한 바이트 수를 유지하면서 HEAD 또는 다운로드마다 정밀도를 유지한 새 export 시각을 기록합니다. 저장 방식이 고정된 `export.json`에는 고정 폭 시각 문자열을 사용하므로 시각이 바뀌어도 인코딩 길이는 같습니다. Stable 목록 행에는 `downloadSizeMaxAgeMs`가 없고 stable HEAD 응답에는 `X-KPL-Result-Size-Max-Age-Ms`가 없습니다. Pending publication이 있으면 바이트 수와 export 경계를 짧게 함께 재사용합니다. 목록 행은 `downloadSizeMaxAgeMs`를 포함하고 HEAD는 `X-KPL-Result-Size-Max-Age-Ms`를 제공합니다. 두 값은 서버의 남은 cache 유효 시간에서 응답 안전 여유 1초를 뺀 양의 밀리초이므로 client와 server의 시계가 일치하지 않아도 됩니다. 준비된 값을 처음 확인한 목록만 유효 시간을 한 번 연장하고, 연장한 경계에서 계산한 max age를 응답하므로 바로 이어지는 GET이 표시 크기와 일치합니다. 목록을 반복해서 새로고침해도 deadline이 계속 밀리지는 않습니다. 안전 여유를 제외하고 남은 유효 시간이 없으면 목록은 `downloadBytes`와 `downloadSizeMaxAgeMs`를 모두 생략하고, HEAD는 응답 전에 새 경계를 측정합니다.

Controller 시작 후 첫 HEAD나 원본 파일·상태 변경 뒤에는 캡처한 파일을 읽고 지표를 다시 구성해야 하며, pending 결과는 짧은 cache가 만료된 뒤 이 작업을 반복합니다. 측정 slot 대기, 원본 로그 읽기와 재구성한 지표 집계 단계에서 HTTP 요청 취소를 확인하며, 취소된 요청은 server error 응답 없이 중단합니다. 저장한 이벤트 로그가 크면 측정 시간이 길어질 수 있습니다. 이 비용은 직접 요청한 HEAD나 실제 다운로드에만 발생하며, 화면의 원본 용량 표시에는 필요하지 않습니다.

재시작 후 저장 상태가 `running` 또는 `queued`인 실험은 `interrupted`로 표시하며 ZIP 원본 메타데이터는 변경하지 않습니다. 이는 표시 상태이며 실제 Peer 종료를 증명하지 않습니다. 실시간 상태·카운터를 복원하거나 실행을 자동 재개하지 않지만 ZIP 지표는 보존된 로그에서 재계산합니다. 메타데이터를 읽을 수 없는 항목은 `unreadable`로 표시하므로 로그·저장 파일을 확인하십시오.

**Saved results → Retry from run N (수량)**으로 실패·중단된 회차를 포함한 미완료 회차부터 다시 실행할 수 있습니다. 완료된 회차의 결과는 유지합니다. 단일 실험 실패에는 **Retry experiment**를 사용합니다. Controller가 먼저 이전 Peer를 정리한 뒤 원래 시나리오·회차 번호·seed로 각 회차의 첫 phase부터 실행합니다. 정리가 실패하면 새 회차는 취소 상태로 남으며 원인을 해결한 뒤 다시 이어하기를 누를 수 있습니다. 이전 로그는 **Previous attempts**에 보존하고 배치 진행률·평균에서 제외합니다. 기존 **Continue remaining**은 시도했던 회차를 건너뛰고 미시작 잔여 회차만 실행합니다.

**Saved results → Delete**는 확인 후 선택한 run의 시나리오·메타데이터·이벤트와 실시간 지표 인덱스를 영구 삭제합니다. 실행·대기 중 실험, 활성 배치 구성원, 다운로드 중 결과는 보호합니다. API는 로그인 세션을 사용하는 `DELETE /api/v1/results/{id}`입니다. Peer를 종료하지 않으며 기존 Prometheus/Grafana 시계열은 유지합니다. 삭제 표식은 지연 telemetry의 결과 재생성을 막으므로 백업·이전 때 Controller 데이터와 함께 보존하십시오. ZIP 원본 복사는 스트리밍하며 지표 재구성에는 고유 이벤트 ID·메시지/수신 쌍에 비례한 메모리가 필요합니다. 상태 코드와 다운로드 header는 [REST API 가이드](api.kr.md)를 참고하십시오.

직접 요청한 ZIP 크기 조회(`HEAD`)와 목록 조회는 삭제를 막지 않습니다. 실제 ZIP 다운로드(`GET`)는 요청이 끝날 때까지 결과를 보호합니다. 삭제 요청은 브라우저에서 30초 제한 시간을 적용하며, 시간 초과 후에도 Controller에서 완료될 수 있으므로 목록을 갱신하거나 같은 결과의 삭제를 재시도할 수 있습니다. 후속 목록 갱신이 느려도 삭제창 버튼은 다시 사용할 수 있습니다.

시리즈 헤더의 **Delete group**은 저장된 run 수를 확인한 뒤 `DELETE /api/v1/result-batches/{batchId}`로 배치 전체를 삭제합니다. 개별 원본 삭제와 달리 별도 `batch-analyses/{batchId}` 평균 파일까지 제거하며 진행 중인 분석도 취소합니다. 삭제 전 모든 구성원의 활성 상태·다운로드를 검사합니다. 저장 장치 오류로 일부만 삭제됐을 수 있으므로 오류를 해결한 뒤 목록을 갱신하고 재시도하십시오. 기존 Prometheus/Grafana 시계열은 유지합니다.

저장 결과 목록과 다운로드에도 로그인이 필요합니다. [API 로그인](api.kr.md#인증) 후 쿠키 파일을 재사용하십시오.

```bash
curl --fail -b "${KPL_COOKIE_JAR:?Log in first}" http://control-node:8080/api/v1/results
curl --fail -b "${KPL_COOKIE_JAR:?Log in first}" --output run-results.zip \
  http://control-node:8080/api/v1/experiments/RUN_ID/download
```

`RUN_ID`를 저장 목록의 실험 ID로 바꾸십시오. Swarm에서는 control 노드 주소를 사용합니다. `KPL_STACK_NAME=kpl`이면 원본은 해당 노드의 `kpl_controller-data` 볼륨에 있습니다. 이 볼륨을 Controller 내부 `/var/lib/kpl/data`에 마운트하며, 실험별 파일은 그 아래 `runs/<run-id>`에 저장됩니다. 매니저의 `swarm.sh remove`는 이 볼륨을 보존합니다. 원시 이벤트의 자동 보존 기한이나 노드 간 복제는 없으므로 디스크 공간과 백업을 별도로 관리하십시오.

내보내는 내용은 구독 세션 start/checkpoint/stop과 기록된 Agent 종료 확인을 포함한 수집 telemetry입니다. Peer 재시도, Agent 큐 backpressure, 정상 종료 drain은 유실을 줄이지만 한도가 있으며 강제 종료는 Peer를 drain하지 않습니다. 원본 sequence 간격이 있으면 미수신은 unknown입니다. 다운로드로 누락을 복구하거나 아예 보이지 않은 세션을 발견할 수는 없습니다. `/api/v1/events`는 여전히 최근 300개만 반환하고 웹 이벤트 화면은 그중 최신 40개를 표시하지만 ZIP은 저장된 전체 로그를 읽습니다.

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

## 지표의 의미

| 지표 | 의미 |
|---|---|
| `kpl_events_total` | Controller가 수신한 이벤트 누적 수. `run_id`, `agent_id`, `event_type`, `topic`으로 구분 |
| `kpl_message_bytes_total` | 발행/수신 이벤트의 `fields.wireBytes` 합계. Envelope 사용 시 JSON/base64를 포함한 PubSub data이며 libp2p framing 및 TCP/IP 헤더 제외 |
| `kpl_p2p_stream_bytes_total`, `kpl_p2p_protocol_stream_bytes_total` | Run·Agent·방향별 실제 libp2p 스트림 누적 바이트. 전체 및 negotiated protocol별 값 |
| `kpl_p2p_stream_bits_per_second`, `kpl_p2p_protocol_stream_bits_per_second` | 소스 수집 구간의 평균 bit/s gauge. `rate()` 없이 직접 조회 |
| `kpl_p2p_bandwidth_sample_timestamp_seconds`, `kpl_p2p_bandwidth_sessions` | 최근 소스 표본 시각과 정상 host 종료 후 표본 수신·미수신 세션 수. [대역폭 품질과 한계](bandwidth.kr.md) 참고 |
| `kpl_gossipsub_control_rpcs_total` | 각 GossipSub 제어 타입을 포함한 RPC envelope 수. `send`, `recv`, 로컬 송신 전 `drop`으로 구분 |
| `kpl_gossipsub_control_entries_total` | 해당 RPC에 담긴 repeated protobuf control entry 수 |
| `kpl_gossipsub_control_message_ids_total` | IHAVE, IWANT, IDONTWANT entry 안의 중복 제거하지 않은 메시지 ID 참조 출현 횟수 |
| `kpl_gossipsub_control_peer_exchange_records_total` | PRUNE entry에 담긴 peer-exchange record 수 |
| `kpl_window_stable_pairs`, `kpl_window_reached_pairs` | 기간이 지난 메시지 중 수신 기간 전체의 구독이 증명된 수신 세션 쌍 및 기한 내 성공. `run_id`로 구분 |
| `kpl_window_unknown_pairs`, `kpl_window_missed_pairs`, `kpl_window_late_pairs` | 안정 쌍의 수신 불명·확인된 미도달. 기한 내 수신이 없는 late 수는 앞의 두 결과 중 하나와 겹침 |
| `kpl_window_delivery_ratio` (`bound`: `lower` / `upper`) | 안정 대상 조건부 도달률의 논리적 상·하한. 안정 쌍이 없으면 생략하며 신뢰구간이 아님 |
| `kpl_window_initial_pairs`, `kpl_window_initial_reached_pairs`, `kpl_window_initial_unknown_pairs` | 확인된 발행 시점 대상, 이탈자를 포함한 기한 내 성공, 수신 불명 수 |
| `kpl_window_initial_delivery_ratio` (`bound`: `lower` / `upper`) | 확인된 발행 시점 대상 안의 논리적 도달률 범위. 해당 집단이 비었을 때만 생략 |
| `kpl_window_stable_coverage`, `kpl_window_stable_coverage_upper_bound` | coverage 하한인 안정/확인된 발행 시점 대상과 상한인 (안정 + 지속 여부 불명)/확인된 발행 시점 대상. 확인된 집단이 비었을 때만 생략 |
| `kpl_window_departed_pairs`, `kpl_window_continuity_unknown_pairs` | 마감 전 이탈이 확인된 발행 시점 쌍과, 이탈·마감까지 지속 어느 쪽도 증명하지 못한 쌍 수 |
| `kpl_window_publication_availability_unknown_pairs`, `kpl_window_availability_unknown_pairs` | 발행 시점 생존을 증명하지 못한 후보 쌍과 발행 시점·지속 불명의 합계 |
| `kpl_window_pending_publications`, `kpl_window_finalized_publications` | 마감 전 메시지와 기간이 지난 메시지. 확정 결과도 늦은 telemetry로 정정 가능 |
| `kpl_window_measurement_incomplete`, `kpl_window_legacy_publications` | 관측된 측정 범위 불명 stream 여부(0/1)와 과거 정의 발행 수. 0도 아예 보이지 않은 telemetry는 검출하지 못함 |
| `kpl_window_propagation_latency_seconds` | 안정 대상의 기한 내 첫 원격 envelope 수신 지연. `run_id`, 수신 `agent_id`, `topic`으로 구분. raw·로컬·마감 후·이탈·불명·무효 표본 제외 |
| `kpl_window_duplicate_copies`, `kpl_window_duplicates_per_reached_pair` | 안정 대상의 원격 성공 쌍에서 기간 안에 관측한 추가 PubSub 복사본 및 성공당 평균 |
| `kpl_operation_failures_total` | `onError: continue` 정책에서 기록한 publish/leave 실패 |
| `kpl_telemetry_dropped_events_total` | 보고된 telemetry 유실. P2P 패킷 손실이나 미보고 유실이 0이라는 증거가 아님 |
| `kpl_nodes` | 실험·Agent·그룹·역할·타입·상태별 피어 수 |
| `kpl_agent_*` | Controller에서 관측한 Agent 온라인 상태, 용량, 마지막 heartbeat |
| `kpl_experiment_*` | 실험 상태, 단계, job 상태 |
| `kpl_network_configured_*` | starting/ready 피어의 확정 설정을 그룹별 집계. delay는 평균/최소/최대, jitter/loss는 평균 |
| `kpl_local_*` | 각 Agent의 로컬 피어 상태, 용량, 정리 대기, telemetry 큐 길이 |
| `go_*`, `process_*` | scrape 대상 Controller/Agent 프로세스의 런타임·CPU·메모리. Peer 컨테이너 전체 자원 사용량은 아님 |

`kpl_network_configured_loss_ratio`는 설정한 패킷 손실률이며 실제 관측 손실률이 아닙니다. 설정 delay도 실제 RTT와 구분합니다. graft/prune/remove_peer 이벤트는 PubSub mesh 변화를 분석하는 자료이며 TCP 연결 그래프나 완전한 mesh snapshot을 뜻하지 않습니다.

제어 counter는 `run_id`, `agent_id`, `direction`, `control_type` label만 사용합니다. IWANT와 IDONTWANT에는 topic이 없고 하나의 RPC가 여러 topic을 담을 수 있으므로 Topic filter는 적용하지 않습니다. `send`는 원격 수신이 아닌 outbound queue 수락, `recv`는 이후 router 정책 검사 전 관측, `drop`은 `netem` 손실이 아닌 로컬 queue/크기 거부입니다. RPC, entry, 메시지 ID 참조, PRUNE peer-exchange 수를 서로 다른 단위로 읽으십시오. RPC별 정확한 topic 수와 `metrics.json`의 Agent별 누적치는 결과 ZIP에 남습니다. [실험 지표 정의](experiment-metrics.kr.md#gossipsub-제어-트래픽)를 참고하십시오.

현재 관계는 Dashboard의 [대화형 토폴로지](topology.kr.md)가 Peer 상태 snapshot으로 transport·Kademlia 라우팅 테이블·GossipSub mesh를 각각 표시합니다. Controller는 `observations.jsonl`에도 표본 프로토콜 그래프를 저장하며, 이 그래프는 프로토콜 안에서 토픽 간선을 합치고 중간 전이 전부나 개별 점수 쌍을 보존하지 않습니다. Prometheus에는 이에 대응하는 전체 그래프 이력이 없습니다.

`session-window-v1`은 실제 발행 시각과 `publish.deliveryWindow`(기본 10초, 양수·최대 1시간)를 사용합니다. 주 조건부 도달률은 기간 전체의 구독을 세션 증거로 확인해야 합니다. 마감 전 이탈은 조기 성공했어도 제외하며 발행 시점 대상 도달률에는 유지합니다. 늦은 join과 발행자 로컬 수신은 양쪽 모두 제외합니다. 단절이나 mesh 변화로 구독자를 제거하지 않습니다. 안정 도달률 범위, 발행 시점 대상 범위, coverage, pending, 불명을 함께 보십시오. sequence 누락은 확인된 미도달이 아닌 unknown이며 가용성 증거 부재도 확정 이탈은 아닙니다.

Grafana 세션 패널에는 Run 필터만 적용하고 백분율 평균이 아닌 수신 쌍 합계로 집계합니다. 범위는 관측된 집단에 대한 값이며 보이지 않는 모집단을 증명하지 않습니다. 발행 시점 가용성 경고나 `measurementIncomplete`가 있어도 확인된 발행 시점 도달률과 안정 coverage를 표시하며, 선택한 run 전체에 확인된 발행 시점 쌍이 하나도 없을 때만 N/A입니다. 각 집계에서 확인된 발행 시점 대상 = 안정 + 이탈 + 지속 여부 불명이 성립합니다. 과거 발행 패널은 새 결과에서 제외한 과거 데이터를 표시합니다. 새 패널은 `kpl_window_*`만 사용하고 이전 `kpl_delivery_*`·`kpl_propagation_latency_seconds`는 과거 의미를 유지하므로 섞지 마십시오.

전체 `deliver`에는 로컬 수신도 포함되므로 발행 수로 나누어 도달률을 구하지 않습니다. TCP 재전송은 패킷 손실을 지연으로 바꿀 수 있습니다. [지표 정의와 수식](experiment-metrics.kr.md)을 확인하십시오. 늦은 배치가 기간 gauge와 histogram 버킷을 정정할 수 있어 `rate`/`increase` 대신 직접 조회합니다. Grafana 지연은 실험 전체 누적 분위수이며 선택 시간 범위만의 분위수가 아닙니다.

누적 카운터는 최근 300개 웹 이벤트 버퍼와 독립적입니다. Controller 프로세스가 재시작되면 카운터가 초기화되며 과거 `events.jsonl`을 자동 재생하지 않습니다. Prometheus에 이미 저장된 시계열은 유지되고 `rate`/`increase`는 관측된 카운터 재설정을 처리합니다. 단, scrape 전에 사라진 이벤트나 telemetry 전송 실패를 복구하는 기능은 아닙니다. `increase`는 scrape 표본으로 추정한 구간 증가량이므로 누적 정수 이벤트 수와 항상 정확히 일치하지는 않습니다.

raw 수신도 수신 수·바이트에는 포함되지만 지연 히스토그램에는 포함되지 않습니다. 지연 표본이 없는 구간은 0ms로 해석하지 마십시오. 각 Peer는 시작 시 최대 5초 동안 Controller health 표본을 최대 7개 수집하고, 빠른 실패 사이를 250ms 띄운 뒤 최소 RTT 표본의 중간점을 사용합니다. 실패해도 Ready 진행을 막지 않습니다. 미동기화 상태에서는 5초마다 다시 시도하고 동기화 상태에서는 30초마다 갱신합니다. 성공한 표본은 2분 후 만료되며 마지막 offset은 timestamp 연속성을 위해 유지하지만, 재동기화할 때까지 신뢰 시계 metadata를 중단합니다. 범위로 기한 내 수신을 증명할 수 있는 음수 추정은 도달 성공에 포함하지만 지연 histogram에서는 제외합니다. 모든 호스트에서 chrony/NTP를 계속 사용하십시오. checkpoint는 계측 애플리케이션 세션의 증거이며 물리적 uptime을 뜻하지 않습니다.

## 실제 실행 검증

Linux에서 새 실행을 검증하고 ZIP, 코드 revision 또는 image digest, Prometheus 시간 범위를 보존하십시오. 저장소에는 이전 모니터링 실행 예시의 원본 ZIP이 포함되어 있지 않으므로 해당 과거 수치를 현재 세션 기간 구현의 감사 가능한 기준으로 사용할 수 없습니다.

[`examples/monitoring.yaml`](../examples/monitoring.yaml)에서는 다음을 확인하십시오.

| 확인 항목 | 소스에 따른 기대값 |
|---|---|
| 성공 발행 | 모든 예약 발행이 성공하고 telemetry가 수집되면 envelope 60 + raw 6 = 66건 |
| worker 네트워크 설정 | worker 세 개에 delay 50ms, jitter 5ms, loss 1% 설정 |
| 도달률 분모 | 전체 `deliver` 수가 아니라 각 발행의 수신 기간에 해당하는 동일 run/topic 세션 증거에서 재구성 |
| 지연 표본 | 안정 대상의 기한 내 원격 envelope 쌍만 포함. raw와 발행자 로컬 수신은 제외 |
| 관측 품질 | pending, unknown, incomplete와 보고된 telemetry-drop 값을 보존. 보고된 drop이 0이어도 완전한 로그를 증명하지 않음 |
| 정리 | 마지막 `stop-all`이 Peer 제거를 요청하므로 Agent 상태와 각 Linux 호스트의 잔존 Peer 컨테이너 확인 |

`metrics.json`을 정확히 같은 `events.jsonl` 경계와 `export.json.exportedAt`에 맞춰 비교하십시오. Counter 비교에도 같은 Controller 생명주기와 수집 경계가 필요합니다. Heartbeat가 `add_peer`/`remove_peer`의 0 기준값을 초기화하지만 첫 scrape 전 이벤트는 `increase` 추정에서 누락될 수 있습니다. 수신·중복·mesh 이벤트·지연 합계는 실행과 관측에 따라 달라지므로 고정된 합격 기준값이 아닙니다.

지표 구현과 회귀 사례는 [실험 지표](experiment-metrics.kr.md#구현과-관련-가이드)에 연결되어 있습니다. Linux 검증 명령은 [개발 가이드](development.kr.md)를 사용하십시오.

## 보존과 운영

Prometheus 시계열은 `prometheus-data`, Grafana 설정은 `grafana-data` named volume에 저장됩니다. 일반적인 컨테이너 재생성 후에도 유지됩니다. Prometheus 보존 설정은 15일/5GB이며 먼저 도달한 정책이 적용됩니다. 5GB는 디스크 사용량의 엄격한 상한이 아니며 WAL·head·압축 작업에는 추가 공간이 필요합니다. [Prometheus 저장소 문서](https://prometheus.io/docs/prometheus/latest/storage/)

Swarm config는 불변이므로 Prometheus와 dashboard 설정 변경은 버전이 붙은 config 참조를 사용하며 stack을 다시 배포해야 적용됩니다. 배포된 원본은 파일로 관리하며 별도 대시보드를 저장하려면 관리자 로그인 후 복사본을 사용하십시오. [Grafana provisioning 문서](https://grafana.com/docs/grafana/latest/administration/provisioning/)

```bash
# 수집 대상 상태 확인
curl http://control-node:9090/api/v1/targets

# manager에서 서비스 상태와 로그를 확인합니다.
sh scripts/swarm.sh status
sh scripts/swarm.sh logs prometheus
sh scripts/swarm.sh logs grafana
```

Controller target도 브라우저에서 열 수 있는 주소를 사용합니다. `GET /api/v1/prometheus/controller-targets`는 control 노드의 Swarm 주소와 게시된 `KPL_HTTP_PORT`를 반환합니다. 배포 helper는 매 배포에서 `KPL_CONTROLLER_METRICS_URL`과 `KPL_PROMETHEUS_EXTERNAL_URL`을 생성하며 사용자 지정 포트와 IPv6를 반영합니다. Prometheus 자체 링크에는 후자를 [`--web.external-url`](https://prometheus.io/docs/prometheus/latest/command-line/prometheus/)로 전달합니다. 탐색 요청과 Grafana datasource는 내부 DNS를 사용합니다. Prometheus 컨테이너에서 control 노드의 게시된 HTTP 포트에 접근할 수 있어야 합니다. Controller metrics URL이 미설정이면 target 목록은 비어 있습니다.

Dashboard는 몰려오는 telemetry를 초당 최대 4회 화면 갱신으로 병합하고, 동일한 텍스트·목록·토폴로지 요소를 유지합니다. 지표 갱신은 펼친 카드와 가로 스크롤 위치를 유지하면서 수치를 업데이트합니다.

Swarm stack은 `GET /api/v1/prometheus/agent-targets`에서 등록된 target을 탐색합니다. `sh scripts/swarm.sh access`로 광고 주소를 확인하고, 설정한 포트가 선택된 모든 Agent 노드에서 비어 있으며 control 노드에서 TCP 접근이 허용되는지 확인하십시오. 운영자가 Agent metrics 링크를 직접 열 때에는 브라우저가 속한 신뢰 관리망에서도 접근을 허용하고 신뢰하지 않는 출발지는 차단하십시오. `up{job="kpl-agent"}`를 보면 등록되었지만 방화벽이나 잘못된 Swarm `NodeAddr` 때문에 접근할 수 없는 target을 성공한 scrape와 구분할 수 있습니다.

현재 이미지 버전은 Prometheus `v3.13.2`와 Grafana `13.2.1`로 고정했습니다. 업데이트 시 공식 [Prometheus 다운로드](https://prometheus.io/download/)와 [Grafana Docker 설치 문서](https://grafana.com/docs/grafana/latest/setup-grafana/installation/docker/)를 참고하고 설정·대시보드를 재검증하십시오.

[Bandwidth 측정](bandwidth.kr.md) · [v2 전체 분석 대조](v2-analysis-coverage.kr.md)

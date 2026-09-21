# REST API

[English](api.md) | 한국어

Controller의 대시보드와 운영 API는 로그인 세션으로 보호합니다. 아래 [인증](#인증)에서 공개 모니터링 경로와 내부 서비스 인증을 구분합니다.

## Controller endpoint

| Method | Path | 설명 |
|---|---|---|
| `POST` | `/api/v1/auth/login` | JSON 계정으로 로그인하고 세션 쿠키 발급 |
| `GET` | `/api/v1/auth/session` | 현재 사용자·세션 만료 시각 |
| `POST` | `/api/v1/auth/logout` | 현재 세션과 SSE 연결 종료 |
| `GET` | `/metrics` | Controller와 실험 metric의 Prometheus exposition |
| `GET` | `/api/v1/health` | Controller 상태와 Peer 시계 측정에 쓰는 현재 UTC 시각 |
| `GET` | `/api/v1/ui-config` | 대시보드 메뉴에 사용하는 Prometheus·Grafana 게시 포트 |
| `GET` | `/api/v1/prometheus/controller-targets` | Controller 공개 metrics 주소의 Prometheus HTTP service-discovery group |
| `GET` | `/api/v1/prometheus/agent-targets` | metrics URL을 알린 online Agent의 Prometheus HTTP service-discovery group |
| `GET` | `/api/v1/snapshot` | 노드 `peerScores`를 포함한 대시보드 전체 snapshot |
| `GET` | `/api/v1/agents` | Agent 상태 |
| `GET` | `/api/v1/nodes` | inspection으로 수집한 `peerScores`를 포함한 Peer 상태 |
| `GET` | `/api/v1/network` | `peerScores`가 포함된 Peer, 연결 edge와 전파 지표 |
| `GET` | `/api/v1/bootstrap?runId={runId}` | 필수 run ID에 속한 준비 상태의 bootstrap peer만 반환 |
| `GET` | `/api/v1/discovery?runId={runId}&topic={topic}&requesterNodeId={nodeId}` | 동일 run·정확한 topic에서 ready·online 상태인 PubSub transport 후보 |
| `GET` | `/api/v1/events` | 최근 trace event |
| `GET` | `/api/v1/stream` | `peerScores`를 포함한 실시간 snapshot SSE |
| `GET` | `/api/v1/experiments` | 실험 상태와 `activeJobs`, `completedJobs`, `failedJobs`, `canceledJobs` counter |
| `GET` / `POST` | `/api/v1/scenarios` | 시나리오 요약 목록 조회 또는 검증한 `{name, yaml}` 저장 |
| `POST` | `/api/v1/scenarios/validate` | 원본 YAML을 저장·실행 없이 검증. 로그인 필요 |
| `GET` / `PUT` / `DELETE` | `/api/v1/scenarios/{id}` | 저장 시나리오 하나를 불러오기, 갱신 또는 삭제 |
| `GET` | `/api/v1/results` | 이전 Controller 실행에서 저장한 실험을 포함하는 결과 목록 |
| `DELETE` | `/api/v1/results/{id}` | 비활성 저장 결과 삭제. 진행 중 배치·다운로드 보호 |
| `DELETE` | `/api/v1/result-batches/{batchId}` | 그룹의 모든 저장 run과 평균 분석 삭제. 전체 구성원의 활성 상태·다운로드를 먼저 검사 |
| `POST` | `/api/v1/result-batches/{batchId}/resume` | 실패 또는 시작 후 interrupted 회차를 제외하고 미시작 회차만 재개. 배치·회차 ID 유지. 첫 queued run과 `202`, 진행 중·재개 불가 시 `409`, 없는 배치면 `404`. 로그인 세션과 `X-KPL-Request: dashboard` 필요. |
| `POST` | `/api/v1/result-batches/{batchId}/retry` | 이전 Peer를 정리한 뒤 저장된 미완료 회차를 첫 phase부터 재실행. 완료 회차·배치 ID·회차 번호·seed 유지, 새 run ID 사용. 실패·중단·취소, 단일 실험과 마지막 회차 실패 포함. 첫 queued run과 `202`, 진행 중·재개 불가 시 `409`, 없는 배치면 `404`. 로그인 세션과 `X-KPL-Request: dashboard` 필요. |
| `GET` | `/api/v1/experiments/{id}/analysis` | 저장 이벤트·관측치를 분석한 그래프 데이터와 집계 JSON |
| `GET` / `POST` | `/api/v1/analysis-jobs/{id}` | 백그라운드 분석 상태 조회 / 접수. 중복 요청 재사용, `?refresh=1`로 새 snapshot 분석 |
| `GET` / `HEAD` | `/api/v1/analysis-jobs/{id}/result?jobId={jobId}` | 서버에 보관된 완료 분석 JSON 다운로드. 작업 미완료·다른 attempt는 `409` |
| `GET` / `HEAD` | `/api/v1/analysis-jobs/{id}/summary?jobId={jobId}` | 비교용 경량 분석 JSON. 메시지별 경로와 원본 시계열을 제외하고 집계·분포·적합 결과를 반환 |
| `GET` / `POST` | `/api/v1/batch-analysis-jobs/{batchId}` | 동일 반복 실험의 통합 평균 작업 상태 조회 / 시작. `?refresh=1`은 새 로그 경계로 갱신 |
| `GET` / `HEAD` | `/api/v1/batch-analysis-jobs/{batchId}/result?jobId={jobId}` | 저장된 통합 평균과 개요용 run별 입력 다운로드 |
| `GET` / `HEAD` | `/api/v1/experiments/{id}/download` | 시나리오·메타데이터·이벤트·선택적 관측 파일·파생 지표를 ZIP으로 다운로드하거나 응답 본문 없이 크기 계산 |
| `POST` | `/api/v1/experiments` | YAML 1회 실행 또는 JSON `{scenario, repetitions}`로 1~100회 순차 실행 |
| `POST` | `/api/v1/experiments/{id}/stop` | 실행을 취소한 뒤 제한 시간 내 job 종료와 generation-fenced Peer cleanup 수행 |

`/api/v1/bootstrap`의 `runId` query parameter는 필수입니다. registry는 해당 run에서 준비 상태이고 유효한 identity와 address 정보가 있는 `boot` 노드만 반환하므로 동시에 실행되는 실험끼리 bootstrap peer를 발견하지 않습니다. `/api/v1/discovery`에는 표시된 query 세 개가 모두 필요하며 요청자 자신을 제외하고 실제 전달·mesh 결과가 아닌 설정상 topic 참가자를 반환합니다. `/api/v1/prometheus/agent-targets`는 기본 Swarm Prometheus 설정이 사용하는 읽기 전용 운영 endpoint이며 offline Agent와 유효한 metrics URL이 없는 Agent는 제외합니다.

Bootstrap 응답은 `{nodeId, peerId, addresses}` 항목 배열이며 비어 있으면 `null`입니다. Discovery와 달리 bootstrap은 Agent online 상태로 필터링하지 않습니다. Discovery는 후보가 없을 때 빈 배열을 반환하며 각 항목에는 `subscribed` flag도 포함합니다. 조건에 맞는 전체 후보를 반환하고, 요청하는 Peer가 [토폴로지](topology.kr.md#bootstrap과-topic-discovery)에 설명한 rendezvous 순위, connection budget과 retry를 적용합니다.

`/api/v1/prometheus/controller-targets`는 Controller의 `--metrics-url` (`KPL_CONTROLLER_METRICS_URL`)을 광고하며 미설정 시 `[]`를 반환합니다. 인증 없이 조회하는 읽기 전용 endpoint입니다. URL은 HTTP(S)의 `/metrics` 경로여야 하며 인증정보·query·fragment·loopback/unspecified 주소를 허용하지 않습니다. Swarm helper가 control 노드 주소와 `KPL_HTTP_PORT`로 자동 설정합니다.

## 실행 제출, 중지와 관측

`POST /api/v1/scenarios/validate`는 원본 YAML 본문(`Content-Type: application/yaml`)을 최대 1 MiB까지 받습니다. 유효하면 `200`과 `{valid: true, name, phases}`를 반환합니다. 빈 입력, YAML 문법 오류, 알 수 없는 필드, 잘못된 설정, 여러 YAML 문서는 `400`과 `{error: "…"}`를 반환하며 파서가 제공하는 줄 번호를 보존합니다. 본문 한도 초과는 `413`입니다. 저장·실행과 같은 파서를 사용하지만 기록이나 job을 만들지 않으며 로그인 세션을 사용합니다. 설정 검증이며 Agent 용량이나 실행 시 연결성을 검사하지는 않습니다.

`POST /api/v1/experiments`는 첫 run의 experiment 객체와 `202`를 반환합니다. `{scenario, repetitions}`를 제출하려면 `Content-Type: application/json`을 사용하십시오. `scenario`는 YAML 문자열이며 `repetitions`를 생략하면 기본값은 `1`입니다. 다른 content type은 원시 YAML로 처리합니다. 원시 YAML과 JSON에서 해석한 `scenario` 문자열의 한도는 각각 1 MiB입니다. JSON 요청 본문은 escape를 고려해 `6 * 1 MiB + 64 KiB`까지 허용합니다. 요청 본문 자체가 한도를 넘으면 `413`, 해석한 YAML 문자열이 한도를 넘거나 scenario/repetition이 유효하지 않으면 `400`입니다.

반복 제출은 매 iteration에 별도 run ID와 결과 기록을 예약하고 `batchId`, `iteration`, `repetitions`를 공유합니다. 순차 실행하며 실패하거나 취소된 iteration은 대기 중인 나머지 실행도 취소합니다. 반복 배치가 진행 중일 때 구성원 하나를 중지하면 해당 배치를 취소합니다. `repetitions > 1`에서는 마지막을 포함한 매 iteration이 최종 상태를 기록하기 전에 Peer를 fence하고 제거합니다. 자연스럽게 성공한 단일 실행만 YAML에 `stop-all`이 없을 때 Peer를 남겨둘 수 있습니다.

`POST /api/v1/result-batches/{batchId}/retry`는 저장된 미완료 회차를 회차 순서대로 수동 재실행합니다. 예를 들어 10회 중 4회차가 실패했다면 완료된 1~3회차는 유지하고 4~10회차를 각 회차의 첫 phase부터 다시 실행합니다. 뒤쪽 회차 중 이미 완료된 회차도 유지합니다. 실패·중단(재시작 후 미시작으로 남은 기록 포함)·취소 기록을 대상으로 합니다. 시나리오와 seed를 유지하면서 새 run ID를 부여하며 experiment/result 메타데이터의 `previousRunIds`에 이전 시도 ID를 기록합니다. 기존 로그는 **Previous attempts**에서 다운로드할 수 있고, 회차별 가장 최근의 저장된 시도만 배치 진행률과 평균에 반영합니다. 기존 `/resume`은 시도한 회차를 건너뛰고 미시작 잔여 회차만 재개합니다.

두 이어하기 모드는 접수한 HTTP 요청과 독립적으로 Controller에서 실행합니다. 새 회차 실행 전에 이전 Peer를 fence·제거하고 Agent 상태를 갱신합니다. 정리가 실패하거나 정리 중 사용자가 정지하면 대기 회차를 `canceled`로 기록하고 experiment 메타데이터에 이유를 남기며 새 회차는 시작하지 않습니다. Agent·정리 오류를 해결한 뒤 다시 이어하기를 실행할 수 있습니다. 실행·최종 정리·다운로드·개별 및 배치 분석이 진행 중이면 접수를 거절하며, 완료된 회차 기록은 덮어쓰지 않습니다.

`GET /api/v1/experiments`, Dashboard snapshot/SSE와 최초 제출 응답의 실시간 experiment 객체에는 `timing`이 포함될 수 있습니다. 필드는 `estimatedFinishAt`(UTC), 음수가 아닌 `remainingSeconds`, `basis`(`scenario` 또는 `observed-runs`), `observedRuns`, `overdue`입니다. 진행 중인 반복 Run에는 `batchEstimatedFinishAt`, `batchRemainingSeconds`, `batchOverdue`도 포함됩니다(0·false인 선택 필드는 생략 가능). 대기 Run의 종료 시각에는 같은 배치의 선행 Run 시간이 포함됩니다. 완료·실패·취소된 Run이나 시간 정보가 부족한 경우 `timing`을 생략하며 결과 manifest에 저장하지 않습니다. 계산 방식과 불확실성은 [예상 종료 시각](scenario-library.kr.md#예상-종료-시각)을 참고하십시오.

중지 endpoint는 cleanup 완료 전, 취소 요청을 접수하면 `202`와 JSON `{"runId":"run-id","status":"stopping"}`을 반환합니다. Dashboard는 snapshot에서 실행·대기 중인 구성원이 없음을 확인할 때까지 해당 실행 또는 배치의 버튼을 **Stopping…**으로 비활성화하며, 접수 응답만으로 버튼을 다시 활성화하지 않습니다. `/api/v1/experiments` 또는 snapshot에서 최종 상태를 확인하십시오. 취소 handle이 더 이상 없는 run은 `404`입니다. SSE는 최초 `event: snapshot`, 상태 변경을 최대 초당 한 번으로 합친 전체 snapshot(클라이언트 간 인코딩 공유), 이벤트가 없어도 15초마다 전체 snapshot를 보내며 event ID 기반 replay는 제공하지 않습니다. `/api/v1/events`는 현재 Controller 상태에서 가장 최근 event 최대 300개를 포함합니다.

저장소 루트에서 실행합니다. 호스트는 `access`가 표시한 Controller 주소로 바꾸고, 먼저 [인증](#인증) 절차로 `KPL_COOKIE_JAR`를 생성하십시오.

```bash
curl -X POST http://control-node:8080/api/v1/experiments \
  -H 'Content-Type: application/yaml' \
  -b "${KPL_COOKIE_JAR:?Log in first}" -H 'X-KPL-Request: dashboard' \
  --data-binary @examples/smoke.yaml
```

시나리오 라이브러리 endpoint는 재사용할 편집기 입력을 실험 결과와 별도로 저장합니다. 목록에서는 YAML을 제외하고, 개별 GET·POST·PUT 응답에는 포함합니다. UI 사용 순서, payload, 검증 제한과 저장 위치는 [시나리오 라이브러리 안내](scenario-library.kr.md)를 참고하십시오.

typed 대역폭 표본을 포함한 원시 이벤트는 `<data-dir>/runs/<run-id>/events.jsonl`, 실행 입력은 같은 디렉터리의 `scenario.yaml`, 실험 메타데이터는 `experiment.json`, 수집한 그래프 표본·토폴로지·점수 요약은 선택적 `observations.jsonl`에 저장됩니다. 로컬 Controller의 기본 data 디렉터리는 `data`입니다. Swarm에서는 영구 `controller-data` 볼륨을 `/var/lib/kpl/data`에 마운트하며, 실험별 파일은 그 아래 `runs/<run-id>`에 저장됩니다.

대시보드의 **Download results**로 실험 결과를 ZIP으로 받을 수 있습니다. **Saved results**에는 이전 Controller 실행에서 보존된 결과도 표시되며, **Refresh**로 목록을 다시 읽습니다. 실행 중 실험의 **Download snapshot**은 다운로드 시작 시점까지 저장된 기록을 담습니다. 최근 300개 이벤트 버퍼와 별개로 저장된 전체 이벤트 로그를 내보냅니다. 파일 구성과 수집 한계는 [실험 결과 다운로드](monitoring.kr.md#실험-결과-다운로드)를 참고하십시오.

`GET /api/v1/results`의 `sourceBytes`는 마지막 조회 시점에 저장된 시나리오·실험 메타데이터·이벤트·관측 원본 파일의 압축 전 바이트 합계입니다. 실행·대기 중 결과에도 제공하며 로그 본문을 읽거나 ZIP을 생성하지 않습니다. 분석 캐시와 생성 이미지는 제외합니다. 안전하게 파일 크기를 조회할 수 없을 때만 생략합니다. 화면은 이 값을 사용하며 자동 ZIP 크기 측정을 요청하지 않습니다.

`DELETE /api/v1/results/{id}`는 삭제 성공 시 `204`, 결과가 없으면 `404`, 실행·배치가 활성 상태이거나 실제 `GET` 다운로드가 결과를 사용 중이면 `409`를 반환합니다. 직접 요청한 `HEAD` 크기 계산과 목록 조회는 다운로드 충돌로 처리하지 않습니다.

`DELETE /api/v1/result-batches/{batchId}`는 로그인 세션과 정확한 배치 ID를 사용합니다. 파일을 삭제하기 전에 전체 구성원을 검사하며 실행·정리 중인 run이나 구성원의 ZIP 다운로드가 있으면 `409`를 반환합니다. 실패·취소 run을 포함한 모든 저장 구성원, 개별 분석, 별도 통합 평균을 제거합니다. 성공 시 `200`과 `{"deletedIds":["run-id", "..."]}`를 반환하며 그룹이 없으면 `404`입니다. 원본 없이 남은 통합 평균도 제거할 수 있습니다. 저장 장치 오류는 `500`이며 일부 삭제가 끝났을 수 있으므로 목록을 갱신하고 저장 오류를 해결한 뒤 재시도하십시오. 브라우저는 요청에 30초 제한 시간을 적용하며 그 이후에도 Controller에서 완료될 수 있습니다.

## 백그라운드 분석

저장 실행 하나를 분석하려면 `POST /api/v1/analysis-jobs/{id}`로 접수하고 같은 경로의 `GET`으로 상태를 확인합니다. POST와 GET/HEAD 모두 로그인 세션이 필요합니다. 접수된 대기·실행 작업은 `202`, 현재 버전의 완료 결과를 재사용하면 `200`입니다. Controller 전체에서 대기·실행 작업을 최대 32개 접수하고(가득 차면 `503`), 공유 분석 슬롯은 한 번에 하나씩 계산합니다. 클라이언트를 닫아도 접수된 작업은 취소되지 않습니다.

상태 `state`는 `idle`, `queued`, `running`, `completed`, `failed`, `canceled`, `interrupted`입니다. `idle`은 해당 저장 실행에 요청한 분석이 없다는 뜻입니다. 실패·취소·중단 작업은 다시 접수할 수 있습니다. 재시작 후 복구한 미완료 기록은 `interrupted`이며 정상 종료 과정에서 이미 `canceled`를 저장했을 수도 있습니다. 별도 작업 취소 endpoint는 없습니다. 삭제 가능한 저장 결과를 지우면 분석 작업도 취소하고 분석 파일을 제거합니다.

| 상태 필드 | 의미 |
|---|---|
| `id`, `runId` | 분석 시도 ID와 원본 실행 ID. 재시도·갱신으로 새 시도가 생길 수 있음 |
| `version`, `analysisVersion` | 상태·응답 형식 버전 `1`, 현재 작업의 계산 버전 `3`. 지표의 `definition`, 로그의 `rpcMetadataVersion`과 별개 |
| `phase`, `processedBytes`, `totalBytes`, `progress` | 현재 단계와 로그 읽기 바이트 진행률(0–100). 읽기 100% 뒤에도 그래프·전파 계산과 저장이 남으므로 `state: completed`를 확인 |
| `createdAt`, `startedAt`, `updatedAt`, `finishedAt` | 작업 시각. 아직 도달하지 않은 단계의 시각에는 의미가 없음 |
| `snapshotAt` | POST 접수 시점이 아니라 worker가 분석 슬롯을 얻은 뒤 잡은 원본 경계 |
| `error`, `resultUrl` | 실패 내용 또는 시도 ID가 포함된 완료 전체 분석 URL |

단일·통합 분석은 `phase: saving` 동안에도 `running` 상태이며, 결과 파일을 저장하는 중에 상태 조회·실험 이벤트 수신·중단 요청 처리를 계속할 수 있습니다. 완료 결과를 게시하기 전에 현재 분석 시도인지와 취소 여부를 다시 확인하고, 개별 분석은 원본 삭제 여부도 확인합니다.

기존 대기·실행 작업은 `?refresh=1`이어도 재사용합니다. 현재 버전의 완료 결과는 refresh 때 새로 계산하고, 오래된 `analysisVersion`의 완료 결과는 POST 때 재생성합니다. GET 상태 조회만으로는 갱신하지 않습니다. **Images**가 이 버전 검사를 자동 수행합니다. 새 분석은 새 경계를 사용하며 과거에 없던 메타정보는 여전히 복원되지 않습니다.

완료 후 `GET` 또는 `HEAD` `/api/v1/analysis-jobs/{id}/result?jobId={jobId}`는 전체 JSON, `/summary?jobId={jobId}`는 비교용 자료를 반환합니다. `jobId`를 지정하면 다른 분석 시도를 내려받지 않도록 확인하며 생략하면 현재 완료 시도를 선택합니다. 미완료·시도 불일치는 `409`, 실행 부재는 `404`, 잘못되거나 읽을 수 없는 저장 자료는 보통 `422`입니다. 경량 응답은 `observations`, `timeline`, `bandwidthTimeline`, `research.messages`를 빈 배열로 두고 `messageCount`·지표·집계·분포·적합 결과를 유지합니다. 개별 메시지 경로나 원본 시계열 용도로 사용할 수 없습니다. 두 응답 모두 `analysisId`, `analysisVersion`, 원본 경계 `asOf`를 포함합니다.

`GET /api/v1/experiments/{id}/analysis`는 2분 요청 제한의 동기 호환 경로입니다. 보존되는 백그라운드 작업을 만들지 않고 응답을 계산합니다. 긴 분석과 나중 다운로드에는 job API를 사용하십시오. 연구 정의는 [지표 가이드](experiment-metrics.kr.md#저장-결과-연구-지표), 보존 파일은 [모니터링](monitoring.kr.md#분석-파일과-이미지-보존), 브라우저 조작은 [시각화](visualization.kr.md)에서 관리합니다.

## 반복 실험 통합 분석

`POST /api/v1/batch-analysis-jobs/{batchId}`는 실행 제출에 기록된 동일 배치의 완료 run을 분석합니다. 모든 배치 구성원의 실행·정리 완료와 최소 2개의 완료 run이 필요하며, 실행 중이면 `409`, 유효 완료 run 부족·메타데이터 불일치는 `422`, 배치가 없으면 `404`입니다. 인증·중복 접수·재시도·재시작 처리는 개별 분석과 같고, 개별·배치 작업이 합쳐서 최대 32개의 대기·실행 작업을 공유합니다. 최대 100회 반복을 하나의 배치 작업으로 순차 처리합니다.

상태의 배치 식별자는 `batchId`이며 `runId`는 비어 있습니다. 선택한 `runIds`, `completedRuns`, `totalRuns`, `expectedRuns`, 구성원 서명 `membership`도 제공합니다. `progress`는 run별 동일 가중 진행률이고 바이트 필드는 현재 처리 중인 run에 해당합니다. `snapshotAt`은 가장 최근에 캡처한 run의 경계이며 결과의 각 `runs[].asOf`로 개별 경계를 확인합니다. 구성원·완료 상태 변경 시 상태 조회는 기존 완료 캐시를 `idle`로 표시하고 새 POST가 재계산합니다. 로그 내용만 변경되면 `?refresh=1`을 사용합니다. 계산·저장 중 배치 구성원이 바뀌면 이전 구성으로 완료 결과를 게시하지 않고 작업을 실패 처리합니다. 현재 구성원으로 다시 분석하려면 재시도하십시오.

결과는 `version: 1`, `analysisVersion: 3`, `aggregation: "equal-run-mean-v1"`, 배치·시도 ID, `expectedRuns`, `missingRuns`, `excluded`, `runs`, `summary`를 제공합니다. `summary`는 `metrics.*`, `research.*`, `bandwidth.sentBytes`, `bandwidth.receivedBytes`별 `average`, `deviation`(run 간 표본 SD), `median`, `count`(유효 run 수)입니다. 결측 평균은 `null`, 유효 run 1개의 SD도 `null`입니다. `runs`는 그래프 지표·관측·대역폭·분포와 메시지 시계열/수신 곡선/기원 수를 담은 `research.overview`를 보존하고 원시 메시지·노드 경로를 생략합니다. 평균 차트의 축 정렬·분포 규칙은 [통합 평균 사용법](visualization.kr.md#같은-실험의-반복-run-통합-평균)을 참고하십시오.

완료 파일은 `<data-dir>/batch-analyses/{batchId}/job.json`, `result.json`에 저장됩니다. 창 종료와 서버 재시작 후 재사용할 수 있습니다. 명시적인 `jobId` 다운로드는 해당 완료 스냅샷에 고정되며 다른 시도로 교체됐으면 `409`입니다. 개별 원본 삭제는 기존 통합 스냅샷 파일을 지우지 않습니다. 변경된 구성으로 다시 분석하면 현재 파일을 교체하므로 이전 결과가 필요하면 먼저 다운로드하십시오.

## 내부 cluster endpoint

이 REST/JSON endpoint는 컴포넌트 사이 통신에 사용합니다. 등록, heartbeat와 전달 event는 **Controller로**, lifecycle 명령은 **Agent로** 전송합니다.

| 호출자 → 서버 | Method | Path | 목적 |
|---|---|---|---|
| Agent → Controller | `POST` | `/api/v1/agents/register` | Agent instance 등록 |
| Agent → Controller | `POST` | `/api/v1/agents/heartbeat` | Agent와 Peer snapshot 보고 |
| Agent → Controller | `POST` | `/api/v1/events/batch` | batch당 최대 5000개 event 전달 |
| Controller → Agent | `GET` | `/api/v1/status` | Agent와 Peer 상태 갱신 |
| Controller → Agent | `POST` | `/api/v1/nodes` | `CreateNodeRequest`로 Peer 생성 |
| Controller → Agent | `DELETE` | `/api/v1/nodes` | 기존 run을 fence 처리하고 남은 Peer와 대기 telemetry를 모두 정리한 뒤 `204` 반환. Controller 종료 시 사용 |
| Controller → Agent | `DELETE` | `/api/v1/nodes/{nodeId}` | Peer 하나의 종료 요청 |
| Controller → Agent | `POST` | `/api/v1/nodes/{nodeId}/publish` | publish 요청 중계 |
| Peer → Agent | `POST` | `/api/v1/nodes/{nodeId}/status` | 해당 Peer의 최신 상태 보고 |
| Peer → Agent | `POST` | `/api/v1/telemetry` | telemetry batch 제출 |
| Agent → Peer | `GET` / `POST` | `/health` / `/publish` | Peer HTTP API로 readiness 확인 또는 publish |

정기 Agent heartbeat는 `partial: true`를 사용하며 Peer 기록을 JSON 본문 최대 10 MiB씩 나누어 보냅니다. Peer 기록 하나가 한도를 넘으면 Agent는 해당 노드 ID가 포함된 오류를 보고하고 그 기록을 수신 미확인 상태로 유지합니다. 실행 중이거나 실패한 Peer와 Controller 수신 확인을 기다리는 정상 종료 Peer를 포함합니다. 수신 확인된 정상 종료 기록은 이후 정기 보고에서 생략하고 재등록 뒤 다시 전송합니다. 부분 보고에서 빠진 노드를 종료하거나 그 capacity를 해제하지 않습니다. `GET /api/v1/status`와 `partial`이 없거나 false인 heartbeat는 정상 종료 축약 기록을 포함한 전체 인벤토리 계약을 유지합니다. Agent는 전체 인벤토리를 메모리에 보존하고 명시적 상태 조회에 제공합니다.

telemetry 요청은 이벤트 5000개와 JSON 본문 10 MiB로 제한됩니다. Peer와 Agent는 이스케이프와 envelope를 포함한 인코딩 바이트 수로 batch를 나누며, 재시도에도 원본 순서와 이벤트 식별자를 유지합니다. 수락된 앞부분은 제거한 후 나머지를 전송합니다. 두 수신자는 admission 전에 모든 이벤트를 검사합니다. 단일 이벤트가 인코딩된 10 MiB batch에 들어갈 수 없으면 `413`을 반환하며 해당 요청의 이벤트는 하나도 수락하지 않습니다. Agent는 자신의 식별자로 정규화한 뒤 검사합니다. Controller에 직접 제출한 이벤트도 이 검사를 거치므로 저장 분석에서 읽을 수 없는 과대 로그 행을 만들지 않습니다.

Agent 정상 종료 또는 `DELETE /api/v1/nodes`의 telemetry drain은 마지막 Peer 이벤트를 포함해 대기 큐와 이미 전송 중인 batch 모두 Controller의 수신 확인을 받을 때까지 기다립니다. 실패한 batch는 순서를 유지해 재시도하며, 제한 시간 내 수신 확인을 완료하지 못하면 drain 실패를 보고합니다.

JSON 본문에는 값 하나만 있어야 합니다. 본문 한도 내의 후행 공백은 허용하며, 두 번째 JSON 값·후행 쓰레기 데이터·디코더 본문 한도 초과는 `400`을 반환합니다. Controller/Agent의 일반 JSON handler는 10 MiB, Peer `/publish`는 1 MiB 한도를 사용합니다. 시나리오 요청 envelope에는 위에서 설명한 별도 한도가 적용됩니다. Peer 내부에서 생성된 이벤트가 JSON으로 인코딩되지 않거나 단일 batch 한도를 넘으면 로그와 `telemetry_drop`에 유실 수를 남기고, 소스 sequence의 빈 번호를 유지한 채 후속 이벤트를 전송합니다. 네트워크 실패 시에는 대기 중인 batch를 보존해 재시도합니다.

Agent는 Controller가 cleanup에 사용하는 다음 endpoint도 제공합니다.

| Method | Agent path | 설명 |
|---|---|---|
| `DELETE` | `/api/v1/runs/{runId}/nodes?generation=N` | unsigned `generation`이 필수이며, run fence를 N까지 원자적으로 높이고 이후 generation N 이하의 create를 거부하며 해당 generation의 기존 노드를 종료한 뒤 `202 Accepted`를 반환합니다. |

내부 endpoint는 운영자 API와 별개로 변경될 수 있습니다. 요청과 snapshot의 필드 정의는 [`internal/model/model.go`](../internal/model/model.go), 대역폭 타입은 [`internal/model/bandwidth.go`](../internal/model/bandwidth.go), handler는 [`internal/controller/api.go`](../internal/controller/api.go)와 [`internal/agent/api.go`](../internal/agent/api.go)에 있습니다.

## 상세 Peer 로그

새 Peer는 `events.jsonl`에 upstream libp2p에서 실제 제공하는 메타정보를 추가합니다. 기존 `publish`/`deliver`/`duplicate`와 메인 Metrics 집계는 유지됩니다. `messageId`는 envelope의 애플리케이션 ID 또는 raw의 `pubsub-<hex native ID>`이며, `fields.pubsubMessageId` 및 아래 상세 ID 목록은 pubsub wire ID의 hex 표현입니다.

| 이벤트 / 필드 | 의미 |
|---|---|
| `send_*`, `recv_*`, `drop_*`의 `rpcMetadataVersion`, `rpcObservationId` | 메타정보 형식 버전 `1`, 한 로컬 RPC 콜백의 연결 ID. 각 이벤트의 `eventId`는 별도로 유지 |
| `rpcMessageCount`, `rpcSubscriptionCount` | 같은 RPC에 포함된 데이터 메시지·구독 항목의 전체 개수 |
| IHAVE의 `topicMessageIds` | 토픽별 광고 메시지 ID 목록 |
| IWANT/IDONTWANT의 `messageIds` | 요청·비요청 메시지 ID 목록. 해당 프로토콜 항목에는 원래 토픽이 없음 |
| `messageIdsComplete`, `omittedMessageIds` | ID 목록 완전성 및 제한으로 생략한 개수. `messageIdCount`는 전체 개수 |
| PRUNE의 `peerExchangeIdsByTopic`, `peerExchangeIdsComplete`, `omittedPeerExchangeIds` | 실제 trace가 제공한 토픽별 교환 Peer ID 및 목록 완전성 |
| `rpc_metadata`의 `direction`, `messages`, `subscriptions` | send/recv/drop 구분, `{topic,pubsubMessageId}` 목록, `{topic,subscribe}` 목록. 데이터·구독 메타정보가 있을 때만 별도 이벤트 생성 |
| `subscriptionsComplete`, `omittedSubscriptions` | 구독 목록 완전성 및 생략 개수 |
| `pubsub_reject`의 `reason`, `pubsubMessageId` | libp2p가 보고한 메시지 거부 사유와 ID |
| `timestampSource`, `sourceTimestamp` | trace 시각의 출처와 보정 전 원본 시각. upstream 시각이 없으면 `peer-clock`으로 표시하고 원본 시각을 만들지 않음 |
| `clockBasis`, `clockOffsetMs`, `clockUncertaintyMs` | 유효한 Controller 동기화 근거가 있을 때 추가하는 시각 보정 정보 |

ID 상세는 종류별 최대 8,192개 및 hex 합계 512KiB, 구독 목록은 최대 8,192개입니다. 상세 목록이 잘려도 해당 이벤트의 RPC·entry·ID 집계는 전체 개수를 유지합니다. 이벤트 유실은 기존 `telemetry_drop`과 소스 sequence로 추적합니다. 원본 payload, IWANT 발신 원인, wire에 없는 RPC 전역 ID는 만들지 않습니다. Eager/Lazy는 [지표 문서](experiment-metrics.kr.md#eager-push와-lazy-pull-추정)의 메타정보 추정 규칙을 적용합니다.

## 인증

Controller와 모든 Agent에 같은 `KPL_USER`·`KPL_PASSWORD`를 설정하며 시작 시 둘 다 필수입니다. 대시보드는 먼저 **Log in** 화면을 열고, 로그인 성공 시 임의의 서버 세션을 생성해 HttpOnly·SameSite=Strict 쿠키로 식별합니다. HTTPS에서는 Secure 쿠키를 사용하며 신뢰할 수 있는 reverse proxy의 `X-Forwarded-Proto: https`도 지원합니다. 세션은 12시간 뒤 만료되고 **Log out** 또는 Controller 재시작 시 무효화됩니다. 로그아웃·만료 시 해당 세션의 SSE 연결도 닫지만 실행 중 실험과 백그라운드 분석은 계속됩니다. 설정 계정 하나가 대시보드 전체 권한을 가집니다.

대시보드/API 조회·변경, SSE, 시나리오 검증, 분석, 다운로드는 모두 로그인 세션이 필요합니다. 미인증 API는 `401`, 대시보드는 `/login` 리다이렉트를 반환합니다. 세션을 사용하는 변경 요청은 `X-KPL-Request: dashboard` 헤더도 필요하며 다른 origin의 요청은 거부합니다. UI는 이 헤더를 자동으로 넣으므로 추가 토큰 입력이 없고 비밀번호·세션 쿠키를 localStorage에 저장하지 않습니다. 로그인은 JSON `{user, password}`만 받으며 잘못된 계정은 `401`, 같은 접속 IP에서 5분 동안 10회 실패한 이후에는 `429`를 반환합니다. 세션과 실패 기록은 각각 최대 1,024개입니다.

로그인 리소스, health/시각 endpoint, `/metrics`, 두 Prometheus target-discovery endpoint만 공개로 유지합니다. 내부 Agent 등록·heartbeat·이벤트와 Peer bootstrap·discovery는 계정 설정에서 용도를 분리해 자동 생성한 서비스 키를 사용합니다. Peer에는 로그인 비밀번호 대신 이 키만 전달하며, 키로 대시보드 결과 API에 접근하거나 실험을 시작할 수 없습니다. `KPL_API_TOKEN`과 `--token` CLI 옵션은 폐기했습니다. 자격 증명 변경 시 이전 키를 쓰는 진행 중 실험·잔존 Peer를 정리한 뒤 Controller와 Agent를 함께 재배포하십시오. Grafana 로그인은 별도입니다.

REST 클라이언트는 아래 주소를 `access` 출력으로 바꾸고 한 번 로그인합니다. 비공개 쿠키 파일을 위 예제들에서 재사용하십시오.

```bash
KPL_URL=http://control-node:8080
KPL_COOKIE_JAR=$(mktemp)
read -r -p 'Username: ' KPL_USER
read -r -s -p 'Password: ' KPL_PASSWORD; printf '\n'
export KPL_USER KPL_PASSWORD
python3 -c 'import json,os; print(json.dumps({"user":os.environ["KPL_USER"],"password":os.environ["KPL_PASSWORD"]}))' |
  curl --fail-with-body -c "$KPL_COOKIE_JAR" \
    -H 'Content-Type: application/json' --data-binary @- "$KPL_URL/api/v1/auth/login"
unset KPL_PASSWORD
curl --fail-with-body -b "$KPL_COOKIE_JAR" "$KPL_URL/api/v1/snapshot"
```

요청을 마친 뒤 로그아웃하고 쿠키 파일을 삭제합니다.

```bash
curl --fail-with-body -b "$KPL_COOKIE_JAR" -H 'X-KPL-Request: dashboard' \
  -X POST "$KPL_URL/api/v1/auth/logout"
rm -f "$KPL_COOKIE_JAR"
```

HTTP 자체는 자격 증명과 쿠키를 암호화하지 않습니다. HTTPS 또는 신뢰 관리망을 사용하고 HTTPS proxy는 forwarded protocol 헤더를 덮어써야 합니다.

시나리오의 같은 네 가지 job counter가 `/api/v1/snapshot`과 SSE snapshot에도 포함됩니다. 대시보드는 각 run에 이를 표시하므로 Controller 로그를 열지 않아도 실행 중, 성공, 실패, 취소된 background 작업 수를 확인할 수 있습니다.

[시각화 가이드](visualization.kr.md)에서 분석 응답, 그래프와 내보내기 형식을 확인하십시오.

[Bandwidth 측정](bandwidth.kr.md) · [v2 전체 분석 대조](v2-analysis-coverage.kr.md)

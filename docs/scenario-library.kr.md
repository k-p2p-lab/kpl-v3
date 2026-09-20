# 시나리오 라이브러리

[English](scenario-library.md) | 한국어

대시보드는 재사용할 검증된 YAML 시나리오를 Controller에 저장합니다. 라이브러리와 실험 결과는 서로 독립적입니다. 시나리오 저장만으로 실행되지 않고 편집한 YAML을 실행해도 자동 저장되지 않으며, 어느 한쪽을 삭제해도 다른 쪽은 삭제되지 않습니다.

## 대시보드에서 사용하기

1. **Run experiment**을 엽니다. 현재 Controller에서 **Saved scenarios** 목록을 불러옵니다.
2. 알아보기 쉬운 **Saved scenario name**을 입력하고 YAML을 편집합니다.
3. **Save scenario**를 누릅니다. 새 항목이 선택된 저장 시나리오가 됩니다.
4. 목록의 **Load**를 누르면 해당 이름과 YAML로 편집기 내용을 교체합니다.
5. 이름 또는 YAML을 편집하고 **Save changes**를 누르면 선택한 항목을 갱신합니다. 원본을 유지한 채 새 항목을 만들려면 **Save as new**를 누릅니다.
6. **Delete**와 **Confirm delete**를 차례로 눌러 항목을 삭제합니다. **Refresh**는 Controller에서 목록을 다시 읽습니다.

**New**는 편집기를 기본 예제로 초기화하고 선택한 저장 항목을 해제합니다. 다른 항목을 불러오거나 새 항목을 시작하면 저장하지 않은 편집 내용은 복구 사본 없이 교체됩니다. **Run**은 저장 여부와 관계없이 편집기에 있는 YAML을 실행하며 1~100회 반복할 수 있습니다.

이름은 앞뒤 공백을 제거한 뒤 검사하며 필수이고, Unicode 128자 이하이며 제어 문자를 포함할 수 없습니다. YAML도 필수이며 1 MiB 이하이고 저장 전에 일반 시나리오 검증을 통과해야 합니다. 이름은 중복될 수 있으며 각 항목에는 별도의 생성 ID가 부여됩니다. 목록은 최근 수정 순으로 정렬합니다.

라이브러리 이름과 YAML 최상위 `name`은 독립적입니다. 전자는 저장한 편집기 입력을 구분하고 후자는 실험 run의 이름으로 사용합니다. 저장은 [시나리오 schema](scenario-reference.kr.md)를 검증하며 Agent 용량 예약이나 Docker, 커널 지원, 연결성 검사는 수행하지 않습니다.

## 작성 중인 시나리오 검증

**YAML scenario** 옆의 **Validate**를 누르면 저장·실행과 같은 파서로 현재 내용을 검증합니다. 작성 칸 아래 패널에 성공 시 시나리오 이름과 phase 개수를, 실패 시 오류 원인을 표시합니다. YAML 파서가 제공하면 줄 번호도 표시하며 설정 오류에는 관련 필드나 phase가 포함됩니다. 긴 오류는 스크롤하거나 복사할 수 있습니다.

검증은 로그인 세션을 사용하며 저장용 이름이 필요 없고 저장·실행도 하지 않습니다. YAML은 한 문서여야 하고 1 MiB 이하여야 합니다. YAML 수정, **New**, 다른 시나리오 불러오기는 이전 결과를 지우며 예전 입력에 대한 늦은 응답은 무시합니다. 요청·네트워크 실패와 잘못된 YAML은 구분해서 표시합니다. 검증 성공이 Agent 용량, Docker 지원이나 실행 시 연결성까지 확인한 것은 아닙니다.

## 예상 종료 시각

**Experiment progress**의 실행 중·대기 중인 각 Run에 **Est. finish**와 대략적인 남은 시간을 표시합니다. 카드 위 요약에는 진행 중인 반복 실행 그룹별 **Group est. finish**가 표시됩니다. 종료 시각은 브라우저의 현지 시간대이며, 남은 시간은 Controller가 같은 그룹에서 먼저 실행될 Run의 대기 시간까지 포함해 계산합니다. 서로 다른 그룹은 독립적으로 실행됩니다.

초기 **Scenario estimate**는 대기 시간, 반복 횟수와 샘플링한 interval을 계산하고 백그라운드 작업의 겹침, `wait-jobs`, `stop-all`, `onExit`을 반영합니다. 준비 배리어는 처음에 설정된 timeout을 시간 여유로 사용합니다. 요청 수는 설정된 최댓값을 사용하므로 실제 publish·leave 후보가 적으면 더 빨리 끝날 수 있습니다. 노드 기동, 용량 대기, 요청 처리와 정리 시간은 사전에 알 수 없습니다. 단계가 끝나면 실제 시작·종료 시각으로 예측을 갱신하고, 같은 진행 중 그룹에서 성공한 Run의 평균 단계 시간과 정리를 포함한 전체 소요 시간으로 이후 추정치를 보정합니다. 이때 **Based on N completed runs**로 보정 근거를 표시합니다.

예상치는 종료 기한이 아닙니다. 시간 정보가 부족하면 **Estimating…**, 예상을 초과하면 **Taking longer than estimated**를 표시하고 뒤의 대기 Run과 그룹의 예상 시각을 늦춥니다. Run 중지·종료 후에는 해당 예측을 제거합니다. 이 값으로 자동 중지하지 않으며 Saved results에 예측을 보존하지 않습니다.

## REST API

요청과 응답 본문은 모두 JSON을 사용합니다. 큰 라이브러리도 가볍게 열 수 있도록 목록 응답에는 YAML을 넣지 않으므로, 편집하기 전에 개별 항목을 조회하십시오.

| Method | Path | 본문 | 성공 응답 |
|---|---|---|---|
| `GET` | `/api/v1/scenarios` | 없음 | `id`, `name`, `createdAt`, `updatedAt` 요약 목록 |
| `POST` | `/api/v1/scenarios` | `{ "name": "…", "yaml": "…" }` | 전체 항목과 `201` |
| `GET` | `/api/v1/scenarios/{id}` | 없음 | `yaml`을 포함한 전체 항목 |
| `PUT` | `/api/v1/scenarios/{id}` | `{ "name": "…", "yaml": "…" }` | 갱신된 전체 항목과 `200` |
| `DELETE` | `/api/v1/scenarios/{id}` | 없음 | 본문 없는 `204` |

조회·검증·변경 모두 로그인 세션이 필요합니다. 변경 요청의 `X-KPL-Request: dashboard` 헤더는 UI가 자동으로 전달합니다. 해석한 YAML이 1 MiB를 넘는 경우를 포함하여 유효하지 않은 입력은 `400`입니다. JSON 요청 본문은 escape된 문자를 고려해 `6 * 1 MiB + 64 KiB`까지 허용하며 이 한도를 넘으면 `413`입니다. 알 수 없는 JSON 필드와 후행 JSON 값은 거부합니다. 없거나 유효하지 않은 ID는 `404`, 저장소 오류는 `500`입니다. ID는 소문자 16진수 32자로 구성됩니다.

`control-node:8080`은 `access`가 표시한 Controller 주소로 바꾸고, 먼저 [API 인증](api.kr.md#인증)으로 `KPL_COOKIE_JAR`를 생성하십시오.

```sh
curl --fail http://control-node:8080/api/v1/scenarios

curl --fail -X POST http://control-node:8080/api/v1/scenarios \
  -H 'Content-Type: application/json' \
  -b "${KPL_COOKIE_JAR:?Log in first}" -H 'X-KPL-Request: dashboard' \
  --data-binary @- <<'JSON'
{"name":"Smoke baseline","yaml":"version: 2\nname: smoke-baseline\nphases:\n  - action: stop-all\n"}
JSON
```

## 저장과 백업

각 항목은 `<data-dir>/scenarios` 아래의 개별 JSON 파일에 저장됩니다. Swarm은 Controller 영구 데이터 볼륨을 `/var/lib/kpl/data`에 마운트하므로 일반적인 서비스 재시작과 `scripts/swarm.sh remove` 후에도 라이브러리를 보존합니다. 시나리오 라이브러리와 실험 결과를 함께 보존하려면 Controller 데이터 디렉터리 전체를 백업하십시오. 라이브러리는 해당 Controller에만 있으며 control node 사이에 복제되지 않습니다.

Controller는 임시 파일과 원자적 파일시스템 연산으로 각 항목을 기록합니다. 목록 또는 항목을 읽을 때 형식이 잘못되거나 예상하지 않은 내용이 있으면 부분적으로 신뢰한 시나리오를 반환하지 않고 오류로 처리합니다.

저장, payload 한도와 검증은 [`internal/controller/scenarios.go`](../internal/controller/scenarios.go), 편집기 사용 흐름은 [`internal/webui/static/app.js`](../internal/webui/static/app.js)에 구현되어 있습니다.

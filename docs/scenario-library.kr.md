# 시나리오 라이브러리

[English](scenario-library.md) | 한국어

[문서 안내](README.kr.md) · [저장소](../README.kr.md)

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

실시간 진행 예측과 불확실성은 [실행 예상 종료 시각](experiments.kr.md#예상-종료-시각)을 참고하십시오.

## REST API

요청 본문, 응답, 제한과 curl 예제는 [시나리오 라이브러리 API](api.kr.md#시나리오-라이브러리)에 있습니다.

## 저장과 백업

각 항목은 `<data-dir>/scenarios` 아래의 개별 JSON 파일에 저장됩니다. Swarm은 Controller 영구 데이터 볼륨을 `/var/lib/kpl/data`에 마운트하므로 일반적인 서비스 재시작과 `scripts/swarm.sh remove` 후에도 라이브러리를 보존합니다. 시나리오 라이브러리와 실험 결과를 함께 보존하려면 Controller 데이터 디렉터리 전체를 백업하십시오. 라이브러리는 해당 Controller에만 있으며 control node 사이에 복제되지 않습니다.

Controller는 임시 파일과 원자적 파일시스템 연산으로 각 항목을 기록합니다. 목록 또는 항목을 읽을 때 형식이 잘못되거나 예상하지 않은 내용이 있으면 부분적으로 신뢰한 시나리오를 반환하지 않고 오류로 처리합니다.

저장, payload 한도와 검증은 [`internal/controller/scenarios.go`](../internal/controller/scenarios.go), 편집기 사용 흐름은 [`internal/webui/static/app.js`](../internal/webui/static/app.js)에 구현되어 있습니다.

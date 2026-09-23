# 대시보드 사용 안내

[English](dashboard.md) | 한국어

[문서 안내](README.kr.md) · [저장소](../README.kr.md)

`sh scripts/swarm.sh access`의 Controller 주소를 열고 설정한 계정으로 로그인합니다. 이 문서는 실시간 패널과 조작을 설명합니다. 실행 절차는 [실험 실행·복구](experiments.kr.md), 기록 보존은 [저장 결과](results.kr.md), 노드·간선의 의미는 [토폴로지](topology.kr.md)를 참고하십시오.

## Dashboard 내장 시각화

페이지 제목 아래 **Network / Experiments / Agents / Results** 바로가기로 원하는 패널에 이동합니다. 접힌 패널은 자동으로 펼칩니다. 키보드 사용자는 **Skip to dashboard** 링크로 상단 메뉴를 건너뛸 수 있습니다.

**Metrics**는 클러스터·전달·대역폭·관측 지표 카드 11개를 가로 한 행에 표시합니다. 가로로 스크롤하거나 **Previous metrics / Next metrics** 화살표 버튼으로 이동하십시오. 카드에 마우스를 올리면 해당 카드 하나만 가로로 넓어지면서 상세 수치와 설명을 보여 줍니다. 긴 상세 내용은 펼친 카드 안에서 세로로 스크롤합니다. 폭이 좁은 모바일 화면에서는 상세 내용을 지표 수치 아래로 펼쳐 텍스트를 읽기 쉽게 표시합니다.

- 카드를 클릭하거나 탭하면 상세를 열고, 다시 선택하면 닫습니다. 터치 화면에서는 가로로 밀어 행을 이동합니다.
- 키보드 초점을 받으면 상세가 열립니다. Left/Right로 이전·다음 카드, Home/End로 첫·마지막 카드로 이동합니다. Enter/Space는 상세를 열고 닫으며, Escape 또는 행 바깥 클릭·탭으로 닫습니다.
- **How delivery is measured**는 행 바로 아래에 유지합니다. 설명과 펼침 상태는 실시간 갱신 중에도 유지됩니다.

**Online Agents**와 **Ready Peers**는 전체 클러스터, 메시지·대역폭 카드는 **Run metrics**에 표시된 실행을 대상으로 합니다. **Observation quality**의 대표 값은 `Receipt / Continuity / Start` 순서의 각 unknown 수이며, 서로 겹칠 수 있는 범주를 합산하지 않습니다. 펼치면 전체 레이블과 outcome 수를 확인할 수 있습니다.

Run이 완료·실패·취소되면 해당 실험의 캐러셀 수치는 초기 N/A·0으로 돌아갑니다. 카드는 그대로 유지하며 다음 run이 시작되면 새 수치를 표시합니다. 클러스터 수치는 실시간 상태를 유지하고 최종 측정값은 **Saved results**에서 확인합니다.

**Available slots**는 online Agent가 보고한 여유 Peer 수를 합산하며 offline Agent는 포함하지 않습니다. Capacity는 CPU·메모리 예약이 아닌 생성 허용 개수입니다. 값을 늘리기 전 [Peer 배치와 capacity](agents.kr.md)를 확인하십시오.

**Saved results → Images**에서 서버 백그라운드 분석을 접수하고 개요·메시지·반복 비교 그림을 PNG/CSV/ZIP으로 다운로드합니다. 저장 기록을 사용하며 Prometheus 보존과 독립적입니다. 조작 방법은 [결과 이미지](visualization.kr.md), 계산 정의는 [실험 지표](experiment-metrics.kr.md#저장-결과-연구-지표)를 참고하십시오.

### 패널 접기와 펼치기

**Network overview**, **Experiment progress**, **Agent status**, **Recent network events**, **Saved results** 헤더의 **Collapse / Expand** 버튼으로 패널을 접고 펼칩니다. 좁은 화면에는 화살표 아이콘을 표시합니다. 클릭·탭과 키보드 Enter/Space를 지원합니다. 다섯 패널은 기본으로 열리며, 같은 브라우저의 Dashboard origin에 선택을 저장해 새로고침 뒤에도 유지합니다.

**Network overview**와 **Experiment progress**는 전체 폭의 세로 배치를 유지합니다. 넓은 화면에서 **Agent status** 또는 **Recent network events** 중 하나를 접으면 짧은 제목을 위에 두고 열린 패널을 아래에 전체 폭으로 표시합니다. 둘 다 펼치면 기존 Agent/events 열 배치를 복원합니다. 둘 다 접으면 짧은 제목 둘을 나란히 표시하며, 모바일에서는 세로 배치를 유지합니다.

패널을 숨겨도 실험·백그라운드 분석과 실시간 데이터 갱신은 계속됩니다. **Network overview**를 접으면 배치 애니메이션도 멈추고, 다시 펼치면 최신 상태를 표시하며 기존 **Pause motion** 선택을 유지합니다. 이 버튼은 패널 표시만 바꿉니다.

## 로그인과 실시간 갱신

세션이 만료되면 **Login required**를 표시하며 다시 로그인해 대시보드로 돌아옵니다. 페이지를 닫거나 숨기면 실시간 연결은 끊지만 접수된 실험과 분석은 Controller에서 계속됩니다. 페이지가 다시 보이면 현재 상태를 동기화합니다. **Reconnecting**은 브라우저 연결 상태이며 실험 결과를 뜻하지 않습니다.

복구 상태가 계속되면 [스트림 진단](monitoring.kr.md#대시보드-스트림-전송량)을 확인하십시오. 세션 동작은 [인증](api.kr.md#인증), 전송 형식은 [Dashboard SSE](api.kr.md#dashboard-sse)가 정의합니다.

Dashboard는 몰려오는 telemetry를 초당 최대 4회 화면 갱신으로 병합하고, 동일한 텍스트·목록·토폴로지 요소를 유지합니다. 지표 갱신은 펼친 카드와 가로 스크롤 위치를 유지하면서 수치를 업데이트합니다.

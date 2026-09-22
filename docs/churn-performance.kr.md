# 반복 Churn 실험의 Controller 부하

2026-09-22 조사에서 누적 Peer 기록은 65,227개였고, Prometheus의 최근 5분 평균에서 Controller는 약 7.17개 코어와 초당 약 526MB의 메모리 할당을 기록했습니다. 메모리 할당률은 사용 중인 메모리 크기와 별개이며, 짧게 쓰고 버리는 객체의 생성·회수 비용도 포함합니다.

## 원인과 변경

- `/api/v1/bootstrap`이 주소 목록을 구하기 위해 전체 snapshot을 생성했습니다. 이 과정에서 과거 Peer 전체 복사·정렬, 실험 지표 및 토폴로지 계산이 발생했습니다. 이제 활성 노드 인덱스에서 해당 실행의 ready boot 주소만 읽습니다.
- Discovery와 부분 heartbeat의 점유량 확인도 활성 노드 인덱스를 사용합니다. 부분 heartbeat에서 생략된 노드를 찾기 위한 전체 이력 순회를 제거했습니다.
- 대시보드 SSE는 종료 노드를 제외한 후 snapshot을 만듭니다. 과거 노드를 복사·정렬한 뒤 버리는 비용을 제거했습니다. 전체 inventory와 저장 결과에는 이력을 보존합니다.
- Agent·실험·최근 이벤트 목록 REST API는 필요한 목록만 복사합니다. Agent 등록도 Agent 정보만 구성하고 Peer 전체 snapshot을 만들지 않습니다.
- 완료되어 변하지 않는 실험의 Prometheus 전파 지연 히스토그램을 재사용합니다. 새로운 이벤트, 늦게 도착한 증거, 수신 기간 만료는 기존 규칙대로 요약과 히스토그램을 갱신합니다. 지표 정의와 저장 결과 계산은 유지합니다.
- Agent와 Controller의 이벤트 크기 검증은 이벤트별 인코딩 결과의 길이를 검사하고 버립니다. 검증 목적으로 전체 전송 배치를 다시 할당하지 않습니다. JSON escaping, 정확한 10MiB 경계, 잘못된 후속 이벤트의 일괄 거부를 유지합니다.

## 재현 벤치마크

Intel Xeon E5-2620 v3, `GOMAXPROCS=2`에서 동일한 입력으로 측정했습니다. 조회 테스트는 종료 Peer 65,000개와 활성 Peer 100개를 사용합니다. 이벤트 검증 테스트는 4KiB 문자열을 가진 이벤트 250개를 사용합니다. MB와 KB는 각각 1,000,000바이트와 1,000바이트입니다.

| 경로 | 변경 전 시간/회 | 변경 후 시간/회 | 변경 전 할당/회 | 변경 후 할당/회 |
|---|---:|---:|---:|---:|
| Bootstrap 주소 조회 | 196.56ms | 0.111ms | 149.22MB | 28.86KB |
| 대시보드 frame 생성 | 242.60ms | 1.132ms | 171.83MB | 320.91KB |
| 이벤트 크기 검증 | 4.33ms | 1.70ms | 7.05MB | 12.26KB |

종료 이력이 없는 경우와 65,000개인 경우 모두 변경 후 활성 노드 조회의 할당량은 비슷했습니다. 실제 운영 CPU는 활성 Peer 수, 토폴로지, 이벤트 발생률, Docker 부하에도 영향을 받습니다. 위 수치는 로컬 경로별 벤치마크이며 운영 배포 후 전체 실험의 CPU·진입률을 다시 확인해야 합니다.

```bash
GOMAXPROCS=2 go test ./internal/controller ./internal/model \
  -run '^$' \
  -bench 'Benchmark(BootstrapChurnHistory|DashboardChurnHistory|ValidateTelemetryBatch)$' \
  -benchtime=300ms -benchmem
```

수정 적용에는 Controller와 Agent 이미지 재빌드·재배포가 필요합니다. 실험 이력 삭제를 전제로 하지 않습니다. 배포 후에는 같은 시나리오·동일 활성 Peer 규모에서 다음 Prometheus 지표와 Controller의 `churn join schedule delayed` 로그를 함께 비교하십시오.

```promql
rate(process_cpu_seconds_total{job="kpl-controller"}[5m])
rate(go_memstats_alloc_bytes_total{job="kpl-controller"}[5m])
sum by (state) (kpl_nodes)
kpl_local_telemetry_queue_events
```

# 모니터링·접근 로그·연결 진단

[English](monitoring.md) | 한국어

[문서 안내](README.kr.md) · [저장소](../README.kr.md)

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

화면 조작은 [대시보드](dashboard.kr.md), 실행 복구는 [실험](experiments.kr.md), 원본 내보내기와 백업은 [저장 결과](results.kr.md)를 참고하십시오.

## 실행과 분석

1. 대시보드의 **Run experiment**에서 [`examples/monitoring.yaml`](../examples/monitoring.yaml)을 실행합니다. envelope 발행과 raw 발행을 함께 확인하는 작은 실험입니다.
   **Run** 옆 **Runs**를 1~100으로 지정하면 순차 반복합니다. 회차마다 별도 결과를 남기며 실패 또는 **Stop batch**는 나머지를 취소합니다. [실행과 반복](experiments.kr.md)를 참고하십시오.
2. Grafana에서 **Run**(run_id), **Agent**, **Topic**을 선택합니다. 여러 run을 고르면 선택한 실험의 트래픽·지연을 합산하며, 네트워크 설정·대역폭 시계열은 범례에서 run별로 구분합니다. 세션 도달률 패널은 Run만, 대역폭·control 패널은 Topic 없이 Run과 Agent를 적용합니다.
3. 실험이 끝난 뒤에도 시간 범위를 해당 실행 구간으로 지정하면 시계열을 볼 수 있습니다. 기본 새로고침은 5초입니다.

Prometheus는 Controller와 Agent의 `/metrics`를 5초마다 수집합니다. Controller가 등록된 Agent의 metrics URL을 HTTP service discovery로 반환하고, Prometheus가 service VIP 대신 각 Agent의 host-mode 포트에 직접 접근합니다. Peer를 직접 scrape하거나 각 컨테이너에 exporter를 추가하지 않습니다. Controller는 기존 Peer telemetry를 누적 집계하므로 `scope: all`에서 telemetry가 손실되면 Controller가 만드는 Peer 지표도 영향을 받습니다. 모니터링 서비스는 별도 Docker network에 있으며 Peer에는 추가 네트워크·권한을 부여하지 않습니다.

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

Grafana 세션 패널에는 Run 필터만 적용하고 백분율 평균이 아닌 수신 쌍 합계로 집계합니다. 범위는 관측된 집단에 대한 값이며 보이지 않는 모집단을 증명하지 않습니다. 발행 시점 가용성 경고나 `measurementIncomplete`가 있어도 확인된 발행 시점 도달률과 안정 coverage를 표시하며, 선택한 run 전체에 확인된 발행 시점 쌍이 하나도 없을 때만 N/A입니다. 각 집계에서 확인된 발행 시점 대상 = 안정 + 이탈 + 지속 여부 불명이 성립합니다. 과거 발행 패널은 새 결과에서 제외한 과거 데이터를 표시합니다. 새 패널은 `kpl_window_*`만 사용하고 이전 `kpl_delivery_*`·`kpl_propagation_latency_seconds`는 과거 의미를 유지하므로 섞지 마십시오.

전체 `deliver`에는 로컬 수신도 포함되므로 발행 수로 나누어 도달률을 구하지 않습니다. TCP 재전송은 패킷 손실을 지연으로 바꿀 수 있습니다. [지표 정의와 수식](experiment-metrics.kr.md)을 확인하십시오. 늦은 배치가 기간 gauge와 histogram 버킷을 정정할 수 있어 `rate`/`increase` 대신 직접 조회합니다. Grafana 지연은 실험 전체 누적 분위수이며 선택 시간 범위만의 분위수가 아닙니다.

누적 카운터는 최근 300개 웹 이벤트 버퍼와 독립적입니다. Controller 프로세스가 재시작되면 카운터가 초기화되며 과거 `events.jsonl`을 자동 재생하지 않습니다. Prometheus에 이미 저장된 시계열은 유지되고 `rate`/`increase`는 관측된 카운터 재설정을 처리합니다. 단, scrape 전에 사라진 이벤트나 telemetry 전송 실패를 복구하는 기능은 아닙니다. `increase`는 scrape 표본으로 추정한 구간 증가량이므로 누적 정수 이벤트 수와 항상 정확히 일치하지는 않습니다.

## 웹 접근·인증 로그

Controller는 `<data-dir>/logs` 아래에 한 줄당 JSON 객체 하나를 자동 기록합니다. 로컬 기본 경로는 `data/logs`, Swarm에서는 영속 `controller-data` volume의 `/var/lib/kpl/data/logs`입니다.

| 파일 | 기록 대상 |
|---|---|
| `access.jsonl` | 대시보드·정적 파일·API 접근, 리다이렉트, 실패 응답, SSE 연결 시작·종료 |
| `auth.jsonl` | 로그인 성공·실패, 로그아웃 시도, 세션 부재·만료로 거절된 접근과 동일 출처 검사 거절 |

UTC `timestamp`, `event`, 서버가 생성한 `requestId`, `remoteIp`, HTTP `method`·`path`, 응답 `status`, 본문 `bytes`, 처리 시간 `durationMs`, `userAgent`, 인증 방식 `authentication`(`anonymous`·`session`·`internal`)과 확인 가능한 `user`를 기록합니다. 인증 기록에는 `outcome`과 `invalid_credentials`, `rate_limited`, `invalid_request`, `origin_denied`, `login_required` 등의 이유가 포함됩니다. 유효한 JSON 로그인 요청에서는 실패하더라도 입력한 계정명을 남깁니다. 같은 요청의 접근·인증 기록은 request ID로 연결하며, SSE는 응답 시작 즉시 `sse_open`, 종료 시 `sse_close`를 남깁니다.

비밀번호, 세션 쿠키, Authorization 헤더, 요청·응답 본문, Referer와 URL 쿼리는 기록하지 않습니다. 입력 문자열의 길이를 제한하고 JSON escaping을 적용합니다. `remoteIp`는 실제 연결 주소이며 클라이언트가 보낸 `Forwarded`·`X-Forwarded-For`를 신뢰하지 않습니다. reverse proxy 뒤에서는 proxy 주소가 기록됩니다. 실험 중 이벤트마다 디스크에 쓰지 않도록 인증에 성공한 정상 Agent·Peer 통신과 정상 health·metrics·Prometheus target 점검은 제외합니다. 해당 요청의 오류와 인증 실패는 기록합니다.

각 파일은 **10 MiB**를 넘기기 전에 회전하며 현재 파일과 **백업 5개**(`.1`이 최신, `.5`가 가장 오래됨), 두 종류 합계 약 **120 MiB**까지 보관합니다. 재시작해도 기존 파일에 이어 씁니다. 디렉터리는 `0700`, 파일은 `0600` 권한입니다. 시작 시 로그 경로를 사용할 수 없으면 오류로 종료합니다. 실행 중 쓰기 실패는 Controller의 일반 서비스 로그에 알리고 HTTP 요청 처리는 유지하며 이후 요청에서 기록을 재시도합니다. 메모리 큐 없이 파일에 append하지만 건마다 fsync하지는 않습니다. 접근 기록을 보존하려면 Controller volume 백업에 `logs`를 포함하십시오. 대시보드에서 이 파일을 제공하거나 실험 ZIP에 넣지는 않습니다.

로컬 Controller에서는 다음과 같이 두 파일을 확인합니다.

```sh
tail -F data/logs/access.jsonl data/logs/auth.jsonl
```

Swarm에서는 manager helper로 최근 100줄을 JSONL 원문 그대로 출력합니다.

```sh
sh scripts/swarm.sh logs access
sh scripts/swarm.sh logs auth
sh scripts/swarm.sh logs access --tail 500
sh scripts/swarm.sh logs auth --tail all > auth.jsonl
```

`--tail`은 양의 정수 또는 `all`을 받습니다. 현재 파일만 조회하며 회전된 `.1`~`.5` 백업은 Controller volume에 남아 있습니다. 명령 실행 시점의 내용을 출력하므로 `jq` 같은 도구로 파이프 처리할 수 있습니다. 기존 `logs controller`, `logs agent`, `logs prometheus`, `logs grafana`는 타임스탬프가 있는 서비스 로그를 계속 조회하며 `--tail`도 사용할 수 있습니다.

Helper는 현재 manager Docker context에서 실행 중인 Controller task를 찾고, `docker exec`로 파일을 읽습니다. 기본적으로 현재 Docker daemon이 해당 task를 실행하는 노드여야 합니다. Controller가 다른 노드(worker 포함)에 있으면 그 노드의 Docker context를 만들고 **파일 조회용으로만** 지정하십시오.

```sh
# user와 control-node를 실제 SSH 계정과 Controller 호스트로 바꿉니다.
docker context create kpl-control --docker "host=ssh://user@control-node"
sh scripts/swarm.sh logs access --context kpl-control
sh scripts/swarm.sh logs auth --context kpl-control --tail 500
```

호출자의 Docker context는 manager에 연결된 상태로 유지합니다. 위 두 명령의 `--context`는 노드 확인과 파일 조회에만 적용되며, task 탐색은 manager에서 수행합니다. 선택한 daemon이 task의 실제 실행 노드인지 검사합니다. 웹 로그 기능이 포함된 Controller가 실행 중이어야 하며, 파일 부재·task 실행 불가·잘못된 노드는 오류로 안내합니다. 컨테이너를 새로 띄우거나 stack을 변경하지 않습니다. 실시간 추적이나 회전된 백업 조회는 Controller 노드에서 파일을 직접 확인하십시오.

## 대시보드 스트림 전송량

대시보드가 보이는 동안 `/api/v1/stream?view=dashboard`를 사용하며 첫 `snapshot` 이후 `snapshot_delta` 또는 유휴 `heartbeat`를 받습니다. 페이지를 숨기면 연결을 끊고 다시 보이면 동기화합니다. 오래 유지한 연결의 누적 바이트는 정적 페이지 크기가 아닙니다. 본문 형식, 연결 제한시간, 재시도와 기존 클라이언트 규칙은 [Dashboard SSE](api.kr.md#dashboard-sse)가 정의합니다.

전송량이 크거나 **Reconnecting**이 지속되면 다음을 확인하십시오.

1. 이미지 갱신 후 페이지를 새로고침하고 브라우저 Network에서 위의 dashboard view를 사용하는지 확인합니다.
2. 접근 로그의 `sse_close.bytes`와 `durationMs`를 비교하고 `sse_open`·`sse_close`, `/api/v1/auth/session` 응답 상태·시간을 함께 확인합니다.
3. Controller·서비스의 부하와 연결을 확인합니다. 로그인 거절과 느린 세션 조회는 다르며 브라우저 연결 해제만으로 실험 실패를 판단하지 않습니다. 저장된 실행 상태를 확인한 뒤 [복구](experiments.kr.md)를 선택합니다.

기존 대용량 trace 측정과 churn 벤치마크는 검증 범위와 함께 [성능 근거](churn-performance.kr.md)에 보존합니다.

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

Swarm stack은 `GET /api/v1/prometheus/agent-targets`에서 등록된 target을 탐색합니다. `sh scripts/swarm.sh access`로 광고 주소를 확인하고, 설정한 포트가 선택된 모든 Agent 노드에서 비어 있으며 control 노드에서 TCP 접근이 허용되는지 확인하십시오. 운영자가 Agent metrics 링크를 직접 열 때에는 브라우저가 속한 신뢰 관리망에서도 접근을 허용하고 신뢰하지 않는 출발지는 차단하십시오. `up{job="kpl-agent"}`를 보면 등록되었지만 방화벽이나 잘못된 Swarm `NodeAddr` 때문에 접근할 수 없는 target을 성공한 scrape와 구분할 수 있습니다.

현재 이미지 버전은 Prometheus `v3.13.2`와 Grafana `13.2.1`로 고정했습니다. 업데이트 시 공식 [Prometheus 다운로드](https://prometheus.io/download/)와 [Grafana Docker 설치 문서](https://grafana.com/docs/grafana/latest/setup-grafana/installation/docker/)를 참고하고 설정·대시보드를 재검증하십시오.


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

## Dashboard 내장 시각화

이 내용은 [대시보드](dashboard.kr.md)로 옮겼습니다.

### 저장 결과 찾기

[저장 결과 검색](results.kr.md#저장-결과-찾기)으로 이동했습니다.

### 패널 접기와 펼치기

[대시보드 패널](dashboard.kr.md#패널-접기와-펼치기)로 이동했습니다.

## 실험 결과 다운로드

이 내용은 [저장 결과](results.kr.md#실험-결과-다운로드)로 옮겼습니다.

## 분석 파일과 이미지 보존

이 내용은 [저장 결과](results.kr.md#분석-파일과-이미지-보존)로 옮겼습니다.

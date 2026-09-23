# Swarm 배포와 운영

[English](swarm.md) | 한국어

[문서 안내](README.kr.md) · [저장소](../README.kr.md)

하나 이상의 Linux 서버에서 활성 Swarm manager의 `scripts/swarm.sh`로 `stack.swarm.yaml`을 배포·관리합니다. Swarm은 **선택한 서버마다 Agent 하나**를 유지하고, Controller가 실험의 Peer를 Agent에 배분합니다. Peer는 해당 서버의 독립 Docker 컨테이너입니다. Swarm 서비스처럼 Peer를 다른 서버로 자동 재배치하지 않습니다.

실행 가능한 배포 근거는 [`stack.swarm.yaml`](../stack.swarm.yaml), [`scripts/swarm.sh`](../scripts/swarm.sh), [`scripts/swarm-config.sh`](../scripts/swarm-config.sh)입니다. 실제 서비스·네트워크 대응은 [v3 아키텍처](architecture.kr.md)를, 개념 아키텍처는 [Hub](https://github.com/k-p2p-lab/hub/blob/master/README.kr.md)를 참고하십시오.

## 배포 전 준비

Linux 서버들이 이미 같은 Swarm에 가입되어 있고, rootful Docker와 userns-remap 미사용 구성을 갖춘 상태에서 시작합니다. manager에 이 저장소와 Docker CLI, coreutils의 `timeout`, `-f`를 지원하는 `readlink`, `awk`, 일반 POSIX 셸 유틸리티를 준비하십시오. 저장소 디렉터리에서 현재 Docker context가 가리키는 **active Swarm manager**를 대상으로 실행합니다. worker마다 저장소를 복사하거나 Agent를 수동으로 시작할 필요는 없습니다.

Swarm 가입은 이미지 registry 생성을 의미하지 않습니다. manager와 모든 대상 노드에서 접근할 수 있는 registry가 필요하며, 필요한 인증과 HTTPS 신뢰 설정은 미리 준비해야 합니다. helper는 registry를 설치하거나 Docker daemon의 전역 TLS/insecure-registry 설정을 변경하지 않습니다. 예를 들어 기존 registry가 있다면 아래 이미지 대신 `172.20.4.171:5000/kpl-v3:v3`를 사용할 수 있습니다. HTTP registry라면 관련 Docker daemon이 이를 사용하도록 사전에 설정되어 있어야 합니다.

서버 간 TCP 2377(manager), TCP/UDP 7946, UDP 4789 통신이 필요합니다. control 노드의 Prometheus가 선택된 모든 Agent 노드 주소의 TCP `KPL_AGENT_METRICS_PORT`(기본 `9091`)에도 접근할 수 있어야 합니다. 운영자가 Agent metrics 링크를 직접 열 경우 운영자 브라우저가 속한 신뢰 관리망에서도 이 포트를 허용하십시오. VXLAN과 metrics 접근은 신뢰하는 네트워크 안으로 제한하고 VPN·클라우드망에서는 underlay MTU와 VXLAN overhead를 확인하십시오. smoke 실험에는 네트워크 조건 적용을 위한 커널 지원도 필요합니다. [Docker overlay 요구사항](https://docs.docker.com/engine/network/drivers/overlay/)

분산 smoke 검증에는 Agent를 실행할 노드가 최소 두 대 필요합니다. manager 한 대와 worker 두 대이면 `--workers`로 manager를 실험에서 제외합니다. manager 한 대와 worker 한 대이면 대신 `--all`을 사용해 두 노드에 Agent를 배치하십시오. 이 경우 manager도 실험과 CPU·메모리를 공유합니다. 전체 과정에서 같은 사용자와 Docker context를 사용하십시오. Docker 접근에 sudo가 필요하다면 `init`, `login`, `publish`, 배포 명령을 포함해 일관되게 `sudo sh scripts/swarm.sh ...`로 실행합니다.

### Linux 호스트와 Peer 격리 요구사항

Agent는 활성 Swarm 노드와 명시적으로 설정한 Docker 이미지 및 Peer 네트워크를 요구합니다. Peer 네트워크는 Swarm scope의 attachable overlay여야 합니다. 각 Agent는 자기 노드의 `/var/run/docker.sock`으로 standalone Peer 컨테이너를 생성합니다. Rootless Docker와 userns-remap은 지원하는 배포 기준에 포함되지 않습니다.

Docker socket은 Agent에만 전달하며 Agent는 `0:0`으로 실행합니다. Socket 접근은 해당 Docker 노드에 대한 넓은 제어 권한을 부여합니다. Peer도 비공개 설정을 읽기 위해 `0:0`으로 실행하지만 Docker socket, 호스트 디렉터리 mount와 published host port를 받지 않습니다. 모든 기본 capability를 제거하고 `no-new-privileges`를 활성화하며 네트워크 조건이 있는 Peer에만 `NET_ADMIN`을 추가합니다. Privileged 모드를 부여하거나 socket을 누구나 쓸 수 있도록 바꾸지 마십시오. SELinux 또는 AppArmor 호스트에서는 전역 enforcement를 끄거나 시스템 디렉터리를 relabel하지 말고 호스트 정책에 따라 Agent socket 접근을 허용하십시오.

네트워크 조건에는 Linux `sch_prio`, `sch_netem`, `cls_u32`와 TBF를 사용할 때의 `sch_tbf`가 필요합니다. 커널에 내장될 수도 있으므로 `lsmod`만으로 지원 여부를 판단할 수 없습니다. Peer는 호스트 커널에 모듈을 적재할 수 없으므로 선택한 모든 노드의 커널을 준비하십시오. `tc` 명령이 실패하면 Peer 시작도 명시적인 오류로 실패합니다.

Peer는 overlay IPv4 주소의 TCP 20000으로 통신하며 Peer 제어 HTTP는 TCP 18000을 사용합니다. 기본 `scope: p2p`는 Peer network namespace의 송신 TCP 20000 트래픽을 제어하고 `scope: all`은 제어·telemetry 트래픽에도 영향을 줍니다. IPv6 전용 Peer 실험은 지원 기준 밖입니다. 허용하는 최대 jitter는 `2.147483647s`이며 시나리오 seed가 커널 패킷 조건과 스케줄링까지 결정적으로 만들지는 않습니다. 지연을 비교할 때는 모든 호스트의 시계를 chrony/NTP로 동기화하십시오.

### 단일 노드 Swarm

Linux 노드 하나에서도 같은 stack과 attachable overlay로 개발 또는 작은 실험을 수행할 수 있습니다. 활성 Swarm manager를 준비하고 아래 `init` 명령의 `KPL_MIN_AGENTS=1`을 설정한 뒤 `deploy --all`을 사용하십시오. Controller, 모니터링 서비스, Agent와 Peer가 해당 호스트 자원을 공유합니다. Registry, 커널, 인증과 정리 요구사항은 동일합니다. Smoke 시나리오를 실행할 수 있지만 호스트 간 분산과 연결을 검증하려면 Agent 노드가 최소 두 개 필요합니다.

## Bash 자동완성

저장소 디렉터리에서 다음 명령으로 현재 Bash에 단축 명령과 자동완성을 활성화합니다.

```bash
source scripts/activate-swarm.sh
```

이 저장소의 `scripts/swarm.sh`를 실행하는 `swarm` 셸 함수가 만들어지며 인자를 그대로 전달합니다. 다른 디렉터리로 이동해도 활성화한 저장소를 계속 사용합니다. Tab으로 명령·지원 옵션, `init`/`configure` 설정 키, 시나리오·설정 파일 경로, 노드 ID·호스트명, Docker context 이름을 완성할 수 있습니다. 아래 `<Tab>`은 문자를 입력하는 것이 아니라 Tab 키를 누르는 위치입니다.

```text
swarm lo<Tab>
swarm logs auth --<Tab>
swarm logs auth --context <Tab>
swarm deploy --<Tab>
swarm add-node worker-<Tab>
swarm configure KPL_IMAGE_<Tab>
swarm configure KPL_CONTROL_NODE_ID=<Tab>
swarm --env-file .env.<Tab>
```

별도 `bash-completion` 패키지는 필요하지 않습니다. 노드·context 후보는 호출자의 현재 Docker context에서 읽기 전용으로 조회하며, 조회 제한시간은 1초(종료되지 않으면 1초 뒤 강제 종료), 캐시는 5초입니다. Docker가 응답하지 않으면 동적 후보만 생략하고 명령·옵션·경로 완성은 계속 동작합니다. 자격 증명 파일을 읽거나 저장된 비밀번호 값을 제안하지 않으며, `sh`·`bash`의 기존 자동완성을 교체하지 않습니다. 자동완성은 `swarm` 단축 명령에서 사용하십시오.

활성화 스크립트는 `sh`나 `bash`로 실행하지 말고 반드시 **source**로 불러옵니다. 현재 셸에만 적용됩니다. 새 터미널에도 적용하려면 `~/.bashrc`에 `source /저장소/절대경로/kpl/v3/scripts/activate-swarm.sh`를 추가하십시오(공백이 있는 경로는 따옴표로 감쌉니다). 현재 셸에서 해제하려면 `complete -r swarm`, `unset -f swarm`을 실행합니다. 자동으로 권한을 높이지 않으므로 Docker 접근 권한은 기존 운영 방식대로 준비하십시오.

## 첫 배포부터 실험과 다운로드까지

### 1. 설정과 이미지 게시

```sh
sh scripts/swarm.sh nodes
sh scripts/swarm.sh init KPL_IMAGE=registry.example.com/kpl-v3:v3 KPL_AGENT_CAPACITY=20 KPL_MIN_AGENTS=2
sh scripts/swarm.sh config
# registry가 인증을 요구할 때만 실행합니다.
sh scripts/swarm.sh login
sh scripts/swarm.sh publish
```

이미지 참조를 실제 registry와 repository로 바꾸십시오. `init`은 전달한 설정을 검증하고 현재 manager를 기본 control 노드로 선택하며, 기존 파일을 덮어쓰지 않고 비공개 `.env.swarm`을 만듭니다. 대시보드와 Grafana 자격 증명을 각각 생성합니다. 이미 설정이 있으면 `configure KEY=VALUE...`로 변경합니다. `config`는 비밀 값을 가린 유효 설정을 표시합니다.

`login`은 `KPL_IMAGE`에서 registry를 추론해 helper와 같은 계정으로 대화형 로그인을 실행합니다. `publish`는 이 저장소를 빌드하고 설정한 이미지 태그로 push합니다. 해당 이미지가 이미 게시되어 있다면 `publish`는 생략합니다. 기본 빌드는 native 플랫폼을 대상으로 하며, 아키텍처가 혼합되어 있다면 해당 빌드가 가능한 기존 Buildx builder에서 manager를 포함한 모든 대상 아키텍처를 게시하십시오.

```sh
sh scripts/swarm.sh publish --platforms linux/amd64,linux/arm64
```

helper는 builder나 CPU 에뮬레이션을 설치하지 않습니다. native 실행에는 노드의 아키텍처에 맞는 이미지가 필요합니다. [Docker multi-platform 빌드](https://docs.docker.com/build/building/multi-platform/)

### 2. 배포와 화면 접속

```sh
# manager 한 대와 worker 한 대인 클러스터에서는 대신 --all을 사용합니다.
sh scripts/swarm.sh deploy --workers
sh scripts/swarm.sh check
sh scripts/swarm.sh status
sh scripts/swarm.sh access
sh scripts/swarm.sh credentials
```

`deploy`는 이미지를 확정하고 선택한 노드에 label을 추가하며, 필요하면 attachable Peer overlay를 생성한 뒤 배포 검사와 stack 배포를 접수합니다. 모든 서비스와 Agent가 준비되기 전에 반환합니다. `check`는 불러온 설정으로 구성·네트워크·배치 사전 검사를 다시 수행하며, 실제 서버 간 통신이나 모든 worker의 커널 규칙 설치를 검증하지 않습니다. `status`는 Docker 서비스·task·노드 상태이며 Controller 등록 상태는 아닙니다.

`access`가 출력한 Controller URL을 여십시오. Controller, Prometheus, Grafana 포트는 명령을 실행하는 manager와 다를 수 있는 지정된 **control 노드**에 게시됩니다. 같은 명령은 선택된 각 Agent 노드의 host-mode metrics URL도 표시합니다. 브라우저에서 Agent 링크를 열려면 운영자 기기의 신뢰 관리망에서 각 Agent 노드로 직접 접근할 수 있어야 합니다. control 노드에 접근할 수 있는 기기에서 대시보드의 **Online Agents가 2 이상**이 될 때까지 기다리십시오. **Agent status**에서는 `online` 행만 확인합니다. **Peers**는 점유량 / 용량이며, 용량에서 점유량을 뺀 여유 슬롯의 합계가 **6 이상**이어야 합니다. 요약의 **Available slots**에는 offline Agent도 포함될 수 있으므로 이 조건은 표로 확인하십시오. Swarm task가 실행 중이라는 사실만으로 등록까지 확인되지는 않습니다. 시작에 문제가 있으면 `sh scripts/swarm.sh logs agent` 또는 `sh scripts/swarm.sh logs controller`를 확인하십시오.

`credentials`는 대시보드의 `KPL_USER`·`KPL_PASSWORD`와 별도 Grafana 계정을 평문으로 표시합니다. Controller에서는 **Log in**으로 로그인하십시오. Grafana의 기존 사용자명은 유지하며 시작할 때 원래 관리자 비밀번호를 설정값으로 동기화합니다.

### 3. 분산 smoke 실험 실행

```sh
# examples/swarm-smoke.yaml을 출력합니다. FILE을 지정하면 해당 시나리오를 출력합니다.
sh scripts/swarm.sh scenario
```

대시보드에서 **Run experiment**를 누르고 **YAML scenario** 전체를 출력된 YAML로 교체한 다음 **Run**을 누르십시오. 웹 폼의 기본 시나리오는 다른 smoke 실험이며 Swarm 예제를 자동으로 불러오지 않습니다.

예제는 boot Peer 한 개와 worker 다섯 개를 만듭니다. worker에는 25ms 지연, 2ms jitter, 0.5% loss를 적용합니다. readiness 확인과 20초 대기 후 `payloadSize: 4096`으로 메시지 50개를 발행하고, 수집을 위해 1분 기다린 뒤 `stop-all`을 실행합니다. 용량이 같고 다른 부하가 없는 Agent들에서는 balanced 배치가 Peer를 분산합니다. 서로 다른 Agent에 Peer가 나타나고 publish/deliver 이벤트가 기록되는지 확인하십시오. readiness는 프로세스 초기화 완료 기준이며 GossipSub mesh 수렴을 보장하지 않습니다. 전달 수는 실제 조건에 따라 달라집니다.

분석하려면 Grafana URL에서 로그인하고 **KP2PLab Experiment Analysis**의 **Run**에 해당 실험을 선택하십시오. **Agent**와 **Topic**으로 서버와 트래픽을 비교합니다. 대시보드 메시지 요약은 최근 이벤트 버퍼와 독립적으로 선택 실행 전체를 누적합니다. 실시간 counter는 Controller 재시작 시 초기화되지만 저장 분석과 ZIP 지표는 기록 파일에서 다시 계산합니다. Grafana는 각 패널의 필터·집계 규칙을 적용합니다. [모니터링 가이드](monitoring.kr.md)를 참고하십시오.

### 4. 결과 다운로드 후 철거

실험이 끝나고 해당 Peer들이 종료되었는지 확인하십시오. 다른 실험이 없다면 Agent 점유량은 0으로 돌아와야 합니다. 실행 중인 실험을 취소하려면 해당 실험의 **Stop**을 누르고 정리가 완료될 때까지 기다립니다.

실험 항목이나 **Saved results**에서 **Download results**를 선택합니다. ZIP에는 `scenario.yaml`, `experiment.json`, `events.jsonl`, 선택적 `observations.jsonl`, `metrics.json`, `export.json`이 들어갑니다. 수집된 관측 요약·표본 그래프·대역폭 카운터도 포함합니다. 최근 300개 버퍼와 별개로 전체 저장 로그와 같은 경계에서 재계산한 지표를 포함하며 Prometheus/Grafana DB는 포함하지 않습니다. 실행 중 **Download snapshot**은 다운로드 경계까지의 기록입니다. **Delete**는 확인 후 비활성 결과를 삭제합니다. [다운로드 구성과 한계](results.kr.md#실험-결과-다운로드)

철거하면 웹 화면도 내려가므로 먼저 다운로드한 뒤 실행하십시오.

```sh
sh scripts/swarm.sh remove
```

철거는 실패 task, task 이력 부재, offline worker가 있어도 stack 서비스를 직접 삭제합니다. Controller·Agent 정상 종료를 기다리지 않으며 최종 로그와 standalone Peer 정리 완료를 확인하지 않습니다. 실험·Prometheus·Grafana volume과 외부 Peer network는 남습니다. 보존된 volume에 다시 배포할 때는 같은 설정을 재사용하십시오. 이후 **Saved results**에서 기존 기록을 다시 볼 수 있습니다. 이전에 실행 중이던 기록은 `interrupted`로 표시하며 자동 재개하지 않습니다. 이 상태는 Peer 정리 완료를 뜻하지 않습니다.

worker가 계속 들어오고 수명에 따라 나가는 더 긴 실험은 [churn 중 무작위 반복 발행](swarm-churn-publish.kr.md)을 참고하십시오. 안정적인 bootstrap 두 개와 반복적인 발행 대상 선택, 마지막 수집·정리 단계를 사용합니다.

배포 helper는 control 노드 주소와 게시 포트로 Controller metrics 및 Prometheus 자체 링크의 브라우저용 주소를 자동 생성합니다. 별도의 `.env.swarm` 설정은 필요하지 않습니다. Prometheus에서 해당 노드의 `KPL_HTTP_PORT`에 접근할 수 있어야 합니다. 자세한 내용은 [모니터링](monitoring.kr.md)을 참고하십시오.

## 설정과 네트워크

일반적인 설정 변경은 helper를 사용합니다.

```sh
sh scripts/swarm.sh configure KPL_AGENT_CAPACITY=20 KPL_MIN_AGENTS=2 KPL_AGENT_METRICS_PORT=9091
# 선택 사항: Peer overlay를 생성하기 전에 사용하지 않는 subnet을 지정합니다.
sh scripts/swarm.sh configure KPL_PEER_SUBNET=10.11.0.0/24
sh scripts/swarm.sh config
```

`configure`는 변경 사항을 검증하고 설정 파일을 원자적으로 교체하며, 명시적으로 변경하지 않은 기존 자격 증명은 보존합니다. 기존 파일 설정에 요청한 변경만 저장하고 무관한 환경변수의 덮어쓰기 값은 저장하지 않습니다. 변경한 설정은 다음 `deploy`에서 서비스에 적용되며 실행 중인 Peer를 재설정하지 않습니다. 파일 권한은 `0600`입니다. `init`은 자격 증명을 명시적으로 지정하지 않으면 각각 32바이트 난수의 64자리 hex로 생성합니다. `config`는 비밀 값을 가리고 `credentials`만 의도적으로 표시합니다.

기본 stack 이름은 `kpl`, helper의 기본 Peer network는 `kpl-peers`입니다. `KPL_PEER_SUBNET`은 선택 사항이며 생략하면 overlay 생성 시 Docker가 subnet을 할당합니다. 지정한다면 서버·LAN·VPN·다른 Docker 네트워크와 겹치지 않아야 합니다. 기존 네트워크의 subnet이 다르면 배포가 실패하며, 설정을 바꾼다고 기존 네트워크 크기가 바뀌지는 않습니다. `/16` 같은 더 큰 subnet도 입력할 수 있지만 대규모 실험 지원을 보장하지는 않습니다. 아래 용량 한계를 참고하고 별도 배포에는 서로 다른 stack·Peer network 이름을 사용하십시오.

`KPL_CONTROL_NODE_ID`는 **데이터 volume을 보관하는 노드의 정확한 Node ID**로 고정하여 Controller·Prometheus·Grafana가 빈 로컬 volume을 가진 다른 서버로 이동하지 않게 하십시오. `nodes`로 선택할 노드를 조회합니다. helper의 기본 최소 Agent 수는 1이며 위 절차에서는 `KPL_MIN_AGENTS=2`를 명시했습니다. 사전 검사의 최소 수 조건은 UI에서 실제 등록과 여유 슬롯을 확인하는 절차를 대체하지 않습니다.

helper는 `.env.swarm`의 허용된 `KEY=VALUE` 항목을 **문자 그대로** 읽으며 파일을 source하거나 셸 표현식을 실행하지 않습니다. 바깥쪽 한 쌍의 따옴표는 제거하지만 변수·명령 치환·escape를 확장하지 않습니다. export된 환경변수가 파일보다 우선하므로 파일 변경이 적용되지 않은 것 같으면 `config`를 확인하십시오. `sudo`는 환경변수를 제거할 수 있으므로 같은 계정을 사용하고 일상 설정은 helper 파일에 보관합니다. 다른 파일은 `sh scripts/swarm.sh --env-file /path/to/lab.env config`로 선택합니다.

`publish`에서 사용자 지정 설정 파일을 사용하려면 저장소 밖이나 [`.dockerignore`](../.dockerignore)가 제외하는 루트의 `.env.*` 경로에 두십시오. helper는 빌드 전에 지정 경로와 symlink의 최종 대상을 검사하고, 제외 여부를 확인할 수 없는 저장소 내부 경로와 `Dockerfile.dockerignore` override를 거부합니다. 이를 통해 helper의 자격 증명 파일이 이미지 build context에 전달되지 않도록 합니다.

Agent는 배치된 노드의 로컬 Docker socket만 사용하며 manager API 접근이 필요 없습니다. `{{.Service.Name}}-{{.Node.ID}}`로 ID를 만들고 자신의 task가 연결된 Peer overlay IPv4를 `advertise-url`과 `self-url`로 사용합니다. 해당 daemon의 Swarm `NodeAddr`를 읽어 host-mode 포트를 통한 별도 metrics URL도 알립니다. control API는 overlay 내부 TCP 8090에 유지되고 게시되지 않으며 Peer control 및 libp2p 포트도 게시하지 않습니다. 서비스 VIP나 공통 DNSRR 주소는 개별 Agent를 안정적으로 식별할 수 없습니다. Agent는 시작 시 실제 로컬 image ID를 확인하여 Peer 생성에 사용하므로 같은 노드의 Agent·Peer 바이너리가 일치합니다. [Swarm global 서비스와 템플릿](https://docs.docker.com/engine/swarm/services/)

## 동일 태그로 이미지 업데이트

이 저장소의 소스를 갱신한 뒤 진행 중 실험을 마치고 실행하십시오.

```sh
sh scripts/swarm.sh publish
sh scripts/swarm.sh deploy
sh scripts/swarm.sh status
```

설정에는 같은 태그를 유지합니다. 이전 digest에서 태그로 바꾸거나 다른 태그를 사용하려면 먼저 실행하십시오.

```sh
sh scripts/swarm.sh configure KPL_IMAGE=registry.example.com/kpl-v3:v3
```

이후 동일하게 `publish` → `deploy`를 사용합니다. `publish`에는 digest가 아닌 태그가 필요합니다. 아키텍처가 혼합되어 있다면 native 빌드 대신 `publish --platforms linux/amd64,linux/arm64`로 manager와 모든 대상 아키텍처를 포함하십시오.

매 배포마다 설정된 태그를 pull하고 결과의 registry digest를 확인하여 `.env.swarm`을 바꾸지 않고 **이번 배포만** 해당 digest로 고정합니다. **push할 때마다 SHA를 수동으로 바꿀 필요가 없습니다.** pull에 실패하면 오래된 로컬 태그를 사용하지 않고 배포를 중단합니다. 변경되지 않는 버전으로 배포하려면 기존처럼 `repository@sha256:<digest>`를 사용할 수 있습니다. [Docker pull과 digest 고정](https://docs.docker.com/reference/cli/docker/image/pull/#pull-an-image-by-digest-immutable-identifier)

제한 시간은 초 단위이며 기본값은 `KPL_IMAGE_BUILD_TIMEOUT=1800`, `KPL_IMAGE_PUSH_TIMEOUT=600`, `KPL_IMAGE_PULL_TIMEOUT=300`입니다. 별도 push 제한은 native 게시에 적용하며, Buildx의 통합 빌드·push에는 build 제한이 적용됩니다. 태그 확인용 pull은 manager 데몬의 native 플랫폼을 사용하고 해당 pull에서만 `DOCKER_DEFAULT_PLATFORM`을 무시하며, Docker가 반환한 manifest/index digest를 유지합니다. Agent 교체 시 담당 Peer가 종료될 수 있으므로 배포 후 Agent 등록을 기다린 뒤 다음 실험을 실행하십시오.

## Manager에서 Agent 추가·철거

명시적 `NODE` 목록에는 Swarm node ID 또는 hostname을 사용합니다. `deploy`, `add-node`, `remove-node`에는 목록 대신 다음 선택 옵션 중 하나를 사용할 수 있습니다. 선택 옵션끼리 또는 선택 옵션과 명시적 노드 목록을 섞을 수 없습니다. 아래 명령은 서버를 Swarm에 가입시키거나 탈퇴시키지 않고, 해당 stack의 Agent 배치만 변경합니다.

| 선택 옵션 | 후보 노드 |
|---|---|
| `--all` | manager를 포함한 전체 노드 |
| `--all-excluding-self` | 현재 Docker context로 접속한 manager를 제외한 전체 노드 |
| `--workers` | Swarm 역할이 `worker`인 노드. manager는 제외 |

**self는 현재 context로 선택한 Docker daemon이 보고하는 `.Swarm.NodeID`입니다.** `KPL_CONTROL_NODE_ID`나 명령을 입력하는 셸의 hostname을 기준으로 하지 않습니다. manager가 여러 개인 클러스터에서는 `--all-excluding-self`가 다른 manager를 선택할 수 있습니다. 모든 manager를 제외하려면 `--workers`를 사용하십시오.

자동 `deploy`/`add-node`는 Linux·Ready·Active 노드만 포함하며 조건에 맞지 않는 후보는 건너뛰었다고 안내합니다. 자동 `remove-node`는 먼저 이 stack의 `kpl.<stack>.agent=true` label이 있는 노드로 대상을 제한합니다. 철거 대상이 접근 불가이거나 조건에 맞지 않으면 건너뛰지 않고 실패하여 기존 정리 확인 절차를 유지합니다. 선택 결과가 비어도 실패합니다.

배포·추가는 기존 배치 label에 대상을 추가하며, 선택 밖의 노드에서 label을 제거하지 않습니다. 인자 없는 `deploy`는 기존 label을 재사용합니다. 선택 옵션은 해당 명령 실행 시에만 평가하므로, 이후 Swarm에 가입한 노드에 Agent를 자동 배치하는 영구 규칙이 되지는 않습니다.

```sh
# 새로 가입한 worker를 포함하여 현재 조건에 맞는 worker 역할 노드를 추가합니다.
sh scripts/swarm.sh add-node --workers
# 해당 노드의 실험을 마친 뒤 이 stack의 선택된 Agent를 종료합니다.
sh scripts/swarm.sh remove-node --all-excluding-self
```

Makefile은 `NODES`를 그대로 전달하므로 `make swarm-add-node NODES='--workers'`도 같은 대상을 선택합니다.

| 명령 | 동작 |
|---|---|
| `sh scripts/swarm.sh deploy [NODE...]` | 노드 목록 또는 선택 옵션 하나로 배포·업데이트하며, 둘 다 없으면 기존 label 재사용. 이미지 태그를 pull·확정하거나 명시 digest 사용 |
| `sh scripts/swarm.sh status` | Docker stack 서비스·Agent task·선택된 노드 표시. API readiness는 검사하지 않음 |
| `sh scripts/swarm.sh nodes` | 현재 Swarm 노드 목록 조회 |
| `sh scripts/swarm.sh init [KEY=VALUE...]` | 검증된 비공개 설정과 자격 증명 생성. 기존 파일은 덮어쓰지 않음 |
| `sh scripts/swarm.sh configure KEY=VALUE...` | 다른 설정과 자격 증명을 보존하며 검증된 변경을 원자적으로 저장 |
| `sh scripts/swarm.sh config` / `credentials` | 비밀 값을 가린 유효 설정 조회 / API·Grafana 자격 증명 명시적 표시 |
| `sh scripts/swarm.sh login` | 설정된 이미지에서 추론한 registry에 대화형 로그인 |
| `sh scripts/swarm.sh publish [--platforms CSV]` | 설정 태그로 빌드·push. 플랫폼 지정 시 기존 Buildx builder 사용 |
| `sh scripts/swarm.sh check` | 불러온 설정으로 배포 사전 검사 재실행 |
| `sh scripts/swarm.sh access` | control 노드 URL과 선택된 각 Agent 노드의 metrics URL 출력. 접근 가능 여부나 서비스 readiness는 검사하지 않음 |
| `sh scripts/swarm.sh logs [COMPONENT] [--tail N\|all]` | 기본 최근 100줄. controller·agent·prometheus·grafana 서비스 로그 또는 access·auth 웹 JSONL 파일 조회 |
| `sh scripts/swarm.sh scenario [FILE]` | 웹 폼에 넣을 YAML 출력. 기본은 분산 smoke 예제이며 실행 요청은 하지 않음 |
| `sh scripts/swarm.sh add-node worker-c` | 기존 서비스에 Agent 배치 노드 추가. Linux·Ready·Active 상태 필요 |
| `sh scripts/swarm.sh remove-node worker-b` | 서비스에서 대상 노드를 제외하고 Agent 정상 종료 확인 후 배치 label 정리 |
| `sh scripts/swarm.sh remove` | 비정상 task가 있어도 stack 서비스 직접 삭제; 데이터 volume·Peer network 보존 |

웹 접근·로그인 기록은 `logs access`, `logs auth`로 조회합니다. Controller가 다른 노드에 있으면 호출자는 manager에 연결한 상태에서 해당 노드의 Docker daemon을 가리키는 `--context NAME`을 추가합니다. 사용 예와 파일 보존 방식은 [웹 접근·인증 로그](monitoring.kr.md#웹-접근인증-로그)를 참고하십시오.

서버를 추가하면 이후 join부터 새 용량을 사용하며 이미 실행 중인 Peer는 이동하지 않습니다. **`remove-node`는 해당 서버 Agent가 관리하는 Peer를 종료하므로 진행 중 실험에 영향을 줍니다.** 다른 stack의 label은 변경하지 않습니다.

전체 `remove`는 해당 stack이 알려진 KPL 서비스만 포함하는지 확인한 뒤, 해당 서비스 ID를 `docker service rm`으로 직접 삭제합니다. 이어 `docker stack rm`으로 stack 소유 config와 모니터링 network를 제거하고 남은 stack 서비스가 없는지 확인합니다. 실패·Rejected task, 이력 부재, offline worker는 이 경로를 막지 않습니다. 배치 label은 서비스 삭제 후 가능한 범위에서 정리합니다. label 조회·변경 실패는 안내하며, 다시 `remove`하면 서비스가 이미 없어도 남은 stack 리소스와 label을 재정리합니다. Manager API 오류나 서비스가 계속 남는 경우에는 실패를 반환합니다. 실험·Prometheus·Grafana의 **데이터 volume과 외부 Peer network는 보존**합니다.

`remove-node`는 기존처럼 Agent 서비스에서 선택 노드를 제외하고 clean container exit를 기다린 뒤 배치 label과 임시 제외 조건을 정리합니다. label부터 제거하면 정상 종료도 Swarm이 `Rejected`로 기록할 수 있기 때문입니다. offline 노드, 보존된 과거 이력을 포함한 실패 task, 이력 부재로 Peer 정리를 확인할 수 없으면 이 명령은 중단합니다. 이미 수행한 배치 변경은 자동 복구하지 않으므로 오류에 나온 task와 노드를 점검한 뒤 재시도하십시오. 다시 Agent를 실행하려는 경우 `add-node`가 남은 제외 조건도 해제합니다.

## 대시보드 로그인

`KPL_USER`·`KPL_PASSWORD`가 `KPL_API_TOKEN`을 대체합니다. `init`의 기본 사용자명은 `admin`이며 대시보드와 Grafana 비밀번호를 각각 생성합니다. `credentials`는 두 로그인 정보를 명시적으로 표시하고 `config`는 비밀번호를 가립니다. Controller의 **Log in** 화면에서 한 번 로그인하면 실험 실행·중지, 시나리오 저장, 결과 삭제, 분석·다운로드는 세션을 재사용합니다. **Log out**으로 종료하며 세션은 12시간 뒤 만료됩니다. Controller 재시작 후에는 다시 로그인합니다. 별도 API token 입력란은 없습니다.

기존 토큰 설정은 volume을 다시 만들지 않고 전환합니다.

```sh
sh scripts/swarm.sh configure KPL_USER=admin
sh scripts/swarm.sh credentials
```

전환 시 폐기된 토큰 항목을 제거하고 다른 설정을 보존하며, `KPL_PASSWORD`가 없을 때만 새 비밀번호를 생성합니다. 이전 토큰을 비밀번호로 재사용하지 않습니다. 직접 지정하려면 `configure KPL_USER=... KPL_PASSWORD=...`를 사용하십시오. 배포 시 환경변수 우선순위는 유지됩니다. 실험과 잔존 Peer를 정리한 뒤 새 이미지를 publish하고 Controller·Agent를 함께 deploy하십시오. 내부 서비스 키는 로그인 설정에서 자동 생성하므로 브라우저에 입력할 필요가 없습니다.

대시보드 데이터, SSE, 검증·분석·다운로드에는 모두 로그인이 필요합니다. 모니터링용 health/시각, Prometheus metrics·target discovery만 공개로 유지합니다. 내부 등록·telemetry와 Peer discovery에는 서비스 키를 사용하며 Peer 설정 JSON 권한은 `0600`입니다. Grafana는 별도 계정입니다. HTTP는 암호화되지 않으므로 신뢰 관리망 또는 HTTPS proxy를 사용하십시오. 세션 쿠키를 사용하는 curl 예제는 [API 인증](api.kr.md#인증)을 참고하십시오.

## 이전 v3 stack에서 전환

**Legacy 예외:** 신규 배포라면 이 절차를 건너뛰십시오. 아래 직접 Docker 명령은 표시가 없는 이전 stack을 한 번 전환할 때만 사용하며, 위의 일반 운영 절차는 helper로 진행합니다. helper의 소유권 검사를 우회하지 않습니다.

helper는 기존 서비스가 `${KPL_STACK_NAME}_{controller,agent,prometheus,grafana}`이고 `io.kpl.application=kp2plab-v3` 서비스 label이 있을 때만 관리합니다. 이 표시가 없는 이전 v3 stack은 같은 이름이어도 변경을 거부합니다.

먼저 기존 실험을 종료하고 Peer 정리를 확인하십시오. 기존 stack 이름, control Node ID, Peer network 이름, 이미지 참조와 자격 증명을 그대로 셸에 export한 뒤, 최신 `stack.swarm.yaml`을 한 번 직접 재배포합니다. `.env.swarm`은 Docker가 직접 읽지 않으므로 파일의 실제 값을 환경에 명시해야 합니다.

```sh
# 위의 기존 배포 변수가 export된 manager에서 실행합니다.
docker node update --label-add "kpl.${KPL_STACK_NAME}.agent=true" worker-a
docker node update --label-add "kpl.${KPL_STACK_NAME}.agent=true" worker-b
control_address=$(docker node inspect --format '{{.Status.Addr}}' "$KPL_CONTROL_NODE_ID")
case "$control_address" in *:*) control_address=[$control_address] ;; esac
export KPL_CONTROLLER_METRICS_URL="http://$control_address:${KPL_HTTP_PORT:-8080}/metrics"
export KPL_PROMETHEUS_EXTERNAL_URL="http://$control_address:${PROMETHEUS_PORT:-9090}/"
docker stack config --compose-file stack.swarm.yaml >/dev/null
docker stack deploy --with-registry-auth --compose-file stack.swarm.yaml "$KPL_STACK_NAME"
sh scripts/swarm.sh status
```

새 배치는 공용 `kpl.agent` 대신 stack별 `kpl.<stack>.agent` label을 사용합니다. 기존 Peer network가 `kpl-swarm-peers`였다면 그 이름을 `.env.swarm`에도 유지하십시오. helper 기본 이름으로 바꾸면 이전 Peer 소유 범위와 달라집니다. 이후 배포·추가·철거는 helper를 사용합니다.

## 분배와 용량

시작 기본값과 Agent별 UI 예외는 [Agent 용량과 배치](agents.kr.md)에서 설정합니다. 해당 문서에서 balanced/random/고정 배치, 비례 할당, 생성 허용과 정리 중 점유를 설명합니다. 아래는 배포 네트워크 제약입니다.

Docker는 일반적인 overlay에 `/24` 규모를 권장합니다. 주소에는 Peer뿐 아니라 서비스 task·endpoint도 포함되므로 `서버 수 × capacity`를 주소 수만큼 꽉 채우지 마십시오. 기본 20은 시작용 설정이며 성능 보장이 아닙니다. v2의 `/16`을 그대로 확대하거나 capacity만 올리는 방식으로 수천 Peer 지원을 주장할 수 없습니다. 현재 구성은 단일 공통 Peer overlay를 사용하므로 수백·수천 Peer에는 여러 네트워크 간 도달성 설계와 별도 부하 시험이 필요합니다. [Swarm overlay 크기 제한](https://docs.docker.com/engine/swarm/networking/#overlay-network-size-limitations)

Peer는 Agent로 향하는 경로의 overlay IPv4만 libp2p에 광고합니다. 다른 서버에서 접근할 수 없는 `docker_gwbridge` 주소가 bootstrap/Identify에 섞이지 않게 합니다. tc 지연·손실은 Peer namespace의 송신에 적용되며, 실제 NIC·VXLAN·서버 부하의 추가 지연은 그대로 존재합니다. 호스트 간 one-way latency 비교에는 시계 동기화도 필요합니다.

## 분석과 운영

Prometheus는 5초마다 Controller의 HTTP service-discovery endpoint에 등록된 Agent metrics URL을 요청합니다. 각 Agent는 자신의 Swarm node 주소와 설정된 host-mode metrics 포트를 알리므로 수집이 service VIP를 통과하지 않습니다. task가 Controller에 등록된 뒤에만 target이 됩니다. Grafana는 KP2PLab 대시보드를 자동 구성합니다. 다음 항목을 함께 확인하십시오.

- `up{job="kpl-agent"}` 대상 수와 실제 Agent task 수
- Controller의 `kpl_agent_active_nodes`, `kpl_agent_capacity`, `kpl_agent_up`
- Agent의 `kpl_local_cleanup_pending`, `kpl_local_telemetry_queue_events`
- 실험의 실제 ready/started 시각, 실패 수 및 서버별 CPU·메모리·네트워크 사용량

Go process 지표는 Controller·Agent 자체만 나타내며 Peer 전체의 자원 사용량이 아닙니다. 호스트 모니터링 또는 별도 container exporter로 Peer 부하를 확인하십시오. 전체 노드 상태 보고·Controller 저장/집계·중앙 HTTP 수집, Docker CLI의 생성/삭제 비용도 확장 한계입니다. 종료된 노드 기록과 실행별 Prometheus series가 유지되므로 긴 churn 실험은 메모리와 수집 지연을 함께 측정해야 합니다.

이 stack은 host mode로 control 노드의 8080·9090·3000과 선택된 모든 Agent 노드의 `KPL_AGENT_METRICS_PORT`(기본 9091)를 게시합니다. Agent metrics 포트는 모든 대상 노드에서 비어 있어야 하며 같은 노드를 공유하는 별도 stack에는 서로 다른 포트를 지정해야 합니다. 또한 `KPL_HTTP_PORT`, `PROMETHEUS_PORT`, `GRAFANA_PORT` 및 Swarm TCP 포트 2377·7946과 달라야 하며 config helper와 배포 사전검사가 이러한 충돌을 거부합니다. control 노드와, 직접 브라우저 접근이 필요한 경우 운영자의 신뢰 관리망에서 이 포트를 허용하십시오. metrics endpoint는 읽기 전용이지만 인증되지 않으므로 신뢰하지 않는 출발지는 차단해야 합니다. Agent control API 8090과 Peer 포트는 내부에 유지하며 게시하지 않습니다. control 노드 포트는 관리망 방화벽이나 인증 reverse proxy로 접근을 제한하십시오. 대시보드 조회·변경은 로그인으로 보호하며 모니터링 endpoint는 공개로 유지합니다. Grafana 익명 접속은 이 stack에서 비활성화했습니다. [Swarm host mode 게시](https://docs.docker.com/engine/swarm/services/#publish-ports)

모니터링 설정은 Swarm configs로 전달하므로 모든 서버에 저장소를 복사할 필요가 없습니다. Swarm config는 immutable이므로 설정 파일 변경 시 `stack.swarm.yaml`의 config key와 해당 참조를 함께 버전명으로 바꾸어 새 config를 배포하십시오. 데이터 volume은 같은 control Node ID에서 유지됩니다.

## 장애·업데이트·종료

Agent update/rollback은 `stop-first`입니다. `start-first`로 바꾸면 같은 서버·Agent ID의 두 task가 동시에 실행되어 이전 Peer 정리와 충돌할 수 있습니다. Agent 교체 시 담당 Peer가 종료되므로 진행 중인 실험의 무중단 업그레이드를 보장하지 않습니다. 서버를 추가해도 이미 실행 중인 Peer를 옮기지 않으며, 이후 join부터 새 용량을 사용합니다.

노드 drain은 Swarm 서비스 task에 적용됩니다. v3 Peer는 standalone 컨테이너이므로 Swarm이 직접 이전하지 않습니다. 정상 Agent 종료에서 Peer를 정리하고, 비정상 종료 후에는 동일 Agent ID·Peer network name으로 같은 노드에서 재시작할 때 잔존 컨테이너를 회수합니다. 노드 재가입, stack/service 이름 변경 또는 네트워크 이름 변경 시 이전 소유 범위의 잔존 Peer는 해당 서버에서 label을 확인해 별도로 정리해야 합니다. 전체 호스트 정지·네트워크 단절을 다른 서버의 Peer 재생성으로 숨기지 않습니다.

Controller는 단일 인스턴스이며 공유 DB/leader election을 구현하지 않았습니다. `replicas: 1`을 유지하십시오. control 노드 장애 시 자동으로 빈 로컬 volume을 쓰는 다른 노드로 이동하지 않으며, 백업 복원과 새 Node ID 지정이 필요합니다. Controller는 실시간 실험 상태·counter를 시작 시 메모리에 복원하거나 실행 중 실험을 자동 재개하지 않습니다. 보존된 파일은 재시작 후에도 대시보드의 **Saved results**와 [결과 다운로드 API](results.kr.md#실험-결과-다운로드)에서 받을 수 있습니다. 이전에 실행 중이었던 기록은 `interrupted`로 표시하지만 Peer 정리를 확인한 상태는 아닙니다.

Controller crash만으로 Agent의 Peer가 종료되지는 않습니다. 정상 Controller 종료는 활성 run을 취소한 뒤 등록된 Agent에 남은 Peer 제거를 요청하며, `stop-all`을 생략한 완료된 단일 run의 Peer도 포함합니다. 계획된 업데이트 전에는 활성 run을 완료하거나 취소하고 이 정리가 끝날 때까지 기다리십시오. Agent에 연결할 수 없거나 정리가 실패하면 종료 오류를 보고하므로 해당 호스트의 잔존 컨테이너를 확인해야 합니다.

전체 철거와 비정상 서비스 복구에는 `sh scripts/swarm.sh remove`를 사용하십시오. Controller·Agent 정리를 기다리지 않고 서비스 정의를 삭제합니다. 실제 컨테이너 종료는 Docker가 설정된 종료 유예에 따라 처리합니다. 명령 성공은 Manager에 stack 서비스가 남지 않았다는 뜻이며, offline 호스트의 모든 컨테이너가 종료되었다는 뜻은 아닙니다. Agent 비정상 종료 후 standalone Peer가 남을 수 있으므로 해당 호스트를 점검하거나 같은 소유 범위로 재시작하여 회수하십시오. 계획된 종료라면 철거 전에 실험을 완료·취소하고 결과를 다운로드하십시오.

SIGTERM을 받으면 Controller는 새 실험 수락을 중단하고 HTTP 연결, 실험 job, Peer 정리와 최종 상태 저장을 기다립니다. 기본 `jobShutdownTimeout: 3m`은 job 종료와 Peer 정리에 각각 적용되므로 stack은 Controller 종료 유예를 `10m`로 설정합니다. Agent에는 최대 175초의 Peer 정리, HTTP handler 종료와 제한된 telemetry drain을 포함하는 `4m`를 제공합니다. 시나리오의 `jobShutdownTimeout`을 늘리면 Controller 유예도 함께 늘리고 외부 서비스 관리자가 이 시간을 단축하지 않도록 하십시오.

Controller는 실행 취소 후에도 Agent 이벤트 수신을 유지하고, 완료된 실험이 남긴 Peer와 Agent 전송 큐까지 정리한 뒤 HTTP를 닫습니다. 이 최종 Agent 정리·전송에는 최대 190초를 추가로 허용합니다. 전체 `remove`는 이 과정을 기다리지 않습니다. `KPL_CONTROLLER_STOP_TIMEOUT`은 기존 설정 호환을 위해 읽지만 더 이상 사용하지 않습니다. `KPL_AGENT_STOP_TIMEOUT`은 `remove-node`의 정리 확인과 각 Docker 조회에 적용하며, `KPL_DOCKER_TIMEOUT`은 삭제 명령과 서비스 제거 확인 대기에 적용합니다.

### 데이터 저장소와 재시작

Controller 기록, Prometheus 시계열과 Grafana 데이터는 `KPL_CONTROL_NODE_ID`의 별도 named volume에 저장됩니다. 일반 서비스 재생성과 `swarm.sh remove`는 이 volume을 보존합니다. 시나리오, 실행 기록과 삭제 표식을 유지하려면 Controller 데이터 디렉터리 전체를 보존하고 Prometheus 이력과 Grafana 설정이 필요하면 별도로 백업하십시오. Volume 삭제는 일반 종료 절차에 포함되지 않습니다. 사용자 지정 저장소나 bind mount를 사용하면 컨테이너 UID/GID의 쓰기 권한을 확인하고 경로가 Docker daemon 호스트에서 해석됨을 고려하십시오.

Controller 데이터 디렉터리에 쓸 수 없으면 시작이 실패합니다. 실행 메타데이터는 임시 파일에 쓴 뒤 같은 파일시스템에서 rename하여 부분 JSON 읽기를 방지하며 이벤트 로그는 append로 기록합니다. 쓰기마다 강제로 디스크에 동기화하지 않으므로 전원 장애 때 최근 기록을 잃을 수 있습니다. 재시작 후 보존 결과에 접근할 수 있지만 실시간 실행과 counter는 복원하지 않습니다.

백그라운드 분석의 상태·완료 JSON·비교 요약도 Controller 볼륨에 보존됩니다. 완료 결과는 재시작 후 재사용하며 미완료·취소 작업은 다시 접수합니다. 일반 실행 ZIP에는 이 분석 캐시가 포함되지 않으므로 보존 경계는 [분석 파일과 이미지](results.kr.md#분석-파일과-이미지-보존)를 참고하십시오.

## 검증 범위

현재 Go 회귀 suite와 셸 회귀 네 개인 `test-swarm-agent.sh`, `test-check-swarm.sh`, `test-swarm-config.sh`, `test-swarm.sh`는 `make test-linux` 또는 `docker build --target test -t kpl-v3:test .`로 실행합니다. 셸 테스트는 Docker 응답을 모사하여 비정상 task·offline 노드·이력 부재에도 직접 철거, 다른 stack 변경 방지, 삭제·조회 오류, 부분 정리 후 재시도, `remove-node` 정상 종료 확인과 설정 파일 처리를 검증하며 실제 클러스터를 배포하지 않습니다. 별도 브라우저 테스트와 명시적으로 활성화하는 커널 테스트는 [개발 검사](development.kr.md#컨테이너와-브라우저-회귀-검사)를 참고하십시오.

이전 검증 기록은 별도 Docker 29.7.2 daemon에서 SIGTERM 종료용 Controller·Agent 테스트 서비스로 `remove-node`, `add-node`, 전체 `remove`를 실행했다고 보고합니다. 정상 종료의 `shutdown / PID 0 / exit 0`, 재배치 후 새 task, 최종 stack 서비스 0개, 다른 노드를 제외할 때 영향 없는 실행 중 task ID 유지가 기록되어 있습니다. 원본 실행 산출물은 이 저장소에 포함되어 있지 않으므로 현재 checkout이나 전체 registry·Peer 실험의 검증 결과가 아닌 과거 보고입니다. 이 기록은 현재 `remove`의 서비스 직접 삭제 방식으로 변경하기 전 보고입니다. 현재 구현은 [`scripts/swarm.sh`](../scripts/swarm.sh)와 mock 테스트를 참고하십시오.

Swarm 사전 검사는 `sh scripts/swarm.sh check`를 사용하십시오. helper가 설정을 불러와 배포 검사를 실행합니다. 설정·배치·overlay를 검사하지만 모든 Agent 노드에 커널 규칙을 설치하거나 호스트 간 트래픽을 검증하지는 않습니다. 실제 네트워크 조건은 선택한 Swarm 노드에서 시나리오를 실행해 검증하십시오. 배포 사전 검사나 패키지 테스트만으로 분산 smoke 실험·실제 cross-host VXLAN 통신·처리량 측정을 대체할 수는 없습니다.

최소 두 서버에서 [분산 실험 예제](../examples/swarm-smoke.yaml)를 실행해 서로 다른 Agent에 ready Peer가 생기는지, 광고 주소가 Peer overlay에 속하는지, publish/deliver가 양쪽 서버에서 발생하는지 확인하십시오. 예제는 boot 1개와 worker 5개를 생성하므로 총 capacity 6 이상이 필요합니다. 그 뒤 목표 Peer 수를 단계적으로 올려 ready 지연·실패율·분배·자원 사용량을 기록하십시오. 네트워크 단절/Agent 재시작 시 stale 제외·잔존 컨테이너 정리와 Prometheus 대상 교체도 확인해야 합니다.

### 2026-09-04 기존 stack 실행 검증

아래 과거 기록은 manager 관리 CLI `scripts/swarm.sh` 추가 전이며 원본 실행 아카이브는 이 저장소에 포함되어 있지 않습니다. 현재 checkout이나 CLI의 `init/deploy/add-node/remove-node/remove` 전체 경로를 실제 클러스터에서 검증한 결과는 아닙니다.

검증 환경은 **동일 WSL2 Linux 커널 위의 격리된 Docker 데몬 두 개**입니다. Docker 29.7.2의 manager·worker와 실제 attachable overlay를 구성했습니다. 기존 호스트의 Swarm 상태는 변경하지 않았습니다. 이는 두 데몬 사이의 배치·VXLAN·서비스 탐색 검증이며, 서로 다른 물리 서버의 NIC·MTU·장애·대규모 처리량 검증은 아닙니다. 이미지는 양쪽 데몬에 같은 로컬 이미지를 미리 적재했으며 registry 인증·pull 경로는 별도 검증 대상입니다.

| 항목 | 결과 |
|---|---|
| Linux 코드 검증 | 전체 Go 테스트 통과. 마지막 시간 예산 수정 후 Agent 패키지와 Swarm shell 회귀 테스트 재검증 통과 |
| Swarm 배포 | global Agent 2개, Controller 1개, Prometheus 1개, Grafana 1개 정상 실행. 실제 manager에서 사전 검사 통과 |
| 노드 분배 | capacity 4인 Agent 두 개에 Peer 6개를 3개씩 배치, 모두 ready 및 상호 연결 확인 |
| Peer 주소 | 모든 광고 주소가 Peer overlay IPv4의 TCP 20000 한 개. gateway bridge 주소 미포함 |
| 네트워크·발행 | worker 25ms 지연·2ms jitter·0.5% 손실, 4096-byte payload 50회 발행, 수신 300회(Agent별 150회) |
| 모니터링 | Prometheus의 Agent 대상 2개와 Controller·자기 수집 모두 UP. Grafana 대시보드 UID `kpl-experiments`와 데이터소스 API 연결 OK |
| task 교체 | 이미지 업데이트 후 Node 기반 Agent ID 유지, task IP 변경 및 Prometheus 대상 갱신 확인 |
| 정상 종료 | `run-20260904T040249Z-34d32bbb9559adbba6519a558c6cc107` completed, 양쪽 activeNodes 0, 실제 잔존 Peer 컨테이너 각각 0개 |

위 과거 발행·수신 수는 누적 `kpl_events_total` 기준이며 당시 snapshot 요약은 최근 이벤트 버퍼를 사용했습니다. 현재 v3 실행 요약은 이 버퍼와 독립적으로 누적하며 [실험 지표](experiment-metrics.kr.md)의 정의를 따릅니다. 첫 부하 실행에서 생성·시작 전체의 공유 45초 제한이 Peer를 종료시키는 문제를 발견해 단계별 예산으로 수정한 뒤 위 결과를 확인했습니다.

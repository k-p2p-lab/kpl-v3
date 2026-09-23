# 개발 가이드

[English](development.md) | 한국어

[문서 안내](README.kr.md) · [저장소](../README.kr.md)

정식 Go 모듈 경로는 `github.com/k-p2p-lab/v3`이며 [`go.mod`](../go.mod)는 Go 1.25 이상을 요구합니다. 이 문서는 v3 개발 절차를 다룹니다. 프로젝트 목표와 개념 설계는 [Hub](https://github.com/k-p2p-lab/hub/blob/master/README.kr.md)에서 관리합니다.

## Linux 빌드와 검증

Linux 환경의 저장소 루트에서 다음 명령을 실행하십시오.

```sh
go build -o bin/kpl ./cmd/kpl
go test -buildvcs=false ./... -timeout 120s
go vet ./...
sh scripts/test-swarm-agent.sh
sh scripts/test-check-swarm.sh
sh scripts/test-swarm-config.sh
sh scripts/test-swarm.sh
```

동시성 검사는 다음 명령으로 실행합니다.

```sh
GORACE=atexit_sleep_ms=0 go test -race -buildvcs=false ./... -count=1 -timeout 120s
```

Agent 테스트는 Docker 명령을 모의하기 위해 테스트 실행 파일을 여러 번 자식 프로세스로 실행합니다. 이 설정은 각 자식 프로세스가 종료할 때 race 런타임이 기다리는 시간을 없애며, race 검출은 계속 활성화합니다.

실행하지 않고 시나리오를 검증하는 명령은 다음과 같습니다.

```sh
./bin/kpl validate --scenario examples/smoke.yaml
```

## Swarm에서 개발 실험 실행

변경한 이미지를 빌드하고 게시한 뒤 [Swarm 절차](swarm.kr.md)로 배포하십시오. Linux 노드 하나로 개발용 Swarm을 구성할 수 있으며 `KPL_MIN_AGENTS=1`과 `deploy --all`을 사용합니다. Agent는 활성 Swarm 노드와 명시적으로 설정한 이미지 및 attachable Peer overlay를 요구합니다. 호스트 간 배치와 연결을 검증할 때에는 최소 두 Agent 노드에서 분산 smoke 시나리오를 실행하십시오.

## 컨테이너와 브라우저 회귀 검사

이미 사용할 수 있는 Linux Docker daemon이 있다면 호스트에 Go를 설치하지 않고 Docker 테스트 target을 실행합니다.

```sh
make test-linux
# Equivalent when make is unavailable:
docker build --target test -t kpl-v3:test .
```

[`Dockerfile`](../Dockerfile)의 test target은 Swarm 셸 회귀 스크립트 네 개와 Go 패키지 테스트를 실행합니다. 셸 테스트는 Docker 응답을 모의합니다. 별도 컨테이너 두 개를 사용하는 네트워크 조건 테스트는 명시적으로 활성화해야 하며 일반 test 빌드에서는 건너뜁니다. 따라서 이 target만으로 실제 Swarm 배치, 호스트 간 연결, 커널 네트워크 조건 동작을 검증한 것은 아닙니다.

브라우저 로직에는 Docker test target과 별개인 Node.js 회귀 테스트도 있습니다. Node.js가 설치되어 있으면 다음 명령을 실행하십시오.

```sh
node --test internal/webui/*_test.cjs
```

## Windows 작업 공간 규칙

Windows에서는 편집과 정적 검사를 수행합니다. Windows 네이티브 `go test`, `go test -c`, 생성된 테스트 실행 파일 또는 이를 호출하는 `make test` 등의 helper를 실행하지 마십시오. 대화형 방화벽·보안·권한 상승 창이 필요한 명령도 실행하지 않습니다. 실행 검증에는 위의 Linux Docker test target이나 기존의 비대화형 Linux 호스트를 사용하십시오. 둘 다 사용할 수 없으면 정적 검사와 실행 검증 여부를 구분하여 보고합니다.

런타임 이미지는 `CGO_ENABLED=0`으로 빌드합니다. 셸 스크립트, Dockerfile과 Makefile은 [`.gitattributes`](../.gitattributes)를 통해 LF 줄바꿈을 유지합니다. 런타임 이미지는 빌드한 애플리케이션과 실행 유틸리티를 포함하며 Go나 Node.js 개발 도구를 포함하지 않습니다.

## 문서 관리

프로젝트 공통 목적·설계·연구·논문은 [Hub](https://github.com/k-p2p-lab/hub/blob/master/README.kr.md), 실행 동작·운영 계약은 v3에서 관리합니다. 영어가 기본이며 변경한 안내는 대응하는 `.kr.md`에도 반영합니다. 문서 제목과 UI 조작 이름을 구분하고 UI 이름은 실제 영어 표시를 유지합니다.

[공통 문서 관리 기준](https://github.com/k-p2p-lab/hub/blob/master/docs/DOCUMENTATION.kr.md)의 저장소 책임·분할·언어 쌍 규칙을 따르고 [문서 안내](README.kr.md)를 최신으로 유지하십시오. v3에서 `experiments`는 실행·복구, `results`는 저장 파일, `monitoring`은 운영 지표·로그, `api`는 HTTP·SSE 계약을 담당합니다. `experiment-metrics`와 `bandwidth`는 수식, `visualization`은 그림 조작의 기준입니다. 정의를 복제하지 않고 기준 문서로 연결합니다.

v2 자료가 나란히 있을 때 분석 소스 목록은 다음과 같이 확인합니다.

```sh
python3 scripts/audit-v2-analysis.py --v2 ../v2
```

이 검사는 v2 분석 필드·소스 hash의 변경을 찾으며 수치 동등성이나 현재 runtime 검증을 증명하지 않습니다. 수행한 검증과 보존된 과거 기록을 구분하고, 문서 수정 때 오류 수정 일지를 별도로 추가하지 않습니다.

---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit CLI(orbit) 바이너리로 로컬 코드 그래프를 구축하고 쿼리합니다. GitLab 계정이나 네트워크 연결이 필요하지 않습니다.
title: GitLab Orbit CLI(`orbit`)로 GitLab Orbit Local 사용
---

{{< details >}}

- 티어: Free, Premium, Ultimate
- 제공 서비스: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- 상태:  베타

{{< /details >}}

{{< history >}}

- GitLab 19.0에서 [도입](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)되었으며 [실험](https://docs.gitlab.com/policy/development_stages_support/#experiment)입니다.
- GitLab 19.1에서 [변경](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)되어 [베타](https://docs.gitlab.com/policy/development_stages_support/#beta) 단계가 되었습니다.

{{< /history >}}

GitLab Orbit CLI(`orbit`)는 모든 로컬 리포지토리에 대한 코드 그래프를 구축하고 로컬 DuckDB 파일에 대해 쿼리합니다. GitLab 연결이 필요하지 않습니다.

## 설치 {#install}

단일 라인 설치 프로그램으로 독립형 `orbit` 바이너리를 설치합니다:

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash
```

`orbit`을 `PATH`에 추가합니다. 새 터미널을 열고 설치를 확인합니다:

```shell
orbit help
```

`npm install -g @gitlab/orbit`로 npm에서도 설치할 수 있습니다.

이미 GitLab CLI(`glab`)를 사용 중인 경우 `glab orbit local --install`로 관리되는 바이너리를 대신 설치할 수 있습니다. 해당 바이너리는 `glab orbit local <command>`로 호출되며 `orbit` 직접 호출이 아닙니다. [glab을 사용하여 GitLab Orbit Local 사용](glab.md)을 참조하세요.

### 소스에서 빌드 {#build-from-source}

GitLab Orbit에 기여하거나 출시되지 않은 빌드를 실행하려면 바이너리를 직접 컴파일합니다.

전제 조건:

- [Rust 도구 모음](https://rustup.rs/)(안정 버전)
- 도구 관리용 [`mise`](https://mise.jdx.dev/)

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise install
mise run build:cli
```

컴파일된 바이너리는 `target/release/orbit`에 있습니다. `PATH`에 추가하거나 직접 호출합니다.

## 리포지토리 인덱싱 {#index-a-repository}

```shell
orbit index /path/to/your/repo
```

GitLab Orbit는 리포지토리를 파싱하고 DuckDB 그래프를 `~/.orbit/graph.duckdb`에 씁니다. 여러 리포지토리를 인덱싱할 수 있습니다. 각각은 프로젝트 ID와 매니페스트 테이블의 브랜치로 범위가 지정됩니다.

| 플래그 | 목적 |
|------|---------|
| `--threads` | 워커 스레드 개수입니다. `0`(기본값)은 CPU 코어 수를 자동으로 감지합니다. |
| `--stats` | JSON 출력에 상세한 통계를 포함합니다. |
| `--verbose` | stderr에 자세한 로깅을 수행합니다. |
| `--db` | DuckDB 파일 경로를 재정의합니다(기본값: `~/.orbit/graph.duckdb`). |

## 스키마 검사 {#inspect-the-schema}

`orbit schema`은 로컬 DuckDB 그래프의 모든 테이블과 열을 나열합니다:

```shell
orbit schema
```

테이블 이름을 위치 인수로 전달하여 출력 범위를 좁힙니다:

```shell
orbit schema gl_definition              # scoped to one table
orbit schema gl_definition gl_edge      # scoped to two tables
```

| 플래그 | 목적 |
|------|---------|
| `--raw` | 기본 테이블 보기 대신 JSON을 내보냅니다. |
| `--db` | DuckDB 경로를 재정의합니다. 기본값은 `~/.orbit/graph.duckdb`입니다. |

## 로컬 그래프에 대해 SQL 실행 {#run-sql-against-the-local-graph}

```shell
orbit sql 'SELECT count(*) FROM gl_definition'
orbit sql -F json 'SELECT name FROM gl_definition LIMIT 3'
echo 'SELECT 1+1' | orbit sql -
orbit sql --file query.sql
```

| 플래그 | 목적 |
|------|---------|
| `-F`, `--format` | `table`(기본값), `json`, `ndjson` 또는 `csv`입니다. |
| `-f`, `--file` | 파일에서 SQL을 읽습니다. |
| `--db` | DuckDB 경로를 재정의합니다. 기본값은 `~/.orbit/graph.duckdb`입니다. |

## 인덱싱된 리포지토리 나열 {#list-indexed-repositories}

그래프는 하나 이상의 리포지토리를 보유할 수 있습니다. 포함된 내용을 확인하려면 다음을 실행합니다:

```shell
orbit list
orbit list -F json
```

각 행은 리포지토리 경로, 브랜치, 커밋, 인덱싱 상태, 마지막 인덱싱 시간 및 상태가 `error`일 때의 오류 메시지를 보고합니다:

```plaintext
+------------------------+--------+------------+---------+---------------------+---------------+
| repo_path              | branch | commit_sha | status  | last_indexed_at     | error_message |
+------------------------+--------+------------+---------+---------------------+---------------+
| /home/dev/workspace/kg | main   | 9606ae8... | indexed | 2026-05-18 10:14:02 |               |
| /tmp/cli-test          | main   | 654f3a6... | indexed | 2026-05-18 10:13:55 |               |
+------------------------+--------+------------+---------+---------------------+---------------+
```

인덱싱이 실패한 리포지토리는 `status = error`로 기록되고 `error_message`의 이유가 제공되므로, 실패했거나 인덱싱할 수 없는 리포지토리가 조용히 사라지지 않고 계속 표시됩니다.

| 플래그 | 목적 |
|------|---------|
| `-F`, `--format` | `table`(기본값), `json`, `ndjson` 또는 `csv`입니다. |
| `--db` | DuckDB 경로를 재정의합니다. 기본값은 `~/.orbit/graph.duckdb`입니다. |

아직 인덱싱된 항목이 없으면 `orbit list`은 `0`로 종료됩니다. 테이블 보기는 아무것도 인쇄하지 않습니다. 구조화된 형식은 유효한 빈 출력(`[]` for `json`, `ndjson`의 경우 레코드 없음)을 내보내므로 `orbit list -F json | jq`과 같은 파이프라인이 계속 작동합니다.

## MCP 서버로 실행 {#run-as-an-mcp-server}

로컬 그래프를 stdio를 통해 MCP 호환 AI 에이전트에 노출합니다:

```shell
orbit mcp serve
```

`run_sql`, `get_graph_schema` 및 `index`를 `~/.orbit/graph.duckdb`에 대해 제공합니다. 클라이언트별 구성에 대해 [MCP를 통해 연결](mcp.md)을 참조하세요.

## AI 어시스턴트 설정 {#set-up-your-ai-assistant}

`orbit setup`는 AI 코딩 어시스턴트가 grep을 찾기 전에 그래프를 참고하도록 구성합니다. 구성하려는 어시스턴트의 이름을 지정합니다:

```shell
orbit setup claude
```

지원되는 어시스턴트는 `claude`, `codex`, `opencode` 및 `pi`입니다. 기본적으로 지침은 원격 GitLab Orbit 그래프를 가리킵니다. 대신 로컬 그래프를 가리키려면 `--local`를 전달합니다:

```shell
orbit setup claude --local
```

### 변경 사항 {#what-it-changes}

이 명령은 사용자에게 속한 파일을 수정합니다. 자신의 상황에서 실행되지 않으며, 호출할 때만 실행됩니다.

명명한 모든 어시스턴트에 대해 `orbit setup`은 다음을 수행합니다:

- 어시스턴트의 명령 파일(예: `CLAUDE.md` 또는 `AGENTS.md`)에 블록을 추가합니다. 블록은 `<!-- orbit:setup:begin -->` 및 `<!-- orbit:setup:end -->` 마커 사이에 위치하며, 마커 외부의 모든 항목은 그대로 둡니다. 명령을 다시 실행하면 두 번째 복사본을 추가하지 않고 제자리에 블록이 바뀝니다.
- 어시스턴트가 지원하는 경우 어시스턴트의 JSON 구성에 항목을 추가합니다. Claude Code의 경우 `PreToolUse` 후크가 `settings.json`에 있으며, OpenCode의 경우 플러그인 파일과 등록입니다. 항목은 `orbit` 마커를 운반하며, 표시된 항목만 대체되거나 제거됩니다.

기본적으로 `~/.claude/CLAUDE.md`과 같은 사용자 전역 구성에 씁니다. 현재 프로젝트에 쓰려면 `--project`를 전달하거나, 특정 프로젝트 디렉토리를 대상으로 `--dir <path>`을 전달합니다.

`orbit setup`이 처음으로 기존 파일을 수정하기 전에 원본을 `<name>.orbit-backup` 옆에 복사합니다. 원본을 원하면 해당 복사본을 수동으로 복원합니다. 기존 백업은 덮어쓰이지 않으므로 복사본은 항상 `orbit` 이전 버전을 보유합니다.

`--project`을 주의해서 사용하세요. 프로젝트 명령 파일은 일반적으로 버전 제어에 커밋되므로 변경 사항이 `git status`에 나타나며 팀원에게 도달할 수 있습니다. 사용자 전역 범위(기본값)는 사용자에게만 영향을 미칩니다.

### 제거 {#remove-it}

변경을 취소하려면 다음을 실행합니다:

```shell
orbit setup claude --remove
```

마커로 구분된 블록과 표시된 JSON 항목을 제거하고 각 파일의 나머지는 그대로 둡니다. 파일에 `orbit` 항목만 포함된 경우 삭제됩니다. 어시스턴트 이름을 생략하여 모든 항목의 설정을 제거합니다. 백업 파일은 삭제되지 않습니다.

`orbit setup`이 파일에 접근하지 않도록 하려면 건너뛰고 동일한 명령 블록과 후크를 수동으로 추가합니다.

## 스토리지 {#storage}

그래프는 `~/.orbit/graph.duckdb`에 저장됩니다. 여러 리포지토리가 같은 데이터베이스를 공유합니다. 파일을 삭제하여 다시 시작합니다.

## CLI 구성 {#configure-the-cli}

`orbit config`은 `~/.orbit/settings.json`에서 지속된 설정을 읽고 쓰습니다. 저장된 설정은 모든 이후 실행에 적용됩니다.

```shell
orbit config list                          # all settings and their saved values
orbit config get telemetry.enabled         # one setting
orbit config set telemetry.enabled false   # save a setting
```

| 설정 | 값 | 기본값 | 목적 |
|---------|--------|---------|---------|
| `telemetry.enabled` | `true`, `false` | `true` | CLI가 사용 원격 측정을 보내는지 여부입니다. |

## 원격 측정 {#telemetry}

CLI는 GitLab 제품 분석 서비스에 사용 이벤트를 전송하므로 팀이 GitLab Orbit의 사용 방식을 볼 수 있습니다. 각 이벤트는 실행된 명령만 기록합니다. 리포지토리 내용, 파일 경로 또는 쿼리 텍스트는 전송되지 않습니다. 원격 측정은 기본적으로 활성화되어 있습니다.

저장된 설정으로 끄거나 CI의 환경 변수로 끕니다:

```shell
orbit config set telemetry.enabled false   # persists for every run
export ORBIT_TELEMETRY_ENABLED=false        # for CI or one shell
```

환경 변수는 저장된 설정을 재정의합니다.

| 변수 | 목적 |
|----------|---------|
| `ORBIT_TELEMETRY_ENABLED` | `false`은 원격 측정을 비활성화하고, `true`는 이를 활성화합니다. 저장된 설정을 재정의합니다. |
| `ORBIT_TELEMETRY_COLLECTOR_URL` | 테스트용 다른 수집기로 이벤트를 보냅니다. GitLab 수집기로 기본값이 설정됩니다. |

## 청구 {#billing}

GitLab Orbit Local은 GitLab Credits를 사용하지 않습니다. 모든 처리는 로컬입니다.

## 다음으로 시도할 내용 {#what-to-try-next}

- [MCP를 통해 연결](mcp.md) \- Claude Code, Codex 및 기타 에이전트를 로컬 그래프에 연결합니다.
- [glab을 사용하여 GitLab Orbit Local 사용](glab.md) - `glab orbit local`을 통해 CLI를 호출합니다.
- [스키마 참조](../../remote/schema.md) \- 사용 가능한 노드 유형 및 속성입니다.
- [요리책](../../remote/cookbook.md) \- 일반적인 사용 사례를 위한 복사-붙여넣기 쿼리입니다.

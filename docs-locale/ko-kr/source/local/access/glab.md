---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "GitLab CLI의 glab orbit local 및 glab orbit setup을 통해 GitLab Orbit Local을 설치, 색인화 및 쿼리합니다."
title: GitLab CLI(`glab`)를 사용하여 GitLab Orbit Local 사용
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

> [!disclaimer]

[GitLab CLI(`glab`)](https://docs.gitlab.com/cli/)는 GitLab Orbit Local을 AI 에이전트와 설치, 실행 및 통합하는 정식 방법입니다. `glab orbit local`는 `glab orbit remote`를 반영하므로 GitLab 인스턴스를 쿼리하든 로컬 머신을 쿼리하든 동일한 패턴이 작동합니다.

> [!note]
`glab orbit local` 및 `glab orbit setup`는 현재 `glab` 1.94 이상에서 출시되었습니다.

2개의 최상위 명령:

- `glab orbit local`: 관리되는 `orbit` 바이너리를 래핑하여 로컬 그래프를 인덱싱하고 쿼리합니다.
- `glab orbit setup`: 액세스를 검증하고, GitLab Orbit 스킬을 설치하며, 로컬 바이너리를 설치하는 가이드 온보딩입니다.

## 전제 조건 {#prerequisites}

- `glab` 1.94 이상이 설치되어 있습니다.
- 인덱싱할 로컬 Git 리포지토리입니다.

바이너리가 설치되면 `glab orbit local`을 사용하기 위해 GitLab 계정이나 네트워크 연결이 필요하지 않습니다.

## 설치 {#install}

관리되는 `orbit` 바이너리를 설치합니다:

```shell
glab orbit local --install
```

`glab`은 바이너리를 다운로드하고 체크섬을 검증하며 최신 상태로 유지합니다. 설치를 검증합니다:

```shell
glab orbit version
```

## AI 에이전트 설정 {#set-up-your-ai-agent}

`glab orbit setup`은 AI 코딩 에이전트를 구성하여 그래프를 참조하도록 하며, 각 에이전트의 지시 파일에 관리되는 섹션을 작성하고 GitLab Orbit 스킬을 설치합니다.

```shell
glab orbit setup
```

`glab orbit setup --help`을 실행하여 전체 옵션 목록을 확인합니다: 구성할 에이전트, 프로젝트 또는 사용자 범위, 로컬 또는 원격 그래프 및 제거하려면 `--remove`를 사용합니다.

스킬은 `orbit` 바이너리를 직접 구동합니다. MCP 클라이언트를 로컬 그래프에 연결하려면 [MCP를 통해 연결](mcp.md)을 참조하세요.

[GitLab Orbit 스킬을 수동으로 설치](../../ai_coding_agents.md)할 수도 있습니다(`glab skills install --global orbit` 사용).

## 리포지토리 인덱싱 {#index-a-repository}

```shell
glab orbit local index /path/to/your/repo
```

| 플래그 | 목적 |
|------|---------|
| `--threads` | 워커 스레드 개수입니다. `0`(기본값)은 CPU 코어 수를 자동으로 감지합니다. |
| `--stats` | JSON 출력에 상세한 통계를 포함합니다. |
| `--verbose` | stderr에 자세한 로깅을 수행합니다. |

## 그래프에 대해 SQL 실행 {#run-sql-against-the-graph}

```shell
glab orbit local sql 'SELECT count(*) FROM gl_definition'
echo 'SELECT name FROM gl_definition LIMIT 3' | glab orbit local sql -
```

## 스키마 검사 {#inspect-the-schema}

`glab orbit local schema`은 로컬 DuckDB 그래프의 모든 테이블과 열을 나열합니다:

```shell
glab orbit local schema
```

테이블 이름을 위치 인수로 전달하여 출력 범위를 좁힙니다:

```shell
glab orbit local schema gl_definition              # scoped to one table
glab orbit local schema gl_definition gl_edge      # scoped to two tables
```

| 플래그 | 목적 |
|------|---------|
| `--raw` | 기본 테이블 보기 대신 JSON을 내보냅니다. |
| `--db` | DuckDB 경로를 재정의합니다. 기본값은 `~/.orbit/graph.duckdb`입니다. |

## MCP 서버로 실행 {#run-as-an-mcp-server}

로컬 그래프를 MCP 호환 AI 에이전트에 노출합니다:

```shell
glab orbit local mcp serve
```

`run_sql`, `get_graph_schema` 및 `index`를 MCP 프로토콜을 통해 `~/.orbit/graph.duckdb`에 대해 제공합니다. 전체 에이전트 통합 가이드는 [MCP를 통해 연결](mcp.md)을 참조하세요.

## 종료 코드 {#exit-codes}

`glab orbit local`는 성공 시 `0`를 반환하고 실패 시 0이 아닌 종료 코드를 반환하며, stderr에 세부 정보가 표시됩니다. 스크립트와 에이전트는 성공 또는 실패 시 분기할 수 있습니다.

## 청구 {#billing}

GitLab Orbit Local은 GitLab Credits를 사용하지 않습니다. 모든 처리는 로컬입니다.

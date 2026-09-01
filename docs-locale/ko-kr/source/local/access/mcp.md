---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit Local MCP 서버로 AI 에이전트를 로컬 그래프에 연결합니다.
title: GitLab Orbit Local MCP 서버
---

{{< details >}}

- 티어: Free, Premium, Ultimate
- 제공 서비스: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- 상태:  실험적 기능

{{< /details >}}

{{< history >}}

- GitLab 19.2에서 [소개됨](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/643), [실험](https://docs.gitlab.com/policy/development_stages_support/#experiment) 단계입니다.

{{< /history >}}

GitLab Orbit Local [Model Context Protocol](https://modelcontextprotocol.io/)(MCP) 서버를 사용하면 AI 도구 및 애플리케이션을 로컬 그래프에 안전하게 연결할 수 있습니다. Claude Code, Codex, Cursor 및 OpenCode 같은 AI 어시스턴트는 그래프에 액세스하고 이에 대해 SQL 쿼리를 작성할 수 있습니다.

GitLab Orbit Local MCP 서버는 상태 비저장입니다. 이는 서버가 다음과 같은 특성을 가집니다:

- 로컬 그래프를 쿼리합니다. GitLab 인스턴스가 아닙니다.
- 결과를 캐시하거나 쿼리 기록을 유지하지 않습니다.
- 여러 클라이언트에 독립적으로 응답할 수 있습니다.

## 전제 조건 {#prerequisites}

- 다음 도구 중 하나를 설치합니다:
  - [GitLab Orbit CLI](./cli.md)(`orbit`)
  - [GitLab CLI](./glab.md)(`glab orbit`)

## 클라이언트를 GitLab Orbit Local MCP 서버에 연결 {#connect-a-client-to-the-gitlab-orbit-local-mcp-server}

GitLab Orbit Local MCP 서버는 stdio 전송을 지원합니다. 인수 및 명령은 MCP 클라이언트와 로컬 환경에 따라 다릅니다.

### Claude Code 연결 {#connect-claude-code}

Claude Code는 세 가지 범위 중 하나에 MCP 서버 구성을 저장합니다. 사용 사례와 일치하는 범위를 선택합니다.

| 범위 | 가용성 | 저장 위치 |
| ----- | ------------ | --------- |
| `local`(기본값) | 현재 프로젝트에서만 사용자만 | `~/.claude.json` |
| `user` | 모든 프로젝트에서만 사용자만 | `~/.claude.json` |
| `project` | 리포지토리를 체크아웃하는 모든 사람 | 리포지토리 루트의 `.mcp.json` |

현재 프로젝트에 대해 MCP 서버를 추가하려면 다음을 실행합니다:

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```shell
claude mcp add orbit-local -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```shell
claude mcp add orbit-local -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

모든 프로젝트에 대해 서버를 추가하려면 다음을 실행합니다:

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```shell
claude mcp add orbit-local --scope user -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```shell
claude mcp add orbit-local --scope user -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

리포지토리를 확인하는 모든 사람을 위해 서버를 추가하려면 다음을 실행합니다:

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```shell
claude mcp add orbit-local --scope project -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```shell
claude mcp add orbit-local --scope project -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

`.mcp.json` 파일을 직접 편집할 수도 있습니다:

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "glab",
      "args": ["orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

MCP 서버가 연결되었는지 확인하려면 다음을 실행합니다:

```shell
claude mcp list
```

연결한 후 [AI 어시스턴트 설정](./cli.md)합니다.

> [!note]
프로젝트 범위 명령의 경우, Claude Code는 `mcp.json` 파일을 생성하기 전에 승인을 요청합니다. 승인할 때까지 `claude mcp list`은 서버를 `Pending approval`로 표시합니다.

### Codex 연결 {#connect-codex}

Codex는 MCP 서버 구성을 `~/.codex/config.toml`에 저장합니다. Codex는 항상 모든 프로젝트에 대해 서버를 구성합니다.

Codex에 연결하려면 다음을 실행합니다:

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```shell
codex mcp add orbit-local -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```shell
codex mcp add orbit-local -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

서버가 등록되었는지 확인하려면 다음을 실행합니다:

```shell
codex mcp list
```

연결한 후 [AI 어시스턴트 설정](./cli.md)합니다.

### Cursor 연결 {#connect-cursor}

Cursor는 `mcp.json` 파일에서 MCP 서버 구성을 읽습니다. 서버를 사용 가능하게 할 범위와 일치하는 범위를 선택합니다.

| 범위 | 가용성 | 저장 위치 |
| ----- | ------------ | --------- |
| 프로젝트 | 현재 프로젝트에서만 사용자만 | 리포지토리 루트의 `.cursor/mcp.json` |
| 전역 | 모든 프로젝트에서만 사용자만 | `~/.cursor/mcp.json` |

Cursor에 연결하려면 원하는 범위에 대해 `mcp.json` 파일을 생성하거나 편집합니다:

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "type": "stdio",
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "type": "stdio",
      "command": "glab",
      "args": ["orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

서버가 연결되었는지 확인하려면 다음을 실행합니다:

```shell
agent mcp list
```

### OpenCode 연결 {#connect-opencode}

MCP 서버 구성을 `opencode.json`(리포지토리 루트) 또는 `~/.config/opencode/opencode.json`에 추가하여 모든 프로젝트에서 사용합니다:

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "orbit-local": {
      "type": "local",
      "command": ["orbit", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "orbit-local": {
      "type": "local",
      "command": ["glab", "orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

서버가 연결되었는지 확인하려면 다음을 실행합니다:

```shell
opencode mcp list
```

연결한 후 [AI 어시스턴트 설정](./cli.md)합니다.

### 다른 MCP 클라이언트 연결 {#connect-other-mcp-clients}

GitLab Orbit Local MCP 서버는 연결 URL을 노출하지 않습니다. URL을 필요로 하는 클라이언트는 연결할 수 없습니다.

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

GitLab Orbit CLI를 사용하여 연결하려면 클라이언트의 MCP 구성 파일을 편집합니다:

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

GitLab CLI를 사용하여 연결하려면:

1. `glab orbit local --install`를 실행합니다. 이 명령은 `orbit` 바이너리를 다운로드합니다.
1. 그런 다음 클라이언트의 MCP 구성 파일을 편집합니다:

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "glab",
      "args": ["orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

서버가 연결되었는지 확인하려면 클라이언트가 `run_sql`, `get_graph_schema` 및 `index` 도구를 나열하는지 확인합니다.

## MCP 도구 {#mcp-tools}

GitLab Orbit Local MCP 서버는 로컬 그래프와 상호작용하는 도구 집합을 제공합니다.

### `index` {#index}

리포지토리 또는 리포지토리 디렉터리를 로컬 그래프에 인덱싱합니다.

예제:

```plaintext
Index my checked out project.
```

### `get_graph_schema` {#get_graph_schema}

스키마를 가져옵니다. 로컬 그래프에 있는 테이블 이름, 열 및 데이터 유형을 포함합니다.

예제:

```plaintext
Use the `get_graph_schema` tool to show me what tables are in my local graph.
```

### `run_sql` {#run_sql}

로컬 그래프에 대해 읽기 전용 SQL을 실행합니다. 명령문 배열을 가져오고 동일한 인덱스에서 명령문당 하나의 JSON 행 배열을 반환합니다.

단일 `run_sql` 호출은 최대 약 1MB를 반환합니다(호출의 모든 명령문을 함께 세어서). 더 큰 결과는 실패하며 에이전트는 더 좁은 쿼리로 다시 시도합니다. 에이전트가 복구되지 않으면 더 적은 결과를 요청합니다.

예제:

```plaintext
Show me the most used imports in this repository.
```

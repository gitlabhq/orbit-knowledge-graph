---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Claude Code, Codex 또는 MCP와 호환되는 모든 AI 에이전트를 GitLab Orbit에 연결하여 두 가지 MCP 도구인 list_commands와 invoke_command를 사용합니다."
title: MCP를 통해 연결
---

{{< details >}}

- 티어: Premium, Ultimate
- 제공 서비스: GitLab.com
- 상태:  베타

{{< /details >}}

{{< history >}}

- [GitLab 18.10에서 도입](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)되었으며 [기능 플래그](https://docs.gitlab.com/administration/feature_flags/)로 명명된 `knowledge_graph`입니다. 기본적으로 비활성화되었습니다. 이 기능은 [실험](https://docs.gitlab.com/policy/development_stages_support/#experiment)입니다.
- GitLab 19.1에서 [베타](https://docs.gitlab.com/policy/development_stages_support/#beta)로 [변경](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)되었습니다.

{{< /history >}}

> [!flag]
이 기능의 사용 가능 여부는 기능 플래그에 의해 제어됩니다. 자세한 내용은 이력을 참조하세요. 이 기능은 테스트 가능하지만 프로덕션 사용은 준비되지 않았습니다.

GitLab Orbit은 MCP와 호환되는 모든 AI 에이전트가 GitLab Orbit 명령을 검색하고 그래프에 대해 실행할 수 있도록 하는 두 가지 MCP 도구를 제공합니다. 이를 Claude Code, OpenAI Codex 또는 Model Context Protocol을 지원하는 다른 도구와 함께 사용합니다.

## 전제 조건 {#prerequisites}

- GitLab Orbit이 [그룹에서 활성화](../getting-started.md)되어 있습니다.
- GitLab에 인증되어 있습니다. `glab auth login`을 실행합니다(기본적으로 OAuth 사용; `read_api` 범위가 있는 개인 액세스 토큰도 작동합니다).
- 인증에 쿼리하려는 그룹에 액세스할 수 있습니다.
- MCP 클라이언트가 네이티브 HTTP를 통해 직접 연결되는 경우(`mcp-remote` 제외), OAuth 요청에 `mcp_orbit` 범위가 포함되어야 합니다. 아래의 Gemini CLI 예제를 참조하십시오.

## MCP 도구 {#mcp-tools}

| 도구 | 설명 |
|------|-------------|
| `list_commands` | 설명 및 입력 스키마와 함께 사용 가능한 GitLab Orbit 명령을 나열합니다. |
| `invoke_command` | 이름과 매개변수로 명령을 실행합니다. 입력된 결과를 반환합니다. |

`invoke_command`을 통해 사용 가능한 명령:

| 명령 | 설명 |
|---------|-------------|
| `query_graph` | GitLab Orbit 쿼리 DSL을 사용하여 그래프 쿼리를 실행합니다. |
| `get_graph_schema` | 현재 스키마를 가져옵니다: 모든 노드 유형, 해당 속성 및 관계 유형입니다. |
| `get_query_dsl` | `query_graph` JSON DSL 문법 및 버전을 반환합니다. |
| `get_response_format` | `query_graph` 응답 JSON 스키마 및 버전을 반환합니다. |

## MCP 클라이언트 연결 {#connect-your-mcp-client}

MCP 클라이언트를 `https://gitlab.com/api/v4/orbit/mcp`을 가리키도록 구성합니다.

**Claude Code**는 기본 제공 HTTP 전송을 통해 GitLab Orbit 끝점을 지원합니다. 한 가지 명령으로 등록합니다:

```shell
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

첫 번째 `list_commands` 또는 `invoke_command` 호출은 GitLab으로 인증하기 위해 브라우저를 엽니다. JSON 구성 편집이 필요하지 않습니다.

> [!note]
Claude Code는 HTTP를 통해 직접 연결합니다. `npx mcp-remote`를 Claude Code와 함께 사용하지 마십시오. 끝점을 stdio 프로세스로 래핑하여 기본 전송과 충돌하고 "연결 실패" 오류를 발생시킵니다. 위에 표시된 `claude mcp add --transport http` 명령을 대신 사용하십시오.

일부 클라이언트는 로컬 stdio MCP 서버만 지원합니다. 해당 경우 [`mcp-remote`](https://www.npmjs.com/package/mcp-remote)가 GitLab Orbit 끝점을 로컬 명령으로 래핑합니다.

**Cursor, Codex, and other JSON-config clients** \- 에이전트의 MCP 구성에 추가:

```json
{
  "mcpServers": {
    "gitlab-orbit": {
      "command": "npx",
      "args": ["mcp-remote", "https://gitlab.com/api/v4/orbit/mcp"]
    }
  }
}
```

**opencode** - `~/.config/opencode/opencode.json`에 추가:

```json
{
  "mcp": {
    "gitlab-orbit": {
      "type": "local",
      "command": ["npx", "mcp-remote", "https://gitlab.com/api/v4/orbit/mcp"]
    }
  }
}
```

> [!note]
opencode는 `"type": "local"`가 필요하며 명령 및 인수를 단일 배열에 함께 배치합니다. 별도의 `args` 필드를 사용하거나 `type`를 생략하면 `ConfigInvalidError`가 발생합니다.

**Gemini CLI** \- 네이티브 HTTP 전송을 통해 GitLab Orbit 끝점을 지원합니다. `~/.gemini/settings.json`에 추가:

```json
{
  "mcpServers": {
    "gitlab-orbit": {
      "url": "https://gitlab.com/api/v4/orbit/mcp",
      "type": "http",
      "timeout": 5000,
      "oauth": {
        "enabled": true,
        "scopes": ["mcp_orbit"]
      }
    }
  }
}
```

`gemini mcp add gitlab-orbit https://gitlab.com/api/v4/orbit/mcp -t http -s user`로도 생성할 수 있으며, 그 후 `oauth.scopes` 블록을 직접 추가할 수 있습니다.

> [!note]
네이티브 HTTP MCP 클라이언트는 `mcp_orbit` OAuth 범위를 명시적으로 요청해야 합니다. `oauth.scopes: ["mcp_orbit"]` 없이는 GitLab의 다른 곳에 이미 로그인되어 있어도 인증이 실패합니다. 네이티브 HTTP 전송의 클라이언트가 인증할 수 없으면 MCP 서버 구성에 이 범위를 추가합니다.
>
> 이전 Gemini CLI 구성은 `url` + `type: "http"` 대신 `httpUrl`를 사용할 수 있습니다. `httpUrl`는 계속 작동하지만 사용이 중단되었습니다. 새 설정의 경우 `url` + `type`을 사용합니다.

**Antigravity** \- Antigravity IDE 및 CLI는 `~/.gemini/config/mcp_config.json`에서 동일한 MCP 구성을 읽습니다. Antigravity는 아직 원격 서버에 대한 MCP OAuth 흐름을 실행하지 않습니다(`serverUrl` 항목이 토큰 없이 `initialize`를 보내고 `Unauthorized`로 인해 실패함). 따라서 `mcp-remote`으로 끝점을 래핑합니다:

```json
{
  "mcpServers": {
    "gitlab-orbit": {
      "command": "npx",
      "args": ["mcp-remote", "https://gitlab.com/api/v4/orbit/mcp"]
    }
  }
}
```

> [!note]
`oauth` 블록은 여기에 필요하지 않습니다. `mcp-remote`는 끝점의 OAuth 메타데이터에서 `mcp_orbit` 범위를 발견하고 처음 사용할 때 권한을 부여하기 위해 브라우저를 엽니다.

인증은 기존 `glab auth login` 세션을 사용합니다. 복사하거나 붙여넣을 토큰이 없습니다. 지원되는 클라이언트: Claude Code, OpenCode, Cursor, Codex, Gemini CLI, Antigravity

> [!note]
계획된 `glab orbit setup` 하위 명령은 GitLab Orbit 스킬을 설치하고 한 단계에서 이 MCP 구성을 작성합니다. 배포될 때까지 위에 표시된 대로 MCP 클라이언트를 수동으로 구성합니다.

또한 [GitLab Orbit 스킬을 수동으로 설치](../../ai_coding_agents.md)하여 에이전트에 쿼리 레시피, DSL 지침 및 문제 해결을 제공할 수 있습니다.

### 테스트 {#test-it}

AI 에이전트에서 다음을 요청합니다:

> "GitLab Orbit을 사용하여 내 그룹에서 최근에 업데이트된 상위 5개 프로젝트를 나열합니다."

프로젝트 이름과 경로가 있는 입력된 결과를 다시 받아야 합니다. 그렇다면 연결되어 있습니다. 그렇지 않으면 `glab auth status`을 실행하여 인증되었는지 확인하고 GitLab Orbit이 그룹 중 하나 이상에서 활성화되어 있는지 확인합니다.

## 청구 {#billing}

MCP를 통한 쿼리는 GitLab Credits를 사용합니다. `invoke_command` 호출마다 `query_graph`을 실행하면 GitLab 구독에서 크레딧을 사용합니다. `list_commands`와 `get_graph_schema`, `get_query_dsl` 및 `get_response_format` 명령은 무료입니다.

## 도구 사용 {#using-the-tools}

연결되면 AI 에이전트에 GitLab Orbit 도구를 직접 사용하도록 지시합니다:

명령 및 스키마 발견:
> "`list_commands`을 사용하여 사용 가능한 GitLab Orbit 명령을 표시한 다음 `get_graph_schema` 명령을 실행하여 GitLab Orbit이 인덱싱하는 노드 유형을 표시합니다."

쿼리 실행:
> "`query_graph` 명령을 사용하여 그룹에서 열린 머지 리퀘스트가 가장 많은 상위 10개 프로젝트를 찾습니다."

영향 반경 분석:
> "GitLab Orbit을 사용하여 이 프로젝트의 `AuthService`를 직접 또는 전이적으로 가져오는 모든 파일을 찾습니다."

온보딩:
> "GitLab Orbit을 사용하여 이 그룹의 주요 서비스, 해당 언어 및 의존하는 프로젝트를 매핑합니다."

에이전트는 JSON 쿼리 DSL을 구성하고 사용자를 대신하여 `query_graph` 명령을 호출합니다. 결과를 정확하게 제어하려면 원시 JSON 쿼리를 직접 전달할 수도 있습니다.

## 예: query_graph에 대한 수동 invoke_command 호출 {#example-manual-invoke_command-call-for-query_graph}

아래 쿼리를 `invoke_command`로 `{"command_name": "query_graph", "parameters": {"query": ...}}`와 함께 전달합니다:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]},
    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "mr", "to": "p"}
  ],
  "group_by": ["p"],
  "aggregations": [
    { "count": "mr", "as": "open_mrs" }
  ],
  "aggregation_sort": "-open_mrs",
  "limit": 10
}
```

## 문제 해결 {#troubleshooting}

### Claude Code에서 "연결 실패" {#failed-to-connect-in-claude-code}

Claude Code는 기본 제공 HTTP MCP 지원을 제공합니다. GitLab Orbit을 `--transport http` 대신 `npx mcp-remote`로 등록한 경우, `mcp-remote` 래퍼가 기본 전송과 충돌하는 로컬 stdio 프로세스를 만듭니다.

수정하려면 끊어진 등록을 제거하고 HTTP 전송으로 다시 추가합니다:

```shell
claude mcp remove gitlab-orbit
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

### 처음 사용할 때 "인증 필요" {#needs-authentication-on-first-use}

이는 예상된 동작입니다. 첫 번째 `list_commands` 또는 `invoke_command` 호출은 GitLab으로 OAuth를 완료하기 위해 브라우저를 엽니다. 브라우저 흐름이 트리거되지 않으면 세션을 확인하십시오:

```shell
glab auth status
```

세션이 만료된 경우 다시 인증합니다:

```shell
glab auth login
```

### 연결 후 쿼리 오류 {#query-errors-after-connecting}

쿼리 시간 오류(유효성 검사 실패, 빈 결과, 속도 제한)의 경우 [GitLab Orbit 스킬 문서](../../ai_coding_agents.md)를 참조하십시오. 여기에는 DSL 지침, 쿼리 레시피 및 종료 코드 진단이 포함됩니다. 인라인 지침을 위해 스킬을 설치합니다:

```shell
glab skills install --global orbit
```

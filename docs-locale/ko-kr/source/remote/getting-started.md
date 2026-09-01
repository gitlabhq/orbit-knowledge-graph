---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab.com에서 GitLab Orbit Remote를 활성화하고 첫 번째 쿼리를 실행합니다.
title: GitLab Orbit Remote 시작하기
---

{{< details >}}

- 티어: Premium, Ultimate
- 제공 서비스: GitLab.com
- 상태:  베타

{{< /details >}}

{{< history >}}

- GitLab 18.10에서 [도입](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)되었으며 [기능 플래그](https://docs.gitlab.com/administration/feature_flags/)의 이름은 `knowledge_graph`입니다. 기본적으로 비활성화되었습니다. 이 기능은 [실험](https://docs.gitlab.com/policy/development_stages_support/#experiment) 단계입니다.
- GitLab 19.1에서 [변경](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)되어 [베타](https://docs.gitlab.com/policy/development_stages_support/#beta) 단계가 되었습니다.

{{< /history >}}

> [!flag]
이 기능의 사용 가능 여부는 기능 플래그에 의해 제어됩니다. 자세한 내용은 이력을 참조하세요. 이 기능은 테스트 가능하지만 프로덕션 사용은 준비되지 않았습니다.

## 전제 조건 {#prerequisites}

- GitLab Orbit을 활성화하려면 최상위 그룹에서 Owner 역할이 필요합니다.
- 인덱싱된 후 그룹을 쿼리하려면 Reporter 역할 이상이 필요합니다.
- 보안 데이터를 보려면 보안 관리자 역할이 필요합니다. 자세한 내용은 [GitLab Orbit 쿼리에 필요한 역할](security.md#roles-required-to-query-gitlab-orbit)을 참조하세요.

GitLab Orbit은 최상위 그룹만 인덱싱합니다. 하위 그룹 및 프로젝트는 자동으로 인덱싱을 상속합니다.

## 1단계:  GitLab Orbit 활성화 {#step-1-enable-gitlab-orbit}

1. 왼쪽 사이드바에서 **Your Work**를 확장합니다.
1. **Orbit** > **구성**을 선택합니다.
1. **인덱스** 목록에서 최상위 그룹을 찾습니다.
1. **활성화**를 전환합니다.

GitLab Orbit은 즉시 인덱싱을 시작합니다. 초기 인덱싱은 소규모 그룹의 경우 몇 분, 수천 개의 프로젝트가 있는 그룹의 경우 최대 30분이 소요됩니다.

언제든지 인덱싱 상태를 확인하세요:

```shell
glab orbit remote status
```

## 2단계:  첫 번째 쿼리 실행 {#step-2-run-your-first-query}

GitLab Orbit Remote는 동일한 그래프를 세 가지 방식으로 노출합니다. 쿼리하는 사람과 일치하는 것을 선택하세요:

| 방법 | 최적용 | 설정 | 청구 |
|---|---|---|---|
| **GitLab Duo Agent Platform** | GitLab UI의 최종 사용자 | 없음 | 무료 |
| **MCP** | Claude Code, Codex, 기타 AI 에이전트 | 일회성 에이전트 구성 | GitLab Credits |
| **REST API** | 스크립트, 대시보드, 사용자 지정 도구 | API 토큰 | GitLab Credits |

### GitLab Duo Agent Platform(설정 없음) {#gitlab-duo-agent-platform-no-setup-required}

GitLab Orbit은 GitLab Duo Agent Platform에 통합되어 있습니다. GitLab Duo 에이전트, Planner 에이전트, Security Analyst 에이전트, Data Analyst 에이전트, CI Expert 에이전트 및 Developer 플로우는 GitLab Orbit의 `list_commands` 및 `invoke_command` 도구를 자동으로 호출하여 `query_graph` 및 `get_graph_schema`와 같은 명령을 실행하며, 그래프 순회로 질문에 최적으로 답할 수 있을 때 실행됩니다. 도구 선택 또는 구성이 필요하지 않습니다.

예를 들어, `deploy_user` 메서드의 이름을 바꾸도록 요청하는 작업 항목을 제출합니다. Developer 플로우는 GitLab Orbit을 사용하여 이를 호출하는 모든 서비스를 식별한 다음 각 서비스를 업데이트하는 머지 리퀘스트를 작성합니다.

GitLab Duo 쿼리는 무료이며 GitLab Credits를 소비하지 않습니다.

### MCP(Claude Code, Codex, 기타 에이전트) {#mcp-claude-code-codex-other-agents}

설정을 위해 [MCP를 통해 GitLab Orbit 사용](access/mcp.md)을 참조하세요. 구성되면 `query_graph` 및 `get_graph_schema`의 두 가지 도구가 있습니다.

### AI 에이전트용 GitLab Orbit 스킬 설치 {#install-the-gitlab-orbit-skill-for-ai-agents}

GitLab Orbit 스킬은 AI 에이전트에 쿼리 레시피, DSL 지침 및 문제 해결을 제공하여 첫 번째 시도에서 올바른 GitLab Orbit 쿼리를 작성하도록 합니다:

```shell
glab skills install --global orbit
```

프로젝트 범위 설치, 업데이트 지침 및 스킬에 포함된 내용을 위해 [GitLab Orbit 스킬을 사용하여 AI 코딩 에이전트 설정](../ai_coding_agents.md)을 참조하세요.

### REST API {#rest-api}

`your-group`을(를) GitLab Orbit을 활성화한 최상위 그룹 경로로 바꿉니다. `full_path` 필터는 쿼리의 범위를 지정하여 GitLab Orbit의 선택성 유효성 검사를 통과하도록 합니다.

요청 본문을 `request.json`에 넣습니다:

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [{
      "id": "p",
      "entity": "Project",
      "columns": ["name", "full_path"],
      "filters": {
        "full_path": {"starts_with": "your-group/"}
      }
    }],
    "limit": 10
  },
  "format": "raw"
}
```

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data @request.json \
  "https://gitlab.com/api/v4/orbit/query"
```

## 다음으로 시도할 내용 {#what-to-try-next}

- [GitLab Orbit이 인덱싱하는 것](indexing.md) \- 쿼리를 작성하기 전에 적용 범위 이해
- [스키마 참조](schema.md) \- 28가지 노드 유형 및 해당 속성 탐색
- [요리책](cookbook.md) \- 일반적인 사용 사례에 대한 복사-붙여넣기 쿼리
- [GitLab Orbit Local 시작하기](../local/getting-started.md) \- 로컬 리포지토리 오프라인 쿼리

---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: glab 1.94에서 사용 가능한 glab orbit remote를 사용하여 명령줄에서 GitLab Orbit을 쿼리합니다. glab orbit setup 도우미는 향후 glab 릴리스에서 계획되어 있습니다.
title: GitLab CLI(`glab`)를 사용하여 GitLab Orbit 활용
---

{{< details >}}

- 티어: Premium, Ultimate
- 제공 서비스: GitLab.com
- 상태:  베타

{{< /details >}}

{{< history >}}

- [GitLab 18.10에서 도입됨](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) [기능 플래그](https://docs.gitlab.com/administration/feature_flags/)로 이름이 `knowledge_graph`입니다. 기본적으로 비활성화되었습니다. 이 기능은 [실험](https://docs.gitlab.com/policy/development_stages_support/#experiment)입니다.
- GitLab 19.1에서 [베타](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)로 [변경](https://docs.gitlab.com/policy/development_stages_support/#beta)되었습니다.

{{< /history >}}

> [!flag]
이 기능의 사용 가능 여부는 기능 플래그에 의해 제어됩니다. 자세한 내용은 이력을 참조하세요. 이 기능은 테스트 가능하지만 프로덕션 사용은 준비되지 않았습니다.

<!-- -->

> [!disclaimer]

[GitLab CLI(`glab`)](https://docs.gitlab.com/cli/)는 명령줄에서 GitLab Orbit을 설정하고 쿼리하는 표준적인 방법입니다.

`glab orbit`은 관리되는 `orbit` 이진 파일을 실행합니다. 각 명령을 이진 파일로 전달하며, `glab`이 이를 다운로드하고, 검증하고, 최신 상태로 유지합니다. `glab orbit remote <command> --help`을 실행하여 이진 파일의 자체 명령 참조를 확인합니다.

- `glab orbit remote`: GitLab Orbit Remote REST API를 쿼리합니다. `glab`이 GitLab 자격 증명을 자동으로 주입합니다. `glab` 1.94 이상에서 사용 가능합니다.
- `glab orbit setup`은 GitLab Orbit 스킬을 설치하고 AI 에이전트를 설정하는 안내형 온보딩입니다.

## 전제 조건 {#prerequisites}

- GitLab Orbit이 [그룹에서 활성화되어](../getting-started.md) 있습니다.
- `glab`이 설치되고 인증되었습니다:

  ```shell
  glab auth login
  ```

- 사용자가 GitLab Orbit이 활성화된 최상위 그룹에 액세스할 수 있어야 합니다.

## AI 에이전트 설정 {#set-up-your-ai-agent}

`glab orbit setup`은 AI 에이전트 코딩 에이전트(Claude Code, OpenCode, Cursor, Codex, Gemini CLI)를 구성하여 그래프를 참고하고 GitLab Orbit 스킬을 설치합니다:

```shell
glab orbit setup
```

대신 MCP 클라이언트를 연결하려면 [수동으로 구성](mcp.md#connect-your-mcp-client)하세요.

## 명령줄에서 GitLab Orbit 쿼리 {#query-gitlab-orbit-from-the-command-line}

`glab orbit remote`을 사용하여 GitLab Orbit Remote REST API를 직접 호출합니다. 스크립팅, 디버깅 및 쿼리 작성 전 스키마 탐색에 유용합니다. `glab` 1.94 이상이 필요합니다.

`glab`은 자격 증명을 해결하고 이를 이진 파일로 전달하므로 추가 인증 단계가 필요하지 않습니다. `--hostname`을 사용하여 특정 GitLab 인스턴스를 대상으로 지정하고, `--yes`을 사용하여 스크립트의 일회성 실행 확인을 건너뜁니다.

| 하위 명령 | 엔드포인트 | 목적 |
|------------|----------|---------|
| `glab orbit remote status` | `GET orbit/status` | 클러스터 상태. |
| `glab orbit remote schema [node...]` | `GET orbit/schema` | 그래프 온톨로지. 위치 지정 인수가 특정 노드를 확장합니다. |
| `glab orbit remote dsl` | `GET orbit/schema/dsl` | 쿼리 DSL JSON Schema. 쿼리 본문 모양의 단일 정보 출처입니다. |
| `glab orbit remote tools` | `GET orbit/tools` | 전체 DSL JSON Schema를 포함한 MCP 도구 매니페스트입니다. |
| `glab orbit remote query [file\|-]` | `POST orbit/query` | 파일 또는 표준 입력에서 쿼리를 실행합니다. |
| `glab orbit remote graph-status` | `GET orbit/graph_status` | 네임스페이스, 프로젝트 또는 전체 경로에 대한 인덱싱 진행률입니다. |

### 스키마 발견 {#discover-the-schema}

```shell
glab orbit remote status
glab orbit remote schema
glab orbit remote schema MergeRequest Project
glab orbit remote dsl
glab orbit remote tools
```

### 쿼리 실행 {#run-a-query}

`your-group`을 자신의 그룹 경로로 바꾸세요. 이 쿼리는 해당 그룹의 처음 다섯 개 프로젝트를 반환합니다:

`query.json`에 요청 본문을 입력하세요:

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [{
      "id": "p",
      "entity": "Project",
      "filters": {
        "full_path": {"starts_with": "your-group/"}
      }
    }],
    "limit": 5
  }
}
```

```shell
glab orbit remote query query.json
```

`--response-format` 플래그는 본문의 `response_format`에 매핑됩니다:

- `--response-format llm` - AI 에이전트 소비에 최적화된 간결한 텍스트입니다.
- `--response-format raw` - `jq`로 파이핑하기에 적합한 구조화된 JSON입니다.

`--response-format`이 설정되지 않으면 본문의 `response_format`가 우선이며, `llm`이 최종 폴백입니다.

### 인덱싱 진행률 확인 {#check-indexing-progress}

정확히 하나의 범위 기능 플래그를 전달하세요:

```shell
glab orbit remote graph-status --full-path your-group/your-project
glab orbit remote graph-status --namespace-id 24
glab orbit remote graph-status --project-id 2
```

## 종료 코드 {#exit-codes}

`glab orbit remote`은 HTTP 오류를 안정적인 종료 코드에 매핑하므로 스크립트와 에이전트가 stderr를 분석하지 않고도 이를 기반으로 분기할 수 있습니다.

| 상태 | 종료 코드 | 의미 |
|--------|-----------|---------|
| `200` | `0` | 성공입니다. |
| `404` | `2` | `knowledge_graph` 기능 플래그가 꺼져 있거나 경로 오타입니다. |
| `401` | `3` | 토큰 누락 또는 만료됨. |
| `403` | `4` | 사용 가능한 지식 그래프 활성화된 네임스페이스가 없습니다. |
| `429` | `5` | 속도 제한됨. `Retry-After`을 검사하고 백오프합니다. |
| 기타 | `1` | 구조화되지 않은 오류입니다. 응답 본문(있는 경우)이 포함됩니다. |

## 청구 {#billing}

`glab orbit remote query`은 MCP 쿼리와 동일한 방식으로 GitLab Credits를 소비합니다. `status`, `schema`, `tools` 및 `graph-status` 호출은 무료입니다.

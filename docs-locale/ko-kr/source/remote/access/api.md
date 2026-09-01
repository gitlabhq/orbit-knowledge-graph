---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: REST API를 사용하여 GitLab Orbit 그래프를 직접 쿼리합니다. 인증 요구 사항 및 예제 요청이 포함된 4개 엔드포인트에 대한 참조입니다.
title: REST API
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

GitLab Orbit REST API를 사용하면 스크립트, CI 파이프라인, 또는 사용자 지정 도구에서 직접 그래프를 쿼리할 수 있습니다.

## 인증 {#authentication}

모든 엔드포인트에는 `read_api` 범위를 가진 GitLab 개인 액세스 토큰이 필요하며, Bearer 토큰으로 전달됩니다:

```shell
--header "Authorization: Bearer <your_token>"
```

결과는 토큰 소유자가 GitLab에서 액세스할 수 있는 엔티티로 범위가 지정됩니다.

## 청구 {#billing}

API 호출은 구독에서 GitLab Credits를 소비합니다. `POST /api/v4/orbit/query`에 대한 각 호출은 Credits를 사용합니다. 다른 엔드포인트는 무료입니다.

## 엔드포인트 {#endpoints}

| 방법 | 엔드포인트 | 설명 |
|--------|----------|-------------|
| `POST` | `/api/v4/orbit/query` | 그래프 쿼리 실행 |
| `GET` | `/api/v4/orbit/schema` | 현재 스키마 가져오기 |
| `GET` | `/api/v4/orbit/status` | 인덱싱 상태 확인 |
| `GET` | `/api/v4/orbit/tools` | 사용 가능한 MCP 도구 정의 나열 |

## 쿼리 엔드포인트 {#query-endpoint}

GitLab Orbit 쿼리 DSL을 사용하여 그래프 쿼리를 실행합니다.

요청 본문에 포함되는 내용:

- `query`: GitLab Orbit 쿼리 객체입니다.
- `format`: 선택적 응답 형식입니다. 구조화된 JSON의 경우 `raw`을 사용하고, AI 에이전트에 최적화된 컴팩트 텍스트의 경우 `llm`을 사용합니다. 기본값: `llm`.

예를 들어:

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{"query": <query_json>, "format": "raw"}' \
  "https://gitlab.com/api/v4/orbit/query"
```

전체 DSL은 [쿼리 언어 참조](../queries/query-language.md)를 참조합니다.

### 요청 예제 {#example-request}

예를 들어, 가장 많은 파이프라인 실패가 있는 프로젝트를 찾는 요청입니다:

요청 본문을 `request.json`에 넣습니다:

```json orbit-query
{
  "query": {
    "query_type": "aggregation",
    "nodes": [
      {"id": "pl", "entity": "Pipeline", "filters": {"status": "failed"}},
      {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
    ],
    "relationships": [
      {"type": "IN_PROJECT", "from": "pl", "to": "p"}
    ],
    "group_by": ["p"],
    "aggregations": [
      {
        "count": "pl",
        "as": "failed_pipelines"
      }
    ],
    "aggregation_sort": "-failed_pipelines",
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

응답 예제:

```json
{
  "result": {
    "format_version": "2.0.0",
    "query_type": "aggregation",
    "nodes": [],
    "edges": [],
    "group_columns": [
      {
        "name": "p",
        "kind": "node",
        "node": "p",
        "entity": "Project"
      }
    ],
    "columns": [
      {
        "name": "failed_pipelines",
        "function": "count",
        "target": "pl"
      }
    ],
    "rows": [
      {
        "p": {
          "type": "Project",
          "id": "1",
          "properties": {
            "name": "payments-api",
            "full_path": "my-org/payments-api"
          }
        },
        "failed_pipelines": 47
      }
    ]
  },
  "query_type": "aggregation",
  "raw_query_strings": null,
  "row_count": 1
}
```

## 스키마 엔드포인트 {#schema-endpoint}

현재 온톨로지를 반환합니다. 모든 노드 유형, 해당 속성 및 유형, 그리고 모든 관계 유형입니다.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/schema"
```

쿼리를 작성하기 전에 사용 가능한 엔티티 유형 및 속성을 검색할 때 이를 사용합니다.

## 상태 엔드포인트 {#status-endpoint}

GitLab Orbit이 활성화된 그룹의 인덱싱 상태를 반환합니다.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/status"
```

응답 예제:

```json
{
  "status": "indexed",
  "domains": {
    "sdlc": {"indexed": true, "last_updated": "2026-05-05T14:22:00Z"},
    "code": {"indexed": true, "last_updated": "2026-05-05T14:18:00Z"}
  },
  "projects": {
    "total": 847,
    "indexed": 847
  }
}
```

## 도구 엔드포인트 {#tools-endpoint}

`list_commands` 및 `invoke_command`에 대한 MCP 도구 정의를 MCP 클라이언트와 호환되는 형식으로 반환합니다.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/tools"
```

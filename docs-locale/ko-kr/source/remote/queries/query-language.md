---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit 쿼리 언어를 사용하여 그래프를 검색하고 탐색합니다.
title: GitLab Orbit 쿼리 언어
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

GitLab Orbit 쿼리 언어는 평면 API 응답 대신 그래프 형식으로 GitLab 데이터가 필요할 때 사용합니다. 쿼리는 JSON 객체입니다. 일치할 엔터티, 따를 관계 및 반환할 속성을 나타냅니다.

## 요청 봉투 {#request-envelope}

REST API 또는 `glab orbit remote query`을 통해 쿼리를 제출할 때 쿼리 객체를 최상위 `query` 필드에 래핑합니다:

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [{
      "id": "mr",
      "entity": "MergeRequest",
      "node_ids": [12345],
      "columns": ["iid", "title", "state"]
    }],
    "limit": 1
  },
  "response_format": "raw"
}
```

| 필드 | 필수 | 설명 |
|-------|----------|-------------|
| `query` | 예 | 아래에 문서화된 쿼리 객체입니다. |
| `response_format` | 아니요 | `"llm"` (생략할 때 기본값; LLM 소비에 최적화된 압축 [GOON](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/docs/design-documents/querying/graph_engine.md) 텍스트) 또는 `"raw"` (구조화된 JSON)입니다. `"raw"`를 `jq`로 출력을 파이프할 때 사용합니다. |

`orbit query` CLI(로컬 그래프의 경우)는 봉투 **without** 원본 쿼리 본문을 가져옵니다.

## 쿼리 형태 {#query-shape}

모든 쿼리는 `query_type`과 노드 선택자의 `nodes` 배열을 가집니다.

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345],
    "columns": ["iid", "title", "state"]
  }],
  "limit": 1
}
```

## 쿼리 유형 {#query-types}

| 쿼리 유형 | 사용 목적 |
|------------|-----------|
| `traversal` | 일치하는 노드를 가져오거나 노드 간의 관계를 따릅니다. |
| `aggregation` | 일치하는 그래프 결과를 계산, 합산, 평균, 그룹화 또는 정렬합니다. |
| `path_finding` | 두 노드 선택자 간의 경계된 경로를 찾습니다. |
| `neighbors` | 경계된 하나의 노드에 연결된 노드를 반환합니다. |

단일 노드 `traversal`는 검색 형태입니다. 별도의 `search` 쿼리 유형은 없습니다.

## 최상위 필드 {#top-level-fields}

| 필드 | 형식 | 설명 |
|-------|------|-------------|
| `query_type` | `string` | `traversal`, `aggregation`, `path_finding` 또는 `neighbors` 중 하나입니다. |
| `nodes` | `array` | 노드 선택자입니다. 항상 필수입니다. 단일 노드 쿼리(`neighbors`, 검색 형태 `traversal`)는 1개 요소 배열을 사용합니다. 최대 5개입니다. |
| `relationships` | `array` | 순회 또는 집계를 위한 관계 선택자입니다. 최대 5개입니다. |
| `aggregations` | `array` | 집계 정의입니다. `aggregation`에 필요합니다. 최대 10개입니다. |
| `group_by` | `array` | 집계 행의 그룹 키입니다. 최대 4개입니다. |
| `path` | `object` | 경로 찾기 구성입니다. `path_finding`에 필요합니다. |
| `neighbors` | `object` | 인접 노드 조회 구성입니다. `neighbors`에 필요합니다. |
| `limit` | `integer` | `cursor`이 설정되지 않았을 때 반환할 최대 행입니다. 기본값 30입니다. 최대 1000입니다. 응답에서 `pagination.truncated`를 확인합니다. true일 때 더 많은 일치하는 행이 있습니다. |
| `cursor` | `object` | 키 집합 페이지 매김: 첫 페이지는 `{"page_size": N}`, 그 다음 `next_cursor`이 없을 때까지 `{"page_size": N, "after": "<pagination.next_cursor>"}`입니다. 데이터 크기에 관계없이 모든 행에 도달합니다. 토큰은 이를 발행한 정확한 쿼리에 바인딩됩니다. |
| `order_by` | `string` | 노드 속성으로 행을 정렬합니다: `"node.property"` (오름차순) 또는 `"-node.property"` (내림차순). |
| `aggregation_sort` | `string` | 출력 열(집계 또는 그룹 키 별칭)로 집계 행을 정렬합니다: `"column"` (오름차순) 또는 `"-column"` (내림차순). |
| `options` | `object` | 표현 및 디버그 옵션입니다. |

페이지 매김은 요청 시간에 라이브 데이터를 읽습니다. 스냅샷이 없습니다. 각 페이지는 독립적으로 모든 행의 최신 버전을 확인하고 소프트 삭제된 행을 필터링하므로 페이지 간 버전 변동 및 묘지 정리는 결과를 건너뛰거나 중복하지 않습니다. 정렬 순서에서 커서 위치 이후에 삽입된 행은 이후 페이지에 나타나고, 뒤에 삽입되거나 재정렬된 행은 다시 방문하지 않습니다. 정렬 키가 NULL인 행은 마지막으로 정렬되고 다른 행처럼 페이지 매김합니다. 정렬 키가 페이지 간에 변경되는 행은 두 번 나타나거나 전혀 나타나지 않을 수 있습니다. 이는 스냅샷이 없는 다른 키 집합 페이지 매김과 동일합니다.

## 노드 선택자 {#node-selectors}

노드 선택자는 온톨로지에서 하나의 엔터티 유형을 지정합니다.

| 필드 | 형식 | 설명 |
|-------|------|-------------|
| `id` | `string` | 노드의 로컬 별칭입니다. 관계, 집계, 경로 및 인접 노드는 이 별칭을 참조합니다. |
| `entity` | `string` | `Project`, `User`, `MergeRequest`, `File` 또는 `Definition`과 같은 온톨로지 노드 유형입니다. |
| `columns` | `string` 또는 `array` | 반환할 속성입니다. `"*"`을 모든 제한되지 않은 속성에 사용하거나 이름 배열을 사용합니다. 생략하면 GitLab Orbit은 엔터티의 기본 열을 반환합니다. |
| `filters` | `object` | 속성 필터입니다. |
| `node_ids` | `array` | 일치할 정확한 ID입니다. 정수 또는 숫자 문자열을 허용합니다. 최대 500입니다. |
| `id_range` | `object` | `start`과 `end`를 포함하는 포함적 ID 범위입니다. |
| `id_property` | `string` | `node_ids`과 `id_range`에서 사용하는 속성입니다. 기본값 `id`. |

그래프 ID를 이미 알고 있을 때 `node_ids`을 사용합니다. `filters`를 사용하면 `username`, `full_path`, `state`, `path` 같은 자연스러운 속성을 알 수 있습니다.

## 관계 {#relationships}

관계는 별칭으로 노드 선택자를 연결합니다.

```json
{
  "type": "AUTHORED",
  "from": "user",
  "to": "mr",
  "direction": "outgoing"
}
```

| 필드 | 형식 | 설명 |
|-------|------|-------------|
| `type` | `string` 또는 `array` | 관계 유형 또는 유형입니다. 경계된 쿼리가 있고 모든 관계가 필요할 때만 `"*"`을 사용합니다. |
| `from` | `string` | 시작 노드 선택자의 별칭입니다. |
| `to` | `string` | 끝 노드 선택자의 별칭입니다. |
| `direction` | `string` | `outgoing`, `incoming` 또는 `both`입니다. 기본값 `outgoing`. |
| `hops` | `array` | 포함적 `[min, max]` 홉 범위(`[1, 3]`, 정확히 2는 `[2, 2]`)입니다. 기본값 `[1, 1]`. 최대 3입니다. |
| `filters` | `object` | 관계 속성 필터입니다. 최대 5개 필터입니다. |

예를 들어, 머지 리퀘스트는 `IN_PROJECT`로 프로젝트를 가리키고, 사용자는 `AUTHORED`로 머지 리퀘스트를 가리킵니다.

## 필터 {#filters}

필터는 단순 같음을 사용할 수 있습니다:

```json
{
  "filters": {
    "state": "merged"
  }
}
```

또는 연산자 객체를 사용할 수 있습니다. 같은 속성의 여러 연산자 키는 AND로 결합되며, 이는 범위를 표현하는 방식입니다:

```json
{
  "filters": {
    "created_at": {"gte": "2026-01-01", "lt": "2026-02-01"},
    "state": {"in": ["opened", "merged"]}
  }
}
```

같은 속성의 연산자를 반복하려면 연산자 객체 배열을 사용합니다: `{"title": [{"contains": "foo"}, {"contains": "bar"}]}`.

| 연산자 | 사용 |
|----------|-----|
| `eq` | 스칼라 값과 같습니다. |
| `gt`, `gte`, `lt`, `lte` | 숫자, 날짜 또는 타임스탬프 비교입니다. |
| `in` | 값이 배열에 있습니다. 최대 100개 값입니다. |
| `contains` | 문자열이 부분 문자열을 포함합니다. |
| `starts_with` | 문자열이 접두사로 시작합니다. |
| `ends_with` | 문자열이 접미사로 끝납니다. |
| `is_null` | Null 확인입니다. 부울을 사용합니다: `false`은 null이 아닌 것과 일치합니다. |
| `is_not_null` | Not-null 확인입니다. 부울을 사용합니다: `false`은 null과 일치합니다. |
| `token_match` | 텍스트 인덱스가 하나의 토큰을 포함합니다. |
| `all_tokens` | 텍스트 인덱스가 모든 토큰을 포함합니다. |
| `any_tokens` | 텍스트 인덱스가 모든 토큰을 포함합니다. |

토큰 연산자는 텍스트 인덱스가 있는 속성에서만 작동합니다.

### 텍스트 인덱싱된 속성 {#text-indexed-properties}

다음 속성은 `token_match`, `all_tokens` 및 `any_tokens`을 지원합니다. 다른 속성에서 이러한 연산자를 사용하면 전체 문자열 스캔으로 폴백되므로 속도가 느립니다.

<!-- The table below is generated from the ontology's `text(...)` storage indexes. -->
<!-- Do not edit it by hand: run `mise run docs:query-language` and commit. CI fails on drift. -->
<!-- BEGIN GENERATED: text-indexed-properties -->

| 엔터티 | 텍스트 인덱싱된 속성 |
|--------|------------------------|
| `Branch` | `name` |
| `Definition` | `file_path`, `fqn`, `name` |
| `Deployment` | `ref` |
| `Directory` | `name`, `path` |
| `Environment` | `environment_type`, `name` |
| `File` | `name`, `path` |
| `Finding` | `description`, `name` |
| `Group` | `description`, `name` |
| `ImportedSymbol` | `file_path`, `import_path` |
| `Job` | `name`, `ref` |
| `Label` | `description`, `title` |
| `MergeRequest` | `description`, `source_branch`, `target_branch`, `title` |
| `MergeRequestDiffFile` | `new_path`, `old_path` |
| `Milestone` | `description`, `title` |
| `Note` | `note` |
| `Pipeline` | `ref` |
| `Project` | `description`, `name` |
| `Runner` | `name` |
| `Stage` | `name` |
| `User` | `name`, `username` |
| `Vulnerability` | `description`, `title` |
| `VulnerabilityIdentifier` | `external_id`, `external_type`, `name` |
| `VulnerabilityOccurrence` | `description`, `name` |
| `VulnerabilityScanner` | `external_id`, `name` |
| `WorkItem` | `description`, `title` |

<!-- END GENERATED: text-indexed-properties -->

## 열 및 가상 열 {#columns-and-virtual-columns}

대부분의 열은 ClickHouse의 인덱싱된 그래프 테이블에서 가져옵니다. 일부 열은 가상입니다: GitLab Orbit은 그래프 쿼리가 반환된 후 다른 서비스에서 이들을 가져옵니다.

가상 열을 `columns`에서 명시적으로 요청합니다. `dynamic_columns` 옵션(`path_finding` 및 `neighbors`에서 사용)은 가상 열을 제외합니다. 외부 서비스 호출이 필요할 수 있기 때문입니다.

| 엔터티 | 가상 열 | 반환 내용 |
|--------|----------------|-----------------|
| `MergeRequest` | `diff` | 머지 리퀘스트의 전체 통합 diff입니다. |
| `MergeRequestDiff` | `patch` | 하나의 머지 리퀨스트 diff 스냅샷의 전체 패치입니다. |
| `MergeRequestDiffFile` | `diff` | 파일별 통합 diff 텍스트입니다. `too_large`이 `true`일 때 `null`을 반환합니다. |
| `File` | `content` | 파일의 원본 소스 텍스트입니다. |
| `Definition` | `content` | 하나의 인덱싱된 정의의 소스 텍스트입니다. |

`content` 열은 소스 코드용입니다. 머지 리퀘스트 diff 텍스트의 경우 `MergeRequest.diff`, `MergeRequestDiff.patch` 또는 `MergeRequestDiffFile.diff`를 사용합니다.

### 가상 열 필터링 {#filtering-on-virtual-columns}

가상 열은 `eq`, `contains`, `starts_with`, `ends_with`, `is_null` 및 `is_not_null` 필터 연산자를 `traversal` 쿼리에서 지원하며, 명시적 `node_ids`을 고정하는 노드에서만 지원합니다. 필터는 해당 행의 열을 확인하고 메모리에서 비교합니다. 가상 열 필터를 사용하여 아직 식별하지 않은 행을 검색하는 것(콘텐츠 검색)은 지원되지 않으며 거부됩니다. 먼저 인덱싱된 필터로 후보 행을 찾은 다음 가상 열 콘텐츠로 고정된 행을 필터링합니다.

열을 필터링하기 위해 요청할 필요는 없습니다. 필터링되지만 요청되지 않은 가상 열은 비교를 위해 확인되고 응답에서 생략됩니다.

`aggregation`, `neighbors` 및 `path_finding` 쿼리는 가상 열에 필터를 거부합니다.

## 순회 예제 {#traversal-examples}

전체 diff가 있는 하나의 머지 리퀘스트를 가져옵니다:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345],
    "columns": ["iid", "title", "state", "diff"]
  }],
  "limit": 1
}
```

diff 스냅샷에서 파일별 diff 콘텐츠를 가져옵니다:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "mr",
      "entity": "MergeRequest",
      "node_ids": [12345],
      "columns": ["iid", "title", "state"]
    },
    {
      "id": "snapshot",
      "entity": "MergeRequestDiff",
      "columns": ["id", "state", "patch"]
    },
    {
      "id": "file",
      "entity": "MergeRequestDiffFile",
      "columns": ["new_path", "old_path", "too_large", "diff"]
    }
  ],
  "relationships": [
    {"type": "HAS_DIFF", "from": "mr", "to": "snapshot"},
    {"type": "HAS_FILE", "from": "snapshot", "to": "file"}
  ],
  "limit": 20
}
```

`HAS_DIFF`은 머지 리퀨스트가 가진 모든 diff 스냅샷을 반환합니다(`MergeRequestDiff.merge_request_id` FK). `HAS_LATEST_DIFF`은 가장 최근 스냅샷만 반환합니다(`MergeRequest.latest_merge_request_diff_id` FK) — "머지 리퀘스트가 지금 어떻게 보이는가"에는 유용하지만 과거 질문에는 유용하지 않습니다. "파일을 건드린 모든 머지 리퀨스트"의 경우 모든 스냅샷에 대해 `HAS_DIFF`를 순회합니다. "과거 적용 범위" 질문을 위해 `HAS_LATEST_DIFF`을 사용하면 오래 유지된 파일에서 상당히 부족할 수 있습니다. 이전 리비전에서 파일을 건드렸지만 최종 diff에서는 건드리지 않은 MR은 `HAS_LATEST_DIFF`를 통해 보이지 않습니다.

`MergeRequestDiffFile.old_path`은 파일 조회에 선호되는 열입니다. `new_path`은 이름 바꾸기에서만 `old_path`과 다릅니다. `old_path`로 필터링 및 그룹화는 MR의 과거에서 동일한 행 ID를 유지합니다. [`merge_request_diff_file.yaml`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/config/ontology/nodes/code_review/merge_request_diff_file.yaml)의 온톨로지 필드 설명을 참조하세요.

소스 파일 콘텐츠를 가져옵니다:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "file",
    "entity": "File",
    "filters": {
      "path": {"ends_with": "app/models/project.rb"}
    },
    "columns": ["path", "language", "content"]
  }],
  "limit": 5
}
```

특정 함수 또는 클래스 정의의 소스 텍스트를 가져옵니다. `content` 열은 전체 파일이 아닌 해당 정의의 원본 소스 텍스트만 반환합니다. `fqn`(정규화된 이름)을 정확히 일치시키거나 더 넓은 검색을 위해 `name`을 `contains`와 함께 사용합니다:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "d",
    "entity": "Definition",
    "filters": {
      "fqn": {"eq": "Gitlab::Auth::authenticate"}
    },
    "columns": ["name", "fqn", "file_path", "start_line", "end_line", "content"]
  }],
  "limit": 5
}
```

프로젝트에서 병합된 머지 리퀘스트를 찾습니다:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "project",
      "entity": "Project",
      "filters": {"full_path": "your-group/your-project"},
      "columns": ["name", "full_path"]
    },
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"state": "merged"},
      "columns": ["iid", "title", "state", "merged_at"]
    }
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "mr", "to": "project"}
  ],
  "limit": 25
}
```

하나의 머지 리퀘스트를 위해 실행된 모든 파이프라인을 찾습니다. 항상 `Pipeline.source = "merge_request_event"`을 필터링하여 머지 리퀘스트의 **파이프라인** 탭이 표시하는 것과 일치하도록 합니다:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "p",
    "entity": "Pipeline",
    "filters": {
      "merge_request_id": {"eq": 482908721},
      "source": {"eq": "merge_request_event"}
    },
    "columns": ["id", "status", "source", "sha", "ref", "created_at"]
  }],
  "order_by": "-p.created_at",
  "limit": 100
}
```

`merge_request_id`은 머지 리퀘스트의 내부 숫자 `id`이며, 프로젝트 범위 `iid`이 아닙니다. 먼저 `iid`과 `project_id`으로 필터링하는 `MergeRequest` 순회로 조회한 다음 `id`을 위의 쿼리에 연결합니다.

`Pipeline.merge_request_id`과 `MergeRequest --TRIGGERED-->
Pipeline` 엣지 모두 MR을 해당 컨텍스트에서 생성된 모든 CI 파이프라인에 연결하며, 상위 MR 파이프라인이 트리거하는 다운스트림 자식 파이프라인(`source = "parent_pipeline"`)을 포함합니다. `source = "merge_request_event"` 필터가 없으면 상위-하위 파이프라인 팬 아웃을 사용하는 모든 MR에서 결과가 많이 초과되며, MR **파이프라인** 탭이 표시하는 것과 일치하지 않습니다. 다중 노드 쿼리에서 `MergeRequest --TRIGGERED--> Pipeline`을 순회할 때 동일한 필터를 적용합니다.

`MergeRequest --HAS_HEAD_PIPELINE--> Pipeline`은 다른 엣지입니다. 머지 리퀘스트의 소스 브랜치 끝에 대해 실행 중인 가장 최근의 단일 파이프라인을 가리킵니다. "현재 실행 중인 것"에는 사용하지만 파이프라인 기록에는 사용하지 않습니다.

## 집계 {#aggregation}

집계 쿼리는 `aggregations`을 사용합니다. 각 집계는 집계할 값이 있는 단일 함수 키가 있는 객체이며, 선택적 `as` 출력 열 이름이 있습니다: `{"avg": "mr.merge_duration", "as": "avg_dur"}`.

| 함수 키 | 값 | 지원되는 속성 유형 |
|--------------|-------|--------------------------|
| `count` | `"node"` (일치하는 행 계산) 또는 `"node.property"` (null이 아닌 값 계산) | 모두 |
| `sum` | `"node.property"` | 숫자만 |
| `avg` | `"node.property"` | 숫자만 |
| `min` | `"node.property"` | 숫자, 문자열, 부울, `Date` 또는 `DateTime` |
| `max` | `"node.property"` | 숫자, 문자열, 부울, `Date` 또는 `DateTime` |

`as` 없으면 출력 열 이름은 `<function>_<node>` (`count_mr`) 또는 `<function>_<node>_<property>` (`avg_mr_merge_duration`)로 파생됩니다. `aggregation_sort`에서 이 이름들을 참조합니다.

`sum`과 `avg`은 `DateTime` 속성을 검증 오류로 거부합니다. 날짜에 대해 집계하려면 `min` 또는 `max`를 사용합니다.

최상위 `group_by`을 사용하여 집계 행을 그룹화합니다. 쿼리의 모든 집계에 적용됩니다. 개별 집계 내에 그룹화를 넣지 마십시오.

그룹 키는 이러한 형태를 지원합니다:

| 그룹 키 | 형태 | 결과 값 |
|-----------|-------|--------------|
| 노드 | `"<node-id>"` (예: `"p"`) | 각 행의 중첩 엔터티 객체입니다. |
| 속성 | `"<node-id>.<property>"` (예: `"mr.state"`) | 각 행의 스칼라 버킷 값입니다. |
| 잘린 날짜 | `{"key": "<node-id>.<property>", "truncate": "<unit>"}` | 단위의 시작으로 잘린 속성입니다. |

출력 열 이름은 파생됩니다: 노드 키는 노드 ID(`p`)를 사용하고, 속성 키는 `<node>_<property>` (`mr_state`)를 사용하며, 잘린 키는 단위(`mr_created_at_month`)를 추가합니다. `aggregation_sort`에서 이 이름들을 참조합니다. 중복된 그룹 또는 집계 출력 이름은 거부됩니다.

파생된 이름을 사용합니다. 소비자가 특정 열 이름을 요구할 때만 객체 형식의 선택적 `as`로 이름을 바꿉니다: `{"key": "mr.state", "as":
"state"}`, 또는 잘림 `{"key": "mr.created_at", "truncate": "month",
"as": "month"}`.

잘림 단위는 `minute`, `hour`, `day`, `week`, `month`, `quarter` 및 `year`이며, `Date`/`DateTime` 속성에만 적용됩니다. `minute`과 `hour`은 `node_ids` 또는 잘린 속성의 필터를 요구하여 버킷 카디널리티를 경계합니다.

속성 그룹은 호출자가 사용할 수 있는 실제 ClickHouse 지원 필터링 가능한 속성을 참조해야 합니다. 가상 필드 및 필터링 불가능한 필드는 검증 중에 거부됩니다.

프로젝트당 병합된 머지 리퀘스트를 계산합니다:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "project",
      "entity": "Project",
      "filters": {"full_path": "your-group/your-project"}
    },
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"state": "merged"}
    }
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "mr", "to": "project"}
  ],
  "group_by": ["project"],
  "aggregations": [
    { "count": "mr", "as": "merged_mrs" }
  ],
  "aggregation_sort": "-merged_mrs",
  "limit": 10
}
```

심각도별로 감지된 취약점을 계산합니다:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "filters": {"state": "detected"}
    }
  ],
  "group_by": ["v.severity"],
  "aggregations": [
    { "count": "v", "as": "vulnerability_count" }
  ],
  "aggregation_sort": "-vulnerability_count",
  "limit": 10
}
```

집계 응답은 표 형태입니다. `columns`은 계산된 집계 값을 설명하고, `group_columns`은 그룹화 키를 설명하며, `rows`은 그룹 값 및 메트릭 값을 전달합니다. 노드 그룹화된 행은 그룹 키 아래에 그룹화된 엔터티를 저장합니다. 속성 그룹화된 행은 그룹 키 아래에 스칼라 버킷을 저장합니다.

`collect`은 입력 유형에 나열되지만 현재 검증에 의해 거부됩니다.

## 경로 찾기 {#path-finding}

경로 찾기 쿼리는 `path`을 사용합니다.

| 필드 | 형식 | 설명 |
|-------|------|-------------|
| `type` | `string` | `shortest`. |
| `from` | `string` | 시작 노드 선택자의 별칭입니다. |
| `to` | `string` | 끝 노드 선택자의 별칭입니다. |
| `max_depth` | `integer` | 최대 경로 길이입니다. 최대 3입니다. |
| `rel_types` | `array` | 순회할 관계 유형입니다. 두 끝점이 `node_ids`을 모두 사용하지 않는 한 필수입니다. |

두 끝점은 `node_ids`, 필터 또는 500 이하의 범위를 가진 `id_range`로 경계되어야 합니다. 끝점이 필터 또는 `id_range`을 사용하면 `rel_types`을 제공합니다.

```json orbit-query
{
  "query_type": "path_finding",
  "nodes": [
    {"id": "start", "entity": "Project", "node_ids": [278964]},
    {"id": "end", "entity": "User", "node_ids": [1]}
  ],
  "path": {
    "type": "shortest",
    "from": "start",
    "to": "end",
    "max_depth": 3,
    "rel_types": ["CREATOR", "AUTHORED", "IN_PROJECT"]
  },
  "limit": 5
}
```

## 인접 노드 {#neighbors}

인접 노드 쿼리는 1개 요소 `nodes` 배열과 `neighbors` 객체를 사용합니다. 중심 노드는 `node_ids`, 필터 또는 좁은 `id_range`로 경계되어야 합니다.

```json orbit-query
{
  "query_type": "neighbors",
  "nodes": [{
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345]
  }],
  "neighbors": {
    "direction": "both",
    "rel_types": ["AUTHORED", "IN_PROJECT", "HAS_DIFF"]
  },
  "options": {
    "dynamic_columns": "default"
  },
  "limit": 25
}
```

`options.dynamic_columns`을 동적으로 발견된 인접 노드 또는 경로 노드에 대해 제한되지 않은 모든 ClickHouse 지원 열이 필요한 경우 `"*"`로 설정합니다. 가상 열은 순회 쿼리에서 명시적 요청이 필요합니다.

## 검증 한계 {#validation-limits}

GitLab Orbit은 SQL을 컴파일하기 전에 광범위하거나 모호한 쿼리를 거부합니다.

| 한도 | 값 |
|-------|-------|
| 쿼리당 노드 | 5 |
| 쿼리당 관계 | 5 |
| 쿼리당 집계 | 10 |
| 선택자당 `node_ids` | 500 |
| `in` 필터의 값 | 100 |
| 노드 선택자당 열 | 50 |
| 선택자당 관계 유형 | 10 |
| 관계 홉 | 3 |
| 경로 깊이 | 3 |
| 노드당 필터 | 10 |
| 관계당 필터 | 5 |

순회 및 집계 쿼리는 최소 하나의 선택적 노드를 포함해야 합니다: `node_ids`, 필터 또는 100,000 이하의 범위를 가진 `id_range`.

단일 노드 순회는 또한 선택성이 필요합니다. 광범위한 엔터티를 검사하려면 필터를 추가하거나 ID를 제공하거나 좁은 `id_range`을 사용합니다.

## 옵션 {#options}

| 옵션 | 설명 |
|--------|-------------|
| `dynamic_columns` | `path_finding`과 `neighbors` 이월입니다. 각 엔터티의 기본 열에 `default`, 또는 제한되지 않은 모든 ClickHouse 지원 열에 `"*"`을 사용합니다. 기본값 `default`. |
| `include_debug_sql` | 호출자가 볼 수 있을 때 응답 메타데이터에 컴파일된 ClickHouse SQL을 포함합니다. |

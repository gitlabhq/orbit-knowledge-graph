---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "GitLab Orbit 그래프를 쿼리하여 GitLab 데이터, 코드 및 관계를 찾습니다."
title: 쿼리
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

GitLab Orbit 쿼리는 그래프 작업을 설명하는 JSON 객체입니다. 쿼리는 한 종류의 객체를 가져오거나, 객체 간 관계를 순회하거나, 일치하는 객체를 계산하거나, 경로를 찾거나, 노드의 이웃을 요청할 수 있습니다.

쿼리는 GitLab 권한 부여를 통해 실행됩니다. 응답에는 현재 사용자가 GitLab에서 읽을 수 있는 데이터만 포함됩니다.

## 쿼리 형태 선택 {#choose-a-query-shape}

| 사용 사례 | 쿼리 형태 |
|----------|-------------|
| 한 가지 엔티티 유형의 일치하는 노드 가져오기 | 단일 노드 `traversal` |
| 알려진 엔티티 유형 간의 관계 따르기 | 다중 노드 `traversal` |
| 그래프 결과 계산, 합계, 평균 또는 그룹화 | `aggregation` |
| 두 개의 제한된 끝점 간의 경로 찾기 | `path_finding` |
| 하나의 제한된 노드에 연결된 항목 확인 | `neighbors` |

단일 노드 `traversal`은 검색 형태입니다. GitLab Orbit에는 별도의 `search` 쿼리 유형이 없습니다.

## 예제: 머지 리퀘스트 diff 가져오기 {#example-fetch-a-merge-request-diff}

`diff` 열을 `MergeRequest`에서 사용하여 머지 리퀘스트의 전체 통합 diff를 가져옵니다. 가상 열을 이름별로 명시적으로 요청합니다.

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

머지 리퀘스트 diff 콘텐츠에는 몇 가지 다른 형태가 있습니다:

| 엔티티 | 열 | 반환되는 항목 |
|--------|--------|-----------------|
| `MergeRequest` | `diff` | 머지 리퀘스트의 전체 통합 diff |
| `MergeRequestDiff` | `patch` | 한 개의 diff 스냅샷에 대한 전체 패치 |
| `MergeRequestDiffFile` | `diff` | 파일별 통합 diff 텍스트 |
| `File` | `content` | 원본 소스 파일 텍스트 |
| `Definition` | `content` | 인덱싱된 정의에 대한 소스 텍스트 |

`content` 열은 소스 코드 노드용입니다. 머지 리퀘스트 diff 텍스트의 경우, 엔티티에 따라 `diff` 또는 `patch`를 사용합니다.

## 예제: diff 스냅샷 및 변경된 파일 가져오기 {#example-fetch-diff-snapshots-and-changed-files}

`HAS_DIFF`를 사용하여 머지 리퀘스트에서 diff 스냅샷으로 이동한 후, `HAS_FILE`를 사용하여 해당 스냅샷의 파일을 가져옵니다.

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

`MergeRequestDiffFile.diff`은 `too_large`이 `true`일 때 `null`입니다.

## 예제: 소스 파일 콘텐츠 가져오기 {#example-fetch-source-file-content}

소스 코드 엔티티에 `content`를 사용합니다. 이 예제는 경로별로 인덱싱된 파일을 검색하고 원본 파일 텍스트를 반환합니다.

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

전체 구문, 사용 가능한 필드 및 검증 규칙에 대해서는 [GitLab Orbit 쿼리 언어](query-language.md)를 참조하세요.

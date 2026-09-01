---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "GitLab Orbit를 사용하여 코드베이스, 파이프라인, 종속성 및 보안에 대한 전문가로 AI 에이전트를 전환하는 즉시 사용 가능한 프롬프트 라이브러리입니다."
title: 요리책
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

GitLab Orbit은 전체 소프트웨어 개발 수명 주기(코드, 머지 리퀘스트, 파이프라인, 종속성 및 보안)에 대한 질문에 답합니다. 그래프 쿼리를 직접 작성할 필요는 없습니다. AI 에이전트에 일반 언어로 질문하면 에이전트는 GitLab Orbit을 사용하여 그래프를 통과하고 답변합니다.

이 페이지는 작동하는 프롬프트 라이브러리입니다. 각각은 에이전트를 자신의 프로젝트에 대한 전문가로 전환합니다.

## 이 페이지를 사용하는 방법 {#how-to-use-this-page}

1. 에이전트를 GitLab Orbit에 연결하세요. GitLab Duo Agent Platform에는 GitLab Orbit이 기본 제공됩니다. Claude Code 또는 Codex와 같은 외부 에이전트는 [MCP 또는 `glab` CLI](access/mcp.md)를 통해 연결됩니다.
1. 원하는 결과를 선택하고 프롬프트를 복사하세요.
1. `<angle brackets>`의 값을 자신의 그룹, 프로젝트, 파일 또는 시간 범위로 바꾸세요.
1. 프롬프트를 에이전트에 붙여넣고 작동하게 하세요. 더 자세히 알아보려면 같은 대화에서 후속 질문을 하세요.

모든 프롬프트에는 **See the GitLab Orbit queries behind this** 섹션이 있습니다. 절대로 열 필요가 없지만, 에이전트가 실행하는 정확한 그래프 쿼리를 감사하거나 [REST API](access/api.md)를 직접 호출하려는 경우 표시됩니다.

## CI 지출을 원인이 되는 코드에 적용하세요 {#attribute-your-ci-spend-to-the-code-that-causes-it}

CI 컴퓨팅은 비싸며, 비용 대부분은 반복해서 재시도되는 실패에 숨어 있습니다. 이 프롬프트는 조직 전체의 실패를 순위 매기고, 공유 CI/CD 템플릿으로 인한 실패를 찾은 다음, 각각을 계속 끊어지는 정확한 파일과 코드 정의로 추적합니다. 마지막 단계는 비용 속성 체인입니다: "CI는 비쌉니다"를 "이 파일들이 이 작업들을 계속 깨뜨립니다"로 바꿉니다.

```plaintext
Using GitLab Orbit, help me understand what is driving our CI compute cost.

1. Find the job and pipeline failures across my organization over the last
   60 days, covering at least 20 projects. Rank the job names by how often
   they fail.
2. Flag any failing job name that recurs across three or more projects. Those
   usually point to a shared CI/CD template that is worth fixing once.
3. For the top recurring failures, find the merge requests that generate the
   most repeated failed pipelines.
4. Trace those failures back through the merge request diffs to the specific
   files, and the code definitions inside those files, that keep changing.
5. Show me the full chain from failing job to the exact code to review, and
   tell me where to focus a fix to cut the most CI spend.

Prioritize correctness and depth over speed.
```

돌아오는 것: 가장 비싼 반복되는 실패의 순위 목록, 교차 프로젝트 실패의 뒤에 있는 공유 템플릿, 그리고 수정할 파일 및 함수의 짧은 목록(각각 그것이 야기하는 실패에 연결됨)입니다.

조정하세요: 시간 범위를 변경하고, 한 그룹 또는 프로젝트로 범위를 지정하거나, 상위 3개를 수정했을 경우 절약되는 컴퓨팅을 추정하도록 에이전트에 요청하세요.

<details>
<summary>이 뒤의 GitLab Orbit 쿼리 보기</summary>

에이전트는 순서대로 이들을 실행합니다. 예제 타임스탬프를 시간 범위의 시작 날짜로 바꾸고, 머지 리퀘스트 ID와 파일 경로를 이전 단계에서 반환된 값으로 바꾸세요.

조직 전체에서 가장 자주 발생하는 작업 실패를 순위 매기세요:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "j",
      "entity": "Job",
      "filters": {
        "status": "failed",
        "created_at": {"gte": "2025-01-01T00:00:00Z"}
      }
    }
  ],
  "group_by": ["j.name"],
  "aggregations": [{ "count": "j", "as": "failures" }],
  "aggregation_sort": "-failures",
  "limit": 40
}
```

여러 프로젝트에서 반복되는 실패한 작업을 찾으세요. GitLab Orbit에는 고유 개수 함수가 없으므로 작업 이름과 프로젝트로 함께 그룹화하세요: 3개 이상의 프로젝트에서 나타나는 작업 이름은 공유 템플릿 핫스팟입니다.

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "j",
      "entity": "Job",
      "filters": {
        "status": "failed",
        "created_at": {"gte": "2025-01-01T00:00:00Z"}
      }
    },
    {"id": "p", "entity": "Project"}
  ],
  "relationships": [{"type": "IN_PROJECT", "from": "j", "to": "p"}],
  "group_by": [
    "j.name",
    "p.full_path"
  ],
  "aggregations": [{ "count": "j", "as": "failures" }],
  "aggregation_sort": "-failures",
  "limit": 200
}
```

가장 반복된 실패를 생성하는 머지 리퀘스트를 찾으세요. `source`을 `merge_request_event`로 필터링하세요. 그러면 이러한 파이프라인이 트리거한 다운스트림 자식 파이프라인도 계산되지 않습니다.

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "pl",
      "entity": "Pipeline",
      "filters": {
        "status": "failed",
        "source": "merge_request_event",
        "created_at": {"gte": "2025-01-01T00:00:00Z"}
      }
    }
  ],
  "group_by": ["pl.merge_request_id"],
  "aggregations": [{ "count": "pl", "as": "failed_pipelines" }],
  "aggregation_sort": "-failed_pipelines",
  "limit": 20
}
```

한 머지 리퀘스트를 계속 변경되는 파일로 추적하세요. 한 머지 리퀘스트로 한정하세요. 한 번에 모든 실패한 파이프라인에 대해 같은 통과를 수행하면 시간이 초과됩니다. `HAS_FILE` 엣지는 희박하게 채워져 있으므로, 짧은 결과를 권위적이지 않은 불완전한 범위로 취급하세요.

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "mr", "entity": "MergeRequest", "filters": {"id": {"eq": 123456789}}},
    {"id": "d", "entity": "MergeRequestDiff"},
    {"id": "f", "entity": "MergeRequestDiffFile"}
  ],
  "relationships": [
    {"type": "HAS_DIFF", "from": "mr", "to": "d"},
    {"type": "HAS_FILE", "from": "d", "to": "f"}
  ],
  "group_by": ["f.old_path"],
  "aggregations": [{ "count": "d", "as": "diff_snapshots" }],
  "aggregation_sort": "-diff_snapshots",
  "limit": 20
}
```

핫스팟 파일 내의 코드 정의로 드릴다운하세요. `File` 및 `Definition` 노드는 인덱싱된 소스 파일에만 존재하므로, 테스트 지원 헬퍼와 같은 일부 경로는 인덱싱되지 않을 수 있습니다.

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "f",
      "entity": "File",
      "filters": {"path": {"eq": "app/models/project.rb"}}
    },
    {
      "id": "def",
      "entity": "Definition",
      "columns": ["name", "fqn", "definition_type", "start_line"]
    }
  ],
  "relationships": [{"type": "DEFINES", "from": "f", "to": "def"}],
  "limit": 30
}
```

</details>

## 코드베이스를 빠르게 이해하세요 {#understand-a-codebase-fast}

낯선 프로젝트에 들어가서 며칠이 아닌 분 단위로 방향을 파악하세요.

```plaintext
I'm new to the <my-org/my-project> project. Using GitLab Orbit, give me a tour:
- The most active contributors over the last few months.
- The core classes, modules, and how they relate.
- The main entry points and the files I should read first.

Then summarize how this codebase is structured and suggest the three files
to read first to understand it.
```

<details>
<summary>이 뒤의 GitLab Orbit 쿼리 보기</summary>

프로젝트에 가장 활발한 기여자를 찾으세요:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "u", "entity": "User", "columns": ["username", "name"]},
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"state": "merged"}
    },
    {
      "id": "p",
      "entity": "Project",
      "filters": {"full_path": "my-org/my-project"}
    }
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"},
    {"type": "IN_PROJECT", "from": "mr", "to": "p"}
  ],
  "group_by": ["u"],
  "aggregations": [
    { "count": "mr", "as": "merged_mrs" }
  ],
  "aggregation_sort": "-merged_mrs",
  "limit": 10
}
```

</details>

## 종속성 및 폭발 범위 매핑 {#map-dependencies-and-blast-radius}

변경하기 전에 "내가 이것을 변경하면 무엇이 깨질까요?"에 답하세요.

```plaintext
Using GitLab Orbit, map the blast radius of <shared-auth-lib>.
- Which projects and files import it?
- Which code definitions depend on it?
- What would break if I changed its public interface?

Rank the affected areas by how many places depend on them, and tell me the
riskiest change I could make.
```

<details>
<summary>이 뒤의 GitLab Orbit 쿼리 보기</summary>

특정 모듈을 가져오는 모든 파일을 찾으세요. `payments-service`를 추적하려는 모듈 또는 라이브러리로 바꾸세요:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "sym",
    "entity": "ImportedSymbol",
    "columns": ["file_path", "import_path", "identifier_name"],
    "filters": {
      "import_path": {"contains": "payments-service"}
    }
  }],
  "limit": 100
}
```

공유 라이브러리에 종속된 프로젝트를 찾으세요:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "f",
      "entity": "File",
      "filters": {"path": {"contains": "shared-auth-lib"}}
    },
    {"id": "b", "entity": "Branch", "columns": ["name", "is_default"]},
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "ON_BRANCH", "from": "f", "to": "b"},
    {"type": "CONTAINS", "from": "p", "to": "b"}
  ],
  "limit": 100
}
```

가장 많은 코드가 가져오는 정의를 순위 매기세요:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "sym",
      "entity": "ImportedSymbol",
      "columns": ["import_path"],
      "filters": {
        "import_path": {"contains": "payments"}
      }
    },
    {"id": "def", "entity": "Definition", "columns": ["name", "fqn", "file_path"]}
  ],
  "relationships": [
    {"type": "IMPORTS", "from": "sym", "to": "def"}
  ],
  "group_by": ["def"],
  "aggregations": [
    { "count": "sym", "as": "import_count" }
  ],
  "aggregation_sort": "-import_count",
  "limit": 20
}
```

</details>

## 파이프라인을 건강하게 유지하세요 {#keep-your-pipelines-healthy}

가장 나쁜 CI/CD 위반자와 그들이 실패하는 이유를 찾으세요.

```plaintext
Using GitLab Orbit, show me where our CI/CD is unhealthy over the last 30 days:
- The projects with the most failed pipelines.
- The jobs that fail most often.
- The most common failure reasons.

Group the results so I can see which failures are worth fixing first.
```

<details>
<summary>이 뒤의 GitLab Orbit 쿼리 보기</summary>

가장 많은 실패한 파이프라인이 있는 프로젝트를 찾으세요:

```json orbit-query
{
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
    { "count": "pl", "as": "failed_count" }
  ],
  "aggregation_sort": "-failed_count",
  "limit": 10
}
```

실패한 작업 및 그 실패 이유를 찾으세요:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "j",
    "entity": "Job",
    "columns": ["name", "status", "failure_reason"],
    "filters": {"status": "failed"}
  }],
  "limit": 10
}
```

</details>

## 보안 위험을 원천으로 추적하세요 {#trace-security-risk-to-its-source}

위험이 있는 곳과 어떻게 발생했는지 확인하세요.

```plaintext
Using GitLab Orbit, find the critical and high severity vulnerabilities across
<my-org> that are still detected:
- Which projects are affected?
- How did each one get there? Trace it back to the scan and, where possible,
  the merge request that introduced the change.

Prioritize by severity and give me a short remediation shortlist.
```

<details>
<summary>이 뒤의 GitLab Orbit 쿼리 보기</summary>

모든 심각한 및 높은 취약점을 찾으세요:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "columns": ["title", "severity", "state", "report_type"],
      "filters": {
        "severity": {"in": ["critical", "high"]},
        "state": "detected"
      }
    },
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "v", "to": "p"}
  ],
  "order_by": "-v.severity",
  "limit": 50
}
```

프로젝트별로 취약점을 세세요:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "filters": {"state": "detected"}
    },
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "v", "to": "p"}
  ],
  "group_by": ["p"],
  "aggregations": [
    { "count": "v", "as": "vuln_count" }
  ],
  "aggregation_sort": "-vuln_count",
  "limit": 20
}
```

심각도별로 취약점을 세세요:

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
    { "count": "v", "as": "vuln_count" }
  ],
  "aggregation_sort": "-vuln_count",
  "limit": 10
}
```

</details>

## 실제 원본을 읽으세요 {#read-the-actual-source}

에이전트를 떠나지 않고 실제 코드를 대화에 당겨오세요.

```plaintext
Using GitLab Orbit, show me the source of <app/models/project.rb> and the definition
of <MyModule::my_function>, so I can review them here.
```

가상 열(`content` on `File` 및 `Definition`)은 그래프 쿼리 후 Gitaly fetch를 트리거하므로, 이 응답은 다른 쿼리보다 느립니다.

<details>
<summary>이 뒤의 GitLab Orbit 쿼리 보기</summary>

파일의 원본 텍스트를 가져오세요. `limit: 1`을 사용하여 큰 응답을 피하세요:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "f",
    "entity": "File",
    "columns": ["path", "language", "content"],
    "filters": {
      "path": {"ends_with": "app/models/project.rb"}
    }
  }],
  "limit": 1
}
```

특정 함수 또는 클래스 정의의 원본 텍스트를 가져오세요. `content` 필드는 전체 파일이 아닌 그 정의만의 원본 텍스트를 반환합니다:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "d",
    "entity": "Definition",
    "columns": ["name", "fqn", "file_path", "start_line", "end_line", "content"],
    "filters": {
      "fqn": {"eq": "Gitlab::Auth::authenticate"}
    }
  }],
  "limit": 5
}
```

</details>

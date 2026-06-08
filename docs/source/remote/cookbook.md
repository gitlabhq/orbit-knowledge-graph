---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Copy-paste Orbit queries for common use cases including organization mapping, onboarding, blast radius analysis, dependency mapping, code review, planning, pipeline health, and vulnerability tracing.
title: Cookbook
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

Ready-to-use queries for the most common Orbit use cases. All examples use the
REST API format. To run them via MCP, pass the JSON body to `query_graph`.

Each recipe answers a real question, then hides the query behind a **Show query**
toggle. Expand the ones you need, replace the example IDs and paths with your
own, and run them. For the full query grammar, see the
[Orbit query language](queries/query-language.md). For every entity and property
you can query, see the [schema reference](schema.md).

## Use cases

- [Explore your organization](#explore-your-organization) - groups and projects
- [Onboarding and codebase exploration](#onboarding-and-codebase-exploration) - contributors, directories
- [Blast radius analysis](#blast-radius-analysis) - what breaks if I change this
- [Dependency mapping](#dependency-mapping) - how services are connected
- [Merge requests and code review](#merge-requests-and-code-review) - diffs and review discussion
- [Planning and delivery](#planning-and-delivery) - issues, milestones, labels
- [Pipeline health](#pipeline-health) - CI/CD problems, stages, jobs
- [Vulnerability tracing](#vulnerability-tracing) - findings, scanners, CVE tracing

## Explore your organization

Answer: "What do we have, and where does it live?"

### List the projects in a group

Replace `my-org` with the full path of the group you want to map.

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "g",
      "entity": "Group",
      "filters": {"full_path": "my-org"},
      "columns": ["full_path", "name"]
    },
    {"id": "p", "entity": "Project", "columns": ["name", "full_path", "star_count"]}
  ],
  "relationships": [
    {"type": "CONTAINS", "from": "g", "to": "p"}
  ],
  "limit": 100
}
```

</details>

## Onboarding and codebase exploration

Answer: "Help me understand this codebase."

### Find the most active contributors to a project

<details><summary>Show query</summary>

```json
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
  "group_by": [{"kind": "node", "node": "u"}],
  "aggregations": [
    {"function": "count", "target": "mr", "alias": "merged_mrs"}
  ],
  "aggregation_sort": {"column": "merged_mrs", "direction": "DESC"},
  "limit": 10
}
```

</details>

### List the files in a directory

The `path` filter uses `starts_with`, so it also returns nested subdirectories.
Replace the `project_id` and `path` with your own.

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "d",
      "entity": "Directory",
      "filters": {
        "project_id": 278964,
        "path": {"op": "starts_with", "value": "app/models"}
      },
      "columns": ["path", "name"]
    },
    {"id": "f", "entity": "File", "columns": ["name", "language"]}
  ],
  "relationships": [
    {"type": "CONTAINS", "from": "d", "to": "f"}
  ],
  "limit": 50
}
```

</details>

## Blast radius analysis

Answer: "What breaks if I change this?"

### Find all files that import a specific module

Replace `payments-service` with the module or library you want to trace.

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "node": {
    "id": "sym",
    "entity": "ImportedSymbol",
    "columns": ["file_path", "import_path", "identifier_name"],
    "filters": {
      "import_path": {"op": "contains", "value": "payments-service"}
    }
  },
  "limit": 100
}
```

</details>

### Find projects that depend on a shared library

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "f",
      "entity": "File",
      "filters": {"path": {"op": "contains", "value": "shared-auth-lib"}}
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

</details>

## Dependency mapping

Answer: "How are our services connected?"

### Map imported definitions

<details><summary>Show query</summary>

```json
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "sym",
      "entity": "ImportedSymbol",
      "columns": ["import_path"],
      "filters": {
        "import_path": {"op": "contains", "value": "payments"}
      }
    },
    {"id": "def", "entity": "Definition", "columns": ["name", "fqn", "file_path"]}
  ],
  "relationships": [
    {"type": "IMPORTS", "from": "sym", "to": "def"}
  ],
  "group_by": [{"kind": "node", "node": "def"}],
  "aggregations": [
    {"function": "count", "target": "sym", "alias": "import_count"}
  ],
  "aggregation_sort": {"column": "import_count", "direction": "DESC"},
  "limit": 20
}
```

</details>

## Merge requests and code review

Answer: "What changed, and what did reviewers say?"

### Read the review discussion on a merge request

Returns every note on a merge request with its author. `internal` is `true` for
notes visible only to users with Reporter or higher access. Replace the
`node_ids` value with your own merge request ID.

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "mr", "entity": "MergeRequest", "node_ids": [12345], "columns": ["iid", "title"]},
    {"id": "n", "entity": "Note", "columns": ["note", "internal"]},
    {"id": "u", "entity": "User", "columns": ["username"]}
  ],
  "relationships": [
    {"type": "HAS_NOTE", "from": "mr", "to": "n"},
    {"type": "AUTHORED", "from": "u", "to": "n"}
  ],
  "limit": 50
}
```

</details>

### Find the largest merge requests in a project

Ranks merge request diffs by the number of files changed, to surface oversized
changes that are hard to review. Replace the `project_id` with your own.

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "node": {
    "id": "diff",
    "entity": "MergeRequestDiff",
    "filters": {
      "project_id": 278964,
      "files_count": {"op": "gte", "value": 1}
    },
    "columns": ["merge_request_id", "commits_count", "files_count"]
  },
  "order_by": {"node": "diff", "property": "files_count", "direction": "DESC"},
  "limit": 20
}
```

</details>

To pull the per-file diff text for a merge request, see
[virtual columns](queries/query-language.md#columns-and-virtual-columns) in the
query language reference.

## Planning and delivery

Answer: "What is the team working on?"

### List the open issues in a project

Replace `my-org/my-project` with your own project full path.

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "p",
      "entity": "Project",
      "filters": {"full_path": "my-org/my-project"},
      "columns": ["full_path"]
    },
    {
      "id": "wi",
      "entity": "WorkItem",
      "filters": {"state": "opened", "work_item_type": "issue"},
      "columns": ["iid", "title", "weight"]
    }
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "wi", "to": "p"}
  ],
  "limit": 50
}
```

</details>

### Count open issues by label

Groups open work items in a project by label to show where work is concentrated.

<details><summary>Show query</summary>

```json
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "p",
      "entity": "Project",
      "filters": {"full_path": "my-org/my-project"}
    },
    {
      "id": "wi",
      "entity": "WorkItem",
      "filters": {"state": "opened"}
    },
    {"id": "l", "entity": "Label", "columns": ["title"]}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "wi", "to": "p"},
    {"type": "HAS_LABEL", "from": "wi", "to": "l"}
  ],
  "group_by": [{"kind": "node", "node": "l"}],
  "aggregations": [
    {"function": "count", "target": "wi", "alias": "open_issues"}
  ],
  "aggregation_sort": {"column": "open_issues", "direction": "DESC"},
  "limit": 20
}
```

</details>

### List the milestones in a project

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "p",
      "entity": "Project",
      "filters": {"full_path": "my-org/my-project"}
    },
    {
      "id": "m",
      "entity": "Milestone",
      "columns": ["title", "state", "due_date", "start_date"]
    }
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "m", "to": "p"}
  ],
  "order_by": {"node": "m", "property": "due_date", "direction": "DESC"},
  "limit": 25
}
```

</details>

## Pipeline health

Answer: "Where are our CI/CD problems?"

### Find projects with the most failed pipelines

<details><summary>Show query</summary>

```json
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "pl", "entity": "Pipeline", "filters": {"status": "failed"}},
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "pl", "to": "p"}
  ],
  "group_by": [{"kind": "node", "node": "p"}],
  "aggregations": [
    {"function": "count", "target": "pl", "alias": "failed_count"}
  ],
  "aggregation_sort": {"column": "failed_count", "direction": "DESC"},
  "limit": 10
}
```

</details>

### Find failed jobs and their failure reasons

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "node": {
    "id": "j",
    "entity": "Job",
    "columns": ["name", "status", "failure_reason"],
    "filters": {"status": "failed"}
  },
  "limit": 10
}
```

</details>

### See the stage-by-stage status of a pipeline

Breaks a pipeline into its stages, in execution order. Replace the `project_id`
with your own.

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "pl", "entity": "Pipeline", "filters": {"project_id": 278964}},
    {"id": "s", "entity": "Stage", "columns": ["name", "status", "position"]}
  ],
  "relationships": [
    {"type": "HAS_STAGE", "from": "pl", "to": "s"}
  ],
  "order_by": {"node": "s", "property": "position", "direction": "ASC"},
  "limit": 50
}
```

</details>

## Vulnerability tracing

Answer: "Where are our security risks, and how did they get there?"

### Find all critical and high vulnerabilities in a group

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "columns": ["title", "severity", "state", "report_type"],
      "filters": {
        "severity": {"op": "in", "value": ["critical", "high"]},
        "state": "detected"
      }
    },
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "v", "to": "p"}
  ],
  "order_by": {"node": "v", "property": "severity", "direction": "DESC"},
  "limit": 50
}
```

</details>

### Count vulnerabilities by project

<details><summary>Show query</summary>

```json
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
  "group_by": [{"kind": "node", "node": "p"}],
  "aggregations": [
    {"function": "count", "target": "v", "alias": "vuln_count"}
  ],
  "aggregation_sort": {"column": "vuln_count", "direction": "DESC"},
  "limit": 20
}
```

</details>

### Count vulnerabilities by severity

<details><summary>Show query</summary>

```json
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "filters": {"state": "detected"}
    }
  ],
  "group_by": [
    {"kind": "property", "node": "v", "property": "severity", "alias": "severity"}
  ],
  "aggregations": [
    {"function": "count", "target": "v", "alias": "vuln_count"}
  ],
  "aggregation_sort": {"column": "vuln_count", "direction": "DESC"},
  "limit": 10
}
```

</details>

### Trace a specific CVE across your projects

Finds every vulnerability occurrence tied to an external identifier such as a CVE
or CWE. Replace `CVE-2021-44228` (Log4Shell) with the identifier you are hunting.

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "id",
      "entity": "VulnerabilityIdentifier",
      "filters": {"external_id": "CVE-2021-44228"},
      "columns": ["external_type", "external_id", "name", "url"]
    },
    {
      "id": "occ",
      "entity": "VulnerabilityOccurrence",
      "columns": ["name", "severity", "report_type", "location"]
    }
  ],
  "relationships": [
    {"type": "HAS_IDENTIFIER", "from": "occ", "to": "id"}
  ],
  "limit": 50
}
```

</details>

### List the findings from the latest security scan

Walks from a project's latest scans to the findings they produced. Replace the
`project_id` with your own.

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "scan",
      "entity": "SecurityScan",
      "filters": {"latest": true, "project_id": 278964},
      "columns": ["scan_type", "status"]
    },
    {
      "id": "f",
      "entity": "Finding",
      "columns": ["name", "severity", "description"]
    }
  ],
  "relationships": [
    {"type": "HAS_FINDING", "from": "scan", "to": "f"}
  ],
  "limit": 50
}
```

</details>

### See which scanners run on a project

Lists the security scanners that have produced results, with their vendor.

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "p",
      "entity": "Project",
      "filters": {"full_path": "my-org/my-project"}
    },
    {
      "id": "sc",
      "entity": "VulnerabilityScanner",
      "columns": ["name", "external_id", "vendor"]
    }
  ],
  "relationships": [
    {"type": "SCANS", "from": "sc", "to": "p"}
  ],
  "limit": 25
}
```

</details>

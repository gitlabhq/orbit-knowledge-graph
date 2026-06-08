---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Copy-paste Orbit prompts and queries for common use cases including organization mapping, onboarding, blast radius analysis, dependency mapping, code review, planning, pipeline health, and vulnerability tracing.
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

Ready-to-use recipes for the most common Orbit use cases.

Each recipe starts with a **natural-language prompt** you can copy and paste into
an agent connected to Orbit, such as Claude Code or GitLab Duo. The agent turns
the prompt into a graph query for you. If you want to call the API directly, the
exact query is behind the **Show query** toggle. All queries use the REST API
format. To run them via MCP, pass the JSON body to `query_graph`.

Replace the placeholders in each prompt, such as `<group name>` and
`<project name>`, and the example values in each query, with your own. For the
full query grammar, see the [Orbit query language](queries/query-language.md).
For every entity and property you can query, see the
[schema reference](schema.md).

## Use cases

- [Explore your organization](#explore-your-organization) - map projects to the people who own them
- [Analytics](#analytics) - org-wide reports across your whole estate
- [Onboarding and codebase exploration](#onboarding-and-codebase-exploration) - contributors and the cross-file call graph
- [Blast radius analysis](#blast-radius-analysis) - what breaks if I change this
- [Dependency mapping](#dependency-mapping) - how services are connected
- [Merge requests and code review](#merge-requests-and-code-review) - review discussion and pipeline status
- [Planning and delivery](#planning-and-delivery) - issues linked to merge requests, contributors, and related work
- [Pipeline health](#pipeline-health) - failed pipelines, who triggered them, stages
- [Vulnerability tracing](#vulnerability-tracing) - findings, scanners, CVE tracing

## Explore your organization

Answer: "What do we have, and where does it live?"

### Map a group's projects and their owners

_See every project in a group next to who created it and when it last saw activity._

```plaintext
Use Orbit to map the projects in <group name>, showing who created each one and when it was last active.
```

<details><summary>Show query</summary>

Joins every project in the group to the user who created it, in one query.

```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "g", "entity": "Group", "filters": {"full_path": "my-org"}, "columns": ["full_path"]},
    {"id": "p", "entity": "Project", "columns": ["full_path", "last_activity_at"]},
    {"id": "u", "entity": "User", "columns": ["username", "name"]}
  ],
  "relationships": [
    {"type": "CONTAINS", "from": "g", "to": "p"},
    {"type": "CREATOR", "from": "u", "to": "p"}
  ],
  "limit": 100
}
```

</details>

## Analytics

Org-wide reports that roll your whole estate up into a single answer. Each one
sweeps across every project you can access, so they work best with a group or
top-level namespace token.

### Find dead or dormant repositories

_Surface repositories that have gone quiet, so you can archive, consolidate, or reassign them._

```plaintext
Use Orbit to find all the dead repositories across our organization that have had no activity in the last two years.
```

<details><summary>Show query</summary>

Adjust the `last_activity_at` cutoff to match your definition of dormant.

```json
{
  "query_type": "traversal",
  "node": {
    "id": "p",
    "entity": "Project",
    "filters": {
      "archived": false,
      "last_activity_at": {"op": "lt", "value": "2024-01-01"}
    },
    "columns": ["full_path", "name", "last_activity_at", "star_count"]
  },
  "order_by": {"node": "p", "property": "last_activity_at", "direction": "ASC"},
  "limit": 100
}
```

</details>

### Measure merge request throughput

_Track delivery velocity by counting merged merge requests per month._

```plaintext
Use Orbit to show our merge request throughput: how many merge requests we merged each month over the past year.
```

<details><summary>Show query</summary>

Groups merged merge requests by the month they merged. Adjust the `merged_at`
window to change the reporting period.

```json
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {
        "state": "merged",
        "merged_at": {"op": "gte", "value": "2025-06-01"}
      }
    }
  ],
  "group_by": [
    {"kind": "property", "node": "mr", "property": "merged_at", "transform": {"kind": "truncate", "unit": "month"}, "alias": "month"}
  ],
  "aggregations": [
    {"function": "count", "target": "mr", "alias": "merged"}
  ],
  "aggregation_sort": {"column": "month", "direction": "ASC"},
  "limit": 18
}
```

</details>

### Generate a security posture report

_Show how your vulnerability load has shifted year over year._

```plaintext
Use Orbit to generate a security posture report showing how many vulnerabilities we detected each year over the last five years.
```

<details><summary>Show query</summary>

Groups vulnerability occurrences by the year they were detected. The agent can
run further cuts, such as by severity, scanner, or project, to fill out the report.

```json
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "occ",
      "entity": "VulnerabilityOccurrence",
      "filters": {"detected_at": {"op": "gte", "value": "2021-01-01"}}
    }
  ],
  "group_by": [
    {"kind": "property", "node": "occ", "property": "detected_at", "transform": {"kind": "truncate", "unit": "year"}, "alias": "year"}
  ],
  "aggregations": [
    {"function": "count", "target": "occ", "alias": "occurrences"}
  ],
  "aggregation_sort": {"column": "year", "direction": "ASC"},
  "limit": 10
}
```

</details>

### Map critical services and their owners

_Rank your most important services and pin down who owns each one._

```plaintext
Use Orbit to generate a report of our most critical services and who owns them.
```

<details><summary>Show query</summary>

Ranks services by star count as a popularity proxy and returns the user who
created each one. Order by `last_activity_at` instead to rank by recent activity.

```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "u", "entity": "User", "columns": ["username", "name"]},
    {
      "id": "p",
      "entity": "Project",
      "filters": {"star_count": {"op": "gte", "value": 100}},
      "columns": ["full_path", "star_count"]
    }
  ],
  "relationships": [
    {"type": "CREATOR", "from": "u", "to": "p"}
  ],
  "order_by": {"node": "p", "property": "star_count", "direction": "DESC"},
  "limit": 25
}
```

</details>

## Onboarding and codebase exploration

Answer: "Help me understand this codebase."

### Find the most active contributors to a project

_Learn who to ask about a codebase by ranking authors by merged work._

```plaintext
Use Orbit to find the top 10 contributors to <project name> by merged merge requests.
```

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

### Find everywhere a function is called

_Trace every caller of a function across files before you change it._

```plaintext
Use Orbit to find every place that calls the <function name> method, with the file each call lives in.
```

<details><summary>Show query</summary>

Follows the `CALLS` edge from each caller to the target definition. This is the
resolved cross-file call graph, not a text search, so it finds real references
even when the name is reused elsewhere.

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "callee",
      "entity": "Definition",
      "filters": {"project_id": 278964, "name": "execute", "definition_type": "Method"},
      "columns": ["fqn", "file_path"]
    },
    {"id": "caller", "entity": "Definition", "columns": ["fqn", "file_path"]}
  ],
  "relationships": [
    {"type": "CALLS", "from": "caller", "to": "callee"}
  ],
  "limit": 25
}
```

</details>

## Blast radius analysis

Answer: "What breaks if I change this?"

### Find all files that import a specific module

_See which files pull in a module, so you know what a change touches._

```plaintext
Use Orbit to find which files import the <module name> module.
```

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

_Find every project that leans on a shared library across your estate._

```plaintext
Use Orbit to find which projects depend on the <library name> library.
```

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

_Rank the most depended-on definitions, so you know what is risky to change._

```plaintext
Use Orbit to find which definitions in our <module name> code are imported the most.
```

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

_Pull every comment on a merge request, with its author, in one query._

```plaintext
Use Orbit to show the review discussion on merge request <merge request ID>, including who said what.
```

<details><summary>Show query</summary>

`internal` is `true` for notes visible only to users with Reporter or higher access.

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

### Find merge requests whose pipeline failed

_Catch merged merge requests that shipped on a red pipeline._

```plaintext
Use Orbit to find merged merge requests in <project name> whose head pipeline failed.
```

<details><summary>Show query</summary>

Joins each merge request to its head pipeline and keeps only the failed ones.

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"project_id": 278964, "state": "merged"},
      "columns": ["iid", "title"]
    },
    {
      "id": "pl",
      "entity": "Pipeline",
      "filters": {"status": "failed"},
      "columns": ["ref", "status", "failure_reason"]
    }
  ],
  "relationships": [
    {"type": "HAS_HEAD_PIPELINE", "from": "mr", "to": "pl"}
  ],
  "limit": 20
}
```

</details>

To pull the per-file diff text for a merge request, see
[virtual columns](queries/query-language.md#columns-and-virtual-columns) in the
query language reference.

## Planning and delivery

Answer: "How does our planning connect to the work that delivered it?"

These recipes traverse from issues to the merge requests, people, and related
work that surround them, in a single query. Joining planning data to code
review like this is what the graph gives you over the issues API.

### Find the merge request that resolved an issue

_Jump from an issue straight to the merge requests that closed it._

```plaintext
Use Orbit to find which merge requests closed issue <issue ID> in <project name>.
```

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "wi",
      "entity": "WorkItem",
      "filters": {"project_id": 278964, "iid": 21584},
      "columns": ["iid", "title", "state"]
    },
    {"id": "mr", "entity": "MergeRequest", "columns": ["iid", "title", "merged_at"]}
  ],
  "relationships": [
    {"type": "CLOSES", "from": "mr", "to": "wi"}
  ],
  "limit": 10
}
```

</details>

### See what a contributor shipped

_Summarize a person's impact by the issues their merged work resolved._

```plaintext
Use Orbit to show the issues that <username>'s merged merge requests closed in <project name>.
```

<details><summary>Show query</summary>

Walks from a user to their merged merge requests to the issues those merge
requests closed. There is no single API call for this.

```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "u", "entity": "User", "filters": {"username": "<username>"}, "columns": ["username", "name"]},
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"state": "merged", "project_id": 278964},
      "columns": ["iid", "title"]
    },
    {"id": "wi", "entity": "WorkItem", "columns": ["iid", "title", "state"]}
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"},
    {"type": "CLOSES", "from": "mr", "to": "wi"}
  ],
  "limit": 25
}
```

</details>

### See related and blocking work

_Map the issues linked to a given issue to understand its dependencies._

```plaintext
Use Orbit to show the work items related to issue <issue ID> in <project name>.
```

<details><summary>Show query</summary>

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "wi",
      "entity": "WorkItem",
      "filters": {"project_id": 278964, "iid": 476658},
      "columns": ["iid", "title"]
    },
    {"id": "rel", "entity": "WorkItem", "columns": ["iid", "title", "state"]}
  ],
  "relationships": [
    {"type": "RELATED_TO", "from": "wi", "to": "rel", "direction": "both"}
  ],
  "limit": 25
}
```

</details>

## Pipeline health

Answer: "Where are our CI/CD problems?"

### Find projects with the most failed pipelines

_Spot the projects where CI is breaking most often._

```plaintext
Use Orbit to find which projects have the most failed pipelines.
```

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

### Find failed pipelines and who triggered them

_Connect failed pipelines to the person who kicked each one off._

```plaintext
Use Orbit to show failed pipelines in <project name> and the person who triggered each one.
```

<details><summary>Show query</summary>

Links each failed pipeline back to the user who triggered it.

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "pl",
      "entity": "Pipeline",
      "filters": {"project_id": 278964, "status": "failed"},
      "columns": ["iid", "ref", "failure_reason"]
    },
    {"id": "u", "entity": "User", "columns": ["username"]}
  ],
  "relationships": [
    {"type": "TRIGGERED", "from": "u", "to": "pl"}
  ],
  "limit": 20
}
```

</details>

### See the stage-by-stage status of a pipeline

_Break a pipeline into its stages to see exactly where it stalls._

```plaintext
Use Orbit to show the stage-by-stage status of <project name>'s pipelines, in execution order.
```

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

_Get a prioritized list of the vulnerabilities that matter most across a group._

```plaintext
Use Orbit to list the critical and high severity vulnerabilities across <group name>.
```

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

_Rank projects by open vulnerability count to focus remediation._

```plaintext
Use Orbit to count our detected vulnerabilities by project.
```

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

_See the shape of your risk by breaking vulnerabilities down by severity._

```plaintext
Use Orbit to count our detected vulnerabilities by severity.
```

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

_Find every place a specific CVE appears, the way you would during an incident._

```plaintext
Use Orbit to find where <CVE ID> appears across our projects.
```

<details><summary>Show query</summary>

Replace `CVE-2021-44228` with the CVE or CWE identifier you are hunting.

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

_Review what your most recent scan surfaced for a project._

```plaintext
Use Orbit to show what the latest security scan found in <project name>.
```

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

_Audit which security scanners are actually producing results._

```plaintext
Use Orbit to show which security scanners run on <project name>.
```

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

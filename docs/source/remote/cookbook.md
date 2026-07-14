---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: A library of ready-to-use prompts that turn your AI agent into an expert on your codebase, pipelines, dependencies, and security using Orbit.
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

Orbit answers questions about your entire software development lifecycle: code,
merge requests, pipelines, dependencies, and security. You do not write graph
queries by hand. You ask an AI agent a question in plain language, and the agent
uses Orbit to traverse the graph and answer.

This page is a library of prompts that work. Each one turns your agent into an
expert on your own projects.

## How to use this page

1. Connect an agent to Orbit. GitLab Duo Agent Platform has Orbit built in.
   External agents such as Claude Code or Codex connect through
   [MCP or the `glab` CLI](access/mcp.md).
1. Pick the outcome you want and copy its prompt.
1. Replace the values in `<angle brackets>` with your own group, project, file,
   or time window.
1. Paste the prompt into your agent and let it work. Ask follow-up questions in
   the same conversation to go deeper.

Every prompt also has a **See the Orbit queries behind this** section. You never
need to open it, but it shows the exact graph queries the agent runs if you want
to audit them or call the [REST API](access/api.md) directly.

## Attribute your CI spend to the code that causes it

CI compute is expensive, and most of the cost hides in failures that get
retried over and over. This prompt ranks the failures across your whole
organization, finds the ones caused by a shared CI/CD template, then follows
each one back to the exact files and code definitions that keep breaking. That
last step is the cost-attribution chain: it turns "CI is expensive" into "these
files keep breaking these jobs."

```plaintext
Using Orbit, help me understand what is driving our CI compute cost.

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

What you get back: a ranked list of your most expensive recurring failures, the
shared templates behind the cross-project ones, and a short list of files and
functions to fix, each tied to the failures it causes.

Adapt it: change the time window, scope it to one group or project, or ask the
agent to estimate the compute saved if you fixed the top three.

<details>
<summary>See the Orbit queries behind this</summary>

The agent runs these in sequence. Replace the example timestamp with a date at
the start of your window, and replace the merge request ID and file path with
values returned by the earlier steps.

Rank the most frequent job failures across your organization:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "j",
      "entity": "Job",
      "filters": {
        "status": "failed",
        "created_at": {"op": "gte", "value": "2025-01-01T00:00:00Z"}
      }
    }
  ],
  "group_by": [{"kind": "property", "node": "j", "property": "name", "alias": "job_name"}],
  "aggregations": [{"function": "count", "target": "j", "alias": "failures"}],
  "aggregation_sort": {"column": "failures", "direction": "DESC"},
  "limit": 40
}
```

Find failing jobs that recur across multiple projects. Orbit has no
distinct-count function, so group by job name and project together: a job name
that appears under three or more projects is a shared-template hot spot.

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "j",
      "entity": "Job",
      "filters": {
        "status": "failed",
        "created_at": {"op": "gte", "value": "2025-01-01T00:00:00Z"}
      }
    },
    {"id": "p", "entity": "Project"}
  ],
  "relationships": [{"type": "IN_PROJECT", "from": "j", "to": "p"}],
  "group_by": [
    {"kind": "property", "node": "j", "property": "name", "alias": "job_name"},
    {"kind": "property", "node": "p", "property": "full_path", "alias": "project"}
  ],
  "aggregations": [{"function": "count", "target": "j", "alias": "failures"}],
  "aggregation_sort": {"column": "failures", "direction": "DESC"},
  "limit": 200
}
```

Find the merge requests generating the most repeated failures. Filter `source`
to `merge_request_event` so you do not also count the downstream child
pipelines those pipelines triggered.

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
        "created_at": {"op": "gte", "value": "2025-01-01T00:00:00Z"}
      }
    }
  ],
  "group_by": [{"kind": "property", "node": "pl", "property": "merge_request_id", "alias": "mr_id"}],
  "aggregations": [{"function": "count", "target": "pl", "alias": "failed_pipelines"}],
  "aggregation_sort": {"column": "failed_pipelines", "direction": "DESC"},
  "limit": 20
}
```

Trace one merge request to the files that keep changing. Bound this to a single
merge request; the same traversal across every failed pipeline at once times
out. `HAS_FILE` edges are sparsely populated, so treat a short result as
incomplete coverage rather than authoritative.

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "mr", "entity": "MergeRequest", "filters": {"id": {"op": "eq", "value": 123456789}}},
    {"id": "d", "entity": "MergeRequestDiff"},
    {"id": "f", "entity": "MergeRequestDiffFile"}
  ],
  "relationships": [
    {"type": "HAS_DIFF", "from": "mr", "to": "d"},
    {"type": "HAS_FILE", "from": "d", "to": "f"}
  ],
  "group_by": [{"kind": "property", "node": "f", "property": "old_path", "alias": "file"}],
  "aggregations": [{"function": "count", "target": "d", "alias": "diff_snapshots"}],
  "aggregation_sort": {"column": "diff_snapshots", "direction": "DESC"},
  "limit": 20
}
```

Drill into the code definitions inside a hot-spot file. `File` and `Definition`
nodes exist only for indexed source files, so some paths, such as test-support
helpers, might not be indexed.

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "f",
      "entity": "File",
      "filters": {"path": {"op": "eq", "value": "app/models/project.rb"}}
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

## Understand a codebase fast

Drop into an unfamiliar project and get oriented in minutes instead of days.

```plaintext
I'm new to the <my-org/my-project> project. Using Orbit, give me a tour:
- The most active contributors over the last few months.
- The core classes, modules, and how they relate.
- The main entry points and the files I should read first.

Then summarize how this codebase is structured and suggest the three files
to read first to understand it.
```

<details>
<summary>See the Orbit queries behind this</summary>

Find the most active contributors to a project:

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
  "group_by": [{"kind": "node", "node": "u"}],
  "aggregations": [
    {"function": "count", "target": "mr", "alias": "merged_mrs"}
  ],
  "aggregation_sort": {"column": "merged_mrs", "direction": "DESC"},
  "limit": 10
}
```

</details>

## Map dependencies and blast radius

Answer "what breaks if I change this?" before you change it.

```plaintext
Using Orbit, map the blast radius of <shared-auth-lib>.
- Which projects and files import it?
- Which code definitions depend on it?
- What would break if I changed its public interface?

Rank the affected areas by how many places depend on them, and tell me the
riskiest change I could make.
```

<details>
<summary>See the Orbit queries behind this</summary>

Find all files that import a specific module. Replace `payments-service` with
the module or library you want to trace:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "sym",
    "entity": "ImportedSymbol",
    "columns": ["file_path", "import_path", "identifier_name"],
    "filters": {
      "import_path": {"op": "contains", "value": "payments-service"}
    }
  }],
  "limit": 100
}
```

Find projects that depend on a shared library:

```json orbit-query
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

Rank the definitions that the most code imports:

```json orbit-query
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

## Keep your pipelines healthy

Find your worst CI/CD offenders and the reasons they fail.

```plaintext
Using Orbit, show me where our CI/CD is unhealthy over the last 30 days:
- The projects with the most failed pipelines.
- The jobs that fail most often.
- The most common failure reasons.

Group the results so I can see which failures are worth fixing first.
```

<details>
<summary>See the Orbit queries behind this</summary>

Find projects with the most failed pipelines:

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
  "group_by": [{"kind": "node", "node": "p"}],
  "aggregations": [
    {"function": "count", "target": "pl", "alias": "failed_count"}
  ],
  "aggregation_sort": {"column": "failed_count", "direction": "DESC"},
  "limit": 10
}
```

Find failed jobs and their failure reasons:

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

## Trace security risk to its source

See where your risk is and how it got there.

```plaintext
Using Orbit, find the critical and high severity vulnerabilities across
<my-org> that are still detected:
- Which projects are affected?
- How did each one get there? Trace it back to the scan and, where possible,
  the merge request that introduced the change.

Prioritize by severity and give me a short remediation shortlist.
```

<details>
<summary>See the Orbit queries behind this</summary>

Find all critical and high vulnerabilities:

```json orbit-query
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
  "order_by": "-v.severity",
  "limit": 50
}
```

Count vulnerabilities by project:

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
  "group_by": [{"kind": "node", "node": "p"}],
  "aggregations": [
    {"function": "count", "target": "v", "alias": "vuln_count"}
  ],
  "aggregation_sort": {"column": "vuln_count", "direction": "DESC"},
  "limit": 20
}
```

Count vulnerabilities by severity:

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

## Read the actual source

Pull real code into the conversation without leaving your agent.

```plaintext
Using Orbit, show me the source of <app/models/project.rb> and the definition
of <MyModule::my_function>, so I can review them here.
```

Virtual columns (`content` on `File` and `Definition`) trigger a Gitaly fetch
after the graph query, so these responses are slower than other queries.

<details>
<summary>See the Orbit queries behind this</summary>

Fetch the source text of a file. Use `limit: 1` to avoid large responses:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "f",
    "entity": "File",
    "columns": ["path", "language", "content"],
    "filters": {
      "path": {"op": "ends_with", "value": "app/models/project.rb"}
    }
  }],
  "limit": 1
}
```

Fetch the source text of a specific function or class definition. The `content`
field returns the raw source text of just that definition, not the full file:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "d",
    "entity": "Definition",
    "columns": ["name", "fqn", "file_path", "start_line", "end_line", "content"],
    "filters": {
      "fqn": {"op": "eq", "value": "Gitlab::Auth::authenticate"}
    }
  }],
  "limit": 5
}
```

</details>

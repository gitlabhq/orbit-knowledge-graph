---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Find the projects, jobs, and failure reasons behind your most unreliable pipelines.
title: Triage pipeline failures
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

Follow these steps when CI/CD is unreliable and you need to decide what to fix first.

- Time estimate: 5-15 minutes
- Level: Beginner

## The challenge

Pipeline failures are visible one project at a time, which makes it hard to see the failures
that repeat everywhere.

## The approach

Rank the projects, inspect the failure reasons, then prioritize the fixes.
Use GitLab Duo Chat or another agent connected to GitLab Orbit.

## Prerequisites

- A top-level group that GitLab Orbit Remote indexes.
- For an external agent, a connection through the [MCP server](../access/mcp.md) or the
  [`glab` CLI](../access/glab.md).

### Step 1: Rank the projects

Use your agent to find where the failures concentrate:

```plaintext
Using GitLab Orbit, show me the projects with the most failed pipelines over the
last 30 days.
```

Expected outcome: Ranked list of projects by failed pipeline count.

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
  "group_by": ["p"],
  "aggregations": [
    { "count": "pl", "as": "failed_count" }
  ],
  "aggregation_sort": "-failed_count",
  "limit": 10
}
```

### Step 2: Inspect the reasons

Use your agent to find what is actually failing:

```plaintext
Now show me the jobs that fail most often in those projects, and the most common
failure reasons.
```

Expected outcome: Ranked job names, each with the reasons its runs fail.

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

### Step 3: Prioritize the fixes

Use your agent to group the results into a work list:

```plaintext
Group these results so I can see which failures are worth fixing first, and tell
me which ones look like a shared problem rather than a single broken project.
```

Expected outcome: A short fix list, with the failures that repeat across projects called out.

## Tips

- Add a `created_at` filter to bound the result. Otherwise, the query counts every failure GitLab Orbit has indexed.
- Continue with [Attribute CI/CD compute cost to code](attribute-ci-cost.md) to find the code behind the failures.
- Name a single project in the prompt to rank that project's jobs instead of ranking projects
against each other.

## Verify

Ensure that:

- Projects are ranked by a failure count, not by name.
- The failure reasons are named, such as `script_failure` or `runner_system_failure`.

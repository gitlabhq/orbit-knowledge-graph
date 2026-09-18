---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Rank recurring CI/CD failures and trace them back to the files and definitions that cause them.
title: Attribute CI/CD compute cost to code
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

Follow these steps when CI/CD compute costs are rising and you cannot tell which code is
responsible.

- Time estimate: 30-60 minutes
- Level: Advanced

## The challenge

CI/CD compute is expensive, and most of the cost hides in failures that get retried over and
over.
Reports show you which jobs fail, but not which code keeps breaking them.

## The approach

Rank the failures, trace them back to code, then prioritize the fixes.
Use GitLab Duo Chat or another agent connected to GitLab Orbit.
Each step shows the queries the agent sends to GitLab Orbit, which you read to audit the answer
rather than edit.

## Prerequisites

- A top-level group that GitLab Orbit Remote indexes.
- For an external agent, a connection through the [MCP server](../access/mcp.md) or the
  [`glab` CLI](../access/glab.md).

### Step 1: Rank

Use your agent to find the failures worth investigating:

```plaintext
Using GitLab Orbit, find the job and pipeline failures across my organization
over the last 60 days, covering at least 20 projects. Rank the job names by how
often they fail.

Then flag any failing job name that recurs across three or more projects. Those
usually point to a shared CI/CD template that is worth fixing once.
```

Expected outcome: Ranked list of failing job names, with the cross-project ones flagged as
likely shared-template failures.

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

Find failing jobs that recur across multiple projects.
GitLab Orbit has no distinct-count function, so group by job name and project together.
A job name that fails in three or more projects usually comes from a CI/CD template those
projects share, so one fix to the template can fix every project.
Check the template before assuming it, because common job names such as `test` appear in many
projects independently.

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

### Step 2: Trace

Use your agent to follow the top failures back to the code:

```plaintext
For the top recurring failures, find the merge requests that generate the most
repeated failed pipelines.

Then trace those failures through the merge request diffs to the specific files,
and the code definitions inside those files, that keep changing.
```

Expected outcome: The merge requests behind the worst failures, and the files and definitions
that keep changing inside them.

Find the merge requests generating the most repeated failures.
Filter `source` to `merge_request_event` so you do not also count the downstream child pipelines
those pipelines triggered.

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

Trace one merge request to the files that keep changing:

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

Drill into the code definitions inside a file that keeps changing:

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

### Step 3: Prioritize

Use your agent to turn the chain into a fix list:

```plaintext
Show me the full chain from failing job to the exact code to review, and tell me
where to focus a fix to cut the most CI/CD spend.

Prioritize correctness and depth over speed.
```

Expected outcome: A short list of files and functions to fix, each tied to the failures it
causes.

## Tips

- Limit merge request traversal to a single merge request. Running the same traversal across every failed pipeline at once times out.
- Treat a short file list as incomplete coverage, because `HAS_FILE` edges are sparsely populated.
- Expect gaps for paths that GitLab Orbit does not index, such as test-support helpers.
- Scope the prompts to one group when the organization-wide result is too broad.
- Ask the agent to estimate the compute saved by fixing the top three failures.

## Verify

Ensure that:

- The chain names specific jobs and projects, not only failure counts.
- At least one failing job connects to a file and a definition inside that file.
- The results cover the time window you asked for.

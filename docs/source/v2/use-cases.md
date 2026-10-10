---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Prompts to ask your AI agent about your code, merge requests, pipelines, and security with GitLab Orbit.
title: GitLab Orbit use cases
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed
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

You do not write graph queries by hand.
You ask your agent a question, and the agent queries GitLab Orbit for the answer.

To use a prompt:

1. [Connect your agent](_index.md) to GitLab Orbit, or use GitLab Duo Agent Platform.
1. Copy the prompt.
1. Replace the values in `<angle brackets>` with your group, project, file, or time window.
1. Paste the prompt into your agent. Ask follow-up questions in the same conversation.

Code questions work on the local code graph.
Questions about merge requests, pipelines, or vulnerabilities need the GitLab server graph.

Each section also shows one query that the agent can run.
You do not need it, but you can use it to check the answer.
Each query is the `query` object of a request.
To run it yourself, put it in a [request envelope](queries/query-language.md#request-envelope).

## Understand a codebase

Get oriented in an unfamiliar project.

```plaintext
I'm new to <my-org/my-project>. Using GitLab Orbit, give me a tour:
- The core classes and modules, and how they relate.
- The main entry points.
- The three files I should read first, and why.
```

To look inside one file, the agent lists the definitions that the file contains:

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

## Find the blast radius of a change

Find out what breaks before you change it.

```plaintext
Using GitLab Orbit, map the blast radius of <shared-auth-lib>.
- Which projects and files import it?
- Which code definitions depend on it?
- What breaks if I change its public interface?

Rank the affected areas by how many places depend on them.
```

The agent ranks the definitions that the most code imports:

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

## Review the history of a project

Find out who changes the code and who to ask for a review.

```plaintext
Using GitLab Orbit, show me the review history of <my-org/my-project>:
- The most active authors and reviewers in the last 90 days.
- The merge requests that touched <app/models/project.rb>.
- The people I should ask to review a change to that file.
```

The agent counts the merged merge requests of each author:

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

## Keep your pipelines healthy

Find the CI/CD failures that cost the most.

```plaintext
Using GitLab Orbit, show me where our CI/CD is unhealthy in the last 30 days:
- The projects with the most failed pipelines.
- The jobs that fail most often, and their failure reasons.
- The job names that fail in three or more projects. These often come from
  a shared CI/CD template.

Group the results so I can see which failures to fix first.
```

To find the code behind the failures, ask a follow-up question:

```plaintext
For the top recurring failures, find the merge requests with the most failed
pipelines. Trace them through the merge request diffs to the files and code
definitions that keep changing. Tell me where a fix saves the most CI/CD time.
```

The agent ranks the projects by failed pipelines:

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

## Trace security risk to its source

Find where your risk is and how it got there.

```plaintext
Using GitLab Orbit, find the critical and high severity vulnerabilities in
<my-org> that are still detected:
- Which projects have them?
- Trace each one back to the scan and, where possible, to the merge request
  that introduced the change.

Sort by severity and give me a short list of fixes.
```

The agent finds the critical and high vulnerabilities and their projects:

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

## Read the source

Get the code into the conversation without a checkout.

```plaintext
Using GitLab Orbit, show me the source of <app/models/project.rb> and the
definition of <MyModule::my_function>.
```

The `content` column on `File` and `Definition` fetches the source from the repository after the graph query.
These queries are slower than other queries.
For a `Definition`, `content` returns only the source of that definition, not the full file:

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

## Related topics

- [Get started with GitLab Orbit](_index.md)
- [Connect AI agents](agents/_index.md)
- [Query the graph](queries/_index.md)
- [What GitLab Orbit indexes](schema.md)

---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Prompts to ask your agent about your code, merge requests, pipelines, and security with GitLab Orbit.
title: GitLab Orbit prompt examples
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- [Enabled on GitLab.com, GitLab Self-Managed, and GitLab Dedicated](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/245620) in GitLab 19.3.
- [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5. Feature flag `knowledge_graph` removed.

{{< /history >}}

Use any of the following interfaces to send a prompt to your agent:

- GitLab Duo Chat and GitLab Duo Agent Platform.
- Other agents such as Claude Code or Codex that connect to GitLab Orbit through the
  [GitLab Orbit CLI](agents/_index.md) or an [MCP server](agents/mcp.md).

To instruct the agent to search the graph instead of your repository, start the first prompt with something like `Using GitLab Orbit, ...`.
That exact phrase is not required, but if you don't mention `GitLab Orbit` in the prompt, the agent might use
another source to answer the question.
Later prompts continue in the same conversation, so you don't need to repeat the phrase.

Replace the values in `<angle brackets>`. The first two prompts, about code, also work on the
[local graph](local/_index.md) of a repository on your machine.

Each section also shows a [query](queries/query-language.md) that the agent can run.
To check an answer, replace the example values in the query, then run it with `glab orbit query "<query>"`.
Your agent can run any read-only query on the graph, including counts, averages, and trends over time.

## Understand a codebase

Get oriented in an unfamiliar project.

```plaintext
I'm new to <my-group/my-project>. Using GitLab Orbit, give me a tour:
- The core classes and modules, and how they relate.
- The main entry points.
- The three files I should read first, and why.
```

To look inside one file, the agent lists the definitions that the file contains:

```gql orbit-query
MATCH (f:File {path: 'app/models/user.rb'})-[:DEFINES]->(d:Definition)
RETURN d.name, d.fqn, d.definition_type, d.start_line
LIMIT 30
```

## Assess the impact of changing a shared library

Find out what breaks before you change shared code.

```plaintext
Using GitLab Orbit, find what depends on <shared-payments-lib>.
- Which projects and files import it?
- Which code definitions depend on it?
- What breaks if I change its public interface?

Rank the affected areas by how many places depend on them.
```

The agent ranks the definitions that the most code imports:

```gql orbit-query
MATCH (s:ImportedSymbol)-[:IMPORTS]->(d:Definition)
WHERE s.import_path CONTAINS 'payments'
RETURN d.name, d.fqn, d.file_path, count(s) AS import_count
ORDER BY import_count DESC
LIMIT 20
```

## Review the history of a project

Find out who changes the code and who to ask for a review.

```plaintext
Using GitLab Orbit, show me the review history of <my-group/my-project>:
- The most active authors and reviewers in the last 90 days.
- The merge requests that touched <app/models/project.rb>.
- The people I should ask to review a change to that file.
```

The agent counts the merged merge requests of each author:

```gql orbit-query
MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest)-[:IN_PROJECT]->(p:Project {full_path: 'my-group/my-project'})
WHERE mr.state = 'merged'
RETURN u.username, u.name, count(mr) AS merged_mrs
ORDER BY merged_mrs DESC
LIMIT 10
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

```gql orbit-query
MATCH (pl:Pipeline)-[:IN_PROJECT]->(p:Project)
WHERE pl.status = 'failed' AND p.full_path STARTS WITH 'my-group/'
RETURN p.name, p.full_path, count(pl) AS failed_count
ORDER BY failed_count DESC
LIMIT 10
```

## Find where vulnerabilities come from

Find which projects have vulnerabilities, and which changes introduced them.

```plaintext
Using GitLab Orbit, find the critical and high severity vulnerabilities in
<my-group> that are still detected:
- Which projects have them?
- Trace each one back to the scan and, where possible, to the merge request
  that introduced the change.

Sort by severity and give me a short list of fixes.
```

The agent finds the critical and high vulnerabilities in a project:

```gql orbit-query
MATCH (v:Vulnerability)-[:IN_PROJECT]->(p:Project {full_path: 'my-group/my-project'})
WHERE v.severity IN ['critical', 'high'] AND v.state = 'detected'
RETURN v.title, v.severity, v.state, v.report_type, p.full_path
ORDER BY v.severity ASC
LIMIT 50
```

## Answer package registry questions

Find the packages that depend on a library, and where a package came from.

```plaintext
Using GitLab Orbit, in <my-group>:
- Which packages declare a dependency on <lodash>, and in which versions?
- Which pipeline built <my-lib>, and did it pass?
- Which packages have a known vulnerability?
```

The agent finds the packages that declare the dependency:

```gql orbit-query
MATCH (pkg:Package)-[:DECLARES_DEPENDENCY]->(dep:Dependency {name: 'lodash'})
MATCH (pkg)-[:IN_PROJECT]->(p:Project)
RETURN p.full_path, pkg.name, pkg.version, pkg.package_type, dep.version_pattern
LIMIT 50
```

To trace a package to the pipeline that built it, the agent follows `BUILT_BY`:

```gql orbit-query
MATCH (pkg:Package {name: 'my-lib'})-[:BUILT_BY]->(pl:Pipeline)
RETURN pkg.name, pkg.version, pl.id, pl.ref, pl.sha, pl.status
ORDER BY pkg.version DESC
LIMIT 20
```

To find packages with known vulnerabilities, the agent follows `HAS_VULNERABILITY`:

```gql orbit-query
MATCH (pkg:Package)-[:HAS_VULNERABILITY]->(v:Vulnerability)
MATCH (pkg)-[:IN_PROJECT]->(p:Project)
WHERE p.full_path STARTS WITH 'my-group/'
RETURN p.full_path, pkg.name, pkg.version, v.title, v.severity
ORDER BY v.severity ASC
LIMIT 50
```

## Analyze trends across your group

Measure how your group delivers over time.

```plaintext
Using GitLab Orbit, for <my-group>:
- How many merge requests did we merge each month this year?
- Which projects have the slowest successful pipelines?
```

The agent counts the merged merge requests by month:

```gql orbit-query
MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project)
WHERE p.full_path STARTS WITH 'my-group/' AND mr.state = 'merged' AND mr.merged_at >= '2026-01-01T00:00:00Z'
RETURN date_trunc('month', mr.merged_at) AS month, count(mr) AS merged
ORDER BY month
LIMIT 12
```

Then it ranks the projects by the average duration of successful pipelines, in seconds:

```gql orbit-query
MATCH (pl:Pipeline)-[:IN_PROJECT]->(p:Project)
WHERE p.full_path STARTS WITH 'my-group/' AND pl.status = 'success'
RETURN p.full_path, avg(pl.duration) AS avg_seconds, count(pl) AS pipelines
ORDER BY avg_seconds DESC
LIMIT 10
```

## Read source code without a checkout

```plaintext
Using GitLab Orbit, show me the source of <app/models/project.rb> and the
definition of <MyModule::my_function>.
```

The `content` property of `File` and `Definition` fetches the source from the repository after the graph query.
These queries are slower than other queries.
For a `Definition`, `content` returns only the source of that definition, not the full file:

```gql orbit-query
MATCH (d:Definition {fqn: 'Gitlab::Auth::authenticate'})
RETURN d.name, d.fqn, d.file_path, d.start_line, d.end_line, d.content
LIMIT 5
```

## Related topics

- [GitLab Orbit quickstart](quickstart.md)
- [Connect your agent](agents/_index.md)
- [Query the graph](queries/_index.md)
- [Query language](queries/query-language.md)
- [Indexed data](schema.md)

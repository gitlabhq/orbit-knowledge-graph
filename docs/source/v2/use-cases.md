---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Prompts to ask your agent about your code, merge requests, pipelines, and security with GitLab Orbit.
title: GitLab Orbit use cases
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

Copy a prompt into your [connected agent](agents/_index.md) or GitLab Duo, replace the values in `<angle brackets>`,
and ask follow-up questions in the same conversation. The first two prompts, about code, also work on the
[local graph](local/_index.md) of a repository on your machine.

Each section also shows a [Cypher](queries/cypher.md) query that the agent can run.
To check an answer, replace the example values in the query, then run it with `glab orbit query "<query>"`.

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

## Find the blast radius of a change

Find out what breaks before you change shared code.

```plaintext
Using GitLab Orbit, map the blast radius of <shared-payments-lib>.
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

## Trace security risk to its source

Find where your risk is and how it got there.

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
- [Cypher query language](queries/cypher.md)
- [What GitLab Orbit indexes](schema.md)

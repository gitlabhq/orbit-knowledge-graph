---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Prompts to ask your agent about your code, merge requests, pipelines, deployments, and security with GitLab Orbit.
title: GitLab Orbit prompt examples
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

Send these prompts to GitLab Duo Chat, or to an agent that uses the [GitLab Orbit CLI](cli.md)
or a [Model Context Protocol (MCP) server](agents/mcp.md).
Start the first prompt with `Using GitLab Orbit`, and replace the values in `<angle brackets>`.
To check an answer, run the [query](queries/query-language.md) with `glab orbit query "<query>"`.
The queries use `project_id: 278964`.
To find your project ID, see [bounded nodes](queries/query-language.md#bounded-nodes).

## Change a class hierarchy safely

Find every class that inherits from a base class, before you change the base class.

```plaintext
Using GitLab Orbit, in <my-group>/<my-project>, which classes inherit from
<BasePolicy>, directly or through other classes? Which open merge requests
touch their files right now?
```

```gql orbit-query
MATCH (child:Definition)
  -[:EXTENDS*1..3]->(parent:Definition {
    fqn: 'BasePolicy', project_id: 278964, branch: 'master'})
RETURN child.fqn, child.file_path
ORDER BY child.file_path
LIMIT 100
```

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** about 2,400 REST API calls for a project with about 2,400 open merge requests,
  one for the changes of each, after you clone and parse the repository.

## See what shipped to production this week

Find the issues, tasks, and incidents that production deployments closed.

```plaintext
Using GitLab Orbit, which issues, tasks, and incidents did the merge
requests deployed to <production> in <my-group>/<my-project> close this
week? How many of them are bugs?
```

```gql orbit-query
MATCH (wi:WorkItem)
  <-[:CLOSES]-(mr:MergeRequest)
  -[:DEPLOYED_TO]->(d:Deployment)
  -[:IN_ENVIRONMENT]->(e:Environment {name: 'production'})
WHERE mr.project_id = 278964 AND d.created_at >= '2026-10-16T00:00:00Z'
RETURN wi.work_item_type, count(wi) AS shipped
ORDER BY shipped DESC
LIMIT 10
```

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** about 1,800 REST API calls for one week of a project that closes about 1,900 work items
  a week, one for each deployment and each deployed merge request.

## Find out how agent-written code performs

Find how the merge requests that agents create perform, from the head pipeline to production.

```plaintext
Using GitLab Orbit, for <my-group>/<my-project> in the last <30 days>: -
How many merge requests did agents create, by flow type? - How many merged
or closed, and what did their head pipelines do? - How many of them
reached <production>, and were any reverted?
```

```gql orbit-query
MATCH (a:AgentSession)
  -[:CREATED_MR]->(mr:MergeRequest)
  -[:HAS_HEAD_PIPELINE]->(pl:Pipeline)
WHERE mr.project_id = 278964 AND mr.created_at >= '2026-09-01T00:00:00Z'
RETURN a.flow_type, mr.state, pl.status, count(mr) AS mrs
ORDER BY mrs DESC
LIMIT 30
```

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** about 1,000 to 3,000 REST API calls for a project with about 800 merged agent
  merge requests, and no API lists the agent sessions of all users.

## Find the right reviewer for a file

Find who approves changes to a file, and what reviewers said about earlier changes.

```plaintext
Using GitLab Orbit, in <my-group>/<my-project>: - Who approved the merged
changes to <app/models/ability.rb> in the last six months? - What did
reviewers say about <N+1 queries> on merge requests that changed the file?
```

```gql orbit-query
MATCH (u:User)
  -[:APPROVED]->(mr:MergeRequest)
  -[:HAS_LATEST_DIFF]->(d:MergeRequestDiff)
  -[:HAS_FILE]->(df:MergeRequestDiffFile {
    project_id: 278964, old_path: 'app/models/ability.rb'})
WHERE mr.state = 'merged' AND mr.merged_at >= '2026-04-23T00:00:00Z'
RETURN u.username, count(mr) AS approved
ORDER BY approved DESC
LIMIT 10
```

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** about 40,000 REST API calls for a project that merges about 40,000 merge requests
  in six months, one for the changes of each, before you read approvals and comments.

## Unblock a release

Find the open merge requests in a milestone that have a failed pipeline, and their owners.

```plaintext
Using GitLab Orbit, in <my-group>/<my-project>: - Which open merge
requests in milestone <19.6> have a failed head pipeline, and who is
assigned to them? - Which open merge requests fail the job <unit-tests>,
and who authored them?
```

```gql orbit-query
MATCH (u:User)
  -[:ASSIGNED]->(mr:MergeRequest)
  -[:IN_MILESTONE]->(m:Milestone {title: '19.6'})
MATCH (mr)-[:HAS_HEAD_PIPELINE]->(pl:Pipeline {status: 'failed'})
WHERE mr.state = 'opened' AND mr.project_id = 278964
RETURN u.username, count(mr) AS red_mrs
ORDER BY red_mrs DESC
LIMIT 10
```

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** about 10,000 REST API calls for a project with about 2,400 open merge requests,
  one for the head pipeline of each, and three to four pages of jobs for each pipeline.

## Find a shared CI/CD job that fails across the group

Find the projects in a group where a job from a shared CI/CD template fails.

```plaintext
Using GitLab Orbit, which projects in <my-group> failed the job <docs-lint
markdown> in the last <seven days>, and how many times each?
```

```gql orbit-query
MATCH (j:Job {status: 'failed', name: 'docs-lint markdown'})
  -[:IN_PROJECT]->(p:Project)
WHERE p.full_path STARTS WITH 'my-group/'
  AND j.created_at >= '2026-10-16T00:00:00Z'
RETURN p.full_path, count(j) AS failures
ORDER BY failures DESC
LIMIT 20
```

- **With GitLab Orbit:** one query.
- **Without GitLab Orbit:** more than 9,400 REST API calls for a group with about 9,400 projects,
  at least one page of jobs for each project.

## Check that a security fix reached production

Find the vulnerabilities whose fix reached production, and if they are resolved.

```plaintext
Using GitLab Orbit, which vulnerabilities of <my-group>/<my-project> have
a fix that was deployed to <production>, and are they resolved?
```

```gql orbit-query
MATCH (v:Vulnerability)
  <-[:FIXES]-(mr:MergeRequest)
  -[:DEPLOYED_TO]->(d:Deployment)
  -[:IN_ENVIRONMENT]->(e:Environment {name: 'production'})
WHERE v.project_id = 278964
RETURN v.severity, v.state, count(v) AS vulns
ORDER BY vulns DESC
LIMIT 10
```

- **With GitLab Orbit:** one query.
- **Without GitLab Orbit:** no count, because no REST API call goes from a merge request to its deployments.
  You compare the merge commit of each fix with each deployment of the environment.

## Trace a function to its callers

Find the most called definitions in a file, and read the source of one.

```plaintext
Using GitLab Orbit, in <my-group>/<my-project>, which definitions in
<app/models/ability.rb> have the most callers? Show me the source of the
most called one.
```

```gql orbit-query
MATCH (caller:Definition)
  -[:CALLS]->(d:Definition)
  <-[:DEFINES]-(f:File {
    path: 'app/models/ability.rb', project_id: 278964, branch: 'master'})
RETURN d.fqn, count(caller) AS callers
ORDER BY callers DESC
LIMIT 30
```

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** no count, because no REST API call returns callers.
  You check out the repository and run a call-graph tool for the language.

## Find the consumers of a shared library

Find the projects in a group that import a library, before you change its interface.

```plaintext
Using GitLab Orbit, which projects in <my-group> import
<gitlab.com/my-group/my-sdk> on their default branch, and how many import
sites does each have?
```

```gql orbit-query
MATCH (s:ImportedSymbol)
  <-[:IMPORTS]-(f:File)
  -[:ON_BRANCH]->(b:Branch)
  <-[:CONTAINS]-(p:Project)
WHERE s.import_path STARTS WITH 'gitlab.com/my-group/my-sdk'
  AND p.full_path STARTS WITH 'my-group/'
  AND b.is_default = true
RETURN p.full_path, count(s) AS import_sites
ORDER BY import_sites DESC
LIMIT 30
```

- **With GitLab Orbit:** one query.
- **Without GitLab Orbit:** about 9,400 repository clones for a group with about 9,400 projects,
  then a search of the import statements in each.

## Related topics

- [GitLab Orbit quickstart](quickstart.md)
- [Connect your agent](agents/_index.md)
- [Query the graph](queries/_index.md)
- [Indexed data](schema.md)

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
or the [GitLab Model Context Protocol (MCP) endpoint](agents/mcp.md).
Start the first prompt with `Using GitLab Orbit`, and replace the values in `<angle brackets>`.
To check an answer, run the [query](queries/query-language.md) with `glab orbit query "<query>"`.
The queries run on GitLab.com, in the `gitlab-org` group and the `gitlab-org/gitlab` project
(`project_id: 278964`), where the production environment is `gprd`.
To find your project ID, see [bounded nodes](queries/query-language.md#bounded-nodes).
Each **Without GitLab Orbit** line counts REST API calls.
With the GraphQL API, some of these lookups take fewer calls.

## Change a class hierarchy safely

Find every class that inherits from a base class, and the open merge requests that touch those classes,
before you change the base class.

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

`MergeRequestDiffFile` connects to `Project`, not to `File` or `Definition`.
For the open merge requests, the agent runs a second query on the file paths that the first query returns.
The query filters on `old_path`, because `new_path` is empty when a merge request changes a file
but does not rename the file.

```gql orbit-query
MATCH (mr:MergeRequest)
  -[:HAS_LATEST_DIFF]->(d:MergeRequestDiff)
  -[:HAS_FILE]->(f:MergeRequestDiffFile {project_id: 278964})
WHERE mr.state = 'opened'
  AND f.old_path IN ['app/policies/base_policy.rb',
    'app/policies/group_policy.rb', 'app/policies/project_policy.rb']
RETURN mr.iid, f.old_path
ORDER BY mr.iid DESC
LIMIT 100
```

- **With GitLab Orbit:** two queries, one for the subclasses and one for the open merge requests.
- **Without GitLab Orbit:** about 2,300 REST API calls for a project with about 2,300 open merge requests,
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
  -[:IN_ENVIRONMENT]->(e:Environment {name: 'gprd'})
WHERE mr.project_id = 278964 AND d.created_at >= '2026-10-16T00:00:00Z'
RETURN wi.work_item_type, count(wi) AS shipped
ORDER BY shipped DESC
LIMIT 10
```

For the bugs, the agent adds the `type::bug` label to the query.
During an incident, ask a follow-up question:

```plaintext
Which merge requests reached <production> in the four hours before
<incident time>?
```

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** about 1,800 REST API calls for a project that deploys about 1,800 merge requests
  to production in a week, one for the merge requests of each deployment
  and one for the closed issues of each merge request.

## Find out how agent-written code performs

Find the share of merge requests from GitLab Duo agents and flows that merge, and how long they take.

```plaintext
Using GitLab Orbit, for merge requests that GitLab Duo agents and flows
created in <my-group>/<my-project> in the last <30 days>:
- What share merged, by flow type, and what is the median time from
  creation to merge?
- Which ones are still open after two weeks, and is their head pipeline
  failing?
```

```gql orbit-query
MATCH (a:AgentSession)
  -[:CREATED_MR]->(mr:MergeRequest)
WHERE mr.project_id = 278964 AND mr.created_at >= '2026-09-23T00:00:00Z'
RETURN a.flow_type, mr.state, count(mr) AS mrs
ORDER BY mrs DESC
LIMIT 30
```

The query language has no date arithmetic.
For the time to merge, the agent returns `mr.created_at` and `mr.merged_at` for each merged merge request,
and computes the median.
The graph has agent sessions for GitLab Duo agents and flows only,
so merge requests from other agents are not in the answer.

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** about 400 REST API calls for a project with about 400 agent merge requests in 30 days,
  one for each merge request, after you find them.
  No REST API call lists the agent sessions of all users.

## Find the right reviewer for a file

Find what reviewers said about earlier changes to a file, and who approved those changes.

```plaintext
Using GitLab Orbit, in <my-group>/<my-project>:
- What did reviewers say about <N+1 queries> on the merge requests that
  changed <app/models/ability.rb>?
- Who approved those merge requests?
```

```gql orbit-query
MATCH (n:Note)
  <-[:HAS_NOTE]-(mr:MergeRequest)
  -[:HAS_LATEST_DIFF]->(d:MergeRequestDiff)
  -[:HAS_FILE]->(f:MergeRequestDiffFile {
    project_id: 278964, old_path: 'app/models/ability.rb'})
WHERE n.note CONTAINS 'N+1'
RETURN mr.iid, n.note
ORDER BY mr.iid DESC
LIMIT 20
```

`HAS_LATEST_DIFF` follows the current diff version of each merge request.
`HAS_DIFF` covers every diff version, so it also matches a file that only an earlier version changed,
and it can return one row for each version.
Anchor the file on `old_path` and `project_id`, so the same path in another project does not match.

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** about 160 REST API calls for a file that about 80 merged merge requests changed,
  one to find each merge request from its commit and one for its comments.
  No REST API call searches review comments by file.

## Unblock a release

Find the failed jobs that open merge requests in a milestone have in common, and who owns those merge requests.

```plaintext
Using GitLab Orbit, for the open merge requests in milestone <19.6> of
<my-group>/<my-project> that have a failed head pipeline:
- Which failed jobs do they have in common?
- Who is assigned to the merge requests that fail each job?
```

```gql orbit-query
MATCH (u:User)
  -[:ASSIGNED]->(mr:MergeRequest)
  -[:IN_MILESTONE]->(m:Milestone {title: '19.6'})
MATCH (mr)-[:HAS_HEAD_PIPELINE]->(pl:Pipeline {status: 'failed'})
  -[:HAS_JOB]->(j:Job {status: 'failed'})
WHERE mr.state = 'opened' AND mr.project_id = 278964
  AND j.allow_failure = false
RETURN j.name, mr.iid, u.username
ORDER BY j.name
LIMIT 100
```

The agent groups the rows by `j.name`, so a job that fails on many merge requests stands out.

- **With GitLab Orbit:** one query.
- **Without GitLab Orbit:** about 1,500 REST API calls for a milestone with about 1,250 open merge requests,
  one for the head pipeline of each and one for the failed jobs of each failed pipeline.
  You then compare the jobs across merge requests.

## Find out if a failed job is flaky

Find out if a job fails because of your change, or if the job also fails on the default branch.

```plaintext
Using GitLab Orbit, my merge request in <my-group>/<my-project> fails the
job <docs-lint markdown>. Is it my change or a flaky job?
- How often did the job pass on <master> in the last <14 days>, and how
  did it fail?
- Which other open merge requests fail the job now?
```

```gql orbit-query
MATCH (j:Job {project_id: 278964, name: 'docs-lint markdown', ref: 'master'})
WHERE j.created_at >= '2026-10-09T00:00:00Z'
RETURN j.status, j.failure_reason, count(j) AS jobs
ORDER BY jobs DESC
LIMIT 10
```

To find out if the job fails in other projects too, ask a follow-up question:

```plaintext
Which projects in <my-group> failed the job <docs-lint markdown> in the
last <seven days>, and how many times each?
```

```gql orbit-query
MATCH (j:Job {status: 'failed', name: 'docs-lint markdown'})
  -[:IN_PROJECT]->(p:Project)
WHERE p.full_path STARTS WITH 'gitlab-org/'
  AND j.created_at >= '2026-10-16T00:00:00Z'
RETURN p.full_path, count(j) AS failures
ORDER BY failures DESC
LIMIT 20
```

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** at least 890 REST API calls for a project that ran the job about 890 times
  on the default branch in 14 days, one page of jobs for each pipeline,
  because the jobs API does not filter by job name.

## Fix a vulnerability across every affected project

Find every project that a vulnerability affects, fix each one, and follow the fixes to production.

```plaintext
Using GitLab Orbit, find every project in <my-group> affected by
<CVE-2025-58183>. Create an issue in each project, and start an agent
session to fix it. Then follow the deployments, and tell me when every fix
is in <production> or needs manual work.
```

GitLab Orbit is read-only, so your agent creates the issues and starts the sessions with `glab` or GitLab Duo.
GitLab Orbit finds the projects and tracks the deployments.

```gql orbit-query
MATCH (v:Vulnerability)-[:HAS_FINDING]->(o:VulnerabilityOccurrence)
  -[:HAS_IDENTIFIER]->(i:VulnerabilityIdentifier {external_id: 'CVE-2025-58183'})
MATCH (v)-[:IN_PROJECT]->(p:Project)
WHERE p.full_path STARTS WITH 'gitlab-org/'
  AND v.state IN ['detected', 'confirmed']
RETURN p.full_path, count(v) AS open_vulnerabilities
ORDER BY open_vulnerabilities DESC
LIMIT 50
```

To follow the fixes, the agent runs a second query.
This query lists the linked fixes in the group that reached a production environment.
To limit it to the CVE, the agent adds `AND v.id IN [...]` with the IDs of the vulnerabilities that it found.
`FIXES` covers only the merge requests that are linked to a vulnerability,
so tell your agent to link each fix to its vulnerability.
A project from the first answer with no linked fix in production needs manual work.

```gql orbit-query
MATCH (v:Vulnerability)
  <-[:FIXES]-(mr:MergeRequest)
  -[:DEPLOYED_TO]->(d:Deployment)
  -[:IN_ENVIRONMENT]->(e:Environment {tier: 'production'})
MATCH (v)-[:IN_PROJECT]->(p:Project)
WHERE p.full_path STARTS WITH 'gitlab-org/'
RETURN p.full_path, v.state, d.status, count(v) AS vulns
ORDER BY vulns DESC
LIMIT 50
```

To query vulnerabilities, you must have the Security Manager, Developer, Maintainer, or Owner role.
With the Reporter role, the queries return no rows.
For more information, see [roles](security.md#roles).

- **With GitLab Orbit:** two queries, one to find the projects and one to follow the fixes.
- **Without GitLab Orbit:** at least one REST API call for each project in a group with about 9,200 projects,
  to list its vulnerabilities and find the CVE.
  No REST API call goes from a vulnerability to the merge request that fixes it.

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
WHERE s.import_path STARTS WITH 'gitlab.com/gitlab-org/labkit'
  AND p.full_path STARTS WITH 'gitlab-org/'
  AND b.is_default = true
RETURN p.full_path, count(s) AS import_sites
ORDER BY import_sites DESC
LIMIT 30
```

Forks and mirrors import the library too, and they are often at the top of the results.
The graph does not mark forks or mirrors, so tell your agent which projects to leave out.

- **With GitLab Orbit:** one query.
- **Without GitLab Orbit:** a group code search for the import path, in a group with about 9,200 projects.
  Code search matches text, so it cannot count the parsed imports on the default branch of each project.
  To count them, you clone each project that the search finds.

## Related topics

- [GitLab Orbit quickstart](quickstart.md)
- [Connect your agent](agents/_index.md)
- [Query the graph](queries/_index.md)
- [Indexed data](schema.md)

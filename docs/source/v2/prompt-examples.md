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
Start the first prompt of a conversation with `Using GitLab Orbit` to make sure the agent uses GitLab Orbit, and replace the values in `<angle brackets>`.

Each example also shows the [queries](queries/query-language.md) that the agent runs, on the `gitlab-org/gitlab` project on GitLab.com.
To check an answer, replace the paths and IDs with those of your project, then run the query with `glab orbit query "<query>"`.
To find the IDs, see [bounded nodes](queries/query-language.md#bounded-nodes).

## Fix a vulnerability across every affected project

Find every project that a vulnerability affects, fix each one, and follow the fixes to production.

To query vulnerabilities, you must have the Security Manager, Developer, Maintainer, or Owner role.
With the Reporter role, the queries return no rows.
For more information, see [roles](security.md#roles).

```plaintext
Using GitLab Orbit, find every project in <my-group> affected by
<CVE-2025-58183>. Create an issue in each project, and start an agent
session to fix it.
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

To follow the fixes, ask a follow-up question:

```plaintext
Which of those fixes reached <production>, and which projects need manual
work?
```

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

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** at least one REST API call for each project in a group with about 9,200 projects,
  to list its vulnerabilities and find the CVE.
  No REST API call goes from a vulnerability to the merge request that fixes it.

## Respond to a production incident

Find the changes to a failing component that reached production, and the components that it depends on.

```plaintext
Using GitLab Orbit, <app/models/ci/> in <my-group>/<my-project> throws errors
in <production>. Which merge requests that changed it reached production in
the last <seven days>, and when was each deployed?
```

```gql orbit-query
MATCH (f:MergeRequestDiffFile)
  <-[:HAS_FILE]-(md:MergeRequestDiff)
  <-[:HAS_LATEST_DIFF]-(mr:MergeRequest)
  -[:DEPLOYED_TO]->(d:Deployment)
  -[:IN_ENVIRONMENT]->(e:Environment {name: 'gprd'})
WHERE mr.project_id = 278964 AND d.created_at >= '2026-10-17T00:00:00Z'
  AND f.old_path STARTS WITH 'app/models/ci/'
RETURN d.created_at, mr.iid, f.old_path
ORDER BY d.created_at DESC
LIMIT 50
```

To find the related components that reached production in the same window, ask a follow-up question:

```plaintext
Which components does <app/models/ci/build.rb> call, and which of them
changed in production deployments in the same window?
```

```gql orbit-query
MATCH (f:File {path: 'app/models/ci/build.rb', project_id: 278964, branch: 'master'})
  -[:DEFINES]->(d:Definition)
  -[:CALLS]->(callee:Definition)
RETURN callee.file_path, count(callee) AS calls
ORDER BY calls DESC
LIMIT 20
```

The agent then runs the first query on those file paths.

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** about 1,800 REST API calls for a project that deploys about 1,800 merge requests
  to production in a week, one for the changes of each deployed merge request.
  No REST API call returns the code that a file calls.

## Change shared code safely

Find what depends on a class or a library before you change it.

```plaintext
Using GitLab Orbit, in <my-group>/<my-project>, which classes inherit from
<BasePolicy>, directly or through other classes?
```

```gql orbit-query
MATCH (child:Definition)
  -[:EXTENDS*1..3]->(parent:Definition {
    fqn: 'BasePolicy', project_id: 278964, branch: 'master'})
RETURN child.fqn, child.file_path
ORDER BY child.file_path
LIMIT 500
```

On `gitlab-org/gitlab`, 366 classes inherit from `BasePolicy`.
To see more of what the change affects, ask follow-up questions:

```plaintext
Which definitions call <Ability::allowed?>, and how many callers does it have?
```

```gql orbit-query
MATCH (caller:Definition)
  -[:CALLS]->(d:Definition {
    fqn: 'Ability::allowed?', project_id: 278964, branch: 'master'})
RETURN caller.fqn, caller.file_path, caller.start_line
ORDER BY caller.file_path
LIMIT 1000
```

```plaintext
Which open merge requests touch <app/policies/base_policy.rb> right now?
```

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

The query filters on `old_path`, because `new_path` is empty when a merge request changes a file
but does not rename the file.

```plaintext
Which projects in <my-group> import <gitlab.com/my-group/my-sdk> on their
default branch, and how many import sites does each have?
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

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** no REST API call returns subclasses or callers.
  You check out the repository and run a call-graph tool for the language.
  For the projects that import a library, a group code search matches text, not parsed imports,
  so you clone each project that the search finds.

## Review a merge request with proof

Review a merge request against everything that it affects: the callers of the code that it changes,
other open merge requests on the same files, the issues that it closes, and the projects that import its code.

```plaintext
Using GitLab Orbit, review merge request <!264595> in <my-group>/<my-project>:
- Which definitions does it change, and which callers of those definitions
  are outside the merge request? Show the file and line of each caller.
- Which other open merge requests change the same files?
- Which issues does it close, and which projects in <my-group> import the
  code that it changes?
```

```gql orbit-query
MATCH (mr:MergeRequest {iid: 264595, project_id: 278964})
  -[:HAS_LATEST_DIFF]->(d:MergeRequestDiff)
  -[:HAS_FILE]->(f:MergeRequestDiffFile)
RETURN f.old_path, f.new_path, f.diff
LIMIT 100
```

A query cannot join two separate patterns, so the agent runs a second query on the files that the first one returns:

```gql orbit-query
MATCH (caller:Definition)
  -[:CALLS]->(d:Definition)
  <-[:DEFINES]-(f:File {project_id: 278964, branch: 'master'})
WHERE f.path IN ['app/services/discussions/resolve_service.rb',
  'app/services/discussions/unresolve_service.rb', 'app/services/notes/resolve_service.rb']
RETURN d.fqn, d.start_line, d.end_line, caller.fqn, caller.file_path, caller.start_line
LIMIT 100
```

For the other open merge requests and the importing projects, the agent runs the queries from
[Change shared code safely](#change-shared-code-safely) on the same files.
Calls are resolved inside one project, so imports show the other projects that depend on the code.

- **With GitLab Orbit:** two queries for the callers, and one query for each other check.
- **Without GitLab Orbit:** no REST API call returns callers.
  A reviewer searches a local checkout, and an agent that reviews the merge request sees only the diff.

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

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** about 400 REST API calls for a project with about 400 agent merge requests in 30 days,
  one for each merge request, after you find them.
  No REST API call lists the agent sessions of all users.

## Check projects against your security scan standard

Find the projects in a group that ran pipelines but skipped a required security scan.

To query security scans, you must have the Security Manager, Developer, Maintainer, or Owner role.
With the Reporter role, the scan query returns no rows.
For more information, see [roles](security.md#roles).

```plaintext
Using GitLab Orbit, which projects in <my-group> ran pipelines in the last
<30 days> but no <SAST>, <dependency scanning>, or <secret detection> scan?
```

```gql orbit-query
MATCH (pl:Pipeline)-[:IN_PROJECT]->(p:Project)
WHERE p.full_path STARTS WITH 'gitlab-org/ci-cd/' AND pl.created_at >= '2026-09-24T00:00:00Z'
RETURN p.full_path, count(pl) AS pipelines
LIMIT 1000
```

The query language has no subqueries, so the agent runs a second query for the scans, and compares the two lists:

```gql orbit-query
MATCH (s:SecurityScan)-[:IN_PROJECT]->(p:Project)
WHERE p.full_path STARTS WITH 'gitlab-org/ci-cd/' AND s.created_at >= '2026-09-24T00:00:00Z'
RETURN p.full_path, s.scan_type, count(s) AS scans
LIMIT 1000
```

The answer lists the active projects that are missing each scan type.

- **With GitLab Orbit:** two queries.
- **Without GitLab Orbit:** at least 550 REST API calls for a group with about 280 active projects,
  one for the pipelines and one for the security reports of each project.

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

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** about 160 REST API calls for a file that about 80 merged merge requests changed,
  one to find each merge request from its commit and one for its comments.
  No REST API call searches review comments by file.

## Related topics

- [GitLab Orbit quickstart](quickstart.md)
- [Connect your agent](agents/_index.md)
- [Query the graph](queries/_index.md)
- [Indexed data](schema.md)

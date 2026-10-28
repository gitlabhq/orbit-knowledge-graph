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
or the [GitLab Orbit Model Context Protocol (MCP) endpoint](agents/mcp.md).
Each example is one prompt that can have several steps.
Start the prompt with `Using GitLab Orbit` to make sure the agent uses GitLab Orbit, and replace the values in `<angle brackets>`.

Each example also shows the [queries](queries/query-language.md) that the agent runs, on projects in the `gitlab-org` group on GitLab.com.
To check an answer, replace the group and project paths, IDs, branch and environment names, and dates with your own, then run the query with `glab orbit query "<query>"`.
To find the IDs, see [bounded nodes](queries/query-language.md#bounded-nodes).

## Review a merge request with proof

An agent that reviews only the diff guesses what the change affects.
With GitLab Orbit, the agent gets proof from the graph: the code that calls the changed definitions,
the projects that import the changed package, and the other open merge requests on the same files.
Each finding in the review points to the query result that shows it.

```plaintext
Using GitLab Orbit, review merge request <!769> in <my-group>/<my-project>.
Back every finding with a query result, not a guess from the diff:
1. Which definitions does it change, and which callers of those
   definitions are outside the merge request? Give the file and line.
2. Which projects in <my-group> import the changed package, and how many
   files in each?
3. Which other open merge requests change the same files?
Then list the risks, each with the result that shows it.
```

For merge request `!769` in `gitlab-org/labkit`, which changes how the `v2/log` package reads its settings,
the agent runs four queries: two for the first step, and one for each other step.
First, it gets the files that the merge request changes, with their diffs:

```gql orbit-query
MATCH (mr:MergeRequest {iid: 769, project_id: 9438583})
  -[:HAS_LATEST_DIFF]->(d:MergeRequestDiff)
  -[:HAS_FILE]->(f:MergeRequestDiffFile)
RETURN f.old_path, f.new_path, f.diff
LIMIT 100
```

A query cannot join two separate patterns, so the agent reads the changed definitions from the diff,
then finds their callers outside the files that the merge request changes:

```gql orbit-query
MATCH (caller:Definition)
  -[:CALLS]->(d:Definition)
  <-[:DEFINES]-(f:File {project_id: 9438583, branch: 'master'})
WHERE f.path = 'v2/log/log.go'
  AND d.fqn IN ['log.NewWithConfig', 'log.applyConfigOverrides']
  AND NOT caller.file_path IN ['v2/log/log.go', 'v2/log/log_test.go', 'v2/log/doc.go']
RETURN d.fqn, d.start_line, caller.fqn, caller.file_path, caller.start_line
LIMIT 100
```

Calls are resolved inside one project, so imports show the other projects that depend on the code:

```gql orbit-query
MATCH (s:ImportedSymbol {import_path: 'gitlab.com/gitlab-org/labkit/v2/log'})
  <-[:IMPORTS]-(f:File)
  -[:ON_BRANCH]->(b:Branch)
  <-[:CONTAINS]-(p:Project)
WHERE p.full_path STARTS WITH 'gitlab-org/' AND p.id <> 9438583 AND b.is_default = true
RETURN p.full_path, count(f) AS files
ORDER BY files DESC
LIMIT 100
```

The other open merge requests on the same files:

```gql orbit-query
MATCH (mr:MergeRequest)
  -[:HAS_LATEST_DIFF]->(d:MergeRequestDiff)
  -[:HAS_FILE]->(f:MergeRequestDiffFile {project_id: 9438583})
WHERE mr.state = 'opened'
  AND f.old_path IN ['v2/log/log.go', 'v2/log/doc.go']
RETURN mr.iid, mr.title, f.old_path
LIMIT 50
```

The query filters on `old_path`, because `new_path` is empty when a merge request changes a file
but does not rename the file.

Inside `gitlab-org/labkit`, the only caller outside the merge request is a test.
But 23 other public projects in `gitlab-org` import `v2/log` in 481 files,
so the review reports a behavior change that reaches each of them.

- **With GitLab Orbit:** one prompt and four queries, and each finding cites the rows that show it.
- **Without GitLab Orbit:** no REST API call returns callers, or the projects that import a package.
  An agent that reviews the merge request sees only the diff, and a group code search matches text, not parsed imports.

## Fix a vulnerability across every affected project

Find every project that a vulnerability affects, fix each one, and follow the fixes to production.

To query vulnerabilities, you must have the Security Manager, Developer, Maintainer, or Owner role.
With the Reporter role, the queries return no rows.
For more information, see [roles](security.md#roles).

```plaintext
Using GitLab Orbit, for <CVE-2025-58183> in <my-group>:
1. Which projects have open vulnerabilities for the CVE?
2. Which fixes for it reached <production>?
3. For each project with no fix in production, create an issue and start
   an agent session to fix it.
```

GitLab Orbit is read-only, so your agent creates the issues and starts the sessions with `glab` or GitLab Duo.
GitLab Orbit finds the projects and tracks the deployments, with one query for each of the first two steps.
The first query returns the vulnerabilities in every state, because a fix that reached production usually resolves its vulnerability:

```gql orbit-query
MATCH (v:Vulnerability)-[:HAS_FINDING]->(o:VulnerabilityOccurrence)
  -[:HAS_IDENTIFIER]->(i:VulnerabilityIdentifier {external_id: 'CVE-2025-58183'})
MATCH (v)-[:IN_PROJECT]->(p:Project)
WHERE p.full_path STARTS WITH 'gitlab-org/'
RETURN p.full_path, v.id, v.state, v.severity
ORDER BY p.full_path
LIMIT 50
```

```gql orbit-query
MATCH (v:Vulnerability)
  <-[:FIXES]-(mr:MergeRequest)
  -[:DEPLOYED_TO]->(d:Deployment)
  -[:IN_ENVIRONMENT]->(e:Environment {tier: 'production'})
MATCH (v)-[:IN_PROJECT]->(p:Project)
WHERE v.id IN [236746171, 234672550, 241357118, 241981811, 234562423, 234562740, 234563175]
RETURN p.full_path, v.id, mr.iid, d.status
LIMIT 50
```

The agent passes the vulnerability IDs from the first query to the second.
In the public projects of `gitlab-org`, the CVE has 5 open vulnerabilities in 3 projects, and no fix for it reached production,
so the agent creates an issue in each of those projects.

- **With GitLab Orbit:** one prompt and two queries.
- **Without GitLab Orbit:** a group vulnerability export lists the vulnerabilities for the CVE in a group with about 9,200 projects,
  but no REST API call goes from a vulnerability to the merge request that fixes it,
  so you cannot tell which fixes reached production.

## Respond to a production incident

Find the changes to a failing component that reached production, and the components that it depends on.

```plaintext
Using GitLab Orbit, <app/models/ci/> in <my-group>/<my-project> throws errors
in <production>:
1. Which merge requests that changed it reached production in the last
   <seven days>, and when was each deployed?
2. Which components does <app/models/ci/build.rb> call, and which of them
   changed in production deployments in the same window?
Then rank the changes by how likely they are to cause the errors.
```

For the first step, the agent runs one query:

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

For the second step, it finds the code that the file calls:

```gql orbit-query
MATCH (f:File {path: 'app/models/ci/build.rb', project_id: 278964, branch: 'master'})
  -[:DEFINES]->(d:Definition)
  -[:CALLS]->(callee:Definition)
  <-[:DEFINES]-(cf:File)
WHERE cf.branch = 'master' AND cf.project_id = 278964
RETURN cf.path, count(callee) AS calls
ORDER BY calls DESC
LIMIT 20
```

The agent then runs the first query on those file paths.

- **With GitLab Orbit:** one prompt and one query for each step.
- **Without GitLab Orbit:** about 1,550 REST API calls for a project that deploys about 1,500 merge requests
  to production in a week: about 15 to list the deployed merge requests, and one for the changes of each.
  No REST API call returns the code that a file calls.

## Change shared code safely

Find what depends on a class or a library before you change it.

```plaintext
Using GitLab Orbit, before I change <BasePolicy> and <Ability::allowed?> in
<my-group>/<my-project>:
1. Which classes inherit from <BasePolicy>, directly or through other
   classes?
2. Which definitions call <Ability::allowed?>?
3. If the code is in a library, which projects in <my-group> import
   <gitlab.com/my-group/my-sdk> on their default branch?
```

For the first step, the agent finds the subclasses:

```gql orbit-query
MATCH (child:Definition)
  -[:EXTENDS*1..3]->(parent:Definition {
    fqn: 'BasePolicy', project_id: 278964, branch: 'master'})
RETURN child.fqn, child.file_path
ORDER BY child.file_path
LIMIT 500
```

A pattern follows up to three levels of inheritance.
On `gitlab-org/gitlab`, 366 classes inherit from `BasePolicy` in up to three levels.
Then it finds the callers:

```gql orbit-query
MATCH (caller:Definition)
  -[:CALLS]->(d:Definition {
    fqn: 'Ability::allowed?', project_id: 278964, branch: 'master'})
RETURN caller.fqn, caller.file_path, caller.start_line
ORDER BY caller.file_path
LIMIT 1000
```

The projects that import a library:

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
LIMIT 200
```

- **With GitLab Orbit:** one prompt and one query for each step.
- **Without GitLab Orbit:** no REST API call returns subclasses or callers.
  You check out the repository and run a call-graph tool for the language.
  For the projects that import a library, a group code search matches text, not parsed imports,
  so you clone each project that the search finds.

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

For the share that merged, the agent counts the merge requests by flow type and state:

```gql orbit-query
MATCH (a:AgentSession)
  -[:CREATED_MR]->(mr:MergeRequest)
WHERE mr.project_id = 278964 AND mr.created_at >= '2026-09-24T00:00:00Z'
RETURN a.flow_type, mr.state, count(mr) AS mrs
ORDER BY mrs DESC
LIMIT 30
```

The query language has no median function, so the agent gets the merged merge requests with their timestamps
and computes the median time to merge itself:

```gql orbit-query
MATCH (a:AgentSession)
  -[:CREATED_MR]->(mr:MergeRequest)
WHERE mr.project_id = 278964 AND mr.created_at >= '2026-09-24T00:00:00Z'
  AND mr.state = 'merged'
RETURN a.flow_type, mr.iid, mr.created_at, mr.merged_at
LIMIT 1000
```

For the merge requests that are still open after two weeks, with a failed head pipeline:

```gql orbit-query
MATCH (a:AgentSession)
  -[:CREATED_MR]->(mr:MergeRequest)
  -[:HAS_HEAD_PIPELINE]->(pl:Pipeline {status: 'failed'})
WHERE mr.project_id = 278964 AND mr.state = 'opened'
  AND mr.created_at >= '2026-09-24T00:00:00Z' AND mr.created_at < '2026-10-10T00:00:00Z'
RETURN mr.iid, mr.title, a.flow_type
LIMIT 100
```

- **With GitLab Orbit:** one prompt and three queries.
- **Without GitLab Orbit:** no REST API call lists agent sessions or links a merge request to the flow that created it,
  so you cannot find these merge requests or group them by flow type.

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
  AND s.status = 'succeeded'
RETURN p.full_path, s.scan_type, count(s) AS scans
LIMIT 1000
```

The answer lists the active projects that are missing each scan type.

- **With GitLab Orbit:** two queries.
- **Without GitLab Orbit:** about 4,800 REST API calls for a group with about 230 public projects:
  one to list the pipelines of each project, and one to list the jobs of each of the about 4,600 pipelines,
  because no REST API call lists the security scans that ran.

## Find out if a failed job is flaky

Find out if a job fails because of your change, or if the job also fails on the default branch.

```plaintext
Using GitLab Orbit, my merge request in <my-group>/<my-project> fails the
job <docs-lint markdown>. Is it my change or a flaky job?
- How often did the job pass on <master> in the last <14 days>, and how
  did it fail?
- Which other open merge requests fail the job now?
- Which projects in <my-group> failed the job in the last <seven days>,
  and how many times each?
```

```gql orbit-query
MATCH (j:Job {project_id: 278964, name: 'docs-lint markdown', ref: 'master'})
WHERE j.created_at >= '2026-10-10T00:00:00Z'
RETURN j.status, j.failure_reason, count(j) AS jobs
ORDER BY jobs DESC
LIMIT 10
```

The other open merge requests that fail the job in their head pipeline:

```gql orbit-query
MATCH (mr:MergeRequest {project_id: 278964, state: 'opened'})
  -[:HAS_HEAD_PIPELINE]->(pl:Pipeline)
  -[:HAS_JOB]->(j:Job {name: 'docs-lint markdown', status: 'failed'})
RETURN mr.iid, mr.title, j.failure_reason, j.created_at
ORDER BY j.created_at DESC
LIMIT 20
```

To find out if the job fails in other projects too:

```gql orbit-query
MATCH (j:Job {status: 'failed', name: 'docs-lint markdown'})
  -[:IN_PROJECT]->(p:Project)
WHERE p.full_path STARTS WITH 'gitlab-org/'
  AND j.created_at >= '2026-10-17T00:00:00Z'
RETURN p.full_path, count(j) AS failures
ORDER BY failures DESC
LIMIT 20
```

- **With GitLab Orbit:** one prompt and one query for each question.
- **Without GitLab Orbit:** at least 2,900 REST API calls for a project that ran about 2,900 pipelines
  on the default branch in 14 days, one or more pages of jobs for each pipeline,
  because the jobs API does not filter by job name. The job ran in about 800 of those pipelines.

## Unblock a release

Find the failed jobs that open merge requests in a milestone have in common, and who owns those merge requests.

```plaintext
Using GitLab Orbit, for the open merge requests in milestone <19.6> of
<my-group>/<my-project> that have a failed head pipeline:
- Which failed jobs do they have in common?
- Who is assigned to the merge requests that fail each job?
```

For the jobs in common, the agent gets the failed jobs of each head pipeline.
It leaves out retried jobs and jobs that are allowed to fail:

```gql orbit-query
MATCH (mr:MergeRequest {project_id: 278964, state: 'opened'})
  -[:IN_MILESTONE]->(m:Milestone {title: '19.6'})
MATCH (mr)-[:HAS_HEAD_PIPELINE]->(pl:Pipeline {status: 'failed'})
  -[:HAS_JOB]->(j:Job {status: 'failed', allow_failure: false, retried: false})
WHERE pl.created_at >= '2026-10-17T00:00:00Z'
RETURN j.name, mr.iid
ORDER BY j.name
LIMIT 1000
```

The agent groups the rows by `j.name`, so a job that fails on many merge requests stands out.
Then, for each of those jobs, it finds who is assigned to the merge requests that fail it:

```gql orbit-query
MATCH (u:User)
  -[:ASSIGNED]->(mr:MergeRequest {project_id: 278964, state: 'opened'})
  -[:IN_MILESTONE]->(m:Milestone {title: '19.6'})
MATCH (mr)-[:HAS_HEAD_PIPELINE]->(pl:Pipeline {status: 'failed'})
  -[:HAS_JOB]->(j:Job {name: 'e2e:test-on-cng', status: 'failed', allow_failure: false, retried: false})
WHERE pl.created_at >= '2026-10-17T00:00:00Z'
RETURN u.username, count(mr) AS mrs
ORDER BY mrs DESC
LIMIT 20
```

- **With GitLab Orbit:** one prompt and two queries.
- **Without GitLab Orbit:** about 1,700 REST API calls for a milestone with about 1,400 open merge requests,
  one for the head pipeline of each and one for the failed jobs of each of the about 280 failed pipelines.
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

Then it finds who approved the merged merge requests that changed the file:

```gql orbit-query
MATCH (u:User)
  -[:APPROVED]->(mr:MergeRequest {state: 'merged'})
  -[:HAS_LATEST_DIFF]->(d:MergeRequestDiff)
  -[:HAS_FILE]->(f:MergeRequestDiffFile {project_id: 278964, old_path: 'app/models/ability.rb'})
RETURN u.username, count(mr) AS approvals
ORDER BY approvals DESC
LIMIT 10
```

`HAS_LATEST_DIFF` follows the current diff version of each merge request.
`HAS_DIFF` covers every diff version, so it also matches a file that only an earlier version changed,
and it can return one row for each version.

- **With GitLab Orbit:** one query for each question.
- **Without GitLab Orbit:** about 470 REST API calls for a file that about 300 commits and about 80 merged merge requests changed:
  one for each commit to find its merge request, then one for the comments and one for the approvals of each merge request.
  No REST API call filters comments by the file that a merge request changed.

## Related topics

- [GitLab Orbit quickstart](quickstart.md)
- [Connect your agent](agents/_index.md)
- [Query the graph](queries/_index.md)
- [Indexed data](schema.md)

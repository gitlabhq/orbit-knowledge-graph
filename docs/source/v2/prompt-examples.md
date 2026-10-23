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

Use any of the following interfaces to send a prompt to your agent:

- GitLab Duo Chat and GitLab Duo Agent Platform
- Other agents such as Claude Code or Codex that connect to GitLab Orbit through the
  [GitLab Orbit CLI](cli.md) or a [Model Context Protocol (MCP) server](agents/mcp.md)

To instruct the agent to search the graph instead of your repository, start the first prompt with a phrase like `Using GitLab Orbit`.
You do not have to use that exact phrase, but if you don't mention `GitLab Orbit` in the prompt, the agent might use
another source to answer the question.
Later prompts continue in the same conversation, so you don't need to repeat the phrase.

Replace the values in `<angle brackets>`.
Questions about code alone, such as which classes inherit from a class, also work on the
[local graph](cli.md#local-graph) of a repository on your machine.

Each example shows a [query](queries/query-language.md) like the one that GitLab Orbit runs.
It also shows the API calls or manual steps that the same answer takes without GitLab Orbit.
To check an answer, replace the example values in the query, then run it with `glab orbit query "<query>"`.
The examples use `project_id: 278964` where a query needs a project ID.
To find the ID of your project, see [bounded nodes](queries/query-language.md#bounded-nodes).
Your agent can run any read-only query on the graph, including counts, averages, and trends over time.

## Code

### Change a class hierarchy safely

Find every class that inherits from a base class, directly or through other classes, before you change the base class.

```plaintext
Using GitLab Orbit, in <my-group>/<my-project>, which classes inherit from <BasePolicy>,
directly or through other classes? Which open merge requests touch their files right now?
```

GitLab Orbit runs a query like this:

```gql orbit-query
MATCH (child:Definition)-[:EXTENDS*1..3]->(parent:Definition {fqn: 'BasePolicy', project_id: 278964, branch: 'master'})
RETURN child.fqn, child.file_path
ORDER BY child.file_path
LIMIT 100
```

Without GitLab Orbit, you clone the repository and parse the code, because a text search finds only the direct subclasses.
The merge requests API cannot filter by changed file.
You read the changes of each open merge request instead: thousands of REST API calls for a large project.

### Trace a function to its callers

Find the definitions in a file that the most code calls, and read the source of one of them.

```plaintext
Using GitLab Orbit, in <my-group>/<my-project>, which definitions in <app/models/ability.rb>
have the most callers? Show me the source of the most called one.
```

GitLab Orbit runs a query like this:

```gql orbit-query
MATCH (caller:Definition)-[:CALLS]->(d:Definition)<-[:DEFINES]-(f:File {path: 'app/models/ability.rb', project_id: 278964, branch: 'master'})
RETURN d.fqn, count(caller) AS callers
ORDER BY callers DESC
LIMIT 30
```

Without GitLab Orbit, you check out the repository and run a tool that resolves calls for the language.
A text search for a common method name returns every method with that name, which can be thousands of matches
in a large project.

### Find the consumers of a shared library

Find the projects in a group that import a library, before you change its interface.

```plaintext
Using GitLab Orbit, which projects in <my-group> import <gitlab.com/my-group/my-sdk>
on their default branch, and how many import sites does each have?
```

GitLab Orbit runs a query like this:

```gql orbit-query
MATCH (s:ImportedSymbol)<-[:IMPORTS]-(f:File)-[:ON_BRANCH]->(b:Branch)<-[:CONTAINS]-(p:Project)
WHERE s.import_path STARTS WITH 'gitlab.com/my-group/my-sdk' AND p.full_path STARTS WITH 'my-group/' AND b.is_default = true
RETURN p.full_path, count(s) AS import_sites
ORDER BY import_sites DESC
LIMIT 30
```

Without GitLab Orbit, you clone every project in the group and search its import statements.
A large group has hundreds or thousands of projects to clone.

## Merge requests

### Find the right reviewer for a file

Find the people who approve changes to a file, and what reviewers said about earlier changes to it.

```plaintext
Using GitLab Orbit, in <my-group>/<my-project>:
- Who approved the merged changes to <app/models/ability.rb> in the last six months?
- What did reviewers say about <N+1 queries> on merge requests that changed the file?
```

GitLab Orbit runs a query like this:

```gql orbit-query
MATCH (u:User)-[:APPROVED]->(mr:MergeRequest)-[:HAS_LATEST_DIFF]->(d:MergeRequestDiff)-[:HAS_FILE]->(df:MergeRequestDiffFile {project_id: 278964, old_path: 'app/models/ability.rb'})
WHERE mr.state = 'merged' AND mr.merged_at >= '2026-04-23T00:00:00Z'
RETURN u.username, count(mr) AS approved
ORDER BY approved DESC
LIMIT 10
```

Without GitLab Orbit, you run `git log` on the file in a checkout, and make one REST API call for each commit
to find its merge request.
Then you make one call for each merge request to read its approvals and comments.
Without a checkout, you read the changes of every merged merge request, because the merge requests API cannot
filter by changed file.
For six months of a large project, that takes tens of thousands of calls.

### Find out how agent-written code performs

Find how the merge requests that AI agents create perform, from the head pipeline to the merge.

```plaintext
Using GitLab Orbit, for <my-group>/<my-project> in the last <30 days>:
- How many merge requests did agents create, by flow type?
- How many merged or closed, and what did their head pipelines do?
- How many of them reached <production>, and were any reverted?
```

GitLab Orbit runs a query like this:

```gql orbit-query
MATCH (a:AgentSession)-[:CREATED_MR]->(mr:MergeRequest)-[:HAS_HEAD_PIPELINE]->(pl:Pipeline)
WHERE mr.project_id = 278964 AND mr.created_at >= '2026-09-01T00:00:00Z'
RETURN a.flow_type, mr.state, pl.status, count(mr) AS mrs
ORDER BY mrs DESC
LIMIT 30
```

Without GitLab Orbit, you collect the agent sessions and the merge requests that they created.
Then you make one REST API call for each merge request to get its head pipeline.
More calls match the merge requests to deployments and reverts: thousands of calls for a large project.

## CI/CD pipelines

### Unblock a release

Find the open merge requests in a milestone that have a failed pipeline, and the people who own them.

```plaintext
Using GitLab Orbit, in <my-group>/<my-project>:
- Which open merge requests in milestone <19.6> have a failed head pipeline, and who is assigned to them?
- Which open merge requests fail the job <unit-tests>, and who authored them?
```

GitLab Orbit runs a query like this:

```gql orbit-query
MATCH (u:User)-[:ASSIGNED]->(mr:MergeRequest)-[:IN_MILESTONE]->(m:Milestone {title: '19.6'})
MATCH (mr)-[:HAS_HEAD_PIPELINE]->(pl:Pipeline {status: 'failed'})
WHERE mr.state = 'opened' AND mr.project_id = 278964
RETURN u.username, count(mr) AS red_mrs
ORDER BY red_mrs DESC
LIMIT 10
```

Without GitLab Orbit, you list the open merge requests.
Then you make one REST API call for each merge request to get its head pipeline.
More calls page through the jobs of each pipeline: thousands of calls for a large project.

### Find a shared CI/CD job that fails across the group

Find the projects where a job from a shared CI/CD template fails.

```plaintext
Using GitLab Orbit, which projects in <my-group> failed the job <docs-lint markdown>
in the last <seven days>, and how many times each?
```

GitLab Orbit runs a query like this:

```gql orbit-query
MATCH (j:Job {status: 'failed', name: 'docs-lint markdown'})-[:IN_PROJECT]->(p:Project)
WHERE p.full_path STARTS WITH 'my-group/' AND j.created_at >= '2026-10-16T00:00:00Z'
RETURN p.full_path, count(j) AS failures
ORDER BY failures DESC
LIMIT 20
```

Without GitLab Orbit, you page through the failed jobs of each project in the group, and filter them by job name.
That takes at least one REST API call for each project, and many pages for a busy project.

## Deployments and security

### See what shipped to production this week

Find the issues, tasks, and incidents that the merge requests deployed to production closed.

```plaintext
Using GitLab Orbit, which issues, tasks, and incidents did the merge requests deployed to
<production> in <my-group>/<my-project> close this week? How many of them are bugs?
```

GitLab Orbit runs a query like this:

```gql orbit-query
MATCH (wi:WorkItem)<-[:CLOSES]-(mr:MergeRequest)-[:DEPLOYED_TO]->(d:Deployment)-[:IN_ENVIRONMENT]->(e:Environment {name: 'production'})
WHERE mr.project_id = 278964 AND d.created_at >= '2026-10-16T00:00:00Z'
RETURN wi.work_item_type, count(wi) AS shipped
ORDER BY shipped DESC
LIMIT 10
```

Without GitLab Orbit, you list the deployments to the environment.
Then you make one REST API call for each deployment to list its merge requests.
One more call for each merge request lists the issues that it closes: thousands of calls for a large project.

### Check that a security fix reached production

Find the vulnerabilities whose fix reached production, and the ones that are still detected after the fix shipped.

```plaintext
Using GitLab Orbit, which vulnerabilities of <my-group>/<my-project> have a fix that was
deployed to <production>, and are they resolved?
```

GitLab Orbit runs a query like this:

```gql orbit-query
MATCH (v:Vulnerability)<-[:FIXES]-(mr:MergeRequest)-[:DEPLOYED_TO]->(d:Deployment)-[:IN_ENVIRONMENT]->(e:Environment {name: 'production'})
WHERE v.project_id = 278964
RETURN v.severity, v.state, count(v) AS vulns
ORDER BY vulns DESC
LIMIT 10
```

Without GitLab Orbit, you list the vulnerabilities and find the merge request that fixes each one.
Then you compare each merge commit with the deployments of the environment:
several REST API calls for each vulnerability.

## Related topics

- [GitLab Orbit quickstart](quickstart.md)
- [Connect your agent](agents/_index.md)
- [Query the graph](queries/_index.md)
- [Indexed data](schema.md)

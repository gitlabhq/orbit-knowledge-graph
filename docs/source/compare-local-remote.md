---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Learn about the differences between GitLab Orbit Remote and Local, and the intended use cases for each.
title: Compare GitLab Orbit Remote and Local
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Beta

{{< /details >}}

{{< history >}}

- GitLab Orbit Remote [introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- GitLab Orbit Remote [changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- GitLab Orbit Local [introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an experiment.
- GitLab Orbit Local [changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) to beta in GitLab 19.1.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

GitLab Orbit creates a read-only property graph you can query.

GitLab Orbit Local runs on your machine and builds a code-only graph from a repository you have
checked out.

GitLab Orbit Remote runs on GitLab infrastructure and builds a graph of merged source code together
with your software development lifecycle (SDLC) data, including groups, projects, users, merge
requests, pipelines, work items, and security findings.

The following sections compare GitLab Orbit Remote and Local so you
can determine which one is right for you, and how to use them
effectively.

## GitLab Orbit on GitLab Self-Managed

GitLab Orbit on GitLab Self-Managed is GitLab Orbit Remote that you run yourself.
You deploy it as a Helm chart on a Kubernetes cluster next to your instance, together with the
data pipeline that feeds it.

For more information, see [GitLab Orbit on GitLab Self-Managed](self-managed/_index.md).

## Deployment and network

| Deployment | GitLab Orbit Local | GitLab Orbit Remote |
|------------|--------------------|---------------------|
| Runs on your machine | {{< yes >}} | {{< no >}} |
| Runs on GitLab infrastructure | {{< no >}} | {{< yes >}} |
| Graph storage | DuckDB file at `~/.orbit/graph.duckdb` | Managed ClickHouse |
| Storage you set up | {{< no >}} | {{< no >}} |
| Network connection required to query | {{< no >}} | {{< yes >}} |
| GitLab instance required | {{< no >}} | {{< yes >}} |

Choose GitLab Orbit Local when the network is unavailable, or when the code must not leave your
machine. You still need a network connection to:

- Install or update the binary.
- Install the GitLab Orbit skill.
- Send telemetry data that the CLI sends by default. You can turn off telemetry.

After you install the GitLab Orbit binary, index and query commands are entirely local, and no request leaves your
computer to build or read the graph.

GitLab Orbit Remote:

- Runs in a separate Kubernetes cluster from your GitLab instance, so the two do
not share compute or memory.
- Is read-only. It reads changes from GitLab and never writes back.

## Authentication and authorization

| Access control | GitLab Orbit Local | GitLab Orbit Remote |
|----------------|--------------------|---------------------|
| GitLab account required | {{< no >}} | {{< yes >}} |
| Token or sign-in required | {{< no >}} | {{< yes >}} |
| Results scoped to your role | {{< no >}} | {{< yes >}} |
| Minimum role to query | None | Reporter |
| Minimum role for security data | None | Security Manager |
| Minimum role to turn on indexing | None | Owner on the top-level group |

GitLab Orbit Local has no authorization layer, and does not
require authentication.
GitLab Orbit Remote delegates every access decision to GitLab.

Programmatic access to GitLab Orbit Remote uses your existing GitLab authentication.

For more information, see [GitLab Orbit Remote security](remote/security.md).

## What data GitLab Orbit indexes

GitLab Orbit Local and Remote index different types of data.
The following sections list what each feature indexes.

GitLab Orbit Remote and Local do not index:

- Binary files
- Branches other than the checked out branch (GitLab Orbit Local) or the default branch (GitLab Orbit Remote)

### Source code

| Code structure | GitLab Orbit Local | GitLab Orbit Remote |
|----------------|--------------------|---------------------|
| Files and directories | {{< yes >}} | {{< yes >}} |
| Function, class, method, and module definitions | {{< yes >}} | {{< yes >}} |
| Import declarations | {{< yes >}} | {{< yes >}} |
| Cross-file symbol references | {{< yes >}} | {{< yes >}} |

### Groups, projects, and users

| Groups, projects, and users | GitLab Orbit Local | GitLab Orbit Remote |
|-----------------------------|--------------------|---------------------|
| Groups | {{< no >}} | {{< yes >}} |
| Projects | {{< no >}} | {{< yes >}} |
| Users | {{< no >}} | {{< yes >}} |
| Notes and comments | {{< no >}} | {{< yes >}} |

### Code review

| Code review | GitLab Orbit Local | GitLab Orbit Remote |
|-------------|--------------------|---------------------|
| Merge requests | {{< no >}} | {{< yes >}} |
| Merge request diffs | {{< no >}} | {{< yes >}} |
| Changed files | {{< no >}} | {{< yes >}} |

### CI/CD pipelines

| CI/CD | GitLab Orbit Local | GitLab Orbit Remote |
|-------|--------------------|---------------------|
| Pipelines | {{< no >}} | {{< yes >}} |
| Stages | {{< no >}} | {{< yes >}} |
| Jobs | {{< no >}} | {{< yes >}} |

### Code planning

| Code planning | GitLab Orbit Local | GitLab Orbit Remote |
|---------------|--------------------|---------------------|
| Issues | {{< no >}} | {{< yes >}} |
| Epics | {{< no >}} | {{< yes >}} |
| Tasks | {{< no >}} | {{< yes >}} |
| Incidents | {{< no >}} | {{< yes >}} |
| Milestones | {{< no >}} | {{< yes >}} |
| Labels | {{< no >}} | {{< yes >}} |

### Security

| Security | GitLab Orbit Local | GitLab Orbit Remote |
|----------|--------------------|---------------------|
| Vulnerabilities | {{< no >}} | {{< yes >}} |
| Security findings | {{< no >}} | {{< yes >}} |
| Security scans | {{< no >}} | {{< yes >}} |
| Scanners | {{< no >}} | {{< yes >}} |
| CVE identifiers | {{< no >}} | {{< yes >}} |
| CWE identifiers | {{< no >}} | {{< yes >}} |

## Supported languages

GitLab Orbit Remote and Local index code in the same languages.

| Language | Definitions | Cross-file references |
|----------|-------------|-----------------------|
| Ruby | {{< yes >}} | {{< yes >}} |
| Java | {{< yes >}} | {{< yes >}} |
| Kotlin | {{< yes >}} | {{< yes >}} |
| Python | {{< yes >}} | {{< yes >}} |
| TypeScript | {{< yes >}} | {{< yes >}} |
| JavaScript | {{< yes >}} | {{< yes >}} |
| Rust | {{< yes >}} | {{< yes >}} |
| Go | {{< yes >}} | {{< yes >}} |
| C# | {{< yes >}} | {{< yes >}} |
| C | {{< yes >}} | {{< yes >}} |
| C++ | {{< yes >}} | {{< yes >}} |
| PHP | {{< yes >}} | {{< yes >}} |
| Bash/Shell | {{< yes >}} | {{< no >}} |

## Work scope and freshness

| Scope | GitLab Orbit Local | GitLab Orbit Remote |
|-------|--------------------|---------------------|
| Working tree, including uncommitted files | {{< yes >}} | {{< no >}} |
| Default branch only | {{< no >}} | {{< yes >}} |
| Multiple repositories in one graph | {{< yes >}} | {{< yes >}} |
| Whole top-level group | {{< no >}} | {{< yes >}} |
| Branch selection | {{< no >}} | {{< no >}} |
| Updates automatically | {{< no >}} | {{< yes >}} |

GitLab Orbit Remote and Local see different versions of your code.

GitLab Orbit Local:

- Indexes the working tree as it is on disk
- Includes files you have
not committed, and excludes `.gitignore`
- Never checks out another branch
- Requires manual re-indexing to refresh the graph

GitLab Orbit Remote:

- Indexes the default branch of every project in the top-level groups where you
turned GitLab Orbit on
- Reindexes the graph automatically when the default branch changes

## Supported tooling

| Access method | GitLab Orbit Local | GitLab Orbit Remote |
|---------------|--------------------|---------------------|
| GitLab Orbit CLI (`orbit`) | {{< yes >}} | {{< no >}} |
| GitLab CLI (`glab orbit local` and `glab orbit remote`) | {{< yes >}} | {{< yes >}} |
| MCP | {{< yes >}} | {{< yes >}} |
| REST API | {{< no >}} | {{< yes >}} |
| GitLab Duo Agent Platform | {{< no >}} | {{< yes >}} |
| GitLab UI | {{< no >}} | {{< yes >}} |

## Query interface

| Querying | GitLab Orbit Local | GitLab Orbit Remote |
|----------|--------------------|---------------------|
| Read-only SQL | {{< yes >}} | {{< no >}} |
| JSON query DSL | {{< no >}} | {{< yes >}} |
| Natural language through an agent | {{< yes >}} | {{< yes >}} |
| Query results scoped by permissions | {{< no >}} | {{< yes >}} |

## The GitLab Orbit skill

| Skill capability | GitLab Orbit Local | GitLab Orbit Remote |
|------------------|--------------------|---------------------|
| Query language guidance | Read-only SQL | JSON query DSL |
| Paste-ready query recipes | {{< no >}} | {{< yes >}} |
| Repository map helper | {{< yes >}} | {{< yes >}} |
| Reporting and coverage guidance | {{< no >}} | {{< yes >}} |
| Setup checklist and troubleshooting | {{< yes >}} | {{< yes >}} |

The GitLab Orbit skill gives AI coding agents structured guidance for graph queries.
You use the same skill for GitLab Orbit Remote and Local, but the guidance differs.

For more information, see
[set up AI coding agents with the GitLab Orbit skill](ai_coding_agents.md).

## Recommendations for developers

Use GitLab Orbit Local when you have questions about the code you're working on.
GitLab Orbit Local works offline, indexes the branch you are on, and
gives AI coding agents real structure to work with.

Developers use GitLab Orbit Local to:

- Get oriented with an unfamiliar repository
- Find every caller of a function
before a rename
- Map what a change touches

Use GitLab Orbit Remote when you need more context about the code
you're working on.

Developers use GitLab Orbit Remote to:

- Assess a blast radius across projects
- Check code review history
- Trace a vulnerability
back to the change that introduced it

## Recommendations for product and engineering managers, security teams, and support

Use GitLab Orbit Remote with GitLab Duo Agent Platform.

You can ask a question in plain language in the GitLab UI, and the agent queries the graph and answers.
Results are scoped to what your role already permits, so you see the same data you would see
elsewhere in GitLab.


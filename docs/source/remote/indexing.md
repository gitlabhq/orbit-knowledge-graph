---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: What data Orbit indexes, which languages are supported for code indexing, and how indexing is scoped.
title: What Orbit indexes
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

## Scope

Orbit indexes top-level groups only. Enable Orbit on a top-level group and all its
subgroups and projects are indexed automatically. You cannot enable Orbit on a
subgroup or individual project.

## SDLC data

Orbit indexes the following GitLab objects and their relationships:

| Domain | Objects indexed |
|--------|----------------|
| Core | Groups, projects, users, notes (comments) |
| Code review | Merge requests, merge request diffs, changed files |
| CI/CD | Pipelines, stages, jobs |
| Planning | Work items (issues, epics, tasks, incidents), milestones, labels |
| Security | Vulnerabilities, security findings, security scans, scanners, CVE/CWE identifiers |

SDLC data is updated continuously via change data capture. Changes in your GitLab instance
appear in Orbit within minutes.

## Source code

Orbit indexes source code from your repositories and builds a code graph on top of it.

What gets indexed:

- Files and directories
- Function, class, and module definitions (with start/end line and full source content)
- Import and cross-file reference relationships between files

Code is indexed from the default branch only. Orbit re-indexes automatically when
the default branch changes.

### Supported languages

| Language | Definitions | Cross-file references |
|----------|-------------|----------------------|
| Ruby | Yes | Yes |
| Java | Yes | Yes |
| Kotlin | Yes | Yes |
| Python | Yes | Yes |
| TypeScript | Yes | Yes |
| JavaScript | Yes | Yes |
| Rust | Yes | Yes |
| Go | Yes | Yes |
| C# | Yes | Yes |
| C | Yes | Yes |
| C++ | Yes | Yes |
| PHP | Yes | Yes |
| Bash/Shell | Yes | No |

Languages not currently indexed: Swift, COBOL, Terraform, YAML.

## What is not indexed

- Branches other than the default branch
- Binary files
- Files in archived projects (SDLC metadata for archived projects is still indexed)
- Private content the requesting user does not have access to (authorization is enforced at query time)

## Permissions

Orbit has no separate permission model. A query runs as you and returns only the
entities you can already see in GitLab. Enabling Orbit on a group does not give anyone
access they did not already have. Access is hierarchical: a role on a top-level group
applies to every subgroup and project beneath it.

To query a group, you need the Reporter role or higher on it. This matches the access
level other GitLab Analytics features require.

Security data has a higher floor. The security domain (vulnerabilities, security findings,
security scans, scanners, and CVE/CWE identifiers) requires the Security Manager role. A
user with only the Reporter role still queries the rest of the graph, but security entities
are dropped from the results, including from aggregate counts.

| Data domain | Minimum role |
|---|---|
| Core, code review, CI/CD, planning | Reporter |
| Security | Security Manager |

These floors apply to every access method: the REST API, MCP, `glab`, and the GitLab Duo
Agent Platform, including the Security Analyst Agent. Enabling Orbit on a group is a
separate action that requires the Owner role. See [Get started](getting-started.md#prerequisites).

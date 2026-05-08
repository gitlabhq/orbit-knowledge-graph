---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: What Orbit Local indexes on your machine, which languages are supported, and the boundaries of the local code graph.
title: What Orbit Local indexes
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Experimental

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0.

{{< /history >}}

> [!disclaimer]

<!-- -->

> [!note]
> Orbit Local is experimental. Capabilities and command shape may
> change before GA.

Orbit Local builds a **code-only** graph from a local repository. It does not
connect to GitLab and does not index SDLC data.

## Scope

Orbit Local indexes the working tree of any local repository you point it at.
There is no group, project, or branch concept - the index is scoped to the
directory passed to `orbit index`.

You can index multiple repositories into the same DuckDB file. Each is tracked
separately by its absolute path.

## Source code

Orbit Local indexes:

- Files and directories (respecting `.gitignore`)
- Function, class, method, and module definitions, including start/end line
  numbers and full source content
- Import declarations and cross-file symbol references

Indexing runs on whatever is currently on disk. There is no concept of a
default branch - whatever you have checked out is what gets indexed.

### Supported languages

Orbit Local supports the same 11 languages as Orbit Remote, with full
cross-file reference resolution.

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

Languages not currently indexed: Swift, COBOL, Terraform, YAML.

## What is not indexed

Orbit Local has no GitLab connection, so none of the following are available:

- Groups, projects, or users
- Merge requests, comments, or reviewers
- Pipelines, jobs, or stages
- Work items, milestones, or labels
- Vulnerabilities or security findings

For SDLC-aware queries, use [Orbit Remote](../remote/indexing.md).

Also not indexed by Orbit Local:

- Binary files
- Files matched by `.gitignore`
- Branches other than what is checked out at index time

## Authorization

Orbit Local has no authorization layer. All data in the graph is accessible
to whoever runs the CLI. The graph file at `~/.orbit/graph.duckdb` is
protected by your operating system's file permissions.

## Billing

Orbit Local does not consume GitLab Credits. All processing is local.

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
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).

{{< /history >}}

> [!note]
> Orbit Local is experimental. Capabilities and command shape may
> change before GA.

Orbit Local builds a code-only graph from a local repository. It does not
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

Orbit Local supports the same 11 languages as Orbit Remote, with the same
definition extraction and cross-file reference resolution.

| Language | File extensions | Definitions | Cross-file references | Framework / ecosystem support |
|----------|----------------|-------------|----------------------|-------------------------------|
| Ruby | `.rb`, `.rbw`, `.rake`, `.gemspec` | Yes | Yes | Rails, Forwardable |
| Java | `.java` | Yes | Yes | Annotations, records |
| Kotlin | `.kt`, `.kts` | Yes | Yes | Extension functions, operator desugaring |
| Python | `.py` | Yes | Yes | Relative imports, decorators |
| TypeScript | `.ts`, `.tsx`, `.mts`, `.cts` | Yes | Yes | Path aliases (`tsconfig.json`) |
| JavaScript | `.js`, `.jsx`, `.mjs`, `.cjs`, `.vue`, `.graphql`, `.gql`, `.json` | Yes | Yes | Vue SFC, React/JSX, CommonJS, ESM |
| Rust | `.rs` | Yes | Yes | Cargo workspaces, macro expansion |
| Go | `.go` | Yes | Yes | Struct embedding, composite literals |
| C# | `.cs` | Yes | Yes | Records, `using static`, attributes |
| C | `.c`, `.h` | Yes | Yes | Include graph |
| C++ | `.cpp`, `.cc`, `.cxx`, `.hpp`, `.hh`, `.hxx` | Yes | Yes | Namespaces, include graph |

Languages not currently indexed: Swift, COBOL, Terraform, YAML.

For per-language details on what constructs are extracted and how cross-file references
are resolved, see the [language details](../remote/indexing.md#language-details) section
in the Orbit Remote documentation. The code indexer is identical between Local and Remote.

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

---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: How Orbit Local builds and queries a code graph on your machine using the orbit CLI and DuckDB.
title: How Orbit Local works
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).

{{< /history >}}

> [!disclaimer]

<!-- -->

> [!note]
> Orbit Local is experimental. Capabilities and command shape may
> change before GA.

## Indexing pipeline

When you run `orbit index`, Orbit Local:

1. Walks the repository directory tree, respecting `.gitignore`.
1. Passes each source file to a language-specific parser (rust-analyzer, tree-sitter, or a custom parser depending on language).
1. Extracts definitions (functions, classes, modules), import declarations, and cross-file symbol references.
1. Writes the results as nodes and edges into a local DuckDB file at `~/.orbit/graph.duckdb`.

The v2 pipeline runs all language parsers in parallel. Indexing a medium-sized repository typically completes in seconds.

## The graph model

Orbit Local builds a code-only graph. It does not have access to SDLC data (merge requests, pipelines, users) because there is no GitLab connection.

Nodes in the local graph:

- **File** - a source file in the repository
- **Directory** - a directory in the repository
- **Definition** - a function, class, module, or other named symbol
- **ImportedSymbol** - a symbol imported from another file or package

Edges connect files to their definitions, files to their imports, and definitions to the symbols they reference across files.

## Query execution

When you run `orbit query`:

1. Orbit Local parses the JSON query payload.
1. The query engine compiles the same JSON DSL as Orbit Remote, but targets DuckDB SQL instead of ClickHouse SQL.
1. DuckDB executes the query against the local graph tables.
1. Results are returned as typed JSON (or formatted text by default).

There is no authorization layer. All data in the graph is accessible to whoever runs the CLI.

## Storage

The graph is stored in a single DuckDB file at `~/.orbit/graph.duckdb`. Multiple repositories share the same database. Each repository is scoped by its project ID and branch in the manifest table.

## Supported languages

All 11 languages supported by Orbit Remote are also supported locally:
Ruby, Java, Kotlin, Python, TypeScript, JavaScript, Rust, Go, C#, C, C++.

See [What Orbit indexes](../remote/indexing.md#supported-languages) for the full language support table.

## Billing

Orbit Local does not consume GitLab Credits. All processing is local.

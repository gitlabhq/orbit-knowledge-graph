---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: How Orbit Local builds and queries a code graph on your machine using the Orbit CLI and DuckDB.
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

Orbit Local exposes the graph as a DuckDB database. Run any read-only SQL
against it with `orbit sql`:

1. `orbit sql` opens `~/.orbit/graph.duckdb` read-only.
1. Your SQL runs directly against the graph tables — no DSL compilation,
   no authorization layer.
1. Results stream back as a table, JSON, NDJSON, or CSV.

All data in the graph is accessible to whoever runs the CLI.

## Storage

The graph is stored in a single DuckDB file at `~/.orbit/graph.duckdb`. Multiple repositories share the same database. Each repository is scoped by its project ID and branch in the manifest table.

## Supported languages

All 12 languages supported by Orbit Remote are also supported locally:
Ruby, Java, Kotlin, Python, TypeScript, JavaScript, Rust, Go, C#, C, C++, PHP.

See [What Orbit indexes](../remote/indexing.md#supported-languages) for the full language support table.

## Billing

Orbit Local does not consume GitLab Credits. All processing is local.

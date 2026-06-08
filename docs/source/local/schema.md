---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Reference for the four node types in the Orbit Local code graph and how they connect.
title: Schema reference
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

> [!note]
> Orbit Local is experimental. Capabilities and command shape may
> change before GA.

Orbit Local indexes 4 node types - all in the source code domain. There is no
SDLC layer, because Orbit Local does not connect to GitLab.

To inspect the live DuckDB schema at any time:

```shell
orbit schema
```

## Source code

| Node type | Description | Key properties |
|-----------|-------------|----------------|
| `Directory` | Directory in the indexed repository | `id`, `path`, `name` |
| `File` | Source code file | `id`, `path`, `name`, `extension`, `language`, `content` |
| `Definition` | Function, class, method, or module definition | `id`, `file_path`, `fqn`, `name`, `definition_type`, `start_line`, `end_line`, `content` |
| `ImportedSymbol` | Import or cross-file symbol reference | `id`, `file_path`, `import_type`, `import_path`, `identifier_name` |

## Relationships

Edges in the local graph connect:

- Directories to the files and subdirectories they contain
- Files to the definitions they declare
- Files to the symbols they import
- Imported symbols to the definitions they resolve to in other files

## Differences from Orbit Remote

[Orbit Remote](../remote/schema.md) indexes 24 node types across 6 domains. Orbit Local
covers only the source code domain. Anything that requires GitLab data -
merge requests, pipelines, users, vulnerabilities, work items - is unavailable.

## Notes

- Definition IDs are content-hashed integers scoped per file path. The same
function in two indexed repositories will have different IDs.
- `content` fields on `Definition` and `File` nodes contain the full source
text. These are populated so agent tools can hydrate code without separate
file reads.
- There is no authorization layer. Orbit Local does not enforce per-user access
control. The graph file at `~/.orbit/graph.duckdb` is protected only by file
system permissions.

---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use the GitLab Orbit Local schema reference to learn about supported node types in your local code graph and how they connect.
title: GitLab Orbit Local schema reference
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

The GitLab Orbit Local schema reference defines
the available nodes, edges, and relationships in
your local graph.

## Source code nodes

The following sections define the available
nodes and their properties.

### `Directory`

A directory in an indexed repository.

| Properties | Definition |
|------------|------------|
| `id` | Unique identifier for the directory |
| `project_id` | Identifier of the indexed repository, derived from its absolute path. Not a GitLab project ID. |
| `branch` | Branch the directory was indexed from |
| `commit_sha` | Commit the directory was indexed at |
| `path` | Path relative to the repository root |
| `name` | Directory name |

### `File`

A source code file in an indexed repository.

| Properties | Definition |
|------------|------------|
| `id` | Unique identifier for the file |
| `project_id` | Identifier of the indexed repository, derived from its absolute path. Not a GitLab project ID |
| `branch` | Branch the file was indexed from |
| `commit_sha` | Commit the file was indexed at |
| `path` | Path relative to the repository root |
| `name` | Filename, including the extension |
| `extension` | File extension, without the leading dot |
| `language` | Detected programming language, or `unknown` when no language matched |
| `size_bytes` | File size in bytes, measured when the file was indexed |
| `reason` | Why the file was skipped or failed to parse, such as `skip_excluded_extension` or `fault_invalid_utf8`. Empty for files parsed successfully. |

### `Definition`

A function, class, method, module, or other named symbol declared in a file.

| Properties | Definition |
|------------|------------|
| `id` | Unique identifier for the definition, generated per file path. The same function in two indexed repositories has different IDs. |
| `project_id` | Identifier of the indexed repository, derived from its absolute path. Not a GitLab project ID. |
| `branch` | Branch the definition was indexed from |
| `commit_sha` | Commit the definition was indexed at |
| `file_path` | Path of the file that declares the definition |
| `fqn` | Fully qualified name of a function, class, module, method, struct, enum, or trait |
| `name` | Short name, without namespace qualification |
| `definition_type` | Kind of definition, such as `Class`, `Function`, `Method`, `Struct`, or `Section`. Values are capitalized and language-specific. |
| `start_line` | First line of the definition |
| `end_line` | Last line of the definition |
| `start_byte` | Byte offset where the definition starts |
| `end_byte` | Byte offset where the definition ends |
| `start_char` | Column on `start_line` where the definition starts |
| `end_char` | Column on `end_line` where the definition ends |
| `search_text` | Lowercase identifier tokens from `fqn` and `file_path`, used for ranked code search |
| `token_count` | Number of tokens in `search_text` |

### `ImportedSymbol`

An import statement or cross-file symbol reference in a file.

| Properties | Definition |
|------------|------------|
| `id` | Unique identifier for the imported symbol |
| `project_id` | Identifier of the indexed repository, derived from its absolute path. Not a GitLab project ID. |
| `branch` | Branch the symbol was indexed from |
| `commit_sha` | Commit the symbol was indexed at |
| `file_path` | Path of the file that contains the import |
| `import_type` | Kind of import, such as `Use`, `NamedImport`, or `CjsRequire`. Values are capitalized and language-specific. |
| `import_path` | Module or path the symbol is imported from |
| `identifier_name` | Name of the imported symbol. Empty for wildcard and side-effect imports. |
| `identifier_alias` | Local name the import binds the symbol to. Empty when the analyzer records none. Some languages populate it even when the import is not renamed. |
| `start_line` | First line of the import |
| `end_line` | Last line of the import |
| `start_byte` | Byte offset where the import starts |
| `end_byte` | Byte offset where the import ends |
| `start_char` | Column on `start_line` where the import starts |
| `end_char` | Column on `end_line` where the import ends |

## Relationships

Edges in the local graph have one
of the following relationship types:

- `CONTAINS`
- `DEFINES`
- `IMPORTS`
- `CALLS`
- `EXTENDS`

The same relationship type can connect different
node types.

| Relationship type | Source node | Target node | Description |
|--------------|--------|--------|-------------|
| `CONTAINS` | `Directory` | `Directory` | Directory contains a subdirectory |
| `CONTAINS` | `Directory` | `File` | Directory contains a file |
| `DEFINES` | `File` | `Definition` | File defines a code definition, such as a class or function |
| `DEFINES` | `Definition` | `Definition` | Outer definition lexically contains a nested definition, such as a class and its methods |
| `IMPORTS` | `File` | `ImportedSymbol` | File contains an import statement |
| `IMPORTS` | `ImportedSymbol` | `Definition` | Imported symbol resolves to a concrete definition in another file |
| `CALLS` | `Definition` | `Definition` | Definition invokes another definition |
| `CALLS` | `Definition` | `ImportedSymbol` | Definition invokes an imported symbol that is not resolved to a definition |
| `CALLS` | `File` | `Definition` or `ImportedSymbol` | Top-level call site outside any enclosing definition |
| `EXTENDS` <sup>1</sup> | `Definition` | `Definition` | Definition declares a supertype of a function, class, method, module, or other named symbol declared in a file |

**Footnotes**:

1. `EXTENDS` points from the child to the parent. A class that extends a base
class is the source node, and the base class is the target node.

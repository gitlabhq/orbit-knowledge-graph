# Orbit repo map reference

`orbit repo-map` builds a fast, hierarchical picture of a locally checked-out
repository from the Orbit Local DuckDB property graph. Use it before planning a
large code change, when first opening an unfamiliar repository, or when a
directory-level map is more useful than reading files one by one.

It is a native subcommand of the `orbit` binary — no Python runtime and no
sidecar script. It summarizes languages, top-level structure, key abstractions,
definitions, per-file APIs, inheritance edges, and imports using Orbit Local's
indexed `gl_file`, `gl_definition`, `gl_imported_symbol`, and `gl_edge` tables.

## When to use it

Use the repo map when you need to:

- Orient yourself in an unfamiliar repository without opening dozens of files.
- Decide where a new file, class, module, or crate should live.
- Plan a refactor and identify important base classes, traits, interfaces, or
  other abstractions before editing.
- Compare the API shape of existing siblings before adding a new implementation.
- Audit a whole layer, such as services, policies, workers, packages, or crates.

Skip it when:

- You already know the exact file to inspect; read the file directly.
- The question is a targeted graph lookup such as "who calls X"; use a focused
  Orbit Local SQL query instead.
- The repository is not indexed and a simple file read or structural search is
  enough.

## Prerequisites

The target repository must be indexed by Orbit Local at the current commit.
`repo-map` preflights this and prints the indexing command if the commit is
missing:

```bash
orbit index .
```

Orbit Local stores the graph in the local DuckDB database (default
`~/.orbit/graph.duckdb`, override with `--db`). See [`cli.md`](cli.md) for
installation, configuration, and pass-through argument details.

## Invocation

The map is scoped to the current commit of the repository. By default it uses
the current directory; pass `--repo PATH` to point at another checkout:

```bash
orbit repo-map overview
orbit repo-map --repo ~/workspace/knowledge-graph overview
orbit repo-map --repo ~/workspace/knowledge-graph tree crates
orbit repo-map --repo ~/workspace/knowledge-graph api crates/orbit-local
orbit repo-map --repo ~/workspace/knowledge-graph class Workspace
orbit repo-map --repo ~/workspace/knowledge-graph extends QueryCompiler
orbit repo-map --repo ~/workspace/knowledge-graph imports Workspace
```

Reached through glab, prefix with `glab orbit local --yes`:

```bash
glab orbit local --yes repo-map overview
```

To focus on one or more file extensions, pass `--ext`. Extensions may include
or omit the leading dot and can be repeated or comma-separated:

```bash
orbit repo-map --ext .rs overview
orbit repo-map --ext rs api crates/orbit-local
orbit repo-map --ext rs,toml tree crates
```

## Recommended workflow

Start broad, then drill down once or twice. More than four runs for one planning
task usually means the investigation has become enumeration instead of design.

| Phase | Call | What it tells you |
|---|---|---|
| 1. Orient | `orbit repo-map overview` | Languages, top directories, definition totals, key abstractions, most-imported defined symbols, and most-called callables |
| 2. Locate | `orbit repo-map tree PATH_PREFIX` | Types grouped by file under a subtree, without method-level noise |
| 3. Drill in | `orbit repo-map api PATH_PREFIX` | Types, callables, and extracted signature lines under a subtree |
| 4. Focus | `orbit repo-map class NAME` | One class/module/trait and its members, including same-named overrides |
| 5. Check inheritance | `orbit repo-map extends NAME` | Descendants of a base type through `EXTENDS` edges, up to depth 6 |
| 6. Check imports | `orbit repo-map imports PATTERN` | Files importing matching symbols or paths |

## Subcommands

### `overview` (default)

Always run this first for a new repository or planning session. Omitting the
subcommand runs `overview` automatically. It emits:

- Language breakdown over non-test source files.
- Definition totals by `definition_type`.
- Top-level structure with file, type, and callable counts.
- Key abstractions with the most descendants through `EXTENDS`.
- Most-imported project-defined symbols.
- Most-called callables using `CALLS` edge counts.

Use `--ext` with `overview` when the user asks for a language-specific map, for
example "only Rust files".

### `tree [PATH_PREFIX]`

Lists type-like definitions grouped by file: classes, structs, enums, traits,
interfaces, modules, namespaces, records, and similar language constructs.
This omits signatures and members, so it is useful for a quick "what lives
under this directory?" pass.

Pass a prefix for real use. Without a prefix the output is capped but usually
too broad for a large repository.

### `api PATH_PREFIX`

Prints the richest directory-level view. For every type or callable under the
prefix, it reads the source range recorded by Orbit Local and extracts the first
structural signature line, such as `fn ...`, `class X < Y`, or `def foo`.

Use this before adding a new sibling implementation so the new code follows the
existing naming, inheritance, and method-shape conventions. Avoid running this
on the repository root or a very broad directory; choose a feature directory,
package, or crate.

### `class FQN_OR_NAME`

Finds definitions matching a fully qualified name or short name, then lists
their members and extracted signatures. Same-named definitions in different
namespaces or editions show up together, which is useful for finding override
surfaces.

### `extends NAME`

Walks down the `EXTENDS` relationship from a base class, trait, interface, or
struct up to depth 6. Use it to estimate the blast radius of a base abstraction
change.

### `imports PATTERN`

Searches imported symbol names and import paths with `LIKE %PATTERN%`, returning
matching symbols, paths, and distinct importer counts. This is best for
language ecosystems where Orbit Local indexes named imports.

## Output format and caveats

Output is plain text tables with `path/to/file:line` locators that can be passed
directly to file-reading tools.

Signatures are extracted by reading source files and applying a language-neutral
regular expression to a small window starting at the indexed `start_line`. If a
signature cannot be extracted, the bare definition name is printed.

The repo map is a planning aid over Orbit Local's Code Graph coverage.

## Budget and anti-patterns

- Run one `overview` per session unless the repository or branch changes.
- Use two to four drill-down calls per planning task.
- Do not run `api` on the repository root or a broad top-level directory in a
  large monorepo.
- Do not use the repo map for targeted call graph questions; run focused Orbit
  Local SQL against `CALLS` instead.
- Do not grep for definitions immediately after a repo map call; use the
  returned file and line locators first.

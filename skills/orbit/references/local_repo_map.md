# Orbit repo map reference

`glab orbit repo-map` builds a hierarchical picture of a locally checked-out repository from the Orbit Local DuckDB graph. Use it before you plan a large change or when you first open an unfamiliar repository. It beats reading files one by one when you need a directory-level map. It is a native subcommand of the managed binary, with no Python runtime or sidecar script.

Skip it when you already know the exact file to read. For a targeted graph lookup such as "who calls X", use `glab orbit grep` instead. If the repository is not indexed, a file read is often enough.

## Prerequisites

The repository must be indexed by Orbit Local at the current commit. `repo-map` checks this first and prints the index command if the commit is missing:

```bash
glab orbit index .
```

## Invocation

The map is scoped to the current commit. It uses the current directory by default; pass `--repo PATH` for another checkout. Pass `--ext` to limit output to some file extensions. The leading dot is optional, and the flag repeats or takes a comma-separated list.

```bash
glab orbit --yes repo-map overview
glab orbit --yes repo-map --repo ~/workspace/knowledge-graph tree crates
glab orbit --yes repo-map --repo ~/workspace/knowledge-graph api crates/orbit-cli
glab orbit --yes repo-map --ext rs,toml tree crates
```

With the standalone binary the prefix is just `orbit`.

## Recommended workflow

Start broad, then drill down once or twice. More than four runs for one planning task usually means the investigation has become enumeration instead of design.

| Phase | Call | What it tells you |
|---|---|---|
| 1. Orient | `repo-map overview` | Languages over non-test files, definition totals, top-level structure, key abstractions by `EXTENDS` descendants, most-imported symbols, and most-called callables. Run once per session. |
| 2. Locate | `repo-map tree PATH_PREFIX` | Type-like definitions grouped by file under a subtree, without signatures or members. Pass a prefix; the unscoped form is capped and too broad for large repositories. |
| 3. Drill in | `repo-map api PATH_PREFIX` | Types, callables, and the first structural signature line of each, such as `fn ...` or `class X < Y`. Run it on a feature directory, package, or crate, never on the root. |
| 4. Focus | `repo-map class NAME` | One class, module, or trait with its members and signatures. Same-named definitions in other namespaces appear together, which exposes override surfaces. |
| 5. Check inheritance | `repo-map extends NAME` | Descendants of a base type through `EXTENDS` edges, up to depth 6. Use it to estimate the blast radius of a base change. |
| 6. Check imports | `repo-map imports PATTERN` | Files that import symbols or paths matching `%PATTERN%`, with distinct importer counts. Best where Orbit Local indexes named imports. |

Use `--ext` with `overview` when the user asks for a language-specific map. Use `api` before you add a sibling implementation, so the new code follows the existing naming and method shape.

## Output format and caveats

Output is plain text tables with `path/to/file:line` locators that file-reading tools accept directly.

Signatures come from a language-neutral regular expression applied to a small window at the indexed `start_line`. If no signature matches, the bare definition name is printed. The repo map is a planning aid over Orbit Local's Code Graph coverage.

## Budget and anti-patterns

- Run one `overview` per session unless the repository or branch changes.
- Use two to four drill-down calls per planning task.
- Do not run `api` on the repository root or a broad top-level directory in a large monorepo.
- Do not use the repo map for targeted call graph questions. Use `glab orbit grep "<fqn>" --callers` instead.
- Do not grep for definitions right after a repo map call. Use the returned file and line locators first.

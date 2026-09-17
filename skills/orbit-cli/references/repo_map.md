# Orbit repo map reference

`orbit repo-map` builds a hierarchical picture of a locally checked-out
repository from the Orbit Local DuckDB graph. It beats reading files one by
one when you need a directory-level map.

## When to use it

Use the repo map to:

- Orient yourself in an unfamiliar repository without opening dozens of files.
- Decide where a new file, class, module, or crate should live.
- Find the base classes, traits, or interfaces that a refactor will touch.
- Compare the API shape of existing siblings before adding a new implementation.
- Audit a whole layer, such as services, policies, workers, packages, or crates.

Skip it when:

- You already know the exact file to inspect. Read the file directly.
- The question is a targeted graph lookup such as "who calls X". Use
  `orbit grep` and `orbit context Definition:<id>` instead.
- The repository is not indexed and a simple file read is enough.

## Prerequisites

The target repository must be indexed at the current commit:

```shell
orbit index .
```

## Invocation

```shell
orbit repo-map overview
orbit repo-map --repo ~/workspace/knowledge-graph tree crates
orbit repo-map --ext rs,toml api crates/orbit-cli
glab orbit --yes repo-map overview       # through glab
```

## Recommended workflow

Start broad, then drill down once or twice. More than four runs for one planning
task usually means the investigation has become enumeration instead of design.

| Phase | Call | What it tells you |
|---|---|---|
| 1. Orient | `orbit repo-map overview` | Languages, top directories, definition totals, key abstractions by `EXTENDS` descendants, most-imported symbols, most-called callables. Run it first, once per session. |
| 2. Locate | `orbit repo-map tree PATH_PREFIX` | Type-like definitions grouped by file, without members or signatures. Pass a prefix; the root is capped and too broad. |
| 3. Drill in | `orbit repo-map api PATH_PREFIX` | Types, callables, and the first structural signature line of each. Use it on a feature directory, package, or crate before adding a sibling. Never on the root. |
| 4. Focus | `orbit repo-map class NAME` | Members and signatures of one class, module, or trait. Same-named definitions in other namespaces list together, which exposes override surfaces. |
| 5. Check inheritance | `orbit repo-map extends NAME` | Descendants of a base type through `EXTENDS` edges, up to depth 6. Use it to size the blast radius of a base change. |
| 6. Check imports | `orbit repo-map imports PATTERN` | Importers of symbols or paths matching `LIKE %PATTERN%`, with distinct importer counts. Empty output can also mean the language has no indexed imports. |

## Output

Output is plain text tables with `path/to/file:line` locators. Signatures come
from a language-neutral pattern applied to a small window at the indexed
`start_line`. When no signature matches, the bare definition name is printed.

## Budget and anti-patterns

- Do not run `api` on the repository root or a broad top-level directory.
- Do not grep for definitions right after a repo map call. Use the returned
  file and line locators first.

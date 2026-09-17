# Orbit repo map reference

`orbit repo-map` maps an indexed repository from the local DuckDB graph. Use it to orient in unfamiliar code, place a new file, or size a refactor. Skip it when you know the exact file, or the question is a targeted lookup. For that, use `orbit grep` and `orbit context`.

## Workflow

Start broad, then drill down once or twice. More than four runs per planning task means you are enumerating, not designing.

| Phase | Call | Tells you |
|---|---|---|
| 1. Orient | `orbit repo-map overview` | Languages, top directories, key types, most-imported and most-called symbols. Run once per session. |
| 2. Locate | `orbit repo-map tree PREFIX` | Type-like definitions grouped by file. Pass a prefix. Root output is truncated. |
| 3. Drill in | `orbit repo-map api PREFIX` | Types, callables, and one signature line each. Never the root. |
| 4. Focus | `orbit repo-map class NAME` | Members and signatures of one class, module, or trait. Same-named definitions list together. |
| 5. Inheritance | `orbit repo-map extends NAME` | Descendants of a base type via `EXTENDS`, depth 6. Sizes a base change. |
| 6. Imports | `orbit repo-map imports PATTERN` | Importers matching `LIKE %PATTERN%`. Empty can mean no indexed imports. |

## Output and limits

Output is plain-text tables with `path/to/file:line` locators. Signatures come from a small window at the definition. Without a match, orbit prints the bare name.

Do not grep for a definition right after a repo map. Use the returned locators.

## Orbit knowledge graph

Repositories on this machine are indexed into a local Orbit code graph (`orbit list` shows which). For code-structure questions — where something is defined or belongs, who calls or imports it, what an API looks like — query the graph before reaching for grep: grep matches text and returns noise; the graph returns resolved definitions, callers, and imports.

Copy-paste recipes (DuckDB SQL):

- Find a definition: `orbit sql "SELECT name, definition_type, file_path, start_line FROM gl_definition WHERE name LIKE '%Retry%'"`
- Who calls X: `orbit sql "SELECT s.name, s.file_path, s.start_line FROM gl_edge e JOIN gl_definition s ON e.source_id=s.id JOIN gl_definition t ON e.target_id=t.id WHERE e.relationship_kind='CALLS' AND t.name='run_sql'"` (flip source/target for what X calls; `EXTENDS` for subtypes)
- Orient in unfamiliar code / where should new code live: `orbit repo-map` (drill down: `tree`, `api <Type>`, `class`, `extends`, `imports`)
- `orbit schema` lists tables; `orbit skill` prints the full recipe reference

If orbit reports the current commit is not indexed, run `orbit index .` and retry. Grep and raw file reads are fine once the graph has oriented you, or to modify and debug specific lines. For questions spanning other projects or GitLab entities (merge requests, issues, pipelines, users), use `glab orbit` when available.

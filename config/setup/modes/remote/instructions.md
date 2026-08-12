## Orbit knowledge graph

GitLab projects are covered by the remote Orbit graph. For code-structure questions — where something is defined, who calls or imports it, what an API looks like — and GitLab-entity questions (merge requests, issues, pipelines, users, cross-project dependencies), query the graph before reaching for grep: grep matches text and returns noise; the graph returns resolved definitions and relationships.

- Run queries with `glab orbit remote query <file.json|->`; `glab orbit remote schema` lists entities and edges.
- The `orbit` agent skill has copy-paste query recipes and the full DSL reference — consult it before writing a query by hand.
- Raw grep and file reads are fine once the graph has oriented you, or to modify and debug specific lines.

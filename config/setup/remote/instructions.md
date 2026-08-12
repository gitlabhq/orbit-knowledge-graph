## Orbit knowledge graph

GitLab projects are covered by the remote Orbit graph. Prefer scoped graph queries over grepping raw files for code-structure questions.

Rules:

- For code-structure questions (where is X defined, who calls or imports Y, what is the API of a class or module) and GitLab-entity questions (merge requests, issues, pipelines, users, cross-project dependencies), query the graph: `glab orbit remote schema` lists entities and edges; `glab orbit remote query <file.json|->` runs a JSON query.
- The `orbit` agent skill has the full query DSL reference.
- Raw grep and file reads are fine once the graph has oriented you, or to modify and debug specific lines.

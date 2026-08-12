## Orbit knowledge graph

Repositories on this machine are indexed into a local Orbit code graph (`orbit list` shows which). Prefer scoped graph queries over grepping raw files for code-structure questions.

Rules:

- For code-structure questions (where is X defined, who calls or imports Y, what is the API of a class or module), first run `orbit sql "<DuckDB SQL>"`. `orbit schema` lists the tables; `orbit skill` prints the full query reference.
- To orient in an unfamiliar part of the repository, run `orbit repo-map` (drill down with `tree`, `api`, `class`, `extends`, `imports`) instead of reading files one by one.
- If orbit reports the current commit is not indexed, run `orbit index .` and retry.
- Raw grep and file reads are fine once the graph has oriented you, or to modify and debug specific lines.
- For questions spanning other projects or GitLab entities (merge requests, issues, pipelines, users), use `glab orbit` when available.

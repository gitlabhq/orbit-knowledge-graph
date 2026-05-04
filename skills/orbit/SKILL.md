---
name: orbit
description: Query the GitLab Knowledge Graph (Orbit) via the /api/v4/orbit REST endpoints using `glab api`. Use for code-structure questions (who calls this function, where is this symbol defined), cross-project dependency and blast-radius analysis, merge-request and contributor queries, and any question answerable by traversing GitLab's unified entity graph (projects, users, MRs, issues, pipelines, files, definitions, vulnerabilities).
version: 0.4.0
license: MIT
metadata:
  audience: developers
  keywords: orbit, knowledge-graph, gkg, graph, query, glab, api
  workflow: ai
---

# Orbit (GitLab Knowledge Graph) skill

Query the GitLab Knowledge Graph (product name **Orbit**) from the CLI using `glab api`.
The API is **self-describing** — always call `orbit/schema` or `orbit/tools` first to discover the
authoritative ontology and query DSL before writing queries.

## Prerequisites

- `glab auth login` against an instance that has the `knowledge_graph` feature flag enabled
  for your user. FF off → every `/api/v4/orbit/*` endpoint returns `404`.
- The instance must have Orbit turned on for at least one top-level group you belong to.
  Otherwise `orbit/query` returns `403 No Knowledge Graph enabled namespaces available`.

## Endpoints

All endpoints live under `/api/v4/orbit/*` and are **user-scoped**, not project-scoped.
Do **not** pass `-R owner/repo`.

| Endpoint                                | Method | Purpose                                                                       |
|-----------------------------------------|--------|-------------------------------------------------------------------------------|
| `orbit/status`                          | GET    | Cluster health (always returns 200).                                          |
| `orbit/schema`                          | GET    | Graph ontology: domains, nodes, edges.                                        |
| `orbit/schema?expand=A,B`               | GET    | Drill into nodes for properties and relationships.                            |
| `orbit/schema?include_response_format=1`| GET    | Schema plus the query response JSON Schema (the formatter output shape).      |
| `orbit/query_dsl`                       | GET    | Query DSL grammar. `?format=raw` returns the full JSON Schema; default is condensed TOON. |
| `orbit/tools`                           | GET    | MCP tool manifest. Tool descriptions are kept short — call `orbit/query_dsl` for the grammar. |
| `orbit/query`                           | POST   | Execute a query. **Requires `Content-Type` header.**                          |

## Discovery workflow (always start here)

```bash
glab api orbit/status                                   # is the service up?
glab api orbit/schema                                   # what entities and edges exist?
glab api "orbit/schema?expand=MergeRequest,Project"     # properties of specific nodes
glab api orbit/query_dsl                                # query DSL grammar (condensed TOON)
glab api orbit/tools                                    # MCP tool manifest
```

These calls are cheap and return the authoritative ontology + query-DSL schema. Prefer them over
memorised structures — the ontology evolves. Budget ≤ 1 discovery call per new entity type in a session.

## Running a query

POST to `orbit/query` requires an explicit `Content-Type`. Without it you get
`HTTP 415: The provided content-type '' is not supported.`

```bash
cat > /tmp/q.json <<'JSON'
{
  "query": {
    "query_type": "traversal",
    "node": {"id": "p", "entity": "Project"},
    "limit": 5
  },
  "response_format": "llm"
}
JSON

glab api --method POST orbit/query \
  --header "Content-Type: application/json" \
  --input /tmp/q.json
```

Or use the bundled wrapper, which injects the header automatically.
Invoke it by its absolute path (or put the skill's `scripts/` dir on `PATH`) —
the skill can be installed anywhere, so relative `scripts/orbit-query` only
works from inside the skill directory:

```bash
# Adjust path to wherever the skill is installed:
~/.config/opencode/skills/orbit/scripts/orbit-query /tmp/q.json
# or via stdin:
cat /tmp/q.json | ~/.config/opencode/skills/orbit/scripts/orbit-query
```

`response_format`:

- `"llm"` — compact text optimised for LLM consumption (recommended for agent use).
- `"raw"` — structured JSON suitable for `| jq`.

## Where to find more

These files are part of the skill itself and are always available alongside `SKILL.md`:

| Topic | Location |
|---|---|
| Full query DSL reference | [`references/query_language.md`](references/query_language.md) |
| Paste-ready `glab api` recipes per query type | [`references/recipes.md`](references/recipes.md) |
| Common errors and fixes | [`references/troubleshooting.md`](references/troubleshooting.md) |
| Query-body wrapper script | [`scripts/orbit-query`](scripts/orbit-query) |

External links (require internet):

- [MCP tool definitions](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/docs/source/queries/mcp_tools.md)
- [Orbit product overview](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/docs/source/_index.md)
- [Real query examples (`sdlc_queries.yaml`)](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/fixtures/queries/sdlc_queries.yaml)
- [Code-graph traversal examples (`code_graph_queries.yaml`)](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/fixtures/queries/code_graph_queries.yaml) — `CALLS` and `EXTENDS` traversal patterns

## Agent guidelines

1. **Always discover before querying.** Call `orbit/schema` and/or `orbit/tools` first. Do not guess
   node names, edge types, or property names — validate against the live ontology.
2. **Use `"response_format": "llm"`** for compact agent-friendly output unless piping to `jq`.
3. **Set `Content-Type: application/json` on POST.** Missing → 415.
4. **No `-R owner/repo`.** Orbit endpoints are user-scoped at the API level.
5. **Keep `limit` small while iterating** (5–10). Queries can fan out across many authorised namespaces.
6. **`query_type` dictates the top-level key:** `neighbors` and single-node `traversal` → `node` (singular);
   multi-node `traversal` / `aggregation` / `path_finding` → `nodes` (array).
7. **Pagination uses `cursor: {offset, page_size}`**, not `page`/`per_page`.
   `offset + page_size` must not exceed `limit`. `page_size` max 100.
8. **`max_depth` and `max_hops` ceiling is 3.** Enforced server-side.
9. **`path_finding` with `filters` or `id_range` endpoints requires `rel_types`.**
   When an endpoint uses `filters` or `id_range`, specify `rel_types` in the `path` config to
   constrain which relationship types the frontier traverses. When both endpoints use `node_ids`,
   `rel_types` is optional.
10. **Read-only.** All endpoints are idempotent queries — no data is modified.
11. **Stay sequential.** Run queries one at a time — `orbit/query` is rate-limited
    (see `HTTP 429` in troubleshooting). Prefer aggregation/traversal in one
    query over N separate queries.

## Contributing improvements

If any guidance here is **inaccurate or outdated** (a flag name, an endpoint path, a DSL field),
confirm with the user and open an MR to `gitlab-org/orbit/knowledge-graph` with a fix and a
`version` bump in the frontmatter. Keep changes focused — one fix per MR.

**`references/query_language.md` is synced from `docs/source/queries/query_language.md`.**
Edit the upstream file, then run `mise run skill:sync:orbit` to propagate. A Lefthook pre-commit
job (`orbit-skill-docs-sync`) will fail the commit if the two files drift.

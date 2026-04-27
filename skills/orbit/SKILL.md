---
name: orbit
description: Query the GitLab Knowledge Graph (Orbit) via the /api/v4/orbit REST endpoints using `glab api`. Use for code-structure questions (who calls this function, where is this symbol defined), cross-project dependency and blast-radius analysis, merge-request and contributor queries, and any question answerable by traversing GitLab's unified entity graph (projects, users, MRs, issues, pipelines, files, definitions, vulnerabilities).
version: 0.3.0
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

| Endpoint                    | Method | Purpose                                                  |
|-----------------------------|--------|----------------------------------------------------------|
| `orbit/status`              | GET    | Cluster health (always returns 200).                     |
| `orbit/schema`              | GET    | Graph ontology: domains, nodes, edges.                   |
| `orbit/schema?expand=A,B`   | GET    | Drill into nodes for properties and relationships.       |
| `orbit/tools`               | GET    | MCP tool manifest with the full query DSL JSON Schema.   |
| `orbit/query`               | POST   | Execute a query. **Requires `Content-Type` header.**     |

## Discovery workflow (always start here)

```bash
glab api orbit/status                                   # is the service up?
glab api orbit/schema                                   # what entities and edges exist?
glab api "orbit/schema?expand=MergeRequest,Project"     # properties of specific nodes
glab api orbit/tools                                    # full DSL JSON Schema
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
9. **Read-only.** All endpoints are idempotent queries — no data is modified.
10. **Stay sequential.** Run queries one at a time — `orbit/query` is rate-limited
    (see `HTTP 429` in troubleshooting). Prefer aggregation/traversal in one
    query over N separate queries.

## Reading aggregation results

`aggregation` rows fold per-row hydration values together with the aggregate
columns onto the same group node. A row like

```json
{"username": "alice", "mrs": 175, "avg_added": 2737, "total_added": 21894}
```

reads naturally as a roll-up across 175 MRs, but only the `count` / `avg` /
`sum` columns are computed across the group. Per-row columns from the
aggregation `target` (e.g. `mr.added_lines`) on the same row are one
representative MR's value, not a total. Verify totals with a separate
aggregation query, or drop hydration columns from the `target` node when you
only need rollups.

`count` rows for groups whose `target` was fully redacted out by per-entity
authorization still surface (group survives, the count reflects what the
caller can see). Aggregation columns hydrated from those redacted target
rows come back as null — treat null aggregates as "the redacted bucket"
rather than "no data."

## Definition IDs are content-hashed and branch-scoped

`Definition.id` is a hash of the definition's content + branch, so a single
symbol name like `compile` resolves to N IDs — one per indexed branch. IDs
are large signed i64 strings (e.g. `"-3105496773625129529"`). When you want
full coverage across branches, look up the IDs first via a single-node
`traversal` and pin all of them with `node_ids` in the follow-up query;
filtering on `name` alone misses content-equal definitions on other
branches.

## Contributing improvements

If any guidance here is **inaccurate or outdated** (a flag name, an endpoint path, a DSL field),
confirm with the user and open an MR to `gitlab-org/orbit/knowledge-graph` with a fix and a
`version` bump in the frontmatter. Keep changes focused — one fix per MR.

**`references/query_language.md` is synced from `docs/source/queries/query_language.md`.**
Edit the upstream file, then run `mise run skill:sync:orbit` to propagate. A Lefthook pre-commit
job (`orbit-skill-docs-sync`) will fail the commit if the two files drift.

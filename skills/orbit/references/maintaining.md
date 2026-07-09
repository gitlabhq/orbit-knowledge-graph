# Maintaining the Orbit skill

For agents and contributors editing this skill itself, not for answering Orbit
queries. See [`SKILL.md`](../SKILL.md) for usage.

## Contributing

If Orbit guidance, recipes, or helper behavior is inaccurate, update this skill
in `gitlab-org/orbit/knowledge-graph` rather than working around it silently.
Keep `SKILL.md`, `references/`, and `scripts/` in sync, and use `opencode run`
for meaningful behavior changes.

## Syncing the query-language reference

`references/query_language.md` is synced from
`docs/source/remote/queries/query-language.md`. Edit the upstream file, then run:

```bash
mise run skill:sync:orbit
```

The lefthook `orbit-skill-docs-sync` job fails the commit if the two files
drift. Do **not** edit `references/query_language.md` directly.

## Version bumps

Bump the `version` field in `SKILL.md` frontmatter on every change under
`skills/orbit/`. The lefthook `skill-version-bump` job enforces this.

## Trigger test

Canonical prompts for validating skill-discovery routing between `orbit`, its
sibling `orbit-local` skill, and the `glab` skill. After changing the
`description` field, present all three skill descriptions to the model and ask
it to route each prompt to exactly one skill, then check the routing matches the
expectations below. This is harness-agnostic — any agent runner that exposes
skill descriptions to the model works.

The sibling skill descriptions used for routing are:

- **`glab`**: `"GitLab workflow automation using glab CLI"`. Routing currently
  relies on orbit's deferral clause ("Do not use for single-entity…"); glab's
  description carries no counter-signal.
- **`orbit-local`**: `"Index and query a LOCAL checkout of a repository offline
  with the Orbit local CLI (the orbit binary, run directly or via
  glab orbit local). It builds a DuckDB property graph from the working tree and
  you query it with read-only SQL. Use when the request targets the current
  checkout, working tree, or a branch that is not pushed/indexed remotely, or is
  explicitly offline/local: index this repo locally, who calls X in my checkout,
  list definitions in a file, generate a repo map of a local checkout, run SQL
  over the local code graph, or serve the local graph over MCP. For queries
  against already-indexed production data in GitLab (a project such as
  gitlab-org/gitlab, cross-project blast radius, contributor or merge-request
  aggregation) use the orbit skill; for single-entity GitLab lookups or write
  operations use glab."`. Routing relies on the matching deferral clauses in
  both orbit and orbit-local.

**Tie-break rule for boundary cases:** when a prompt names a **single known
entity** (one MR, one project) but phrases the question relationally (e.g.
"who reviewed MR !X?"), route to **glab** unless the question explicitly spans
**multiple entities/projects** or requires a **group-by or multi-entity
aggregation**. A simple single-entity count that `glab mr list | wc` can
answer stays with glab; a group-by breakdown (e.g. "how many MRs per state")
or a count that joins across entities needs Orbit. `glab mr view` and similar
commands surface relationship metadata (reviewers, labels, pipelines) for a
single entity without a graph query.

### Should fire orbit

1. "Who calls the `process_event` function in gitlab-org/gitlab?"
2. "What is the blast radius of changing the `users` table across all projects?"
3. "List all subclasses of ApplicationRecord in gitlab-org/gitlab"
4. "Which contributors touched the most files in gitlab-org/gitlab last quarter?"
5. "Give me a repo map of gitlab-org/gitlab"
6. "How many MRs were merged per project in the gitlab-org group last month?"
7. "Which projects depend on the gitlab-shell gem?"
8. "Which MRs touched both app/models/user.rb and app/models/project.rb?"

### Should fire orbit-local (not orbit)

1. "Serve the local graph over MCP" — local MCP serving (`orbit mcp serve`)
2. "Index this local checkout into the knowledge graph" — local indexing (`orbit index .`)
3. "Run SQL over the local code graph to find unused imports" — local SQL query (`orbit sql`)
4. "Generate a repo map of the working tree" — local repo map, targets working tree not a remote project
5. "Who calls this function in my checkout?" — code-structure query scoped to the local checkout

### Should fire glab (not orbit)

1. "Show me the diff of MR !1216" — single-entity lookup (`glab mr diff`)
2. "Create a new merge request for my branch" — write operation (`glab mr create`)
3. "What is the current pipeline status for MR !500?" — single-entity lookup (`glab ci status`)
4. "Approve MR !789" — write operation (`glab mr approve`)
5. "List open MRs in gitlab-org/gitlab" — simple list (`glab mr list`)
6. "Who are the reviewers on MR !1216?" — single known entity, relationship metadata available via `glab mr view` (tie-break: glab)
7. "What files did MR !1216 change?" — single known entity, `glab mr diff` suffices (tie-break: glab)
8. "How many open MRs are in gitlab-org/gitlab?" — single-project count, `glab mr list | wc` suffices (tie-break: glab)

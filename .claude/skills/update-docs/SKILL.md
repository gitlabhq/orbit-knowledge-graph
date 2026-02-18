---
name: update-docs
description: Audit and update documentation after code changes. Use when architecture, APIs, or behavior changed and docs may have drifted.
allowed-tools: Read, Edit, Write, Glob, Grep, Bash(mise *), Bash(npx *), Bash(git *), Bash(glab *)
---

# Update docs

Docs rot when code moves faster than prose. After a code change, check if the docs still describe reality. Fix what drifted. Skip what didn't.

If agent teams are available, create an agent team for each major section of the documents.

## Where to look

`README.md` is the SSOT. Use `/related-repositories` for dependent repos.

| Location | Contents |
|---|---|
| `README.md` | Epics, repositories, Helm charts, infra, people |
| `docs/design-documents/` | Architecture (indexing, querying, data model, security, observability) |
| `docs/dev/` | Local setup, infrastructure, runbook |
| `CLAUDE.md` / `AGENTS.md` | Agent context. Must stay identical — CI enforces this. |
| `crates/*/README.md` | Per-crate docs |
| `crates/indexer/AGENTS.md` | Indexer agent context |

## Gathering context

Before editing, research what changed and what the current state looks like:

```shell
git log --oneline --since="2 weeks ago" -- crates/query-engine/
glab issue list --label "knowledge graph" --state opened
glab mr list --state merged --per-page 20
```

Use `glab` to check epics, issues, and MRs for context on what shipped recently. Cross-reference against what the docs say.

## What drifts most

- New ontology entities missing from `data_model.md`
- Query engine changes not in `querying/graph_engine.md`
- New CDC topics or handlers missing from `indexing/sdlc_indexing.md`
- Security model changes not in `security.md`
- Crate renames, splits, or new crates not reflected in `CLAUDE.md`
- People or epic changes not in `README.md`
- Stale IPs or secrets in `docs/dev/INFRASTRUCTURE.md`

## Fixing

1. Read the stale doc
2. Read the code it describes
3. Edit the doc to match
4. Run `mise run lint:docs` to validate
5. If you touched CLAUDE.md, mirror the change to AGENTS.md

## Self-improvement

When you find drift this skill doesn't cover, add it to "What drifts most" above. If the user provides feedback about missing documentation updates that the agent didn't catch, add it to this skill. 

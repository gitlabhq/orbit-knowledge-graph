---
name: update-docs
description: Audit and update documentation to match the current state of the codebase. Use when code changes affect architecture, APIs, or behavior that should be reflected in docs.
allowed-tools: Read, Edit, Write, Glob, Grep, Bash(npx markdownlint-cli2*), Bash(git diff*), Bash(git log*)
---

# Update documentation

When code changes land that affect architecture, data flow, APIs, or operational behavior, the docs need to reflect reality. This skill walks through what to check and how to fix it.

## Where docs live

Read `README.md` (the project SSOT) for the full resource map. The short version:

| Location | What it covers |
|---|---|
| `README.md` | Project overview, epic tracker, repositories, Helm charts, infra, people |
| `docs/design-documents/` | Architecture: indexing, querying, data model, security, observability, schema management |
| `docs/dev/` | Operational: local dev setup, infrastructure, runbook, K8s CI/CD |
| `CLAUDE.md` / `AGENTS.md` | Agent context (must stay in sync, CI enforces this) |
| `crates/*/README.md` | Per-crate documentation |
| `crates/etl-engine/AGENTS.md` | ETL engine agent context |

Use `/related-repositories` to find documentation in dependent repos (Siphon, Gitaly, GitLab, handbook).

## What to check

After a code change, work through this list. Skip sections that aren't affected.

### 1. Design documents (`docs/design-documents/`)

Compare what the code does now against what the design docs say. Common drift:

- New entity types added to the ontology but missing from `data_model.md`
- Query engine changes not reflected in `querying/graph_engine.md` or `querying/intermediary_llm_query_language.md`
- New indexing pipelines or CDC topics missing from `indexing/sdlc_indexing.md` or `indexing/code_indexing.md`
- Security model changes not in `security.md`
- New metrics or logging patterns not in `observability.md`
- Schema migration patterns not in `schema_management.md`

To find drift, diff the design doc against recent commits:

```shell
git log --oneline --since="2 weeks ago" -- crates/query-engine/
```

Then read the relevant design doc and check if the described behavior still matches.

### 2. README.md (project SSOT)

The README tracks epics, repositories, Helm charts, infrastructure, and people. Check:

- Epic links still resolve and descriptions match current scope
- Repository table is complete (new repos added?)
- Helm chart versions and descriptions are current
- Infrastructure section reflects actual environment state
- People table is current (team changes?)

### 3. Agent context (CLAUDE.md / AGENTS.md)

These files must be identical (CI checks this). Update when:

- New crates are added (add to the crate descriptions section)
- Architecture changes (update the data flow description)
- New skills are created (reference them)
- Development workflow changes (new mise tasks, new CI jobs)

### 4. Crate READMEs

Each crate in `crates/` should have a README.md describing what it does, how to use it, and how to test it. Check that:

- Public API examples still compile conceptually
- Build instructions are current
- Test instructions work

### 5. Operational docs (`docs/dev/`)

- `local-development.md` -- setup steps still work?
- `RUNBOOK.md` -- operational procedures still valid?
- `INFRASTRUCTURE.md` -- IPs, secrets, firewall rules current?

## How to fix

1. Read the doc that needs updating
2. Read the relevant code to understand the current state
3. Edit the doc to match reality
4. Run markdownlint: `npx markdownlint-cli2@0.19.0 'path/to/file.md'`
5. If you updated CLAUDE.md, copy the change to AGENTS.md (or vice versa)

## Linting rules

All markdown must pass these linters (CI enforces them via the `check-docs` job):

- **markdownlint-cli2**: config in `.markdownlint-cli2.yaml`. Use `shell` not `bash` for code fences, `plaintext` not `text`, ordered list prefixes (1, 2, 3), dash-style unordered lists.
- **Vale**: config in `.vale.ini`. Uses `gitlab_base` style. No "GitLab's" (possessive), use "lifecycle" not "life cycle", use "shell" not "bash" in code fences.
- **lychee**: config in `lychee.toml`. All internal links must resolve. Use `.md` extension on relative links.

## Self-improvement

When you find a pattern of documentation drift that this skill doesn't cover, add it. Append a new entry to the "Common drift" list in the relevant section above, or add a new section if the category doesn't exist. This makes the skill more useful for the next run.

If you discover a new documentation location or convention, update the "Where docs live" table.

If a linting rule causes repeated false positives, note the workaround here:

### Known workarounds

- `gitlab_base.Substitutions` must be disabled inline for GCP resource names containing "postgres" (use `<!-- vale gitlab_base.Substitutions = NO -->`)
- `gitlab_base.British` is disabled globally because "Cypher" is a query language name, not British spelling

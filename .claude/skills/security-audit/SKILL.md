---
name: security-audit
description: >
  Security posture analysis combining Orbit remote (SDLC graph: MRs, diffs,
  labels, vulnerabilities) with local orbit index (code graph: callgraph,
  inheritance, definitions) across quarterly snapshots. Discovers root cause
  themes from vulnerability fix diffs, measures whether those themes are being
  systemically addressed over time, and produces a posture report with positive
  findings and areas needing further analysis.
version: 0.5.0
metadata:
  audience: security-engineers
  keywords: security, audit, vulnerability, callgraph, root-cause, posture
  workflow: analysis
---

# Security Audit Skill

Answers: **what are the recurring root causes of security vulnerabilities
in this codebase, and is the security posture improving or degrading over time?**

## How it works

**Phase 1 (Orbit Remote):** Query the SDLC graph for security-related MRs.
Retrieve diff file paths and diff content. Discover root cause themes by
analyzing what the fixes actually change in the code.

**Phase 2 (Local Orbit Index):** Index the repository at quarterly snapshots
into a single DuckDB. For each theme discovered in Phase 1, measure structural
metrics across snapshots -- surface area, coupling, attack surface size,
enforcement coverage, whatever the agent determines is meaningful for that theme.

**Report:** For each theme, connect the evidence (specific MRs and diffs),
the structural trend (metrics over time), and the assessment (positive or
needs further analysis). End with an overall security posture assessment.

## Tools

Both remote and local use the same JSON query DSL.

| Tool | Use | Notes |
|------|-----|-------|
| `glab orbit remote query <file>` | SDLC graph (MRs, labels, diffs, vulns) | Wrap in `{"query": {...}}`, limit 1000 |
| `orbit query '<json>'` | Local code graph (definitions, calls, extends) | No wrapper, inline JSON |
| `orbit index <path>` | Index a repo checkout into `~/.orbit/graph.duckdb` | ~2-13s per snapshot |
| `orbit schema --ontology --expand '*'` | Local schema discovery | |
| `glab orbit remote schema <Entity>` | Remote schema discovery | |
| `duckdb ~/.orbit/graph.duckdb -json -c "..."` | SQL fallback for queries DSL can't express | Anti-joins, cross-column filters, scalar counts |

Local DSL limitation: `contains` filter uses a ClickHouse function.
Use `eq` or `starts_with` locally.

## Quarterly snapshots

Index all snapshots into ONE DuckDB. Don't wipe between indexes. Each
worktree path gets a unique `project_id` in `_orbit_manifest`. Query
across snapshots by filtering on `project_id`.

```bash
rm -f ~/.orbit/graph.duckdb   # one clean start
for each quarterly commit:
  git worktree add --detach /tmp/snap-$quarter $commit
  orbit index /tmp/snap-$quarter
  git worktree remove --force /tmp/snap-$quarter
```

## Key Orbit query patterns

Fetch security MRs with diff files (paginate with offset/page_size):
- MR → HAS_LABEL → Label (filter title="security")
- MR → HAS_LATEST_DIFF → MergeRequestDiff → HAS_FILE → MergeRequestDiffFile
- `old_path` is populated on MergeRequestDiffFile; `new_path` is not
- `diff` column on MR has full unified diff but must be fetched per-MR (batch causes content_resolution_error)
- `HAS_LATEST_DIFF` only populated for ~2024+ MRs

Fetch callers of security-relevant code (local):
- Definition → CALLS → Definition, filter target by file_path
- Definition → EXTENDS → Definition for inheritance/mixin chains
- Use `project_id` filter to query a specific quarterly snapshot

## Report structure

Each root cause theme gets: evidence (MRs, diffs, call chains),
structural metrics over time, and an assessment.

The report ends with two sections:

**POSITIVE** -- themes and metrics showing security posture is improving.
Each finding includes a metric the agent derived from the data that
supports the assessment.

**NEEDS FURTHER ANALYSIS** -- themes and metrics showing posture is
degrading or status is unclear. Each finding includes the metric that
signals the concern, why the current approach isn't sufficient, a
structural recommendation, and what additional data would help.

The agent should derive whatever metrics tell the security posture story
for the specific codebase -- not just predefined measures, but whatever
emerges from the data as meaningful (recurrence rates, fix velocity
trends, surface-to-enforcement ratios, coupling changes, coverage gaps,
migration completion percentages, etc).

## Output sanitization

`exec.sh` is a shell wrapper that pipes all stdout/stderr through sed,
replacing local paths, temp dirs, and long commit hashes with
placeholders. **Route every shell command through it.**

### Setup (once per session)

```bash
export AUDIT_REPO="/actual/path/to/repo"
export AUDIT_ORBIT="/actual/path/to/orbit"
export AUDIT_OUTPUT="/actual/scratch/dir"
EXEC=".claude/skills/security-audit/exec.sh"
```

### Usage

Prefix every command with `$EXEC`:

```bash
$EXEC git -C "$AUDIT_REPO" log --oneline -5
$EXEC "$AUDIT_ORBIT" index "$AUDIT_REPO"
$EXEC duckdb ~/.orbit/graph.duckdb -json -c "SELECT ..."
$EXEC bash rolling_window.sh "$AUDIT_REPO" "$AUDIT_ORBIT" "$AUDIT_OUTPUT"
```

### What it replaces

| Pattern | Placeholder |
|---|---|
| `$AUDIT_REPO` | `<repo>` |
| `$AUDIT_ORBIT` | `<orbit>` |
| `$AUDIT_OUTPUT` | `<output>` |
| `~/.orbit/graph.duckdb` | `<graph.db>` |
| `/tmp/...`, `/var/folders/...` | `<tmpdir>` |
| `$HOME` | `~` |
| 40-char hex hashes | first 7 chars |

### Presentation guidance

Beyond path sanitization, present tool output as a clean analysis log.
Nothing is suppressed — everything is shown, but transformed.

**Schema and API responses** — summarize structure, don't dump raw JSON:

```
# not this
$ glab orbit remote schema Label 2>&1
{"schema_version":"0.1","domains":[{"name":"ci",...400 lines...

# this
$ $EXEC glab orbit remote schema MergeRequest
  → 22 properties (id, iid, title, state, merged_at, diff, ...)
$ $EXEC glab orbit remote schema Label
  → 9 properties (id, title, description, color, ...)
  → relevant edges: MR →HAS_LABEL→ Label, MR →HAS_LATEST_DIFF→ Diff →HAS_FILE→ DiffFile
```

**Query payloads** — describe what the query does, not the raw JSON:

```
# not this
$ cat > <tmpdir>/fetch.json << 'EOF'
{ "query": { "query_type": "traversal", "nodes": [ { "id": "mr", ... 40 lines ... } ] } }
EOF
$EXEC glab orbit remote query --format raw <tmpdir>/fetch.json 2>&1 | python3 -c "..."

# this
$ $EXEC glab orbit remote query --format raw <query>
  # traversal: MergeRequest[state=merged, source_branch ^= "security-"] → limit 1000
  → 1,000 MRs returned (2024-11 → 2026-05)
```

**SQL and Python** — pseudocode or short description, result inline:

```
# not this
$ duckdb <graph.db> -json -c "
  SELECT (SELECT COUNT(DISTINCT s.fqn) FROM gl_definition s
    JOIN gl_edge e ON ... WHERE ... ) as authz_unguarded, ..."

# this
$ $EXEC duckdb <graph.db> -c <authz_unguarded_query>
  # controller methods calling services without inline auth check
  → 2020-Q1: 171/173 (99%), 2025-Q4: 236/239 (99%)
```

```
# not this
$ python3 << 'PYEOF'
import json, re
VULN_PATTERNS = [ (r'(?i)(authori[sz]ation|...)', 'authz', '...'), ... ]
for mr in mrs: ...
PYEOF

# this
$ python3 classify_mrs.py   # title + branch + diff → vuln_type
  → authz: 879, authn: 569, dos: 522, xss: 404, ...
```

**Git operations** — short hashes, placeholder paths:

```
# not this
HEAD is now at 6b3d82ef9ff9e2fb9b064b864a1229f1d8b38c67 Merge branch '38096-create-resource-weight...'

# this (exec.sh handles this automatically)
$ $EXEC git worktree add <tmpdir> 6b3d82e
$ $EXEC <orbit> index <tmpdir>
  → 58,292 defs, 85,790 calls (2.3s)
```

**Errors and retries** — show what happened, paths are auto-sanitized:

```
# not this
ERROR
  Orbit API error (HTTP 400): schema violation: Additional properties are not allowed
  ('select', 'where', 'traverse', 'page_size', 'offset' were unexpected) at ; ...

# this
$ $EXEC glab orbit remote query <query>   # first attempt, schema mismatch
  → 400: query envelope rejected (wrong top-level keys)
  retrying with corrected DSL format...
  → 1,000 MRs returned
```

### Report body

The final report uses concrete numbers, file paths, and class names
from the analysis. Don't anonymize findings, only tooling output.

## Operational notes

- Large monorepos: 24 quarterly snapshots take ~4-15 min to index
- Diff fetch: ~2 min for 100 MRs (sequential, one request per MR)
- `security` label covers vuln fixes AND hardening — classify by diff content
- Ruby CALLS/EXTENDS resolution is strongest; JS cross-file is weaker
- Git history (`security-*` branch merges) supplements for pre-2024 data

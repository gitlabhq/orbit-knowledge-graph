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

## Presentation rules

The audit output is a deliverable. Sanitize all tool output so it reads
as a clean analysis log, not a shell transcript. Nothing is suppressed —
everything is shown, but transformed.

### Path anonymization

Replace absolute filesystem paths with role placeholders. Apply
dynamically based on whatever the actual values are, not hardcoded
strings.

| Actual value | Placeholder |
|---|---|
| Repository checkout path | `<repo>` |
| Orbit binary path | `<orbit>` |
| Output/scratch directory | `<output>` |
| Graph database path | `<graph.db>` |
| Temp/worktree directories | `<tmpdir>` |
| Home directory prefix | omit or `~` |

Example:
```
# not this
$ /Users/jane/code/gkg/target/release/orbit index /Users/jane/gitlab/gdk/gitlab
# this
$ <orbit> index <repo>
```

### Command output

Show the command being run, but replace path arguments and inline
payloads with placeholders or pseudocode descriptions.

```
# not this
$ cat > /var/folders/8q/.../fetch.json << 'EOF'
{ "query": { "query_type": "traversal", "nodes": [ { "id": "mr", ... 40 lines ... } ] } }
EOF
glab orbit remote query --format raw /var/folders/8q/.../fetch.json 2>&1 | python3 -c "..."

# this
$ glab orbit remote query --format raw <query>
  # traversal: MergeRequest[state=merged, source_branch ^= "security-"] → limit 1000
  → 1,000 MRs returned (2024-11 → 2026-05)
```

### Query and code blocks

Show SQL, Python, and JSON as pseudocode or as a short description of
what the query/script computes. Include the result inline.

```
# not this
$ duckdb ~/.orbit/graph.duckdb -json -c "
  SELECT (SELECT COUNT(DISTINCT s.fqn) FROM gl_definition s
    JOIN gl_edge e ON e.source_id = s.id AND e.source_kind = 'Definition'
    JOIN gl_definition t ON ...
    WHERE s.file_path LIKE 'app/controllers/%' ... ) as authz_unguarded,
  ..."

# this
$ duckdb <graph.db> -c <authz_unguarded_query>
  # controller methods calling services without inline auth check
  → 2020-Q1: 171/173 (99%), 2025-Q4: 236/239 (99%)
```

```
# not this
$ python3 << 'PYEOF'
import json, re
from collections import Counter, defaultdict
outdir = '/var/folders/8q/...'
VULN_PATTERNS = [
    (r'(?i)(authori[sz]ation|access.control|...)', 'authz', '...'),
    ...
]
for mr in mrs: ...
PYEOF

# this
$ python3 classify_mrs.py   # title + branch + diff → vuln_type
  → authz: 879, authn: 569, dos: 522, xss: 404, ...
```

### Schema and API responses

Summarize structure rather than dumping raw JSON. Show entity names,
property counts, and relevant edges.

```
# not this
$ glab orbit remote schema Label 2>&1
{"schema_version":"0.1","domains":[{"name":"ci",...400 lines...

# this
$ glab orbit remote schema MergeRequest
  → 22 properties (id, iid, title, state, merged_at, diff, ...)
$ glab orbit remote schema Label
  → 9 properties (id, title, description, color, ...)
  → relevant edges: MR →HAS_LABEL→ Label, MR →HAS_LATEST_DIFF→ Diff →HAS_FILE→ DiffFile
```

### Git operations

Show the operation with short commit hashes. Replace worktree paths and
commit messages with placeholders or truncate.

```
# not this
HEAD is now at 6b3d82ef9ff9e2fb9b064b864a1229f1d8b38c67 Merge branch '38096-create-resource-weight...'

# this
$ git worktree add <tmpdir> 6b3d82e   # 2020-Q1 snapshot
$ <orbit> index <tmpdir>
  → 58,292 defs, 85,790 calls (2.3s)
```

### Errors and retries

Show that a retry happened and why, but anonymize paths and payloads.

```
# not this
ERROR
  Orbit API error (HTTP 400): schema violation: Additional properties are not allowed
  ('select', 'where', 'traverse', 'page_size', 'offset' were unexpected) at ; ...

# this
$ glab orbit remote query <query>   # first attempt, schema mismatch
  → 400: query envelope rejected (wrong top-level keys)
  retrying with corrected DSL format...
  → 1,000 MRs returned
```

### Report body

The final report names the target repo and uses concrete numbers,
file paths, and class names — those are the analysis, not
implementation details. Don't anonymize findings, only tooling.

## Operational notes

- Large monorepos: 24 quarterly snapshots take ~4-15 min to index
- Diff fetch: ~2 min for 100 MRs (sequential, one request per MR)
- `security` label covers vuln fixes AND hardening — classify by diff content
- Ruby CALLS/EXTENDS resolution is strongest; JS cross-file is weaker
- Git history (`security-*` branch merges) supplements for pre-2024 data

---
title: "GKG ADR 012: GOON Format (Graph Object Output Notation)"
creation-date: "2026-04-15"
last-updated: "2026-05-10"
authors: [ "@michaelangeloio", "@jgdoyon1" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-05-10

## Context

The GKG server returns graph query results through `ResponseFormat`: `RAW` produces structured JSON from `GraphFormatter`; `LLM` produces text from `GoonFormatter`. The `LLM` path existed in proto, gRPC routing, and CLI wiring before any encoding shipped — `GoonFormatter` delegated to `GraphFormatter` and returned the same JSON. No LLM-optimized encoding existed.

When an agent calls `query_graph` and receives the full `GraphResponse` JSON, a traversal of 50 users and 200 merge requests with 200 edges runs roughly 12,000–15,000 tokens. The same information in a columnar text format fits in 5,000–7,000 tokens while preserving the graph topology the agent needs. Over a multi-turn session of 5–10 graph queries, that is 50,000–80,000 tokens of context spent on structural JSON syntax.

Encoding choice also affects correctness. Google's "Talk like a Graph" study (Fatemi et al., ICLR 2024) measured graph reasoning accuracy as a function of encoding alone and observed swings between 4.8% and 61.8% per task. Incident encoding (grouping edges by source) outperformed flat edge lists and adjacency matrices across most tasks. Dense encodings with many edges acted as distractors. Application-context framing ("who authored which merge requests") outperformed abstract graph framing by up to 18 percentage points.

The encoding choice is a correctness concern as much as a token-budget one.

## Decision

Adopt GOON, a line-oriented text format for `format=llm` responses. Section-marker grammar with deterministic ordering, type-grouped node bodies, relationship-grouped edges, table-shaped aggregation rows, and chain notation for paths.

The format spec was validated by:

- A **5-variant Pareto benchmark** on Haiku 4.5 against the production GitLab.com graph (`gitlab-org/orbit/gkg-evals-harness`). 430 task-runs over two cohorts measured cost, duration, and tool-call correctness for `kv`, `col`, `hier`, `min`, `incident`, and the raw JSON baseline.
- A **corpus audit pass** running the full `fixtures/queries/corpus-input.json` against production via four parallel sub-agents, surfacing six production-confirmed encoder gaps (each fixed and regression-tested).
- A **post-merge data-loss audit** using two parallel sub-agents to walk every field of `GraphResponse` and verify the encoder reads it. Four silent drops found and fixed.

The `kv` variant was Pareto-dominant over raw JSON: −11% cost, −15% duration, +4.8pp correctness, p=0.043 on `tool_sequence_length`. The `min` variant matched `kv` on accuracy at lower token cost, which led to the `@hints` block being descoped from the format — its presence in `kv` did not improve agent behavior over `min`'s absence of it.

## Format specification

GOON is line-oriented text. Sections are delimited by `@`-prefixed markers, emitted in a fixed order: `@header`, `@nodes`, then exactly one of `@edges` (for `traversal`, `search`, `neighbors`) or `@paths` (for `path_finding`), and `@rows` for `aggregation`. Empty sections still emit their marker so a parser does not have to special-case absence.

### `@header`

Query metadata. One field per line. Always present.

```plaintext
@header
query_type:traversal
goon_version:1.0.0
nodes:10
edges:5
```

Fields:

| Field | When | Meaning |
|---|---|---|
| `query_type` | always | One of `traversal`, `aggregation`, `path_finding`, `search`, `neighbors`. |
| `goon_version` | always | This encoder's wire version (semver). Distinct from the upstream `GraphResponse.format_version`, which is the source schema version owned by ADR 004. |
| `nodes` / `edges` | always | Counts of entries in `@nodes` / `@edges`. For aggregation with node-kind group columns, `nodes` reflects the deduplicated entities lifted from rows. |
| `rows` | aggregation | Number of rows in `@rows`. |
| `group_by` | aggregation with `group_by` | Comma-separated descriptors: `name(kind)` or `name(kind:detail)`. Kind is `node` (detail is the entity type, e.g. `severity(node:Vulnerability)`) or `property` (detail is the underlying ontology property when the alias differs, e.g. `severity_bucket(property:severity)`). |
| `aggregations` | aggregation with metrics | Comma-separated descriptors: `name(function)`, `name(function:target)`, or `name(function:target.property)`. The richest form names both the node alias and the property being aggregated, so `latest_update(max:v.updated_at)` is unambiguous. |
| `has_more` | when cursor paginated and there are more rows | `true`. |
| `total_rows` | when cursor paginated | Authorized row count before cursor slicing. |

### `@nodes`

Nodes grouped by entity type. Each type starts with `TypeName(count):` then one entity per line. Each line begins with the integer ID, followed by `key=value` pairs.

```plaintext
@nodes
User(3):
1 username=alice name="Alice Smith" state=active
2 username=bob name="Bob Chen" state=active
3 username=carol name="Carol Davis" state=blocked

MergeRequest(2):
42 iid=101 state=merged title="Fix auth bug"
43 iid=102 state=merged title="Add caching layer"
```

Ordering is `(entity_type, id)` for `traversal`, `search`, `neighbors`, and `path_finding`. For `aggregation`, server row order is preserved so the order specified by `aggregation_sort` survives the encode pass.

For aggregation queries with node-kind group columns, the encoder lifts each unique `{type, id, properties}` cell from `@rows` into `@nodes` (deduplicated by `(entity_type, id)`). This keeps row lines one line each and avoids repeating node bodies on every bucket.

#### Value formatting

| Source | Encoded as | Example |
|---|---|---|
| `null`, empty string | omitted from the row | (key does not appear) |
| `null` in a `@rows` cell | bare `null` (literal) | `severity=null count=5` — a real bucket value, distinct from the string `"null"` which is quoted |
| `true` / `false` (JSON bool) | bare token | `draft=true` |
| `"true"` / `"false"` / `"null"` (JSON string) | quoted | `state="true"` — distinguishes a string from a native boolean |
| Integer | bare digits | `iid=18`, `id=12971673076` (precision preserved up to `i64`) |
| Finite float | bare | `avg_duration=941.131772070606` |
| `NaN`, `±Inf` | dropped | (key does not appear) |
| String matching `[A-Za-z0-9_\-:./@+]+` or an ISO datetime | bare | `username=stanhu`, `created_at=2026-05-08T22:55:58Z` |
| ClickHouse datetime `YYYY-MM-DD HH:MM:SS[.fraction]` | T-form (space at position 10 swapped to `T`) | `created_at=2026-05-08T22:55:58.467450` |
| Any other string | double-quoted with `\\`, `\"`, `\n`, `\r`, `\t` escapes; other control chars dropped | `title="line one\nline two"` |
| Long text (`body`, `description`, `name`, `note`, `title`) over 200 chars | truncated with `...` plus a sibling `<key>_len=N` breadcrumb | `description="..." description_len=2308` |
| Any other string over 1000 chars | same truncation + breadcrumb | |

Datetime validation goes through `chrono::NaiveDateTime::parse_from_str` and `DateTime::parse_from_rfc3339`. The output is built byte-for-byte from the input with at most one byte (the space at position 10) swapped to `T`; the source's fractional precision is preserved exactly rather than being round-tripped through chrono's nanosecond default.

Property order within a node row is column-priority then alphabetical: identity (`iid`, `username`, `name`, `full_path`, `path`, `uuid`) first, then status enums (`state`, `status`, `visibility_level`), then everything else, then timestamps (`created_at`, `updated_at`, `merged_at`, `closed_at`), then long text (`title`, `description`, `body`, `note`) last. This means a truncated description never hides a shorter identity field.

### `@edges`

Edges grouped by relationship type. Arrow notation matches what models naturally produce when formatting graph data.

```plaintext
@edges
AUTHORED(3):
User:1 --> MergeRequest:42
User:1 --> MergeRequest:43
User:2 --> MergeRequest:44

IN_PROJECT(2):
MergeRequest:42 --> Project:100
MergeRequest:43 --> Project:100
```

Ordering is total: `(path_id, step, edge_type, from, from_id, to, to_id, depth)`. Duplicates are removed using the same key. The total ordering means shuffle-invariance under property tests.

For variable-length traversals, an edge with a depth tag carries it on the row:

```plaintext
@edges
MEMBER_OF(2):
User:1 --> Group:100 depth=1
User:1 --> Group:200 depth=2
```

### `@paths` (path_finding only)

For `path_finding`, `@edges` is replaced by `@paths`. Each path is one line: a chain of `Type:id --REL--> Type:id` segments. The agent gets the path identity without reassembling it from scattered edges.

```plaintext
@paths
path=0: User:64248 --AUTHORED--> MergeRequest:482927048 --IN_PROJECT--> Project:278964
```

The `@nodes` section still carries full properties for each entity referenced in the chain.

### `@rows` (aggregation only)

Table-shaped aggregation rows. One row per line. Group columns come first, then metric columns, in the order declared by `group_by:` and `aggregations:`.

Property grouping with a single metric:

```plaintext
@header
query_type:aggregation
goon_version:1.0.0
nodes:0
edges:0
rows:5
group_by:severity(property)
aggregations:vulnerability_count(count:v)
@nodes
@edges
@rows
severity=medium vulnerability_count=8421
severity=high vulnerability_count=2350
severity=low vulnerability_count=1542
severity=critical vulnerability_count=120
severity=info vulnerability_count=42
```

Node grouping, with the lifted entity in `@nodes`:

```plaintext
@header
query_type:aggregation
goon_version:1.0.0
nodes:3
edges:0
rows:3
group_by:u(node:User)
aggregations:merged_count(count:u)
@nodes
User(3):
1243277 username=ghost1
35702613 username=bot_a
26832240 username=bot_b
@edges
@rows
u=User:1243277 merged_count=65555
u=User:35702613 merged_count=21277
u=User:26832240 merged_count=20289
```

Ungrouped (scalar) aggregation flows through the same `@rows` path with a single row:

```plaintext
@header
query_type:aggregation
goon_version:1.0.0
nodes:0
edges:0
rows:1
aggregations:total(count:u)
@nodes
@edges
@rows
total=2347
```

## Implementation

`GoonFormatter` lives in `crates/query-engine/formatters/src/goon/`. It implements the `ResultFormatter` trait the same way `GraphFormatter` does: `format(&self, output: &PipelineOutput) -> Value`. For LLM responses it composes `GraphFormatter::build_response(output)` with `goon::encode(&response, &GOON_OUTPUT_FORMAT_VERSION)` and wraps the result in `Value::String`.

Wiring at `crates/gkg-server/src/grpc/service.rs` dispatches statically per request: `req.format == ResponseFormat::Llm` calls `GoonFormatter.format_stamped(&output)`; otherwise `GraphFormatter.format_stamped(&output)`. The result rides the gRPC `ExecuteQueryResult.formatted_text` field with format-name and format-version metadata.

The encoder reads every field of `GraphResponse` (audited via parallel sub-agents post-implementation). Fields that travel:

- All node and edge fields, including `GraphEdge.depth` for variable-length traversals.
- `ColumnDescriptor.target` and `.property` (rendered in the `aggregations:` line as `function:target.property`).
- `GroupColumnDescriptor.entity` (rendered as `name(node:Entity)`) and `.property` when the alias differs from the underlying property name (rendered as `name(property:underlying)`).
- `Value::Null` in `@rows` cells renders as bare `null` so a "no severity assigned" bucket stays distinguishable from an absent column.

Fields intentionally not surfaced:

- `GraphResponse.format_version` — the upstream RAW schema version. The encoder emits `goon_version` instead; mixing both in one header creates the same field-name conflict that motivated the rename.
- `GroupColumnDescriptor.node` — the source node alias is internal compiler state.
- `GraphEdge.path_id` / `step` — used as sort keys and to drive `@paths` chain order; not surfaced as visible fields.

### Determinism

Locked by property tests with 64 cases each:

- `shuffle_invariant`: a random payload shuffled by a seeded RNG must encode byte-identically.
- `encoding_is_pure`: same input encodes to same bytes across calls.
- `output_starts_with_header`: `@header\n` always first.
- `no_unescaped_control_chars`: no raw `\r` or `\t` reaches the output.

### Versioning

`config/GOON_OUTPUT_FORMAT_VERSION` (semver, `1.0.0` at first release) follows the same discipline as `RAW_OUTPUT_FORMAT_VERSION` from ADR 004:

- `scripts/check-goon-format-version.sh` mirrors `check-response-schema-version.sh`. It watches `crates/query-engine/formatters/src/goon/**.rs`, `graph.rs`, and `lib.rs`, and requires a version bump on any change.
- Lefthook runs the check pre-commit; GitLab CI runs it on MRs in the `lint` stage.
- Bypass for wire-neutral edits: `[skip goon-format-version-check]` in the MR description or `SKIP_GOON_FORMAT_VERSION_CHECK=1` locally.

## Test coverage

| Layer | Where | Count | Covers |
|---|---|---|---|
| Unit | `crates/query-engine/formatters/src/goon/tests.rs` | 51 | Header structure, sections, quoting, escape rules, datetime normalization, truncation, numerics, edges, dedup, path-finding, aggregation shapes (property + node + ungrouped), `Value::Null` row cells, depth on variable-length edges |
| Property (`proptest`) | `tests/goon_properties.rs` | 4 × 64 | Shuffle invariance, idempotence, header prefix, no unescaped control chars |
| Snapshot (`insta`) | `tests/goon_snapshots.rs` | 7 | One golden file per query shape + pagination |
| Integration | `crates/integration-tests/tests/server/goon_formatter.rs` | 8 subtests | Full compile → execute → redact → hydrate → format path against ClickHouse testcontainers; asserts `format_stamped` returns `(Value::String, version, FormatName::Goon)`, headers carry `goon_version`, escape behavior, aggregation shapes, raw/goon count agreement |

## Why not the alternatives

**Return JSON with a token-efficient serialization (MessagePack, CBOR).** Binary formats save bytes, not tokens. LLMs tokenize text; a base64-wrapped MessagePack payload tokenizes about the same as the JSON it encodes.

**Return natural language summaries.** Natural language is the least token-efficient encoding. "User alice authored merge request 42 titled Fix auth bug which is merged" is 15 tokens; `42 iid=101 title="Fix auth bug" state=merged` is 9. It is also unparseable for follow-up queries.

**Pipe-delimited columnar tables (TOON-style headers + values).** Declaring column names once and emitting `1|alice|active` is ~10–15% more token-efficient than `key=value` for large result sets. Analysis of 316 AI coding sessions (83 Claude Code, 233 Codex) showed models never produce columnar output when formatting graph data. They consistently use inline `key=value`. The Pareto benchmark confirmed: `col` did not beat `kv` on cost-adjusted correctness.

**Return the same JSON as `format=raw`.** Wastes 40–60% of tokens on `{`, `}`, `"key":`, and commas. The starting point that motivated this ADR.

**JSON with abbreviated keys.** Shortening `"username"` to `"u"` saves tokens but forces the model to maintain a key mapping across the response. The `key=value` shape keeps full names inline at comparable token cost.

**Keep the `@hints` navigation block.** The `kv` variant carried a `@hints` block listing available outgoing relationships per entity type. The Pareto benchmark showed `min` (no hints) matched `kv` on accuracy at lower token cost. The hints did not change agent behavior enough to pay for themselves, so the block was descoped.

**Hierarchical / incident-only encoding (one section per source node).** The `hier` and `incident` variants scored higher on isolated graph-reasoning tasks but worse on the cost/duration axes in the benchmark. The Pareto front was won by `kv`'s flat type-grouped shape.

## Consequences

What improves:

- 40–60% token reduction on the LLM path, validated across all five query shapes against production data.
- −11% cost, −15% duration, +4.8pp correctness against raw JSON on Haiku 4.5 (p=0.043 on `tool_sequence_length`).
- Format matches what models naturally produce and consume for graph data (validated against 316 session transcripts).
- The shape is a pure function of `GraphResponse` — every visible field is a function of one input field. Adding a new field to the wire response is the only way to extend the format.
- `orbit query --format=llm` outputs human-scannable text instead of dense JSON.

What gets harder:

- GOON is GKG-specific. Changes to `GraphResponse` need corresponding changes to the encoder (caught by the versioned CI check).
- String quoting has more rules than pipe-delimited (escapes for `\\`, `\"`, `\n`, `\r`, `\t`; control char drops). The encoder treats this as one pass; the rules are tested.
- Snapshot tests catch any unintended format drift, which is intentional but means any deliberate format change is a multi-line snapshot diff.

Cross-language parity (Rust encoder vs the Python prototype in `gkg-evals-harness:vendor/skills/orbit-goon-kv/scripts/goon_encode.py`) is locked on the Rust side only. Byte-identical output on identical inputs is a deferred follow-up.

## Out of scope

**HTTP response body shape.** Workhorse owns the HTTP response. For `format=llm`, the body is currently `{result: "<goon-text>", query_type, raw_query_strings, row_count}` — the goon string lives in the `result` field of a JSON envelope. Returning the goon text as a plain `text/plain` body (so a viewer renders real newlines instead of `\n` escapes) requires a change in `workhorse/internal/orbit/sendquery.go`, not in GKG. That change is not part of this ADR.

## References

- Issue: [#271](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/271)
- Implementation MR: [!1289](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1289) (merged)
- Audit follow-up MR: [!1291](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1291)
- Property grouping (changed aggregation wire shape): [!1287](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1287)
- Benchmark harness: [`gkg-evals-harness!1`](https://gitlab.com/gitlab-org/orbit/gkg-evals-harness/-/merge_requests/1)
- Benchmark result note: [MR note 3331102607](https://gitlab.com/gitlab-org/orbit/gkg-evals-harness/-/merge_requests/1#note_3331102607)
- Eval archive: `gitlab-org/orbit/orbit-evals-results-archive`
- ADR 003 (API Design): [`003_api_design.md`](003_api_design.md)
- ADR 004 (Unified Query Response Schema): [`004_unified_response_schema.md`](004_unified_response_schema.md)
- ADR 008 (Workhorse Query Acceleration): [`008_workhorse_query_acceleration.md`](008_workhorse_query_acceleration.md)
- Fatemi, Halcrow, Perozzi. "Talk like a Graph: Encoding Graphs for Large Language Models." ICLR 2024.
- [TOON specification](https://github.com/toon-format/spec/blob/main/SPEC.md)
- [Orbit API Design (snippet)](https://gitlab.com/gitlab-org/gitlab/-/snippets/4929205)
- Encoder source: `crates/query-engine/formatters/src/goon/`
- Wire version constant: `config/GOON_OUTPUT_FORMAT_VERSION`
- Response JSON Schema: `config/schemas/query_response.json`
- Proto: `crates/gkg-server/proto/gkg.proto` (`ResponseFormat::RESPONSE_FORMAT_LLM`)

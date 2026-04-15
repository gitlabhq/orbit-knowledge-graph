---
title: "GKG ADR 009: GOON Format (Graph Object Output Notation)"
creation-date: "2026-04-15"
authors: [ "@michaelangeloio", "@jgdoyon1" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-04-15

## Context

### The problem

The GKG server returns graph query results in two formats via `ResponseFormat`: `RAW` (structured JSON from `GraphFormatter`) and `LLM` (from `GoonFormatter`). The LLM path exists in proto, gRPC routing, and CLI wiring, but `GoonFormatter` is a stub that delegates to `GraphFormatter` and returns the same JSON. No LLM-optimized encoding exists.

When an agent calls `query_graph`, it receives the full `GraphResponse` JSON. A traversal of 50 users and 200 merge requests with 200 edges runs roughly 12,000-15,000 tokens. The same information in a columnar text format fits in 5,000-7,000 tokens while preserving all graph topology needed for reasoning. Over a multi-turn session with 5-10 graph queries, that is 50,000-80,000 tokens of context window spent on structural JSON syntax.

The encoding format also affects accuracy. Google's "Talk like a Graph" study (Fatemi et al., ICLR 2024) measured that encoding choice alone changes LLM graph reasoning accuracy by 4.8% to 61.8% depending on the task. Incident encoding (grouping edges by source node) outperformed flat edge lists and adjacency matrices across most tasks and prompting methods. Dense graph encodings with many edges acted as distractors, degrading performance on node degree, connected nodes, and counting tasks. Application-context framing ("who authored which merge requests") outperformed abstract graph framing ("what is the degree of node i?") by up to 18 percentage points.

The encoding choice is a correctness concern as much as an efficiency one.

### What GOON needs to do

1. Encode `GraphResponse` (nodes + edges + columns + pagination) as text optimized for LLM consumption
2. Deduplicate properties by declaring column headers once per entity type, then using compact delimiter-separated rows (TOON-style columnar encoding)
3. Group edges by relationship type with source/target references using `Type:id` handles
4. Include count annotations so the agent can validate completeness (`[N]` declarations)
5. Provide navigation hints for agents to plan follow-up queries (available edge types, expansion directions)
6. Support all five query types with the same base structure
7. Fit within `ResultFormatter::format() -> Value` by returning `Value::String`

### Prior research

TOON (Token-Oriented Object Notation) benchmarks show 27.7 accuracy-points per 1,000 tokens. It uses ~40% fewer tokens than JSON with comparable comprehension accuracy. The technique: eliminate braces, quotes, and commas; declare field names once as a header row; use delimiter-separated values for data rows; add explicit count annotations (`[N]`).

The "Talk like a Graph" study tested 9 encoding functions across 6 graph tasks on PaLM 2 (XXS through L) and GPT-3.5-turbo. Findings relevant to GOON:

| Finding | Implication for GOON |
|---------|---------------------|
| Incident encoding ranked #1 across COT-BAG (1.33), COT (2.33), FEW-SHOT (2.00) | Group edges by source node or relationship type, not as flat pairs |
| Integer node IDs improve arithmetic tasks (degree, count) | Use `Type:id` integer handles for structural references |
| Named nodes improve non-numeric tasks (edge existence, cycle check) | Include human-readable labels alongside integer IDs |
| Application-context framing: 42.8% to 60.8% on edge existence | Use GitLab domain vocabulary in type hints, not abstract graph terms |
| Multiple relation types do not hurt and can help | Use actual relationship names (AUTHORED, MEMBER_OF) not generic labels |
| Dense graphs degrade performance (complete graphs worst on most tasks) | Budget response density; materialize relevant edges, summarize or paginate the rest |
| LLMs fail at absent-connection reasoning (~0% accuracy) | Never express queries as "which X is NOT connected to Y"; format for positive traversal |
| Few-shot examples from different graph structures still help | Include a compact GOON example in the `query_graph` tool description |
| Model capacity matters (XXS: 53.4% vs L: 95.4% on node degree with incident+COT-BAG) | The format must be parseable by smaller models too; keep structure simple and consistent |

Claude-Mem's 3-layer progressive disclosure pattern reduced context consumption from 25,000 tokens at 0.8% relevance to ~955 tokens at 100% relevance. For graph results: summary header (counts, types) at low cost, then columnar data for materialized nodes/edges, then on-demand expansion via follow-up queries with cursor pagination.

## Decision

### Design approach

We analyzed 316 AI coding sessions (83 Claude Code, 233 Codex) to find how models naturally format graph query results when given freedom. Only one session (c73bc421) contained a model formatting actual graph response data. Across ~30 formatting calls in that session, the model consistently chose:

- `[Type] id key=value key=value` for nodes (type-tagged, one entity per line)
- `Source(id) --RELATIONSHIP--> Target(id)` for edges (arrow notation)
- Vertical `key: value` per line when dumping all columns on a single entity
- Grouping by type via separate queries (bash loop with `=== TYPE ===` headers)
- Summary count at the top (`Rows: 5`)
- Natural language summary after a batch of queries (markdown table)

The model never used `tabulate`, `pandas`, `rich`, or any table library. It never produced columnar/CSV-style output. All formatting was f-string based with inline `key=value` pairs.

GOON follows this natural format with two targeted efficiency adjustments: (1) nodes grouped under type headers with count annotations so the type tag does not repeat per line, and (2) edges grouped under relationship type headers so the relationship name does not repeat per line. These adjustments align with the ICLR 2024 "incident encoding" finding while matching the model's natural preference for type-grouped, key=value data.

### GOON encoding specification

GOON is a line-oriented text format. Each response has four sections:

```
@header
@nodes
@edges
@hints
```

Sections are delimited by `@`-prefixed markers. Empty sections are omitted.

#### `@header`

Query metadata. One line per field.

```
@header
query_type:traversal
nodes:10
edges:5
has_more:true
total_rows:47
```

Fields: `query_type` (always present), node and edge counts, pagination info (when cursor was requested).

For neighbors queries, the header includes `center:Type:id` to identify the ego node:

```
@header
query_type:neighbors
center:User:64248
nodes:6
edges:5
```

For ungrouped (scalar) aggregation, the header carries the computed values directly (no `@nodes` section):

```
@header
query_type:aggregation
columns:total=count(mr)
total=30997
```

For paginated responses after the first page, `@hints` is omitted (the agent already has it from page 1).

#### `@nodes`

Nodes grouped by entity type. Each type group starts with `TypeName(count):` then one line per entity. Each line starts with the integer id, followed by `key=value` pairs. String values containing spaces are quoted.

```
@nodes
User(3):
1 username=alice name="Alice Smith" state=active
2 username=bob name="Bob Chen" state=active
3 username=carol name="Carol Davis" state=blocked

MergeRequest(5):
42 iid=101 title="Fix auth bug" state=merged
43 iid=102 title="Add caching layer" state=merged
44 iid=103 title="Refactor pipeline" state=opened
45 iid=104 title="Update docs" state=merged
46 iid=105 title="Remove dead code" state=closed
```

The `(count)` annotation lets the agent validate it processed all rows. The id is always first with no `id=` prefix since it is always present and always an integer.

This format mirrors what the model naturally produces: `[Type] id key=value key=value`. The type tag moves to a group header to avoid repeating it on every line.

**Value formatting rules:**

| Value type | Format | Example |
|------------|--------|---------|
| Simple identifiers (no spaces) | Unquoted | `username=stanhu` |
| Strings with spaces | Double-quoted | `title="Fix auth bug"` |
| Embedded double quotes | Backslash-escaped | `title="Remove \"Integration\" line"` |
| Embedded newlines | Escaped as `\n` | `note="Line 1\nLine 2"` |
| Null properties | Omitted entirely | (key does not appear) |
| Empty strings | Omitted entirely | (same as null) |
| Booleans | Unquoted | `draft=false` |
| Enums | Unquoted | `state=merged`, `severity=critical` |
| Hex colors | Unquoted with `#` | `color=#428BCA` |
| UUIDs | Unquoted | `uuid=e6f48912-32fb-5903-8ddd-08f639f2fd1f` |
| Datetimes | Unquoted ISO 8601 | `created_at=2026-03-25T07:47:37Z` |

**Long text truncation.** Text fields longer than 200 characters (primarily `description`) are truncated with a `...` suffix. The agent can fetch the full value via `node_ids=[X], columns=["description"]`. This follows the progressive disclosure pattern: the first response gives the agent enough to reason about the entity, and it can drill down if needed.

**Column ordering.** When `columns="*"` is requested, properties are ordered by utility: identity fields first (`iid`, `title`, `name`), then state fields (`state`, `merge_status`), then booleans, then dates, then long text (`description` always last). This means truncation of a description never hides shorter fields.

**Internal columns excluded.** `traversal_path` and any other `filterable: false` columns are stripped unconditionally, even with `columns="*"`.

For aggregation queries with `group_by`, computed columns appear as additional `key=value` pairs. The `columns:` line in `@header` serves as a legend:

```
@header
query_type:aggregation
nodes:5
columns:mr_count=count(mr)

@nodes
User(5):
1677357 username=terrichu mr_count=14617
64248 username=stanhu mr_count=2063
113870 username=iamphill mr_count=1658
15139 username=rspeicher mr_count=1566
128633 username=rymai mr_count=1562
```

#### `@edges`

Edges grouped by relationship type. Arrow notation matches the model's natural `Source(id) --REL--> Target(id)` pattern, with the relationship name in the group header instead of repeated per line.

```
@edges
AUTHORED(5):
User:1 --> MergeRequest:42
User:1 --> MergeRequest:43
User:2 --> MergeRequest:44
User:2 --> MergeRequest:45
User:3 --> MergeRequest:46

MEMBER_OF(3):
User:1 --> Project:101
User:2 --> Project:101
User:3 --> Project:102
```

Edge references use `Type:id` handles that correspond to nodes in `@nodes`. For variable-length traversals, edges include depth as a trailing key=value: `User:1 --> Project:101 depth=2`.

For neighbors queries, incoming edges (where the center node is the target) use `<--` to show direction:

```
@edges
AUTHORED(5):
User:64248 --> MergeRequest:35121
MEMBER_OF(3):
User:58926 --> Project:278964
```

When `@edges` is empty (search, scalar aggregation), the section is omitted.

#### `@paths` (path_finding only)

For `path_finding` queries, `@edges` is replaced by `@paths`. Each path is a chain of `Type:id --REL_TYPE--> Type:id` segments on a single line. This is immediately readable without requiring the agent to reassemble paths from scattered edge metadata.

```
@paths
path[0] depth=1:
User:64248 --MEMBER_OF--> Project:2009901

path[1] depth=1:
User:64248 --CREATOR--> Project:4108541

path[2] depth=2:
User:64248 --AUTHORED--> MergeRequest:4652021 --REVIEWER--> User:1
```

The `@nodes` section still provides full properties for each entity referenced in the path chains. The `path[N]` header carries the depth so the agent does not need to count arrows.

#### `@hints`

Navigation hints. These tell the agent what follow-up queries are possible without calling `get_graph_schema` first.

```
@hints
label:User=username,MergeRequest=title,Project=name
next:User-[AUTHORED]->MergeRequest,User-[MEMBER_OF]->Project,MergeRequest-[IN_PROJECT]->Project
```

`label` maps entity types to their label field from the ontology. The agent uses these when presenting results to the user.

`next` lists available relationship traversals from the entity types in the response. Only outgoing edges from materialized node types are included.

When pagination is active (`has_more:true` in header), the agent issues a follow-up query with `cursor: { offset: N, page_size: M }`.

### Examples from staging queries

These examples are from actual staging queries (`staging.gitlab.com/api/v4/orbit`), converted to GOON.

#### Search

Query: merged MergeRequests, limit 5.

JSON response is ~850 tokens. GOON encoding (~250 tokens):

```
@header
query_type:search
nodes:5

@nodes
MergeRequest(5):
3796072 iid=12075 title="Test todos_count_format helper at the correct level to improve speed" state=merged
9612987 iid=653 title="admin_user_path correction" state=merged
9613808 iid=3038 title="Add commit full time tooltip to `commited_ago`" state=merged
899439 iid=750 title="Refactor repo restrictions docs" state=merged
648214 iid=103 title="Refactor Refs to preserve their target objects instead of just a string representation" state=merged

@hints
label:MergeRequest=title
next:MergeRequest-[IN_PROJECT]->Project,MergeRequest-[FROM_BRANCH]->Branch,MergeRequest-[TARGETS]->Branch
```

~70% token reduction.

#### Traversal

Query: User -[AUTHORED]-> MergeRequest, limit 5.

```
@header
query_type:traversal
nodes:10
edges:5

@nodes
User(5):
513969 username=jacobvosmaer-gitlab
288833 username=singingwolfboy
378152 username=niijv
163582 username=gforcada
357032 username=tmaier

MergeRequest(5):
3666842 iid=169 title="Set GL_PROTOCOL during SmartHTTP.PostReceivePack" state=merged
202736 iid=53 title="Improve gitlab flow doc" state=opened
604071 iid=512 title="Fix typo in file_lock.md" state=opened
621289 iid=535 title="Removed duplicated entry" state=opened
934416 iid=764 title="Remove duplicate line on Integration" state=opened

@edges
AUTHORED(5):
User:513969 --> MergeRequest:3666842
User:288833 --> MergeRequest:202736
User:378152 --> MergeRequest:604071
User:163582 --> MergeRequest:621289
User:357032 --> MergeRequest:934416

@hints
label:User=username,MergeRequest=title
next:User-[MEMBER_OF]->Project,MergeRequest-[IN_PROJECT]->Project
```

#### Aggregation (grouped)

Query: count MergeRequests per User, top 5 by count.

```
@header
query_type:aggregation
nodes:5
columns:mr_count=count(mr)

@nodes
User(5):
1677357 username=terrichu mr_count=14617
64248 username=stanhu mr_count=2063
113870 username=iamphill mr_count=1658
15139 username=rspeicher mr_count=1566
128633 username=rymai mr_count=1562

@hints
label:User=username
next:User-[AUTHORED]->MergeRequest,User-[MEMBER_OF]->Project
```

#### Neighbors

Query: outgoing neighbors of User:64248 via AUTHORED.

```
@header
query_type:neighbors
center:User:64248
nodes:6
edges:5

@nodes
User(1):
64248 username=stanhu

MergeRequest(5):
35121 iid=237 title="Add more Slack notifications for issue and merge request events" state=closed
55033 iid=350 title="Add merge and issue event notification for HipChat" state=merged
55150 iid=352 title="Fix merge request URL passed to Webhooks" state=merged
56054 iid=355 title="Add channel override to Slack service" state=closed
56410 iid=287 title="Add custom listen_port to nginx config for reverse proxies" state=merged

@edges
AUTHORED(5):
User:64248 --> MergeRequest:35121
User:64248 --> MergeRequest:55033
User:64248 --> MergeRequest:55150
User:64248 --> MergeRequest:56054
User:64248 --> MergeRequest:56410

@hints
label:User=username,MergeRequest=title
next:User-[MEMBER_OF]->Project,MergeRequest-[IN_PROJECT]->Project,MergeRequest-[FROM_BRANCH]->Branch
```

#### Path finding

Query: shortest path from User:64248 to Project, max_depth 2.

```
@header
query_type:path_finding
nodes:3
paths:3

@nodes
User(1):
64248 username=stanhu name="Stan Hu" state=active

Project(2):
2009901 name=gitaly full_path=gitlab-org/gitaly
4108541 name=openssh-packages full_path=gitlab-org/openssh-packages

@paths
path[0] depth=1:
User:64248 --MEMBER_OF--> Project:2009901

path[1] depth=1:
User:64248 --CREATOR--> Project:4108541

path[2] depth=1:
User:64248 --CREATOR--> Project:4108541

@hints
label:User=username,Project=name
next:Project-[CONTAINS]->File,Project-[CONTAINS]->Directory
```

Multi-hop path example (User 64248 to User 1 via MergeRequests):

```
@paths
path[0] depth=2:
User:64248 --AUTHORED--> MergeRequest:4652021 --REVIEWER--> User:1

path[1] depth=2:
User:64248 --AUTHORED--> MergeRequest:4652237 --REVIEWER--> User:1
```

### `query_graph` tool description update

The `query_graph` tool description should include a ~100 token GOON format guide. A single concrete example is enough (the ICLR study found that few-shot examples help even across different graph structures):

```
Response format (GOON):
@header   -- query metadata, counts
@nodes    -- TypeName(count): then "id key=value ..." per line
@edges    -- REL_TYPE(count): then "Source:id --> Target:id" per line
@paths    -- path_finding only: "path[N] depth=D:" then chain notation
@hints    -- label:Type=field, next:traversal options

Example (traversal):
@header
query_type:traversal
nodes:3
edges:2
@nodes
User(1):
1 username=alice
Project(2):
10 name=Alpha
11 name=Beta
@edges
MEMBER_OF(2):
User:1 --> Project:10
User:1 --> Project:11
@hints
label:User=username,Project=name

Example (path_finding):
@paths
path[0] depth=2:
User:1 --AUTHORED--> MergeRequest:42 --IN_PROJECT--> Project:10
```

### Why each design choice

| Design choice | Rationale |
|---------------|-----------|
| `key=value` pairs per entity line | This is the format models naturally produce when formatting graph data (observed across 30 formatting calls in session c73bc421). No header lookup needed to interpret a line |
| Nodes grouped under `TypeName(count):` | Incident encoding (ICLR 2024, ranked #1 across COT-BAG, COT, FEW-SHOT). Avoids repeating `[Type]` on every line while preserving type grouping |
| Arrow notation `Source:id --> Target:id` | Models naturally produce this format for edge data. Immediately readable without parsing rules |
| Edges grouped under `REL_TYPE(count):` | The model achieved this only by running separate queries per relationship type. GOON provides it in a single response |
| Integer id first with no `id=` prefix | Always present, always an integer. Saves 3 tokens per line |
| `Type:id` handles in edges | Integer node encoding improves arithmetic performance (ICLR 2024). Named labels preserved via `@hints.label` |
| `@hints.next` with GitLab vocabulary | Application-context framing: 42.8% to 60.8% accuracy improvement. Semantic relationship names outperform generic labels |
| `@paths` chain notation for path_finding | Path identity is the organizing principle for path queries. Grouping by relationship type (like traversals) forces the agent to reassemble paths from scattered edge metadata |
| `center:Type:id` in header for neighbors | Identifies the ego node. The agent needs to know which node is the center vs a neighbor |
| Count annotations on type headers | Lets the agent validate it processed all rows |
| Null/empty properties omitted | Avoids blank values. The model naturally skips missing fields when formatting |
| Long text truncated at 200 chars | Progressive disclosure: summary in first response, full value on follow-up. Prevents descriptions from dominating the token budget |
| `traversal_path` excluded | Internal auth column with no semantic value for the agent |
| `@hints` omitted after page 1 | Hints are static for a given query shape. Repeating on every page wastes tokens |
| Empty sections omitted | Dense encodings degrade performance; fewer distractors is better |

### Implementation

`GoonFormatter` in `crates/query-engine/formatters/src/goon.rs` replaces its current `GraphFormatter` delegation with actual GOON text encoding:

1. Call `GraphFormatter` internally to get the structured `GraphResponse`
2. Serialize `GraphResponse` into GOON text
3. Return `Value::String(goon_text)`

This reuses the extraction logic (deduplication, edge key tracking, aggregation dispatch) that `GraphFormatter` already has. GOON is a serialization layer on top of `GraphResponse`, not a parallel pipeline.

The `@hints` section needs ontology access for `label` and `next` fields. The formatter gets `PipelineOutput` which includes the compiled query context. The ontology is available via the `OnceLock<Ontology>` global, same as the compiler.

Snapshot tests per query type go in `crates/query-engine/formatters/src/goon.rs` using `insta`, matching compiler test patterns.

## Why not the alternatives

### Return JSON with a token-efficient serialization (MessagePack, CBOR)

Binary formats save bytes but not tokens. LLMs tokenize text, not bytes. A 500-byte MessagePack payload becomes the same tokens as the JSON it encodes when base64-wrapped for text transport. The savings are on the wire, not in the context window.

### Return natural language summaries

Natural language is the least token-efficient encoding. "User alice authored merge request 42 titled Fix auth bug which is merged" is 15 tokens. `42|101|Fix auth bug|merged` is 9 tokens. At scale the difference compounds. Natural language also cannot be programmatically parsed by the agent for follow-up queries.

### Pipe-delimited columnar tables (TOON-style)

Declare column names once as a header row (`User[5]{id,username,state}`), then emit values as `1|alice|active`. This is ~10-15% more token-efficient than `key=value` for large result sets because key names are not repeated per row. We considered this initially, but analysis of 316 AI coding sessions showed that models never produce or prefer columnar/CSV-style output when formatting graph data. They consistently use inline `key=value` pairs. A format the model naturally reads is worth the small token cost.

### Return the same JSON as `format=raw`

This is the current behavior. It works but wastes 40-60% of tokens on structural syntax (`{`, `}`, `"key":`, commas) that carry no information for the LLM. Over a multi-turn session this crowds out the agent's reasoning context.

### Use JSON with abbreviated keys

Shortening `"username"` to `"u"` saves tokens but destroys readability. The LLM needs to maintain a key mapping across the response. The `key=value` format keeps full names inline at comparable token cost.

### Omit `@hints` to save tokens

The `@hints` section costs ~50-80 tokens. Without it, the agent must call `get_graph_schema` before every follow-up query, which costs 500-2000 tokens for the schema response plus a round trip. The hints pay for themselves on the first follow-up.

## Consequences

What improves:

- 50-70% token reduction depending on query type and result size
- Format matches what models naturally produce and consume when working with graph data (validated against 316 session transcripts)
- Encoding aligns with incident encoding patterns from the ICLR 2024 graph encoding study
- `@hints` lets agents explore the graph across multiple turns without calling `get_graph_schema` each time
- `orbit query --format=llm` produces human-scannable text instead of dense JSON

What gets harder:

- GOON is a GKG-specific format. Changes to `GraphResponse` structure need corresponding updates to the GOON serializer.
- String quoting: values containing spaces need quotes. Values containing quotes need escaping (`\"`). Newlines must be escaped as `\n`. More complex than pipe-delimited (which only needs pipe escaping).
- GOON output is text, so any formatting change breaks snapshot tests. This is intentional: snapshots catch unintended format drift.

Known bugs that affect GOON output (filed during staging validation):

- [#466](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/466): Center node properties are not hydrated in neighbors queries. The GOON encoder will show the center node with only its id until this is fixed.
- [#467](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/467): IN_PROJECT edges report `source_kind=Job` instead of `MergeRequest` in multi-hop traversals. The GOON encoder will show the wrong source type on these edges until this is fixed.
- [#468](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/468): Boolean properties are returned as strings (`"false"`) instead of booleans (`false`). The GOON encoder should normalize these to `true`/`false` using the ontology `data_type`.
- [#469](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/469): Neighbors query does not deduplicate edges. The same edge can appear multiple times in the response.

## References

- Issue: [#271](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/271) (Implement GOON encoder)
- [Orbit API Design ADR (snippet)](https://gitlab.com/gitlab-org/gitlab/-/snippets/4929205)
- [ADR 004](004_unified_response_schema.md) (Unified Query Response Schema)
- [ADR 003](003_api_design.md) (API Design)
- [TOON Specification](https://github.com/toon-format/spec/blob/main/SPEC.md)
- Fatemi, Halcrow, Perozzi. "Talk like a Graph: Encoding Graphs for Large Language Models." ICLR 2024.
- Formatter stub: `crates/query-engine/formatters/src/goon.rs`
- Response JSON Schema: `crates/gkg-server/schemas/query_response.json`
- Proto: `crates/gkg-server/proto/gkg.proto` (`ResponseFormat.RESPONSE_FORMAT_LLM`)

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

### GOON encoding specification

GOON is a line-oriented text format. Each response has four sections, always in this order:

```
@header
@nodes
@edges
@hints
```

Sections are delimited by `@`-prefixed markers. Within `@nodes` and `@edges`, entity types and relationship types are subsection headers. Data rows use pipe (`|`) delimiters. Strings containing pipes or newlines are quoted.

#### `@header`

Query metadata. One line per field.

```
@header
query_type:traversal
nodes:10
edges:8
has_more:true
total_rows:47
```

Fields: `query_type` (always present), node and edge counts, pagination info (when cursor was requested). This costs ~30 tokens and tells the agent what it got back.

#### `@nodes`

Nodes grouped by entity type. Each type block starts with a type header declaring column names, then compact rows.

```
@nodes
User[3]{id,username,name,state}
1|alice|Alice Smith|active
2|bob|Bob Chen|active
3|carol|Carol Davis|blocked

MergeRequest[5]{id,iid,title,state}
42|101|Fix auth bug|merged
43|102|Add caching layer|merged
44|103|Refactor pipeline|opened
45|104|Update docs|merged
46|105|Remove dead code|closed
```

The type header format: `TypeName[count]{col1,col2,...}`. The count annotation `[N]` lets the agent validate it processed all rows. Column order matches the header. `id` is always the first column.

Null values are empty between delimiters (`42||untitled|merged` means iid is null). Boolean values are `t`/`f`. Datetime values use ISO 8601.

For aggregation queries with `group_by`, computed columns are appended after entity properties:

```
@nodes
User[5]{id,username,mr_count}
1|alice|47
2|bob|31
3|carol|28
4|dave|22
5|eve|19
```

For ungrouped (scalar) aggregation, `@nodes` is empty and results go in `@header`:

```
@header
query_type:aggregation
agg:total=42,avg_size=128.5
```

#### `@edges`

Edges grouped by relationship type. Each type block has a header and rows.

```
@edges
AUTHORED[5]{from,to}
User:1->MergeRequest:42
User:1->MergeRequest:43
User:2->MergeRequest:44
User:2->MergeRequest:45
User:3->MergeRequest:46

MEMBER_OF[3]{from,to}
User:1->Project:101
User:2->Project:101
User:3->Project:102
```

Edge references use `Type:id` handles that correspond to nodes in `@nodes`. The `{from,to}` header is always present. For variable-length traversals, edges include depth: `{from,to,depth}` with rows like `User:1->Project:101|2`. For path finding, edges include path_id and step: `{from,to,path_id,step}` with rows like `User:1->MergeRequest:42|0|0`.

When `@edges` is empty (search, scalar aggregation), the section is omitted entirely.

#### `@hints`

Navigation hints. These tell the agent what follow-up queries are possible without calling `get_graph_schema` first.

```
@hints
label:User=username,MergeRequest=title,Project=name
next:User-[AUTHORED]->MergeRequest,User-[MEMBER_OF]->Project,MergeRequest-[IN_PROJECT]->Project
```

`label` maps entity types to their human-readable label field from the ontology. The agent should use these when presenting results to the user.

`next` lists available relationship traversals from the entity types present in the response. The agent can use these to plan follow-up queries without a schema lookup. Only outgoing edges from materialized node types are included.

When pagination is active (`has_more:true` in header), the agent can issue a follow-up query with `cursor: { offset: N, page_size: M }` to fetch the next page.

### Examples from staging queries

These examples are from actual staging queries (`staging.gitlab.com/api/v4/orbit`), converted to GOON.

#### Search

Query: merged MergeRequests, limit 5.

**Current JSON response** (~850 tokens):
```json
{"query_type":"search","nodes":[{"type":"MergeRequest","id":3796072,"state":"merged","title":"Test todos_count_format helper...","iid":12075},...],"edges":[]}
```

**GOON encoding** (~220 tokens):
```
@header
query_type:search
nodes:5

@nodes
MergeRequest[5]{id,iid,title,state}
3796072|12075|Test todos_count_format helper at the correct level to improve speed|merged
9612987|653|admin_user_path correction|merged
9613808|3038|Add commit full time tooltip to `commited_ago`|merged
899439|750|Refactor repo restrictions docs|merged
648214|103|Refactor Refs to preserve their target objects instead of just a string representation|merged

@hints
label:MergeRequest=title
next:MergeRequest-[IN_PROJECT]->Project,MergeRequest-[FROM_BRANCH]->Branch,MergeRequest-[TARGETS]->Branch
```

74% token reduction.

#### Traversal

Query: User -[AUTHORED]-> MergeRequest, limit 5.

**GOON encoding** (~280 tokens):
```
@header
query_type:traversal
nodes:10
edges:5

@nodes
User[5]{id,username}
513969|jacobvosmaer-gitlab
288833|singingwolfboy
378152|niijv
163582|gforcada
357032|tmaier

MergeRequest[5]{id,iid,title,state}
3666842|169|Set GL_PROTOCOL during SmartHTTP.PostReceivePack|merged
202736|53|Improve gitlab flow doc|opened
604071|512|Fix typo in file_lock.md|opened
621289|535|Removed duplicated entry|opened
934416|764|Remove duplicate line on "Integration"|opened

@edges
AUTHORED[5]{from,to}
User:513969->MergeRequest:3666842
User:288833->MergeRequest:202736
User:378152->MergeRequest:604071
User:163582->MergeRequest:621289
User:357032->MergeRequest:934416

@hints
label:User=username,MergeRequest=title
next:User-[MEMBER_OF]->Project,MergeRequest-[IN_PROJECT]->Project
```

#### Aggregation (grouped)

Query: count MergeRequests per User, top 5 by count.

**GOON encoding** (~160 tokens):
```
@header
query_type:aggregation
nodes:5
columns:mr_count=count(mr)

@nodes
User[5]{id,username,mr_count}
1677357|terrichu|14617
64248|stanhu|2063
113870|iamphill|1658
15139|rspeicher|1566
128633|rymai|1562

@hints
label:User=username
next:User-[AUTHORED]->MergeRequest,User-[MEMBER_OF]->Project
```

#### Neighbors

Query: outgoing neighbors of User:64248 via AUTHORED.

**GOON encoding** (~320 tokens):
```
@header
query_type:neighbors
nodes:6
edges:5

@nodes
User[1]{id,username}
64248|stanhu

MergeRequest[5]{id,iid,title,state}
35121|237|Add more Slack notifications for issue and merge request events|closed
55033|350|Add merge and issue event notification for HipChat|merged
55150|352|Fix merge request URL passed to Webhooks|merged
56054|355|Add channel override to Slack service|closed
56410|287|Add custom listen_port to nginx config for reverse proxies|merged

@edges
AUTHORED[5]{from,to}
User:64248->MergeRequest:35121
User:64248->MergeRequest:55033
User:64248->MergeRequest:55150
User:64248->MergeRequest:56054
User:64248->MergeRequest:56410

@hints
label:User=username,MergeRequest=title
next:User-[MEMBER_OF]->Project,MergeRequest-[IN_PROJECT]->Project,MergeRequest-[FROM_BRANCH]->Branch
```

#### Path finding

Query: shortest path from User:64248 to Project, max_depth 2.

**GOON encoding** (~200 tokens):
```
@header
query_type:path_finding
nodes:3
edges:3
paths:3

@nodes
User[1]{id,username,name,state}
64248|stanhu|Stan Hu|active

Project[2]{id,name,full_path}
2009901|gitaly|gitlab-org/gitaly
4108541|openssh-packages|gitlab-org/openssh-packages

@edges
MEMBER_OF[1]{from,to,path_id,step}
User:64248->Project:2009901|0|0

CREATOR[2]{from,to,path_id,step}
User:64248->Project:4108541|1|0
User:64248->Project:4108541|2|0

@hints
label:User=username,Project=name
next:Project-[CONTAINS]->File,Project-[CONTAINS]->Directory
```

### `query_graph` tool description update

The `query_graph` tool description should include a ~100 token GOON format guide so the agent can parse responses. A single concrete example is enough (the ICLR study found that few-shot examples help even across different graph structures):

```
Response format (GOON):
@header -- query metadata, counts
@nodes  -- TypeName[count]{col1,col2,...} then pipe-delimited rows
@edges  -- EDGE_TYPE[count]{from,to} then Type:id->Type:id rows
@hints  -- label:Type=field, next:traversal options

Example:
@header
query_type:traversal
nodes:3
edges:2
@nodes
User[1]{id,username}
1|alice
Project[2]{id,name}
10|Alpha
11|Beta
@edges
MEMBER_OF[2]{from,to}
User:1->Project:10
User:1->Project:11
@hints
label:User=username,Project=name
```

### Why each design choice

| Design choice | Rationale |
|---------------|-----------|
| Pipe-delimited columnar rows with header declaration | TOON: 40% fewer tokens than JSON with comparable accuracy. Header-once pattern eliminates per-row key repetition |
| Edges grouped by relationship type | Incident encoding (ICLR 2024): ranked #1 across COT-BAG, COT, FEW-SHOT. Grouping by type is the incident pattern applied to typed graphs |
| `Type:id` integer handles | Integer node encoding improves arithmetic performance (degree, count). Named labels preserved via `@hints.label` for non-numeric tasks |
| `@hints.next` with GitLab vocabulary | Application-context framing: 42.8% to 60.8% improvement. Semantic relationship names (AUTHORED, MEMBER_OF) outperform generic labels |
| Count annotations `[N]` on type headers | TOON's array-length declarations help LLMs validate data completeness |
| Empty sections omitted | Dense encodings with irrelevant edges degrade performance; fewer distractors is better |
| Pagination via cursor, not bulk | Agent fetches what it needs, keeping context lean |

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

### Return the same JSON as `format=raw`

This is the current behavior. It works but wastes 40-60% of tokens on structural syntax (`{`, `}`, `"key":`, commas) that carry no information for the LLM. Over a multi-turn session this crowds out the agent's reasoning context.

### Use JSON with abbreviated keys

Shortening `"username"` to `"u"` saves tokens but destroys readability. The LLM must maintain a key mapping across the response. TOON-style headers achieve the same savings while keeping full column names declared once and visible.

### Omit `@hints` to save tokens

The `@hints` section costs ~50-80 tokens. Without it, the agent must call `get_graph_schema` before every follow-up query, which costs 500-2000 tokens for the schema response plus a round trip. The hints pay for themselves on the first follow-up.

## Consequences

What improves:

- 40-74% token reduction depending on query type and result size
- Encoding aligns with the patterns that performed best in the ICLR 2024 graph encoding study
- `@hints` lets agents explore the graph across multiple turns without calling `get_graph_schema` each time
- `orbit query --format=llm` produces human-scannable text instead of dense JSON

What gets harder:

- GOON is a GKG-specific format. Changes to `GraphResponse` structure need corresponding updates to the GOON serializer.
- Pipe-delimited text needs escaping rules for values containing pipes, newlines, or leading/trailing whitespace.
- GOON output is text, so any formatting change (column order, whitespace, delimiter choice) breaks snapshot tests. This is intentional: snapshots catch unintended format drift.

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

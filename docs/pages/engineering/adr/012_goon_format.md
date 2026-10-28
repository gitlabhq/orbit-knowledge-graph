---
title: "GKG ADR 012: GOON Format (Graph Object Output Notation)"
creation-date: "2026-04-15"
last-updated: "2026-05-10"
authors: [ "@michaelangeloio", "@jgdoyon1" ]
toc_hide: true
---

## Status

Superseded.
Query results in the large language model (LLM) format now use Token-Oriented Object Notation (TOON), with one table for each node type.
We removed the Graph Object Output Notation (GOON) encoder.

## Date

2026-05-10

## Context

Orbit returns a graph query result in one of two formats.
The raw format is structured JSON, and the LLM format is text for an agent.
Orbit, the gRPC contract, and the command-line interface (CLI) accepted the LLM format before it had its own encoding.
Until then, it returned the same JSON as the raw format, so an agent that ran a graph query received the full JSON response.
A traversal of 50 users, 200 merge requests, and 200 edges used about 12,000 to 15,000 tokens.
A columnar text format held the same information in 5,000 to 7,000 tokens, with the graph topology that the agent needs.
A session with 5 to 10 graph queries spent 50,000 to 80,000 tokens of context on JSON syntax.

The encoding also affects correctness.
In a Google study, the encoding alone changed graph reasoning accuracy by 4.8 to 61.8 percentage points, depending on the task.
Incident encoding, which groups edges by source node, did better than flat edge lists and adjacency matrices on most tasks.
Dense encodings with many edges distracted the model.
Application-context framing, such as "who authored which merge requests", did better than abstract graph framing by up to 18 percentage points.

## Decision

**We adopt GOON, a line-oriented text format, for query results in the LLM format.**

The format uses section markers in a fixed order, and groups node bodies by type and edges by relationship.
Aggregation results are table-shaped rows, and paths use chain notation.

Three checks validated the format:

- A benchmark against the production GitLab.com graph, on Claude Haiku 4.5.
  It had 430 task runs in two cohorts, and it compared cost, duration, and tool-call correctness together.
  It compared five GOON variants and the raw JSON baseline.
- An audit that ran the full query corpus against production.
  It found six encoder gaps, and each got a fix and a regression test.
- A data-loss audit that checked that the encoder reads every field of the response.
  It found four silent drops, and each got a fix.

The key-value variant was better than raw JSON on all three measures at once.
It had 11% lower cost, 15% lower duration, and 4.8 percentage points higher correctness, with p=0.043 on tool sequence length.
A minimal variant without the block of navigation hints matched the key-value variant on accuracy at a lower token cost, so we removed the hints.

### Format

A result has a header section, then a nodes section.
Traversal, search, neighbors, and aggregation results then have an edges section, and path finding results have a paths section.
An aggregation result adds a rows section.
A section with no entries still has its marker, so a parser does not need a special case.

This example shows a small traversal:

```plaintext
@header
query_type:traversal
goon_version:1.0.0
nodes:2
edges:1
@nodes
User(1):
1 username=alice name="Alice Smith" state=active

MergeRequest(1):
42 iid=101 state=merged title="Fix auth bug"
@edges
AUTHORED(1):
User:1 --> MergeRequest:42
```

The header has one field on each line, with the query type, the GOON version, and the counts.
An aggregation header also describes each group column and each metric.
A paginated result with more rows has a flag for that.
The GOON version is separate from the version of the raw format, which [ADR 004](004_unified_response_schema.md) owns.

Nodes are grouped by type with a count for each type, and sorted by type and ID.
Each node line starts with the integer ID, followed by key-value pairs.
Aggregation results keep the row order from Orbit, so the requested sort order survives.
When an aggregation groups by a node, the encoder lifts each unique node from the rows into the nodes section.
A row then refers to the node by type and ID, and does not repeat the node body.

Values follow fixed rules:

- A null value or an empty string is omitted from a node line.
- A null value in an aggregation row is the bare literal null, so a "no value" bucket stays distinct from a missing column.
- Booleans, integers with full 64-bit precision, finite floats, and strings of safe characters are bare, and that includes ISO 8601 timestamps.
- A string that looks like a boolean or null is quoted, and so is any other string, with escapes for control characters.
- A float that is not a number or is infinite is dropped, and so are other control characters.
- Long text is truncated at a cap, and a sibling key records the full length.

Properties on a node line follow a fixed priority, then alphabetical order.
Identity fields come first, then status values, then other fields, then timestamps.
Long text comes last, so a truncated description never hides a short identity field.

The edges section groups edges by relationship type and uses arrow notation, as models do when they format graph data.
Edges have a total order and no duplicates.
An edge from a variable-length traversal carries its depth.
A path is one line with a chain of typed nodes and relationships, so the agent does not rebuild it from separate edges.
The nodes section still carries the full properties of each node in a path.
An aggregation row is one line with the group columns first, then the metrics.

### Encoder

The encoder builds the raw response, then encodes it as GOON text.
Orbit selects the encoder for each request, and the gRPC result carries the format name and version.
The encoder reads every field of the raw response.
It does not show three kinds of fields:

- The raw format version, because a second version field in the header causes a name conflict.
- The source node alias of a group column, because it is internal compiler state.
- The path and step positions of an edge, which set the sort order and the chain order of paths.

Property tests lock the output.
A shuffled input encodes to the same bytes, the header is always first, and no unescaped control character gets through.

GOON has its own semantic version, with the same rules as the raw format in ADR 004.
A continuous integration (CI) check requires a version bump on any encoder change, unless the change does not affect the wire output.

The HTTP response body is out of scope.
Workhorse owns the HTTP response and puts the GOON text in a field of a JSON envelope.
A plain text body would show real line breaks, and needs a change in Workhorse only.

## Benefits of this approach

The LLM path used 40% to 60% fewer tokens, across all five query shapes on production data.
The format matches what models produce and read for graph data, as 316 session transcripts showed.
The output is a pure function of the raw response, and only a new field in the wire response can extend the format.
The CLI shows readable text for the LLM format, in place of dense JSON.

## Consequences

- GOON is specific to Orbit.
  A change to the raw response needs a matching change in the encoder, and the version check catches a missed change.
- Quoting has more rules than a pipe-delimited format.
  The encoder applies the rules in one pass, and tests cover them.
- Snapshot tests catch unintended format drift.
  A deliberate format change is therefore a large snapshot diff.

A Python prototype encoder exists next to the Rust encoder, and byte-identical output from both is deferred.

## Why not the alternatives

### Binary JSON encodings

Binary formats such as MessagePack and CBOR save bytes, but they do not save tokens.
Models tokenize text, and a base64 payload uses about the same tokens as the JSON in it.

### Natural language summaries

Natural language uses the most tokens.
"User Alice authored merge request 42 titled Fix auth bug which is merged" uses 15 tokens.
The GOON line for the same merge request uses 9 tokens.
An agent also cannot parse natural language for a follow-up query.

### Pipe-delimited columnar tables

A table with column names in a header and pipe-delimited values uses 10% to 15% fewer tokens than key-value pairs on large results.
In 316 AI coding sessions, models never produced columnar output when they formatted graph data.
They always used inline key-value pairs.
In the benchmark, the columnar variant did not beat the key-value variant on cost-adjusted correctness.

### The same JSON as the raw format

JSON spends 40% to 60% of its tokens on braces, quoted keys, and commas.
That cost was the starting point for this decision.

### JSON with short keys

Short keys save tokens, but the model must keep a key map across the response.
Key-value pairs keep full names at a similar token cost.

### Keep the navigation hints block

The key-value variant had a block that listed the outgoing relationships for each entity type.
In the benchmark, the minimal variant without hints matched it on accuracy at a lower token cost.
The hints did not change agent behavior enough to pay for their tokens.

### Hierarchical or incident encoding

These variants put one section for each source node.
They did better on isolated graph-reasoning tasks, but worse on cost and duration in the benchmark.
The flat key-value shape, grouped by type, had the best balance of cost, duration, and correctness.

## References

- [Implement Graph Object Oriented Notation (GOON) encoder](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/271)
- [Implement the GOON encoder](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1289)
- [Surface every response field in GOON output](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1291)
- [Support property grouping aggregations](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1287)
- [Python GOON encoder prototype and benchmark configuration](https://gitlab.com/gitlab-org/orbit/gkg-evals-harness/-/merge_requests/1)
- [Benchmark result note](https://gitlab.com/gitlab-org/orbit/gkg-evals-harness/-/merge_requests/1#note_3331102607)
- [GOON format snippet](https://gitlab.com/gitlab-org/gitlab/-/snippets/4929205)
- [TOON specification](https://github.com/toon-format/spec/blob/main/SPEC.md)
- Fatemi, Halcrow, and Perozzi, "Talk like a Graph: Encoding Graphs for Large Language Models", ICLR 2024
- [ADR 003: Orbit API design (unified REST and GraphQL)](003_api_design.md)
- [ADR 004: Unified query response schema](004_unified_response_schema.md)
- [ADR 008: Workhorse query acceleration](008_workhorse_query_acceleration.md)

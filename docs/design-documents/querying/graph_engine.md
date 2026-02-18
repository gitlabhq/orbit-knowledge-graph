---
title: ClickHouse Graph Query Engine
description: Plan for using ClickHouse as the primary database for graph queries and building a graph engine on top.
---

## Overview

We will use ClickHouse as the primary database for graph queries in deployed environments. The query tier compiles high‑level graph operations into ClickHouse SQL and executes them directly on adjacency‑ordered edge tables and typed node tables. This pairs OLAP throughput with property‑graph semantics (paths, traversals, pattern matching) without introducing another datastore.

This plan draws on the [team's research](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/267). The approach keeps the service stateless and focuses on schema design, SQL generation, and guardrails.

## Storage Model in ClickHouse

The storage model aims to replicate the CSR (Compressed Sparse Row) adjacency list index concepts from [KuzuDB’s whitepaper](https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf).

- Nodes by type: separate ReplacingMergeTree tables per entity (e.g., `groups`, `projects`, `issues`, `merge_requests`, `files`, `symbols`).
  - Primary key and ORDER BY include `organization_id`, then `traversal_id` (for SDLC entities) or `branch` (for code node types), followed by the local identifier.
  - SDLC example: `(organization_id, traversal_id, issue_id)`
  - Code example: `(organization_id, branch, symbol_id)`

- Edges by relationship: separate MergeTree tables per relationship (e.g., `has_subgroup`, `has_project`, `mr_closes_issue`, `imports`, `calls`).
  - ORDER BY `(organization_id, traversal_id, src_id, dst_id)` for SDLC edges or `(organization_id, branch, src_id, dst_id)` for code edges to create contiguous adjacency lists per source. This is the on-disk adjacency index the query engine exploits for fast neighbor scans.
  - Optional projection per edge table ordered by destination for reverse traversals: `ORDER BY (organization_id, traversal_id, dst_id, src_id)` or `ORDER BY (organization_id, branch, dst_id, src_id)`.
  - Branched edges for code relationships; current-state edges for SDLC hierarchy unless historical analysis is required.

- `traversal_id`: SDLC entities (issues, merge requests, pipelines, etc.) are enriched with a `traversal_id` column during indexing. This array represents the full ancestor hierarchy of the entity's parent namespace (e.g., `[100, 200, 300]` for a project inside a subgroup inside a top-level group). The query engine uses `traversal_id` for prefix-based permission filtering, enabling efficient authorization checks without joining back to the namespace hierarchy. See [Security Architecture](../security.md) for details on how `traversal_id` filters are injected.

- `branch`: Code node types (`files`, `symbols`, `definitions`) include a `branch` column to track which Git branch the indexed code belongs to. SDLC entities do not use this column as it is not relevant to their current state. **Note**: we intend to only index the **current** state of a particular branch in the initial iteration, not the historical state. See [Code Indexing](/handbook/engineering/architecture/design-documents/gitlab_knowledge_graph/indexing/code_indexing.md) for more details.

- Multi‑tenancy and authorization are enforced in every query by prefix filtering on `organization_id` and `traversal_id` to keep scans local and permission-scoped.

## Query Engine Design

There will be two ways to interact with the graph engine:

1. Intermediate JSON tools (MCP/HTTP): existing JSON schemas describe graph intents (`find_nodes`, `neighbors`, `paths`, `aggregate_nodes`). The server validates input and compiles to parameterized SQL.
2. Cypher reader (optional): Cypher to SQL translation à la ClickGraph for teams that prefer property‑graph syntax or need Neo4j driver compatibility.

The planner emits ClickHouse SQL similar to these patterns:

- One‑hop neighbors: equality filter on the edge table’s leading keys, `WHERE organization_id = ? AND branch = ? AND src_id IN (...)` (for code) or `WHERE organization_id = ? AND hasAll(traversal_id, ?) AND src_id IN (...)` (for SDLC), producing O(degree) scans per source.
- Multi‑hop fixed depth (2–3): chained JOINs/CTEs with DISTINCT frontiers between hops to avoid blow‑ups.
- Variable‑length paths: `WITH RECURSIVE` over the edge table(s) with a depth limit and optional accumulation of `nodes(path)` and `relationships(path)` as arrays.
- Reverse hops: use the destination‑ordered projection or a reversed view produced on the fly.
- Alternate relationship types: `UNION ALL` of the participating edge tables inside a single `edges_all` CTE, then join once per hop.
- Aggregations: push filters early; perform groupings on the smallest necessary sets; avoid post‑filtering of large results.

These choices preserve factorization: each hop operates on a compact frontier and prunes the next edge scan via semi‑joins, mirroring Kùzu’s accumulate → semijoin → probe execution.

## Request Flow (Deployed)

1. Client (MCP or REST) submits a tool call or Cypher.
2. Adapter validates/normalizes input pursuant to the currently deployed schema, resolves `organization_id`, computes the user's `traversal_id` prefixes for SDLC queries, and selects the active `branch` for code queries.
3. Planner compiles to ClickHouse SQL (CTEs, recursive CTEs, unions, joins) with bound parameters.
4. ClickHouse executes; the server returns rows plus the generated SQL for audit.

Namespace graph updates arrive via an ETL worker, described in [SDLC Indexing](../indexing/sdlc_indexing.md). The indexer publishes a small state record (namespace → active state). The web tier caches namespace metadata and injects appropriate filters into queries; no file swapping is required.

## Authorization and Safety

- Hard filters in SQL: every query carries `organization_id` and an allow‑list of resource scopes (e.g., pre‑authorized projects).
- Redaction layer: final pass to drop rows the upstream filters could not precisely exclude (e.g., confidential flags). Avoid redaction for aggregates; either pre‑filter or block the query shape.
- All queries will be parameterized.
- Depth caps and relationship allow‑lists to prevent runaway traversals; row and time limits per request.
- Grammar‑based query validation: the planner validates generated SQL against a strict subset of the ClickHouse grammar. The validator walks the AST to verify that required predicates (e.g., `organization_id`, traversal ID filters) are present in `WHERE` clauses and rejects queries that omit them. This "fail closed" approach ensures malformed or overly broad queries are blocked before execution rather than relying solely on downstream filtering.

## Observability

- Per‑phase timings (parse/plan/render/execute) and row counts.
- Emitted SQL and parameter map for debugging.
- ClickHouse query metrics (system.query_log) correlated by request ID.

## Integration with Indexing

The indexer writes denormalized, typed node and edge tables in ClickHouse via ETL rather than synchronous materialized views. The exact mechanisms for this are covered in [SDLC Indexing](../indexing/sdlc_indexing.md) and [Schema Management](../schema_management.md). Materialized views would require filtered license checks on every inserted row, reducing ingestion efficiency. ETL decouples transformation from ingestion, allowing the indexer to batch writes and maintain control over schema evolution without impacting ClickHouse insert performance. Materialized views are reserved for precomputing stable summaries (e.g., group closure) that change infrequently and do not require per-row filtering, but these are optional enhancements for performance and may be subject to change. For reverse access paths, projections are built per table.

## Unified Security and Performance Testing

Security testing and performance testing share the same underlying techniques for the query engine. We treat them as a single validation effort:

- **Fuzzing**: Automated generation of malformed, edge-case, and adversarial inputs to the JSON tool interface and (optionally) Cypher parser. The same fuzzer that finds performance regressions (e.g., queries that blow up in time or memory) will also surface authorization bypass attempts (e.g., queries missing required predicates).
- **Automated Query Generation**: Property-based testing that generates random valid query shapes and verifies:
  - All generated SQL includes `organization_id` and traversal ID predicates (security invariant).
  - Query execution time stays within bounds (performance invariant).
  - Result sets respect authorization constraints (correctness invariant).
- **Automated Penetration Testing**: Scripted scenarios that attempt common bypass techniques (SQL injection, predicate stripping, cross-tenant access). These run as part of CI and are informed by the threat model.

This unified approach ensures that security and performance are validated together—an authorization check that slows queries unacceptably is as much a bug as one that fails to block unauthorized access. Results from fuzzing and automated testing feed back into both the threat model and the grammar-based validation described in [Authorization and Safety](#authorization-and-safety).

In addition to the above, a formal threat model is being developed for the query engine. This will be tracked as an epic under the broader GKGaaS effort, with specific issues for high-risk components such as the query planner and JSON-to-SQL transformation pipeline. For the full authorization model (tenant segregation, traversal ID filtering, JWT verification, and final redaction), see [Security Architecture](../security.md).

## References

- graphhouse experiments and benchmarks (multi‑table schema, adjacency ordering, recursive CTEs)
- ClickGraph engine (Cypher → ClickHouse SQL, recursive CTEs, path functions, Bolt/HTTP front ends)

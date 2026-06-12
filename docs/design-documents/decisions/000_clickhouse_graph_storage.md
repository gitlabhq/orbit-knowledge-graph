---
title: "GKG ADR 000: ClickHouse as graph storage"
creation-date: "2025-11-03"
authors: [ "@michaelangeloio", "@jgdoyon1", "@bohdanpk", "@michaelusa" ]
toc_hide: true
---

## Status

Accepted (recorded retroactively; this decision predates the numbered ADR series)

## Date

2025-11-03

## Context

The Knowledge Graph was originally built on [Kuzu](https://docs.kuzudb.com/), a
file-embedded graph database, as a local-only desktop tool (see
[previous design](../previous_design/README.md)). In October 2025, KuzuDB
[was archived](https://www.theregister.com/2025/10/14/kuzudb_abandoned/) by its
maintainers, forcing a storage decision for the deployed service.

The team validated database options against both Code Indexing and SDLC
indexing workloads, using the SDLC
[dataset generator](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/merge_requests/292)
and pre-existing Code Index parquet files
([Database Selection Epic](https://gitlab.com/groups/gitlab-org/rust/-/epics/31)).
The evaluation covered both new databases (Neo4j, FalkorDB, Memgraph, Neptune,
SpannerGraph) and already-deployed, approved GitLab databases (PostgreSQL and
ClickHouse).

## Decision

Build a Graph Query Engine on ClickHouse, modeling GitLab data as a property
graph in ClickHouse tables, rather than adopting a dedicated graph database.
PostgreSQL is the fallback option.

### Validation

Inspired by [Brahmand](https://www.brahmanddb.com/) and
[SQL 2023's standardization of property graphs](https://www.iso.org/standard/79473.html)
(ISO/IEC 9075-16:2023), the team created a modified version of the
[Demo Instance](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/263)
(which originally used Kuzu) and swapped it out with ClickHouse
([demo](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/268#note_2873427090),
[code](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/merge_requests/391)),
proving a functioning product with a ClickHouse/Postgres-backed property graph
model. `@andrewn` also created a
[Cypher to Postgres](https://gitlab.com/andrewn/opencypher-to-postgres#project-walkthrough)
project that
[passes 70%](https://gitlab.com/gitlab-com/gl-infra/sandbox/opencypher-to-postgres/-/merge_requests/20)
of OpenCypher's TCK suite, which much of the team can leverage.

Kùzu is a columnar system similar to modern read-optimized analytical DBMSs,
like ClickHouse. The team conducted
[research and benchmarking](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/267)
against a ClickHouse and Postgres-backed property graph, which alleviated
performance concerns: <300ms p95 query speeds for 3-hop traversals on a
20M+ row, 11GB dataset, leveraging CSR adjacency list index concepts from
[KuzuDB's whitepaper](https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf).

### Why a Graph Query Engine on ClickHouse?

- The **data model** (property graphs with arbitrary nodes and edges) is the
  most critical aspect of this product and enables the "Knowledge Graph"
  capabilities, irrespective of the underlying database.
- GitLab has significantly **more operational experience** with ClickHouse and
  Postgres than with graph databases (Neo4j, FalkorDB).
- By leveraging the existing stack, there is **one less database to deploy and
  maintain**, reducing SRE and DBRE costs.
- More **engineering investment goes into ClickHouse** over building an ETL
  pipeline from ClickHouse to a graph database, meaning the GKG team can help
  with Siphon and NATS.
- **Faster time to market** with this query layer.
- **Two-way door**: if the database does not suit our needs, the deployed
  components (Siphon, NATS, ClickHouse) remain the foundation for a data
  pipeline to a new graph database (Neo4j, FalkorDB, Memgraph).
- **Legal and procurement barriers**: because of unfriendly licenses, any new
  database has to go through both legal and ZIP review.

### Legal and procurement barriers with graph-native databases

The team evaluated the following databases:

- Neptune (cloud only)
- SpannerGraph (cloud only)
- Neo4j (EE license)
- FalkorDB (SSPL and EE license)
- Memgraph (BSL and EE license)

After meeting with legal and procurement teams, proceeding with any of these
databases would require purchasing an enterprise edition license from the
database provider, in addition to the engineering challenges they introduce.
This would mean a minimum 30-day negotiation and procurement cycle.

### Why not fork Kuzu?

The team evaluated and considered this, but found the risk from a security and
maintenance perspective to be too high. We keep an eye on
[LadyBug](https://github.com/LadybugDB/ladybug), its most popular fork.

## Consequences

- Graph nodes live in typed `gl_*` ClickHouse tables; relationships live in
  ontology-configured edge tables with adjacency-optimized ordering and
  projections. See the [data model](../data_model.md) and
  [Graph Query Engine](../querying/graph_engine.md) design documents for the
  as-built implementation.
- The query layer compiles an intermediate JSON query language into
  parameterized ClickHouse SQL instead of executing Cypher natively.
- PostgreSQL remains the backup option, leveraging the
  opencypher-to-postgres work.

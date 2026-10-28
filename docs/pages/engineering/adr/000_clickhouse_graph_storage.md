---
title: "GKG ADR 000: ClickHouse as graph storage"
creation-date: "2025-11-03"
authors: [ "@michaelangeloio", "@jgdoyon1", "@bohdanpk", "@michaelusa" ]
toc_hide: true
---

## Status

Accepted.
We recorded this decision after the fact, because it predates the numbered ADR series.

## Date

2025-11-03

## Context

Orbit started as a local-only desktop tool on Kuzu, a file-embedded graph database.
In October 2025, its maintainers archived the project.
The deployed service then needed a new storage decision.

We tested database options against the code indexing and software development lifecycle (SDLC) indexing workloads.
The tests used generated SDLC datasets and existing code index Parquet files.
We evaluated new databases: Neo4j, FalkorDB, Memgraph, Neptune, and SpannerGraph.
We also evaluated the approved databases that GitLab already deploys: PostgreSQL and ClickHouse.

## Decision

**We build a graph query engine on ClickHouse and model GitLab data as a property graph in ClickHouse tables.**

We do not adopt a dedicated graph database.
PostgreSQL is the fallback option.

The data model is the most important part of the product.
Property graphs with arbitrary nodes and edges give Orbit its graph capabilities, whatever the database.
GitLab has much more operational experience with ClickHouse and PostgreSQL than with graph databases.
The existing stack means one fewer database to deploy and maintain, which reduces site reliability and database reliability engineering costs.
Engineering effort goes into ClickHouse instead of an extract, transform, and load (ETL) pipeline from ClickHouse to a graph database.
The Orbit team can then help with Siphon and NATS.
The query layer also gets Orbit to market faster.

The decision is a two-way door.
If ClickHouse does not suit our needs, Siphon, NATS, and ClickHouse remain the base for a data pipeline to a graph database.

### Validation

We based the design on the Brahmand project and on the SQL:2023 standard for property graphs (ISO/IEC 9075-16:2023).
We changed the demo instance from the original graph database to ClickHouse.
The demo proved a working product on a property graph model in ClickHouse and PostgreSQL.
A separate OpenCypher-to-PostgreSQL project passes 70% of the OpenCypher Technology Compatibility Kit (TCK), and the team can reuse much of it.

Kuzu is a columnar system, similar to modern read-optimized analytical databases such as ClickHouse.
We ran a benchmark of a property graph on ClickHouse and PostgreSQL, and the results removed the performance concerns.
Three-hop traversals on a dataset of more than 20 million rows and 11 GB ran at a p95 below 300 ms.
The benchmark used compressed sparse row (CSR) adjacency list index ideas from the Kuzu research paper.

## Consequences

- Graph nodes live in typed ClickHouse tables, one for each entity type.
- Relationships live in edge tables that the ontology configures, with ordering and projections tuned for adjacency.
- The query layer compiles an intermediate JSON query language into parameterized ClickHouse SQL, and does not run OpenCypher natively.
- PostgreSQL remains the backup option, and it can reuse the OpenCypher-to-PostgreSQL work.

## Why not the alternatives

### A graph-native database

Neptune and SpannerGraph are available only as cloud services.
Neo4j needs an enterprise edition license.
FalkorDB uses the Server Side Public License (SSPL) and an enterprise edition license.
Memgraph uses the Business Source License (BSL) and an enterprise edition license.

Because of these licenses, any new database must pass legal review and procurement review.
We met with the legal and procurement teams.
Each of these databases would need an enterprise edition license from the vendor, in addition to the engineering challenges.
The negotiation and procurement cycle takes at least 30 days.

### Fork Kuzu

We evaluated a fork and found the security and maintenance risk too high.
We watch LadyBug, the most popular fork.

## References

- [Kuzu documentation](https://docs.kuzudb.com/)
- [Previous Orbit design](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/previous_design/README.md)
- [KuzuDB abandoned](https://www.theregister.com/2025/10/14/kuzudb_abandoned/)
- [GitLab SDLC graph performance analysis](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/merge_requests/292)
- [Knowledge Graph database selection](https://gitlab.com/groups/gitlab-org/rust/-/epics/31)
- [Brahmand](https://www.brahmanddb.com/)
- [ISO/IEC 9075-16:2023 property graph queries](https://www.iso.org/standard/79473.html)
- [Orbit service proof of concept demo and vision](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/263)
- [Orbit discussion log: ClickHouse demo](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/268#note_2873427090)
- [ClickHouse version of the demo](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/merge_requests/391)
- [OpenCypher to PostgreSQL project walkthrough](https://gitlab.com/andrewn/opencypher-to-postgres#project-walkthrough)
- [Add the OpenCypher TCK test suite with CI integration](https://gitlab.com/gitlab-com/gl-infra/sandbox/opencypher-to-postgres/-/merge_requests/20)
- [Database selection: ClickHouse research and benchmarks](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/267)
- [Kuzu whitepaper](https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf)
- [LadyBug](https://github.com/LadybugDB/ladybug)
- [Data model design document](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/data_model.md)
- [Graph query engine design document](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/querying/graph_engine.md)

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

Orbit started as a local desktop tool that stored its graph in Kuzu, an embedded graph database.
In October 2025, the Kuzu maintainers archived the project, so the deployed Orbit needed a new database.

We tested database options against the code indexing and software development lifecycle (SDLC) indexing workloads.
The tests used generated SDLC datasets and existing code index Parquet files.
We evaluated Neo4j, FalkorDB, Memgraph, Neptune, and SpannerGraph, which GitLab does not run today.
We also evaluated PostgreSQL and ClickHouse, which GitLab already deploys and has approved.

## Decision

**We build a graph query engine on ClickHouse and model GitLab data as a property graph in ClickHouse tables.**

We do not adopt a dedicated graph database.
PostgreSQL is the fallback option.

The data model matters more than the database.
A property graph of typed nodes and edges gives Orbit its graph features on any database.
GitLab has much more operational experience with ClickHouse and PostgreSQL than with graph databases.
With no new database to deploy and maintain, site reliability and database reliability engineers have less work.
We also do not build an extract, transform, and load (ETL) pipeline from ClickHouse to a graph database.
The Orbit team can spend that time on Siphon and NATS, the pipeline that brings GitLab data into ClickHouse.
A query layer on ClickHouse also gets Orbit to market faster.

We can reverse this decision.
If ClickHouse does not suit our needs, Siphon, NATS, and ClickHouse can feed a graph database instead.

### Validation

We drew on the Brahmand project and on the SQL:2023 standard for property graphs (ISO/IEC 9075-16:2023).
We moved the demo instance from the original graph database to ClickHouse.
It worked as a product on a property graph in ClickHouse and PostgreSQL.
A separate OpenCypher-to-PostgreSQL project passes 70% of the OpenCypher Technology Compatibility Kit (TCK), and the team can reuse much of it.

Kuzu is a columnar database, like read-optimized analytical databases such as ClickHouse.
So we benchmarked a property graph on ClickHouse and PostgreSQL, with adjacency index ideas from the Kuzu research paper.
Three-hop traversals on more than 20 million rows ran at a p95 below 300 ms, which removed our performance concerns.

## Consequences

- Graph nodes live in typed ClickHouse tables.
- Relationships live in edge tables that the ontology configures, with ordering and projections tuned for adjacency.
- The query layer compiles GQL queries into parameterized ClickHouse SQL, and does not run OpenCypher natively.
- PostgreSQL remains the backup option, and it can reuse the OpenCypher-to-PostgreSQL work.

## Why not the alternatives

### A graph-native database

Neptune and SpannerGraph are available only as cloud services.
Neo4j needs an enterprise edition license.
FalkorDB uses the Server Side Public License (SSPL) and an enterprise edition license.
Memgraph uses the Business Source License (BSL) and an enterprise edition license.

Because of these licenses, any new database must pass legal and procurement review.
We met with the legal and procurement teams.
Each database would need an enterprise edition license from the vendor, on top of the engineering work.
The negotiation and procurement cycle takes at least a month.

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

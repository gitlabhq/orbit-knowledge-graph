.. _gkg-architecture:

1. Simplified Architecture Block Diagram
========================================

Orbit (the cloud-deployed GitLab Knowledge Graph) unifies SDLC metadata and
source code into a single property graph stored in ClickHouse, queryable
through a compiled DSL, and accessible to LLM agents, the Orbit dashboard,
and the GitLab Web IDE.

.. uml:: puml/gkg_architecture_simplified.puml
   :caption: Orbit architecture

For the fully expanded subcomponent diagram, see
:ref:`Appendix: Detailed Cloud Architecture <gkg-appendix-detailed-arch>`.
For the standalone desktop binary (embedded DuckDB, no cloud
dependencies), see :ref:`Appendix: Desktop Mode <gkg-appendix-desktop>`.

Components Inside the Module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 15 60
   :header-rows: 1

   * - Component
     - Technology
     - Purpose
   * - **SDLC Indexer**
     - Rust ETL
     - Replicates SDLC entities (work items, MRs, pipelines, vulnerabilities,
       users, groups, projects, milestones, labels, notes) from PostgreSQL via
       Siphon CDC into ClickHouse property graph tables.  Uses NATS JetStream,
       keyset pagination, per-entity watermarks.
   * - **Code Indexer**
     - parser-core, OXC, tree-sitter
     - Fetches source from Gitaly, parses with language-specific analyzers,
       indexes definitions and references as graph nodes/edges.  Incremental
       fetch with rename detection.  Code nodes link to SDLC context via
       shared ontology.
   * - **Graph Query Engine**
     - Rust (13-phase compiler)
     - Compiles untrusted JSON DSL into parameterized ClickHouse SQL.
       Four query types: multi-hop traversal, aggregation, pathfinding, and
       nearest neighbors (full-text search is a mode of traversal, not a
       separate type).  Auth via traversal_path filter injection.  Semi-Join
       Pushdown, edge-centric traversal, keyset pagination.
   * - **gRPC Server**
     - Tonic (+ Axum)
     - gRPC service (9 RPCs) consumed by the Rails monolith; the only HTTP
       endpoints are the ``/live`` and ``/ready`` health probes.  The public
       REST API (``/api/v4/orbit/*``) and MCP surface live in Rails and proxy
       to this gRPC service.  JWT auth; metrics via OpenTelemetry.
   * - **Orbit Dashboard**
     - Vue 3, Three.js, Monaco
     - ``/dashboard/orbit`` in the Rails monolith.  3D graph visualization,
       query editor with templates, table view + CSV export, schema browser,
       namespace config, cluster health.
       

External Connections
~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 22 15 15 48
   :header-rows: 1

   * - System
     - Protocol
     - Direction
     - Purpose
   * - **ClickHouse Cloud**
     - TCP native / HTTP
     - Orbit <-> CH
     - Code + SDLC data lake (property graph tables)
   * - **PostgreSQL** (via Siphon)
     - CDC / keyset pagination
     - PG -> Siphon -> Orbit
     - Source of SDLC entities
   * - **NATS JetStream**
     - NATS subscribe
     - NATS -> Orbit
     - Event coordination for SDLC and code indexing
   * - **Gitaly**
     - gRPC / streaming archive
     - Orbit -> Gitaly
     - Source code fetch, incremental file download
   * - **Rails (JWT claims)**
     - JWT in request
     - Rails -> Orbit
     - Allowed traversal paths delivered in the request JWT
       (``group_traversal_ids``); no separate Orbit-side auth cache
   * - **Rails Redaction**
     - gRPC stream
     - Orbit <-> Rails
     - ``Ability.allowed?`` batch permission check over the bidirectional
       gRPC stream
   * - **DAP / AI Gateway**
     - HTTP + JWT
     - DAP -> Orbit
     - Agent tool calls (``query_graph``, ``get_graph_schema``)
   * - **Rails gRPC Client**
     - gRPC
     - Rails -> Orbit
     - 9 RPC methods
   * - **Snowplow -> CDot**
     - Event emission
     - Orbit -> Snowplow
     - Billing / credit consumption metering
   * - parser-core
     - Rust lib (compiled in)
     - internal
     - AST parsing (OXC for JS/TS, tree-sitter for others)
   * - gitalisk-core
     - Rust lib (compiled in)
     - internal
     - Git operations

Cloud-Provider Constraints
~~~~~~~~~~~~~~~~~~~~~~~~~~

**Exception: ClickHouse Cloud.**  GitLab.com and GitLab Dedicated plan to use
ClickHouse Cloud as the backing store.  This is a SaaS dependency that is
**not** on the current approved list (GCP, AWS, Anthropic).  Remediation: the
ClickHouse protocol is open-source and wire-compatible --- if ClickHouse Cloud
becomes untenable, a self-hosted ClickHouse cluster can be substituted without
code changes.

All other dependencies are open-source and self-hosted:

- NATS JetStream (open-source message broker)
- Standard HTTP, gRPC, MCP protocols
- Cross-platform Rust binary (Linux, macOS, Windows)

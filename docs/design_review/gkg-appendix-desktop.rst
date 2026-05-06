.. _gkg-appendix-desktop:

Appendix: Desktop Mode (Embedded Ladybug)
==========================================

GKG ships a standalone desktop binary that runs without any external
infrastructure.  This mode is separate from the cloud-deployed Orbit
architecture reviewed in the main sections and is included here for
completeness.

.. uml:: puml/gkg_architecture.puml
   :caption: Desktop mode --- self-contained binary with embedded Ladybug DB

Architecture
------------

The desktop binary is a single statically-linked Rust executable.  It embeds
the Ladybug graph database (Arrow/Parquet columnar storage on local disk) and
requires no external database, message broker, or container runtime.

.. list-table::
   :widths: 25 15 60
   :header-rows: 1

   * - Component
     - Technology
     - Purpose
   * - **CLI (gkg)**
     - Rust / Clap
     - Command-line entry point: ``index``, ``serve``, ``query``
   * - **HTTP Server**
     - Axum 0.8
     - REST API, SSE events, MCP endpoints, embedded Vue SPA
   * - **Indexer**
     - Rayon (parallel)
     - Parses source code, builds graph nodes and relationships
   * - **MCP Server**
     - rmcp 0.8
     - Exposes graph tools to LLMs via Model Context Protocol
   * - **Workspace Manager**
     - Rust
     - Project lifecycle, git operations, state persistence
   * - **Event Bus**
     - Tokio broadcast
     - In-process pub/sub for indexing progress events
   * - **Ladybug Graph DB**
     - lbug 0.15 (embedded)
     - Arrow/Parquet columnar graph storage on local disk
   * - **Observability**
     - Prometheus + OTEL
     - Metrics export, distributed tracing

Operational Modes
-----------------

1. **CLI-only** --- ``gkg index`` + ``gkg query``.  No server process.
2. **Server** --- ``gkg serve`` starts the HTTP API, browser UI, and MCP
   endpoints on ``localhost:27495``.

Resource Footprint
------------------

- **Minimum:** 2 CPU cores, 512 MB RAM, 1 GB disk
- **Recommended:** 4+ CPU cores, 2 GB RAM, 10 GB disk (for large codebases)
- **No external services required**

Limitations vs. Cloud Mode
--------------------------

Desktop mode does **not** include:

- SDLC indexing (no Siphon CDC, no NATS, no PostgreSQL replication)
- The graph query engine (no JSON DSL, no ClickHouse SQL compilation)
- The three-layer authorization stack (no org isolation, no traversal filtering,
  no Rails redaction)
- DAP integration or gRPC API
- Billing / credit metering
- The Orbit dashboard (``/dashboard/orbit``)

Desktop mode is suitable for local code navigation, MCP tool access for IDEs,
and developer experimentation.  It is the only option currently available for
Self-Managed customers (see Section 2 for delivery risk).

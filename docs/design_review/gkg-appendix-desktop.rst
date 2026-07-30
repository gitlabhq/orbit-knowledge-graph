.. _gkg-appendix-desktop:

Appendix: Desktop Mode (``orbit`` CLI, Embedded DuckDB)
======================================================

GKG ships a standalone desktop binary (``orbit``) that runs without any
external infrastructure.  This mode is separate from the cloud-deployed Orbit
architecture reviewed in the main sections and is included here for
completeness.

.. uml:: puml/gkg_architecture.puml
   :caption: Desktop mode --- self-contained ``orbit`` binary with embedded
             DuckDB

Architecture
------------

The desktop binary is a single statically-linked Rust executable (the
``orbit`` binary, crate ``orbit-local``).  It embeds DuckDB (Arrow/Parquet
columnar storage on local disk) and requires no external database, message
broker, or container runtime.

.. list-table::
   :widths: 25 15 60
   :header-rows: 1

   * - Component
     - Technology
     - Purpose
   * - **CLI (orbit)**
     - Rust / Clap
     - Command-line entry point: ``index``, ``sql``, ``schema``, ``list``,
       ``mcp``, ``version``
   * - **Indexer**
     - Rayon (parallel)
     - Parses source code, builds graph nodes and relationships
   * - **MCP Server**
     - rmcp 1.7 (stdio)
     - Exposes graph tools (``run_sql``, ``get_graph_schema``, ``index``) to
       LLMs over the Model Context Protocol, transported on stdio
   * - **Workspace Manager**
     - Rust
     - Project lifecycle, git operations, state persistence
   * - **Event Bus**
     - Tokio broadcast
     - In-process pub/sub for indexing progress events
   * - **DuckDB (embedded)**
     - duckdb 1.x
     - Arrow/Parquet columnar graph storage on local disk
   * - **Observability**
     - Prometheus + OTEL
     - Metrics export, distributed tracing

Operational Modes
-----------------

1. **CLI** --- ``orbit index`` builds the graph; ``orbit sql``, ``orbit
   schema``, and ``orbit list`` inspect it.  Querying is done through SQL
   (``orbit sql``), not the cloud JSON DSL.  No server process.
2. **MCP server** --- ``orbit mcp serve`` exposes the graph tools to LLMs and
   IDEs over the Model Context Protocol, transported on **stdio**.  There is no
   HTTP listener, no network port, and no embedded browser UI.

Resource Footprint
------------------

- **Minimum:** 2 CPU cores, 512 MB RAM, 1 GB disk
- **Recommended:** 4+ CPU cores, 2 GB RAM, 10 GB disk (for large codebases)
- **No external services required**

Limitations vs. Cloud Mode
--------------------------

Desktop mode does **not** include:

- SDLC indexing (no Siphon CDC, no NATS, no PostgreSQL replication)
- The graph query engine (no JSON DSL, no ClickHouse SQL compilation ---
  local queries run as SQL against embedded DuckDB)
- The authorization stack (no org isolation, no JWT-delivered traversal-path
  filtering, no Rails redaction)
- DAP integration or the gRPC API
- Billing / credit metering
- The Orbit dashboard (``/dashboard/orbit``)

Desktop mode is suitable for local code navigation, MCP tool access for IDEs,
and developer experimentation.  It is the only option currently available for
Self-Managed customers (see Section 2 for delivery risk).

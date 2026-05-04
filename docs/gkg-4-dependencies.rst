.. _gkg-dependencies:

4. Dependency Map
=================

.. uml:: puml/gkg_dependencies.puml
   :caption: Orbit dependency map --- ingestion, query, and consumer dependencies

What Orbit Depends On
---------------------

**Build-time dependencies** (compiled into the binary):

.. list-table::
   :widths: 18 12 12 28 30
   :header-rows: 1

   * - Dependency
     - Type
     - Sync/Async
     - What it provides
     - If unavailable
   * - **parser-core**
     - Rust lib
     - Sync
     - AST parsing (OXC for JS/TS, tree-sitter for Ruby, Python, Java,
       Kotlin, C#, Rust)
     - Build fails.  No runtime fallback.
   * - **gitalisk-core**
     - Rust lib
     - Sync
     - Git clone, file enumeration, repo state
     - Build fails.  Cannot discover files.
**Runtime dependencies:**

.. list-table::
   :widths: 18 12 12 28 30
   :header-rows: 1

   * - Dependency
     - Type
     - Sync/Async
     - What it provides
     - If unavailable
   * - **ClickHouse Cloud**
     - External DB
     - Sync
     - Code + SDLC property graph storage and querying
     - All search and indexing fail.  Service is non-functional.
   * - **PostgreSQL** (via Siphon)
     - CDC source
     - Async
     - SDLC entity replication (issues, MRs, pipelines, vulnerabilities,
       users, groups, projects, milestones, labels, notes)
     - SDLC index goes stale.  Code index unaffected.
   * - **NATS JetStream**
     - Message broker
     - Async (sub)
     - Event coordination for SDLC CDC and code re-indexing triggers
     - Automatic re-indexing stops.  Manual index still works.  Per-entity
       watermarks enable catch-up on reconnect.
   * - **Gitaly**
     - gRPC / archive
     - Sync
     - Source code fetch (streaming archive download, incremental with rename
       detection)
     - Code indexing fails.  SDLC index unaffected.
   * - **Redis**
     - TCP
     - Sync
     - Auth cache (prefix-tree of allowed traversal paths)
     - Auth falls back to uncached computation.  Performance degrades but
       correctness maintained.
   * - **Rails Redaction Service**
     - HTTP
     - Sync
     - ``Ability.allowed?`` batch permission check on query results
     - Search results cannot be returned.  Blocks the request.
   * - **DAP** (Data Access Proxy)
     - HTTP proxy
     - Sync
     - Routes JWT-authenticated requests from Rails to Orbit
     - Orbit unreachable from Rails.  Search and DAP tools unavailable.

What Depends on Orbit
---------------------

.. list-table::
   :widths: 22 25 20 15 18
   :header-rows: 1

   * - Consumer
     - What they consume
     - Protocol
     - Required
     - Fallback
   * - **Rails gRPC Client**
     - 5 RPC methods (stub-cached)
     - gRPC
     - Optional
     - Standard code navigation
   * - **Orbit Dashboard**
     - Graph viz, query editor, schema browser, health
     - REST (async)
     - Optional
     - Dashboard shows unavailable state
   * - **DAP / AI Gateway**
     - ``get_graph_entities``, ``execute_query``, ``get_tool`` fallback
     - HTTP + JWT
     - Optional
     - Agents fall back to individual REST API calls
   * - **Web IDE / Duo Chat**
     - Code intelligence (definitions, references, search)
     - MCP (HTTP+SSE)
     - Optional
     - IDE falls back to standard LSP
   * - **Snowplow -> CDot**
     - Billing events (credit consumption)
     - Event emission
     - Required for billing
     - Consumption not tracked (revenue leakage)

No consumer has a hard dependency on Orbit for core GitLab functionality.  If
Orbit is unavailable, all consumers degrade gracefully --- except billing,
where untracked consumption represents revenue leakage.

Monolith Integration Points
----------------------------

Orbit was **not extracted from the monolith** --- it was built as a standalone
Rust application.  No monolith dependencies are being severed.

The integration surface with the monolith:

- **Inbound (Rails -> Orbit):** gRPC client (5 RPCs), DAP proxy (HTTP+JWT)
- **Outbound (Orbit -> Rails):** Redaction service (batch permission check),
  internal API (Gitaly access)
- **Data replication:** Siphon CDC (PostgreSQL -> ClickHouse)
- **Frontend:** Orbit dashboard (``/dashboard/orbit``) served from Rails,
  data fetched async from Orbit REST API
- **Billing:** Orbit emits Snowplow events consumed by CDot

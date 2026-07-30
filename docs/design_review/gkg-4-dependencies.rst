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
     - AST parsing.  OXC for JavaScript/TypeScript, a custom pipeline for
       Rust, and tree-sitter for 15+ other languages (Bash, C, C++, Python,
       Java, Kotlin, Scala, C#, Go, Elixir, Ruby, Lua, PHP, Swift, HCL).
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
     - SDLC entity replication (work items, MRs, pipelines, vulnerabilities,
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
   * - **Rails Redaction**
     - gRPC stream
     - Sync
     - ``Ability.allowed?`` batch permission check on query results, over the
       bidirectional gRPC stream back to Rails
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
     - 9 RPC methods
     - gRPC
     - Optional
     - Standard code navigation
   * - **Orbit Dashboard**
     - Graph viz, query editor, schema browser, health
     - REST (async, via Rails)
     - Optional
     - Dashboard shows unavailable state
   * - **DAP / AI Gateway**
     - ``query_graph``, ``get_graph_schema``; ``list_commands`` /
       ``invoke_command`` fallback
     - HTTP + JWT
     - Optional
     - Agents fall back to individual REST API calls
   * - **Web IDE / Duo Chat**
     - Code intelligence (definitions, references, search)
     - MCP (Rails-side for cloud; stdio for local ``orbit``)
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

- **Inbound (Rails -> Orbit):** gRPC client (9 RPCs), DAP proxy (HTTP+JWT)
- **Outbound (Orbit -> Rails):** batch redaction over the gRPC stream
  (``Ability.allowed?``), internal API (Gitaly access)
- **Data replication:** Siphon CDC (PostgreSQL -> ClickHouse)
- **Frontend:** Orbit dashboard (``/dashboard/orbit``) served from Rails,
  data fetched async from the Rails ``/api/v4/orbit/*`` REST API
- **Billing:** Orbit emits Snowplow events consumed by CDot

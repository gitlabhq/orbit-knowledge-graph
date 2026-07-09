.. _gkg-appendix:

Appendix: Canonical Subcomponent Definitions
=============================================

These definitions are the authoritative descriptions of each GKG/Orbit
subcomponent, referenced throughout the CTO review document.

SDLC Indexing
-------------

Replicates GitLab SDLC entities (work items, MRs, pipelines, vulnerabilities,
users, groups, projects, milestones, labels, notes) from PostgreSQL via Siphon
CDC into a ClickHouse data lake, then transforms them through a Rust ETL
engine into property graph tables.  The pipeline uses NATS JetStream for event
coordination, cursor-based keyset pagination for large tables, and per-entity
watermarks for granular retry.  (The graph now spans ~31 node types across
eight domains --- SDLC, source code, CI, security, packages, and more --- not
just the SDLC entities listed here.)

Code Indexing
-------------

Fetches source code from Gitaly, parses it with language-specific analyzers
(OXC for JS/TS, a custom pipeline for Rust, and tree-sitter grammars for a
growing set that includes Ruby, Python, Java, Kotlin, C#, Go, C, C++, Scala,
Elixir, PHP, Swift, Lua, Bash, and HCL), and indexes definitions and references
as graph nodes and edges.
Supports incremental file fetching with rename detection, event-driven
reconciliation, and streaming archive downloads.  Code nodes link back to
their SDLC context (projects, MRs, branches) through the shared ontology.

Graph Query Engine
------------------

Compiles untrusted JSON DSL into parameterized ClickHouse SQL through a
13-phase pipeline (validate, normalize, restrict, plan, lower, enforce,
security, partition, cursor, check, hydrate_plan, settings, codegen).
Supports four query types: traversal, aggregation, pathfinding, and neighbors
(nearest neighbors); full-text search is a mode of traversal, not a separate
type.  Authorization is enforced via traversal_path filter injection (paths
supplied in the request JWT) and, for the final permission check, resource
extraction for Rails redaction over the gRPC stream.  The engine uses cascading
Semi-Join Pushdown (SIP), edge-centric traversal, and keyset pagination for
performance.

Rails Monolith Backend Integration
-----------------------------------

Rails backend integration for Orbit.  Covers the three-layer authorization
stack (org isolation, traversal ID filtering from JWT-supplied
``group_traversal_ids`` claims, and Rails redaction via ``Ability.allowed?``
over the gRPC stream), JWT authentication, the gRPC client (9 RPC methods with
stub caching), the Rails-hosted MCP endpoint (JSON-RPC 2.0), the REST API under
``/api/v4/orbit/*`` (query, schema, dsl, status, tools) proxying to gRPC,
internal API for Gitaly access, Siphon ClickHouse tables for CDC replication,
and the Orbit dashboard controller.

Monolith Frontend Integration
------------------------------

Vue 3 native dashboard at ``/dashboard/orbit`` using ``@vue/compat``.  The
frontend includes a Three.js graph visualization (node explorer with 2D/3D
toggle), a Monaco-based query editor with template queries, a table view with
CSV export, a schema browser with domain filtering, namespace configuration
with GraphQL mutations, and cluster health monitoring.  All data is fetched
async from the REST API.

DAP Integration
---------------

Integrates Orbit into the Duo Agent Platform as the default context layer for
agent tool calls.  Orbit exposes agent tools --- ``query_graph`` and
``get_graph_schema``, plus a ``list_commands`` / ``invoke_command`` fallback
pattern for named agent commands --- that replace dozens of individual REST API
calls.  Agents construct their own DSL queries against the graph schema, which
uses progressive disclosure to keep token usage around 3k.  Integration requires wiring through
AI Gateway, creating a custom agent for evals, and resolving billing
discernment (standalone vs. within-DAP sessions).

Monetization Engineering
------------------------

Handles metering, billing, and license enforcement for Orbit.  GKG uses a
credit-based pricing model on .com and Dedicated (zero-rated for GitLab-driven
queries, charged for customer-driven queries) and a per-seat add-on for
Self-Managed.  The billing pipeline emits Snowplow events to CDot, which
tracks credit consumption.

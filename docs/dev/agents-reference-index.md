# Reference index

Canonical locations for files, schemas, configs, and tools in the knowledge-graph repo.

| What | Where |
|---|---|
| **Domain glossary** | **`CONTEXT.md`** |
| Indexer crate guide (handlers, reuse-infra checklist) | **`crates/indexer/AGENTS.md`** |
| Architecture and data model | `docs/design-documents/data_model.md` |
| Security / AuthZ design | `docs/design-documents/security.md` |
| Query DSL spec | `docs/design-documents/querying/` |
| SDLC indexing pipeline | `docs/design-documents/indexing/sdlc_indexing.md` |
| Code indexing pipeline | `docs/design-documents/indexing/code_indexing.md` |
| Namespace deletion pipeline | `docs/design-documents/indexing/namespace_deletion.md` |
| Schema migration strategy | `docs/design-documents/schema_management.md` |
| Observability / SLOs | `docs/design-documents/observability.md` |
| Duo / Orbit prompt routing (Rails-side) | `docs/design-documents/duo_orbit_prompt_routing.md` |
| Ontology node definitions | `config/ontology/nodes/` |
| Ontology edge definitions | `config/ontology/edges/` |
| Ontology JSON schema | `config/schemas/ontology.schema.json` |
| Graph query JSON schema | `config/schemas/graph_query.schema.json` |
| Named query definitions | `config/named_queries/` (parsed/embedded by `crates/named-queries`, compiled against the ontology by `crates/gkg-server/build.rs`, executed via gRPC `QUERY_TYPE_NAMED`, listed via gRPC `ListNamedQueries`) |
| Named query JSON schema | `config/schemas/named_query.schema.json` (validate with `mise named-queries:validate`; CI gate `named-query-schema-validate`) |
| Query DSL version | `config/QUERY_DSL_VERSION` |
| Server config JSON schema | `config/schemas/config.schema.json` (generated via `mise schema:generate`) |
| Query response JSON schema | `config/schemas/query_response.json` |
| Query language reference (text-indexed properties table is generated) | `docs/source/remote/queries/query-language.md` (regenerate the ontology-derived table with `mise docs:query-language`; CI gate `query-language-docs-check`) |
| Query test fixtures | `fixtures/queries/` |
| Query corpus (categorized YAML) | `fixtures/queries/corpus/` (smoke-tested in CI: `corpus_smoke`) |
| Graph DDL (ClickHouse) | `config/graph.sql` |
| Refreshable-view MiniJinja SQL templates | `config/ontology/sql/*.sql.j2` (ClickHouse SELECT templates rendered from the schema version and ontology-derived graph table metadata) |
| Schema version file | `config/SCHEMA_VERSION` (bump when `graph.sql` or `config/ontology/` changes) |
| RAW output format version | `config/RAW_OUTPUT_FORMAT_VERSION` (semver, bump when `graph.rs` or `query_response.json` changes) |
| Graph DDL (local DuckDB) | Generated at runtime from ontology via `generate_local_tables()` + `duckdb_ddl` |
| Datalake DDL (ClickHouse) | `fixtures/siphon.sql` |
| gRPC service definition | `crates/gkg-server/proto/gkg.proto` |
| Server config structure | `crates/gkg-server-config/src/app.rs` (`AppConfig`), `config/default.yaml` |
| Query settings (timeouts, cache) | `config/default.yaml` (`query:` section), `crates/gkg-server-config/src/query.rs` |
| Configuration runbook | `docs/dev/runbooks/server_configuration.md` |
| Local development guide | `docs/dev/local-development.md` |
| Local development (`mise run dev`) | `scripts/gkg-native-dev.sh`, `docs/dev/local-development.md` |
| Operational runbooks | `docs/dev/runbooks/` |
| Architecture Decision Records | `docs/design-documents/decisions/` |
| **All project links** (repos, epics, infra, people, Helm charts) | `README.md` (single source of truth) |
| Code history / dead code investigation | `/code-history` skill |
| AST-based code search / rewrite | `ast-grep` skill, `.claude/skills/ast-grep/` |
| Related repos and local paths | `/related-repositories` skill |
| Iglu schemas (committed; codegen'd at build) | `config/schemas/iglu/<name>/<version>.json` (update via `mise iglu:bump -- <name> <version>`) |
| Iglu version pins | `config/schemas/iglu/*.version` (bump via `mise iglu:bump -- <name> <version>`, check via `mise iglu:check`) |
| Analytics event definition | `config/events/gkg_query_executed.yml` |
| Analytics contexts (Snowplow) | `crates/gkg-analytics/src/context.rs` (types), `crates/gkg-server/src/analytics/` (builders + observer) |
| Billing config + observer | `crates/gkg-billing/`, `crates/gkg-server/src/billing_adapter.rs` |
| SOX billing authoring rules | `docs/dev/sox-billing-boundary.md` |
| Query profiler CLI | `crates/query-engine/profiler/`, `mise query:profile` |

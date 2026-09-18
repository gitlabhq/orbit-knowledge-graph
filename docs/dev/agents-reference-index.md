# Reference index

Canonical locations for files, schemas, configs, and tools in the knowledge-graph repo.

| What | Where |
|---|---|
| **Domain glossary** | **`CONTEXT.md`** |
| Indexer crate guide (handlers, reuse-infra checklist) | **`crates/indexer/AGENTS.md`** |
| Architecture and data model | `docs/design-documents/data_model.md` |
| Security / AuthZ design | `docs/design-documents/security.md` |
| FIPS posture (module guard, graph and binary gates) | `crates/orbit-server/src/fips.rs`, `scripts/check-fips-graph.sh`, `scripts/check-fips-binary.sh`; design in `docs/design-documents/security.md` |
| Image signing (keyless cosign, canonical project only) | `scripts/publish-manifest.sh`, `scripts/sign-image.sh`; runbook in `docs/dev/runbooks/image_signing.md`; design in `docs/design-documents/security.md` |
| Query DSL spec | `docs/design-documents/querying/` |
| Orbit query frontend | `crates/query-engine/compiler/src/passes/frontend/`; design in `docs/design-documents/querying/orbit_query_frontend.md` |
| SDLC indexing pipeline | `docs/design-documents/indexing/sdlc_indexing.md` |
| Code indexing pipeline | `docs/design-documents/indexing/code_indexing.md` |
| Namespace deletion pipeline | `docs/design-documents/indexing/namespace_deletion.md` |
| Schema migration strategy | `docs/design-documents/schema_management.md` |
| Observability / SLOs | `docs/design-documents/observability.md` |
| Duo / Orbit prompt routing (Rails-side) | `docs/design-documents/duo_orbit_prompt_routing.md` |
| Ontology node definitions | `config/ontology/nodes/` |
| Ontology edge definitions | `config/ontology/edges/` |
| Ontology derived entity definitions | `config/ontology/derived/` |
| Ontology extraction SQL | Generated from the pipeline (`query: generated`) for nodes and edges; a `.sql.j2` MiniJinja template next to the YAML only for complex nodes (`config/ontology/nodes/`) and derived entities (`config/ontology/derived/`) |
| Ontology JSON schema | `config/schemas/ontology.schema.json` |
| Graph query JSON schema | `config/schemas/graph_query.schema.json` |
| YAML document type configs | `crates/code-graph/src/v2/langs/generic/yaml/document_types/` (one file per document type, embedded via rust-embed, interpreted by `document_types.rs`) |
| YAML document type JSON schema | `config/schemas/yaml_document_type.schema.json` (configs validated against it when the code-graph YAML pipeline first loads them) |
| Named query definitions | `config/named_queries/` (parsed/embedded by `crates/named-queries`, compiled against the ontology by `crates/orbit-server/build.rs`, executed via gRPC `QUERY_TYPE_NAMED`, listed via gRPC `ListNamedQueries`) |
| Named query JSON schema | `config/schemas/named_query.schema.json` (validate with `mise named-queries:validate`; CI gate `named-query-schema-validate`) |
| Agent prompt files (tool descriptions) | `config/prompts/` (versioned YAML, one file per prompt; `remote/` feeds `orbit-server`, `local/` feeds `orbit-cli`; embedded via rust-embed and build-time validated by `crates/orbit-prompts`) |
| Server config JSON schema | `config/schemas/config.schema.json` (generated via `mise schema:generate`) |
| Query response JSON schema | `config/schemas/query_response.json` |
| Query language reference (text-indexed properties table is generated) | `docs/source/remote/queries/query-language.md` (regenerate the ontology-derived table with `mise docs:query-language`; CI gate `query-language-docs-check`) |
| Query test fixtures | `fixtures/queries/` |
| YAML query scenarios (data correctness) | `crates/integration-tests/tests/server/data_correctness/scenarios/<category>/*.yaml` (run with `mise test:integration:server`; filter with `SCENARIO_FILTER=<name>`) |
| Query scenario presets | `crates/integration-tests/tests/server/data_correctness/presets/` (`seed.yaml`, `security.yaml`, `redaction.yaml`) |
| Query scenario format reference | `crates/integration-testkit/README.md` ("Query scenarios" section) and `crates/integration-testkit/src/query_scenario/format.rs` (`QueryScenario`, `QueryExpect`, `NodeExpect`) |
| Query corpus (categorized YAML) | `fixtures/queries/corpus/` (smoke-tested in CI: `corpus_smoke`) |
| Ontology overlays for speculative schema shapes | `config/seeds/overlays/<name>/` (a directory mirroring `config/ontology/`, deep-merged over it; run data correctness against one with `mise test:integration:overlay <name>`) |
| Graph DDL (ClickHouse, versioned) | `config/graph.sql` |
| Graph DDL (ClickHouse, persistent) | `config/graph_persistent.sql` (durable unversioned tables + materialized views created once at boot) |
| Denormalized joins (`settings.denormalized_joins` in `schema.yaml`) | `crates/ontology/src/denormalized.rs` (table chain, column contract), `crates/ontology/src/loading/mod.rs` (`resolve_denormalized_join`), `crates/query-engine/compiler/src/passes/codegen/ddl/denormalized.rs` (table and feeding views composed from the source tables' generated DDL); design in `docs/design-documents/querying/graph_engine.md` |
| Refreshable-view MiniJinja SQL templates | `config/ontology/sql/*.sql.j2` (ClickHouse SELECT templates rendered from the schema version and ontology-derived graph table metadata) |
| Pinned versions | `config/versions.yaml` (`schema` u32 bumped via `mise schema:bump`; `query_dsl`, `raw_output_format`, `goon_output_format` semvers enforced by `scripts/check-pinned-version.sh`; `gitlab_system_note_actions` upstream SHA; `vendored:` section for DuckDB and other vendored deps with sub-pins, artifact dirs, and scripts; embedded at compile time as `orbit_versions::VERSIONS`) |
| Vendored dependency system | `docs/dev/runbooks/vendored_dependencies.md` (lifecycle, YAML contract, script contract, validation layers); generic runner in `scripts/vendored/run.sh` |
| Graph DDL (local DuckDB) | Generated at runtime from ontology via `generate_local_tables()` + `duckdb_ddl` |
| Datalake DDL (ClickHouse) | `fixtures/siphon.sql` |
| gRPC service definition | `crates/orbit-server/proto/orbit.proto` |
| Server config structure | `crates/orbit-server-config/src/app.rs` (`AppConfig`), `config/default.yaml` (embedded; declares every setting, no Rust fallbacks) |
| Object storage config | `config/default.yaml` (`object_storage:` section), `crates/orbit-server-config/src/object_storage.rs`, client in `crates/object-storage/src/lib.rs` |
| Query settings (timeouts, cache) | `config/default.yaml` (`query:` section), `crates/orbit-server-config/src/query.rs` |
| Configuration runbook | `docs/dev/runbooks/server_configuration.md` |
| Local development guide | `docs/dev/local-development.md` |
| Local development (`mise run dev`) | `scripts/orbit-native-dev.sh`, `docs/dev/local-development.md` |
| Operational runbooks | `docs/dev/runbooks/` |
| Architecture Decision Records | `docs/design-documents/decisions/` |
| **All project links** (repos, epics, infra, people, Helm charts) | `README.md` (single source of truth) |
| Code history / dead code investigation | `/code-history` skill |
| AST-based code search / rewrite | `ast-grep` skill, `.claude/skills/ast-grep/` |
| Orbit issue, epic, and MR planning taxonomy | `/orbit-planning` skill |
| Related repos and local paths | `/related-repositories` skill |
| Iglu schemas (committed; codegen'd at build) | `config/schemas/iglu/<name>/<version>.json` (update via `mise vendor -- iglu`) |
| Iglu version pins | `vendored.iglu.pins` in `config/versions.yaml` (edit pin, then `mise vendor -- iglu` to fetch; check via `mise check:vendored -- iglu`) |
| Analytics event definition | `config/events/gkg_query_executed.yml` |
| Analytics contexts (Snowplow) | `crates/orbit-analytics/src/context.rs` (types), `crates/orbit-server/src/analytics/` (builders + observer) |
| Billing config + observer | `crates/orbit-billing/`, `crates/orbit-server/src/billing_adapter.rs` |
| SOX billing authoring rules | `docs/dev/sox-billing-boundary.md` |
| Query profiler CLI | `crates/query-engine/profiler/`, `mise query:profile` |

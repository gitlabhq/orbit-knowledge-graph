# Reference index

Canonical locations for files, schemas, configs, and tools in the knowledge-graph repo.

| What | Where |
|---|---|
| **Domain glossary** | **`CONTEXT.md`** |
| Generated translations | `docs-locale/` is machine output; never read, edit, or reference it |
| Indexer crate guide (handlers, reuse-infra checklist) | **`crates/indexer/AGENTS.md`** |
| Architecture overview and data model | `docs/pages/engineering/architecture/README.md`, `docs/pages/engineering/architecture/data-model.md` |
| Security / AuthZ design | `docs/pages/engineering/architecture/security/` |
| FIPS posture (module guard, graph and binary gates) | `crates/orbit-server/src/fips.rs`, `scripts/checks/fips/check-graph.sh`, `scripts/checks/fips/check-binary.sh`; posture in the FIPS section of `docs/pages/engineering/architecture/security/README.md` |
| Image signing (keyless cosign, canonical project only) | `scripts/release/publish-manifest.sh`, `scripts/release/sign-image.sh`; guide in `docs/pages/engineering/development/image-signing.md` |
| Image hardening (non-root user, Dockerfile scan, release gate, Vulnerability Report) | `Dockerfile`, `dockerfile-scan`, `release-image-scan`, `container_scanning` in `.gitlab-ci.yml` |
| Query language reference | `docs/docs-gitlab-com/queries/query-language.md` |
| Orbit query frontend | `crates/query-engine/compiler/src/passes/frontend/`; design in `docs/pages/engineering/architecture/querying/query-engine.md` |
| SDLC indexing pipeline | `docs/pages/engineering/architecture/indexing/sdlc-indexing.md`, `docs/pages/engineering/runbooks/sdlc_indexing.md` |
| Code indexing pipeline | `docs/pages/engineering/architecture/indexing/code-indexing.md`, `docs/pages/engineering/runbooks/code_indexing.md` |
| Namespace deletion pipeline | `docs/pages/engineering/architecture/indexing/sdlc-indexing.md` |
| Schema migration strategy | `docs/pages/engineering/architecture/indexing/schema-evolution.md` |
| Ontology node definitions | `config/ontology/nodes/` |
| Ontology edge definitions | `config/ontology/edges/` |
| Edge-table routing | Edge YAML `table:` selects a table declared by `settings.edge_tables` in `config/ontology/schema.yaml`; the default is `gl_edge` |
| Ontology derived entity definitions | `config/ontology/derived/` |
| Ontology extraction SQL | Generated from the pipeline (`query: generated`) for nodes and edges; a `.sql.j2` MiniJinja template next to the YAML only for complex nodes (`config/ontology/nodes/`) and derived entities (`config/ontology/derived/`) |
| Ontology JSON schema | `config/schemas/ontology.schema.json` |
| Query language grammar (GQL) | `crates/query-engine/compiler/src/passes/frontend/gql/query.pest`; add a frontend as a sibling module under `passes/frontend/` |
| YAML document type configs | `crates/code-graph/src/v2/langs/generic/yaml/document_types/` (one file per document type, embedded via rust-embed, interpreted by `document_types.rs`) |
| YAML document type JSON schema | `config/schemas/yaml_document_type.schema.json` (configs validated against it when the code-graph YAML pipeline first loads them) |
| Named query definitions | `config/named_queries/` (parsed/embedded by `crates/named-queries`, compiled against the ontology in `orbit-server` tests, executed via gRPC `QUERY_TYPE_NAMED`, listed via gRPC `ListNamedQueries`) |
| Named query JSON schema | `config/schemas/named_query.schema.json` (validate with `mise named-queries:validate`; CI gate `named-query-schema-validate`) |
| Agent prompt files (tool descriptions) | `config/prompts/` (versioned YAML, one file per prompt; `remote/` feeds `orbit-server`, `local/` feeds `orbit-cli`; embedded via rust-embed and validated by `crates/orbit-prompts`) |
| Orbit skill sources, `orbit skills get <name> [path]` command, server contract, and build checks | `skills/orbit/` (served by GitLab Orbit), `skills/orbit-cli/` (embedded in the GitLab Orbit CLI, with references under `references/local/`), `skills/orbit-start/` (a guided first session, served unchanged in both query frontends), `skills/orbit-wrapper/` (thin wrapper that loads the composed skill from the CLI; a drift test in `crates/orbit-server/src/skills/mod.rs` keeps its description equal to `skills/orbit/`), `crates/orbit-server/src/skills/mod.rs` and gRPC `ListSkills`/`GetSkill` (whole-tree contract), `crates/orbit-cli/src/skill.rs` (resolution, cache, composition, and fallback), `crates/orbit-prompts/src/marker.rs` (shared build-time/runtime marker parser and composer), and `crates/orbit-prompts/src/skill.rs` (build-time path, link, and command validation). The trees stay standalone: `orbit-server` embeds the `skills/orbit` tree and `skills/orbit-start`, and the CLI embeds the local tree and the wrapper, which `orbit setup` installs as `orbit` |
| Server config JSON schema | `config/schemas/config.schema.json` (generated via `mise schema:generate`) |
| Query response JSON schema | `config/schemas/query_response.json` |
| Query language reference (text-indexed properties table is generated) | `docs/docs-gitlab-com/queries/query-language.md` (regenerate the ontology-derived table with `mise docs:query-language`; CI gate `query-language-docs-check`) |
| Testing strategy (test layers, CI jobs, where to add a test) | `docs/pages/engineering/testing/` |
| Query test fixtures | `fixtures/queries/` |
| YAML query scenarios (data correctness) | `crates/integration-tests/tests/server/data_correctness/scenarios/<category>/*.yaml` (run with `mise test:integration:data`; filter with `SCENARIO_FILTER=<name>`) |
| Query scenario presets | `crates/integration-tests/tests/server/data_correctness/presets/` (`seed.yaml`, `security.yaml`, `redaction.yaml`) |
| Query scenario format reference | `crates/integration-testkit/README.md` ("Query scenarios" section) and `crates/integration-testkit/src/query_scenario/format.rs` (`QueryScenario`, `QueryExpect`, `NodeExpect`) |
| YAML indexer scenarios | `crates/integration-tests/tests/indexer/scenarios/` (`sdlc/<domain>/`, `dispatch/`, `dispatch_degraded/`; JSON schema `config/schemas/indexer_scenario.schema.json`, validate with `mise scenarios:validate`; CI gate `indexer-scenario-schema-validate`; run SDLC scenarios with `mise test:integration:indexer:sdlc:scenario <filter>`) |
| Query corpus (categorized YAML) | `fixtures/queries/corpus/` (smoke-tested in CI: `corpus_smoke`) |
| Ontology overlays for speculative schema shapes | `config/seeds/overlays/<name>/` (a directory mirroring `config/ontology/`, deep-merged over it; run data correctness against one with `mise test:integration:overlay <name>`) |
| Graph DDL (ClickHouse, versioned) | `config/graph.sql` |
| Graph DDL (ClickHouse, persistent) | `config/graph_persistent.sql` (durable unversioned tables + materialized views created once at boot); `build_unversioned_definitions` in `crates/orbit-migrations/src/schema/translate.rs` builds all kinds |
| Denormalized joins (`settings.denormalized_joins` in `schema.yaml`) | `crates/ontology/src/denormalized.rs` (declarations), `crates/query-data-model/src/implementations/clickhouse/layout/derive.rs` (tables and source bindings), `crates/orbit-migrations/src/schema/translate.rs` (DDL rendering); design in the [pre-rewrite graph engine doc](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/querying/graph_engine.md#denormalized-joins) |
| Refreshable-view MiniJinja SQL templates | `config/ontology/sql/*.sql.j2` (ClickHouse SELECT templates rendered from the schema version and ontology-derived graph table metadata) |
| Pinned versions | `config/versions.yaml` (`schema` u32 bumped via `mise schema:bump`; `query_dsl`, `raw_output_format`, `toon_output_format`, `gql_output_format` semvers enforced by `scripts/checks/check-pinned-version.sh`; `gitlab_system_note_actions` upstream SHA; `vendored:` section for DuckDB and other vendored deps with sub-pins, artifact dirs, and scripts; embedded at compile time as `orbit_versions::VERSIONS`) |
| Vendored dependency system | `docs/pages/engineering/development/vendored-dependencies.md` (YAML contract, script contract, workflows); generic runner in `scripts/vendored/run.sh` |
| Graph DDL (local DuckDB) | Generated at runtime from ontology via `generate_local_tables()` + `duckdb_ddl` |
| Datalake DDL (ClickHouse) | `config/seeds/siphon.sql` |
| gRPC service definition | `crates/orbit-server/proto/orbit.proto` |
| Server config structure | `crates/orbit-server-config/src/app.rs` (`AppConfig`), `config/default.yaml` (embedded; declares every setting, no Rust fallbacks) |
| Object storage config | `config/default.yaml` (`object_storage:` section), `crates/orbit-server-config/src/object_storage.rs`, client in `crates/object-storage/src/lib.rs` |
| Query settings (timeouts, cache) | `config/default.yaml` (`query:` section), `crates/orbit-server-config/src/query.rs` |
| Server configuration reference | `docs/pages/engineering/development/server-configuration.md` |
| NATS on-call commands (nats-box) | `docs/pages/engineering/runbooks/nats.md` |
| Local development guide | `docs/pages/engineering/development/local-development.md` |
| Local development (`mise run dev`, `caproni run`) | `scripts/dev/orbit-native-dev.sh`, `scripts/dev/caproni-edit-mode.sh`, `caproni-project.yaml`, `docs/pages/engineering/development/local-development.md` |
| Operational runbooks | `docs/pages/engineering/runbooks/` |
| Architecture Decision Records | `docs/pages/engineering/adr/` |
| **All project links** (repos, epics, infra, people, Helm charts) | `README.md` (single source of truth) |
| Orbit issue, epic, and MR planning taxonomy | `/orbit-planning` skill |
| Iglu schemas (committed; codegen'd at build) | `config/schemas/iglu/<name>/<version>.json` (update via `mise vendor -- iglu`) |
| Iglu version pins | `vendored.iglu.pins` in `config/versions.yaml` (edit pin, then `mise vendor -- iglu` to fetch; check via `mise check:vendored -- iglu`) |
| Analytics event definition | `config/events/gkg_query_executed.yml` |
| Analytics contexts (Snowplow) | `crates/orbit-analytics/src/context.rs` (types), `crates/orbit-server/src/analytics/` (builders + observer) |
| Billing config + observer | `crates/orbit-billing/`, `crates/orbit-server/src/billing_adapter.rs` |
| SOX billing authoring rules | `docs/pages/engineering/development/sox-billing-boundary.md` |
| GitLab documentation principles (synced) | `.ai/principles/distilled/` (synced daily from the same path in `gitlab-org/gitlab` by `.gitlab/ci/doc-principles-sync.yml`) |
| Query profiler CLI | `crates/query-engine/profiler/`, `mise query:profile` |

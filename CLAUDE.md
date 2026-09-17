# AGENTS.md

GitLab Orbit, previously known as GitLab Knowledge Graph or "gkg". Rust service that builds a property graph from GitLab data and serves queries over gRPC/HTTP. 

## Quick start

All tasks use mise. `mise build`, `mise test:fast`, `mise test:local`, `mise lint:code`, `mise server:start`, `mise server:dispatch`.
Fix linting issues: `mise lint:code:fix`. Validate docs: `mise lint:docs`. Validate ontology: `mise ontology:validate`.
Integration tests need Docker: `mise test:integration`. Correctness subset: `mise test:integration:server`.
CLI integration tests (concurrency, worktrees): `mise test:cli`.

**Worktrees:** after creating a Git worktree, run `mise trust` and `git config core.hooksPath "$(git rev-parse --git-common-dir)/hooks"` so that lefthook and mise work correctly.

## Ignored directories

- `docs-locale/` contains machine-translated documentation. Never read, edit, or reference files under this directory. Treat it as build output.

## How the system works

- **Read-only from the GitLab perspective.** SDLC data flows via Siphon CDC (PostgreSQL logical replication → NATS → ClickHouse). GKG only writes to its own ClickHouse tables.
- **Rails owns authorization.** GKG delegates all access decisions to Rails via gRPC (traversal paths, resource permissions). See `docs/design-documents/security.md`.
- **ClickHouse = datalake + graph.** Datalake DB holds raw Siphon rows; graph DB holds indexed property graph tables. The indexer transforms between them.
- **Ontology-driven graph.** YAML in `config/ontology/nodes/`, `config/ontology/edges/`, and `config/ontology/derived/` drives ETL, query validation, redaction, and edge table routing.
  Nodes, edges, and derived entities declare `pipelines:` with `extract` and `transform` sections.
  Setting `query: generated` lets the indexer build the extract SQL from the declaration's single base table. Node projections come from database-backed properties; standalone edges declare their base projection under `extract.fields`. Optional `extract.lookups` add point-lookup CTEs whose tables resolve from their node pipelines and are not repeated in `extract.tables`.
  Nodes may declare `enrichment_props`. A slim lookup with only `node` and `id` expands its source fields from that contract. A transform endpoint with `enrich: true` independently expands its property bindings from the same contract. Extract and transform compile from their own declarations and meet through `RecordBatch` field names. Same-node references derive distinct field namespaces from their ID fields. Explicit lookup `fields` and endpoint `properties` remain escape hatches.
  An optional `extract.filter` adds a `_batch` predicate (e.g. `state = 5`) and may use `{{version_column}}`/`{{deleted_column}}`.
  Seven complex nodes plus the SystemNote derived entity keep a `.sql.j2` MiniJinja template next to the YAML. The nodes are Group, Project, MergeRequest, Commit, MergeRequestDiffFile, PackageFile, and Finding. All ontology SQL templates render through `ontology::sql_template`. Derived pipelines are always authored SQL. Their rows are neither node properties nor edge endpoints, so no projection can be generated from them.
  New entity types start in the ontology, not in Rust.
  Edge YAML `table:` field + `settings.edge_tables` in `schema.yaml` control which physical table each relationship type writes to and queries from (default: `gl_edge`).
  `settings.denormalized_joins` declares linear chains of tables pre-joined into `gl_denorm_<name>` tables. These tables are composed from the source tables' DDL and fed by materialized views (`crates/ontology/src/denormalized.rs`, `passes/codegen/ddl/denormalized.rs`). Each declaration is a schema bump.
  Unversioned objects are durable tables and materialized views. They are created once at boot, and never version-prefixed or GCed. One `generate_unversioned_objects` path in `crates/query-engine/compiler/src/passes/codegen/ddl/` emits them. Add a new unversioned kind there, not a parallel per-kind generator.
  Schema: `config/schemas/ontology.schema.json`.
- **Schema archives.** Migration and promotion require usable ontology archives. The dispatcher bootstraps missing active archives from the build-validated release bundle; unsupported missing versions fail closed. The Webserver serves the active archive and pins one schema snapshot per request. See `docs/design-documents/schema_management.md` for readiness, supported legacy upgrades, and rollback retention.
- **Orbit query frontend.** `crates/query-engine/compiler/src/passes/frontend/` holds one module per query language; both lower graph queries to language-neutral `Input`. `compiler::gql::prepare` parses once and dispatches MATCH to the complete shared graph compilation phases. Standalone `CALL db.schema(...)` resolves typed ontology metadata inside the GQL frontend without SQL or shared schema state. `compiler::compile` remains graph-query-only. Remote requests still use JSON. See `docs/design-documents/querying/orbit_query_frontend.md`.
- **Agent-facing prompts are YAML.** Tool and command descriptions live as versioned YAML under `config/prompts/` (`remote/` = server, `local/` = CLI). They embed via rust-embed, and a build-time check in `orbit-prompts` validates them.
- **Single binary, four modes.** `gkg-server --mode` runs as Webserver, Indexer, DispatchIndexing, or HealthCheck.
- **Layered configuration.** `AppConfig` in `crates/orbit-server-config/` loads four sources, lowest to highest priority. First is the embedded `config/default.yaml`, compiled in via `include_str!`. Second is an on-disk `config/default.yaml` when present, the Helm ConfigMap key. Third is an overlay file (`--config <path>`, else `config/config.yaml` when present). Fourth is the K8s secret files from `/etc/secrets/`. There is no environment-variable layer. The mise dev tasks generate `.dev/<mode>.yaml` from three inputs, then pass that one file to `--config`. The inputs are `config/dev.yaml`, GDK-derived connection details, and the Git-ignored `config/dev.local.yaml`.
  `config/default.yaml` is the single source of truth for defaults. Every section and scalar is declared there. The Rust structs have no `Default` impls or `serde(default)` fallbacks. Only `Option` fields and empty collections may be omitted. Add a setting by adding the struct field plus its value in `default.yaml`; tests start from `AppConfig::embedded_defaults()`. The CLI (`orbit`) has its own clap-based config and does not use `AppConfig`. See `docs/dev/runbooks/server_configuration.md`.
- **Vendored dependencies.** The repo commits some upstream artifacts, such as DuckDB FTS sources and extension binaries. The `vendored:` section of `config/versions.yaml` pins them with sub-pins, artifact directories, and vendor/check scripts. A generic runner (`scripts/vendored/run.sh`) invokes them with standardized `VENDOR_*` env vars. Vendor scripts write computed checksums back via `yq -i`; check scripts are read-only. Run `mise vendor -- <name>` to regenerate, `mise check:vendored -- <name>` to verify. See `docs/dev/runbooks/vendored_dependencies.md`.
- **FIPS by default.** `gkg-server` links the AWS-LC FIPS module, and refuses to start outside FIPS mode (`crates/orbit-server/src/fips.rs`). The link uses the `rustls` `fips` feature, the `jsonwebtoken` `aws_lc_rs` backend, and `async-nats` and `kube` without `ring`. There is no non-FIPS server variant; the `orbit` CLI is exempt. `scripts/check-fips-graph.sh` and `scripts/check-fips-binary.sh` are the gates. See `docs/design-documents/security.md`.
- **Siphon and NATS are external.** [Siphon](https://gitlab.com/gitlab-org/analytics-section/siphon) (Go, Analytics team) and NATS are consumed, not owned. Use `/related-repositories` for local checkouts.

## What CI enforces

- `AGENTS.md` and `CLAUDE.md` must be identical (`agent-file-sync-check`).
- Clippy with all features, warnings as errors (`lint-check`).
- Ontology YAML validated against JSON schema (`ontology-schema-validate`).
- Named query YAML validated against JSON schema (`named-query-schema-validate`); each query is also compiled against the ontology by `orbit-server`'s build script, so drift fails every build.
- Versions YAML validated against JSON schema (`versions-schema-validate`); enforces key patterns, hex lengths, path restrictions, and vendored dependency structure.
- Assistant setup specs and mode texts in `config/setup/` validated against JSON schema (`setup-schema-validate`).
- Migration ledger validated and scope-checked (`migration-ledger-schema-validate`, `migration-ledger-check`, plus `orbit-server` build-time drift checks); full ledger rules in `docs/design-documents/schema_management.md`.
- `cargo fmt` (`fmt-check`).
- Trailing newlines (`newline-check`, run locally with `mise lint:newlines`).
- Prose that LLMs read passes the prose lint: prompts, the setup block, skills, agent guides, the glossary, design docs, runbooks, and templates (`lint:prose`, advisory; run locally with `mise run lint:prose -- --all`).
- `cargo shear` detects unused workspace and crate dependencies (`unused-deps-check`).
- `cargo audit`, `cargo deny`, `cargo geiger` (security stage).
- Server dependency graph and binary link the AWS-LC FIPS module and no `ring`; the CLI graph stays non-FIPS (`fips-check`).
- Unit tests via nextest (`unit-test`).
- Compiler integration tests: query compilation, ontology validation, pipeline infra (`compiler-integration-test`).
- CLI integration tests: concurrency, worktrees, content resolution (`cli-integration-test`).
- Integration tests with Docker testcontainers (`integration-test`, `integration-test-data-correctness`); data correctness tests are YAML-driven scenarios under `crates/integration-tests/tests/server/data_correctness/scenarios/` (format reference in `crates/integration-testkit/README.md`).
- MR titles must follow conventional commit format: `type(scope): description` (`mr-title-check`).
- `rust-toolchain.toml` must match `mise.toml` (`rust-toolchain-sync-check`; regenerate with `mise toolchain:generate`).
- Markdown files must pass markdownlint, Vale, and lychee checks (`check_docs_markdown`).
- Pins in `config/versions.yaml` bumped when their covered files change: query DSL, RAW response format, GOON format (`pinned-version-check`, one job reporting every stale pin at once).
- Skill version bumped when files under `skills/<name>/` change (`skill-version-bump-check`).
- Prompt version bumped when files under `config/prompts/` change (`prompt-version-bump-check`).
- Metrics catalog regenerated in sync with `orbit-observability` source (`metrics-catalog-check`).
- Query-language text-indexed properties table regenerated in sync with the ontology (`query-language-docs-check`).
- Vendored Iglu schemas match pinned versions and live Iglu server (`iglu-schema-check`; pins in `vendored.iglu.pins`, regenerate with `mise vendor -- iglu`).
- Vendored system-note action list matches upstream Rails `ICON_TYPES` at the pinned SHA (`system-note-actions-check`; pin in `vendored.gitlab_system_note_actions.version`).
- The vendored DuckDB FTS source archive matches its pinned upstream revisions (`duckdb-fts-sources-sync-check`; regenerate with `mise vendor -- duckdb`).
- Every `[workspace]` member has a row in `docs/dev/agents-crate-map.md`, and no stale rows remain (`crates/xtask/build.rs`, so any workspace build/clippy fails on drift).

## Where to find things

Full reference index: [`docs/dev/agents-reference-index.md`](docs/dev/agents-reference-index.md).
Key locations: domain glossary in `CONTEXT.md`, indexer guide in `crates/indexer/AGENTS.md`, architecture in `docs/design-documents/`.

## Crate map

Single binary: `gkg-server` (4 modes: Webserver, Indexer, DispatchIndexing, HealthCheck via `--mode`). Full crate descriptions: [`docs/dev/agents-crate-map.md`](docs/dev/agents-crate-map.md).

## Code quality

- **Do not write narration comments, including in tests.** A comment must explain *why*. Good reasons are a non-obvious constraint, a gotcha, or an ADR/issue link. A comment must never restate *what* the next line does. The most common leak is a label on each test or setup block. Those labels are narration, so delete them. The test name and the `assert_eq!` already say what is being checked. Write clean as you go. Clean-as-you-go alone has proven insufficient. So run a final narration-comment pass over the comments your change added or modified. Run it before you push or open an MR. Discriminator:
  - ❌ `// Test cross_reference with WorkItem` above `assert_eq!(route("cross_reference", "WorkItem"), Some("MENTIONS"));`: restates the call.
  - ❌ `// merged with WorkItem should return None` above `assert_eq!(route("merged", "WorkItem"), None);`: restates the assertion.
  - ❌ `// Clear env vars` / `// Cleanup` / `// Setup`: block labels for self-evident code.
  - ✅ `// merged.yaml only declares User → MergeRequest, so a WorkItem noteable must drop.`: explains an invariant the code does not.
  - ✅ `// Insert the stale row second so argMax (not row order) must resolve it.`: explains intent a reader can't infer.
  - If a comment would survive deleting it without losing *why* information, delete it. The `/remove-llm-comments` skill drives that final pass; it is a backstop for what slipped through, not a license to narrate first.
- **Reuse existing infrastructure before writing new code.** First do an explicit pass: what does the codebase already give me? Check for a cursor or checkpoint, Arrow helpers, ontology-derived specs, SQL filtering, and concurrency. Do this before you scaffold a new handler, pipeline, or module. Reinventing infra the codebase already provides is the most common class of preventable review feedback. For the indexer, see the checklist in **`crates/indexer/AGENTS.md`**. For code-graph, reuse existing types and constructors in the language module. Examples are `CanonicalDefinition` in `src/v2/types/` and the DSL engine helpers in `src/v2/dsl/`. Do not duplicate construction logic per language.
- **No `#[allow(dead_code)]` in shipped code.** Production (non-test) modules must not ship dead-code allows to silence scaffold warnings. If a symbol is test-only, gate it with `#[cfg(test)]`; if it is genuinely unused, delete it. Reserve exceptions for an explicit, justified case: use `#[allow(dead_code, reason = "…")]` (ideally linking an issue) or, preferably, `#[expect(dead_code, reason = "…")]`, which fails once the code is used and self-cleans. The `indexer` and `code-graph` crates enforce this mechanically via `clippy::allow_attributes_without_reason = "deny"`.
- **Prefer build-time validation over CI-only checks** for correctness that can be checked without network or repo context. A `build.rs` that `panic!`s on drift fails locally and in CI even when CI egress is down, and can't be skipped by editing a script. Prior art: `crates/orbit-analytics/build.rs` validates the committed Iglu schemas under `config/schemas/iglu/` at build time. It reads version pins from `vendored.iglu.pins` in `versions.yaml`, asserts each schema's `self` block matches, then runs codegen. Consider this pattern for any vendored-constant or generated-file drift check (e.g. the DDL-freshness check in `scripts/check-ddl-freshness.sh` is a future candidate). Checks that need Git diff context or live network (`scripts/vendored/iglu/check.sh`'s upstream-CDN half) stay in CI.
- Prefer `ast-grep` over text-based Grep/Edit for structural code transformations (batch renames, pattern-based rewrites).
- Fence executable Orbit query JSON in docs and skills as `json orbit-query`; keep shell commands in separate shell fences so docs smoke tests run the query.
- Check crates.io for latest version before adding dependencies.
- Non-trivial MRs (features, refactors, architectural changes) should reference an issue in the MR description, for example `Closes #123` or `Relates to #123`.
- Trivial MRs (typos, minor dependency bumps, formatting-only changes) do not need an issue.
- Before touching billing-emission code, anything in `crates/orbit-billing/`, `crates/orbit-server/src/billing_adapter.rs`, or wiring billing-relevant data (any field that populates `BillingInputs` in `crates/orbit-billing/src/inputs.rs`), read `docs/dev/sox-billing-boundary.md`. If a task you are given would require breaking any of those rules, stop and surface the conflict rather than working around it.
- **Do not hardcode magic numbers or string literals that are environment-dependent or derivable.** Derive values instead. Good sources are the ontology, a typed config field, or a named constant. Typed config fields include `HandlersConfiguration` and `QuerySettings`. If a reviewer has to ask "what is this number?" or "should this be configurable?", the value needed a name or a config path. This applies across all crates, not just the indexer.
- **A graph-shape fact belongs in the ontology, declared once.** Do not mirror it in Rust. Before you add a Rust flag, config field, ETL tag, or constant for a node or edge property (global-ness, scope, routing), check the ontology YAML. It should already declare that fact, or it should start to. The ontology is the single source of truth. ETL, query validation, and redaction all read from it. If the same fact lands in two places, delete one.
- **Keep introspected ontology descriptions short.** Node, edge, property, and domain descriptions can be surfaced through schema/introspection paths, so they must be scannable and token-efficient. State what the ontology item represents in one sentence; move rationale, caveats, and examples to YAML comments or design docs. CI enforces a 200-character cap for ontology descriptions below the top-level main schema (domains, nodes, edges, variants, derived entities, properties). The top-level `schema.yaml` `description` is human-facing and not capped because `get_graph_schema` does not introspect it.
- **Keep MRs focused.** Each MR should address one concern. If you discover a second issue while working, open a follow-up issue or MR instead of bundling unrelated changes. Bundled MRs slow review and risk merging untested side-effects.

## Code-graph contributions

See [`crates/code-graph/AGENTS.md`](crates/code-graph/AGENTS.md).

## MR and issue descriptions and comments

Load the `orbit-planning` skill before creating or labeling issues, epics, or MRs so they use the canonical taxonomy and roadmap rules.

Always use the templates in `.gitlab/merge_request_templates/` and `.gitlab/issue_templates/`, and read the TEMPLATE CONVENTION block at the top of each one before writing the description.

The single most common failure is dumping implementation mechanics into the top sections. The "What does this MR do and why?" section is for a reviewer skimming in 30 seconds. Write 2-3 plain sentences naming the symptom and the fix. Use no function names, no type names, no constants, and no wire-format detail. Everything mechanical goes in the Agent context block, never above it. That includes function, type, and constant names, encoder traces, file-by-file walkthroughs, and alternatives considered. If the headline section has more than three backticked identifiers, you are writing at the wrong level. Move it down.

Comments have no template. These are MR or issue threads and review replies. So apply the convention by hand. Lead with the verdict in a few human sentences. Push long-form reasoning into a collapsed `<details><summary>Agent context</summary>` block, but only when it helps. Drop AI tells.

## Design docs

Design docs live in `docs/design-documents/` and must describe the current repository state, not an aspirational or legacy architecture.

**Rules:**

- **When you change behavior covered by a design doc, update that design doc in the same MR.** Do not leave design-doc cleanup for later.
- **Design-doc and file sync on structural changes.** You may add, remove, rename, or substantially repurpose a subsystem. The same holds for a runtime mode, crate, schema shape, or external dependency. Update the relevant design docs and this file in the same MR.
- **Prefer as-built descriptions over historical ones.** If the code no longer matches a section, rewrite or remove the stale section. Do not leave contradictory text in place.
- **Treat these files as sync points:**
  - `docs/design-documents/README.md` for the high-level architecture and current system state.
  - `docs/design-documents/data_model.md` for implemented entities and relationships.
  - `docs/design-documents/indexing/` for indexing flow and runtime modes.
  - `docs/design-documents/querying/` for query surface, DSL, and response shape.
  - `AGENTS.md` / `CLAUDE.md` for agent-facing architecture summaries and doc-sync rules.
  - `docs/dev/agents-crate-map.md` for the crate inventory.
  - `docs/dev/agents-reference-index.md` for the file/schema/config reference index.
- **If your MR changes the architecture but no design doc changed**, assume the documentation is incomplete and fix it before merging.
- **When you introduce a new domain concept**, check `CONTEXT.md`. Add or update the term if it's missing. A domain concept is a new node type, relationship type, query feature, or pipeline concept. Only add terms that are domain-specific and would confuse a new team member, not implementation details.
- **Before writing documentation, design docs, or MR descriptions, consult `CONTEXT.md` for canonical terminology.** Use the canonical terms, not the aliases listed under _Avoid_.

# AGENTS.md

GitLab Knowledge Graph (Orbit). Rust service that builds a property graph from GitLab data and serves queries over gRPC/HTTP. 

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
  Setting `query: generated` builds the extract SQL from the declaration; entities whose extraction owns multi-table joins or computed columns carry an authored query object (`select`/`from`/`where`, optional `page_join`) instead.
  An optional `extract.filter` adds a source-scan predicate for standalone edges (e.g. `state = 5`).
  New entity types start in the ontology, not in Rust.
  Edge YAML `table:` field + `settings.edge_tables` in `schema.yaml` control which physical table each relationship type writes to and queries from (default: `gl_edge`).
  Schema: `config/schemas/ontology.schema.json`.
- **Single binary, four modes.** `gkg-server --mode` runs as Webserver, Indexer, DispatchIndexing, or HealthCheck.
- **Layered configuration.** `AppConfig` in `crates/gkg-server-config/` loads three sources (lowest to highest priority): `config/default.yaml`, K8s secret files from `/etc/secrets/`, and `GKG_*` environment variables (`__` separates nested keys, e.g. `GKG_GRAPH__DATABASE`). The CLI (`orbit`) has its own clap-based config and does not use `AppConfig`. See `docs/dev/runbooks/server_configuration.md` for full reference.
- **Siphon and NATS are external.** [Siphon](https://gitlab.com/gitlab-org/analytics-section/siphon) (Go, Analytics team) and NATS are consumed, not owned. Use `/related-repositories` for local checkouts.

## What CI enforces

- `AGENTS.md` and `CLAUDE.md` must be identical (`agent-file-sync-check`)
- Clippy with all features, warnings as errors (`lint-check`)
- Ontology YAML validated against JSON schema (`ontology-schema-validate`)
- Named query YAML validated against JSON schema (`named-query-schema-validate`); each query is also compiled against the ontology by `gkg-server`'s build script, so drift fails every build
- Migration ledger validated and scope-checked (`migration-ledger-schema-validate`, `migration-ledger-check`, plus `gkg-server` build-time drift checks); full ledger rules in `docs/design-documents/schema_management.md`
- `cargo fmt` (`fmt-check`)
- `cargo shear` detects unused workspace and crate dependencies (`unused-deps-check`)
- `cargo audit`, `cargo deny`, `cargo geiger` (security stage)
- Unit tests via nextest (`unit-test`)
- Compiler integration tests: query compilation, ontology validation, pipeline infra (`compiler-integration-test`)
- CLI integration tests: concurrency, worktrees, content resolution (`cli-integration-test`)
- Integration tests with Docker testcontainers (`integration-test`)
- MR titles must follow conventional commit format: `type(scope): description` (`mr-title-check`)
- `rust-toolchain.toml` must match `mise.toml` (`rust-toolchain-sync-check`; regenerate with `mise toolchain:generate`)
- Markdown files must pass markdownlint, Vale, and lychee checks (`check_docs_markdown`)
- Response format version bumped when formatter code or response schema changes (`response-schema-version-check`)
- GOON format version bumped when GOON encoder or shared formatter code changes (`goon-format-version-check`)
- Skill version bumped when files under `skills/<name>/` change (`skill-version-bump-check`)
- Metrics catalog regenerated in sync with `gkg-observability` source (`metrics-catalog-check`)
- Query-language text-indexed properties table regenerated in sync with the ontology (`query-language-docs-check`)
- Vendored Iglu schemas match pinned versions and live Iglu server (`iglu-schema-check`)
- Vendored system-note action list matches upstream Rails `ICON_TYPES` at the pinned SHA (`system-note-actions-check`)
- Every `[workspace]` member has a row in `docs/dev/agents-crate-map.md`, and no stale rows remain (`crates/xtask/build.rs`, so any workspace build/clippy fails on drift)

## Where to find things

Full reference index: [`docs/dev/agents-reference-index.md`](docs/dev/agents-reference-index.md).
Key locations: domain glossary in `CONTEXT.md`, indexer guide in `crates/indexer/AGENTS.md`, architecture in `docs/design-documents/`.

## Crate map

Single binary: `gkg-server` (4 modes: Webserver, Indexer, DispatchIndexing, HealthCheck via `--mode`). Full crate descriptions: [`docs/dev/agents-crate-map.md`](docs/dev/agents-crate-map.md).

## Code quality

- **Do not write narration comments — including in tests.** A comment must explain *why* (a non-obvious constraint, a gotcha, an ADR/issue link), never restate *what* the next line does. The most common leak is a label on each test or setup block; those are narration, delete them. The test name and the `assert_eq!` already say what is being checked. Write clean as you go. Clean-as-you-go alone has proven insufficient, so a final narration-comment pass over the comments your change added or modified is required before merge — run it before pushing or opening an MR. Discriminator:
  - ❌ `// Test cross_reference with WorkItem` above `assert_eq!(route("cross_reference", "WorkItem"), Some("MENTIONS"));` — restates the call.
  - ❌ `// merged with WorkItem should return None` above `assert_eq!(route("merged", "WorkItem"), None);` — restates the assertion.
  - ❌ `// Clear env vars` / `// Cleanup` / `// Setup` — block labels for self-evident code.
  - ✅ `// merged.yaml only declares User → MergeRequest, so a WorkItem noteable must drop.` — explains an invariant the code does not.
  - ✅ `// Insert the stale row second so argMax (not row order) must resolve it.` — explains intent a reader can't infer.
  - If a comment would survive deleting it without losing *why* information, delete it. The `/remove-llm-comments` skill drives that final pass; it is a backstop for what slipped through, not a license to narrate first.
- **Reuse existing infrastructure before writing new code.** Before scaffolding a new handler, pipeline, or module, do an explicit "what does the codebase already give me?" pass (cursor/checkpoint, Arrow helpers, ontology-derived specs, SQL filtering, concurrency). Reinventing infra the codebase already provides is the most common class of preventable review feedback. For the indexer, see the checklist in **`crates/indexer/AGENTS.md`**. For code-graph, prefer reusing existing types and constructors in the language module (e.g. `CanonicalDefinition` in `src/v2/types/`, the DSL engine helpers in `src/v2/dsl/`) rather than duplicating construction logic per language.
- **No `#[allow(dead_code)]` in shipped code.** Production (non-test) modules must not ship dead-code allows to silence scaffold warnings. If a symbol is test-only, gate it with `#[cfg(test)]`; if it is genuinely unused, delete it. Reserve exceptions for an explicit, justified case: use `#[allow(dead_code, reason = "…")]` (ideally linking an issue) or, preferably, `#[expect(dead_code, reason = "…")]`, which fails once the code is used and self-cleans. The `indexer` and `code-graph` crates enforce this mechanically via `clippy::allow_attributes_without_reason = "deny"`.
- **Prefer build-time validation over CI-only checks** for correctness that can be checked without network or repo context. A `build.rs` that `panic!`s on drift fails locally and in CI even when CI egress is down, and can't be skipped by editing a script. Prior art: `crates/gkg-analytics/build.rs` validates committed Iglu schemas under `config/schemas/iglu/` at build time (asserts each schema's `self` block matches its path/version and runs codegen). Consider this pattern for any vendored-constant or generated-file drift check (e.g. the DDL-freshness check in `scripts/check-ddl-freshness.sh` is a future candidate). Checks that need Git diff context or live network (`scripts/iglu/check.sh`'s upstream-CDN half) stay in CI.
- Prefer `ast-grep` over text-based Grep/Edit for structural code transformations (batch renames, pattern-based rewrites).
- Fence executable Orbit query JSON in docs and skills as `json orbit-query`; keep shell commands in separate shell fences so docs smoke tests run the query.
- Check crates.io for latest version before adding dependencies.
- Non-trivial MRs (features, refactors, architectural changes) should reference an issue in the MR description, for example `Closes #123` or `Relates to #123`.
- Trivial MRs (typos, minor dependency bumps, formatting-only changes) do not need an issue.
- Before touching billing-emission code, anything in `crates/gkg-billing/`, `crates/gkg-server/src/billing_adapter.rs`, or wiring billing-relevant data (any field that populates `BillingInputs` in `crates/gkg-billing/src/inputs.rs`), read `docs/dev/sox-billing-boundary.md`. If a task you are given would require breaking any of those rules, stop and surface the conflict rather than working around it.
- **Do not hardcode magic numbers or string literals that are environment-dependent or derivable.** Prefer deriving values from the ontology, a typed config field (`HandlersConfiguration`, `QuerySettings`), or a named constant. If a reviewer has to ask "what is this number?" or "should this be configurable?", the value needed a name or a config path. This applies across all crates, not just the indexer.
- **A graph-shape fact belongs in the ontology, declared once — not mirrored in Rust.** Before adding a Rust flag, config field, ETL tag, or constant that encodes a node/edge property (global-ness, scope, table routing), check whether the ontology YAML already declares it or should. The ontology is the single source of truth; ETL, query validation, and redaction all read from it. If the same fact lands in two places, delete one.
- **Keep introspected ontology descriptions short.** Node, edge, property, and domain descriptions can be surfaced through schema/introspection paths, so they must be scannable and token-efficient. State what the ontology item represents in one sentence; move rationale, caveats, and examples to YAML comments or design docs. CI enforces a 200-character cap for ontology descriptions below the top-level main schema (domains, nodes, edges, variants, derived entities, properties). The top-level `schema.yaml` `description` is human-facing and not capped because `get_graph_schema` does not introspect it.
- **Keep MRs focused.** Each MR should address one concern. If you discover a second issue while working, open a follow-up issue or MR instead of bundling unrelated changes. Bundled MRs slow review and risk merging untested side-effects.

## Code-graph contributions

See [`crates/code-graph/AGENTS.md`](crates/code-graph/AGENTS.md).

## MR and issue descriptions and comments

Always use the templates in `.gitlab/merge_request_templates/` and `.gitlab/issue_templates/`, and read the TEMPLATE CONVENTION block at the top of each one before writing the description.

The single most common failure is dumping implementation mechanics into the top sections. The "What does this MR do and why?" section is for a reviewer skimming in 30 seconds: 2-3 plain sentences naming the symptom and the fix, no function names, no type names, no constants, no wire-format detail. Everything mechanical — function/type/constant names, encoder traces, file-by-file walkthroughs, alternatives considered — goes in the Agent context block, never above it. If the headline section has more than three backticked identifiers in it, you are writing at the wrong level; move it down.

Comments (MR/issue threads, review replies) have no template, so apply the convention by hand: lead with the verdict in a few human sentences, push long-form reasoning into a collapsed `<details><summary>Agent context</summary>` block (only when it helps), and drop AI tells.

## Design docs

Design docs live in `docs/design-documents/` and must describe the current repository state, not an aspirational or legacy architecture.

**Rules:**

- **When you change behavior covered by a design doc, update that design doc in the same MR.** Do not leave design-doc cleanup for a later follow-up.
- **When you add, remove, rename, or substantially repurpose a subsystem, runtime mode, crate, schema shape, or external dependency, update the relevant design docs and this file in the same MR.**
- **Prefer as-built descriptions over historical ones.** If the code no longer matches a section, rewrite or remove the stale section instead of leaving contradictory text in place.
- **Treat these files as sync points:**
  - `docs/design-documents/README.md` for the high-level architecture and current system state
  - `docs/design-documents/data_model.md` for implemented entities and relationships
  - `docs/design-documents/indexing/` for indexing flow and runtime modes
  - `docs/design-documents/querying/` for query surface, DSL, and response shape
  - `AGENTS.md` / `CLAUDE.md` for agent-facing architecture summaries and doc-sync rules
  - `docs/dev/agents-crate-map.md` for the crate inventory
  - `docs/dev/agents-reference-index.md` for the file/schema/config reference index
- **If your MR changes the architecture but no design doc changed, assume the documentation is incomplete and fix it before merging.**
- **When you introduce a new domain concept** (new node type, relationship type, query feature, pipeline concept), check `CONTEXT.md` and add or update the term if it's missing. Only add terms that are domain-specific and would confuse a new team member — not implementation details.
- **Before writing documentation, design docs, or MR descriptions, consult `CONTEXT.md` for canonical terminology.** Use the canonical terms, not the aliases listed under _Avoid_.

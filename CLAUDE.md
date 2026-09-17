# AGENTS.md

GitLab Orbit (formerly GitLab Knowledge Graph or GKG) is a Rust service that builds a property graph from GitLab data and serves queries over gRPC/HTTP.

## Start here

All tasks use mise. Common entry points: `mise build`, `mise test:fast`, `mise test:local`, `mise lint:code`, `mise lint:code:fix`, `mise lint:docs`, `mise ontology:validate`, `mise server:start`, and `mise server:dispatch`. Docker-backed gates are `mise test:integration` and `mise test:integration:server`; CLI integration tests use `mise test:cli`.

After creating a worktree, run `mise trust` and `git config core.hooksPath "$(git rev-parse --git-common-dir)/hooks"`.

Never read, edit, or reference `docs-locale/`; it is machine-generated output.

Use these discovery paths instead of expanding this file:

- `docs/dev/agents-reference-index.md`: canonical files, schemas, configs, and tools
- `docs/dev/agents-crate-map.md`: crate inventory
- `CONTEXT.md`: canonical domain terminology
- `docs/design-documents/`: as-built architecture
- `crates/indexer/AGENTS.md` and `crates/code-graph/AGENTS.md`: subsystem rules

## Non-obvious architecture invariants

- Orbit is read-only toward GitLab. Siphon streams PostgreSQL changes through NATS into a ClickHouse datalake of raw rows; the indexer transforms them into indexed tables in a separate ClickHouse graph database, Orbit's only write target.
- Rails owns authorization. Orbit delegates traversal-path and resource-permission decisions to Rails over gRPC. See `docs/design-documents/security.md`.
- The ontology under `config/ontology/` is the single source of truth for graph shape, ETL, query validation, redaction, and edge-table routing. New entities and graph-shape facts start there, not in Rust. See `crates/indexer/AGENTS.md` for pipeline authoring.
- Schema migration, promotion, rollback, and request-time snapshots require usable ontology archives and fail closed when a supported archive is missing. See `docs/design-documents/schema_management.md`.
- `config/default.yaml` declares every server setting. Configuration layers are embedded defaults, an optional on-disk `config/default.yaml`, one overlay, then `/etc/secrets/`; there is no environment-variable layer. Add settings to both the typed config and `config/default.yaml`. The `orbit` CLI has separate clap configuration. See `docs/dev/runbooks/server_configuration.md`.
- `gkg-server` has one FIPS-only dependency graph and refuses to start outside AWS-LC FIPS mode; only the `orbit` CLI is exempt. See `docs/design-documents/security.md`.

## CI rules that are easy to miss

- Keep `AGENTS.md` and `CLAUDE.md` byte-identical.
- Schema, setup, named-query, ontology, and migration-ledger files have build-time or CI validation; use the corresponding mise validation task before hand-editing these files.
- Changes covered by pins in `config/versions.yaml` must bump the relevant pin. Changes under `skills/<name>/` or `config/prompts/` must bump that skill or prompt version.
- Generated artifacts checked in CI include the metrics catalog, query-language property table, vendored Iglu schemas, system-note actions, and DuckDB FTS sources. Follow their entries in `docs/dev/agents-reference-index.md` and the failing job's regeneration command.
- Every workspace member needs a row in `docs/dev/agents-crate-map.md`; `crates/xtask/build.rs` enforces this.
- Markdown must pass markdownlint, Vale, and lychee. Run `mise lint:docs` and `mise lint:newlines`.
- The server dependency graph must contain AWS-LC FIPS and no `ring`; the CLI graph remains non-FIPS.

## Code and review rules

- Comments explain non-obvious reasons, never narrate setup, calls, or assertions. Run the `/remove-llm-comments` skill as a final pass over comments you changed.
- Reuse existing infrastructure before adding handlers, pipelines, helpers, or constructors. For indexer changes, complete the checklist in `crates/indexer/AGENTS.md`; for code graph, reuse its existing types and DSL helpers.
- Do not ship bare `#[allow(dead_code)]`. Delete unused code, gate test-only code with `#[cfg(test)]`, or use a justified `#[expect(..., reason = "...")]` when an exception is unavoidable.
- Prefer build-time validation over CI-only checks when validation needs neither network nor Git diff context.
- Prefer `ast-grep` for structural multi-site rewrites. Check crates.io for the latest release before adding a dependency.
- Do not hardcode environment-dependent or derivable values. Use ontology data, typed configuration, or a named constant.
- Keep introspected ontology descriptions to one sentence and at most 200 characters below the top-level schema.
- Before changing billing emission or anything that populates `BillingInputs`, read `docs/dev/sox-billing-boundary.md`; stop rather than bypassing those rules.
- Fence executable Orbit query JSON as `json orbit-query`, with shell commands in separate fences.
- Keep each MR focused. Non-trivial changes must reference an issue; trivial typo, formatting, and minor dependency-only MRs need not. Follow the MR-title format in `CONTRIBUTING.md`.

## GitLab and documentation

Before creating or labeling an issue, epic, or MR, load `/orbit-planning`. Use the repository templates and obey their `TEMPLATE CONVENTION` block. Keep the reviewer summary to 2-3 plain sentences; put implementation details in the Agent context block. Public comments should lead with the verdict and hide lengthy reasoning in a collapsed Agent context block when useful.

Design docs describe the current system. Update the relevant design doc in the same MR whenever behavior or architecture changes; use `docs/dev/agents-reference-index.md` to find the sync point. When adding, removing, or renaming a subsystem, runtime mode, crate, or external dependency, update `AGENTS.md`, `CLAUDE.md`, and `docs/dev/agents-crate-map.md` in the same MR. Check `CONTEXT.md` before documentation and add only genuinely domain-specific terminology.

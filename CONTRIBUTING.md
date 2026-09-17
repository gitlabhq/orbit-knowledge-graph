# Contributing to Orbit

Thanks for contributing to [GitLab Orbit](https://docs.gitlab.com/orbit/), the service
that turns GitLab SDLC data and source code into a queryable property graph.

Community contributions go through the [community fork](https://gitlab.com/gitlab-community/gitlab-org/orbit/knowledge-graph).
GitLab team members contribute directly to this repository.

## Quickstart

Install [mise](https://mise.jdx.dev/), then:

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise install
mise build
```

Core tasks:

| Task | Command |
|---|---|
| Build | `mise build` |
| Unit + fast tests | `mise test:fast` |
| Lint | `mise lint:code` |
| Apply lint fixes | `mise lint:code:fix` |
| Validate doc changes | `mise lint:docs` |

For the full local setup (GDK, ClickHouse, NATS), see [Local development](docs/dev/local-development.md).

## Testing

```shell
mise test:fast         # Unit tests and fast integration tests (no Docker required)
mise test:integration  # Full integration suite (requires Docker)
mise test:cli          # CLI integration tests: concurrency and worktrees
```

`mise test:integration` spins up ClickHouse via Docker testcontainers — make sure Docker is running
before using it.

## Linting

```shell
mise lint:code        # Clippy with warnings as errors
mise lint:code:fix    # Apply auto-fixable Clippy suggestions
mise lint:docs        # markdownlint + Vale + lychee link checks
```

Run `mise lint:docs` before pushing any documentation changes.

## Engineering conventions

- Explain only reasons that the code cannot show. Do not narrate setup, calls, or assertions in comments.
- Run the `/remove-llm-comments` skill on comments you changed before you push.
- Reuse existing handlers, pipelines, helpers, and constructors before you add new ones.
- Read `crates/indexer/AGENTS.md` before indexer work. Read `crates/code-graph/AGENTS.md` before Code Graph work.
- Do not ship bare `#[allow(dead_code)]`. Delete unused code or use `#[cfg(test)]` for test-only code.
- Use a justified `#[expect(dead_code, reason = "...")]` only when an exception is necessary.
- Put local validation in `build.rs` when it needs no network or Git diff context.
- Use `ast-grep` for structural changes across multiple sites.
- Check crates.io for the latest release before you add a dependency.
- Derive environment-dependent values from the ontology or typed settings. Use named constants for other shared values.
- Put each graph-shape fact in the ontology. Do not copy that fact into Rust settings or constants.
- Limit introspected ontology descriptions to one sentence and 200 characters. The top-level schema description has no limit.
- Read `docs/dev/sox-billing-boundary.md` before you change billing emission or populate `BillingInputs`.
- Fence executable Orbit query JSON as `json orbit-query`. Put shell commands in separate shell fences.

## Documentation conventions

Design documents describe the current system. Update an owning design document when its behavior changes.

Use these synchronization points:

| Change | Update |
| --- | --- |
| High-level architecture or current system state | `docs/design-documents/README.md` |
| Entities or relationships | `docs/design-documents/data_model.md` |
| Indexing flow or runtime modes | `docs/design-documents/indexing/` |
| Query surface, DSL, or response shape | `docs/design-documents/querying/` |
| Crates | `docs/dev/agents-crate-map.md` |
| Canonical files, schemas, or settings | `docs/dev/agents-reference-index.md` |

Update all affected synchronization points in the same MR. This rule covers subsystem, runtime mode, crate, schema shape, and external dependency changes.

Rewrite or remove stale design text instead of preserving history.

Read `CONTEXT.md` before you write docs or MR text. Add only domain terms that can confuse a new contributor.

## MR conventions

MR titles must follow [Conventional Commits](https://www.conventionalcommits.org/) format:

```plaintext
type(scope): short description
```

Examples: `fix(compiler): correct aggregation undercount`, `docs: add CONTRIBUTING.md`.

- Non-trivial MRs (features, refactors, architectural changes) must reference an issue:
  `Closes #N` or `Relates to #N`.
- Trivial MRs (typos, minor formatting) do not need an issue.
- Keep each MR focused on one concern.
- Use the MR and issue templates under [`.gitlab/`](.gitlab/).
- Read each template's `TEMPLATE CONVENTION` block before you write.
- Load the `/orbit-planning` skill before you create or label an issue, epic, or MR.

Keep the reviewer summary to two or three plain sentences. Describe the symptom and fix without implementation names.

Put mechanics, alternatives, and file details in the Agent context block. In comments, lead with the verdict and collapse long reasoning when it helps.

## Where to start

- [Open issues](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues) — the
  `orbit::hackathon` label marks good entry points.
- [AGENTS.md](AGENTS.md) — tooling entry point and map to the owning guides.
- [CONTEXT.md](CONTEXT.md) — domain glossary. Use the canonical terms when writing code,
  docs, or MR descriptions.

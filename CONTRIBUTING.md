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

- Comments explain why. Do not narrate what the code shows.
- Reuse existing infrastructure before adding new infrastructure.
- Before changing an area, read its design document and the crate-local `AGENTS.md`, if present.
- Treat the ontology as the single source of truth for graph-shape facts.
- Read `docs/dev/sox-billing-boundary.md` before changing billing.

## Documentation conventions

- Design documents describe the current system. Update the owning document under
  `docs/design-documents/` in the same MR as a behavior change.
- Rewrite or remove stale text instead of preserving history.
- When you add, move, or remove an owning guide, update its pointers in the
  byte-identical `AGENTS.md` and `CLAUDE.md` files.
- Use the canonical terms in `CONTEXT.md`. Add a term only when a new domain
  concept could confuse a new contributor.

## Issue conventions

- A person creates each issue and epic. An agent that runs the create call
  needs the author's approval of the draft first. The request to do the work
  is not that approval.
- Agents may draft content. The author keeps every section above the Agent
  context block in their own words. Put generated content in that block.

## MR conventions

MR titles must follow [Conventional Commits](https://www.conventionalcommits.org/) format:

```plaintext
type(scope): short description
```

Examples: `fix(compiler): correct aggregation undercount`, `docs: add CONTRIBUTING.md`.

- Non-trivial MRs (features, refactors, architectural changes) must reference an issue:
  `Closes #N` or `Relates to #N`.
- Trivial MRs (typos, minor dependency bumps, or formatting-only changes) do not need an issue.
- Keep each MR focused on one concern.
- Use the MR and issue templates under [`.gitlab/`](.gitlab/).
- Read each template's `TEMPLATE CONVENTION` block before you write.
- Load the `/orbit-planning` skill before you draft or label an issue, epic, or MR.

Keep the reviewer summary to two or three plain sentences. Describe the symptom and fix without implementation names.

Put mechanics, alternatives, and file details in the Agent context block. In comments, lead with the verdict and collapse long reasoning when it helps.

## Where to start

- [Open issues](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues) — the
  `orbit::hackathon` label marks good entry points.
- [AGENTS.md](AGENTS.md) — tooling entry point and map to the owning guides.
- [CONTEXT.md](CONTEXT.md) — domain glossary. Use the canonical terms when writing code,
  docs, or MR descriptions.

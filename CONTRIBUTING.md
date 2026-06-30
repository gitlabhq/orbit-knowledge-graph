# Contributing to Orbit

Thanks for contributing to [GitLab Orbit](https://docs.gitlab.com/orbit/), the knowledge graph
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

## MR conventions

MR titles must follow [Conventional Commits](https://www.conventionalcommits.org/) format:

```text
type(scope): short description
```

Examples: `fix(compiler): correct aggregation undercount`, `docs: add CONTRIBUTING.md`.

- Non-trivial MRs (features, refactors, architectural changes) must reference an issue:
  `Closes #N` or `Relates to #N`.
- Trivial MRs (typos, minor formatting) do not need an issue.
- Use the MR and issue templates under [`.gitlab/`](.gitlab/).

## Where to start

- [Open issues](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues) — the
  `orbit::hackathon` label marks good entry points.
- [AGENTS.md](AGENTS.md) — developer entry point: architecture overview, CI gates,
  code-quality conventions, and links to the [crate map](docs/dev/agents-crate-map.md)
  and [reference index](docs/dev/agents-reference-index.md).
- [CONTEXT.md](CONTEXT.md) — domain glossary. Use the canonical terms when writing code,
  docs, or MR descriptions.

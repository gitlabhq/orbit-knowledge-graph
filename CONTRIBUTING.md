# Contributing to Orbit

[GitLab Orbit](https://docs.gitlab.com/orbit/) turns GitLab SDLC data and source
code into a property graph that you can query.

Community members contribute through the
[community fork](https://gitlab.com/gitlab-community/gitlab-org/orbit/knowledge-graph).
GitLab team members contribute to this repository. To start, look at the
[open issues](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues).

This repository is public. Do not put customer names, tenant names, incident
details, credentials, or other sensitive data in code, commits, MRs, or public
issues. Put them only in a confidential issue or an internal note.

## Quickstart

Install [mise](https://mise.jdx.dev/), then:

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise install
mise build
```

Run `mise tasks ls` for all tasks. The common tasks are in [AGENTS.md](AGENTS.md).
`mise test` needs no Docker. `mise test:integration` runs container tests. On
macOS, start Docker with `colima start gkg --memory 12`.

For the full local stack, see [Local development](docs/pages/engineering/development/local-development.md).
For test layers and CI jobs, see [Testing](docs/pages/engineering/testing/README.md).

## Code

- Default to no code comments. Write a comment only for a business requirement that the code cannot make clear.
  The rule covers doc comments (`///`, `//!`): delete one that only restates the name or signature,
  or cut it to one sentence on that requirement.
- Rust doc examples compile in `mise test:fast`, and `ignore` blocks fail it. Use a plain or `no_run` block with hidden setup lines starting with `#`, or `text` for pseudo-code.
- Reuse infrastructure before you add new infrastructure.
- Declare each crates.io or Git dependency once, in `[workspace.dependencies]` in
  the root `Cargo.toml`. In the crate, inherit it with `name = { workspace = true }`,
  and add `optional = true` or `features` there when the crate needs them.
  `mise lint:deps`, CI, and a pre-commit hook run
  `scripts/checks/check_workspace_deps.py` to find crate-local declarations.
  If a crate must pin its own version, add the dependency with a reason to the
  crate's `Cargo.toml`:

  ```toml
  [package.metadata.workspace-deps.allow]
  some-crate = "why this crate pins its own version"
  ```

- Before you change an area, read its design document and the crate `AGENTS.md`, if present.
- The ontology is the single source of truth for graph-shape facts.
- Read the [SOX billing boundary](docs/pages/engineering/development/sox-billing-boundary.md) page before you change billing.

## Docs

- Do not change docs in an MR that changes code. The exception is a docs change
  that a CI or build check needs. The `docs-in-code-mr-check` job enforces this rule.
  When a check needs the change, add the `docs-in-code-mr` label and retry the job.
- Rewrite or remove stale text. Do not keep history.
- Keep `README.md` a front page. Put each list only in the page that owns it,
  and link to that page with a relative path.
- When you add, move, or remove an owning guide, update `AGENTS.md`.
- Do not add a `CLAUDE.md` or `CLAUDE.local.md`. Either one stops Claude Code
  from reading `AGENTS.md`.
- Use the terms in [CONTEXT.md](CONTEXT.md). Add a term only when a new concept
  could confuse a new contributor.
- For user docs, follow the GitLab documentation principles in
  `.ai/principles/distilled/`. `mise lint:docs` does not check them.

## Issues and MRs

- A person creates each issue and epic. An agent needs the author's approval of
  the draft before it creates one. A request to do the work is not approval.
- The author writes the sections above the Agent context block. Generated
  content goes in that block.
- MR titles follow [Conventional Commits](https://www.conventionalcommits.org/),
  for example, `fix(compiler): correct aggregation undercount`.
- A non-trivial MR references an issue with `Closes #N` or `Relates to #N`.
- Keep each MR on one concern.
- Use the issue and MR templates, and read each `TEMPLATE CONVENTION` block.
  Load the `orbit-planning` skill before you draft or label an issue, epic, or MR.

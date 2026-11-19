# AGENTS.md

## Public repository

This repository is public. Do not put customer names, tenant names, incident
details, credentials, or other sensitive data in code, commits, MRs, or public
issues. Put them only in a confidential issue or an internal note.

## Tooling

Use mise for all tasks. Run `mise tasks ls` for the full list.

| Goal | Command |
| --- | --- |
| Build | `mise build` |
| Unit tests | `mise test` |
| Local tests | `mise test:local` |
| Integration tests | `mise test:integration` |
| Code-graph integration tests | `mise test:integration:codegraph` |
| Server integration tests | `mise test:integration:server` |
| CLI integration tests | `mise test:cli` |
| Check or fix code | `mise lint:code`, `mise lint:code:fix` |
| Check docs | `mise lint:docs` |
| Validate ontology | `mise ontology:validate` |
| Start or dispatch the server | `mise server:start`, `mise server:dispatch` |

The translated locale docs are generated. Never read, edit, or reference them.

After you create a worktree, run `mise trust`. Then set the shared hooks path:

```shell
git config core.hooksPath "$(git rev-parse --git-common-dir)/hooks"
```

## Where to find things

Read the owning guide before you act:

- The engineering docs hold the architecture overview, the contract of each
  area, the ADRs, the runbooks, the crate map, and the reference index.
- Before you work in a crate, read its `AGENTS.md` if it has one.
- Read `CONTRIBUTING.md` for engineering, documentation, issue, and MR
  conventions.
- Read `CONTEXT.md` for domain terms.
- Before you change billing, read the SOX billing ADR. Every file that controls
  billing emission, delivery, payloads, or quota checks is in SOX scope, even
  outside the billing crate. If you add or move such a file, add it to the
  SOX-scoped rules in `CODEOWNERS`.

## Issues and MRs

- Do not create a GitLab issue or epic until the author approves the draft.
- Before you open an MR, fill every section of the default MR template,
  including the Agent context block.
- Copy one `type::` label, the `orbit::<area>` label, and the milestone from
  the linked issue.
- Do not change docs in an MR that changes code. The exception is a docs change
  that a CI or build check needs. The `docs-in-code-mr-check` job enforces this.

Agent-authored MRs follow these steps:

1. Open a Draft MR with `[skip ci]` at the end of the Conventional Commits
   title. Keep it there through review.
2. When the MR is ready for CI, remove `[skip ci]` from the title. Then start a
   pipeline. A title edit does not start one.
3. When the MR is ready for review, add the `~workflow::ready for review` label.

Docs MRs that the docs-write flow opens (branches `docs-write/mr-*`) open as Draft
but never carry `[skip ci]`. The flow waits for a green pipeline.

## Skills and docs

- Before you create or edit a skill, follow the
  [`authoring-skills`](https://gitlab.com/gitlab-org/ai/skills/-/raw/main/skills/authoring-skills/SKILL.md)
  guide.
- For user docs, follow the
  [`gitlab-docs-change`](https://gitlab.com/gitlab-org/ai/skills/-/raw/main/skills/gitlab-docs-change/SKILL.md)
  skill.
- The docs-write flow (`.gitlab/duo/catalog/flows/`) follows the `docs-write`
  and `docs-review` skills from
  [`gitlab-org/orbit/team/skills`](https://gitlab.com/gitlab-org/orbit/team/skills)
  instead.

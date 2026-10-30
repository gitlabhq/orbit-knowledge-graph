# AGENTS.md

## Tooling

Use mise for all tasks.

| Goal | Command |
| --- | --- |
| Build | `mise build` |
| Fast tests | `mise test:fast` |
| Local tests | `mise test:local` |
| Integration tests | `mise test:integration` |
| Code-graph integration tests | `mise test:integration:codegraph` |
| Server integration tests | `mise test:integration:server` |
| CLI integration tests | `mise test:cli` |
| Check or fix code | `mise lint:code`, `mise lint:code:fix` |
| Check docs | `mise lint:docs` |
| Validate ontology | `mise ontology:validate` |
| Start or dispatch the server | `mise server:start`, `mise server:dispatch` |

`docs-locale/` is generated. Never read, edit, or reference it.

Open agent-authored Draft MRs with `[skip ci]` at the end of the Conventional
Commits title and keep it there through review.

When ready for CI, remove `[skip ci]` from the title, then start a pipeline.
Editing the title alone starts no pipeline.

After you create a worktree, run `mise trust`. Then set the shared hooks path:

```shell
git config core.hooksPath "$(git rev-parse --git-common-dir)/hooks"
```

## Where to find things

List these directories and read the relevant owner before you act:

- `docs/pages/engineering/architecture/` for the architecture overview and the contract of each area, and `docs/pages/engineering/adr/` for the ADRs.
- `docs/pages/engineering/development/` for runbooks, the crate map, and the reference index.
- Before working in a crate, check for and read `crates/<crate>/AGENTS.md`.
- `docs/pages/engineering/adr/020_billing_sox_scope.md` documents the SOX scope for billing emission. Read it before touching `crates/orbit-billing/`, `billing_adapter.rs`, or the hook points enumerated in `.gitlab/CODEOWNERS`.
- **SOX billing surface:** files controlling billing emission, delivery, payloads, or quota checks are in scope, including files outside `crates/orbit-billing/`. If you add or move such a file, add it to `.gitlab/CODEOWNERS` under the SOX-scoped rules.

Do not change any file under `docs/` in an MR that changes code. Change it only when a CI or build check fails without it.
Do not create a GitLab issue or epic until the author approves the draft.
Read `CONTRIBUTING.md` for engineering, documentation, issue, and MR
conventions. Read `CONTEXT.md` for domain terms.

## Writing or changing a skill

Before you create or edit a skill, follow the [`authoring-skills`](https://gitlab.com/gitlab-org/ai/skills/-/raw/main/skills/authoring-skills/SKILL.md) guide; shipped skills live in `skills/`, repo-only skills in `.agents/skills/`.

## Writing user docs

User docs live in `docs/docs-gitlab-com/` and publish to the [GitLab Orbit docs](https://docs.gitlab.com/orbit/). Put them in their own MR, separate from the feature MR.

The docs navigation is not in this repository. When an MR adds, moves, or renames a page, open a follow-up MR for `data/en-us/navigation.yaml` in [`docs-gitlab-com`](https://gitlab.com/gitlab-org/technical-writing/docs-gitlab-com), and merge it after the docs MR.

1. Write the change with [`gitlab-docs-change`](https://gitlab.com/gitlab-org/ai/skills/-/raw/main/skills/gitlab-docs-change/SKILL.md), and open the MR as a Draft.
1. Run [`gitlab-docs-review`](https://gitlab.com/gitlab-org/ai/skills/-/raw/main/skills/gitlab-docs-review/SKILL.md) with `--silent` until no `blocker` or `major` findings remain. Keep the MR in Draft until then.
1. Before you mark the MR ready, run one more review pass and start a pipeline. Mark it ready only when the pipeline passes.
1. After the MR is ready, request a review from `@z_painter` (Technical Writing) if a `blocker` or `major` finding remains. Do the same if the MR adds, moves, or renames a page. A TW review never blocks a merge and can happen after it.

# AGENTS.md

## Tooling

Use mise for all tasks.

| Goal | Command |
| --- | --- |
| Build | `mise build` |
| Fast tests | `mise test:fast` |
| Local tests | `mise test:local` |
| Integration tests | `mise test:integration` |
| Server integration tests | `mise test:integration:server` |
| CLI integration tests | `mise test:cli` |
| Check or fix code | `mise lint:code`, `mise lint:code:fix` |
| Check docs | `mise lint:docs` |
| Validate ontology | `mise ontology:validate` |
| Start or dispatch the server | `mise server:start`, `mise server:dispatch` |

`docs-locale/` is generated. Never read, edit, or reference it.

Open agent-authored Draft MRs with `[skip ci]` at the end of the Conventional
Commits title to skip unnecessary merge request pipelines while iterating. Keep
it in the title through review. Editing the title or pushing an unchanged SHA
starts no pipeline. The merge ref keeps the old title until a push with a new
SHA regenerates it.

When ready for CI, remove `[skip ci]` from the title first. Then, with a clean
tree, run `git commit --amend --no-edit --allow-empty` and push with
`--force-with-lease`. If the lease is rejected, someone else pushed: stop and
investigate.

Never push `Run CI` or `chore: trigger pipeline` commits. The only exception is
a refused force-push (protected branch). Then run `git reset --soft
origin/<branch>` and push an empty commit with a real message, such as
`git commit --allow-empty -m "chore: start merge request pipeline"`.

Do not use `glab ci run`. It creates a branch pipeline that MR-only rules can
filter out. With `--mr`, it reuses the stale merge ref and can be skipped.
Afterwards, check that the MR's head pipeline exists and is not skipped.

After you create a worktree, run `mise trust`. Then set the shared hooks path:

```shell
git config core.hooksPath "$(git rev-parse --git-common-dir)/hooks"
```

## Where to find things

List these directories and read the relevant owner before you act:

- `docs/design-documents/` for architecture, security, schema, querying, and indexing.
- `docs/dev/` for runbooks, the crate map, and the reference index.
- Before working in a crate, check for and read `crates/<crate>/AGENTS.md`.

Do not create a GitLab issue or epic until the author approves the draft.
Read `CONTRIBUTING.md` for engineering, documentation, issue, and MR
conventions. Read `CONTEXT.md` for domain terms.

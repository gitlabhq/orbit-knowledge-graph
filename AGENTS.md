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
Commits title to skip unnecessary merge request pipelines while iterating. When
ready for CI, remove `[skip ci]` and push a commit. Editing the title alone
starts no pipeline, and a manually triggered one can still be skipped. The merge
ref keeps the old title until it is regenerated, which a push guarantees.

After you create a worktree, run `mise trust`. Then set the shared hooks path:

```shell
git config core.hooksPath "$(git rev-parse --git-common-dir)/hooks"
```

## Where to find things

List these directories and read the relevant owner before you act:

- `docs/design-documents/` for architecture, security, schema, querying, and indexing.
- `docs/dev/` for runbooks, the crate map, and the reference index.
- Before working in a crate, check for and read `crates/<crate>/AGENTS.md`.
- `docs/design-documents/decisions/020_billing_sox_scope.md` documents the SOX scope for billing emission. Read it before touching `crates/orbit-billing/`, `billing_adapter.rs`, or the hook points enumerated in `.gitlab/CODEOWNERS`.
- **SOX billing surface:** a file outside `crates/orbit-billing/` is in scope if it controls whether events fire, what data they contain, or whether quota checks run. If you add or move such a file, add it to `.gitlab/CODEOWNERS` under the SOX-scoped rules. Update the hook-points table in `docs/design-documents/decisions/020_billing_sox_scope.md` in the same MR.

Do not create a GitLab issue or epic until the author approves the draft.
Read `CONTRIBUTING.md` for engineering, documentation, issue, and MR
conventions. Read `CONTEXT.md` for domain terms.

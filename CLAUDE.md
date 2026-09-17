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

After you create a worktree, run `mise trust`. Then set the shared hooks path:

```shell
git config core.hooksPath "$(git rev-parse --git-common-dir)/hooks"
```

## Where to find things

Read the linked owner before you act in that area.

| Area | Read first |
| --- | --- |
| Architecture and data flow | `docs/design-documents/README.md` |
| Authorization, security, FIPS | `docs/design-documents/security.md` |
| Schema changes | `docs/design-documents/schema_management.md` |
| Query behavior | `docs/design-documents/querying/` |
| Indexer and ontology work | `crates/indexer/AGENTS.md` |
| Code Graph work | `crates/code-graph/AGENTS.md` |
| Server settings | `docs/dev/runbooks/server_configuration.md` |
| Billing emission | `docs/dev/sox-billing-boundary.md` |
| Crate inventory | `docs/dev/agents-crate-map.md` |
| Files, schemas, tools, and checks | `docs/dev/agents-reference-index.md` |
| Code and comment conventions | [`CONTRIBUTING.md`](CONTRIBUTING.md#engineering-conventions) |
| Docs to update per change, MR titles, issue references, templates | [`CONTRIBUTING.md`](CONTRIBUTING.md#documentation-conventions), [MR conventions](CONTRIBUTING.md#mr-conventions) |
| Domain terms | `CONTEXT.md` |

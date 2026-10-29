# Log safety rules

ast-grep rules for log, span and print call sites. They back the expanded logs
policy described in
[Red data in logs](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/observability.md#red-data-in-logs).
Each rule file explains what it rejects and why, and each rule has cases in
`rule-tests/`.

Run them with `mise run lint:logs`. The `log-safety-check` CI job and a
lefthook pre-commit job run the same task. The task skips test code and the
paths it lists.

To suppress one finding, add `// ast-grep-ignore: <rule> -- <reason>` on the
line above it. The reason must be on the same line.

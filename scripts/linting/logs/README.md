# Log safety rules

ast-grep rules for log, span and print call sites. They back the expanded logs
policy described in
[Red data in logs](../../../docs/design-documents/observability.md#red-data-in-logs).
Each rule file explains what it rejects and why, and each rule has cases in
`rule-tests/`.

Run them with `mise run lint:logs`. The `log-safety-check` CI job and a
lefthook pre-commit job run the same task. The task skips test code and the
paths it lists.

To suppress one finding, add `// ast-grep-ignore: <rule> -- <reason>` on the
line above it. The reason must be on the same line.

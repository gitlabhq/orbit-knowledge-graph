# Log safety rules

ast-grep rules for log, span and print call sites. They back the expanded logs
policy described in
[Red data in logs](../../../docs/design-documents/observability.md#red-data-in-logs).

Run them with `mise run lint:logs`. The `log-safety-check` CI job runs the same
task, and a lefthook pre-commit job scans the staged Rust files.

| Rule | What it rejects |
|---|---|
| `log-message-is-literal` | A log or span message with `{}` interpolation. |
| `log-message-has-no-arguments` | A log message with format arguments. |
| `log-field-is-constant` | A field name that is not `{ safe::NAME }` or `{ expanded::NAME }` from `orbit-log-fields`. |
| `log-safe-field-is-plain` | A safe field recorded with `%`, `?`, `display` or `debug`. |
| `instrument-skips-arguments` | `#[instrument]` without `skip_all`. |
| `instrument-field-is-constant` | An `#[instrument]` field name that is not a constant. |
| `span-record-is-constant` | `span.record("name", ..)` with a string literal name. |
| `no-print` | `print!`, `println!`, `eprint!`, `eprintln!`, `dbg!`, `stdout()` and `stderr()` in server crates. Local tools are listed in the rule's `ignores`. |
| `no-log-crate` | Macros of the `log` crate. |

The rules do not check test code: `#[cfg(test)]` items, `#[test]`, `#[rstest]`
and `#[test_case]` functions, and the paths that the `lint:logs` task and the
lefthook job exclude. Each rule has cases in `rule-tests/`.
To suppress one finding, add `// ast-grep-ignore: <rule>` and the reason on the
line above it.

# system-notes-bench — POC for kg#499

Throwaway harness that validates the three risky assumptions behind ADR 013
(in draft): Rust-parser throughput, ClickHouse batch-lookup latency, and
edge-density gain. See the [POC plan][poc-plan] for the full benchmark
contract.

[poc-plan]: https://gitlab.com/dgruzd/droid-workspace/-/tree/main/task/2685/poc-plan.md

## What this is not

- **Not wired into the indexer.** No NATS handler, no `gl_edge` writes, no
  schema-version bump. Pure CLI tool under `xtask`.
- **Not a substitute for the production handler.** The parser and SQL here
  are the *shape* we intend to ship; the integration path is the next MR.
- **Not load-bearing.** This module can be deleted the day ADR 013 is
  accepted and the production handler lands.

## How to run

### Inspect — show what the parser does to the golden corpus

```shell
cargo run -p xtask -- system-notes-bench inspect
```

Prints every sample in the vendored Rails body templates plus the parser's
extracted references. Use this to hand-verify any parser change without
running tests.

### Parser benchmark — pure CPU, no network

```shell
cargo run --release -p xtask -- system-notes-bench parser --iterations 5000
```

Loops the golden corpus (or a `--input` JSON dump) through the parser N
times and reports min / median / max per-pass timings plus a derived
notes/sec figure.

Input format for `--input` is newline-delimited JSON, one note per line:

```jsonl
{"action": "cross_reference", "body": "mentioned in !123"}
{"action": "commit", "body": "added 2 commits\n\n* abc1234 - Fix\n* def5678 - Test"}
```

The action discriminator gates parsing (see `parser::Action::parse`); unknown
actions are logged and skipped, matching the production handler's planned
drift-tolerance behaviour.

### ClickHouse benchmark — exercises resolver SQL end-to-end

```shell
cargo run --release -p xtask -- system-notes-bench clickhouse \
  --url http://localhost:8123 \
  --traversal-path '1/100/' \
  --batch-size 1000
```

Issues the three resolver queries (routes, merge_requests, work_items) and
reports per-stage latency. Requires a ClickHouse instance with the standard
GKG fixture schema (`fixtures/siphon.sql`) populated. Authentication via
`--user` / `--password` or `CLICKHOUSE_*` env vars.

The synthetic batch is drawn from the golden corpus repeated to
`--batch-size`. Real benchmark runs against staging swap this for a streamed
read from `siphon_notes` filtered to a namespace.

## File map

| File | Purpose |
|---|---|
| `parser.rs` | Regex extractors for 16 system-note actions; ~200 LoC + 25 unit tests |
| `resolver.rs` | SQL templates and `ResolutionPlan` for the two-stage batch lookup |
| `golden.rs` | Vendored body templates from Rails `app/services/system_notes/*.rb` |
| `mod.rs` | Clap subcommand wiring (`parser`, `clickhouse`, `inspect`) |

## Outstanding gaps (POC scope)

These items are explicitly out of scope for this POC and live in the
follow-up implementation MR:

- **Real staging benchmark numbers.** Benchmarks 4 and 5 from the POC plan
  (end-to-end on `gitlab-org`, edge-density gain) need staging ClickHouse
  access. The harness is ready; running it on staging is a follow-up.
- **Ontology edge declarations.** `MENTIONS`, `ADDS_COMMIT`,
  `MERGED_AT_COMMIT`, `REOPENED` need YAML files under
  `config/ontology/edges/` and an entry in `config/ontology/schema.yaml`.
  These land with the production handler MR, not this POC.
- **Siphon `system_note_metadata` replication.** Separate MR against
  `gitlab-org/analytics-section/siphon`; tracked in research package.
- **Drift CI check.** `scripts/check-system-note-actions.sh` — modelled on
  `check-goon-format-version.sh` — vendored to keep `Action::parse` in sync
  with Rails `ICON_TYPES`. Lands with the production handler MR.

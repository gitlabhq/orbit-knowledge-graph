# Session Notes: Dirty-namespace change detection (MR !1877)

## What was done
- Implemented dirty-namespace pre-filter in `NamespaceDispatcher` that queries each namespaced Siphon source table for recently-changed `traversal_path` values
- Watermark column resolved per entity from `EtlConfig::watermark()` (not global default), with alias stripping for Query-type ETLs
- Added `SweepConfig` to `NamespaceDispatcherConfig` (cron + slack_secs)
- Added 4 new observability metrics: dirty namespace count, per-table query duration, per-table read rows, sweep-only dispatched counter
- Regenerated metrics catalog (91 entries) and config schema
- Added 6 unit tests including the per-entity watermark regression guard
- Added 3 integration tests: dirty-only dispatch, watermark update detection, deleted tombstone detection
- Updated design docs (sdlc_indexing.md, observability.md)

## Assumptions made
- `unqualified_column()` strips alias prefix — correct for current Query-type ETLs (SystemNote uses `sn.` prefix)
- Dirty detection uses `HashSet<String>` membership + prefix matching for namespace dirtiness
- Full sweep canary metric runs a second dirty-detection during sweep cycles to count sweep-only namespaces (acceptable overhead since sweeps are infrequent)
- The `_siphon_issues`/`_siphon_merge_requests` column question is handled defensively via per-entity ETL config resolution — no special-casing by table name

## What was NOT done
- Did NOT run integration tests with Docker (no Docker runtime available in this env — CI will validate)
- Did NOT add a YAML scenario test under `scenarios/sdlc/` — the dirty-detection tests are dispatcher-level integration tests in `dispatcher.rs`, not pipeline/indexer scenario tests
- Did NOT run the full `mise lint:code` (workspace clippy fails due to missing protoc for lance-encoding); ran clippy on the affected crates only
- Did NOT run markdownlint/vale (tools not installed locally)
- The sweep-only canary metric computation is done during sweep cycles by running dirty-detection a second time — this doubles the ClickHouse cost on sweep cycles but only runs every 15 minutes

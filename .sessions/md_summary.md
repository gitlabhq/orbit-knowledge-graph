# Fix intermittent zero-node failures in integration tests

## Problem

Integration tests were slow and flaky. Each subtest forked its own ClickHouse database, meaning a single test suite could spin up 50+ databases concurrently against one Docker container. This caused two issues:

1. **Flakiness**: ClickHouse's ReplacingMergeTree doesn't guarantee immediate read-after-write visibility under heavy concurrent metadata load. Freshly inserted rows would intermittently not appear in subsequent queries, causing zero-node assertion failures.

2. **Slowness**: The overhead of creating databases, tables, seeding data, and running OPTIMIZE TABLE FINAL ~50 times in parallel dominated test runtime. Actual query execution was trivial by comparison.

## Solution

Audit all four server test suites and classify every subtest as read-only or mutating. Read-only subtests (the vast majority) now share a single database seeded once, eliminating redundant setup. The small number of mutating subtests that write additional data still get their own forked databases.

OPTIMIZE TABLE FINAL runs once after seeding to guarantee part visibility before any queries execute.

## Testing

All four server test suites pass. Wall-clock improvements:

| Suite | Before | After |
|---|---|---|
| data_correctness (20 subtests) | ~67s | ~10s |
| hydration (11 subtests) | ~13s | ~9s |
| graph_formatter (50 subtests) | ~78s | ~23s |
| redaction (64 subtests) | 60s+ | ~32s |

Total server test time dropped from ~3.5 min to ~1.2 min. The zero-node flakiness is eliminated because there is no longer concurrent database setup contention.

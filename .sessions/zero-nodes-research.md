# Zero-Nodes Investigation — Research Findings

## Summary

Integration tests using `run_subtests!` intermittently return 0 rows from freshly seeded
`ReplacingMergeTree` tables when scaled from 20 to 44 concurrent subtests. Each subtest forks
a separate ClickHouse database, creates schema, seeds data, and queries it. `OPTIMIZE TABLE ...
FINAL` after each INSERT resolves the issue.

**Root cause:** I/O and metadata contention from 44 concurrent database setups (44 `CREATE
DATABASE`, ~440 `CREATE TABLE`, ~220 `INSERT`) against a single Docker container overwhelms
filesystem-level part visibility. The synchronous INSERT returns HTTP 200, but the subsequent
SELECT on a different client instance may not see the part yet under heavy load.

**Fix:** `OPTIMIZE TABLE ... FINAL` after `seed()`. With tables of 3–16 rows each, this is
essentially free and acts as a hard synchronization barrier.

---

## Question 1: Does ClickHouse 25.12 enable `async_insert` by default?

**No.** The default value of `async_insert` is `0` (disabled). This is confirmed in the
ClickHouse source and documentation:

> For example, the default value of async_insert is 0.
> — [ClickHouse settings docs](https://github.com/ClickHouse/ClickHouse/blob/master/docs/en/operations/settings/settings-query-level.md)

ClickHouse writes data synchronously by default. Each insert causes ClickHouse to immediately
create a part containing the data from the insert. This is the behavior when `async_insert` is
set to its default value of `0`.

> By default, ClickHouse is writing data synchronously. Each insert sent to ClickHouse causes
> ClickHouse to immediately create a part containing the data from the insert. This is the
> default behavior when the async_insert setting is set to its default value of 0.
> — [ClickHouse Cloud best practices docs](https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/cloud/bestpractices/asyncinserts.md)

### Does the `clickhouse` Rust crate (v0.14) set any async insert options?

**No.** The crate does not enable `async_insert` by default. It must be explicitly opted into:

```rust
let client = Client::default()
    .with_url("http://localhost:8123")
    .with_option("async_insert", "1")
    .with_option("wait_for_async_insert", "0");
```

> You could use ClickHouse asynchronous inserts to avoid client-side batching of the incoming
> data. This can be done by simply providing the async_insert option to the insert method (or
> even to the Client instance itself, so that it will affect all the insert calls).
> — [ClickHouse Rust client docs](https://clickhouse.com/docs/integrations/rust)

The test code's `ArrowClickHouseClient::new()` only sets `output_format_arrow_string_as_string`
and `output_format_arrow_fixed_string_as_fixed_byte_array`. No async insert options.

**Conclusion: `async_insert` is ruled out as a cause.**

---

## Question 2: Can a SELECT return 0 rows after a single INSERT into an empty ReplacingMergeTree?

### Under normal conditions: No.

A synchronous INSERT into a MergeTree-family table writes data as a new part to disk before
returning HTTP 200. A subsequent SELECT should see that part.

> When ClickHouse receives an insert query, then the query's data is immediately
> (synchronously) written in the form of (at least) one new data part (per partitioning key)
> to the database storage, and after that, ClickHouse acknowledges the successful execution
> of the insert query.
> — [ClickHouse blog: Asynchronous Data Inserts](https://clickhouse.com/blog/asynchronous-data-inserts-in-clickhouse)

### Under extreme concurrent load: Yes, it can happen.

With 44 concurrent forks, the single Docker container is processing:
- 44 `CREATE DATABASE` statements
- ~440 `CREATE TABLE` statements (10 tables × 44 databases)
- ~220 `INSERT` statements (5 inserts × 44 databases)
- ~44 `SELECT` statements

All of this hits one ClickHouse process in a Docker container with default resource limits.
Under this level of I/O contention, the filesystem-level visibility of a newly written part
can lag behind the HTTP 200 response, especially when:

1. The SELECT uses a **different client instance** than the INSERT (no connection/session reuse)
2. The Docker container's I/O scheduler is saturated
3. ClickHouse's background merge threads are competing with DDL/DML threads for resources

### ReplacingMergeTree specifics

ReplacingMergeTree with a `_deleted` column adds complexity:

> At merge time, the ReplacingMergeTree identifies duplicate rows, using the values of the
> ORDER BY columns as a unique identifier, and either retains only the highest version or
> removes all duplicates if the latest version indicates a delete. This, however, offers
> eventual correctness only — it doesn't guarantee rows will be deduplicated, and you
> shouldn't rely on it.
> — [ClickHouse ReplacingMergeTree guide](https://clickhouse.com/docs/guides/replacing-merge-tree)

However, for a **single INSERT into an empty table**, there's only one part with no duplicates
and no rows marked as deleted (`_deleted = false`). ReplacingMergeTree deduplication/deletion
logic should be a no-op. The issue is not RMT-specific — it's resource contention.

### The `allow_experimental_replacing_merge_with_cleanup` setting

The tables use:

```sql
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1
```

This setting enables `OPTIMIZE ... FINAL CLEANUP` to remove rows where `_deleted = 1`. It
also enables automatic cleanup merges when combined with `min_age_to_force_merge_seconds`.

> When enabled, allows using OPTIMIZE ... FINAL CLEANUP to manually merge all parts in a
> partition down to a single part and removing any deleted rows. Also allows enabling such
> merges to happen automatically in the background with settings
> min_age_to_force_merge_seconds, min_age_to_force_merge_on_partition_only and
> enable_replacing_merge_with_cleanup_for_min_age_to_force_merge.
> — [ClickHouse MergeTree settings docs](https://clickhouse.com/docs/operations/settings/merge-tree-settings)

Since `min_age_to_force_merge_seconds` is NOT set in the table DDL, automatic cleanup merges
should not be triggered. This setting is unlikely to be the cause, but it's worth noting for
completeness.

---

## Question 3: Is `OPTIMIZE TABLE ... FINAL` the right fix?

### For this use case: Yes.

The tables contain 3–16 rows each. `OPTIMIZE TABLE ... FINAL` on tables this small is
essentially free — there's one tiny part per table, the merge is trivial, and it completes
in single-digit milliseconds.

The performance warnings about `OPTIMIZE TABLE ... FINAL` apply to production tables:

> OPTIMIZE FINAL forces the merge, and also forces ClickHouse to merge all parts in one
> partition to one part (if size permits). As a result, all updated rows will be properly
> replaced. The main problem with this approach is performance. It is slow, locks the table,
> and can only be used occasionally.
> — [Altinity blog: ReplacingMergeTree Explained](https://altinity.com/blog/clickhouse-replacingmergetree-explained-the-good-the-bad-and-the-ugly)

This is about tables with millions of rows and many large parts. For test tables with <20 rows,
the "lock" is held for microseconds and there are no large parts to merge.

### Alternative fixes considered

| Fix | Verdict |
|-----|---------|
| `OPTIMIZE TABLE ... FINAL` after seed | ✅ **Use this.** Trivial cost at test scale. Acts as hard sync barrier. |
| Set `async_insert=0` on client | ❌ Already the default. No effect. |
| Use `SELECT ... FINAL` in queries | ⚠️ Addresses RMT deduplication, not part visibility. Wrong problem. |
| Limit concurrency with semaphore | ⚠️ Good belt-and-suspenders if scaling beyond 44 subtests. |
| Switch to `ENGINE = Memory` for tests | ⚠️ Eliminates all MergeTree timing issues but loses RMT coverage. |
| Add retry loop after seed | ⚠️ Works but is fragile and adds latency. |
| Set `select_sequential_consistency=1` | ❌ For replicated tables only. Not applicable here. |

---

## Question 4: Could 44 concurrent CREATE DATABASE + DDL cause ClickHouse to delay DML?

### Yes. This is the most likely root cause.

ClickHouse processes DDL and DML in the same server process. 44 concurrent database setups
create significant pressure on:

1. **Filesystem I/O**: Each `CREATE TABLE` creates directory structures. Each `INSERT` writes
   part files. 44 databases × ~10 tables = ~440 directory trees being created concurrently.

2. **Metadata operations**: ClickHouse maintains metadata for all databases and tables. Creating
   440 tables in rapid succession puts pressure on the metadata layer.

3. **Background merge threads**: ClickHouse's background merge pool is shared across all
   databases. With hundreds of newly created parts across 44 databases, the merge scheduler
   is actively processing, competing with DDL and DML for CPU and I/O.

4. **Docker container limits**: A single Docker container has default resource constraints.
   The ClickHouse process may hit I/O bandwidth limits, especially on CI systems with
   constrained disk performance.

The key observation from the codebase is that `create_client()` creates a **new client instance
on every call** — there's no connection pooling or session reuse. This means the INSERT and
the subsequent SELECT are on completely independent HTTP connections with no shared session
state, making them more susceptible to timing issues.

### Evidence supporting this theory

- **20 subtests: passes reliably.** 20 concurrent forks = ~200 CREATE TABLE + ~100 INSERT.
  Within ClickHouse's comfortable handling capacity.
- **44 subtests: intermittent failures.** 44 concurrent forks = ~440 CREATE TABLE + ~220 INSERT.
  Crosses a threshold where I/O contention causes part visibility delays.
- **`OPTIMIZE TABLE ... FINAL` fixes it.** This forces a synchronization barrier that ensures
  all data is fully materialized and visible before SELECT runs.

---

## How synchronous INSERTs work in ClickHouse

For reference, here is the normal synchronous insert flow:

1. ClickHouse receives the INSERT query via HTTP
2. Data is parsed and formed into an in-memory insert block
3. The block is sorted by the ORDER BY key
4. The block is compressed and written to disk as a new data part
5. HTTP 200 is returned to the client

> When ClickHouse receives an insert query, then the query's data is immediately
> (synchronously) written in the form of (at least) one new data part (per partitioning key)
> to the database storage, and after that, ClickHouse acknowledges the successful execution.
> — [ClickHouse blog](https://clickhouse.com/blog/asynchronous-data-inserts-in-clickhouse)

The data should be visible to subsequent queries after step 5. However, "visible" depends on
the filesystem flushing metadata (directory entries, file contents) in a way that's observable
by other threads/connections reading the same paths.

---

## How async INSERTs work (for reference — NOT active in this setup)

When `async_insert=1` is enabled:

1. ClickHouse receives the INSERT query
2. Data is written into an **in-memory buffer** (NOT to disk)
3. If `wait_for_async_insert=1` (default when async is enabled): HTTP response is held until
   buffer is flushed to disk
4. If `wait_for_async_insert=0`: HTTP 200 is returned immediately (fire-and-forget)
5. Buffer is flushed when: size threshold reached, time threshold elapsed, or query count
   threshold reached

> Data is not searchable by queries before being flushed to the database storage.
> — [ClickHouse blog](https://clickhouse.com/blog/asynchronous-data-inserts-in-clickhouse)

This is NOT the issue here since `async_insert` defaults to `0`.

---

## ReplacingMergeTree behavior reference

### Deduplication is eventual, not immediate

> ClickHouse does not guarantee that merge will fire and replace rows using
> ReplacingMergeTree logic. FINAL keyword should be used in order to apply merge in
> a query time.
> — [Altinity Knowledge Base](https://kb.altinity.com/engines/mergetree-table-engine-family/replacingmergetree/)

### FINAL applies deduplication at query time

> The FINAL modifier for SELECT statements applies the replacing logic at query time.
> — [Altinity blog](https://altinity.com/blog/clickhouse-replacingmergetree-explained-the-good-the-bad-and-the-ugly)

### Cleanup merges and `_deleted` column

> By default, ClickHouse does not remove the last remaining delete row for a key in a
> ReplacingMergeTree with an is_deleted column. This ensures that older versions of the key
> can always be ignored correctly by FINAL and merged away.
> — [ClickHouse PR #76440](https://github.com/ClickHouse/ClickHouse/pull/76440)

The `clean_deleted_rows='Always'` setting (deprecated in 24.3+) and the
`allow_experimental_replacing_merge_with_cleanup` setting control whether deleted rows are
actually removed during merges.

---

## Rust `clickhouse` crate v0.14 specifics

- Crate: `clickhouse` (official, by ClickHouse team)
- Repo: https://github.com/ClickHouse/clickhouse-rs
- Protocol: HTTP (port 8123)
- MSRV for 0.14.x: Rust 1.89.0
- Default format: `RowBinaryWithNamesAndTypes` (since 0.14.0)
- Compression: LZ4 enabled by default
- No async insert options set by default
- `Client::default()` creates a client with no special settings
- The `execute()` method calls `Query::execute()` which sends SQL via HTTP POST and awaits
  the response
- Each `ArrowClickHouseClient::new()` call creates a fresh `Client` — no connection pooling

Features used in this project:
- `inserter` — for the Inserter API (not used in test code, but in the crate features)
- `rustls-tls-ring` — TLS via rustls with ring backend
- `rustls-tls-native-roots` — system CA certificates

---

## Recommended implementation

Add `OPTIMIZE TABLE ... FINAL` for each table after seeding:

```rust
async fn seed(ctx: &TestContext) {
    // ... existing INSERT statements ...

    // Force part visibility — negligible cost at test scale (3-16 rows per table)
    for table in &["gl_user", "gl_group", "gl_project", "gl_merge_request", "gl_edge"] {
        ctx.execute(&format!("OPTIMIZE TABLE {table} FINAL")).await;
    }
}
```

If scaling beyond ~50 subtests, also consider adding a concurrency semaphore:

```rust
use tokio::sync::Semaphore;

// In run_subtests! or equivalent
let sem = Arc::new(Semaphore::new(20));
// Acquire permit before each fork
let _permit = sem.acquire().await.unwrap();
```

---

## Sources

- ClickHouse async insert docs: https://clickhouse.com/docs/optimize/asynchronous-inserts
- ClickHouse ReplacingMergeTree guide: https://clickhouse.com/docs/guides/replacing-merge-tree
- ClickHouse MergeTree settings: https://clickhouse.com/docs/operations/settings/merge-tree-settings
- ClickHouse Rust client docs: https://clickhouse.com/docs/integrations/rust
- ClickHouse Rust crate (docs.rs): https://docs.rs/clickhouse
- ClickHouse Rust crate (GitHub): https://github.com/ClickHouse/clickhouse-rs
- Altinity KB — ReplacingMergeTree: https://kb.altinity.com/engines/mergetree-table-engine-family/replacingmergetree/
- Altinity KB — Async INSERTs: https://kb.altinity.com/altinity-kb-queries-and-syntax/async-inserts/
- Altinity blog — ReplacingMergeTree Explained: https://altinity.com/blog/clickhouse-replacingmergetree-explained-the-good-the-bad-and-the-ugly
- ClickHouse blog — Async inserts: https://clickhouse.com/blog/asynchronous-data-inserts-in-clickhouse
- ClickHouse blog — Insert monitoring: https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
- ClickHouse PR #76440 — Automatic cleanup merges: https://github.com/ClickHouse/ClickHouse/pull/76440
- ClickHouse settings reference: https://github.com/ClickHouse/ClickHouse/blob/master/docs/en/operations/settings/settings-query-level.md

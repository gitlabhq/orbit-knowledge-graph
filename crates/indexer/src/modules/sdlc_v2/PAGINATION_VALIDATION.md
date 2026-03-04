# SDLC v2 Pagination Validation

Validated on 2026-03-04 against local ClickHouse (`localhost:8123`, database `gkg_test`).

## Methodology

### What was tested

The real Rust `Pipeline::run` code path, end-to-end:

1. `ExtractQuery::to_sql()` generates the paginated SQL
2. `Datalake::query_batches()` executes it against ClickHouse over HTTP (Arrow IPC streaming)
3. `ExtractQuery::advance()` extracts cursor values from the last row of each batch
4. `Pipeline::transform_and_write()` runs DataFusion SQL transforms on each batch
5. `IndexingPositionStore::save_progress()` persists cursor state after each page
6. `IndexingPositionStore::save_completed()` marks the pipeline as done

### How it was run

An `#[ignore]` test in `pipeline.rs` (`milestone_pagination_live`) constructs a real `Pipeline` with:

- **Datalake**: real `Datalake` backed by `ArrowClickHouseClient` pointing at `localhost:8123/gkg_test`
- **Position store**: in-memory `RecordingPositionStore` (records every `save_progress` call)
- **Destination**: `MockDestination` (accepts writes without persisting — we're testing pagination, not graph writes)
- **Logging**: a `LoggingDatalake` wrapper that prints each query's page number, row count, latency, running total, and cursor clause to stdout in real time

A wrapping `LoggingDatalake` intercepts every `query_batches` call to capture the SQL and print progress before delegating to the real `Datalake`.

### Parameters

| Parameter | Value |
|---|---|
| Table | `gkg_test.siphon_milestones` |
| Sort key | `(traversal_path, id)` — inherited from ontology `default_entity_sort_key` |
| Traversal path filter | `startsWith(traversal_path, '/9970/12345')` |
| Batch size | 1,000,000 rows |
| Watermark range | epoch → 2027-01-01 (all data) |
| Total rows in scope | 34,446,174 |

### Reproducing

```sh
cargo test -p indexer --lib milestone_pagination_live -- --ignored --nocapture
```

Requires a local ClickHouse on `localhost:8123` with `gkg_test.siphon_milestones` populated (e.g. via `datalake-generator`).

## Results

34,446,174 rows paginated in **39.5 seconds** across 35 pages.

```
Page  1: 1000000 rows in  1.5s | total:  1000000 | cursor: (none — first page)
Page  2: 1000000 rows in  1.7s | total:  2000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '1799910')
Page  3: 1000000 rows in  1.0s | total:  3000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '3995782')
Page  4: 1000000 rows in  1.7s | total:  4000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '5991830')
Page  5: 1000000 rows in  1.0s | total:  5000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '8659590')
Page  6: 1000000 rows in  1.4s | total:  6000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '11655059')
Page  7: 1000000 rows in  1.0s | total:  7000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '14654991')
Page  8: 1000000 rows in  1.2s | total:  8000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '17652918')
Page  9: 1000000 rows in  1.1s | total:  9000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '20655965')
Page 10: 1000000 rows in  1.1s | total: 10000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '23654567')
Page 11: 1000000 rows in  1.4s | total: 11000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '26653147')
Page 12: 1000000 rows in  1.1s | total: 12000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '29657241')
Page 13: 1000000 rows in  1.2s | total: 13000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '32659740')
Page 14: 1000000 rows in  1.0s | total: 14000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '35660132')
Page 15: 1000000 rows in  1.1s | total: 15000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '38661591')
Page 16: 1000000 rows in  1.0s | total: 16000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '41666772')
Page 17: 1000000 rows in  1.1s | total: 17000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '44668952')
Page 18: 1000000 rows in  1.1s | total: 18000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '47664833')
Page 19: 1000000 rows in  1.3s | total: 19000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '50664454')
Page 20: 1000000 rows in  1.2s | total: 20000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '53659868')
Page 21: 1000000 rows in  1.1s | total: 21000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '56660431')
Page 22: 1000000 rows in  1.1s | total: 22000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '59661146')
Page 23: 1000000 rows in  1.0s | total: 23000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '62661735')
Page 24: 1000000 rows in  1.1s | total: 24000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '65665302')
Page 25: 1000000 rows in  1.0s | total: 25000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '68662375')
Page 26: 1000000 rows in  1.2s | total: 26000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '71661894')
Page 27: 1000000 rows in  0.9s | total: 27000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '74660403')
Page 28: 1000000 rows in  1.0s | total: 28000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '77661705')
Page 29: 1000000 rows in  0.9s | total: 29000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '80661290')
Page 30: 1000000 rows in  0.9s | total: 30000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '83660061')
Page 31: 1000000 rows in  0.9s | total: 31000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '86656105')
Page 32: 1000000 rows in  0.9s | total: 32000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '89656506')
Page 33: 1000000 rows in  0.9s | total: 33000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '92657137')
Page 34: 1000000 rows in  0.8s | total: 34000000 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '95657312')
Page 35:  446174 rows in  0.4s | total: 34446174 | cursor: (traversal_path > '/9970/12345') OR (traversal_path = '/9970/12345' AND id > '98660671')
```

Final position store state: `cursor_values=None` (completed).

## Observations

- **Throughput**: ~880K rows/sec end-to-end (extract + DataFusion transform + mock write + cursor save).
- **Consistent page times**: ~1s per 1M-row page, slightly faster in later pages (possibly warm caches).
- **Cursor advances monotonically**: id values increase strictly across pages with no gaps or overlaps.
- **Last page terminates correctly**: 446,174 rows < batch_size of 1,000,000 triggers the loop break.
- **DNF cursor clause is correct**: `(traversal_path > 'X') OR (traversal_path = 'X' AND id > 'Y')` — equivalent to tuple comparison `(traversal_path, id) > (X, Y)`.
- **Position store semantics**: `save_progress` records the cursor used for the *current* query (at-least-once / idempotent replay on crash). `save_completed` sets `cursor_values=None`.

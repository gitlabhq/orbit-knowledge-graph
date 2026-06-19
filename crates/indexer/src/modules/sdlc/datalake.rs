use std::sync::Arc;

use crate::clickhouse::{ArrowClickHouseClient, ArrowQuery, QuerySummary};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use clickhouse_client::uri_guard;
use futures::StreamExt;
use futures::stream::BoxStream;
use serde_json::Value;
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Error)]
pub(crate) enum DatalakeError {
    #[error("query failed: {0}")]
    Query(String),

    #[error("arrow decode error: {0}")]
    ArrowDecode(#[from] arrow::error::ArrowError),

    /// The serialized request URI would exceed the `http` crate's length cap.
    /// Caught at [`Datalake::build_query`] so an over-limit batch fails loudly
    /// here instead of re-failing every dispatch with an opaque `uri too long`
    /// downstream (KG#881). The caller must split the batch into smaller chunks.
    #[error(
        "request URI is {len} bytes, over the {limit}-byte http limit; \
         the batched param list is too large and must be split into smaller chunks"
    )]
    UriTooLong { len: usize, limit: usize },
}

impl From<clickhouse::error::Error> for DatalakeError {
    fn from(err: clickhouse::error::Error) -> Self {
        DatalakeError::Query(err.to_string())
    }
}

pub(crate) type RecordBatchStream<'a> = BoxStream<'a, Result<RecordBatch, DatalakeError>>;

/// ClickHouse's storage-scan figures from the `X-ClickHouse-Summary` header
/// (its `read_rows`/`read_bytes` fields), the query cost. Greater than or equal
/// to the rows the page actually returned.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ScanStats {
    pub scanned_rows: u64,
    pub scanned_bytes: u64,
}

#[async_trait]
pub(crate) trait DatalakeQuery: Send + Sync {
    async fn query_arrow(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<RecordBatchStream<'_>, DatalakeError>;

    async fn query_batches(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<Vec<RecordBatch>, DatalakeError>;

    /// Buffers a whole page and reports ClickHouse's scan cost from the query
    /// summary. The default reports zero scan stats; only [`Datalake`] surfaces
    /// the real summary.
    async fn query_batches_with_summary(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<(Vec<RecordBatch>, ScanStats), DatalakeError> {
        let batches = self.query_batches(sql, params, max_block_size).await?;
        Ok((batches, ScanStats::default()))
    }
}

/// Byte cap for retry blocks, keeping a block's String column under the 2GB
/// Arrow offset limit even for MB-wide rows. ClickHouse's own default.
const RETRY_PREFERRED_BLOCK_SIZE_BYTES: &str = "1000000";

pub(crate) type DatalakeClient = Arc<ArrowClickHouseClient>;

pub(crate) struct Datalake {
    client: DatalakeClient,
    default_max_block_size: u64,
    /// The URI-length cap `build_query` enforces. Defaults to
    /// [`uri_guard::MAX_REQUEST_URI_LEN`] (the `http` crate's hard limit);
    /// overridable in tests via [`Datalake::with_uri_len_cap`] so an overflow
    /// test can use a small cap + small batch instead of fabricating a real
    /// 64 KB+ payload.
    uri_len_cap: usize,
}

impl Datalake {
    pub fn new(client: DatalakeClient, default_max_block_size: u64) -> Self {
        Self {
            client,
            default_max_block_size,
            uri_len_cap: uri_guard::MAX_REQUEST_URI_LEN,
        }
    }

    #[cfg(test)]
    fn with_uri_len_cap(mut self, cap: usize) -> Self {
        self.uri_len_cap = cap;
        self
    }

    /// Builds an [`ArrowQuery`] from `sql` + `params`, the single chokepoint
    /// where a datalake query's params become the `param_*` settings encoded
    /// into the dispatched URL.
    ///
    /// An over-limit URI fails downstream with an opaque `uri too long` and
    /// re-fails every retry (KG#881), so the guard measures the encoded URI here
    /// and rejects it before `hyper`/`http` sees it. The `debug_assert!` trips
    /// the invariant loudly in dev/CI debug builds; the returned
    /// [`DatalakeError::UriTooLong`] is the release-build backstop so production
    /// degrades to a loud, attributable failure instead of a panic.
    fn build_query(&self, sql: &str, params: Value) -> Result<ArrowQuery, DatalakeError> {
        let len = uri_guard::request_uri_len(
            self.client.base_url(),
            self.client.database(),
            &self.client.request_scaffold_pairs(),
            &params,
        );
        if len > self.uri_len_cap {
            let limit = self.uri_len_cap;
            // The debug_assert fires against the real http cap, not the
            // (potentially lowered) test cap, so injected-cap tests don't panic.
            debug_assert!(
                len <= uri_guard::MAX_REQUEST_URI_LEN,
                "datalake query URI is {len} bytes, over the {}-byte http cap",
                uri_guard::MAX_REQUEST_URI_LEN,
            );
            return Err(DatalakeError::UriTooLong { len, limit });
        }

        let mut query = self.client.query(sql);
        if let Value::Object(map) = params {
            for (key, value) in map {
                query = query.param(&key, value);
            }
        }
        Ok(query)
    }
}

#[async_trait]
impl DatalakeQuery for Datalake {
    async fn query_arrow(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<RecordBatchStream<'_>, DatalakeError> {
        // The Arrow 2GB-overflow byte-cap lives in `query_arrow_with_scan`; this
        // path only serves `query_batches`, which always passes `None`.
        let block_size = max_block_size.unwrap_or(self.default_max_block_size);
        let query = self.build_query(sql, params)?;
        let stream = query
            .fetch_arrow_streamed(block_size)
            .await
            .map_err(|e| DatalakeError::Query(e.to_string()))?;

        Ok(Box::pin(stream.map(|result| {
            result.map_err(|e| DatalakeError::Query(e.to_string()))
        })))
    }

    async fn query_batches(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<Vec<RecordBatch>, DatalakeError> {
        let mut stream = self.query_arrow(sql, params, max_block_size).await?;
        let mut batches = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }

        Ok(batches)
    }

    async fn query_batches_with_summary(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<(Vec<RecordBatch>, ScanStats), DatalakeError> {
        let block_size = max_block_size.unwrap_or(self.default_max_block_size);
        let mut query = self.build_query(sql, params)?;
        if max_block_size.is_some() {
            // Retry after a datalake failure (the Arrow 2GB overflow): byte-cap
            // blocks so the retry is safe regardless of row width.
            query = query.with_setting(
                "preferred_block_size_bytes",
                RETRY_PREFERRED_BLOCK_SIZE_BYTES,
            );
        }
        let (mut stream, summary) = query
            .fetch_arrow_streamed_with_summary(block_size)
            .await
            .map_err(|e| DatalakeError::Query(e.to_string()))?;

        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.map_err(|e| DatalakeError::Query(e.to_string()))?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }

        let scan_stats = summary
            .await
            .ok()
            .flatten()
            .map(scan_stats_from_summary)
            .unwrap_or_default();

        Ok((batches, scan_stats))
    }
}

fn scan_stats_from_summary(summary: QuerySummary) -> ScanStats {
    let scanned_rows = summary.read_rows();
    let scanned_bytes = summary.read_bytes();
    if scanned_rows.is_none() || scanned_bytes.is_none() {
        debug!(
            ?scanned_rows,
            ?scanned_bytes,
            "datalake summary missing read_rows/read_bytes; defaulting to 0"
        );
    }
    ScanStats {
        scanned_rows: scanned_rows.unwrap_or(0),
        scanned_bytes: scanned_bytes.unwrap_or(0),
    }
}

/// True when a datalake read failed because an Arrow column hit the 2 GiB i32
/// offset cap. The pipeline's extract retry uses this to drop straight to the
/// floor block size rather than gradually halving.
pub(in crate::modules::sdlc) fn is_arrow_string_overflow(err: &DatalakeError) -> bool {
    let DatalakeError::Query(message) = err else {
        return false;
    };
    message.contains("2147483646")
        || (message.contains("Arrow") && message.contains("Capacity error"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn test_datalake() -> Datalake {
        let client = ArrowClickHouseClient::new(
            "http://clickhouse:8123",
            "datalake",
            "default",
            None,
            &HashMap::new(),
            &HashMap::new(),
        );
        Datalake::new(Arc::new(client), 65_536)
    }

    fn measured_uri_len(datalake: &Datalake, params: &Value) -> usize {
        uri_guard::request_uri_len(
            datalake.client.base_url(),
            datalake.client.database(),
            &datalake.client.request_scaffold_pairs(),
            params,
        )
    }

    // A `paths` batch whose *encoded* URI clears the cap must be caught at the
    // chokepoint. In a debug build the `debug_assert!` is the loud signal, so
    // `build_query` panics here (this is what CI exercises); the `UriTooLong`
    // return path is covered unconditionally by `build_query_rejects_over_injected_cap`.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "over the")]
    fn build_query_debug_asserts_on_over_cap_uri() {
        let datalake = test_datalake();
        let path: String = std::iter::repeat_n('a', 255).collect();
        let paths: Vec<String> = std::iter::repeat_n(path, 3_000).collect();

        let _ = datalake.build_query("SELECT 1", json!({ "paths": paths }));
    }

    // A batch just under the cap must pass through untouched in any build: the
    // guard cannot false-positive on a legitimately-large-but-valid request.
    #[test]
    fn build_query_passes_under_cap_uri() {
        let datalake = test_datalake();
        let path: String = std::iter::repeat_n('a', 18).collect();
        let paths: Vec<String> = std::iter::repeat_n(path, 1_000).collect();
        let params = json!({ "paths": paths });

        assert!(measured_uri_len(&datalake, &params) < uri_guard::MAX_REQUEST_URI_LEN);
        assert!(datalake.build_query("SELECT 1", params).is_ok());
    }

    // `'` is doubled by the ClickHouse escaper *before* percent-encoding, so the
    // guard must measure the real (escaped, then `%27`-encoded) wire length. A
    // raw-byte proxy would call this batch safe (raw bytes are well under the
    // cap) yet its encoded URI is over, so only the encoded measurement catches
    // it. Pins the encoded measurement, not a raw-byte one.
    #[test]
    fn build_query_measures_escaped_encoded_length_for_adversarial_input() {
        let datalake = test_datalake();
        let quote_heavy: String = std::iter::repeat_n('\'', 500).collect();
        let paths: Vec<String> = std::iter::repeat_n(quote_heavy.clone(), 40).collect();
        let params = json!({ "paths": paths });

        let raw_param_bytes: usize = 40 * quote_heavy.len();
        assert!(
            raw_param_bytes < uri_guard::MAX_REQUEST_URI_LEN,
            "raw byte count {raw_param_bytes} must be under the cap so that only \
             an encoded-length guard can catch this batch"
        );
        assert!(
            measured_uri_len(&datalake, &params) > uri_guard::MAX_REQUEST_URI_LEN,
            "the escaped+percent-encoded URI must clear the cap"
        );
    }

    // A URI right at the cap is allowed (`http` rejects only `> MAX_LEN`); one
    // byte more flips it. Asserted on the measurement so the `<` vs `<=`
    // boundary is checked without the debug-assert aborting either case.
    #[test]
    fn uri_guard_boundary_is_strictly_over_cap() {
        let datalake = test_datalake();
        let base_len = measured_uri_len(&datalake, &json!({}));
        // Top-level string params are unquoted and `a` percent-encodes to
        // itself, so encoded length == base + `&param_p=` + value byte count.
        let framing = "&param_p=".len();
        let value_len = uri_guard::MAX_REQUEST_URI_LEN - base_len - framing;
        let at_cap: String = std::iter::repeat_n('a', value_len).collect();
        let over_cap: String = std::iter::repeat_n('a', value_len + 1).collect();

        assert_eq!(
            measured_uri_len(&datalake, &json!({ "p": at_cap })),
            uri_guard::MAX_REQUEST_URI_LEN
        );
        assert!(
            datalake
                .build_query("SELECT 1", json!({ "p": at_cap }))
                .is_ok()
        );
        assert!(
            measured_uri_len(&datalake, &json!({ "p": over_cap })) > uri_guard::MAX_REQUEST_URI_LEN
        );
    }

    #[test]
    fn build_query_rejects_over_injected_cap() {
        let params = json!({ "paths": ["group/project-1", "group/project-2"] });
        let datalake = test_datalake();
        let measured = measured_uri_len(&datalake, &params);

        let datalake = test_datalake().with_uri_len_cap(measured - 1);
        let result = datalake.build_query("SELECT 1", params).map(|_| ());

        match result {
            Err(DatalakeError::UriTooLong { len, limit }) => {
                assert_eq!(len, measured);
                assert_eq!(limit, measured - 1);
            }
            other => panic!("expected UriTooLong, got {other:?}"),
        }
    }

    #[test]
    fn build_query_passes_at_injected_cap() {
        let params = json!({ "paths": ["group/project-1"] });
        let datalake = test_datalake();
        let measured = measured_uri_len(&datalake, &params);

        let datalake = test_datalake().with_uri_len_cap(measured);
        assert!(datalake.build_query("SELECT 1", params).is_ok());
    }

    #[test]
    fn detects_the_production_arrow_string_overflow() {
        let err = DatalakeError::Query(
            "query failed: Code: 1002. DB::Exception: Error with a Arrow column \"String\": \
             Capacity error: array cannot contain more than 2147483646 bytes, have 2147527792: \
             While executing Arrow. (UNKNOWN_EXCEPTION)"
                .to_string(),
        );
        assert!(is_arrow_string_overflow(&err));
    }

    #[test]
    fn ignores_unrelated_datalake_errors() {
        assert!(!is_arrow_string_overflow(&DatalakeError::Query(
            "Code: 159. DB::Exception: Timeout exceeded".to_string()
        )));
    }
}

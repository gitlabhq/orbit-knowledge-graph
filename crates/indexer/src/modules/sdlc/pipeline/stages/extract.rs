use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use tracing::warn;

use crate::handler::HandlerError;
use crate::modules::sdlc::datalake::DatalakeQuery;
use crate::modules::sdlc::metrics::SdlcMetrics;
use gkg_server_config::DatalakeRetryConfig;

use super::super::page::ExtractedPage;

const MAX_RETRIES: u32 = 3;

pub(in crate::modules::sdlc) struct Extractor {
    datalake: Arc<dyn DatalakeQuery>,
    retry_config: DatalakeRetryConfig,
    metrics: SdlcMetrics,
}

impl Extractor {
    pub fn new(
        datalake: Arc<dyn DatalakeQuery>,
        retry_config: DatalakeRetryConfig,
        metrics: SdlcMetrics,
    ) -> Self {
        Self {
            datalake,
            retry_config,
            metrics,
        }
    }

    /// Reads a whole page into memory and its ClickHouse scan cost, halving the
    /// block size on a datalake failure so an Arrow 2GB offset overflow
    /// self-corrects without bouncing the message to the dead letter stream.
    /// The retry re-reads from the page's start cursor, which is idempotent.
    pub async fn extract(
        &self,
        transform_name: &str,
        sql: &str,
        params: Value,
    ) -> Result<ExtractedPage, HandlerError> {
        let mut last_error = None;
        let mut max_block_size: Option<u64> = None;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = Duration::from_millis(100 * 2u64.pow(attempt - 1));
                tokio::time::sleep(backoff).await;
            }

            let query_start = Instant::now();
            match self
                .datalake
                .query_batches_with_summary(sql, params.clone(), max_block_size)
                .await
            {
                Ok((batches, scan_stats)) => {
                    let extract_elapsed = query_start.elapsed();
                    let bytes: u64 = batches
                        .iter()
                        .map(|b| b.get_array_memory_size() as u64)
                        .sum();
                    self.metrics.record_datalake_query(
                        transform_name,
                        extract_elapsed.as_secs_f64(),
                        bytes,
                    );
                    return Ok(ExtractedPage {
                        batches,
                        scan_stats,
                        extract_elapsed,
                    });
                }
                Err(err) => {
                    warn!(
                        attempt,
                        max_retries = MAX_RETRIES,
                        max_block_size = ?max_block_size,
                        %err,
                        "datalake query failed, retrying with smaller block size"
                    );
                    last_error = Some(HandlerError::Processing(format!(
                        "datalake query failed: {err}"
                    )));
                    max_block_size = Some(match max_block_size {
                        Some(size) => (size / 2).max(self.retry_config.halving_min_block_size),
                        None => self.retry_config.halving_initial_block_size,
                    });
                }
            }
        }

        Err(last_error.expect("loop runs once and only exits here after a failure"))
    }
}

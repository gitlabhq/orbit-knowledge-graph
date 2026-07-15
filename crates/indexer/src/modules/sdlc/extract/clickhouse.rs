use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::durability::WriteDurability;
use crate::engine::retry::{Backoff, RetryMode, RetryPolicy, Step, drive_with};
use crate::handler::HandlerError;
use crate::observer::IndexingMode;

use super::{
    ExtractPage, ExtractPageStats, ExtractResume, ExtractRun, ExtractRunCompletion,
    ExtractRunContext, ExtractSession, Extractor,
};
use crate::modules::sdlc::datalake::{DatalakeQuery, is_arrow_string_overflow};
use crate::modules::sdlc::metrics::SdlcMetrics;
use crate::modules::sdlc::partitioning::{PartitionAssignment, PartitionStrategy};
use crate::modules::sdlc::plan::{
    ClickHouseExtractPlan, Cursor, CursorFilter, DeletedFilter, PreparedQuery, TraversalPathFilter,
    WatermarkFilter,
};
use gkg_server_config::DatalakeRetryConfig;

const CLICKHOUSE_EXTRACT_RESUME_SOURCE: &str = "clickhouse";
const CLICKHOUSE_EXTRACT_RESUME_VERSION: u16 = 1;
const MAX_RETRIES: u32 = 3;
const CLICKHOUSE_EXTRACT_RETRY: RetryPolicy = RetryPolicy {
    mode: RetryMode::Local,
    backoff: Backoff::Fixed(&[
        Duration::from_millis(100),
        Duration::from_millis(200),
        Duration::from_millis(400),
    ]),
    max_attempts: MAX_RETRIES + 1,
    dead_letter: false,
};

pub(in crate::modules::sdlc) struct ClickHouseExtractor {
    entity_name: String,
    plan: ClickHouseExtractPlan,
    datalake: Arc<dyn DatalakeQuery>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    metrics: SdlcMetrics,
    retry_config: DatalakeRetryConfig,
    partition_strategy: Option<PartitionStrategy>,
}

struct ClickHouseExtractSession {
    entity_name: String,
    plan: ClickHouseExtractPlan,
    query: PreparedQuery,
    cursor: Cursor,
    window: ClickHouseExtractWindow,
    position_key: String,
    datalake: Arc<dyn DatalakeQuery>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    metrics: SdlcMetrics,
    retry_config: DatalakeRetryConfig,
    finished: bool,
}

struct NoopExtractRunCompletion;

struct PartitionedExtractRunCompletion {
    entity_name: String,
    parent_position_key: String,
    fallback_watermark: DateTime<Utc>,
    checkpoint_store: Arc<dyn CheckpointStore>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ClickHouseExtractWindow {
    target: DateTime<Utc>,
    floor: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ClickHouseExtractResumePayload {
    #[serde(rename = "c")]
    cursor_values: Vec<String>,
    #[serde(rename = "f", default, skip_serializing_if = "Option::is_none")]
    floor: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LegacyClickHouseExtractResumePayload {
    #[serde(rename = "c")]
    cursor_values: Vec<String>,
    #[serde(rename = "f", default)]
    floor: Option<DateTime<Utc>>,
}

impl ClickHouseExtractor {
    #[allow(
        clippy::too_many_arguments,
        reason = "the source implementation owns its compiled plan and existing runtime collaborators"
    )]
    pub fn new(
        entity_name: String,
        plan: ClickHouseExtractPlan,
        datalake: Arc<dyn DatalakeQuery>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: SdlcMetrics,
        retry_config: DatalakeRetryConfig,
        partition_strategy: Option<PartitionStrategy>,
    ) -> Self {
        Self {
            entity_name,
            plan,
            datalake,
            checkpoint_store,
            metrics,
            retry_config,
            partition_strategy,
        }
    }

    async fn build_session(
        &self,
        position_key: String,
        query: PreparedQuery,
        window: ClickHouseExtractWindow,
        checkpoint: Option<Checkpoint>,
    ) -> Result<ClickHouseExtractSession, HandlerError> {
        let resume_payload = get_clickhouse_resume_payload(checkpoint.as_ref())?;
        let cursor = resume_payload
            .map(|payload| Cursor::from_values(payload.cursor_values))
            .unwrap_or_else(Cursor::first_page);
        if !cursor.is_first_page() {
            info!(position_key, "resuming from saved cursor");
        }
        Ok(ClickHouseExtractSession {
            entity_name: self.entity_name.clone(),
            plan: self.plan.clone(),
            query,
            cursor,
            window,
            position_key,
            datalake: Arc::clone(&self.datalake),
            checkpoint_store: Arc::clone(&self.checkpoint_store),
            metrics: self.metrics.clone(),
            retry_config: self.retry_config.clone(),
            finished: false,
        })
    }
}

#[async_trait]
impl Extractor for ClickHouseExtractor {
    async fn start_extraction(
        &self,
        context: ExtractRunContext,
    ) -> Result<ExtractRun, HandlerError> {
        let parent_checkpoint = self
            .checkpoint_store
            .load(&context.position_key)
            .await
            .map_err(|error| HandlerError::Processing(error.to_string()))?;
        let window =
            get_clickhouse_extract_window(parent_checkpoint.as_ref(), context.requested_watermark)?;
        let indexing_mode = get_indexing_mode(window);
        let base_query = self
            .plan
            .prepare()
            .with(WatermarkFilter {
                column: &self.plan.watermark_column,
                last: window.floor.unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
                current: window.target,
            })
            .with(
                context
                    .traversal_path
                    .as_deref()
                    .map(|path| TraversalPathFilter { path }),
            )
            .with(
                (indexing_mode == IndexingMode::Full).then_some(DeletedFilter {
                    column: &self.plan.deleted_column,
                }),
            );

        let should_partition = self.partition_strategy.is_some() && parent_checkpoint.is_none();
        let ranges = if should_partition {
            self.partition_strategy
                .as_ref()
                .expect("partition strategy checked above")
                .compute_ranges(self.datalake.as_ref(), context.traversal_path.as_deref())
                .await?
        } else {
            Vec::new()
        };

        if ranges.is_empty() {
            let session = self
                .build_session(context.position_key, base_query, window, parent_checkpoint)
                .await?;
            return Ok(ExtractRun {
                indexing_mode,
                sessions: vec![Box::new(session)],
                completion: Box::new(NoopExtractRunCompletion),
            });
        }

        info!(
            entity = %self.entity_name,
            partitions = ranges.len(),
            "running partitioned initial load"
        );
        let mut sessions: Vec<Box<dyn ExtractSession>> = Vec::new();
        for (assignment, query) in base_query.into_partitions(ranges) {
            let position_key = format!("{}{}", context.position_key, assignment.position_suffix());
            let checkpoint = self
                .checkpoint_store
                .load(&position_key)
                .await
                .map_err(|error| HandlerError::Processing(error.to_string()))?;
            if checkpoint
                .as_ref()
                .is_some_and(|checkpoint| checkpoint.resume.is_none())
            {
                info!(partition = %position_key, "skipping already-completed partition");
                continue;
            }
            sessions.push(Box::new(
                self.build_session(position_key, query, window, checkpoint)
                    .await?,
            ));
        }

        Ok(ExtractRun {
            indexing_mode,
            sessions,
            completion: Box::new(PartitionedExtractRunCompletion {
                entity_name: self.entity_name.clone(),
                parent_position_key: context.position_key,
                fallback_watermark: context.requested_watermark,
                checkpoint_store: Arc::clone(&self.checkpoint_store),
            }),
        })
    }
}

#[async_trait]
impl ExtractSession for ClickHouseExtractSession {
    async fn get_next_page(&mut self) -> Result<Option<ExtractPage>, HandlerError> {
        if self.finished {
            return Ok(None);
        }

        let sql = self
            .query
            .clone()
            .with(CursorFilter {
                sort_key: &self.plan.sort_key,
                values: self.cursor.values(),
            })
            .to_sql()?;
        let params = self.query.params();
        let (batches, scanned_rows, scanned_bytes, elapsed) =
            self.query_clickhouse_page(&sql, params).await?;
        if batches.is_empty() {
            self.finished = true;
            return Ok(None);
        }

        let rows = batches
            .iter()
            .map(|batch| batch.num_rows() as u64)
            .sum::<u64>();
        self.cursor = self.cursor.advance(
            batches.last().expect("non-empty page has a last block"),
            &self.plan.sort_key,
        )?;
        let has_more = rows >= self.plan.batch_size;
        self.finished = !has_more;
        let resume = ExtractResume::from_source_payload(
            CLICKHOUSE_EXTRACT_RESUME_SOURCE,
            CLICKHOUSE_EXTRACT_RESUME_VERSION,
            &ClickHouseExtractResumePayload {
                cursor_values: self.cursor.values().to_vec(),
                floor: self.window.floor,
            },
        )?;

        Ok(Some(ExtractPage {
            batches,
            row_count: rows,
            resume,
            stats: ExtractPageStats {
                scanned_rows,
                scanned_bytes,
                elapsed_ms: elapsed.as_millis() as u64,
            },
            has_more,
        }))
    }

    async fn save_page_resume(&self, resume: &ExtractResume) -> Result<(), HandlerError> {
        self.checkpoint_store
            .save_progress(
                &self.position_key,
                &Checkpoint {
                    watermark: self.window.target,
                    resume: Some(resume.to_checkpoint_value()?),
                },
            )
            .await
            .map_err(|error| {
                HandlerError::Processing(format!(
                    "failed to save extract resume for {}: {error}",
                    self.position_key
                ))
            })
    }

    async fn save_completed(&self, durability: WriteDurability) -> Result<(), HandlerError> {
        self.checkpoint_store
            .save_completed(&self.position_key, &self.window.target, durability)
            .await
            .map_err(|error| {
                HandlerError::Processing(format!(
                    "failed to mark {} as completed: {error}",
                    self.entity_name
                ))
            })?;
        self.metrics.record_watermark_lag(&self.window.target);
        Ok(())
    }
}

impl ClickHouseExtractSession {
    async fn query_clickhouse_page(
        &self,
        sql: &str,
        params: Value,
    ) -> Result<(Vec<arrow::record_batch::RecordBatch>, u64, u64, Duration), HandlerError> {
        drive_with(
            &CLICKHOUSE_EXTRACT_RETRY,
            None::<u64>,
            |block_size, attempt| {
                let query_start = Instant::now();
                let query =
                    self.datalake
                        .query_batches_with_summary(sql, params.clone(), block_size);
                let metrics = &self.metrics;
                let retry_config = &self.retry_config;
                let entity_name = &self.entity_name;
                async move {
                    match query.await {
                        Ok((batches, scan_stats)) => {
                            let elapsed = query_start.elapsed();
                            let bytes = batches
                                .iter()
                                .map(|batch| batch.get_array_memory_size() as u64)
                                .sum();
                            metrics.record_datalake_query(
                                entity_name,
                                elapsed.as_secs_f64(),
                                bytes,
                            );
                            Step::Done((
                                batches,
                                scan_stats.scanned_rows,
                                scan_stats.scanned_bytes,
                                elapsed,
                            ))
                        }
                        Err(error) => {
                            warn!(
                                attempt,
                                max_retries = MAX_RETRIES,
                                max_block_size = ?block_size,
                                %error,
                                "datalake query failed, retrying with smaller block size"
                            );
                            let next_block_size = if is_arrow_string_overflow(&error) {
                                retry_config.halving_min_block_size
                            } else {
                                match block_size {
                                    Some(size) => {
                                        (size / 2).max(retry_config.halving_min_block_size)
                                    }
                                    None => retry_config.halving_initial_block_size,
                                }
                            };
                            CLICKHOUSE_EXTRACT_RETRY.retry_or_give_up(
                                attempt,
                                Some(next_block_size),
                                HandlerError::Processing(format!("datalake query failed: {error}")),
                            )
                        }
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl ExtractRunCompletion for NoopExtractRunCompletion {
    async fn finish_extraction(self: Box<Self>) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[async_trait]
impl ExtractRunCompletion for PartitionedExtractRunCompletion {
    async fn finish_extraction(self: Box<Self>) -> Result<(), HandlerError> {
        let checkpoints = self
            .checkpoint_store
            .load_by_prefix(&format!(
                "{}{}",
                self.parent_position_key,
                PartitionAssignment::CHECKPOINT_PREFIX
            ))
            .await
            .map_err(|error| HandlerError::Processing(error.to_string()))?;
        let watermark =
            match get_completed_partition_watermark(&checkpoints, self.fallback_watermark) {
                Ok(watermark) => watermark,
                Err(incomplete) => {
                    info!(
                        entity = %self.entity_name,
                        checkpoint = %self.parent_position_key,
                        incomplete = incomplete.len(),
                        partitions = %incomplete.join(", "),
                        "partitions still in progress; deferring consolidation to next dispatch"
                    );
                    return Ok(());
                }
            };
        self.checkpoint_store
            .consolidate(&self.parent_position_key, &watermark)
            .await
            .map_err(|error| HandlerError::Processing(error.to_string()))
    }
}

fn get_clickhouse_extract_window(
    checkpoint: Option<&Checkpoint>,
    requested_watermark: DateTime<Utc>,
) -> Result<ClickHouseExtractWindow, HandlerError> {
    match checkpoint {
        Some(checkpoint) if checkpoint.resume.is_some() => Ok(ClickHouseExtractWindow {
            target: checkpoint.watermark,
            floor: get_clickhouse_resume_payload(Some(checkpoint))?
                .and_then(|payload| payload.floor),
        }),
        Some(checkpoint) => Ok(ClickHouseExtractWindow {
            target: requested_watermark,
            floor: Some(checkpoint.watermark),
        }),
        None => Ok(ClickHouseExtractWindow {
            target: requested_watermark,
            floor: None,
        }),
    }
}

fn get_indexing_mode(window: ClickHouseExtractWindow) -> IndexingMode {
    match window.floor {
        Some(_) => IndexingMode::Incremental,
        None => IndexingMode::Full,
    }
}

fn get_completed_partition_watermark(
    checkpoints: &[(String, Checkpoint)],
    fallback: DateTime<Utc>,
) -> Result<DateTime<Utc>, Vec<String>> {
    let incomplete = checkpoints
        .iter()
        .filter(|(_, checkpoint)| checkpoint.resume.is_some())
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    if !incomplete.is_empty() {
        return Err(incomplete);
    }
    Ok(checkpoints
        .iter()
        .map(|(_, checkpoint)| checkpoint.watermark)
        .min()
        .unwrap_or(fallback))
}

fn get_clickhouse_resume_payload(
    checkpoint: Option<&Checkpoint>,
) -> Result<Option<ClickHouseExtractResumePayload>, HandlerError> {
    let Some(value) = checkpoint.and_then(|checkpoint| checkpoint.resume.as_deref()) else {
        return Ok(None);
    };
    if let Ok(resume) = ExtractResume::from_checkpoint_value(value) {
        return resume
            .get_source_payload(
                CLICKHOUSE_EXTRACT_RESUME_SOURCE,
                CLICKHOUSE_EXTRACT_RESUME_VERSION,
            )
            .map(Some);
    }
    serde_json::from_str::<LegacyClickHouseExtractResumePayload>(value)
        .map(|legacy| {
            Some(ClickHouseExtractResumePayload {
                cursor_values: legacy.cursor_values,
                floor: legacy.floor,
            })
        })
        .map_err(|error| {
            HandlerError::Processing(format!(
                "failed to decode legacy ClickHouse extract resume: {error}"
            ))
        })
}
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::stream;

    use super::*;
    use crate::checkpoint::CheckpointError;
    use crate::modules::sdlc::datalake::{DatalakeError, RecordBatchStream};
    use crate::modules::sdlc::extract::MemoryExtractor;
    use crate::modules::sdlc::plan::{ExtractPlan, Plan, TransformSpec, Transformation};
    use crate::modules::sdlc::test_helpers::test_metrics;
    use crate::modules::sdlc::transform::TransformRegistry;
    use ontology::EtlScope;

    struct RecordingDatalake {
        calls: Mutex<Vec<(String, Value, Option<u64>)>>,
        responses: Mutex<Vec<Result<Vec<RecordBatch>, DatalakeError>>>,
    }

    #[derive(Default)]
    struct RecordingCheckpointStore {
        checkpoints: Mutex<HashMap<String, Checkpoint>>,
    }

    fn clickhouse_plan(batch_size: u64) -> ClickHouseExtractPlan {
        ClickHouseExtractPlan {
            template: crate::modules::sdlc::plan::ExtractTemplate::new(
                "SELECT id, name, _siphon_watermark AS _version, _siphon_deleted AS _deleted FROM source_table WHERE 1=1 {{filters}} ORDER BY id LIMIT {{batch_size}}".to_string(),
            )
            .expect("valid template"),
            watermark_column: "_siphon_watermark".to_string(),
            deleted_column: "_siphon_deleted".to_string(),
            sort_key: vec!["id".to_string()],
            batch_size,
        }
    }

    fn pipeline_plan() -> Plan {
        Plan {
            name: "Test".to_string(),
            target: "Test".to_string(),
            scope: EtlScope::Namespaced,
            extract: ExtractPlan::ClickHouse(clickhouse_plan(2)),
            transform: TransformSpec::DataFusion(vec![Transformation {
                sql: format!(
                    "SELECT id, name FROM {}",
                    crate::modules::sdlc::plan::SOURCE_DATA_TABLE
                ),
                destination_table: "gl_test".to_string(),
                dict_encode_columns: Default::default(),
            }]),
        }
    }

    fn test_batch(ids: &[i64]) -> RecordBatch {
        let names = ids
            .iter()
            .map(|id| format!("name-{id}"))
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names)),
            ],
        )
        .expect("valid batch")
    }

    fn extract_context() -> ExtractRunContext {
        ExtractRunContext {
            position_key: "ns.1.Test".to_string(),
            requested_watermark: "2026-07-15T00:00:00Z".parse().unwrap(),
            traversal_path: Some("1/".to_string()),
        }
    }

    fn extractor_with_responses(
        responses: Vec<Result<Vec<RecordBatch>, DatalakeError>>,
        retry_config: DatalakeRetryConfig,
    ) -> (
        ClickHouseExtractor,
        Arc<RecordingDatalake>,
        Arc<RecordingCheckpointStore>,
    ) {
        let datalake = Arc::new(RecordingDatalake {
            calls: Mutex::new(Vec::new()),
            responses: Mutex::new(responses),
        });
        let checkpoint_store = Arc::new(RecordingCheckpointStore::default());
        let extractor = ClickHouseExtractor::new(
            "Test".to_string(),
            clickhouse_plan(2),
            datalake.clone(),
            checkpoint_store.clone(),
            test_metrics(),
            retry_config,
            None,
        );
        (extractor, datalake, checkpoint_store)
    }

    #[tokio::test]
    async fn first_clickhouse_read_uses_the_configured_default_block_size() {
        let (extractor, datalake, _) = extractor_with_responses(
            vec![Ok(vec![test_batch(&[1])])],
            DatalakeRetryConfig::default(),
        );
        let mut run = extractor.start_extraction(extract_context()).await.unwrap();

        run.sessions[0].get_next_page().await.unwrap().unwrap();

        assert_eq!(datalake.block_sizes(), vec![None]);
    }

    #[tokio::test]
    async fn clickhouse_retry_halves_the_block_size_without_changing_the_query() {
        let retry_config = DatalakeRetryConfig {
            halving_initial_block_size: 8_000,
            halving_min_block_size: 1_024,
        };
        let (extractor, datalake, _) = extractor_with_responses(
            vec![
                Err(DatalakeError::Query("temporary".to_string())),
                Err(DatalakeError::Query("temporary".to_string())),
                Err(DatalakeError::Query("temporary".to_string())),
                Ok(vec![test_batch(&[1])]),
            ],
            retry_config,
        );
        let mut run = extractor.start_extraction(extract_context()).await.unwrap();

        run.sessions[0].get_next_page().await.unwrap().unwrap();

        assert_eq!(
            datalake.block_sizes(),
            vec![None, Some(8_000), Some(4_000), Some(2_000)]
        );
        let calls = datalake.calls.lock().unwrap();
        assert!(calls.windows(2).all(|calls| calls[0].0 == calls[1].0));
        assert!(calls.windows(2).all(|calls| calls[0].1 == calls[1].1));
    }

    #[tokio::test]
    async fn clickhouse_retry_does_not_drop_below_the_minimum_block_size() {
        let retry_config = DatalakeRetryConfig {
            halving_initial_block_size: 4_096,
            halving_min_block_size: 2_048,
        };
        let (extractor, datalake, _) = extractor_with_responses(
            vec![
                Err(DatalakeError::Query("temporary".to_string())),
                Err(DatalakeError::Query("temporary".to_string())),
                Err(DatalakeError::Query("temporary".to_string())),
                Ok(vec![test_batch(&[1])]),
            ],
            retry_config,
        );
        let mut run = extractor.start_extraction(extract_context()).await.unwrap();

        run.sessions[0].get_next_page().await.unwrap().unwrap();

        assert_eq!(
            datalake.block_sizes(),
            vec![None, Some(4_096), Some(2_048), Some(2_048)]
        );
    }

    #[tokio::test]
    async fn exhausted_clickhouse_retries_return_the_query_error() {
        let (extractor, datalake, _) = extractor_with_responses(
            vec![
                Err(DatalakeError::Query("temporary".to_string())),
                Err(DatalakeError::Query("temporary".to_string())),
                Err(DatalakeError::Query("temporary".to_string())),
                Err(DatalakeError::Query("temporary".to_string())),
            ],
            DatalakeRetryConfig::default(),
        );
        let mut run = extractor.start_extraction(extract_context()).await.unwrap();

        let error = match run.sessions[0].get_next_page().await {
            Ok(_) => panic!("exhausted retries should fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("temporary"), "error: {error}");
        assert_eq!(datalake.block_sizes().len(), 4);
    }

    #[tokio::test]
    async fn arrow_overflow_retry_jumps_to_the_minimum_block_size() {
        let retry_config = DatalakeRetryConfig {
            halving_initial_block_size: 8_000,
            halving_min_block_size: 1_024,
        };
        let (extractor, datalake, _) = extractor_with_responses(
            vec![
                Err(DatalakeError::Query(
                    "Arrow Capacity error 2147483646".to_string(),
                )),
                Ok(vec![test_batch(&[1])]),
            ],
            retry_config,
        );
        let mut run = extractor.start_extraction(extract_context()).await.unwrap();

        run.sessions[0].get_next_page().await.unwrap().unwrap();

        assert_eq!(datalake.block_sizes(), vec![None, Some(1_024)]);
    }

    #[tokio::test]
    async fn legacy_clickhouse_resume_adds_the_saved_cursor_to_the_next_query() {
        let (extractor, datalake, checkpoint_store) =
            extractor_with_responses(vec![Ok(Vec::new())], DatalakeRetryConfig::default());
        checkpoint_store.checkpoints.lock().unwrap().insert(
            "ns.1.Test".to_string(),
            Checkpoint {
                watermark: "2026-07-15T00:00:00Z".parse().unwrap(),
                resume: Some(r#"{"c":["5"],"f":null}"#.to_string()),
            },
        );
        let mut run = extractor.start_extraction(extract_context()).await.unwrap();

        assert!(run.sessions[0].get_next_page().await.unwrap().is_none());

        let calls = datalake.calls.lock().unwrap();
        assert!(calls[0].0.contains("id > '5'"), "sql: {}", calls[0].0);
    }

    #[tokio::test]
    async fn clickhouse_and_memory_extractors_feed_identical_transform_output() {
        let batch = test_batch(&[1, 2]);
        let (clickhouse, _, _) = extractor_with_responses(
            vec![Ok(vec![batch.clone()])],
            DatalakeRetryConfig::default(),
        );
        let mut clickhouse_run = clickhouse
            .start_extraction(extract_context())
            .await
            .unwrap();
        let clickhouse_page = clickhouse_run.sessions[0]
            .get_next_page()
            .await
            .unwrap()
            .unwrap();
        let memory = MemoryExtractor::new(vec![ExtractPage {
            batches: vec![batch],
            row_count: 2,
            resume: ExtractResume::from_source_payload("memory", 1, &[2]).unwrap(),
            stats: ExtractPageStats::default(),
            has_more: false,
        }]);
        let mut memory_run = memory.start_extraction(extract_context()).await.unwrap();
        let memory_page = memory_run.sessions[0]
            .get_next_page()
            .await
            .unwrap()
            .unwrap();
        let plan = pipeline_plan();
        let clickhouse_transform = TransformRegistry::default().build(&plan).unwrap();
        let memory_transform = TransformRegistry::default().build(&plan).unwrap();

        let clickhouse_output = clickhouse_transform
            .transform(&clickhouse_page.batches[0])
            .await
            .unwrap();
        let memory_output = memory_transform
            .transform(&memory_page.batches[0])
            .await
            .unwrap();

        assert_eq!(clickhouse_output.len(), memory_output.len());
        assert_eq!(
            clickhouse_output[0].output_index,
            memory_output[0].output_index
        );
        assert_eq!(clickhouse_output[0].batch, memory_output[0].batch);
    }

    #[test]
    fn in_progress_checkpoint_reuses_its_original_window() {
        let floor = Some("2026-07-14T23:00:00Z".parse().unwrap());
        let resume = ExtractResume::from_source_payload(
            CLICKHOUSE_EXTRACT_RESUME_SOURCE,
            CLICKHOUSE_EXTRACT_RESUME_VERSION,
            &ClickHouseExtractResumePayload {
                cursor_values: vec!["42".to_string()],
                floor,
            },
        )
        .unwrap()
        .to_checkpoint_value()
        .unwrap();
        let checkpoint = Checkpoint {
            watermark: "2026-07-15T00:00:00Z".parse().unwrap(),
            resume: Some(resume),
        };
        let requested = "2026-07-16T00:00:00Z".parse().unwrap();

        assert_eq!(
            get_clickhouse_extract_window(Some(&checkpoint), requested).unwrap(),
            ClickHouseExtractWindow {
                target: checkpoint.watermark,
                floor,
            }
        );
    }

    #[test]
    fn completed_checkpoint_advances_the_incremental_window() {
        let checkpoint = Checkpoint {
            watermark: "2026-07-15T00:00:00Z".parse().unwrap(),
            resume: None,
        };
        let requested = "2026-07-16T00:00:00Z".parse().unwrap();

        assert_eq!(
            get_clickhouse_extract_window(Some(&checkpoint), requested).unwrap(),
            ClickHouseExtractWindow {
                target: requested,
                floor: Some(checkpoint.watermark),
            }
        );
    }

    #[test]
    fn completed_partitions_use_the_oldest_watermark() {
        let checkpoints = vec![
            completed_partition_checkpoint("ns.7.Job.p1of3", "2026-06-07T22:00:00Z"),
            completed_partition_checkpoint("ns.7.Job.p2of3", "2026-06-07T21:30:00Z"),
            completed_partition_checkpoint("ns.7.Job.p3of3", "2026-06-07T22:15:00Z"),
        ];

        assert_eq!(
            get_completed_partition_watermark(
                &checkpoints,
                "2026-06-07T23:00:00Z".parse().unwrap()
            ),
            Ok("2026-06-07T21:30:00Z".parse().unwrap())
        );
    }

    #[test]
    fn in_progress_partitions_defer_parent_consolidation() {
        let checkpoints = vec![
            completed_partition_checkpoint("ns.7.Job.p1of3", "2026-06-07T22:00:00Z"),
            in_progress_partition_checkpoint("ns.7.Job.p2of3", "2026-06-07T21:30:00Z"),
            in_progress_partition_checkpoint("ns.7.Job.p3of3", "2026-06-07T22:15:00Z"),
        ];

        assert_eq!(
            get_completed_partition_watermark(
                &checkpoints,
                "2026-06-07T23:00:00Z".parse().unwrap()
            ),
            Err(vec![
                "ns.7.Job.p2of3".to_string(),
                "ns.7.Job.p3of3".to_string(),
            ])
        );
    }

    #[test]
    fn missing_partition_checkpoints_use_the_requested_watermark() {
        let fallback = "2026-06-07T23:00:00Z".parse().unwrap();

        assert_eq!(
            get_completed_partition_watermark(&[], fallback),
            Ok(fallback)
        );
    }

    impl RecordingDatalake {
        fn block_sizes(&self) -> Vec<Option<u64>> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .map(|(_, _, block_size)| *block_size)
                .collect()
        }
    }

    #[async_trait]
    impl DatalakeQuery for RecordingDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(stream::empty()))
        }

        async fn query_batches(
            &self,
            sql: &str,
            params: Value,
            max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            self.calls
                .lock()
                .unwrap()
                .push((sql.to_string(), params, max_block_size));
            self.responses.lock().unwrap().remove(0)
        }
    }

    #[async_trait]
    impl CheckpointStore for RecordingCheckpointStore {
        async fn load(&self, key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
            Ok(self.checkpoints.lock().unwrap().get(key).cloned())
        }

        async fn save_progress(
            &self,
            key: &str,
            checkpoint: &Checkpoint,
        ) -> Result<(), CheckpointError> {
            self.checkpoints
                .lock()
                .unwrap()
                .insert(key.to_string(), checkpoint.clone());
            Ok(())
        }

        async fn save_completed(
            &self,
            key: &str,
            watermark: &DateTime<Utc>,
            _durability: WriteDurability,
        ) -> Result<(), CheckpointError> {
            self.checkpoints.lock().unwrap().insert(
                key.to_string(),
                Checkpoint {
                    watermark: *watermark,
                    resume: None,
                },
            );
            Ok(())
        }

        async fn load_by_prefix(
            &self,
            prefix: &str,
        ) -> Result<Vec<(String, Checkpoint)>, CheckpointError> {
            Ok(self
                .checkpoints
                .lock()
                .unwrap()
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .map(|(key, checkpoint)| (key.clone(), checkpoint.clone()))
                .collect())
        }

        async fn consolidate(
            &self,
            parent_key: &str,
            watermark: &DateTime<Utc>,
        ) -> Result<(), CheckpointError> {
            self.save_completed(parent_key, watermark, WriteDurability::Durable)
                .await
        }
    }

    fn completed_partition_checkpoint(key: &str, watermark: &str) -> (String, Checkpoint) {
        (
            key.to_string(),
            Checkpoint {
                watermark: watermark.parse().unwrap(),
                resume: None,
            },
        )
    }

    fn in_progress_partition_checkpoint(key: &str, watermark: &str) -> (String, Checkpoint) {
        (
            key.to_string(),
            Checkpoint {
                watermark: watermark.parse().unwrap(),
                resume: Some("resume".to_string()),
            },
        )
    }
}

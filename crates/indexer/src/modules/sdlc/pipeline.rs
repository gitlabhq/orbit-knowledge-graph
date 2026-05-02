use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::clickhouse::TIMESTAMP_FORMAT;
use crate::destination::Destination;
use crate::handler::HandlerError;
use crate::nats::ProgressNotifier;

use super::datalake::DatalakeQuery;
use super::metrics::SdlcMetrics;
use super::plan::{PipelinePlan, SOURCE_DATA_TABLE, Transformation};
use crate::checkpoint::{Checkpoint, CheckpointStore};
use gkg_server_config::DatalakeRetryConfig;
const MAX_RETRIES: u32 = 3;

pub(in crate::modules::sdlc) struct PipelineContext {
    pub watermark: DateTime<Utc>,
    pub position_key: String,
    pub base_conditions: BTreeMap<String, String>,
}

pub(in crate::modules::sdlc) struct Pipeline {
    datalake: Arc<dyn DatalakeQuery>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    metrics: SdlcMetrics,
    retry_config: DatalakeRetryConfig,
}

impl Pipeline {
    pub fn new(
        datalake: Arc<dyn DatalakeQuery>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: SdlcMetrics,
        retry_config: DatalakeRetryConfig,
    ) -> Self {
        Self {
            datalake,
            checkpoint_store,
            metrics,
            retry_config,
        }
    }

    pub async fn run(
        &self,
        plans: &[PipelinePlan],
        context: &PipelineContext,
        destination: &dyn Destination,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError> {
        let mut errors = Vec::new();

        for plan in plans {
            if let Err(err) = self.run_plan(plan, context, destination, progress).await {
                self.metrics
                    .record_pipeline_error(&plan.name, err.error_kind());
                errors.push(format!("{}: {err}", plan.name));
            }
        }

        if errors.is_empty() {
            return Ok(());
        }

        Err(HandlerError::Processing(format!(
            "pipelines failed: {}",
            errors.join("; ")
        )))
    }

    async fn run_plan(
        &self,
        plan: &PipelinePlan,
        context: &PipelineContext,
        destination: &dyn Destination,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError> {
        let started_at = Instant::now();
        let mut extract_query = plan.extract_query.clone();

        let position_key = format!("{}.{}", context.position_key, plan.name);
        let checkpoint = self.load_checkpoint(&position_key).await;
        let params = self.build_query_params(&checkpoint.watermark, context);

        let mut total_rows: u64 = 0;
        extract_query = extract_query.resume_from(&checkpoint);

        if !extract_query.is_first_page() {
            info!(
                pipeline = %plan.name,
                "resuming from saved cursor"
            );
        }

        let session = SessionContext::new();

        loop {
            let batches = self
                .extract_batch(&plan.name, &extract_query.to_sql(), params.clone())
                .await?;

            if batches.is_empty() {
                break;
            }

            let rows_in_batch: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            total_rows += rows_in_batch;
            self.metrics.record_batch_rows(&plan.name, rows_in_batch);

            self.transform_and_write(
                &session,
                &plan.name,
                &batches,
                &plan.transforms,
                destination,
            )
            .await?;

            extract_query = extract_query.advance(batches.last().unwrap())?;

            self.checkpoint_store
                .save_progress(
                    &position_key,
                    &Checkpoint {
                        watermark: context.watermark,
                        cursor_values: Some(extract_query.cursor_values().to_vec()),
                    },
                )
                .await
                .map_err(|err| {
                    HandlerError::Processing(format!(
                        "failed to save cursor for {}: {err}",
                        plan.name
                    ))
                })?;

            progress.notify_in_progress().await;

            if rows_in_batch < plan.extract_query.batch_size() {
                break;
            }
        }

        self.checkpoint_store
            .save_completed(&position_key, &context.watermark)
            .await
            .map_err(|err| {
                HandlerError::Processing(format!(
                    "failed to mark {} as completed: {err}",
                    plan.name
                ))
            })?;

        let elapsed = started_at.elapsed();
        self.metrics
            .record_pipeline_completion(&plan.name, elapsed.as_secs_f64());
        self.metrics
            .record_watermark_lag(&plan.name, &context.watermark);

        if total_rows > 0 {
            info!(
                pipeline = %plan.name,
                total_rows,
                elapsed_ms = elapsed.as_millis() as u64,
                "pipeline completed"
            );
        } else {
            debug!(
                pipeline = %plan.name,
                elapsed_ms = elapsed.as_millis() as u64,
                "pipeline completed with no data"
            );
        }

        Ok(())
    }

    async fn extract_batch(
        &self,
        pipeline_name: &str,
        sql: &str,
        params: Value,
    ) -> Result<Vec<RecordBatch>, HandlerError> {
        let mut last_error = None;
        // First attempt uses the datalake's default `max_block_size`.
        // Subsequent attempts seed and then halve a per-call override so a
        // ClickHouse-side Arrow String offset overflow self-corrects without
        // bouncing the message to the dead letter stream.
        let mut current_block_size: Option<u64> = None;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = Duration::from_millis(100 * 2u64.pow(attempt - 1));
                tokio::time::sleep(backoff).await;
            }

            let query_start = Instant::now();
            match self
                .datalake
                .query_batches(sql, params.clone(), current_block_size)
                .await
            {
                Ok(batches) => {
                    let bytes: u64 = batches
                        .iter()
                        .map(|b| b.get_array_memory_size() as u64)
                        .sum();
                    self.metrics.record_datalake_query(
                        pipeline_name,
                        query_start.elapsed().as_secs_f64(),
                        bytes,
                    );
                    return Ok(batches);
                }
                Err(err) => {
                    warn!(
                        attempt,
                        max_retries = MAX_RETRIES,
                        max_block_size = ?current_block_size,
                        %err,
                        "datalake query failed, retrying"
                    );
                    last_error = Some(HandlerError::Processing(format!(
                        "datalake query failed: {err}"
                    )));
                    // Adaptive halving: shrink the next attempt's block size
                    // so an Arrow String offset overflow on a too-large block
                    // can self-correct without bouncing the message to the
                    // dead letter stream. Bounds come from configuration so
                    // operators can tune the seed and floor without a
                    // release.
                    current_block_size = Some(match current_block_size {
                        Some(size) => (size / 2).max(self.retry_config.halving_min_block_size),
                        None => self.retry_config.halving_initial_block_size,
                    });
                }
            }
        }

        Err(last_error.unwrap())
    }

    async fn transform_and_write(
        &self,
        session: &SessionContext,
        pipeline_name: &str,
        batches: &[RecordBatch],
        transforms: &[Transformation],
        destination: &dyn Destination,
    ) -> Result<(), HandlerError> {
        let schema = batches[0].schema();

        // Deregister previous batch if present, then register the new one.
        let _ = session.deregister_table(SOURCE_DATA_TABLE);
        let mem_table = MemTable::try_new(schema, vec![batches.to_vec()]).map_err(|err| {
            HandlerError::Processing(format!(
                "failed to create mem table for {pipeline_name}: {err}"
            ))
        })?;

        session
            .register_table(SOURCE_DATA_TABLE, Arc::new(mem_table))
            .map_err(|err| {
                HandlerError::Processing(format!(
                    "failed to register table for {pipeline_name}: {err}"
                ))
            })?;

        let mut transform_duration = Duration::ZERO;

        for transform in transforms {
            let transform_start = Instant::now();
            let result_batches = self
                .execute_transform(session, &transform.to_sql())
                .await
                .map_err(|err| {
                    HandlerError::Processing(format!(
                        "failed to transform {pipeline_name} for {}: {err}",
                        transform.destination_table
                    ))
                })?;
            transform_duration += transform_start.elapsed();

            // DataFusion may produce StringView (utf8_view) arrays which
            // ClickHouse 26.x doesn't support in Arrow IPC ingestion.
            // Cast them to Utf8 before writing.
            let result_batches = downcast_string_views(&result_batches);

            let row_count: usize = result_batches.iter().map(|b| b.num_rows()).sum();
            if row_count == 0 {
                continue;
            }

            let writer = destination
                .new_batch_writer(&transform.destination_table)
                .await
                .map_err(|err| {
                    HandlerError::Processing(format!(
                        "failed to create writer for {}: {err}",
                        transform.destination_table
                    ))
                })?;

            writer.write_batch(&result_batches).await.map_err(|err| {
                HandlerError::Processing(format!(
                    "failed to write to {}: {err}",
                    transform.destination_table
                ))
            })?;
        }

        self.metrics
            .record_transform_duration(pipeline_name, transform_duration.as_secs_f64());

        Ok(())
    }

    async fn execute_transform(
        &self,
        session: &SessionContext,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, datafusion::error::DataFusionError> {
        let dataframe = session.sql(sql).await?;
        dataframe.collect().await
    }

    async fn load_checkpoint(&self, position_key: &str) -> Checkpoint {
        match self.checkpoint_store.load(position_key).await {
            Ok(Some(checkpoint)) => checkpoint,
            Ok(None) => Checkpoint {
                watermark: DateTime::<Utc>::UNIX_EPOCH,
                cursor_values: None,
            },
            Err(err) => {
                warn!(
                    position_key,
                    %err,
                    "failed to load checkpoint, starting from epoch"
                );
                Checkpoint {
                    watermark: DateTime::<Utc>::UNIX_EPOCH,
                    cursor_values: None,
                }
            }
        }
    }

    fn build_query_params(
        &self,
        last_watermark: &DateTime<Utc>,
        context: &PipelineContext,
    ) -> Value {
        let mut params = serde_json::Map::new();
        params.insert(
            "last_watermark".to_string(),
            Value::String(last_watermark.format(TIMESTAMP_FORMAT).to_string()),
        );
        params.insert(
            "watermark".to_string(),
            Value::String(context.watermark.format(TIMESTAMP_FORMAT).to_string()),
        );
        for (key, value) in &context.base_conditions {
            params.insert(key.clone(), Value::String(value.clone()));
        }
        Value::Object(params)
    }
}

/// Cast StringView / List<StringView> columns to Utf8 / List<Utf8>.
/// DataFusion 53+ produces StringView for `make_array(concat(...))` results,
/// but ClickHouse 26.x rejects the `utf8_view` Arrow type during IPC ingestion.
fn downcast_string_views(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    batches
        .iter()
        .map(|batch| {
            let schema = batch.schema();
            let mut new_fields: Vec<Field> = Vec::new();
            let mut new_columns: Vec<ArrayRef> = Vec::new();
            let mut changed = false;

            for (i, field) in schema.fields().iter().enumerate() {
                let col = batch.column(i);
                match field.data_type() {
                    DataType::Utf8View => {
                        let casted = compute::cast(col, &DataType::Utf8)
                            .expect("StringView -> Utf8 cast should not fail");
                        new_fields.push(Field::new(
                            field.name(),
                            DataType::Utf8,
                            field.is_nullable(),
                        ));
                        new_columns.push(casted);
                        changed = true;
                    }
                    DataType::List(inner) if *inner.data_type() == DataType::Utf8View => {
                        let target = DataType::List(Arc::new(Field::new(
                            inner.name(),
                            DataType::Utf8,
                            inner.is_nullable(),
                        )));
                        let casted = compute::cast(col, &target)
                            .expect("List<StringView> -> List<Utf8> cast should not fail");
                        new_fields.push(Field::new(field.name(), target, field.is_nullable()));
                        new_columns.push(casted);
                        changed = true;
                    }
                    _ => {
                        new_fields.push(field.as_ref().clone());
                        new_columns.push(Arc::clone(col));
                    }
                }
            }

            if changed {
                let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
                RecordBatch::try_new(new_schema, new_columns)
                    .expect("schema/column mismatch in downcast_string_views")
            } else {
                batch.clone()
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::CheckpointError;
    use crate::modules::sdlc::datalake::DatalakeError;
    use crate::modules::sdlc::plan::ExtractQuery;
    use crate::modules::sdlc::plan::ast::{Expr, Op, Query, SelectExpr, TableRef};
    use crate::modules::sdlc::test_helpers::test_metrics;
    use crate::testkit::MockDestination;
    use arrow::array::{BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
    use async_trait::async_trait;
    use std::sync::Mutex;

    fn simple_extract_query(batch_size: u64) -> ExtractQuery {
        let base_query = Query {
            select: vec![
                SelectExpr::bare(Expr::col("", "id")),
                SelectExpr::bare(Expr::col("", "name")),
                SelectExpr::new(Expr::raw("_siphon_replicated_at"), "_version"),
                SelectExpr::new(Expr::raw("_siphon_deleted"), "_deleted"),
            ],
            from: TableRef::scan("source_table", None),
            where_clause: Some(
                Expr::and_all([
                    Some(Expr::binary(
                        Op::Gt,
                        Expr::raw("_siphon_replicated_at"),
                        Expr::param("last_watermark", "String"),
                    )),
                    Some(Expr::binary(
                        Op::Le,
                        Expr::raw("_siphon_replicated_at"),
                        Expr::param("watermark", "String"),
                    )),
                ])
                .unwrap(),
            ),
            order_by: vec![],
            limit: None,
        };

        ExtractQuery::new(base_query, vec!["id".to_string()], batch_size)
    }

    fn simple_plan(name: &str) -> PipelinePlan {
        let transform_query = Query {
            select: vec![
                SelectExpr::bare(Expr::col("", "id")),
                SelectExpr::bare(Expr::col("", "name")),
            ],
            from: TableRef::scan(SOURCE_DATA_TABLE, None),
            where_clause: None,
            order_by: vec![],
            limit: None,
        };

        PipelinePlan {
            name: name.to_string(),
            extract_query: simple_extract_query(1000),
            transforms: vec![Transformation {
                query: transform_query,
                destination_table: format!("gl_{}", name.to_lowercase()),
            }],
        }
    }

    fn test_context() -> PipelineContext {
        PipelineContext {
            watermark: "2024-06-15T12:00:00Z".parse().unwrap(),
            position_key: "test".to_string(),
            base_conditions: BTreeMap::new(),
        }
    }

    fn test_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
            ArrowField::new("_version", ArrowDataType::Int64, false),
            ArrowField::new("_deleted", ArrowDataType::Boolean, false),
        ]));

        let ids: Vec<i64> = (1..=rows as i64).collect();
        let names: Vec<Option<&str>> = (0..rows).map(|_| Some("test")).collect();
        let versions: Vec<i64> = (1..=rows as i64).collect();
        let deleted: Vec<bool> = vec![false; rows];

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(versions)),
                Arc::new(BooleanArray::from(deleted)),
            ],
        )
        .unwrap()
    }

    use crate::modules::sdlc::test_helpers::EmptyDatalake;
    use crate::modules::sdlc::test_helpers::FailingDatalake;
    use crate::nats::ProgressNotifier;

    struct MultiBatchDatalake {
        call_count: Mutex<u32>,
        batch_size: usize,
    }

    #[async_trait]
    impl DatalakeQuery for MultiBatchDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<crate::modules::sdlc::datalake::RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn query_batches(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            let mut count = self.call_count.lock().unwrap();
            *count += 1;

            let rows = if *count == 1 {
                self.batch_size
            } else {
                self.batch_size / 2
            };

            Ok(vec![test_batch(rows)])
        }
    }

    struct RecordingCheckpointStore {
        state: Mutex<Option<Checkpoint>>,
    }

    impl RecordingCheckpointStore {
        fn new() -> Self {
            Self {
                state: Mutex::new(None),
            }
        }

        fn current_state(&self) -> Option<Checkpoint> {
            self.state.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl CheckpointStore for RecordingCheckpointStore {
        async fn load(&self, _key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
            Ok(self.state.lock().unwrap().clone())
        }

        async fn save_progress(
            &self,
            _key: &str,
            checkpoint: &Checkpoint,
        ) -> Result<(), CheckpointError> {
            *self.state.lock().unwrap() = Some(checkpoint.clone());
            Ok(())
        }

        async fn save_completed(
            &self,
            _key: &str,
            watermark: &DateTime<Utc>,
        ) -> Result<(), CheckpointError> {
            *self.state.lock().unwrap() = Some(Checkpoint {
                watermark: *watermark,
                cursor_values: None,
            });
            Ok(())
        }
    }

    #[tokio::test]
    async fn empty_datalake_completes_without_error() {
        let pipeline = Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            Default::default(),
        );
        let destination = MockDestination::new();

        let result = pipeline
            .run(
                &[simple_plan("Test")],
                &test_context(),
                &destination,
                &ProgressNotifier::noop(),
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn multi_batch_paginates_and_completes() {
        let store = Arc::new(RecordingCheckpointStore::new());
        let mut plan = simple_plan("Test");
        plan.extract_query = simple_extract_query(10);

        let pipeline = Pipeline::new(
            Arc::new(MultiBatchDatalake {
                call_count: Mutex::new(0),
                batch_size: 10,
            }),
            store.clone(),
            test_metrics(),
            Default::default(),
        );
        let destination = MockDestination::new();

        let result = pipeline
            .run(
                &[plan],
                &test_context(),
                &destination,
                &ProgressNotifier::noop(),
            )
            .await;

        assert!(result.is_ok());

        let final_state = store.current_state().unwrap();
        assert!(final_state.cursor_values.is_none(), "should be completed");
    }

    #[tokio::test]
    async fn continues_past_individual_failures() {
        let pipeline = Pipeline::new(
            Arc::new(FailingDatalake),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            Default::default(),
        );
        let destination = MockDestination::new();

        let plans = vec![simple_plan("First"), simple_plan("Second")];
        let result = pipeline
            .run(
                &plans,
                &test_context(),
                &destination,
                &ProgressNotifier::noop(),
            )
            .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("First"), "should mention first failure");
        assert!(err_msg.contains("Second"), "should mention second failure");
    }

    /// Records every `max_block_size` it sees and replays a configurable
    /// success/failure pattern. Lets us assert that the per-plan override
    /// flows through and that the adaptive halving on retry halves on each
    /// attempt.
    struct RecordingDatalake {
        calls: Mutex<Vec<Option<u64>>>,
        responses: Mutex<Vec<Result<Vec<RecordBatch>, &'static str>>>,
    }

    impl RecordingDatalake {
        fn with_responses(responses: Vec<Result<Vec<RecordBatch>, &'static str>>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                responses: Mutex::new(responses),
            }
        }

        fn observed_block_sizes(&self) -> Vec<Option<u64>> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DatalakeQuery for RecordingDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<crate::modules::sdlc::datalake::RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn query_batches(
            &self,
            _sql: &str,
            _params: Value,
            max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            self.calls.lock().unwrap().push(max_block_size);
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                return Ok(vec![]);
            }
            match responses.remove(0) {
                Ok(batches) => Ok(batches),
                Err(msg) => Err(DatalakeError::Query(msg.to_string())),
            }
        }
    }

    #[tokio::test]
    async fn extract_first_attempt_uses_datalake_default() {
        let datalake = Arc::new(RecordingDatalake::with_responses(vec![Ok(vec![
            test_batch(1),
        ])]));
        let pipeline = Pipeline::new(
            datalake.clone(),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            Default::default(),
        );

        let _ = pipeline
            .run(
                &[simple_plan("Test")],
                &test_context(),
                &MockDestination::new(),
                &ProgressNotifier::noop(),
            )
            .await;

        let observed = datalake.observed_block_sizes();
        assert_eq!(
            observed.first().copied(),
            Some(None),
            "first attempt should pass None so the datalake default applies; observed: {observed:?}"
        );
    }

    #[tokio::test]
    async fn extract_halves_max_block_size_on_each_retry() {
        // Three failures then a success on the fourth attempt. The first
        // attempt uses the datalake default (None). Subsequent attempts seed
        // at retry_config.halving_initial_block_size (8000 here) and halve
        // from there: 8000 → 4000 → 2000.
        let datalake = Arc::new(RecordingDatalake::with_responses(vec![
            Err("simulated arrow capacity overflow"),
            Err("simulated arrow capacity overflow"),
            Err("simulated arrow capacity overflow"),
            Ok(vec![test_batch(1)]),
        ]));
        let pipeline = Pipeline::new(
            datalake.clone(),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            DatalakeRetryConfig {
                halving_initial_block_size: 8_000,
                halving_min_block_size: 1024,
            },
        );

        let result = pipeline
            .run(
                &[simple_plan("Test")],
                &test_context(),
                &MockDestination::new(),
                &ProgressNotifier::noop(),
            )
            .await;
        assert!(result.is_ok(), "should recover after halving: {result:?}");

        let observed = datalake.observed_block_sizes();
        assert_eq!(
            observed,
            vec![None, Some(8_000), Some(4_000), Some(2_000)],
            "block size should halve on each retry; observed: {observed:?}"
        );
    }

    #[tokio::test]
    async fn extract_halving_respects_min_block_size_floor() {
        // Halving stops at halving_min_block_size and stays there for
        // subsequent retries.
        let datalake = Arc::new(RecordingDatalake::with_responses(vec![
            Err("simulated arrow capacity overflow"),
            Err("simulated arrow capacity overflow"),
            Err("simulated arrow capacity overflow"),
            Ok(vec![test_batch(1)]),
        ]));
        let pipeline = Pipeline::new(
            datalake.clone(),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            DatalakeRetryConfig {
                halving_initial_block_size: 4_096,
                halving_min_block_size: 2_048,
            },
        );

        let _ = pipeline
            .run(
                &[simple_plan("Test")],
                &test_context(),
                &MockDestination::new(),
                &ProgressNotifier::noop(),
            )
            .await;

        let observed = datalake.observed_block_sizes();
        assert_eq!(
            observed,
            vec![None, Some(4_096), Some(2_048), Some(2_048)],
            "halving should clamp at the configured floor; observed: {observed:?}"
        );
    }

    #[tokio::test]
    async fn resumes_from_stored_cursor() {
        let store = Arc::new(RecordingCheckpointStore {
            state: Mutex::new(Some(Checkpoint {
                watermark: "2024-06-15T12:00:00Z".parse().unwrap(),
                cursor_values: Some(vec!["5".to_string()]),
            })),
        });

        let pipeline = Pipeline::new(
            Arc::new(EmptyDatalake),
            store,
            test_metrics(),
            Default::default(),
        );
        let destination = MockDestination::new();

        let result = pipeline
            .run(
                &[simple_plan("Test")],
                &test_context(),
                &destination,
                &ProgressNotifier::noop(),
            )
            .await;

        assert!(result.is_ok());
    }
}

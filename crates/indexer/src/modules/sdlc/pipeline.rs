//! SDLC indexing pipeline: extract a whole page, transform it, and bulk-write
//! each destination table, overlapping the next page's read with the writes.

use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tracing::{debug, info};

use crate::durability::RunDurability;
use crate::handler::HandlerError;
use crate::nats::ProgressNotifier;
use crate::observer::IndexingObserver;

use super::extract::{ExtractRunContext, ExtractSession, Extractor};
use super::metrics::SdlcMetrics;
use super::plan::Plan;
use super::transform::{BlockTransform, TransformRegistry};

/// `read_*` count the rows/bytes actually returned from the datalake; `scanned_*`
/// ClickHouse's storage-scan cost from the summary; `written_*` the transformed
/// rows/bytes inserted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::modules::sdlc) struct PipelineStats {
    pub read_rows: u64,
    pub read_bytes: u64,
    pub scanned_rows: u64,
    pub scanned_bytes: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
    pub duration_ms: u64,
    // Per-phase time can exceed duration_ms: extract and write overlap via tokio::join!.
    pub extract_ms: u64,
    pub transform_ms: u64,
    pub write_ms: u64,
}

impl PipelineStats {
    pub(in crate::modules::sdlc) fn merge(&mut self, other: PipelineStats) {
        self.read_rows += other.read_rows;
        self.read_bytes += other.read_bytes;
        self.scanned_rows += other.scanned_rows;
        self.scanned_bytes += other.scanned_bytes;
        self.written_rows += other.written_rows;
        self.written_bytes += other.written_bytes;
        self.duration_ms = self.duration_ms.max(other.duration_ms);
        self.extract_ms += other.extract_ms;
        self.transform_ms += other.transform_ms;
        self.write_ms += other.write_ms;
    }
}

pub(in crate::modules::sdlc) struct PipelineContext {
    pub writer: Arc<crate::clickhouse::ClickHouseWriter>,
    pub progress: ProgressNotifier,
    pub observer: Arc<std::sync::Mutex<dyn IndexingObserver>>,
}

pub(in crate::modules::sdlc) struct Pipeline {
    metrics: SdlcMetrics,
    registry: Arc<TransformRegistry>,
}

impl Pipeline {
    pub fn new(metrics: SdlcMetrics) -> Self {
        Self {
            metrics,
            registry: Arc::new(TransformRegistry::default()),
        }
    }

    /// Override the default (empty) registry with one carrying custom
    /// transforms. `data_fusion` plans build inline and need no registry entry.
    pub fn with_registry(mut self, registry: Arc<TransformRegistry>) -> Self {
        self.registry = registry;
        self
    }

    pub async fn run_plan(
        &self,
        context: &PipelineContext,
        plan: &Plan,
        extractor: &dyn Extractor,
        extract_context: ExtractRunContext,
    ) -> Result<PipelineStats, HandlerError> {
        let extract_run = extractor.start_extraction(extract_context).await?;
        {
            let mut observer = context.observer.lock().unwrap();
            observer.set_indexing_mode(extract_run.indexing_mode);
        }
        let durability = RunDurability::for_mode(extract_run.indexing_mode);
        let mut session_futures = FuturesUnordered::new();
        for session in extract_run.sessions {
            session_futures.push(self.run_extract_session(context, plan, session, durability));
        }

        let mut stats = PipelineStats::default();
        let mut errors = Vec::new();
        while let Some(result) = session_futures.next().await {
            match result {
                Ok(session_stats) => stats.merge(session_stats),
                Err(error) => errors.push(error.to_string()),
            }
        }
        if !errors.is_empty() {
            return Err(HandlerError::Processing(format!(
                "extract session failures: {}",
                errors.join("; ")
            )));
        }
        extract_run.completion.finish_extraction().await?;
        Ok(stats)
    }

    async fn run_extract_session(
        &self,
        context: &PipelineContext,
        plan: &Plan,
        mut session: Box<dyn ExtractSession>,
        durability: RunDurability,
    ) -> Result<PipelineStats, HandlerError> {
        let started_at = Instant::now();
        let transform = self.registry.build(plan)?;
        let outputs = transform.outputs().to_vec();
        let mut stats = PipelineStats::default();
        let mut page = session.get_next_page().await?;
        let mut page_number = 0_u64;

        while let Some(current_page) = page {
            page_number += 1;
            let rows_in_page = current_page.rows();
            let bytes_in_page = current_page.bytes();
            stats.read_rows += rows_in_page;
            stats.read_bytes += bytes_in_page;
            stats.scanned_rows += current_page.stats.scanned_rows;
            stats.scanned_bytes += current_page.stats.scanned_bytes;
            stats.extract_ms += current_page.stats.elapsed_ms;

            let transform_start = Instant::now();
            let grouped = self
                .transform_page(transform.as_ref(), &current_page.batches)
                .await?;
            let transform_elapsed = transform_start.elapsed();
            self.metrics
                .record_transform_duration(transform_elapsed.as_secs_f64());
            stats.transform_ms += transform_elapsed.as_millis() as u64;

            let mut write_futures = FuturesUnordered::new();
            for (index, batches) in grouped.into_iter().enumerate() {
                if batches.is_empty() {
                    continue;
                }
                let table = outputs[index].clone();
                let w = Arc::clone(&context.writer);
                let d = durability.data_writes;
                write_futures.push(async move {
                    w.write(&table, batches, d)
                        .await
                        .map_err(|e| HandlerError::Processing(e.to_string()))
                });
            }

            {
                let mut observer = context.observer.lock().unwrap();
                observer.extracted(rows_in_page, bytes_in_page);
            }

            let write_start = Instant::now();
            let drain_writes = async {
                while let Some(report) = write_futures.next().await {
                    let report = report?;
                    let mut observer = context.observer.lock().unwrap();
                    observer.record_graph_write(&report.table, report.rows, report.bytes);
                    stats.written_rows += report.rows;
                    stats.written_bytes += report.bytes;
                }
                Ok::<_, HandlerError>(write_start.elapsed())
            };

            let (write_elapsed, next_page) = if current_page.has_more {
                let (write_result, extract_result) =
                    tokio::join!(drain_writes, session.get_next_page(),);
                (write_result?, extract_result?)
            } else {
                (drain_writes.await?, None)
            };
            stats.write_ms += write_elapsed.as_millis() as u64;

            info!(
                page = page_number,
                rows = rows_in_page,
                scanned_rows = current_page.stats.scanned_rows,
                extract_ms = current_page.stats.elapsed_ms,
                transform_ms = transform_elapsed.as_millis() as u64,
                write_ms = write_elapsed.as_millis() as u64,
                "page indexed"
            );

            session.save_page_resume(&current_page.resume).await?;
            context.progress.notify_in_progress().await;
            page = next_page;
        }

        session.save_completed(durability.completion).await?;

        let elapsed = started_at.elapsed();
        stats.duration_ms = elapsed.as_millis() as u64;
        self.metrics
            .record_pipeline_completion(&plan.name, elapsed.as_secs_f64());

        {
            let mut observer = context.observer.lock().unwrap();
            observer.record_datalake_read(stats.read_rows, stats.read_bytes);
            observer.record_datalake_scan(stats.scanned_rows, stats.scanned_bytes);
            observer.record_duration(stats.duration_ms);
        }

        if stats.written_rows > 0 || stats.read_rows > 0 {
            info!(
                read_rows = stats.read_rows,
                scanned_rows = stats.scanned_rows,
                written_rows = stats.written_rows,
                duration_ms = stats.duration_ms,
                extract_ms = stats.extract_ms,
                transform_ms = stats.transform_ms,
                write_ms = stats.write_ms,
                "pipeline completed"
            );
        } else {
            debug!(
                duration_ms = stats.duration_ms,
                "pipeline completed with no data"
            );
        }

        Ok(stats)
    }

    /// Groups output rows by destination table so each table is written as one bulk insert.
    async fn transform_page(
        &self,
        transform: &dyn BlockTransform,
        batches: &[RecordBatch],
    ) -> Result<Vec<Vec<RecordBatch>>, HandlerError> {
        let mut grouped: Vec<Vec<RecordBatch>> = vec![Vec::new(); transform.outputs().len()];
        for block in batches {
            for output in transform.transform(block).await? {
                grouped[output.output_index].push(output.batch);
            }
        }
        Ok(grouped)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, VecDeque};
    use std::sync::{Arc, Mutex};

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use tokio::sync::Barrier;

    use super::*;
    use crate::durability::WriteDurability;
    use crate::modules::sdlc::extract::{
        ExtractPage, ExtractPageStats, ExtractResume, ExtractRun, ExtractRunCompletion,
        ExtractSession, Extractor, MemoryExtractor,
    };
    use crate::modules::sdlc::plan::{
        ClickHouseExtractPlan, ExtractPlan, ExtractTemplate, TransformSpec, Transformation,
    };
    use crate::modules::sdlc::test_helpers::test_metrics;
    use crate::observer::{IndexingMode, NoOpObserver};
    use crate::testkit::test_writer;

    struct RecordingExtractor {
        sessions: Mutex<Option<Vec<Box<dyn ExtractSession>>>>,
        completion_events: Arc<Mutex<Vec<&'static str>>>,
    }

    struct RecordingExtractSession {
        pages: VecDeque<ExtractPage>,
        events: Arc<Mutex<Vec<&'static str>>>,
        first_page_barrier: Option<Arc<Barrier>>,
    }

    struct RecordingExtractCompletion {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    fn simple_plan() -> Plan {
        Plan {
            name: "Test".to_string(),
            target: "Test".to_string(),
            scope: ontology::EtlScope::Namespaced,
            extract: ExtractPlan::ClickHouse(ClickHouseExtractPlan {
                template: ExtractTemplate::new(
                    "SELECT id, name, _siphon_watermark AS _version, _siphon_deleted AS _deleted FROM source_table WHERE 1=1 {{filters}} ORDER BY id LIMIT {{batch_size}}".to_string(),
                )
                .expect("valid template"),
                watermark_column: "_siphon_watermark".to_string(),
                deleted_column: "_siphon_deleted".to_string(),
                sort_key: vec!["id".to_string()],
                batch_size: 2,
            }),
            transform: TransformSpec::DataFusion(vec![Transformation {
                sql: format!(
                    "SELECT id, name FROM {}",
                    crate::modules::sdlc::plan::SOURCE_DATA_TABLE
                ),
                destination_table: "gl_test".to_string(),
                dict_encode_columns: HashSet::new(),
            }]),
        }
    }

    fn extract_page(ids: &[i64], has_more: bool) -> ExtractPage {
        let names = ids
            .iter()
            .map(|id| format!("name-{id}"))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names)),
            ],
        )
        .expect("valid batch");
        ExtractPage {
            batches: vec![batch],
            row_count: ids.len() as u64,
            resume: ExtractResume::from_source_payload("memory", 1, &ids).unwrap(),
            stats: ExtractPageStats {
                scanned_rows: ids.len() as u64,
                scanned_bytes: 100,
                elapsed_ms: 2,
            },
            has_more,
        }
    }

    fn pipeline_context() -> PipelineContext {
        PipelineContext {
            writer: test_writer(),
            progress: crate::nats::ProgressNotifier::noop(),
            observer: Arc::new(Mutex::new(NoOpObserver)),
        }
    }

    fn extract_run_context() -> ExtractRunContext {
        ExtractRunContext {
            position_key: "ns.1.Test".to_string(),
            requested_watermark: "2026-07-15T00:00:00Z".parse().unwrap(),
            traversal_path: Some("1/".to_string()),
        }
    }

    #[tokio::test]
    async fn memory_extractor_runs_the_unchanged_datafusion_transform() {
        let extractor = MemoryExtractor::new(vec![extract_page(&[1, 2], false)]);
        let stats = Pipeline::new(test_metrics())
            .run_plan(
                &pipeline_context(),
                &simple_plan(),
                &extractor,
                extract_run_context(),
            )
            .await
            .expect("memory extraction should complete");

        assert_eq!(stats.read_rows, 2);
        assert_eq!(stats.written_rows, 2);
        assert_eq!(stats.scanned_rows, 2);
        assert_eq!(stats.extract_ms, 2);
    }

    #[tokio::test]
    async fn next_page_starts_before_current_resume_is_saved() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let extractor = RecordingExtractor::new(vec![RecordingExtractSession {
            pages: vec![extract_page(&[1, 2], true), extract_page(&[3], false)].into(),
            events: Arc::clone(&events),
            first_page_barrier: None,
        }]);

        Pipeline::new(test_metrics())
            .run_plan(
                &pipeline_context(),
                &simple_plan(),
                &extractor,
                extract_run_context(),
            )
            .await
            .expect("pipeline should complete");

        let events = events.lock().unwrap();
        let second_read = events
            .iter()
            .position(|event| *event == "read")
            .and_then(|first| {
                events[first + 1..]
                    .iter()
                    .position(|event| *event == "read")
                    .map(|position| first + position + 1)
            })
            .expect("second page should be read");
        let first_save = events
            .iter()
            .position(|event| *event == "save")
            .expect("first resume should be saved");
        assert!(second_read < first_save, "events: {events:?}");
    }

    #[tokio::test]
    async fn extract_sessions_run_concurrently() {
        let barrier = Arc::new(Barrier::new(2));
        let events = Arc::new(Mutex::new(Vec::new()));
        let sessions = (0..2)
            .map(|_| RecordingExtractSession {
                pages: vec![extract_page(&[1], false)].into(),
                events: Arc::clone(&events),
                first_page_barrier: Some(Arc::clone(&barrier)),
            })
            .collect();
        let extractor = RecordingExtractor::new(sessions);

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            Pipeline::new(test_metrics()).run_plan(
                &pipeline_context(),
                &simple_plan(),
                &extractor,
                extract_run_context(),
            ),
        )
        .await
        .expect("sessions should reach the barrier concurrently")
        .expect("pipeline should complete");
    }

    impl RecordingExtractor {
        fn new(sessions: Vec<RecordingExtractSession>) -> Self {
            Self {
                sessions: Mutex::new(Some(
                    sessions
                        .into_iter()
                        .map(|session| Box::new(session) as Box<dyn ExtractSession>)
                        .collect(),
                )),
                completion_events: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl Extractor for RecordingExtractor {
        async fn start_extraction(
            &self,
            _context: ExtractRunContext,
        ) -> Result<ExtractRun, HandlerError> {
            Ok(ExtractRun {
                indexing_mode: IndexingMode::Full,
                sessions: self.sessions.lock().unwrap().take().unwrap_or_default(),
                completion: Box::new(RecordingExtractCompletion {
                    events: Arc::clone(&self.completion_events),
                }),
            })
        }
    }

    #[async_trait]
    impl ExtractSession for RecordingExtractSession {
        async fn get_next_page(&mut self) -> Result<Option<ExtractPage>, HandlerError> {
            if let Some(barrier) = self.first_page_barrier.take() {
                barrier.wait().await;
            }
            self.events.lock().unwrap().push("read");
            Ok(self.pages.pop_front())
        }

        async fn save_page_resume(&self, _resume: &ExtractResume) -> Result<(), HandlerError> {
            self.events.lock().unwrap().push("save");
            Ok(())
        }

        async fn save_completed(&self, _durability: WriteDurability) -> Result<(), HandlerError> {
            self.events.lock().unwrap().push("complete");
            Ok(())
        }
    }

    #[async_trait]
    impl ExtractRunCompletion for RecordingExtractCompletion {
        async fn finish_extraction(self: Box<Self>) -> Result<(), HandlerError> {
            self.events.lock().unwrap().push("finish");
            Ok(())
        }
    }

    #[test]
    fn pipeline_stats_merge_sums_counters_and_keeps_longest_duration() {
        let mut total = PipelineStats {
            read_rows: 10,
            duration_ms: 20,
            ..PipelineStats::default()
        };
        total.merge(PipelineStats {
            read_rows: 5,
            duration_ms: 10,
            ..PipelineStats::default()
        });
        assert_eq!(total.read_rows, 15);
        assert_eq!(total.duration_ms, 20);
    }

    #[test]
    fn extract_context_watermark_parses_as_utc() {
        let context = extract_run_context();
        let _: DateTime<Utc> = context.requested_watermark;
    }
}

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream;

use std::sync::Arc;

use arrow::array::{BooleanArray, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use ontology::constants::WATERMARK_COLUMN;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

use super::datalake::{DatalakeError, DatalakeQuery, RecordBatchStream};
use super::metrics::SdlcMetrics;
use super::plan::Transformation;
use super::transform::{BlockTransform, TableBatch, TransformRegistry};
use crate::checkpoint::{Checkpoint, CheckpointError, CheckpointStore};
use crate::durability::WriteDurability;
use crate::handler::HandlerError;

pub(crate) fn test_metrics() -> SdlcMetrics {
    SdlcMetrics::with_meter(&crate::testkit::test_meter())
}

pub(crate) struct RecordedMetrics {
    provider: SdkMeterProvider,
    exporter: InMemoryMetricExporter,
}

impl RecordedMetrics {
    pub(crate) fn new() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        Self { provider, exporter }
    }

    pub(crate) fn sdlc_metrics(&self) -> SdlcMetrics {
        SdlcMetrics::with_meter(&self.provider.meter("test"))
    }

    /// Observation count and sum of an f64 histogram.
    pub(crate) fn histogram(&self, name: &str) -> (u64, f64) {
        self.provider.force_flush().unwrap();
        self.exporter
            .get_finished_metrics()
            .unwrap()
            .iter()
            .flat_map(|resource| resource.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .filter(|metric| metric.name() == name)
            .flat_map(|metric| match metric.data() {
                AggregatedMetrics::F64(MetricData::Histogram(histogram)) => histogram
                    .data_points()
                    .map(|point| (point.count(), point.sum()))
                    .collect::<Vec<_>>(),
                _ => panic!("{name} is not an f64 histogram"),
            })
            .fold((0, 0.0), |(count, sum), (c, s)| (count + c, sum + s))
    }
}

struct PassthroughTransform {
    outputs: Vec<String>,
}

#[async_trait]
impl BlockTransform for PassthroughTransform {
    fn name(&self) -> &str {
        "passthrough"
    }

    fn outputs(&self) -> &[String] {
        &self.outputs
    }

    async fn transform(&self, block: &RecordBatch) -> Result<Vec<TableBatch>, HandlerError> {
        Ok(vec![TableBatch {
            output_index: 0,
            batch: block.clone(),
        }])
    }
}

pub(in crate::modules::sdlc) fn passthrough_registry() -> Arc<TransformRegistry> {
    Arc::new(TransformRegistry::new(Arc::new(
        |_name, transforms: Vec<Transformation>| {
            Arc::new(PassthroughTransform {
                outputs: transforms
                    .into_iter()
                    .map(|transform| transform.destination_table)
                    .collect(),
            }) as Arc<dyn BlockTransform>
        },
    )))
}

pub(crate) struct EmptyDatalake;

#[async_trait]
impl DatalakeQuery for EmptyDatalake {
    async fn query_arrow(
        &self,
        _sql: &str,
        _params: serde_json::Value,
        _max_block_size: Option<u64>,
    ) -> Result<RecordBatchStream<'_>, DatalakeError> {
        Ok(Box::pin(stream::empty()))
    }

    async fn query_batches(
        &self,
        _sql: &str,
        _params: serde_json::Value,
        _max_block_size: Option<u64>,
    ) -> Result<Vec<RecordBatch>, DatalakeError> {
        Ok(vec![])
    }
}

/// Returns one changed row shaped like a namespaced extract (`traversal_path`, `id` sort key).
pub(crate) struct ChangedRowDatalake {
    watermark: chrono::DateTime<chrono::Utc>,
}

impl ChangedRowDatalake {
    pub(crate) fn at(watermark: chrono::DateTime<chrono::Utc>) -> Self {
        Self { watermark }
    }

    fn batch(&self) -> RecordBatch {
        let timestamp = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
            Field::new("_version", timestamp(), false),
            Field::new("_deleted", DataType::Boolean, false),
            Field::new(WATERMARK_COLUMN, timestamp(), false),
        ]));
        let micros = self.watermark.timestamp_micros();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["42/100/"])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC")),
                Arc::new(BooleanArray::from(vec![false])),
                Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC")),
            ],
        )
        .unwrap()
    }
}

#[async_trait]
impl DatalakeQuery for ChangedRowDatalake {
    async fn query_arrow(
        &self,
        _sql: &str,
        _params: serde_json::Value,
        _max_block_size: Option<u64>,
    ) -> Result<RecordBatchStream<'_>, DatalakeError> {
        Ok(Box::pin(stream::iter(vec![Ok(self.batch())])))
    }

    async fn query_batches(
        &self,
        _sql: &str,
        _params: serde_json::Value,
        _max_block_size: Option<u64>,
    ) -> Result<Vec<RecordBatch>, DatalakeError> {
        Ok(vec![self.batch()])
    }
}

pub(crate) struct FailingDatalake;

#[async_trait]
impl DatalakeQuery for FailingDatalake {
    async fn query_arrow(
        &self,
        _sql: &str,
        _params: serde_json::Value,
        _max_block_size: Option<u64>,
    ) -> Result<RecordBatchStream<'_>, DatalakeError> {
        Err(DatalakeError::Query("simulated failure".to_string()))
    }

    async fn query_batches(
        &self,
        _sql: &str,
        _params: serde_json::Value,
        _max_block_size: Option<u64>,
    ) -> Result<Vec<RecordBatch>, DatalakeError> {
        Err(DatalakeError::Query("simulated failure".to_string()))
    }
}

#[derive(Default)]
pub(crate) struct MockCheckpointStore {
    checkpoint: Option<Checkpoint>,
}

impl MockCheckpointStore {
    pub(crate) fn completed_at(watermark: chrono::DateTime<chrono::Utc>) -> Self {
        let mut checkpoint = Checkpoint::new(watermark);
        checkpoint.complete(watermark);
        Self {
            checkpoint: Some(checkpoint),
        }
    }
}

#[async_trait]
impl CheckpointStore for MockCheckpointStore {
    async fn load(&self, _key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        Ok(self.checkpoint.clone())
    }

    async fn save(
        &self,
        _key: &str,
        _checkpoint: &Checkpoint,
        _durability: WriteDurability,
    ) -> Result<(), CheckpointError> {
        Ok(())
    }

    async fn load_by_prefix(
        &self,
        _prefix: &str,
    ) -> Result<Vec<(String, Checkpoint)>, CheckpointError> {
        Ok(Vec::new())
    }

    async fn consolidate(
        &self,
        _parent_key: &str,
        _watermark: &chrono::DateTime<chrono::Utc>,
    ) -> Result<(), CheckpointError> {
        Ok(())
    }
}

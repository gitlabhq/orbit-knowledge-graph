//! Drives the pluggable [`BlockTransform`] (ADR 015) per block; the transform
//! itself — `data_fusion` SQL projection or a hand-written Rust transform —
//! is resolved from the registry and built per run.

use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use crate::handler::HandlerError;
use crate::modules::sdlc::metrics::SdlcMetrics;
use crate::modules::sdlc::transform::BlockTransform;

use super::super::page::{ExtractedPage, TransformedPage};
use super::super::stage::PageStage;

pub(in crate::modules::sdlc) struct Transform {
    block: Arc<dyn BlockTransform>,
    metrics: SdlcMetrics,
}

impl Transform {
    pub fn new(block: Arc<dyn BlockTransform>, metrics: SdlcMetrics) -> Self {
        Self { block, metrics }
    }

    /// Destination tables in output-index order, matching
    /// `TransformedPage::batches_by_table`.
    pub fn outputs(&self) -> &[String] {
        self.block.outputs()
    }

    pub fn block_name(&self) -> &str {
        self.block.name()
    }
}

#[async_trait]
impl PageStage for Transform {
    type In = ExtractedPage;
    type Out = TransformedPage;

    async fn run(&self, page: ExtractedPage) -> Result<TransformedPage, HandlerError> {
        let start = Instant::now();

        let mut batches_by_table: Vec<Vec<RecordBatch>> =
            vec![Vec::new(); self.block.outputs().len()];
        for batch in &page.batches {
            for output in self.block.transform(batch).await? {
                batches_by_table[output.output_index].push(output.batch);
            }
        }

        let transform_elapsed = start.elapsed();
        self.metrics
            .record_transform_duration(transform_elapsed.as_secs_f64());

        Ok(TransformedPage {
            batches_by_table,
            transform_elapsed,
        })
    }
}

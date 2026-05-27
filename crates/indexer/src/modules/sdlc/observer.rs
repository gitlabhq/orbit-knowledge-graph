use crate::observer::IndexingObserver;

use super::metrics::SdlcMetrics;

pub(crate) struct SdlcOtelObserver {
    metrics: SdlcMetrics,
    entity_type: Option<String>,
}

impl SdlcOtelObserver {
    pub(crate) fn new(metrics: SdlcMetrics) -> Self {
        Self {
            metrics,
            entity_type: None,
        }
    }
}

impl IndexingObserver for SdlcOtelObserver {
    fn set_entity_type(&mut self, entity_type: &str) {
        self.entity_type = Some(entity_type.to_owned());
    }

    fn extracted(&mut self, rows: u64, _bytes: u64) {
        if let Some(entity) = &self.entity_type {
            self.metrics.record_batch_rows(entity, rows);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observer::{IndexingMode, PipelineType};

    fn test_observer() -> SdlcOtelObserver {
        let metrics = SdlcMetrics::with_meter(&crate::testkit::test_meter());
        SdlcOtelObserver::new(metrics)
    }

    #[test]
    fn extracted_records_batch_rows_with_entity() {
        let mut obs = test_observer();
        obs.set_pipeline_type(PipelineType::Sdlc);
        obs.set_entity_type("MergeRequest");
        obs.set_indexing_mode(IndexingMode::Incremental);
        obs.extracted(500, 25_000);
        obs.finish();
    }

    #[test]
    fn extracted_without_entity_is_safe() {
        let mut obs = test_observer();
        obs.extracted(100, 5_000);
    }
}

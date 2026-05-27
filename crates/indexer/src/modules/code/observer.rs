use crate::observer::IndexingObserver;

use super::metrics::CodeMetrics;

pub(crate) struct CodeOtelObserver {
    metrics: CodeMetrics,
}

impl CodeOtelObserver {
    pub(crate) fn new(metrics: CodeMetrics) -> Self {
        Self { metrics }
    }
}

impl IndexingObserver for CodeOtelObserver {
    fn files_processed(&mut self, discovered: u64, parsed: u64, skipped: u64) {
        self.metrics
            .record_files_processed(discovered, "discovered");
        self.metrics.record_files_processed(parsed, "parsed");
        self.metrics.record_files_processed(skipped, "skipped");
    }

    fn nodes_indexed(&mut self, kind: &str, count: u64) {
        self.metrics.record_nodes_indexed(count, kind);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observer::{IndexingMode, PipelineType};

    fn test_observer() -> CodeOtelObserver {
        let metrics = CodeMetrics::with_meter(&crate::testkit::test_meter());
        CodeOtelObserver::new(metrics)
    }

    #[test]
    fn files_and_nodes_record_metrics() {
        let mut obs = test_observer();
        obs.set_pipeline_type(PipelineType::Code);
        obs.set_indexing_mode(IndexingMode::Full);
        obs.files_processed(500, 480, 20);
        obs.nodes_indexed("definition", 3000);
        obs.nodes_indexed("file", 480);
        obs.finish();
    }
}

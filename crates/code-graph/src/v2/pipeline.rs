use code_graph_types::CanonicalResult;
use std::path::Path;

use crate::linker::v2::{GraphBuilder, GraphData};

pub struct PipelineConfig {
    pub worker_threads: usize,
    pub max_file_size: u64,
    pub respect_gitignore: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            worker_threads: num_cpus::get(),
            max_file_size: 1_000_000,
            respect_gitignore: true,
        }
    }
}

pub struct PipelineResult {
    pub graph: GraphData,
    pub stats: PipelineStats,
    pub errors: Vec<PipelineError>,
}

pub struct PipelineStats {
    pub files_parsed: usize,
    pub files_skipped: usize,
    pub definitions_count: usize,
    pub imports_count: usize,
    pub references_count: usize,
    pub edges_count: usize,
}

#[derive(Debug)]
pub struct PipelineError {
    pub file_path: String,
    pub error: String,
}

/// The v2 code-graph pipeline.
///
/// Orchestrates: filesystem walk → parallel parse → canonical convert → graph build.
///
/// The filesystem walk streams `FileInfo` entries. Parsing and conversion
/// happen in parallel via rayon. Results are collected and fed into the
/// synchronous `GraphBuilder`.
pub struct Pipeline {
    config: PipelineConfig,
}

impl Pipeline {
    pub fn new(config: PipelineConfig) -> Self {
        Self { config }
    }

    /// Run the full pipeline on a directory.
    pub fn run(&self, root: &Path) -> PipelineResult {
        todo!()
    }
}

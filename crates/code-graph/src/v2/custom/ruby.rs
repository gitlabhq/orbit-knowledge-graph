use crate::linker::v2::CodeGraph;
use crate::v2::pipeline::{FileInput, LanguagePipeline, PipelineError};

/// Custom Ruby pipeline.
pub struct RubyPipeline;

impl LanguagePipeline for RubyPipeline {
    fn process_files(
        files: Vec<FileInput>,
        root_path: &str,
    ) -> Result<CodeGraph, Vec<PipelineError>> {
        let _ = &files;
        Ok(CodeGraph::from_results(vec![], root_path.to_string()))
    }
}

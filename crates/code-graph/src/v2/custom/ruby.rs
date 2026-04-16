use crate::linker::v2::{CodeGraph, GraphArena};
use crate::v2::pipeline::{FileInput, LanguagePipeline, PipelineError};

/// Custom Ruby pipeline.
pub struct RubyPipeline;

impl<'a> LanguagePipeline<'a> for RubyPipeline {
    fn process_files(
        files: Vec<FileInput>,
        root_path: &str,
        arena: &'a GraphArena,
    ) -> Result<CodeGraph<'a>, Vec<PipelineError>> {
        let _ = &files;
        Ok(CodeGraph::from_results(
            vec![],
            root_path.to_string(),
            arena,
        ))
    }
}

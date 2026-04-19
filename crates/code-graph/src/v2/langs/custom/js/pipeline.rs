use crate::v2::linker::CodeGraph;
use crate::v2::pipeline::{FileInput, LanguagePipeline, PipelineError, PipelineOutput};
use rustc_hash::FxHashMap;

use super::phase1::analyze_files;
use super::resolver::attach_resolution_edges;
use super::{JsModuleGraphBuilder, JsPhase1FileInfo};

pub struct JsPipeline;

impl LanguagePipeline for JsPipeline {
    fn process_files(
        files: &[FileInput],
        root_path: &str,
    ) -> Result<PipelineOutput, Vec<PipelineError>> {
        if files.is_empty() {
            return Ok(PipelineOutput::Graph(Box::new(CodeGraph::new_with_root(
                root_path.to_string(),
            ))));
        }

        let (analyzed_files, errors) = analyze_files(files, root_path);
        if analyzed_files.is_empty() {
            return Err(errors);
        }

        let mut builder = JsModuleGraphBuilder::new(root_path.to_string());
        let mut file_infos: FxHashMap<String, JsPhase1FileInfo> = FxHashMap::default();
        for file in &analyzed_files {
            let info = builder.add_file(file.phase1.clone());
            file_infos.insert(file.relative_path.clone(), info);
        }

        let (mut graph, modules) = builder.into_parts();
        attach_resolution_edges(
            &mut graph,
            &analyzed_files,
            &file_infos,
            &modules,
            root_path,
        );
        graph.finalize();

        if errors.is_empty() {
            Ok(PipelineOutput::Graph(Box::new(graph)))
        } else {
            for error in &errors {
                log::warn!("[v2-js] skipped {}: {}", error.file_path, error.error);
            }
            Ok(PipelineOutput::Graph(Box::new(graph)))
        }
    }
}

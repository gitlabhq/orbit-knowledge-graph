use std::path::Path;

use crate::v2::linker::CodeGraph;
use crate::v2::pipeline::{FileInput, LanguagePipeline, PipelineError, PipelineOutput};
use rustc_hash::FxHashMap;

use super::extract::{ResolvedJsFile, analyze_files};
use super::resolve::attach_resolution_edges;
use super::{JsModuleGraphBuilder, JsPhase1FileInfo, WorkspaceProbe};

pub struct JsPipeline;

impl LanguagePipeline for JsPipeline {
    fn process_files(
        files: &[FileInput],
        root_path: &str,
        tracer: &crate::v2::trace::Tracer,
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
        let mut resolved_files = Vec::with_capacity(analyzed_files.len());
        for file in analyzed_files {
            let info = builder.add_file(file.phase1);
            file_infos.insert(file.relative_path.clone(), info);
            resolved_files.push(ResolvedJsFile {
                relative_path: file.relative_path,
                analysis: file.analysis,
            });
        }

        // One probe: every manifest/config file JS resolution cares about
        // is read exactly once here, then shared with the resolver,
        // evaluator, and tsconfig discovery below.
        let probe = WorkspaceProbe::load(Path::new(root_path), files);

        let (mut graph, modules) = builder.into_parts();
        attach_resolution_edges(
            &mut graph,
            &resolved_files,
            &file_infos,
            &modules,
            &probe,
            tracer,
        );
        graph.finalize(tracer);

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

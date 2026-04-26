use std::path::Path;

use std::sync::Arc;

use crate::v2::error::CodeGraphError;
use crate::v2::pipeline::{BatchTx, FileInput, LanguagePipeline, PipelineContext, PipelineError};
use rustc_hash::FxHashMap;

use super::extract::{ResolvedJsFile, analyze_files};
use super::resolve::attach_resolution_edges;
use super::{JsModuleGraphBuilder, JsPhase1FileInfo, WorkspaceProbe};

pub struct JsPipeline;

impl LanguagePipeline for JsPipeline {
    fn process_files(
        files: &[FileInput],
        ctx: &Arc<PipelineContext>,
        btx: &BatchTx<'_>,
    ) -> Result<(), Vec<PipelineError>> {
        let root_path = ctx.root_path.as_str();
        let tracer = &ctx.tracer;
        if files.is_empty() {
            return Ok(());
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

        btx.send_graph(graph);

        if !errors.is_empty() {
            for error in &errors {
                tracing::warn!(path = %error.file_path, error = %error.error, "js: skipped file");
                match crate::v2::pipeline::classify_skip_message(&error.error) {
                    Some(reason) => ctx.record_file_skipped(error.file_path.clone(), reason),
                    None => ctx.record_error(CodeGraphError::ParseFailed {
                        path: error.file_path.clone(),
                        message: error.error.clone(),
                    }),
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::langs::custom::js::extract::MAX_FILE_BYTES;
    use crate::v2::pipeline::PipelineConfig;
    use crate::v2::sink::GraphConverter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    struct NoopConverter;
    impl GraphConverter for NoopConverter {
        fn convert(
            &self,
            _graph: crate::v2::linker::CodeGraph,
        ) -> Result<Vec<(String, RecordBatch)>, crate::v2::SinkError> {
            Ok(Vec::new())
        }
    }

    fn make_ctx(root: &Path) -> Arc<PipelineContext> {
        Arc::new(PipelineContext {
            config: PipelineConfig::default(),
            tracer: crate::v2::trace::Tracer::new(false),
            root_path: root.to_string_lossy().into_owned(),
            graph_errors: Mutex::new(Vec::new()),
            files_skipped: Mutex::new(Vec::new()),
        })
    }

    fn run_js(ctx: &Arc<PipelineContext>, files: &[FileInput]) {
        let conv = NoopConverter;
        let (tx, _rx) = crossbeam_channel::unbounded();
        let d = AtomicUsize::new(0);
        let i = AtomicUsize::new(0);
        let e = AtomicUsize::new(0);
        let errs = Mutex::new(Vec::new());
        let btx = BatchTx::new(&tx, &conv, &errs, &d, &i, &e);
        // We expect this not to return Err — at least one file must be
        // analyzable so the pipeline produces a graph and reaches the
        // skip recording path.
        let _ = JsPipeline::process_files(files, ctx, &btx);
    }

    #[test]
    fn oversize_js_file_records_skip_not_error() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let root = tmp.path();
        // A legitimate JS file so the analyzer finishes producing a graph.
        std::fs::write(root.join("ok.js"), "export const x = 1;\n").unwrap();
        // An oversize JS file (> MAX_FILE_BYTES). `analyze_files` rejects
        // it inside `safe_repo_join` with "refusing oversize file: ...".
        let big = vec![b'a'; (MAX_FILE_BYTES + 16) as usize];
        std::fs::write(root.join("big.js"), &big).unwrap();

        let ctx = make_ctx(root);
        run_js(&ctx, &["ok.js".to_string(), "big.js".to_string()]);

        let skipped = ctx.files_skipped.lock().unwrap().clone();
        let errors = ctx.graph_errors.lock().unwrap();
        assert!(
            skipped.iter().any(|s| s.reason == "oversize"),
            "expected an oversize skip, got skipped={skipped:?} errors={:?}",
            errors.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
        );
        assert!(
            errors.is_empty(),
            "oversize must not increment errors; got {:?}",
            errors.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
        );
    }

    #[test]
    fn line_too_long_js_file_records_skip_not_error() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let root = tmp.path();
        std::fs::write(root.join("ok.js"), "export const x = 1;\n").unwrap();
        // A single line longer than MAX_LINE_LENGTH (5_000). The
        // analyzer's eager line-length check raises
        // "Skipping ...: line too long (...)".
        let long_line: String = "x".repeat(5_500);
        std::fs::write(root.join("long.js"), format!("const a = '{long_line}';\n")).unwrap();

        let ctx = make_ctx(root);
        run_js(&ctx, &["ok.js".to_string(), "long.js".to_string()]);

        let skipped = ctx.files_skipped.lock().unwrap().clone();
        let errors = ctx.graph_errors.lock().unwrap();
        assert!(
            skipped.iter().any(|s| s.reason == "line_too_long"),
            "expected a line_too_long skip, got skipped={skipped:?} errors={:?}",
            errors.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
        );
        assert!(
            errors.is_empty(),
            "line_too_long must not increment errors; got {:?}",
            errors.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
        );
    }
}

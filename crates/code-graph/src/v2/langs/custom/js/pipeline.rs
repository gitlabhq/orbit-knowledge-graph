use std::path::Path;

use std::sync::Arc;

use crate::v2::error::AnalyzerError;
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

        // Route per-file outcomes to the typed collections regardless of
        // whether at least one file analyzed; the orchestrator no longer
        // double-counts skipped/errored at the language boundary.
        for (path, error) in &errors {
            match error {
                AnalyzerError::Skip { kind, detail } => {
                    tracing::warn!(path, kind = %kind, %detail, "js: skipped file");
                    ctx.record_skip(path.clone(), *kind, detail.clone());
                }
                AnalyzerError::Fault { kind, detail } => {
                    tracing::warn!(path, kind = %kind, %detail, "js: faulted file");
                    ctx.record_fault(path.clone(), *kind, detail.clone());
                }
            }
        }

        if analyzed_files.is_empty() {
            return Ok(());
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
        if ctx.config.emit_file_inventory_graph {
            graph.mark_parsed_only();
        }
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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::langs::custom::js::extract::MAX_FILE_BYTES;
    use crate::v2::pipeline::GraphStatsCounters;
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
            skipped: Mutex::new(Vec::new()),
            faults: Mutex::new(Vec::new()),
        })
    }

    fn run_js(ctx: &Arc<PipelineContext>, files: &[FileInput]) {
        let conv = NoopConverter;
        let (tx, _rx) = crossbeam_channel::unbounded();
        let dirs = AtomicUsize::new(0);
        let files_count = AtomicUsize::new(0);
        let d = AtomicUsize::new(0);
        let i = AtomicUsize::new(0);
        let e = AtomicUsize::new(0);
        let errs = Mutex::new(Vec::new());
        let btx = BatchTx::new(
            &tx,
            &conv,
            &errs,
            GraphStatsCounters::new(&dirs, &files_count, &d, &i, &e),
        );
        // We expect this not to return Err — at least one file must be
        // analyzable so the pipeline produces a graph and reaches the
        // skip recording path.
        let _ = JsPipeline::process_files(files, ctx, &btx);
    }

    #[test]
    fn oversize_js_file_records_skip_not_fault() {
        use crate::v2::error::FileSkip;
        let tmp = tempfile::tempdir().expect("temp dir");
        let root = tmp.path();
        std::fs::write(root.join("ok.js"), "export const x = 1;\n").unwrap();
        let big = vec![b'a'; (MAX_FILE_BYTES + 16) as usize];
        std::fs::write(root.join("big.js"), &big).unwrap();

        let ctx = make_ctx(root);
        run_js(&ctx, &["ok.js".to_string(), "big.js".to_string()]);

        let skipped = ctx.skipped.lock().unwrap().clone();
        let faults = ctx.faults.lock().unwrap().clone();
        assert!(
            skipped.iter().any(|s| s.kind == FileSkip::Oversize),
            "expected an oversize skip, got skipped={skipped:?} faults={faults:?}",
        );
        assert!(faults.is_empty(), "oversize must not record a fault");
    }

    #[test]
    fn line_too_long_js_file_records_skip_not_fault() {
        use crate::v2::error::FileSkip;
        let tmp = tempfile::tempdir().expect("temp dir");
        let root = tmp.path();
        std::fs::write(root.join("ok.js"), "export const x = 1;\n").unwrap();
        let long_line: String = "x".repeat(5_500);
        std::fs::write(root.join("long.js"), format!("const a = '{long_line}';\n")).unwrap();

        let ctx = make_ctx(root);
        run_js(&ctx, &["ok.js".to_string(), "long.js".to_string()]);

        let skipped = ctx.skipped.lock().unwrap().clone();
        let faults = ctx.faults.lock().unwrap().clone();
        assert!(
            skipped.iter().any(|s| s.kind == FileSkip::LineTooLong),
            "expected a line_too_long skip, got skipped={skipped:?} faults={faults:?}",
        );
        assert!(faults.is_empty(), "line_too_long must not record a fault");
    }
}

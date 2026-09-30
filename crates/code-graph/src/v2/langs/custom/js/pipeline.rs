use std::sync::Arc;

use crate::v2::error::AnalyzerError;
use crate::v2::pipeline::{
    BatchTx, FamilyFileInput, FileTimingEntry, LanguagePipeline, LanguageTimings, PipelineContext,
    PipelineError, ProgressPhase, VIRTUAL_ROOT,
};
use crate::v2::sentinel;
use rustc_hash::FxHashMap;

use super::extract::{ResolvedJsFile, analyze_files};
use super::resolve::attach_resolution_edges;
use super::{JsModuleGraphBuilder, JsPhase1FileInfo, WorkspaceProbe};

pub struct JsPipeline;

impl LanguagePipeline for JsPipeline {
    fn process_files(
        files: &[FamilyFileInput],
        ctx: &Arc<PipelineContext>,
        btx: &BatchTx<'_>,
    ) -> Result<(), Vec<PipelineError>> {
        let tracer = &ctx.tracer;
        let t0 = std::time::Instant::now();
        if files.is_empty() {
            return Ok(());
        }

        let sentinel = ctx
            .config
            .per_file_timeout
            .and_then(sentinel::spawn_sentinel);
        let sentinel_handle = sentinel.as_ref().map(|(h, _)| h);

        let progress = ctx.config.progress.as_ref();
        let (analyzed_files, errors) = analyze_files(files, ctx, sentinel_handle, progress);
        progress.files_advanced(ProgressPhase::Resolve, errors.len());
        let parse_ms = t0.elapsed().as_secs_f64() * 1000.0;

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

        let mut builder = JsModuleGraphBuilder::new(VIRTUAL_ROOT.to_string());
        let mut file_infos: FxHashMap<String, JsPhase1FileInfo> = FxHashMap::default();
        let mut resolved_files = Vec::with_capacity(analyzed_files.len());
        for file in analyzed_files {
            if ctx.is_cancelled() {
                return Ok(());
            }
            ctx.record_file_timing(FileTimingEntry {
                path: file.relative_path.clone(),
                size_bytes: file.phase1.size,
                parse_ms: file.parse_ms,
                resolve_ms: 0.0,
                total_ms: file.parse_ms,
                language: format!("{}", file.phase1.language),
            });
            let info = builder.add_file(file.phase1);
            file_infos.insert(file.relative_path.clone(), info);
            resolved_files.push(ResolvedJsFile::from_analysis(
                file.relative_path,
                file.analysis,
            ));
        }

        // One probe: every manifest/config file JS resolution cares about
        // is read exactly once here, then shared with the resolver,
        // evaluator, and tsconfig discovery below.
        let indexed_paths: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
        let probe = WorkspaceProbe::load(ctx.vfs.clone(), &indexed_paths);

        let (mut graph, modules) = builder.into_parts();
        let graph_build_ms = t0.elapsed().as_secs_f64() * 1000.0 - parse_ms;
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
            sentinel_handle,
            ctx,
        );
        if ctx.is_cancelled() {
            return Ok(());
        }
        graph.finalize(tracer);
        let total_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let resolve_ms = total_ms - parse_ms - graph_build_ms;
        let total_bytes: u64 = files.iter().map(|f| f.size).sum();

        ctx.record_language_timing(LanguageTimings {
            language: "java_script".to_string(),
            file_count: resolved_files.len(),
            total_bytes,
            parse_ms,
            graph_build_ms,
            resolve_ms,
            total_ms,
        });

        btx.send_graph(graph);

        if let Some((handle, join)) = sentinel {
            handle.shutdown();
            let _ = join.join();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::langs::custom::js::extract::MAX_FILE_BYTES;
    use crate::v2::pipeline::{GraphStatsCounters, testing};
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

    fn run_js(ctx: &Arc<PipelineContext>, files: &[FamilyFileInput]) {
        let conv = NoopConverter;
        let on_batch = |_: &str, _: RecordBatch| Ok(());
        let dirs = AtomicUsize::new(0);
        let files_count = AtomicUsize::new(0);
        let d = AtomicUsize::new(0);
        let i = AtomicUsize::new(0);
        let e = AtomicUsize::new(0);
        let errs = Mutex::new(Vec::new());
        let btx = BatchTx::new(
            &on_batch,
            &conv,
            &errs,
            GraphStatsCounters::new(&dirs, &files_count, &d, &i, &e),
        );
        let _ = JsPipeline::process_files(files, ctx, &btx);
    }

    #[test]
    fn oversize_js_file_records_skip_not_fault() {
        use crate::v2::error::FileSkip;
        let big = vec![b'a'; (MAX_FILE_BYTES + 16) as usize];
        let (ctx, files) = testing::repo(&[("ok.js", b"export const x = 1;\n"), ("big.js", &big)]);

        run_js(&ctx, &files);

        let skipped = ctx.skipped.lock().unwrap().clone();
        let faults = ctx.faults.lock().unwrap().clone();
        assert!(
            skipped.iter().any(|s| s.kind == FileSkip::Oversize),
            "expected an oversize skip, got skipped={skipped:?} faults={faults:?}",
        );
        assert!(faults.is_empty(), "oversize must not record a fault");
    }

    #[test]
    fn moderately_long_js_line_is_analyzed() {
        let long_line: String = "x".repeat(17_000);
        let source = format!("// header\nconst a = '{long_line}';\n");
        let (ctx, files) = testing::repo(&[("prettify.js", source.as_bytes())]);

        run_js(&ctx, &files);

        let skipped = ctx.skipped.lock().unwrap().clone();
        let faults = ctx.faults.lock().unwrap().clone();
        assert!(
            skipped.is_empty(),
            "moderately long generated lines should parse, got skipped={skipped:?}",
        );
        assert!(faults.is_empty(), "expected no JS faults, got {faults:?}");
    }
}

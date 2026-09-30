//! A job that outruns its budget cancels the run; the work it started must stop.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use code_graph::v2::config::Language;
use code_graph::v2::{
    BatchTx, CancellationToken, FamilyFileInput, GraphConverter, GraphStatsCounters,
    PipelineConfig, PipelineContext, Vfs, dispatch_language,
};

struct NoopConverter;

impl GraphConverter for NoopConverter {
    fn convert(
        &self,
        _graph: code_graph::v2::linker::CodeGraph,
    ) -> Result<Vec<(String, arrow::record_batch::RecordBatch)>, code_graph::v2::SinkError> {
        Ok(Vec::new())
    }
}

fn context(repo: Vfs, cancel: CancellationToken) -> Arc<PipelineContext> {
    Arc::new(PipelineContext::new(
        Arc::new(repo),
        PipelineConfig {
            cancel,
            ..Default::default()
        },
        code_graph::v2::trace::Tracer::new(false),
    ))
}

/// Bypasses the orchestrator's pre-spawn cancellation check.
fn dispatch(language: Language, ctx: &Arc<PipelineContext>, files: &[FamilyFileInput]) -> usize {
    let converter = NoopConverter;
    let on_batch = |_: &str, _: arrow::record_batch::RecordBatch| Ok(());
    let directories = AtomicUsize::new(0);
    let file_count = AtomicUsize::new(0);
    let definitions = AtomicUsize::new(0);
    let imports = AtomicUsize::new(0);
    let edges = AtomicUsize::new(0);
    let errors = Mutex::new(Vec::new());
    let btx = BatchTx::new(
        &on_batch,
        &converter,
        &errors,
        GraphStatsCounters::new(&directories, &file_count, &definitions, &imports, &edges),
    );
    dispatch_language(language, files, ctx, &btx)
        .expect("language has a registered pipeline")
        .expect("pipeline error");
    definitions.load(Ordering::Relaxed)
}

fn graphed_files(ctx: &Arc<PipelineContext>) -> usize {
    ctx.file_timings.lock().unwrap().len()
}

fn write(repo: &Vfs, relative: &str, language: Language, body: &str) -> FamilyFileInput {
    repo.write(relative, body.as_bytes().to_vec())
        .expect("write fixture");
    FamilyFileInput {
        language,
        path: relative.to_string(),
        size: body.len() as u64,
        checked: true,
    }
}

/// Each file is costly enough to graph that one loop iteration dwarfs the watcher's poll.
fn many_definitions_fixture(repo: &Vfs, files: usize) -> Vec<FamilyFileInput> {
    let mut body = String::new();
    for symbol in 0..400 {
        body.push_str(&format!(
            "export function gen{symbol}(a, b) {{ const t = a + b; return t * {symbol}; }}\n"
        ));
    }
    (0..files)
        .map(|file| {
            write(
                repo,
                &format!("packages/mod{file}/gen.js"),
                Language::JavaScript,
                &body,
            )
        })
        .collect()
}

/// Cancels once `ctx` reports `at_files` graphed, so no wall clock decides the outcome.
fn cancel_at_progress(
    ctx: &Arc<PipelineContext>,
    cancel: CancellationToken,
    at_files: usize,
) -> (Arc<AtomicBool>, std::thread::JoinHandle<()>) {
    let finished = Arc::new(AtomicBool::new(false));
    let handle = std::thread::spawn({
        let ctx = Arc::clone(ctx);
        let finished = Arc::clone(&finished);
        move || {
            while graphed_files(&ctx) < at_files && !finished.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(1));
            }
            cancel.cancel();
        }
    });
    (finished, handle)
}

fn run_cancelled_at(at_files: usize, files_in_repo: usize) -> (Arc<PipelineContext>, usize) {
    let repo = Vfs::default();
    let files = many_definitions_fixture(&repo, files_in_repo);

    let cancel = CancellationToken::new();
    let ctx = context(repo, cancel.clone());
    let (finished, watcher) = cancel_at_progress(&ctx, cancel, at_files);

    let definitions = dispatch(Language::JavaScript, &ctx, &files);
    finished.store(true, Ordering::Relaxed);
    watcher.join().expect("watcher");
    (ctx, definitions)
}

#[test]
fn cancelling_after_the_first_file_stops_the_javascript_graph_build() {
    const FILES: usize = 20;

    let (ctx, definitions) = run_cancelled_at(1, FILES);

    let graphed = graphed_files(&ctx);
    assert!(
        graphed > 0 && graphed < FILES,
        "cancelling after the first file must abandon the rest, got {graphed} of {FILES}"
    );
    assert_eq!(definitions, 0, "an abandoned run must emit no graph");
}

#[test]
fn cancelling_after_the_last_file_stops_javascript_resolution() {
    const FILES: usize = 20;

    let (ctx, definitions) = run_cancelled_at(FILES, FILES);

    assert_eq!(
        graphed_files(&ctx),
        FILES,
        "every file must reach the build"
    );
    assert_eq!(definitions, 0, "an abandoned run must emit no graph");
}

#[test]
fn a_cancelled_rust_run_builds_no_graph() {
    let repo = Vfs::default();
    let files = vec![write(
        &repo,
        "a.rs",
        Language::Rust,
        "pub fn a() -> u32 { 1 }\n",
    )];

    let cancel = CancellationToken::new();
    cancel.cancel();
    let ctx = context(repo, cancel);
    let definitions = dispatch(Language::Rust, &ctx, &files);

    assert!(
        ctx.language_timings.lock().unwrap().is_empty(),
        "a cancelled run must stop before the graph build, not just before the write"
    );
    assert_eq!(definitions, 0);
}

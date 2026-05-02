use crate::v2::config::{Language, parsable_language};
use crate::v2::sink::{BatchSink, GraphConverter};
use arrow::record_batch::RecordBatch;
use crossbeam_channel::Sender;
use ignore::WalkBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::any::Any;
use std::marker::PhantomData;
use std::path::{Component, Path};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::v2::linker::CodeGraph;
use crate::v2::trace::Tracer;

/// Cooperative cancellation token. Clone-cheap (`Arc`).
/// Set `cancel()` from any thread to request pipeline shutdown.
#[derive(Clone, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Signal cancellation. All pipeline phases will exit at their
    /// next check point (per-file granularity).
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    /// Check if cancellation has been requested.
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

/// A chain reference that failed resolution in Phase 2,
/// stored for re-resolution in Phase 3 after return types are merged.
type FailedChain = (
    String,
    Vec<crate::v2::types::ExpressionStep>,
    smallvec::SmallVec<[crate::v2::types::ssa::ParseValue; 2]>,
    Option<u32>,
);

/// Per-file inferred return types keyed by the graph node indices of definitions.
type InferredReturns = (Vec<petgraph::graph::NodeIndex>, Vec<(u32, String)>);

fn progress_bar(len: u64, prefix: &str) -> ProgressBar {
    let pb = ProgressBar::new(len);
    pb.set_style(
        ProgressStyle::with_template("{prefix} [{bar:40}] {pos}/{len} ({per_sec}, {eta})")
            .unwrap()
            .progress_chars("█▓░"),
    );
    pb.set_prefix(prefix.to_string());
    pb
}

fn spinner(msg: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("{spinner} {msg}").unwrap());
    pb.set_message(msg.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb
}

fn panic_payload_message(payload: &Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Input to a language pipeline: file path (source read on demand).
pub type FileInput = String;

/// Immutable context shared across the entire pipeline run.
/// Bundles config, tracer, root path, and cancellation — everything
/// that doesn't change per-language or per-file.
///
/// Owned so it can be stored in `Arc` and shared across threads
/// and into structs like `CodeGraph`.
pub struct PipelineContext {
    pub config: PipelineConfig,
    pub tracer: crate::v2::trace::Tracer,
    pub root_path: String,
    /// Per-file benign skips. Surface under
    /// `gkg.indexer.code.files.skipped{reason}`.
    pub skipped: std::sync::Mutex<Vec<crate::v2::error::SkippedFile>>,
    /// Per-file failures. Surface under
    /// `gkg.indexer.code.file_faults{kind}` and contribute to
    /// `files.processed{outcome="errored"}`.
    pub faults: std::sync::Mutex<Vec<crate::v2::error::FaultedFile>>,
}

impl PipelineContext {
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.config.cancel.is_cancelled()
    }

    pub fn record_skip(
        &self,
        path: impl Into<String>,
        kind: crate::v2::error::FileSkip,
        detail: impl Into<String>,
    ) {
        if let Ok(mut skipped) = self.skipped.lock() {
            skipped.push(crate::v2::error::SkippedFile {
                path: path.into(),
                kind,
                detail: detail.into(),
            });
        }
    }

    pub fn record_fault(
        &self,
        path: impl Into<String>,
        kind: crate::v2::error::FileFault,
        detail: impl Into<String>,
    ) {
        if let Ok(mut faults) = self.faults.lock() {
            faults.push(crate::v2::error::FaultedFile {
                path: path.into(),
                kind,
                detail: detail.into(),
            });
        }
    }
}

/// Per-language context built inside `process_files`. Bundles the
/// pipeline-wide context with the language-specific spec and rules.
pub struct LanguageContext {
    pub pipeline: Arc<PipelineContext>,
    pub spec: crate::v2::dsl::types::LanguageSpec,
    pub rules: Arc<crate::v2::linker::rules::ResolutionRules>,
}

impl LanguageContext {
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.pipeline.is_cancelled()
    }

    #[inline]
    pub fn tracer(&self) -> &crate::v2::trace::Tracer {
        &self.pipeline.tracer
    }

    #[inline]
    pub fn root_path(&self) -> &str {
        &self.pipeline.root_path
    }

    #[inline]
    pub fn sep(&self) -> &str {
        self.rules.fqn_separator
    }
}

/// Handle for streaming Arrow batches out of a pipeline.
///
/// Wraps a channel sender, converter reference, and stat counters.
/// Language pipelines call `send_graph()` for graph-based output
/// or `send_raw()` for pre-built Arrow batches.
#[derive(Clone, Copy)]
pub struct GraphStatsCounters<'a> {
    directories: &'a AtomicUsize,
    files: &'a AtomicUsize,
    definitions: &'a AtomicUsize,
    imports: &'a AtomicUsize,
    edges: &'a AtomicUsize,
}

impl<'a> GraphStatsCounters<'a> {
    pub fn new(
        directories: &'a AtomicUsize,
        files: &'a AtomicUsize,
        definitions: &'a AtomicUsize,
        imports: &'a AtomicUsize,
        edges: &'a AtomicUsize,
    ) -> Self {
        Self {
            directories,
            files,
            definitions,
            imports,
            edges,
        }
    }

    fn record_graph(self, graph: &CodeGraph) {
        if graph.output.includes_structure() {
            self.directories
                .fetch_add(graph.directories().count(), Ordering::Relaxed);
            self.files
                .fetch_add(graph.files().count(), Ordering::Relaxed);
        }
        self.definitions
            .fetch_add(graph.definitions().count(), Ordering::Relaxed);
        self.imports
            .fetch_add(graph.imports_iter().count(), Ordering::Relaxed);
        let emitted_edges = if graph.output.includes_structure() {
            graph.edge_count()
        } else {
            graph
                .graph
                .edge_indices()
                .filter(|&idx| graph.graph[idx].relationship.edge_kind.as_ref() != "CONTAINS")
                .count()
        };
        self.edges.fetch_add(emitted_edges, Ordering::Relaxed);
    }
}

pub struct BatchTx<'a> {
    tx: &'a Sender<(String, RecordBatch)>,
    converter: &'a dyn GraphConverter,
    errors: &'a Mutex<Vec<PipelineError>>,
    stats: GraphStatsCounters<'a>,
}

impl<'a> BatchTx<'a> {
    pub fn new(
        tx: &'a Sender<(String, RecordBatch)>,
        converter: &'a dyn GraphConverter,
        errors: &'a Mutex<Vec<PipelineError>>,
        stats: GraphStatsCounters<'a>,
    ) -> Self {
        Self {
            tx,
            converter,
            errors,
            stats,
        }
    }
}

impl BatchTx<'_> {
    /// Count graph stats, convert to Arrow batches, and send to the
    /// writer thread. Takes ownership — graph is dropped after conversion.
    pub fn send_graph(&self, graph: CodeGraph) {
        self.stats.record_graph(&graph);
        let batches = match self.converter.convert(graph) {
            Ok(batches) => batches,
            Err(error) => {
                self.errors.lock().unwrap().push(
                    crate::v2::error::CodeGraphError::ArrowConversion {
                        message: error.to_string(),
                    }
                    .into(),
                );
                return;
            }
        };
        for (table, batch) in batches {
            if self.tx.send((table.clone(), batch)).is_err() {
                self.errors.lock().unwrap().push(
                    crate::v2::error::CodeGraphError::SinkWrite {
                        table,
                        message: "batch channel closed before writer accepted graph output"
                            .to_string(),
                    }
                    .into(),
                );
                return;
            }
        }
    }

    /// Send a raw pre-built batch (for custom pipelines that bypass CodeGraph).
    pub fn send_raw(&self, table: String, batch: RecordBatch) {
        if self.tx.send((table.clone(), batch)).is_err() {
            self.errors.lock().unwrap().push(
                crate::v2::error::CodeGraphError::SinkWrite {
                    table,
                    message: "batch channel closed before writer accepted raw output".to_string(),
                }
                .into(),
            );
        }
    }
}

fn write_graph_direct(
    graph: CodeGraph,
    converter: &dyn GraphConverter,
    sink: &dyn BatchSink,
    errors: &Mutex<Vec<PipelineError>>,
    stats: GraphStatsCounters<'_>,
) {
    stats.record_graph(&graph);
    let batches = match converter.convert(graph) {
        Ok(batches) => batches,
        Err(error) => {
            errors.lock().unwrap().push(
                crate::v2::error::CodeGraphError::ArrowConversion {
                    message: error.to_string(),
                }
                .into(),
            );
            return;
        }
    };
    for (table, batch) in batches {
        if let Err(error) = sink.write_batch(&table, &batch) {
            errors.lock().unwrap().push(
                crate::v2::error::CodeGraphError::SinkWrite {
                    table,
                    message: error.to_string(),
                }
                .into(),
            );
            return;
        }
    }
}

/// Trait for language-specific pipeline execution.
///
/// All pipelines stream their output through a `BatchTx` handle.
/// Graph-based pipelines use `btx.send_graph()`.
/// Batch-based pipelines use `btx.send_raw()`.
pub trait LanguagePipeline {
    fn process_files(
        files: &[FileInput],
        ctx: &Arc<PipelineContext>,
        btx: &BatchTx<'_>,
    ) -> Result<(), Vec<PipelineError>>;
}

#[derive(Clone)]
pub struct PipelineConfig {
    pub max_file_size: u64,
    /// Max language-supported files accepted for one pipeline run.
    /// 0 = no limit.
    pub max_files: usize,
    pub respect_gitignore: bool,
    pub cancel: CancellationToken,
    /// Rayon threads per language. 0 = use all available cores.
    pub worker_threads: usize,
    /// Max languages processing concurrently. Limits peak memory
    /// (at most N CodeGraphs + N rayon pools alive at once).
    /// 0 = default (2).
    pub max_concurrent_languages: usize,
    /// Global per-file resolution timeout. Applied to all languages
    /// unless the language's own DSL rules specify a different value.
    /// `None` = no global timeout (language rules may still set one).
    pub per_file_timeout: Option<std::time::Duration>,
    /// Optional complete repository file inventory. Server indexing fills this
    /// from archive metadata before extraction filters skip bytes.
    pub file_inventory: Option<Arc<[FileInventoryEntry]>>,
    /// Internal switch set by `Pipeline::run_with_tracer` so parser graphs only
    /// emit parsed nodes and relationships while a separate structural graph
    /// owns repository file/directory rows.
    pub emit_file_inventory_graph: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_file_size: 1_000_000,
            max_files: 0,
            respect_gitignore: true,
            cancel: CancellationToken::new(),
            worker_threads: 0,
            max_concurrent_languages: 0,
            per_file_timeout: None,
            file_inventory: None,
            emit_file_inventory_graph: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileInventoryEntry {
    pub path: String,
    pub size: u64,
}

pub struct PipelineResult {
    pub stats: PipelineStats,
    /// Task-level errors. Fatal entries route to `code_errors_total{stage}`.
    pub errors: Vec<PipelineError>,
    pub skipped: Vec<crate::v2::error::SkippedFile>,
    pub faults: Vec<crate::v2::error::FaultedFile>,
    pub ctx: Arc<PipelineContext>,
}

pub struct PipelineStats {
    pub files_discovered: usize,
    pub bytes_discovered: u64,
    pub directories_indexed: usize,
    pub files_indexed: usize,
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
    pub stage: &'static str,
    pub fatal: bool,
}

impl PipelineError {
    pub fn parse(file_path: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            file_path: file_path.into(),
            error: error.into(),
            stage: "indexing",
            fatal: false,
        }
    }

    pub fn fatal(
        file_path: impl Into<String>,
        error: impl Into<String>,
        stage: &'static str,
    ) -> Self {
        Self {
            file_path: file_path.into(),
            error: error.into(),
            stage,
            fatal: true,
        }
    }
}

impl From<crate::v2::error::CodeGraphError> for PipelineError {
    fn from(err: crate::v2::error::CodeGraphError) -> Self {
        Self {
            file_path: err.scope().to_string(),
            error: err.to_string(),
            stage: err.stage(),
            fatal: true,
        }
    }
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.file_path, self.error)
    }
}

impl std::error::Error for PipelineError {}

pub struct Pipeline;

impl Pipeline {
    pub fn run(
        root: &Path,
        config: PipelineConfig,
        converter: Arc<dyn GraphConverter>,
        sink: Arc<dyn BatchSink>,
    ) -> PipelineResult {
        Self::run_with_tracer(root, config, Tracer::new(false), converter, sink)
    }

    /// Run the pipeline. Each language gets its own CPU thread and a
    /// dedicated writer thread. Results stream through a per-language
    /// channel as phases complete — nodes after Phase 1, edges after
    /// Phase 2/3. Graphs are dropped immediately after conversion.
    ///
    /// Blocks until all languages finish processing and writing.
    pub fn run_with_tracer(
        root: &Path,
        mut config: PipelineConfig,
        tracer: Tracer,
        converter: Arc<dyn GraphConverter>,
        sink: Arc<dyn BatchSink>,
    ) -> PipelineResult {
        let root_str = root.to_string_lossy().to_string();
        config.emit_file_inventory_graph = true;

        // 1. Walk filesystem, group files by language
        let pb_discover = spinner("Discovering files...");
        let discovery = Self::discover_files(root, &config);
        let files_by_language = discovery.files_by_language;
        let file_inventory = discovery.file_inventory;
        let parsed_file_languages = discovery.parsed_file_languages;
        let total_files = file_inventory.len();
        let total_bytes: u64 = file_inventory.iter().map(|entry| entry.size).sum();
        let parsable_files: usize = files_by_language.values().map(|f| f.len()).sum();
        let lang_summary: Vec<String> = files_by_language
            .iter()
            .map(|(l, f)| format!("{l}: {}", f.len()))
            .collect();
        pb_discover.finish_with_message(format!(
            "Found {total_files} files, {parsable_files} parseable ({})",
            lang_summary.join(", ")
        ));

        let ctx = Arc::new(PipelineContext {
            config,
            tracer,
            root_path: root_str,
            skipped: std::sync::Mutex::new(Vec::new()),
            faults: std::sync::Mutex::new(Vec::new()),
        });

        // 2. Process languages with bounded concurrency. At most
        //    max_concurrent_languages run at once (default 2), each
        //    with its own rayon pool and writer thread. Limits peak
        //    memory to N CodeGraphs + N rayon pools.
        let max_langs = match ctx.config.max_concurrent_languages {
            0 => 2,
            n => n,
        };
        let directories_count = AtomicUsize::new(0);
        let files_count = AtomicUsize::new(0);
        let files_parsed = AtomicUsize::new(0);
        let files_skipped = AtomicUsize::new(0);
        let definitions_count = AtomicUsize::new(0);
        let imports_count = AtomicUsize::new(0);
        let edges_count = AtomicUsize::new(0);
        let all_errors = std::sync::Mutex::new(Vec::<PipelineError>::new());

        if !file_inventory.is_empty() {
            let structural_graph =
                Self::build_file_inventory_graph(root, &file_inventory, &parsed_file_languages);
            write_graph_direct(
                structural_graph,
                converter.as_ref(),
                sink.as_ref(),
                &all_errors,
                GraphStatsCounters::new(
                    &directories_count,
                    &files_count,
                    &definitions_count,
                    &imports_count,
                    &edges_count,
                ),
            );
        }

        // Bounded channel as a semaphore: N permits = N concurrent languages
        let (sem_tx, sem_rx) = crossbeam_channel::bounded::<()>(max_langs);
        for _ in 0..max_langs {
            sem_tx.send(()).unwrap();
        }

        std::thread::scope(|s| {
            for (language, files) in &files_by_language {
                // Block until a slot opens
                sem_rx.recv().unwrap();

                let ctx = &ctx;
                let converter = &converter;
                let sink = &sink;
                let sem_tx = &sem_tx;
                let files_parsed = &files_parsed;
                let files_skipped = &files_skipped;
                let directories_count = &directories_count;
                let files_count = &files_count;
                let definitions_count = &definitions_count;
                let imports_count = &imports_count;
                let edges_count = &edges_count;
                let all_errors = &all_errors;

                // Per-language channel: CPU thread → writer thread.
                // Bounded to cap memory if the writer is slower than the converter.
                let (tx, rx) = crossbeam_channel::bounded::<(String, RecordBatch)>(8);

                // Writer thread: drain channel, write each batch to sink
                s.spawn(move || {
                    for (table, batch) in rx {
                        if let Err(e) = sink.write_batch(&table, &batch) {
                            all_errors.lock().unwrap().push(
                                crate::v2::error::CodeGraphError::SinkWrite {
                                    table: table.clone(),
                                    message: e.to_string(),
                                }
                                .into(),
                            );
                        }
                    }
                });

                // CPU thread: acquire permit, build rayon pool, process,
                // release permit when done
                let worker_threads = ctx.config.worker_threads;
                s.spawn(move || {
                    if ctx.is_cancelled() {
                        sem_tx.send(()).ok();
                        return;
                    }
                    let file_count = files.len();
                    let t_lang = std::time::Instant::now();

                    let mut pool_builder =
                        rayon::ThreadPoolBuilder::new().stack_size(2 * 1024 * 1024); // 2MB per worker (vs 8MB default)
                    if worker_threads > 0 {
                        pool_builder = pool_builder.num_threads(worker_threads);
                    }
                    let pool = match pool_builder.build() {
                        Ok(pool) => pool,
                        Err(e) => {
                            all_errors.lock().unwrap().push(
                                crate::v2::error::CodeGraphError::ThreadPoolCreation {
                                    language: language.to_string(),
                                    source: e,
                                }
                                .into(),
                            );
                            sem_tx.send(()).ok();
                            return;
                        }
                    };

                    let btx = BatchTx {
                        tx: &tx,
                        converter: converter.as_ref(),
                        errors: all_errors,
                        stats: GraphStatsCounters::new(
                            directories_count,
                            files_count,
                            definitions_count,
                            imports_count,
                            edges_count,
                        ),
                    };

                    tracing::info!(
                        %language,
                        file_count,
                        threads = pool.current_num_threads(),
                        "processing language"
                    );

                    let result = pool.install(|| {
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            crate::v2::registry::dispatch_language(*language, files, ctx, &btx)
                        }))
                    });

                    // Pool dropped here — rayon threads freed
                    drop(pool);

                    match result.unwrap_or_else(|payload| {
                        // If the panic payload is already a typed CodeGraphError
                        // (e.g. an UnexpectedNodeType from the linker), surface
                        // it directly so its `stage` reaches the dashboard
                        // instead of collapsing into `internal`.
                        let err = match payload.downcast::<crate::v2::error::CodeGraphError>() {
                            Ok(typed) => *typed,
                            Err(payload) => crate::v2::error::CodeGraphError::Internal {
                                context: format!("language_panic:{language}"),
                                message: format!(
                                    "language worker panicked: {}",
                                    panic_payload_message(&payload)
                                ),
                            },
                        };
                        Some(Err(vec![err.into()]))
                    }) {
                        Some(Ok(())) => {
                            tracing::info!(
                                %language,
                                elapsed_ms = t_lang.elapsed().as_millis() as u64,
                                "language done"
                            );
                            files_parsed.fetch_add(file_count, Ordering::Relaxed);
                        }
                        Some(Err(errors)) => {
                            tracing::warn!(
                                %language,
                                error_count = errors.len(),
                                "language processing failed"
                            );
                            // After the typed-outcome refactor the
                            // language pipelines return `Ok(())` even
                            // when every file was skipped or faulted —
                            // so this arm only fires for task-level
                            // failures recovered from the `catch_unwind`
                            // above (rayon panic, linker invariant
                            // violation, etc.). Files that hadn't been
                            // processed yet are silently dropped from
                            // counts; the failure itself surfaces via
                            // `code_errors_total{stage}` so on-call can
                            // see what bailed out.
                            if let Ok(mut errs) = all_errors.lock() {
                                errs.extend(errors);
                            }
                        }
                        None => {
                            tracing::debug!(
                                %language,
                                file_count,
                                "language not supported, skipping"
                            );
                            files_skipped.fetch_add(file_count, Ordering::Relaxed);
                        }
                    }
                    // Release permit — next language can start
                    sem_tx.send(()).ok();
                    // tx dropped here — writer thread exits
                });
            }
        }); // all threads join here

        let skipped = ctx
            .skipped
            .lock()
            .map(|mut e| std::mem::take(&mut *e))
            .unwrap_or_default();
        let faults = ctx
            .faults
            .lock()
            .map(|mut e| std::mem::take(&mut *e))
            .unwrap_or_default();

        PipelineResult {
            stats: PipelineStats {
                files_discovered: total_files,
                bytes_discovered: total_bytes,
                directories_indexed: directories_count.into_inner(),
                files_indexed: files_count.into_inner(),
                files_parsed: files_parsed.into_inner(),
                files_skipped: files_skipped.into_inner(),
                definitions_count: definitions_count.into_inner(),
                imports_count: imports_count.into_inner(),
                references_count: 0,
                edges_count: edges_count.into_inner(),
            },
            errors: all_errors.into_inner().unwrap_or_default(),
            skipped,
            faults,
            ctx,
        }
    }

    #[cfg(test)]
    fn walk_and_group(root: &Path, config: &PipelineConfig) -> FxHashMap<Language, Vec<FileInput>> {
        Self::discover_files(root, config).files_by_language
    }

    fn discover_files(root: &Path, config: &PipelineConfig) -> FileDiscovery {
        let mut groups: FxHashMap<Language, Vec<FileInput>> = FxHashMap::default();
        let mut walked_inventory = Vec::new();
        let mut parsed_file_languages = FxHashMap::default();
        let mut accepted_files = 0usize;

        let walker = WalkBuilder::new(root)
            .git_ignore(config.respect_gitignore)
            .hidden(true)
            .build();

        for entry in walker.filter_map(|result| match result {
            Ok(entry) => Some(entry),
            Err(e) => {
                tracing::debug!(error = %e, "directory walk error, skipping entry");
                None
            }
        }) {
            if !entry.file_type().is_some_and(|ft| ft.is_file()) {
                continue;
            }

            let path = entry.path();
            let rel_path = path.strip_prefix(root).unwrap_or(path);
            let metadata = path.metadata().ok();
            let rel_path_string = rel_path.to_string_lossy().to_string();

            walked_inventory.push(FileInventoryEntry {
                path: rel_path_string.clone(),
                size: metadata.as_ref().map_or(0, |metadata| metadata.len()),
            });

            if metadata
                .as_ref()
                .is_some_and(|metadata| metadata.len() > config.max_file_size)
            {
                continue;
            }

            let Some(lang) = parsable_language(rel_path) else {
                continue;
            };

            if config.max_files > 0 && accepted_files >= config.max_files {
                continue;
            }

            // Verify file is readable, but don't load yet.
            if !path.is_file() {
                continue;
            }

            accepted_files += 1;
            parsed_file_languages.insert(rel_path_string.clone(), lang);
            groups.entry(lang).or_default().push(rel_path_string);
        }

        let inventory_source = config
            .file_inventory
            .as_ref()
            .into_iter()
            .flat_map(|entries| entries.iter().cloned())
            .chain(walked_inventory);
        let file_inventory = canonical_file_inventory(inventory_source);

        FileDiscovery {
            files_by_language: groups,
            file_inventory,
            parsed_file_languages,
        }
    }

    fn build_file_inventory_graph(
        root: &Path,
        inventory: &[FileInventoryEntry],
        parsed_file_languages: &FxHashMap<String, Language>,
    ) -> CodeGraph {
        let mut graph = CodeGraph::new_with_root(root.to_string_lossy().to_string());
        for entry in inventory {
            let language = parsed_file_languages.get(&entry.path).copied();
            graph.add_unparsed_file(&entry.path, language, entry.size);
        }

        graph.drop_construction_indexes();
        graph
    }
}

struct FileDiscovery {
    files_by_language: FxHashMap<Language, Vec<FileInput>>,
    file_inventory: Vec<FileInventoryEntry>,
    parsed_file_languages: FxHashMap<String, Language>,
}

fn canonical_file_inventory(
    entries: impl IntoIterator<Item = FileInventoryEntry>,
) -> Vec<FileInventoryEntry> {
    let mut by_path = FxHashMap::default();
    for entry in entries {
        let Some(path) = normalize_inventory_path(&entry.path) else {
            continue;
        };
        by_path.entry(path).or_insert(entry.size);
    }

    let mut entries: Vec<_> = by_path
        .into_iter()
        .map(|(path, size)| FileInventoryEntry { path, size })
        .collect();
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    entries
}

fn normalize_inventory_path(path: &str) -> Option<String> {
    let mut parts = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join("/"))
    }
}

/// Generic pipeline parameterized by language spec `P` and rules `R`.
///
/// - **Phase 1** (parallel): full AST walk → defs, imports, refs
/// - **Finalize**: build ancestor chains → stream nodes to writer
/// - **Phase 2** (parallel): resolve refs → edges
/// - **Phase 3**: re-resolve failed chains → stream edges to writer
pub struct GenericPipeline<P, R>(PhantomData<(P, R)>);

impl<P, R> LanguagePipeline for GenericPipeline<P, R>
where
    P: crate::v2::dsl::types::DslLanguage + 'static,
    R: crate::v2::linker::HasRules + Send + Sync,
{
    fn process_files(
        files: &[FileInput],
        ctx: &Arc<PipelineContext>,
        btx: &BatchTx<'_>,
    ) -> Result<(), Vec<PipelineError>> {
        let lang_ctx = Arc::new(LanguageContext {
            pipeline: ctx.clone(),
            spec: P::spec(),
            rules: Arc::new(R::rules()),
        });
        let language = P::language();
        let file_count = files.len();
        let root_path = lang_ctx.root_path();
        let tracer = lang_ctx.tracer();
        let spec = &lang_ctx.spec;
        let rules = &lang_ctx.rules;
        let t0 = std::time::Instant::now();

        // Spawn sentinel watchdog if per_file_timeout is configured.
        // Language rules take precedence; global config is the fallback.
        // Falls back to no timeout if the thread can't be spawned.
        let per_file_timeout = rules
            .settings
            .per_file_timeout
            .or(ctx.config.per_file_timeout);
        let sentinel = per_file_timeout.and_then(crate::v2::sentinel::spawn_sentinel);

        // ── Phase 1: parallel full walk, sequential graph build ──
        let pb = progress_bar(file_count as u64, "parse + graph");
        let all_errors: Vec<PipelineError> = Vec::new();

        struct FileInfo {
            file_node: petgraph::graph::NodeIndex,
            def_nodes: Vec<petgraph::graph::NodeIndex>,
            import_nodes: Vec<petgraph::graph::NodeIndex>,
        }

        // Phase 1a: full AST walk in parallel (no locks, no graph).
        // Extracts defs, imports, AND refs+SSA in a single pass.
        // Source bytes are read and dropped per-file — no bulk storage.
        use crate::v2::dsl::engine::{CollectedRef, ParseFullResult};
        use crate::v2::error::{FaultedFile, FileFault};

        enum ParseOutcome {
            Ok(ParsedFile),
            Err(FaultedFile),
        }

        struct ParsedFile {
            path_idx: usize,
            result: ParseFullResult,
            ext: String,
            file_size: u64,
        }

        let parse_outcomes: Vec<Option<ParseOutcome>> = files
            .par_iter()
            .enumerate()
            .map(|(idx, path)| {
                if ctx.is_cancelled() {
                    pb.inc(1);
                    return None;
                }
                let abs_path = format!("{root_path}/{path}");
                let source = match std::fs::read(&abs_path) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::debug!(path, error = %e, "failed to read file");
                        pb.inc(1);
                        return Some(ParseOutcome::Err(FaultedFile {
                            path: path.to_string(),
                            kind: FileFault::FileRead,
                            detail: e.to_string(),
                        }));
                    }
                };

                let result = match spec.parse_full_collect(&source, path, language, tracer) {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::debug!(path, error = %e, "failed to parse file");
                        pb.inc(1);
                        // Exhaustive match on the typed `ParseFullError`
                        // — adding a new variant breaks the build until
                        // it gets a `FileFault` mapping here.
                        let (kind, detail) = match e {
                            crate::v2::dsl::engine::ParseFullError::InvalidUtf8(err) => {
                                (FileFault::InvalidUtf8, err.to_string())
                            }
                        };
                        return Some(ParseOutcome::Err(FaultedFile {
                            path: path.to_string(),
                            kind,
                            detail,
                        }));
                    }
                };
                // source bytes dropped here — single-walk, no re-read needed

                let ext = path
                    .rsplit_once('.')
                    .map(|(_, e)| e)
                    .unwrap_or("")
                    .to_string();
                let file_size = source.len() as u64;
                pb.inc(1);
                Some(ParseOutcome::Ok(ParsedFile {
                    path_idx: idx,
                    result,
                    ext,
                    file_size,
                }))
            })
            .collect();

        let mut parsed_faults: Vec<FaultedFile> = Vec::new();
        let parsed: Vec<Option<ParsedFile>> = parse_outcomes
            .into_iter()
            .map(|outcome| match outcome {
                Some(ParseOutcome::Ok(file)) => Some(file),
                Some(ParseOutcome::Err(e)) => {
                    parsed_faults.push(e);
                    None
                }
                None => None,
            })
            .collect();

        if !parsed_faults.is_empty()
            && let Ok(mut faults) = ctx.faults.lock()
        {
            faults.extend(parsed_faults);
        }

        // Phase 1b: add defs/imports to graph sequentially. Keep refs alive.
        struct FileWithRefs {
            info: FileInfo,
            refs: Vec<CollectedRef>,
            inferred_returns: Vec<(u32, String)>,
            unresolved_aliases: Vec<(usize, String)>,
        }

        let mut graph =
            CodeGraph::new_with_root(root_path.to_string()).with_rules(lang_ctx.rules.clone());
        if ctx.config.emit_file_inventory_graph {
            graph.mark_parsed_only();
        }
        let mut total_defs = 0usize;
        let mut total_imports = 0usize;
        let mut files_with_refs: Vec<Option<FileWithRefs>> =
            (0..file_count).map(|_| None).collect();

        for parsed_file in parsed.into_iter().flatten() {
            let path = &files[parsed_file.path_idx];
            total_defs += parsed_file.result.definitions.len();
            total_imports += parsed_file.result.imports.len();

            let (file_node, def_nodes, import_nodes) = graph.add_file(
                path,
                &parsed_file.ext,
                language,
                parsed_file.file_size,
                &parsed_file.result.definitions,
                &parsed_file.result.imports,
            );

            files_with_refs[parsed_file.path_idx] = Some(FileWithRefs {
                info: FileInfo {
                    file_node,
                    def_nodes,
                    import_nodes,
                },
                refs: parsed_file.result.refs,
                inferred_returns: parsed_file.result.inferred_returns,
                unresolved_aliases: parsed_file.result.unresolved_aliases,
            });
        }

        pb.finish_with_message(format!(
            "{total_defs} defs, {total_imports} imports in {:.2?}",
            t0.elapsed()
        ));

        if !all_errors.is_empty() && files_with_refs.iter().all(|r| r.is_none()) {
            return Err(all_errors);
        }

        graph.finalize(tracer);
        graph.drop_construction_indexes();

        // ── Phase 1c: patch unresolved SSA aliases via graph ────
        // Pass 1.5 equivalent: resolve alias targets that couldn't be
        // resolved without the cross-file graph, then update reaching
        // values on the affected CollectedRefs.
        for fwr in files_with_refs.iter_mut().flatten() {
            if fwr.unresolved_aliases.is_empty() {
                continue;
            }
            for (ref_idx, alias_target) in &fwr.unresolved_aliases {
                let nodes = graph.resolve_scope_nodes(alias_target);
                if let Some(&n) = nodes.first()
                    && graph.def_kind(n).is_type_container()
                {
                    let fqn = graph.def_fqn(n).to_string();
                    if let Some(r) = fwr.refs.get_mut(*ref_idx) {
                        r.reaching = vec![crate::v2::types::ssa::ParseValue::Type(fqn.into())];
                    }
                }
            }
        }

        // ── Phase 2: resolve-only (no I/O, no parsing) ──────────
        let t2 = std::time::Instant::now();
        let pb2 = progress_bar(file_count as u64, "resolve");
        let total_edges = std::sync::atomic::AtomicUsize::new(0);

        type Phase2Result = (
            Vec<(
                petgraph::graph::NodeIndex,
                petgraph::graph::NodeIndex,
                crate::v2::linker::GraphEdge,
            )>,
            Vec<(u32, String)>,
            Option<FileInfo>,
            Vec<FailedChain>,
            bool, // true if file was killed by sentinel timeout
        );

        let resolve_results: Vec<Phase2Result> = files_with_refs
            .into_par_iter()
            .zip(files.par_iter())
            .map(|(fwr_opt, path)| -> Phase2Result {
                if ctx.is_cancelled() {
                    pb2.inc(1);
                    return Default::default();
                }
                let Some(fwr) = fwr_opt else {
                    pb2.inc(1);
                    return Default::default();
                };

                let guard = sentinel.as_ref().map(|(handle, _)| handle.file_start(path));
                let mut resolver = crate::v2::linker::FileResolver::new(
                    &graph,
                    fwr.info.file_node,
                    &fwr.info.def_nodes,
                    &fwr.info.import_nodes,
                    &lang_ctx,
                    guard,
                );
                resolver.set_inferred_returns(&fwr.inferred_returns);

                let mut edges = Vec::new();
                let mut failed_chains: Vec<FailedChain> = Vec::new();

                let mut killed = false;
                for r in &fwr.refs {
                    let before = edges.len();
                    if resolver
                        .resolve(
                            &r.name,
                            r.chain.as_deref(),
                            &r.reaching,
                            r.enclosing_def,
                            &mut edges,
                        )
                        .is_err()
                    {
                        killed = true;
                        break; // file killed by sentinel
                    }
                    if edges.len() == before && r.chain.as_ref().is_some_and(|c| c.len() >= 2) {
                        failed_chains.push((
                            r.name.to_string(),
                            r.chain.clone().unwrap(),
                            r.reaching.clone().into(),
                            r.enclosing_def,
                        ));
                    }
                }

                edges.extend(resolver.drain_import_edges());
                total_edges.fetch_add(edges.len(), std::sync::atomic::Ordering::Relaxed);
                pb2.inc(1);
                (
                    edges,
                    fwr.inferred_returns,
                    Some(fwr.info),
                    failed_chains,
                    killed,
                )
            })
            .collect();

        pb2.finish_with_message(format!(
            "{} edges in {:.2?}",
            total_edges.load(std::sync::atomic::Ordering::Relaxed),
            t2.elapsed()
        ));

        // Insert Phase 2 edges and collect inferred returns + failed chains
        let mut all_inferred: Vec<InferredReturns> = Vec::new();
        let mut all_failed: Vec<(FileInfo, Vec<FailedChain>)> = Vec::new();

        for (edges, inferred, info_opt, failed_chains, killed) in resolve_results {
            if killed && let Some(ref info) = info_opt {
                let path = match &graph.graph[info.file_node] {
                    crate::v2::linker::graph::GraphNode::File(f) => f.path.as_str(),
                    _ => "unknown",
                };
                ctx.record_skip(
                    path.to_string(),
                    crate::v2::error::FileSkip::TimeoutSentinel,
                    "per-file watchdog killed analysis",
                );
            }
            for (src, tgt, edge) in edges {
                graph.graph.add_edge(src, tgt, edge);
            }
            if let Some(info) = info_opt {
                if !inferred.is_empty() {
                    all_inferred.push((info.def_nodes.clone(), inferred));
                }
                if !failed_chains.is_empty() {
                    all_failed.push((info, failed_chains));
                }
            }
        }

        // ── Phase 3: write inferred return types to graph, re-resolve failed chains
        if !all_inferred.is_empty() {
            for (def_nodes, inferred) in &all_inferred {
                for (def_idx, rt) in inferred {
                    if let Some(&node) = def_nodes.get(*def_idx as usize)
                        && let Some(did) = graph.graph[node].def_id()
                    {
                        let rt_id = graph.strings.alloc(rt);
                        graph.defs[did.0 as usize]
                            .metadata
                            .get_or_insert_with(Default::default)
                            .return_type = Some(rt_id);
                    }
                }
            }

            if !all_failed.is_empty() {
                let phase3_results: Vec<_> = all_failed
                    .par_iter()
                    .map(|(info, failed_chains)| {
                        let guard = sentinel
                            .as_ref()
                            .map(|(handle, _)| handle.file_start("phase3"));
                        let mut resolver = crate::v2::linker::FileResolver::new(
                            &graph,
                            info.file_node,
                            &info.def_nodes,
                            &info.import_nodes,
                            &lang_ctx,
                            guard,
                        );
                        let mut edges = Vec::new();
                        for (name, chain, reaching, enclosing_def) in failed_chains {
                            if resolver
                                .resolve(name, Some(chain), reaching, *enclosing_def, &mut edges)
                                .is_err()
                            {
                                break; // file killed by sentinel
                            }
                        }
                        edges
                    })
                    .collect();

                let mut phase3_edges = 0usize;
                for edges in phase3_results {
                    phase3_edges += edges.len();
                    for (src, tgt, edge) in edges {
                        graph.graph.add_edge(src, tgt, edge);
                    }
                }
                if phase3_edges > 0 {
                    tracing::info!(phase3_edges, "phase 3: cross-file edges resolved");
                }
            }
        }

        // Free resolution-only indexes before conversion.
        graph.indexes.definition_ranges.clear();
        graph.indexes.definition_ranges.shrink_to_fit();

        // Shut down sentinel
        if let Some((handle, join)) = sentinel {
            handle.shutdown();
            let _ = join.join();
        }

        tracing::info!(
            elapsed_ms = t0.elapsed().as_millis() as u64,
            nodes = graph.graph.node_count(),
            edges = graph.graph.edge_count(),
            defs = graph.defs.len(),
            imports = graph.imports.len(),
            strings = graph.strings.len(),
            "pipeline complete"
        );

        // ── Stream graph to writer thread, then drop it ─────────
        btx.send_graph(graph);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::linker::CodeGraph;
    use crate::v2::sink::{GraphConverter, NullSink, SinkError};
    use crate::v2::types::{DefKind, NodeKind};

    /// Test-only converter that captures CodeGraphs for inspection.
    struct TestCapture {
        graphs: std::sync::Mutex<Vec<CodeGraph>>,
    }

    impl TestCapture {
        fn new() -> Self {
            Self {
                graphs: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn take(&self) -> Vec<CodeGraph> {
            std::mem::take(&mut *self.graphs.lock().unwrap())
        }
    }

    impl GraphConverter for TestCapture {
        fn convert(&self, graph: CodeGraph) -> Result<Vec<(String, RecordBatch)>, SinkError> {
            self.graphs.lock().unwrap().push(graph);
            Ok(Vec::new())
        }
    }

    fn fixture_path(relative: &str) -> String {
        let manifest = env!("CARGO_MANIFEST_DIR");
        format!("{manifest}/../../fixtures/code/{relative}")
    }

    fn parse_fixture_file(path: &str, language: Language) -> CodeGraph {
        let ctx = Arc::new(PipelineContext {
            config: PipelineConfig::default(),
            tracer: crate::v2::trace::Tracer::new(false),
            root_path: "/".to_string(),
            skipped: std::sync::Mutex::new(Vec::new()),
            faults: std::sync::Mutex::new(Vec::new()),
        });
        let capture = Arc::new(TestCapture::new());
        let (tx, _rx) = crossbeam_channel::unbounded();
        let dirs = AtomicUsize::new(0);
        let files = AtomicUsize::new(0);
        let defs = AtomicUsize::new(0);
        let imps = AtomicUsize::new(0);
        let edgs = AtomicUsize::new(0);
        let errors = Mutex::new(Vec::new());
        let btx = BatchTx::new(
            &tx,
            capture.as_ref(),
            &errors,
            GraphStatsCounters::new(&dirs, &files, &defs, &imps, &edgs),
        );
        crate::v2::registry::dispatch_language(language, &[path.to_string()], &ctx, &btx)
            .unwrap_or_else(|| panic!("Language {language} not supported"))
            .unwrap_or_else(|e| panic!("Failed to parse: {e:?}"));
        let mut graphs = capture.take();
        assert!(!graphs.is_empty(), "expected graph output");
        graphs.remove(0)
    }

    #[test]
    fn walk_and_group_respects_max_files() {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(dir.path().join("a.java"), "class A {}").expect("write a");
        std::fs::write(dir.path().join("b.java"), "class B {}").expect("write b");
        std::fs::write(dir.path().join("c.java"), "class C {}").expect("write c");

        let groups = Pipeline::walk_and_group(
            dir.path(),
            &PipelineConfig {
                max_files: 2,
                ..PipelineConfig::default()
            },
        );
        let accepted = groups.values().map(Vec::len).sum::<usize>();

        assert_eq!(accepted, 2);
    }

    #[test]
    fn file_inventory_emits_unparsed_file_nodes_once() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::write(root.join("src/main.py"), "def hello(): pass\n").unwrap();

        let inventory = vec![
            FileInventoryEntry {
                path: "src/main.py".into(),
                size: 17,
            },
            FileInventoryEntry {
                path: "README.md".into(),
                size: 12,
            },
            FileInventoryEntry {
                path: "./README.md".into(),
                size: 12,
            },
            FileInventoryEntry {
                path: "config/app.yml".into(),
                size: 9,
            },
            FileInventoryEntry {
                path: "assets/logo.png".into(),
                size: 128,
            },
            FileInventoryEntry {
                path: "vendor/jquery.min.js".into(),
                size: 256,
            },
        ];

        let capture = Arc::new(TestCapture::new());
        let result = Pipeline::run_with_tracer(
            root,
            PipelineConfig {
                file_inventory: Some(Arc::from(inventory)),
                ..PipelineConfig::default()
            },
            crate::v2::trace::Tracer::new(false),
            capture.clone(),
            Arc::new(NullSink),
        );

        assert_eq!(result.errors.len(), 0, "Should have no errors");
        assert_eq!(result.stats.files_discovered, 5);
        assert_eq!(result.stats.files_indexed, 5);
        assert_eq!(result.stats.files_parsed, 1);

        let graphs = capture.take();
        let mut structural_files: Vec<_> = graphs
            .iter()
            .filter(|g| g.output.includes_structure())
            .flat_map(|g| {
                g.files()
                    .map(|(_, file)| (file.path.clone(), file.language_name()))
            })
            .collect();
        structural_files.sort();

        assert_eq!(
            structural_files,
            vec![
                ("README.md".into(), "unknown"),
                ("assets/logo.png".into(), "unknown"),
                ("config/app.yml".into(), "unknown"),
                ("src/main.py".into(), "python"),
                ("vendor/jquery.min.js".into(), "unknown"),
            ]
        );
    }

    // ── Python fixture ──────────────────────────────────────────────

    #[test]
    fn python_definitions_fixture() {
        let path = fixture_path("python/definitions.py");
        let cg = parse_fixture_file(&path, Language::Python);

        let defs: Vec<_> = cg.definitions().collect();
        assert!(
            defs.len() >= 10,
            "Expected at least 10 definitions, got {}",
            defs.len()
        );

        let names: Vec<&str> = defs
            .iter()
            .map(|(_, _, d)| cg.strings.get(d.name))
            .collect();
        assert!(names.contains(&"simple_function"));
        assert!(names.contains(&"module_lambda"));
        assert!(names.contains(&"SimpleClass"));
        assert!(names.contains(&"decorated_function"));

        let class_count = defs
            .iter()
            .filter(|(_, _, d)| d.kind == DefKind::Class)
            .count();
        assert!(class_count > 0, "Should find at least one class");
    }

    // ── Java fixture ────────────────────────────────────────────────

    #[test]
    fn java_comprehensive_fixture() {
        let path = fixture_path("java/ComprehensiveJavaDefinitions.java");
        let cg = parse_fixture_file(&path, Language::Java);

        let defs: Vec<_> = cg.definitions().collect();
        assert!(
            defs.len() >= 5,
            "Expected at least 5 definitions, got {}",
            defs.len()
        );

        let kinds: Vec<DefKind> = defs.iter().map(|(_, _, d)| d.kind).collect();
        assert!(kinds.contains(&DefKind::Class), "Should have a class");
        assert!(kinds.contains(&DefKind::Method), "Should have a method");
    }

    // ── Kotlin fixture ──────────────────────────────────────────────

    #[test]
    fn kotlin_comprehensive_fixture() {
        let path = fixture_path("kotlin/ComprehensiveKotlinDefinitions.kt");
        let cg = parse_fixture_file(&path, Language::Kotlin);

        let defs: Vec<_> = cg.definitions().collect();
        assert!(
            defs.len() >= 5,
            "Expected at least 5 definitions, got {}",
            defs.len()
        );

        let kinds: Vec<DefKind> = defs.iter().map(|(_, _, d)| d.kind).collect();
        assert!(kinds.contains(&DefKind::Class), "Should have a class");
        assert!(kinds.contains(&DefKind::Function), "Should have a function");
    }

    // ── C# fixture ──────────────────────────────────────────────────

    #[test]
    fn csharp_comprehensive_fixture() {
        let path = fixture_path("csharp/ComprehensiveCSharp.cs");
        let cg = parse_fixture_file(&path, Language::CSharp);

        let defs: Vec<_> = cg.definitions().collect();
        assert!(
            defs.len() >= 5,
            "Expected at least 5 definitions, got {}",
            defs.len()
        );

        let kinds: Vec<DefKind> = defs.iter().map(|(_, _, d)| d.kind).collect();
        assert!(kinds.contains(&DefKind::Class), "Should have a class");
    }

    // ── Full pipeline e2e ───────────────────────────────────────────

    #[test]
    fn full_pipeline_on_fixture_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        std::fs::write(
            root.join("app.py"),
            r#"
class UserService:
    def get_user(self, id):
        return self.db.find(id)

    def create_user(self, name):
        user = User(name)
        self.db.save(user)
        return user
"#,
        )
        .unwrap();

        std::fs::write(
            root.join("Service.java"),
            r#"
package com.example;

import java.util.List;

public class Service {
    public void run() {
        helper();
    }
    private void helper() {}
}
"#,
        )
        .unwrap();

        std::fs::write(
            root.join("App.kt"),
            r#"
package com.app

class App {
    fun start() {
        println("started")
    }
}
"#,
        )
        .unwrap();

        std::fs::write(
            root.join("Controller.cs"),
            r#"
using System;

namespace MyApp {
    public class Controller {
        public void Index() {}
    }
}
"#,
        )
        .unwrap();

        let capture = Arc::new(TestCapture::new());
        let sink = Arc::new(NullSink);
        let result = Pipeline::run_with_tracer(
            root,
            PipelineConfig::default(),
            crate::v2::trace::Tracer::new(false),
            capture.clone(),
            sink,
        );

        assert_eq!(result.stats.files_parsed, 4, "Should parse 4 files");
        assert_eq!(result.errors.len(), 0, "Should have no errors");

        let graphs = capture.take();
        let total_files: usize = graphs
            .iter()
            .filter(|g| g.output.includes_structure())
            .map(|g| g.files().count())
            .sum();
        let total_dirs: usize = graphs
            .iter()
            .filter(|g| g.output.includes_structure())
            .map(|g| g.directories().count())
            .sum();
        let total_edges: usize = graphs.iter().map(|g| g.edge_count()).sum();
        assert_eq!(total_files, 4);
        assert!(total_dirs > 0);
        assert!(total_edges > 0);

        let def_to_def: usize = graphs
            .iter()
            .flat_map(|g| g.edges())
            .filter(|(_, _, e)| {
                e.relationship.source_node == NodeKind::Definition
                    && e.relationship.target_node == NodeKind::Definition
            })
            .count();
        assert!(
            def_to_def >= 4,
            "Expected at least 4 def-to-def edges, got {def_to_def}"
        );

        let file_to_def: usize = graphs
            .iter()
            .flat_map(|g| g.edges())
            .filter(|(_, _, e)| {
                e.relationship.source_node == NodeKind::File
                    && e.relationship.target_node == NodeKind::Definition
            })
            .count();
        assert!(
            file_to_def >= 8,
            "Expected at least 8 file→def edges, got {file_to_def}"
        );
    }

    #[test]
    fn record_skip_and_fault_route_to_distinct_collections() {
        let ctx = Arc::new(PipelineContext {
            config: PipelineConfig::default(),
            tracer: crate::v2::trace::Tracer::new(false),
            root_path: "/".to_string(),
            skipped: std::sync::Mutex::new(Vec::new()),
            faults: std::sync::Mutex::new(Vec::new()),
        });
        ctx.record_skip(
            "src/slow.rs",
            crate::v2::error::FileSkip::TimeoutSentinel,
            "killed",
        );
        ctx.record_fault("src/bad.js", crate::v2::error::FileFault::OxcPanic, "boom");

        let skipped = ctx.skipped.lock().unwrap().clone();
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].kind, crate::v2::error::FileSkip::TimeoutSentinel);
        assert_eq!(skipped[0].path, "src/slow.rs");

        let faults = ctx.faults.lock().unwrap().clone();
        assert_eq!(faults.len(), 1);
        assert_eq!(faults[0].kind, crate::v2::error::FileFault::OxcPanic);
        assert_eq!(faults[0].path, "src/bad.js");
    }
}

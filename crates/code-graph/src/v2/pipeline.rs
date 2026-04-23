use crate::v2::config::{Language, detect_language_from_extension};
use crate::v2::sink::{BatchSink, GraphConverter};
use arrow::record_batch::RecordBatch;
use crossbeam_channel::Sender;
use ignore::WalkBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

/// Input to a language pipeline: file path (source read on demand).
pub type FileInput = String;

/// Output from a language pipeline.
///
/// - **Streamed**: data was sent through `BatchTx` during processing.
/// - **Batches**: raw Arrow `RecordBatch`es keyed by table name,
///   returned directly for the caller to forward.
pub enum PipelineOutput {
    Streamed,
    Batches(Vec<(String, RecordBatch)>),
}

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
}

impl PipelineContext {
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.config.cancel.is_cancelled()
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
/// Wraps a channel sender + converter reference.
pub struct BatchTx<'a> {
    tx: &'a Sender<(String, RecordBatch)>,
    converter: &'a dyn GraphConverter,
}

impl<'a> BatchTx<'a> {
    pub fn new(tx: &'a Sender<(String, RecordBatch)>, converter: &'a dyn GraphConverter) -> Self {
        Self { tx, converter }
    }
}

impl BatchTx<'_> {
    /// Convert graph to Arrow batches and send to the writer thread.
    /// Takes ownership — graph is dropped after conversion.
    pub fn send_graph(&self, graph: CodeGraph) {
        for (table, batch) in self.converter.convert(graph) {
            let _ = self.tx.send((table, batch));
        }
    }

    /// Send a raw pre-built batch (for custom pipelines that bypass CodeGraph).
    pub fn send_raw(&self, table: String, batch: RecordBatch) {
        let _ = self.tx.send((table, batch));
    }
}

/// Trait for language-specific pipeline execution.
///
/// All pipelines (generic and custom) stream their output through
/// a `BatchTx` handle. Graph-based pipelines use `btx.send_graph()`.
/// Batch-based pipelines use `btx.send_raw()`.
///
/// Returns `Streamed` if data was sent via `btx`, or `Batches` if
/// the caller should forward raw RecordBatches.
pub trait LanguagePipeline {
    fn process_files(
        files: &[FileInput],
        ctx: &Arc<PipelineContext>,
        btx: &BatchTx<'_>,
    ) -> Result<PipelineOutput, Vec<PipelineError>>;
}

pub struct PipelineConfig {
    pub max_file_size: u64,
    pub respect_gitignore: bool,
    pub cancel: CancellationToken,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_file_size: 1_000_000,
            respect_gitignore: true,
            cancel: CancellationToken::new(),
        }
    }
}

pub struct PipelineResult {
    pub stats: PipelineStats,
    pub errors: Vec<PipelineError>,
    /// The pipeline context, including the tracer with accumulated events.
    pub ctx: Arc<PipelineContext>,
}

/// Aggregate stats from the pipeline run.
///
/// Note: `definitions_count`, `imports_count`, `references_count`, and
/// `edges_count` only reflect `PipelineOutput::Graph` outputs. Custom
/// pipelines returning `PipelineOutput::Batches` contribute to
/// `files_parsed` but not to the entity counts.
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
        config: PipelineConfig,
        tracer: Tracer,
        converter: Arc<dyn GraphConverter>,
        sink: Arc<dyn BatchSink>,
    ) -> PipelineResult {
        let root_str = root.to_string_lossy().to_string();

        // 1. Walk filesystem, group files by language
        let pb_discover = spinner("Discovering files...");
        let files_by_language = Self::walk_and_group(root, &config);
        let total_files: usize = files_by_language.values().map(|f| f.len()).sum();
        let lang_summary: Vec<String> = files_by_language
            .iter()
            .map(|(l, f)| format!("{l}: {}", f.len()))
            .collect();
        pb_discover.finish_with_message(format!(
            "Found {total_files} files ({})",
            lang_summary.join(", ")
        ));

        let ctx = Arc::new(PipelineContext {
            config,
            tracer,
            root_path: root_str,
        });

        // 2. Process all languages concurrently. Each language gets
        //    a CPU thread (parse + resolve) and a writer thread (I/O).
        let files_parsed = AtomicUsize::new(0);
        let files_skipped = AtomicUsize::new(0);
        let all_errors = std::sync::Mutex::new(Vec::<PipelineError>::new());

        std::thread::scope(|s| {
            for (language, files) in &files_by_language {
                let ctx = &ctx;
                let converter = &converter;
                let sink = &sink;
                let files_parsed = &files_parsed;
                let files_skipped = &files_skipped;
                let all_errors = &all_errors;

                // Per-language channel: CPU thread → writer thread
                let (tx, rx) = crossbeam_channel::unbounded::<(String, RecordBatch)>();

                // Writer thread: drain channel, write each batch to sink
                s.spawn(move || {
                    for (table, batch) in rx {
                        if let Err(e) = sink.write_batch(&table, &batch) {
                            eprintln!("[v2] {language} writer error: {e}");
                        }
                    }
                });

                // CPU thread: parse + resolve, stream batches at phase boundaries
                s.spawn(move || {
                    if ctx.is_cancelled() {
                        return;
                    }
                    let file_count = files.len();
                    eprintln!("[v2] processing {language}: {file_count} files");
                    let t_lang = std::time::Instant::now();

                    let btx = BatchTx {
                        tx: &tx,
                        converter: converter.as_ref(),
                    };

                    match crate::v2::registry::dispatch_language(*language, files, ctx, &btx) {
                        Some(Ok(PipelineOutput::Streamed)) => {
                            eprintln!("[v2] {language}: done in {:.2?}", t_lang.elapsed());
                            files_parsed.fetch_add(file_count, Ordering::Relaxed);
                        }
                        Some(Ok(PipelineOutput::Batches(batches))) => {
                            eprintln!(
                                "[v2] {language}: done in {:.2?} ({} batches)",
                                t_lang.elapsed(),
                                batches.len(),
                            );
                            for (table, batch) in batches {
                                btx.send_raw(table, batch);
                            }
                            files_parsed.fetch_add(file_count, Ordering::Relaxed);
                        }
                        Some(Err(errors)) => {
                            eprintln!("[v2] {language}: failed with {} errors", errors.len());
                            files_skipped.fetch_add(file_count, Ordering::Relaxed);
                            all_errors.lock().unwrap().extend(errors);
                        }
                        None => {
                            eprintln!(
                                "[v2] {language}: not supported, skipping {file_count} files"
                            );
                            files_skipped.fetch_add(file_count, Ordering::Relaxed);
                        }
                    }
                    // tx dropped here — writer thread exits
                });
            }
        }); // all threads join here

        PipelineResult {
            stats: PipelineStats {
                files_parsed: files_parsed.into_inner(),
                files_skipped: files_skipped.into_inner(),
                definitions_count: 0,
                imports_count: 0,
                references_count: 0,
                edges_count: 0,
            },
            errors: all_errors.into_inner().unwrap(),
            ctx,
        }
    }

    fn walk_and_group(root: &Path, config: &PipelineConfig) -> FxHashMap<Language, Vec<FileInput>> {
        let mut groups: FxHashMap<Language, Vec<FileInput>> = FxHashMap::default();

        let walker = WalkBuilder::new(root)
            .git_ignore(config.respect_gitignore)
            .hidden(true)
            .build();

        for entry in walker.flatten() {
            if !entry.file_type().is_some_and(|ft| ft.is_file()) {
                continue;
            }

            let path = entry.path();

            if let Ok(metadata) = path.metadata()
                && metadata.len() > config.max_file_size
            {
                continue;
            }

            let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
                continue;
            };

            let rel_path = path.strip_prefix(root).unwrap_or(path).to_string_lossy();
            if let Some(lang) = detect_language_from_extension(ext) {
                if lang
                    .exclude_extensions()
                    .iter()
                    .any(|excl| rel_path.ends_with(excl))
                {
                    continue;
                }

                // Verify file is readable, but don't load yet.
                if !path.is_file() {
                    continue;
                }

                groups.entry(lang).or_default().push(rel_path.to_string());
            }
        }

        groups
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
    ) -> Result<PipelineOutput, Vec<PipelineError>> {
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

        // Spawn sentinel watchdog if per_file_timeout is configured
        let sentinel = rules
            .settings
            .per_file_timeout
            .map(crate::v2::sentinel::spawn_sentinel);

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
        struct ParsedFile {
            path_idx: usize,
            result: ParseFullResult,
            ext: String,
            file_size: u64,
        }

        let parsed: Vec<Option<ParsedFile>> = files
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
                    Err(_) => {
                        pb.inc(1);
                        return None;
                    }
                };

                let result = match spec.parse_full_collect(&source, path, language, tracer) {
                    Ok(r) => r,
                    Err(_) => {
                        pb.inc(1);
                        return None;
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
                Some(ParsedFile {
                    path_idx: idx,
                    result,
                    ext,
                    file_size,
                })
            })
            .collect();

        // Phase 1b: add defs/imports to graph sequentially. Keep refs alive.
        struct FileWithRefs {
            info: FileInfo,
            refs: Vec<CollectedRef>,
            inferred_returns: Vec<(u32, String)>,
            unresolved_aliases: Vec<(usize, String)>,
        }

        let mut graph =
            CodeGraph::new_with_root(root_path.to_string()).with_rules(lang_ctx.rules.clone());
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
                        r.reaching = vec![crate::v2::types::ssa::ParseValue::Type(fqn)];
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
                        break; // file killed by sentinel
                    }
                    if edges.len() == before && r.chain.as_ref().is_some_and(|c| c.len() >= 2) {
                        failed_chains.push((
                            r.name.clone(),
                            r.chain.clone().unwrap(),
                            r.reaching.clone().into(),
                            r.enclosing_def,
                        ));
                    }
                }

                edges.extend(resolver.drain_import_edges());
                total_edges.fetch_add(edges.len(), std::sync::atomic::Ordering::Relaxed);
                pb2.inc(1);
                (edges, fwr.inferred_returns, Some(fwr.info), failed_chains)
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

        for (edges, inferred, info_opt, failed_chains) in resolve_results {
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
                    eprintln!("[v2-sp] phase 3: {phase3_edges} cross-file edges resolved");
                }
            }
        }

        // Shut down sentinel
        if let Some((handle, join)) = sentinel {
            handle.shutdown();
            let _ = join.join();
        }

        eprintln!(
            "[v2-sp] total: {:.2?} ({} nodes, {} edges, defs={}, imports={}, pool={})",
            t0.elapsed(),
            graph.graph.node_count(),
            graph.graph.edge_count(),
            graph.defs.len(),
            graph.imports.len(),
            graph.strings.len(),
        );

        // ── Stream graph to writer thread, then drop it ─────────
        btx.send_graph(graph);

        Ok(PipelineOutput::Streamed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::types::{DefKind, NodeKind};

    fn fixture_path(relative: &str) -> String {
        let manifest = env!("CARGO_MANIFEST_DIR");
        format!("{manifest}/src/legacy/parser/{relative}")
    }

    fn parse_fixture_file(path: &str, language: Language) -> CodeGraph {
        let ctx = Arc::new(PipelineContext {
            config: PipelineConfig::default(),
            tracer: crate::v2::trace::Tracer::new(false),
            root_path: "/".to_string(),
        });
        let capture = Arc::new(crate::v2::sink::GraphCapture::new());
        let (tx, _rx) = crossbeam_channel::unbounded();
        let btx = BatchTx {
            tx: &tx,
            converter: capture.as_ref(),
        };
        let output =
            crate::v2::registry::dispatch_language(language, &[path.to_string()], &ctx, &btx)
                .unwrap_or_else(|| panic!("Language {language} not supported"))
                .unwrap_or_else(|e| panic!("Failed to parse: {e:?}"));
        // Forward Batches through btx if needed
        if let PipelineOutput::Batches(batches) = output {
            for (table, batch) in batches {
                btx.send_raw(table, batch);
            }
        }
        let mut graphs = capture.take();
        assert!(!graphs.is_empty(), "expected graph output");
        graphs.remove(0)
    }

    // ── Python fixture ──────────────────────────────────────────────

    #[test]
    fn python_definitions_fixture() {
        let path = fixture_path("python/fixtures/definitions.py");
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
        let path = fixture_path("java/fixtures/ComprehensiveJavaDefinitions.java");
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
        let path = fixture_path("kotlin/fixtures/ComprehensiveKotlinDefinitions.kt");
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
        let path = fixture_path("csharp/fixtures/ComprehensiveCSharp.cs");
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

        let capture = Arc::new(crate::v2::sink::GraphCapture::new());
        let sink = Arc::new(crate::v2::sink::NullSink);
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
        let total_files: usize = graphs.iter().map(|g| g.files().count()).sum();
        let total_dirs: usize = graphs.iter().map(|g| g.directories().count()).sum();
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
}

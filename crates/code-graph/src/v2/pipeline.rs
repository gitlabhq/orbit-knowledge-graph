use crate::v2::config::{Language, detect_language_from_extension};
use ignore::WalkBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Mutex;

use crate::v2::linker::CodeGraph;

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
/// - **Graph**: the standard `CodeGraph` output (generic pipelines).
/// - **Batches**: raw Arrow `RecordBatch`es keyed by table name (custom
///   pipelines that bypass `CodeGraph` entirely).
pub enum PipelineOutput {
    Graph(Box<CodeGraph>),
    Batches(Vec<(String, arrow::record_batch::RecordBatch)>),
}

/// Trait for language-specific graph production.
///
/// Two strategies:
/// - **Generic**: `GenericPipeline<P, R>` for languages using the standard
///   parse+walk → resolve → graph flow.
/// - **Custom**: implement directly for languages that need full control
///   over parsing and linking. Custom pipelines can emit `RecordBatch`es
///   directly without going through `CodeGraph`.
pub trait LanguagePipeline {
    fn process_files(
        files: &[FileInput],
        root_path: &str,
    ) -> Result<PipelineOutput, Vec<PipelineError>>;
}

pub struct PipelineConfig {
    pub max_file_size: u64,
    pub respect_gitignore: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_file_size: 1_000_000,
            respect_gitignore: true,
        }
    }
}

pub struct PipelineResult {
    pub graphs: Vec<CodeGraph>,
    pub batches: Vec<(String, arrow::record_batch::RecordBatch)>,
    pub stats: PipelineStats,
    pub errors: Vec<PipelineError>,
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

pub struct Pipeline {
    config: PipelineConfig,
}

impl Pipeline {
    pub fn new(config: PipelineConfig) -> Self {
        Self { config }
    }

    pub fn run(&self, root: &Path) -> PipelineResult {
        let root_str = root.to_string_lossy().to_string();

        // 1. Walk filesystem, group files by language
        let pb_discover = spinner("Discovering files...");
        let files_by_language = self.walk_and_group(root);
        let total_files: usize = files_by_language.values().map(|f| f.len()).sum();
        let lang_summary: Vec<String> = files_by_language
            .iter()
            .map(|(l, f)| format!("{l}: {}", f.len()))
            .collect();
        pb_discover.finish_with_message(format!(
            "Found {total_files} files ({})",
            lang_summary.join(", ")
        ));

        // 2. Process each language through its pipeline
        let mut all_graphs: Vec<CodeGraph> = Vec::new();
        let mut all_batches: Vec<(String, arrow::record_batch::RecordBatch)> = Vec::new();
        let mut all_errors: Vec<PipelineError> = Vec::new();
        let mut files_parsed = 0usize;
        let mut files_skipped = 0usize;

        for (language, files) in &files_by_language {
            let file_count = files.len();
            eprintln!("[v2] processing {language}: {file_count} files");
            let t_lang = std::time::Instant::now();

            match crate::v2::registry::dispatch_language(*language, files, &root_str) {
                Some(Ok(PipelineOutput::Graph(graph))) => {
                    eprintln!(
                        "[v2] {language}: done in {:.2?} ({} nodes, {} edges)",
                        t_lang.elapsed(),
                        graph.node_count(),
                        graph.edge_count()
                    );
                    files_parsed += file_count;
                    all_graphs.push(*graph);
                }
                Some(Ok(PipelineOutput::Batches(batches))) => {
                    let row_count: usize = batches.iter().map(|(_, b)| b.num_rows()).sum();
                    eprintln!(
                        "[v2] {language}: done in {:.2?} ({} batches, {} total rows)",
                        t_lang.elapsed(),
                        batches.len(),
                        row_count,
                    );
                    files_parsed += file_count;
                    all_batches.extend(batches);
                }
                Some(Err(errors)) => {
                    eprintln!("[v2] {language}: failed with {} errors", errors.len());
                    files_skipped += file_count;
                    all_errors.extend(errors);
                }
                None => {
                    eprintln!("[v2] {language}: not supported, skipping {file_count} files");
                    files_skipped += file_count;
                }
            }
        }

        let definitions_count = all_graphs.iter().map(|g| g.definitions().count()).sum();
        let imports_count = all_graphs.iter().map(|g| g.imports_iter().count()).sum();
        let references_count = all_graphs.iter().map(|g| g.edges().count()).sum();
        let edges_count = all_graphs.iter().map(|g| g.edge_count()).sum();

        PipelineResult {
            graphs: all_graphs,
            batches: all_batches,
            stats: PipelineStats {
                files_parsed,
                files_skipped,
                definitions_count,
                imports_count,
                references_count,
                edges_count,
            },
            errors: all_errors,
        }
    }

    fn walk_and_group(&self, root: &Path) -> FxHashMap<Language, Vec<FileInput>> {
        let mut groups: FxHashMap<Language, Vec<FileInput>> = FxHashMap::default();

        let walker = WalkBuilder::new(root)
            .git_ignore(self.config.respect_gitignore)
            .hidden(true)
            .build();

        for entry in walker.flatten() {
            if !entry.file_type().is_some_and(|ft| ft.is_file()) {
                continue;
            }

            let path = entry.path();

            if let Ok(metadata) = path.metadata()
                && metadata.len() > self.config.max_file_size
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
/// - **Phase 1** (parallel): `parse_defs_only()` → add defs/imports to graph under Mutex
/// - **Finalize**: build ancestor chains, drop construction indexes
/// - **Phase 2** (parallel): `parse_full_and_resolve()` with callback → edges
pub struct GenericPipeline<P, R>(PhantomData<(P, R)>);

impl<P, R> LanguagePipeline for GenericPipeline<P, R>
where
    P: crate::v2::dsl::types::DslLanguage + 'static,
    R: crate::v2::linker::HasRules + Send + Sync,
{
    fn process_files(
        files: &[FileInput],
        root_path: &str,
    ) -> Result<PipelineOutput, Vec<PipelineError>> {
        let spec = P::spec();
        let rules = R::rules();
        let language = P::language();
        let file_count = files.len();
        let t0 = std::time::Instant::now();

        eprintln!(
            "[v2-sp] {file_count} files, {} threads",
            rayon::current_num_threads()
        );

        // ── Phase 1: parallel parse_defs_only + graph build ─────
        let graph = Mutex::new(CodeGraph::new_with_root(root_path.to_string()));
        let pb = progress_bar(file_count as u64, "parse + graph");
        let errors = Mutex::new(Vec::new());
        let total_defs = std::sync::atomic::AtomicUsize::new(0);
        let total_imports = std::sync::atomic::AtomicUsize::new(0);

        struct FileInfo {
            file_node: petgraph::graph::NodeIndex,
            def_nodes: Vec<petgraph::graph::NodeIndex>,
            import_nodes: Vec<petgraph::graph::NodeIndex>,
        }

        let file_infos: Vec<Option<FileInfo>> = files
            .par_iter()
            .enumerate()
            .map(|(_, path)| {
                let abs_path = format!("{root_path}/{path}");
                let source = match std::fs::read(&abs_path) {
                    Ok(s) => s,
                    Err(e) => {
                        errors.lock().unwrap().push(PipelineError {
                            file_path: path.clone(),
                            error: format!("Read error: {e}"),
                        });
                        pb.inc(1);
                        return None;
                    }
                };

                let result = match spec.parse_defs_only(&source, path, language) {
                    Ok(r) => r,
                    Err(e) => {
                        errors.lock().unwrap().push(PipelineError {
                            file_path: path.clone(),
                            error: format!("Parse error: {e}"),
                        });
                        pb.inc(1);
                        return None;
                    }
                };

                total_defs.fetch_add(
                    result.definitions.len(),
                    std::sync::atomic::Ordering::Relaxed,
                );
                total_imports.fetch_add(result.imports.len(), std::sync::atomic::Ordering::Relaxed);

                let ext = path.rsplit_once('.').map(|(_, e)| e).unwrap_or("");
                let file_size = source.len() as u64;

                let (file_node, def_nodes, import_nodes) = {
                    let mut g = graph.lock().unwrap();
                    g.add_file(
                        path,
                        ext,
                        language,
                        file_size,
                        &result.definitions,
                        &result.imports,
                    )
                };

                pb.inc(1);
                Some(FileInfo {
                    file_node,
                    def_nodes,
                    import_nodes,
                })
            })
            .collect();

        pb.finish_with_message(format!(
            "{} defs, {} imports in {:.2?}",
            total_defs.load(std::sync::atomic::Ordering::Relaxed),
            total_imports.load(std::sync::atomic::Ordering::Relaxed),
            t0.elapsed()
        ));

        let errors = errors.into_inner().unwrap();
        if !errors.is_empty() && file_infos.iter().all(|r| r.is_none()) {
            return Err(errors);
        }

        let mut graph = graph.into_inner().unwrap();
        graph.finalize();

        // ── Phase 2: parallel parse_full + resolve (callback) ──
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
            Vec<(
                String,
                Vec<crate::v2::types::ExpressionStep>,
                smallvec::SmallVec<[crate::v2::types::ssa::ParseValue; 2]>,
                Option<u32>,
            )>,
        );

        let resolve_results: Vec<Phase2Result> = file_infos
            .into_par_iter()
            .zip(files.par_iter())
            .map(|(info_opt, path)| -> Phase2Result {
                let Some(info) = info_opt else {
                    pb2.inc(1);
                    return Default::default();
                };

                let abs_path = format!("{root_path}/{path}");
                let source = match std::fs::read(&abs_path) {
                    Ok(s) => s,
                    Err(_) => {
                        pb2.inc(1);
                        return Default::default();
                    }
                };

                let mut resolver = crate::v2::linker::FileResolver::new(
                    &graph,
                    info.file_node,
                    &info.def_nodes,
                    &info.import_nodes,
                    &rules,
                    &rules.settings,
                );
                let mut edges = Vec::new();
                let mut failed_chains: Vec<FailedChain> = Vec::new();
                let mut inferred_set = false;

                let inferred_result = spec.parse_full_and_resolve(
                    &source,
                    path,
                    language,
                    |name, chain, reaching, enclosing_def, inferred| {
                        if !inferred_set {
                            resolver.set_inferred_returns(inferred);
                            inferred_set = true;
                        }
                        let before = edges.len();
                        resolver.resolve(name, chain, reaching, enclosing_def, &mut edges);
                        // Store failed chain refs for Phase 3 re-resolution
                        if edges.len() == before && chain.is_some_and(|c| c.len() >= 2) {
                            failed_chains.push((
                                name.to_string(),
                                chain.unwrap().to_vec(),
                                reaching.into(),
                                enclosing_def,
                            ));
                        }
                    },
                );

                let inferred = inferred_result.unwrap_or_default();

                total_edges.fetch_add(edges.len(), std::sync::atomic::Ordering::Relaxed);
                pb2.inc(1);
                (edges, inferred, Some(info), failed_chains)
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
                        let mut resolver = crate::v2::linker::FileResolver::new(
                            &graph,
                            info.file_node,
                            &info.def_nodes,
                            &info.import_nodes,
                            &rules,
                            &rules.settings,
                        );
                        let mut edges = Vec::new();
                        for (name, chain, reaching, enclosing_def) in failed_chains {
                            resolver.resolve(
                                name,
                                Some(chain),
                                reaching,
                                *enclosing_def,
                                &mut edges,
                            );
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

        eprintln!(
            "[v2-sp] total: {:.2?} ({} nodes, {} edges, defs={}, imports={}, pool={})",
            t0.elapsed(),
            graph.graph.node_count(),
            graph.graph.edge_count(),
            graph.defs.len(),
            graph.imports.len(),
            graph.strings.len(),
        );
        Ok(PipelineOutput::Graph(Box::new(graph)))
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
        let output = crate::v2::registry::dispatch_language(language, &[path.to_string()], "/")
            .unwrap_or_else(|| panic!("Language {language} not supported"))
            .unwrap_or_else(|e| panic!("Failed to parse: {e:?}"));
        match output {
            PipelineOutput::Graph(g) => *g,
            PipelineOutput::Batches(_) => panic!("expected Graph output"),
        }
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

        let pipeline = Pipeline::new(PipelineConfig::default());
        let result = pipeline.run(root);

        assert_eq!(result.stats.files_parsed, 4, "Should parse 4 files");
        assert_eq!(result.errors.len(), 0, "Should have no errors");

        assert!(
            result.stats.definitions_count >= 8,
            "Expected at least 8 definitions, got {}",
            result.stats.definitions_count
        );

        let total_files: usize = result.graphs.iter().map(|g| g.files().count()).sum();
        let total_dirs: usize = result.graphs.iter().map(|g| g.directories().count()).sum();
        let total_edges: usize = result.graphs.iter().map(|g| g.edge_count()).sum();
        assert_eq!(total_files, 4);
        assert!(total_dirs > 0);
        assert!(total_edges > 0);

        let def_to_def: usize = result
            .graphs
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

        let file_to_def: usize = result
            .graphs
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

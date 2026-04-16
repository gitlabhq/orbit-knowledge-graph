use code_graph_config::{Language, detect_language_from_extension};
use code_graph_types::CanonicalParser;
use ignore::WalkBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use parser_core::dsl::types::{DslLanguage, DslParser};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Mutex;

use crate::linker::v2::walker::fused_walk_file;
use crate::linker::v2::{CodeGraph, HasRoot, HasRules};

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
use crate::v2::langs::csharp::CSharpDsl;
use crate::v2::langs::go::{GoDsl, GoRules};
use crate::v2::langs::java::{JavaDsl, JavaRules};
use crate::v2::langs::kotlin::{KotlinDsl, KotlinRules};
use crate::v2::langs::python::{PythonDsl, PythonRules};
use crate::v2::langs::ruby::{RubyDsl, RubyRules};

/// Input to a language pipeline: file path (source read on demand).
pub type FileInput = String;

/// Trait for language-specific graph production.
///
/// Two strategies:
/// - **Generic**: `GenericPipeline<P, R>` for languages using the standard
///   parse+walk → resolve → graph flow.
/// - **Custom**: implement directly for languages that need full control
///   over parsing and linking (e.g. Ruby).
pub trait LanguagePipeline {
    fn process_files(
        files: Vec<FileInput>,
        root_path: &str,
    ) -> Result<CodeGraph, Vec<PipelineError>>;
}

fn report_rss(label: &str) {
    let pid = std::process::id();
    if let Ok(output) = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "rss="])
        .output()
        && let Ok(rss) = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u64>()
    {
        eprintln!("[mem] {label}: {:.1} MB", rss as f64 / 1024.0);
    }
}

/// Generic pipeline parameterized by parser `P` and rules `R`.
///
/// Two-phase fused architecture:
/// 1. **Phase 1** (parallel): parse + extract defs/imports → add to graph, drop AST
/// 2. **Phase 2** (parallel): re-parse + fused walk+resolve → emit edges
///
/// Phase 2 re-parses with tree-sitter (~150ms for 22k files). Peak memory =
/// graph + ~16 concurrent ASTs (rayon threads), not graph + all ASTs.
pub struct GenericPipeline<P, R>(PhantomData<(P, R)>);

impl<P, R> LanguagePipeline for GenericPipeline<P, R>
where
    P: CanonicalParser + Default + Sync + Send,
    P::Ast: HasRoot + Send,
    R: HasRules + Send + Sync,
{
    fn process_files(
        files: Vec<FileInput>,
        root_path: &str,
    ) -> Result<CodeGraph, Vec<PipelineError>> {
        let parser = P::default();
        let rules = R::rules();
        let file_count = files.len();
        let num_threads = rayon::current_num_threads();
        let t0 = std::time::Instant::now();

        eprintln!("[v2] {file_count} files, {num_threads} threads");
        report_rss("before Phase 1");

        let graph = Mutex::new(CodeGraph::new_with_root(root_path.to_string()));

        // ── Phase 1: read + parse defs/imports → graph ──────────
        // Source read per-file on demand, dropped after add_file_nodes.
        let pb = progress_bar(file_count as u64, "Phase 1: defs");
        let phase1_results: Vec<_> = files
            .par_iter()
            .enumerate()
            .map(|(file_idx, path)| {
                let abs_path = format!("{root_path}/{path}");
                let source = std::fs::read(&abs_path).map_err(|e| PipelineError {
                    file_path: path.clone(),
                    error: format!("Read error: {e}"),
                })?;

                let result = parser
                    .parse_defs_only(&source, path)
                    .map_err(|e| PipelineError {
                        file_path: path.clone(),
                        error: format!("Parse error: {e}"),
                    })?;

                let defs = result.definitions.len();
                let imports = result.imports.len();

                let file_node = {
                    let mut g = graph.lock().unwrap();
                    let (file_node, _, _) = g.add_file_nodes(result, file_idx);
                    file_node
                };
                // source dropped here

                pb.inc(1);
                Ok((file_node, defs, imports))
            })
            .collect();
        pb.finish_with_message(format!(
            "Phase 1: {file_count} files in {:.2?}",
            t0.elapsed()
        ));

        let mut file_nodes = Vec::with_capacity(file_count);
        let mut errors = Vec::new();
        let mut total_defs = 0usize;
        let mut total_imports = 0usize;

        for output in phase1_results {
            match output {
                Ok((file_node, defs, imports)) => {
                    total_defs += defs;
                    total_imports += imports;
                    file_nodes.push(file_node);
                }
                Err(err) => {
                    errors.push(err);
                    file_nodes.push(petgraph::graph::NodeIndex::new(0));
                }
            }
        }

        if !errors.is_empty() && file_nodes.is_empty() {
            return Err(errors);
        }

        report_rss("after Phase 1 (graph + source bytes)");

        // ── Finalize graph ──────────────────────────────────────
        let t1 = std::time::Instant::now();
        let mut graph = graph.into_inner().unwrap();
        graph.finalize();
        eprintln!(
            "[v2] {total_defs} defs, {total_imports} imports, {} errors",
            errors.len()
        );
        eprintln!("[v2] graph finalize: {:.2?}", t1.elapsed());

        let node_count = graph.graph.node_count();
        let edge_count = graph.graph.edge_count();
        eprintln!(
            "[mem] graph: {} nodes × {} B + {} edges × {} B = {:.1} MB (estimated vec capacity)",
            node_count,
            std::mem::size_of::<petgraph::graph::Node<super::super::linker::v2::graph::GraphNode>>(
            ),
            edge_count,
            std::mem::size_of::<petgraph::graph::Edge<super::super::linker::v2::graph::GraphEdge>>(
            ),
            (node_count
                * std::mem::size_of::<
                    petgraph::graph::Node<super::super::linker::v2::graph::GraphNode>,
                >()
                + edge_count
                    * std::mem::size_of::<
                        petgraph::graph::Edge<super::super::linker::v2::graph::GraphEdge>,
                    >()) as f64
                / 1048576.0,
        );
        report_rss("after finalize (graph + indexes + source bytes)");

        // ── Phase 2: fused walk+resolve ────────────────────────
        // Re-parse from disk per file, or skip on Phase 1 error.
        let t2 = std::time::Instant::now();
        let pb2 = progress_bar(file_count as u64, "Phase 2: resolve");

        let phase2_results: Vec<_> = files
            .par_iter()
            .zip(file_nodes.par_iter())
            .map(|(path, &file_node)| {
                if file_node.index() == 0 && errors.iter().any(|e| e.file_path == *path) {
                    pb2.inc(1);
                    return None;
                }
                let abs_path = format!("{root_path}/{path}");
                let source = match std::fs::read(&abs_path) {
                    Ok(s) => s,
                    Err(_) => {
                        pb2.inc(1);
                        return None;
                    }
                };
                let source_str = match std::str::from_utf8(&source) {
                    Ok(s) => s,
                    Err(_) => {
                        pb2.inc(1);
                        return None;
                    }
                };
                let lang = code_graph_config::detect_language_from_path(path)?;
                let t_parse = std::time::Instant::now();
                let ast = lang.parse_ast(source_str);
                let root = ast.root();
                let parse_ns = t_parse.elapsed().as_nanos() as u64;
                let t_walk = std::time::Instant::now();
                let mut result = fused_walk_file(&rules, &graph, &root, file_node, &rules.settings);
                let walk_ns = t_walk.elapsed().as_nanos() as u64;
                result.parse_ns = parse_ns;
                result.walk_ns = walk_ns;
                pb2.inc(1);
                // source dropped here
                Some(result)
            })
            .collect();
        pb2.finish_with_message(format!(
            "Phase 2: {file_count} files in {:.2?}",
            t2.elapsed()
        ));

        // ── Collect edges + stats ───────────────────────────────
        let mut combined_stats = crate::linker::v2::ResolveStats::default();
        let mut total_edges = 0usize;
        let mut total_refs = 0usize;
        let mut total_parse_ns = 0u64;
        let mut total_walk_ns = 0u64;

        for result in phase2_results.into_iter().flatten() {
            total_edges += result.edges.len();
            total_refs += result.num_refs;
            total_parse_ns += result.parse_ns;
            total_walk_ns += result.walk_ns;
            combined_stats.merge(&result.stats);
            for (src, tgt, edge) in result.edges {
                graph.graph.add_edge(src, tgt, edge);
            }
        }

        eprintln!(
            "[v2] Phase 2 breakdown: parse {:.2?}, walk+resolve {:.2?} (sum across threads)",
            std::time::Duration::from_nanos(total_parse_ns),
            std::time::Duration::from_nanos(total_walk_ns),
        );

        report_rss("after Phase 2 (graph + edges + source bytes)");

        eprintln!(
            "[v2] resolve: {total_refs} refs → {total_edges} edges in {:.2?}",
            t2.elapsed()
        );
        combined_stats.print();

        Ok(graph)
    }
}

/// Registration macro for v2 pipelines.
///
/// Generates `dispatch_language` which routes files to the correct
/// `LanguagePipeline` implementation per language.
///
/// Adding a new language: implement `LanguagePipeline` (or use
/// `GenericPipeline<YourParser>`), add one line here.
macro_rules! register_v2_pipelines {
    ($( $variant:ident => $pipeline:ty ),+ $(,)?) => {
        fn dispatch_language(
            language: Language,
            files: Vec<FileInput>,
            root_path: &str,
        ) -> Option<Result<CodeGraph, Vec<PipelineError>>> {
            Some(match language {
                $(Language::$variant => <$pipeline>::process_files(files, root_path),)+
                _ => return None,
            })
        }
    };
}

/// No-op rules: parse + chain resolution only, no SSA import strategies.
macro_rules! no_op_rules {
    ($name:ident, $dsl:ty, $sep:expr) => {
        pub struct $name;
        impl HasRules for $name {
            fn rules() -> crate::linker::v2::ResolutionRules {
                let spec = <$dsl>::spec();
                let scopes = crate::linker::v2::ResolutionRules::derive_scopes(&spec);
                crate::linker::v2::ResolutionRules::new(
                    stringify!($name),
                    scopes,
                    spec,
                    vec![],
                    vec![],
                    crate::linker::v2::rules::ChainMode::ValueFlow,
                    crate::linker::v2::rules::ReceiverMode::None,
                    $sep,
                    &[],
                    None,
                )
            }
        }
    };
}

no_op_rules!(CSharpNoRules, CSharpDsl, ".");

register_v2_pipelines! {
    Python  => GenericPipeline<DslParser<PythonDsl>, PythonRules>,
    Java    => GenericPipeline<DslParser<JavaDsl>, JavaRules>,
    Kotlin  => GenericPipeline<DslParser<KotlinDsl>, KotlinRules>,
    CSharp  => GenericPipeline<DslParser<CSharpDsl>, CSharpNoRules>,
    Go      => GenericPipeline<DslParser<GoDsl>, GoRules>,
    Ruby    => GenericPipeline<DslParser<RubyDsl>, RubyRules>,
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
    pub stats: PipelineStats,
    pub errors: Vec<PipelineError>,
}

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
        let mut all_errors: Vec<PipelineError> = Vec::new();
        let mut files_parsed = 0usize;
        let mut files_skipped = 0usize;

        for (language, files) in files_by_language {
            let file_count = files.len();
            eprintln!("[v2] processing {language}: {file_count} files");
            let t_lang = std::time::Instant::now();

            match dispatch_language(language, files, &root_str) {
                Some(Ok(graph)) => {
                    eprintln!(
                        "[v2] {language}: done in {:.2?} ({} nodes, {} edges)",
                        t_lang.elapsed(),
                        graph.node_count(),
                        graph.edge_count()
                    );
                    files_parsed += file_count;
                    all_graphs.push(graph);
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
        let imports_count = all_graphs.iter().map(|g| g.imports().count()).sum();
        let references_count = all_graphs.iter().map(|g| g.edges().count()).sum();
        let edges_count = all_graphs.iter().map(|g| g.edge_count()).sum();

        PipelineResult {
            graphs: all_graphs,
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

#[cfg(test)]
mod tests {
    use super::*;
    use code_graph_types::{DefKind, NodeKind};

    fn fixture_path(relative: &str) -> String {
        let manifest = env!("CARGO_MANIFEST_DIR");
        format!("{manifest}/parser/src/{relative}")
    }

    fn parse_fixture_file(path: &str, language: Language) -> CodeGraph {
        dispatch_language(language, vec![path.to_string()], "/")
            .unwrap_or_else(|| panic!("Language {language} not supported"))
            .unwrap_or_else(|e| panic!("Failed to parse: {e:?}"))
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

        let names: Vec<&str> = defs.iter().map(|(_, _, d)| d.name.as_str()).collect();
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

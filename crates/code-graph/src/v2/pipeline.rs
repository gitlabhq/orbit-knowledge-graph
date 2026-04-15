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

use crate::linker::v2::walker::{FileWalkResult, HasRoot};
use crate::linker::v2::{CodeGraph, HasRules, build_edges};

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
use crate::v2::langs::java::{JavaDsl, JavaRules};
use crate::v2::langs::kotlin::{KotlinDsl, KotlinRules};
use crate::v2::langs::python::{PythonDsl, PythonRules};

/// Input to a language pipeline: file path + source bytes.
pub type FileInput = (String, Vec<u8>);

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

/// Generic pipeline parameterized by parser `P` and rules `R`.
///
/// Streaming architecture:
/// 1. **Parallel**: parse + walk each file, drop AST immediately
/// 2. **Sequential**: build indexes, resolve cross-file references, build graph
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

        // Graph exists before the parallel phase. Each file locks it
        // briefly to add nodes, gets NodeIndex values, then walks.
        let graph = Mutex::new(CodeGraph::new_with_root(root_path.to_string()));

        // ── Parallel phase: parse + add nodes + walk ────────────
        let pb = progress_bar(file_count as u64, "Parse + walk");
        let file_outputs: Vec<_> = files
            .par_iter()
            .enumerate()
            .map(|(file_idx, (path, source))| {
                let (result, ast) = parser.parse_file(source, path).map_err(|e| PipelineError {
                    file_path: path.clone(),
                    error: format!("Parse error: {e}"),
                })?;

                // Add this file's nodes to the graph under the lock.
                let (def_nodes, import_nodes) = {
                    let mut g = graph.lock().unwrap();
                    g.add_file_nodes(&result, file_idx)
                };

                let walk = if let Some(root) = ast.as_root() {
                    crate::linker::v2::walker::walk_file(
                        &rules,
                        file_idx,
                        &result,
                        &root,
                        &def_nodes,
                        &import_nodes,
                    )
                } else {
                    FileWalkResult::empty()
                };

                pb.inc(1);
                Ok((result, walk))
            })
            .collect();
        pb.finish_with_message(format!(
            "Parse + walk: {file_count} files in {:.2?}",
            t0.elapsed()
        ));

        // ── Collect results ─────────────────────────────────────
        let mut results = Vec::with_capacity(file_outputs.len());
        let mut walks = Vec::with_capacity(file_outputs.len());
        let mut errors = Vec::new();

        for output in file_outputs {
            match output {
                Ok((result, walk)) => {
                    results.push(result);
                    walks.push(walk);
                }
                Err(err) => errors.push(err),
            }
        }

        if !errors.is_empty() && results.is_empty() {
            return Err(errors);
        }

        // ── Sequential phase ────────────────────────────────────
        let total_defs: usize = results.iter().map(|r| r.definitions.len()).sum();
        let total_refs: usize = results.iter().map(|r| r.references.len()).sum();
        let total_imports: usize = results.iter().map(|r| r.imports.len()).sum();
        eprintln!(
            "[v2] {total_defs} defs, {total_refs} refs, {total_imports} imports, {} errors",
            errors.len()
        );

        let t2 = std::time::Instant::now();
        let mut graph = graph.into_inner().unwrap();
        graph.finalize(&results);
        eprintln!("[v2] graph finalize: {:.2?}", t2.elapsed());

        let t3 = std::time::Instant::now();
        let result = build_edges(&rules, &graph, &results, &mut walks, &rules.settings);
        eprintln!(
            "[v2] resolve: {} edges in {:.2?}",
            result.edges.len(),
            t3.elapsed()
        );
        result.stats.print();

        for (src, tgt, edge) in result.edges {
            graph.graph.add_edge(src, tgt, edge);
        }

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

/// No-op rules for languages without resolution (parse-only).
pub struct NoRules;
impl HasRules for NoRules {
    fn rules() -> crate::linker::v2::ResolutionRules {
        let spec = CSharpDsl::spec();
        let scopes = crate::linker::v2::ResolutionRules::derive_scopes(&spec);
        crate::linker::v2::ResolutionRules::new(
            "noop",
            scopes,
            spec,
            vec![],
            vec![],
            crate::linker::v2::rules::ChainMode::ValueFlow,
            crate::linker::v2::rules::ReceiverMode::None,
            ".",
            &[],
            None,
        )
    }
}

register_v2_pipelines! {
    // Generic: DSL parser + rules-based SSA resolver
    Python  => GenericPipeline<DslParser<PythonDsl>, PythonRules>,
    Java    => GenericPipeline<DslParser<JavaDsl>, JavaRules>,
    Kotlin  => GenericPipeline<DslParser<KotlinDsl>, KotlinRules>,
    CSharp  => GenericPipeline<DslParser<CSharpDsl>, NoRules>,

    // Custom: full control over parse + link
    Ruby    => crate::v2::custom::ruby::RubyPipeline,
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
    pub graph: CodeGraph,
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

        // 3. Merge all per-language graphs
        let graph = Self::merge_graphs(all_graphs);

        let definitions_count = graph.definitions().count();
        let imports_count = graph.imports().count();
        let references_count = 0;
        let edges_count = graph.edge_count();

        PipelineResult {
            graph,
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

                let source = match std::fs::read(path) {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                groups
                    .entry(lang)
                    .or_default()
                    .push((rel_path.to_string(), source));
            }
        }

        groups
    }

    fn merge_graphs(graphs: Vec<CodeGraph>) -> CodeGraph {
        use crate::linker::v2::graph::GraphNode;
        use petgraph::graph::NodeIndex;
        use rustc_hash::FxHashMap;

        let mut merged = CodeGraph::new();

        for g in graphs {
            // Map old node indices to new ones
            let mut index_map: FxHashMap<NodeIndex, NodeIndex> = FxHashMap::default();

            for old_idx in g.graph.node_indices() {
                let node = g.graph[old_idx].clone();
                let new_idx = merged.graph.add_node(node.clone());
                index_map.insert(old_idx, new_idx);

                // Update quick-lookup indexes
                match &node {
                    GraphNode::Directory(d) => {
                        merged.dir_index.insert(d.path.clone(), new_idx);
                    }
                    GraphNode::File(f) => {
                        merged.file_index.insert(f.path.clone(), new_idx);
                    }
                    _ => {}
                }
            }

            // Remap resolution indexes
            for (fqn, nodes) in &g.def_by_fqn {
                let remapped: Vec<_> = nodes
                    .iter()
                    .filter_map(|i| index_map.get(i).copied())
                    .collect();
                merged
                    .def_by_fqn
                    .entry(fqn.clone())
                    .or_default()
                    .extend(remapped);
            }
            for (name, nodes) in &g.def_by_name {
                let remapped: Vec<_> = nodes
                    .iter()
                    .filter_map(|i| index_map.get(i).copied())
                    .collect();
                merged
                    .def_by_name
                    .entry(name.clone())
                    .or_default()
                    .extend(remapped);
            }
            for (fp, nodes) in &g.defs_by_file {
                let remapped: Vec<_> = nodes
                    .iter()
                    .filter_map(|i| index_map.get(i).copied())
                    .collect();
                merged
                    .defs_by_file
                    .entry(fp.clone())
                    .or_default()
                    .extend(remapped);
            }
            for (fp, nodes) in &g.imports_by_file {
                let remapped: Vec<_> = nodes
                    .iter()
                    .filter_map(|i| index_map.get(i).copied())
                    .collect();
                merged
                    .imports_by_file
                    .entry(fp.clone())
                    .or_default()
                    .extend(remapped);
            }
            for (class_fqn, member_map) in &g.members {
                for (member_name, nodes) in member_map {
                    let remapped: Vec<_> = nodes
                        .iter()
                        .filter_map(|i| index_map.get(i).copied())
                        .collect();
                    merged
                        .members
                        .entry(class_fqn.clone())
                        .or_default()
                        .entry(member_name.clone())
                        .or_default()
                        .extend(remapped);
                }
            }
            merged
                .supers
                .extend(g.supers.iter().map(|(k, v)| (k.clone(), v.clone())));
            merged
                .ancestors
                .extend(g.ancestors.iter().map(|(k, v)| (k.clone(), v.clone())));

            for old_edge in g.graph.edge_indices() {
                let (src, tgt) = g.graph.edge_endpoints(old_edge).unwrap();
                let weight = g.graph[old_edge].clone();
                merged
                    .graph
                    .add_edge(index_map[&src], index_map[&tgt], weight);
            }
        }

        merged
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
        let source = std::fs::read(path).unwrap_or_else(|e| panic!("Failed to read {path}: {e}"));
        dispatch_language(language, vec![(path.to_string(), source)], "/")
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

        assert_eq!(result.graph.files().count(), 4);
        assert!(result.graph.directories().count() > 0);
        assert!(result.graph.edge_count() > 0);

        let def_to_def = result
            .graph
            .edges()
            .filter(|(_, _, e)| {
                e.relationship.source_node == NodeKind::Definition
                    && e.relationship.target_node == NodeKind::Definition
            })
            .count();
        assert!(
            def_to_def >= 4,
            "Expected at least 4 def-to-def edges, got {def_to_def}"
        );

        let file_to_def = result
            .graph
            .edges()
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

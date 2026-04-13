use code_graph_config::{Language, detect_language_from_extension};
use code_graph_types::CanonicalParser;
use ignore::WalkBuilder;
use parser_core::dsl::types::DslParser;
use parser_core::v2::langs::{
    csharp::CSharpDsl, java::JavaDsl, kotlin::KotlinDsl, python::PythonDsl,
};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::marker::PhantomData;
use std::path::Path;

use crate::linker::v2::{
    CodeGraph, GraphBuilder, GraphEdge, NoResolver, ReferenceResolver, ResolutionContext,
    RulesResolver,
};
use crate::v2::lang_rules::java::JavaRules;
use crate::v2::lang_rules::kotlin::KotlinRules;
use crate::v2::lang_rules::python::PythonRules;

/// Input to a language pipeline: file path + source bytes.
pub type FileInput = (String, Vec<u8>);

/// Trait for language-specific graph production.
///
/// Two strategies:
/// - **Generic**: `GenericPipeline<P, R>` for languages using the standard
///   `CanonicalParser → Resolver → GraphBuilder` flow.
/// - **Custom**: implement directly for languages that need full control
///   over parsing and linking (e.g. Ruby).
///
/// Each pipeline receives all files for its language at once (needed
/// for cross-file resolution) and produces a `CodeGraph`.
pub trait LanguagePipeline {
    fn process_files(
        files: Vec<FileInput>,
        root_path: &str,
    ) -> Result<CodeGraph, Vec<PipelineError>>;
}

/// Generic pipeline parameterized by parser `P` and resolver `R`.
///
/// - `P` produces `(CanonicalResult, P::Ast)` per file (parallel)
/// - `R` resolves references across all results + ASTs into edges (sync)
/// - `GraphBuilder` constructs the final graph with structural + resolved edges
pub struct GenericPipeline<P: CanonicalParser, R: ReferenceResolver<P::Ast>>(PhantomData<(P, R)>);

impl<P, R> LanguagePipeline for GenericPipeline<P, R>
where
    P: CanonicalParser + Default + Sync + Send,
    P::Ast: Send,
    R: ReferenceResolver<P::Ast>,
{
    fn process_files(
        files: Vec<FileInput>,
        root_path: &str,
    ) -> Result<CodeGraph, Vec<PipelineError>> {
        let parser = P::default();

        // Parse all files in parallel
        let parse_results: Vec<_> = files
            .par_iter()
            .map(|(path, source)| {
                parser.parse_file(source, path).map_err(|e| PipelineError {
                    file_path: path.clone(),
                    error: format!("Parse error: {e}"),
                })
            })
            .collect();

        let mut canonical_results = Vec::new();
        let mut asts: FxHashMap<String, P::Ast> = FxHashMap::default();
        let mut errors = Vec::new();

        for r in parse_results {
            match r {
                Ok((result, ast)) => {
                    asts.insert(result.file_path.clone(), ast);
                    canonical_results.push(result);
                }
                Err(err) => errors.push(err),
            }
        }

        if !errors.is_empty() && canonical_results.is_empty() {
            return Err(errors);
        }

        // Build resolution context — owns results + ASTs
        let ctx = ResolutionContext::build(canonical_results, asts, root_path.to_string());

        // Resolve references
        let resolved_edges = R::resolve(&ctx);

        // Build the graph with structural edges + resolved edges
        let mut builder = GraphBuilder::new(root_path.to_string());
        for result in &ctx.results {
            builder.add_result(result.clone());
        }

        let mut graph = builder.build();

        // Add resolved edges to the petgraph
        for edge in resolved_edges {
            use crate::linker::v2::EdgeSource;

            let src_node = match edge.source {
                EdgeSource::Definition(def_ref) => graph
                    .def_index
                    .get(&(def_ref.file_idx, def_ref.def_idx))
                    .copied(),
                EdgeSource::File(file_idx) => {
                    let file_path = &ctx.results[file_idx].file_path;
                    let relative: &str = file_path
                        .strip_prefix(root_path)
                        .map(|p: &str| p.strip_prefix('/').unwrap_or(p))
                        .unwrap_or(file_path);
                    graph.file_index.get(relative).copied()
                }
            };
            let tgt_node = graph
                .def_index
                .get(&(edge.target.file_idx, edge.target.def_idx))
                .copied();

            if let (Some(src), Some(tgt)) = (src_node, tgt_node) {
                graph.graph.add_edge(
                    src,
                    tgt,
                    GraphEdge {
                        relationship: edge.relationship,
                        source_definition_range: None,
                        target_definition_range: None,
                    },
                );
            }
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

register_v2_pipelines! {
    // Generic: DSL parser + SSA resolver
    Python  => GenericPipeline<DslParser<PythonDsl>, RulesResolver<PythonRules>>,
    Java    => GenericPipeline<DslParser<JavaDsl>, RulesResolver<JavaRules>>,
    Kotlin  => GenericPipeline<DslParser<KotlinDsl>, RulesResolver<KotlinRules>>,
    CSharp  => GenericPipeline<DslParser<CSharpDsl>, NoResolver>,

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
        let files_by_language = self.walk_and_group(root);

        // 2. Process each language through its pipeline
        let mut all_graphs: Vec<CodeGraph> = Vec::new();
        let mut all_errors: Vec<PipelineError> = Vec::new();
        let mut files_parsed = 0usize;
        let mut files_skipped = 0usize;

        for (language, files) in files_by_language {
            let file_count = files.len();

            match dispatch_language(language, files, &root_str) {
                Some(Ok(graph)) => {
                    files_parsed += file_count;
                    all_graphs.push(graph);
                }
                Some(Err(errors)) => {
                    files_skipped += file_count;
                    all_errors.extend(errors);
                }
                None => {
                    files_skipped += file_count;
                    all_errors.push(PipelineError {
                        file_path: String::new(),
                        error: format!("Language {language} not yet supported in v2 pipeline"),
                    });
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

            let path_str = path.to_string_lossy();
            if let Some(lang) = detect_language_from_extension(ext) {
                if lang
                    .exclude_extensions()
                    .iter()
                    .any(|excl| path_str.ends_with(excl))
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
                    .push((path_str.to_string(), source));
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

            // Remap def_index
            for (key, old_idx) in &g.def_index {
                if let Some(&new_idx) = index_map.get(old_idx) {
                    merged.def_index.insert(*key, new_idx);
                }
            }

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

use code_graph_config::{detect_language_from_extension, Language};
use code_graph_types::CanonicalResult;
use ignore::WalkBuilder;
use parser_core::v2::CanonicalParser;
use parser_core::v2::{
    csharp::CSharpCanonicalParser, java::JavaCanonicalParser, kotlin::KotlinCanonicalParser,
    python::PythonCanonicalParser,
};
use rayon::prelude::*;
use std::path::Path;

use crate::linker::v2::{GraphBuilder, GraphData};

macro_rules! register_v2_parsers {
    ($( $variant:ident => $parser:expr ),+ $(,)?) => {
        fn dispatch_parse(
            language: Language,
            source: &[u8],
            file_path: &str,
        ) -> Option<parser_core::Result<CanonicalResult>> {
            Some(match language {
                $(Language::$variant => $parser.parse_file(source, file_path),)+
                _ => return None,
            })
        }
    };
}

register_v2_parsers! {
    Python  => PythonCanonicalParser,
    Java    => JavaCanonicalParser,
    Kotlin  => KotlinCanonicalParser,
    CSharp  => CSharpCanonicalParser,
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
    pub graph: GraphData,
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

    /// Run the full pipeline on a directory.
    pub fn run(&self, root: &Path) -> PipelineResult {
        let root_str = root.to_string_lossy().to_string();

        // 1. Walk filesystem, collecting file paths + languages
        let file_entries = self.walk_files(root);

        // 2. Parse in parallel → Vec<Result<CanonicalResult, PipelineError>>
        let parse_results: Vec<_> = file_entries
            .par_iter()
            .map(|(path, language)| self.parse_file(path, *language))
            .collect();

        let mut results = Vec::new();
        let mut errors = Vec::new();
        for r in parse_results {
            match r {
                Ok(result) => results.push(result),
                Err(err) => errors.push(err),
            }
        }

        let files_parsed = results.len();
        let files_skipped = file_entries.len() - files_parsed;

        // 3. Build graph (sync — needs all results)
        let mut builder = GraphBuilder::new(root_str);

        let mut definitions_count = 0;
        let mut imports_count = 0;
        let mut references_count = 0;

        for result in results {
            definitions_count += result.definitions.len();
            imports_count += result.imports.len();
            references_count += result.references.len();
            builder.add_result(result);
        }

        let graph = builder.build();
        let edges_count = graph.edges.len();

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
            errors,
        }
    }

    fn walk_files(&self, root: &Path) -> Vec<(String, Language)> {
        let mut entries = Vec::new();

        let walker = WalkBuilder::new(root)
            .git_ignore(self.config.respect_gitignore)
            .hidden(true)
            .build();

        for entry in walker.flatten() {
            if !entry.file_type().is_some_and(|ft| ft.is_file()) {
                continue;
            }

            let path = entry.path();

            if let Ok(metadata) = path.metadata() {
                if metadata.len() > self.config.max_file_size {
                    continue;
                }
            }

            let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
                continue;
            };

            // Skip excluded extensions (e.g. min.js)
            let path_str = path.to_string_lossy();
            if let Some(lang) = detect_language_from_extension(ext) {
                if lang
                    .exclude_extensions()
                    .iter()
                    .any(|excl| path_str.ends_with(excl))
                {
                    continue;
                }
                entries.push((path_str.to_string(), lang));
            }
        }

        entries
    }

    fn parse_file(&self, path: &str, language: Language) -> Result<CanonicalResult, PipelineError> {
        let source = std::fs::read(path).map_err(|e| PipelineError {
            file_path: path.to_string(),
            error: format!("Failed to read file: {e}"),
        })?;

        dispatch_parse(language, &source, path)
            .unwrap_or_else(|| {
                Err(parser_core::Error::Parse(format!(
                    "Language {language} not yet supported in v2 pipeline"
                )))
            })
            .map_err(|e| PipelineError {
                file_path: path.to_string(),
                error: format!("Parse error: {e}"),
            })
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

    fn parse_fixture_file(path: &str, language: Language) -> CanonicalResult {
        let source = std::fs::read(path).unwrap_or_else(|e| panic!("Failed to read {path}: {e}"));
        dispatch_parse(language, &source, path)
            .unwrap_or_else(|| panic!("Language {language} not supported"))
            .unwrap_or_else(|e| panic!("Failed to parse {path}: {e}"))
    }

    // ── Python fixture ──────────────────────────────────────────────

    #[test]
    fn python_definitions_fixture() {
        let path = fixture_path("python/fixtures/definitions.py");
        let result = parse_fixture_file(&path, Language::Python);

        assert!(
            result.definitions.len() >= 10,
            "Expected at least 10 definitions, got {}",
            result.definitions.len()
        );

        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"simple_function"));
        assert!(names.contains(&"module_lambda"));
        assert!(names.contains(&"SimpleClass"));
        assert!(names.contains(&"decorated_function"));

        let class_defs: Vec<_> = result
            .definitions
            .iter()
            .filter(|d| d.kind == DefKind::Class)
            .collect();
        assert!(!class_defs.is_empty(), "Should find at least one class");

        let method_defs: Vec<_> = result
            .definitions
            .iter()
            .filter(|d| d.kind == DefKind::Method)
            .collect();
        assert!(!method_defs.is_empty(), "Should find at least one method");
    }

    // ── Java fixture ────────────────────────────────────────────────

    #[test]
    fn java_comprehensive_fixture() {
        let path = fixture_path("java/fixtures/ComprehensiveJavaDefinitions.java");
        let result = parse_fixture_file(&path, Language::Java);

        assert!(
            result.definitions.len() >= 5,
            "Expected at least 5 definitions, got {}",
            result.definitions.len()
        );

        let kinds: Vec<DefKind> = result.definitions.iter().map(|d| d.kind).collect();
        assert!(kinds.contains(&DefKind::Class), "Should have a class");
        assert!(kinds.contains(&DefKind::Method), "Should have a method");
    }

    // ── Kotlin fixture ──────────────────────────────────────────────

    #[test]
    fn kotlin_comprehensive_fixture() {
        let path = fixture_path("kotlin/fixtures/ComprehensiveKotlinDefinitions.kt");
        let result = parse_fixture_file(&path, Language::Kotlin);

        assert!(
            result.definitions.len() >= 5,
            "Expected at least 5 definitions, got {}",
            result.definitions.len()
        );

        let kinds: Vec<DefKind> = result.definitions.iter().map(|d| d.kind).collect();
        assert!(kinds.contains(&DefKind::Class), "Should have a class");
        assert!(kinds.contains(&DefKind::Function), "Should have a function");
    }

    // ── C# fixture ──────────────────────────────────────────────────

    #[test]
    fn csharp_comprehensive_fixture() {
        let path = fixture_path("csharp/fixtures/ComprehensiveCSharp.cs");
        let result = parse_fixture_file(&path, Language::CSharp);

        assert!(
            result.definitions.len() >= 5,
            "Expected at least 5 definitions, got {}",
            result.definitions.len()
        );

        let kinds: Vec<DefKind> = result.definitions.iter().map(|d| d.kind).collect();
        assert!(kinds.contains(&DefKind::Class), "Should have a class");
    }

    // ── Full pipeline e2e ───────────────────────────────────────────

    #[test]
    fn full_pipeline_on_fixture_directory() {
        // Create a temp directory with fixture files from all 4 languages
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

        // Should have parsed all 4 files
        assert_eq!(result.stats.files_parsed, 4, "Should parse 4 files");
        assert_eq!(result.errors.len(), 0, "Should have no errors");

        // Should have definitions from all languages
        assert!(
            result.stats.definitions_count >= 8,
            "Expected at least 8 definitions, got {}",
            result.stats.definitions_count
        );

        // Graph should have files, directories, and edges
        assert_eq!(result.graph.files.len(), 4);
        assert!(!result.graph.directories.is_empty());
        assert!(!result.graph.edges.is_empty());

        // Should have containment edges (definition → definition)
        let def_to_def = result
            .graph
            .edges
            .iter()
            .filter(|e| {
                e.relationship.source_node == NodeKind::Definition
                    && e.relationship.target_node == NodeKind::Definition
            })
            .count();
        assert!(
            def_to_def >= 4,
            "Expected at least 4 def-to-def edges, got {def_to_def}"
        );

        // Should have file→definition edges
        let file_to_def = result
            .graph
            .edges
            .iter()
            .filter(|e| {
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

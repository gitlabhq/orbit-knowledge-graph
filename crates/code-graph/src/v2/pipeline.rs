use code_graph_types::{CanonicalResult, Language};
use ignore::WalkBuilder;
use rayon::prelude::*;
use std::path::Path;
use strum::IntoEnumIterator;

use crate::linker::v2::{GraphBuilder, GraphData};
use parser_core::v2::{
    java::JavaCanonicalParser, kotlin::KotlinCanonicalParser, python::PythonCanonicalParser,
};

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
        let supported_extensions: std::collections::HashMap<&str, Language> = Language::iter()
            .flat_map(|lang| lang.file_extensions().iter().map(move |ext| (*ext, lang)))
            .collect();

        let excluded_extensions: Vec<&str> = Language::iter()
            .flat_map(|lang| lang.exclude_extensions().iter().copied())
            .collect();

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

            // Check file size
            if let Ok(metadata) = path.metadata() {
                if metadata.len() > self.config.max_file_size {
                    continue;
                }
            }

            // Check extension
            let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
                continue;
            };

            // Skip excluded extensions (e.g. min.js)
            let path_str = path.to_string_lossy();
            if excluded_extensions
                .iter()
                .any(|excl| path_str.ends_with(excl))
            {
                continue;
            }

            if let Some(&language) = supported_extensions.get(ext) {
                entries.push((path.to_string_lossy().to_string(), language));
            }
        }

        entries
    }

    fn parse_file(&self, path: &str, language: Language) -> Result<CanonicalResult, PipelineError> {
        let source = std::fs::read(path).map_err(|e| PipelineError {
            file_path: path.to_string(),
            error: format!("Failed to read file: {e}"),
        })?;

        let parser: &dyn parser_core::v2::CanonicalParser = match language {
            Language::Python => &PythonCanonicalParser,
            Language::Java => &JavaCanonicalParser,
            Language::Kotlin => &KotlinCanonicalParser,
            _ => {
                return Err(PipelineError {
                    file_path: path.to_string(),
                    error: format!("Language {language} not yet supported in v2 pipeline"),
                });
            }
        };

        parser.parse_file(&source, path).map_err(|e| PipelineError {
            file_path: path.to_string(),
            error: format!("Parse error: {e}"),
        })
    }
}

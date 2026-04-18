use crate::legacy::linker::analysis::types::GraphData;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::Duration;

use crate::legacy::linker::graph::RelationshipType;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileTypeStats {
    pub processed: usize,
    pub skipped: usize,
    pub errored: usize,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LanguageStats {
    pub file_count: usize,
    pub definition_count: usize,
    pub definition_types: HashMap<String, usize>,
    pub total_bytes: u64,
}

pub fn finalize_project_statistics(
    project_name: String,
    project_path: String,
    duration: Duration,
    graph_data: &GraphData,
) -> ProjectStatistics {
    let mut language_map: HashMap<String, (usize, usize, HashMap<String, usize>)> = HashMap::new();

    let file_path_to_language: HashMap<&str, &str> = graph_data
        .file_nodes
        .iter()
        .map(|f| (f.path.as_str(), f.language.as_str()))
        .collect();

    for file_node in &graph_data.file_nodes {
        let entry =
            language_map
                .entry(file_node.language.clone())
                .or_insert((0, 0, HashMap::new()));
        entry.0 += 1; // file_count
    }

    for def_node in &graph_data.definition_nodes {
        if let Some(&language) = file_path_to_language.get(def_node.file_path.as_str()) {
            let entry = language_map
                .entry(language.to_string())
                .or_insert((0, 0, HashMap::new()));
            entry.1 += 1;
            *entry
                .2
                .entry(def_node.definition_type.as_str().to_string())
                .or_insert(0) += 1;
        }
    }

    let language_statistics: Vec<LanguageStatistics> = language_map
        .into_iter()
        .map(
            |(language, (file_count, definitions_count, definition_type_counts))| {
                LanguageStatistics {
                    language,
                    file_count,
                    definitions_count,
                    definition_type_counts,
                }
            },
        )
        .collect();

    ProjectStatistics {
        project_name,
        project_path,
        languages: language_statistics,
        indexing_duration_seconds: duration.as_secs_f64(),
        total_files: graph_data.file_nodes.len(),
        total_definitions: graph_data.definition_nodes.len(),
        total_imported_symbols: graph_data.imported_symbol_nodes.len(),
        total_definition_relationships: graph_data
            .relationships
            .iter()
            .filter(|r| r.relationship_type == RelationshipType::Calls)
            .count(),
        total_imported_symbol_relationships: graph_data
            .relationships
            .iter()
            .filter(|r| r.relationship_type == RelationshipType::ImportedSymbolToImportedSymbol)
            .count(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticsMetadata {
    pub gkg_version: String,
    pub timestamp: DateTime<Utc>,
    pub workspace_path: String,
    pub indexing_duration_seconds: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanguageStatistics {
    pub language: String,
    pub file_count: usize,
    pub definitions_count: usize,
    pub definition_type_counts: HashMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectStatistics {
    pub project_name: String,
    pub project_path: String,
    pub total_files: usize,
    pub total_definitions: usize,
    pub total_imported_symbols: usize,
    pub total_definition_relationships: usize,
    pub total_imported_symbol_relationships: usize,

    pub languages: Vec<LanguageStatistics>,
    pub indexing_duration_seconds: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceStatistics {
    pub metadata: StatisticsMetadata,
    pub total_projects: usize,
    pub total_files: usize,
    pub total_definitions: usize,
    pub total_imported_symbols: usize,
    pub total_definition_relationships: usize,
    pub total_imported_symbol_relationships: usize,

    pub total_languages: HashMap<String, LanguageSummary>,
    pub projects: Vec<ProjectStatistics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanguageSummary {
    pub file_count: usize,
    pub definitions_count: usize,
    pub definition_type_counts: HashMap<String, usize>,
}

impl WorkspaceStatistics {
    pub fn new(workspace_path: String, indexing_duration_seconds: f64) -> Self {
        Self {
            metadata: StatisticsMetadata {
                gkg_version: env!("CARGO_PKG_VERSION").to_string(),
                timestamp: Utc::now(),
                workspace_path,
                indexing_duration_seconds,
            },
            total_projects: 0,
            total_files: 0,
            total_definitions: 0,
            total_imported_symbols: 0,
            total_definition_relationships: 0,
            total_imported_symbol_relationships: 0,

            total_languages: HashMap::new(),
            projects: Vec::new(),
        }
    }

    pub fn add_project(&mut self, project_stats: ProjectStatistics) {
        self.total_files += project_stats.total_files;
        self.total_definitions += project_stats.total_definitions;
        self.total_imported_symbols += project_stats.total_imported_symbols;
        self.total_definition_relationships += project_stats.total_definition_relationships;
        self.total_imported_symbol_relationships +=
            project_stats.total_imported_symbol_relationships;

        for lang_stats in &project_stats.languages {
            let lang_summary = self
                .total_languages
                .entry(lang_stats.language.clone())
                .or_insert_with(|| LanguageSummary {
                    file_count: 0,
                    definitions_count: 0,
                    definition_type_counts: HashMap::new(),
                });

            lang_summary.file_count += lang_stats.file_count;
            lang_summary.definitions_count += lang_stats.definitions_count;

            for (def_type, count) in &lang_stats.definition_type_counts {
                *lang_summary
                    .definition_type_counts
                    .entry(def_type.clone())
                    .or_insert(0) += count;
            }
        }

        self.projects.push(project_stats);
        self.total_projects = self.projects.len();
    }

    pub fn export_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        fs::write(path, json)?;
        Ok(())
    }
}

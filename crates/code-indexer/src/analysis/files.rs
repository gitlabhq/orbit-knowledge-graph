use super::{DirectoryNode, FileNode};
use crate::analysis::types::ConsolidatedRelationship;

use crate::graph::RelationshipType;
use crate::parsing::processor::FileProcessingResult;
use internment::ArcIntern;
use std::{collections::HashSet, path::Path};

/// Handles filesystem-related analysis operations
pub struct FileSystemAnalyzer {
    repository_name: String,
    repository_path: String,
}

impl FileSystemAnalyzer {
    /// Create a new filesystem analyzer
    pub fn new(repository_name: String, repository_path: String) -> Self {
        Self {
            repository_name,
            repository_path,
        }
    }

    /// Create directory hierarchy for a file path
    pub fn create_directory_hierarchy(
        &self,
        file_path: &str,
        directory_nodes: &mut Vec<DirectoryNode>,
        relationships: &mut Vec<ConsolidatedRelationship>,
        created_directories: &mut HashSet<String>,
        created_relationships: &mut HashSet<(String, String)>,
    ) {
        // Convert absolute path to relative path by stripping repository path prefix
        let relative_file_path = self.get_relative_path(file_path);
        let path = Path::new(&relative_file_path);
        let mut current_path = String::new();
        let mut parent_path: Option<String> = None;

        // Build directory hierarchy from root to file's parent directory
        for component in path.parent().unwrap_or(Path::new("")).components() {
            if let std::path::Component::Normal(name) = component {
                let dir_name = name.to_string_lossy().to_string();

                if current_path.is_empty() {
                    current_path = dir_name.clone();
                } else {
                    // Use Path joining and normalize for consistent storage
                    current_path = Self::normalize_path(
                        &Path::new(&current_path).join(&dir_name).to_string_lossy(),
                    );
                }

                // Create directory node if not already created
                if !created_directories.contains(&current_path) {
                    // Always construct absolute path by joining repository path with relative path (cross-platform)
                    let absolute_path = Path::new(&self.repository_path)
                        .join(&current_path)
                        .to_string_lossy()
                        .to_string();

                    log::debug!(
                        "Creating directory node: '{current_path}' (from file: '{file_path}')"
                    );
                    directory_nodes.push(DirectoryNode {
                        path: current_path.clone(),
                        absolute_path,
                        repository_name: self.repository_name.clone(),
                        name: dir_name,
                    });
                    created_directories.insert(current_path.clone());
                } else {
                    log::debug!(
                        "Directory already exists: '{current_path}' (from file: '{file_path}')"
                    );
                }

                // Create directory-to-directory relationship if it doesn't already exist
                if let Some(ref parent) = parent_path {
                    let rel_tuple = (parent.clone(), current_path.clone());
                    if !created_relationships.contains(&rel_tuple) {
                        let mut rel = ConsolidatedRelationship::dir_to_dir(
                            ArcIntern::new(parent.clone()),
                            ArcIntern::new(current_path.clone()),
                        );
                        rel.relationship_type = RelationshipType::DirContainsDir;
                        relationships.push(rel);
                        created_relationships.insert(rel_tuple);
                    }
                }

                parent_path = Some(current_path.clone());
            }
        }
    }

    /// Convert absolute path to relative path by stripping repository path prefix
    pub fn get_relative_path(&self, file_path: &str) -> String {
        let file_path_buf = Path::new(file_path);
        let repo_path_buf = Path::new(&self.repository_path);

        // Try to strip the repository path prefix using Path methods (cross-platform)
        if let Ok(relative_path) = file_path_buf.strip_prefix(repo_path_buf) {
            // Convert to string using forward slashes for consistent storage
            Self::normalize_path(&relative_path.to_string_lossy())
        } else {
            // File path doesn't start with repository path - treat as already relative
            Self::normalize_path(file_path)
        }
    }

    /// Get the parent directory path for a file (using relative path)
    pub fn get_parent_directory(&self, file_path: &str) -> Option<String> {
        let relative_file_path = self.get_relative_path(file_path);
        Path::new(&relative_file_path)
            .parent()
            .and_then(|p| p.to_str())
            .filter(|s| !s.is_empty())
            .map(Self::normalize_path)
    }

    /// Create a file node from a file processing result
    pub fn create_file_node(&self, file_result: &FileProcessingResult) -> FileNode {
        // Convert to relative path for storage
        let relative_path = self.get_relative_path(&file_result.file_path);

        // Construct proper absolute path using cross-platform path joining
        let absolute_path = Path::new(&self.repository_path)
            .join(&relative_path)
            .to_string_lossy()
            .to_string();

        // Extract file extension from the relative path
        let extension = Path::new(&relative_path)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("unknown")
            .to_string();

        // Extract file name from the relative path
        let name = Path::new(&relative_path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown")
            .to_string();

        FileNode {
            path: relative_path,
            absolute_path,
            language: format!("{:?}", file_result.language),
            repository_name: self.repository_name.clone(),
            extension,
            name,
        }
    }

    /// Extract directory name from a path
    pub fn extract_directory_name(path: &str) -> String {
        Path::new(path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown")
            .to_string()
    }

    /// Extract file name from a path
    pub fn extract_file_name(path: &str) -> String {
        Path::new(path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown")
            .to_string()
    }

    /// Check if a path is a valid file path
    pub fn is_valid_file_path(path: &str) -> bool {
        !path.is_empty() && Path::new(path).is_file()
    }

    /// Check if a path is a valid directory path
    pub fn is_valid_directory_path(path: &str) -> bool {
        !path.is_empty() && Path::new(path).is_dir()
    }

    /// Normalize path separators for consistent storage
    pub fn normalize_path(path: &str) -> String {
        path.replace('\\', "/")
    }

    /// Calculate the depth of a path (number of directory separators)
    pub fn calculate_path_depth(path: &str) -> usize {
        path.matches('/').count()
    }
}

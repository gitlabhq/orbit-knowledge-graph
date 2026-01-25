//! File loading and discovery for repository indexing.
//!
//! This module provides streaming file discovery with gitignore support,
//! using the `ignore` crate for parallel directory traversal.

mod io;

pub use io::{ProcessingError, read_text_file};

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::indexer::IndexingConfig;
use futures::StreamExt;
use futures::stream::BoxStream;
use ignore::WalkBuilder;
use ignore::gitignore::GitignoreBuilder;
use parser_core::parser::get_supported_extensions;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

// ============================================================================
// FileInfo
// ============================================================================

/// File information with path accessors.
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: PathBuf,
}

impl FileInfo {
    pub fn from_path(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn extension(&self) -> &str {
        self.path
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("")
    }

    pub fn name(&self) -> &str {
        self.path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("")
    }
}

pub trait FileSource {
    type Error: std::fmt::Display + Send + Sync + 'static;

    /// Returns a stream of files to be indexed.
    fn stream_files(
        self,
        config: &IndexingConfig,
    ) -> BoxStream<'static, Result<FileInfo, Self::Error>>;
}

// ============================================================================
// DirectoryFileSource
// ============================================================================

/// A file source that walks a directory with gitignore support.
///
/// Discovers files in a directory tree while respecting .gitignore rules,
/// filtering by supported extensions, and streaming results as they're found.
#[derive(Debug, Clone)]
pub struct DirectoryFileSource {
    path: String,
    supported_extensions: HashSet<String>,
    exclusion_patterns: Vec<String>,
}

impl DirectoryFileSource {
    pub fn new(path: String) -> Self {
        Self {
            path,
            supported_extensions: get_supported_extensions()
                .iter()
                .map(|ext| ext.to_string())
                .collect(),
            exclusion_patterns: Vec::new(),
        }
    }

    pub fn with_exclusions(path: String, exclusion_patterns: Vec<String>) -> Self {
        Self {
            path,
            supported_extensions: get_supported_extensions()
                .iter()
                .map(|ext| ext.to_string())
                .collect(),
            exclusion_patterns,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// Stream files from the directory, applying filters as they're discovered.
    fn stream_directory(
        path: String,
        respect_gitignore: bool,
        exclusion_patterns: Vec<String>,
    ) -> ReceiverStream<Result<FileInfo, std::io::Error>> {
        let (tx, rx) = mpsc::channel(256);

        tokio::task::spawn_blocking(move || {
            // Build custom gitignore matcher from exclusion patterns
            let custom_ignores = if !exclusion_patterns.is_empty() {
                let mut builder = GitignoreBuilder::new(&path);
                for pattern in &exclusion_patterns {
                    let _ = builder.add_line(None, pattern);
                }
                builder.build().ok()
            } else {
                None
            };

            let include_ignored = !respect_gitignore;
            let walker = WalkBuilder::new(&path)
                .standard_filters(respect_gitignore)
                .hidden(false)
                .require_git(false)
                .filter_entry(move |entry| {
                    // Skip .git directories
                    if entry.file_name() == ".git" && !include_ignored {
                        return false;
                    }

                    // Skip nested git repositories
                    if entry.file_type().is_some_and(|ft| ft.is_dir())
                        && entry.depth() > 0
                        && entry.path().join(".git").is_dir()
                    {
                        return false;
                    }

                    // Apply custom exclusion patterns
                    if let Some(ref ignores) = custom_ignores {
                        let entry_path = entry.path();
                        let relative = entry_path.strip_prefix(&path).unwrap_or(entry_path);
                        let is_dir = entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false);
                        if let ignore::Match::Ignore(_) = ignores.matched(relative, is_dir) {
                            return false;
                        }
                    }

                    true
                })
                .build_parallel();

            walker.run(|| {
                let tx = tx.clone();
                Box::new(move |result| match result {
                    Ok(entry) => {
                        if entry.file_type().map(|ft| ft.is_file()).unwrap_or(false)
                            && tx
                                .blocking_send(Ok(FileInfo::from_path(entry.into_path())))
                                .is_err()
                        {
                            return ignore::WalkState::Quit;
                        }
                        ignore::WalkState::Continue
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(std::io::Error::other(e.to_string())));
                        ignore::WalkState::Continue
                    }
                })
            });
        });

        ReceiverStream::new(rx)
    }
}

impl FileSource for DirectoryFileSource {
    type Error = std::io::Error;

    fn stream_files(
        self,
        config: &IndexingConfig,
    ) -> BoxStream<'static, Result<FileInfo, Self::Error>> {
        let supported = self.supported_extensions;

        Self::stream_directory(self.path, config.respect_gitignore, self.exclusion_patterns)
            .filter_map(move |result| {
                let supported = supported.clone();
                async move {
                    match result {
                        Ok(info) if supported.contains(info.extension()) => Some(Ok(info)),
                        Ok(_) => None, // Skip unsupported extensions
                        Err(e) => Some(Err(e)),
                    }
                }
            })
            .boxed()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_git_repo(path: &Path) -> std::io::Result<()> {
        let git_dir = path.join(".git");
        fs::create_dir_all(&git_dir)?;
        fs::write(
            git_dir.join("config"),
            "[core]\n\trepositoryformatversion = 0",
        )
    }

    fn create_file(path: &Path, content: &str) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, content)
    }

    fn setup_test_repo(files: &[&str]) -> (TempDir, String) {
        let temp = TempDir::new().unwrap();
        let path_str = temp.path().to_string_lossy().to_string();

        std::process::Command::new("git")
            .args(["init"])
            .current_dir(temp.path())
            .output()
            .unwrap();

        for file in files {
            create_file(&temp.path().join(file), &format!("# {file}")).unwrap();
        }

        (temp, path_str)
    }

    #[tokio::test]
    async fn test_streams_supported_files() {
        let (_temp, path) = setup_test_repo(&["src/main.py", "src/utils.py", "readme.txt"]);

        let source = DirectoryFileSource::new(path);
        let config = IndexingConfig::default();

        let files: Vec<_> = source
            .stream_files(&config)
            .filter_map(|r| async { r.ok() })
            .collect()
            .await;

        let names: Vec<_> = files.iter().map(|f| f.name()).collect();
        assert!(names.contains(&"main.py"));
        assert!(names.contains(&"utils.py"));
        // readme.txt should be filtered out (not a supported extension)
    }

    #[tokio::test]
    async fn test_exclusion_patterns() {
        let (_temp, path) = setup_test_repo(&["src/main.py", "tests/test.py", "src/agent.py"]);

        let source =
            DirectoryFileSource::with_exclusions(path, vec!["tests/**".into(), "**/agent*".into()]);
        let config = IndexingConfig::default();

        let files: Vec<_> = source
            .stream_files(&config)
            .filter_map(|r| async { r.ok() })
            .collect()
            .await;

        let names: Vec<_> = files.iter().map(|f| f.name()).collect();
        assert!(names.contains(&"main.py"));
        assert!(!names.contains(&"test.py"));
        assert!(!names.contains(&"agent.py"));
    }

    #[tokio::test]
    async fn test_skips_nested_git_repos() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();

        create_git_repo(root).unwrap();
        create_file(&root.join("root.rs"), "// root").unwrap();

        let nested = root.join("nested");
        fs::create_dir_all(&nested).unwrap();
        create_git_repo(&nested).unwrap();
        create_file(&nested.join("nested.rs"), "// nested").unwrap();

        let source = DirectoryFileSource::new(root.to_string_lossy().to_string());
        let config = IndexingConfig::default();

        let files: Vec<_> = source
            .stream_files(&config)
            .filter_map(|r| async { r.ok() })
            .collect()
            .await;

        let names: Vec<_> = files.iter().map(|f| f.name()).collect();
        assert!(names.contains(&"root.rs"));
        assert!(!names.contains(&"nested.rs"));
    }
}

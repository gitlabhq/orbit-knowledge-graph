//! Local filesystem content resolver for virtual columns.
//!
//! Replaces Gitaly in local mode: reads file content directly from disk
//! using the repo root paths passed at construction time.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use gkg_utils::arrow::ColumnValue;
use query_engine::pipeline::PipelineError;
use query_engine::shared::content::{ColumnResolver, PropertyRow, ResolverContext};

/// Resolves file content by reading from the local filesystem.
///
/// Accepts a list of repo roots. For each row, extracts `path` (or
/// `file_path`) and tries each root until the file is found. Supports
/// `start_byte`/`end_byte` slicing for Definition entities.
pub struct LocalContentService {
    roots: Vec<PathBuf>,
}

impl LocalContentService {
    pub fn new(roots: Vec<PathBuf>) -> Self {
        Self { roots }
    }
}

#[async_trait]
impl ColumnResolver for LocalContentService {
    async fn resolve_batch(
        &self,
        _lookup: &str,
        rows: &[&PropertyRow],
        _ctx: &ResolverContext,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError> {
        Ok(rows
            .iter()
            .map(|props| resolve_row(props, &self.roots))
            .collect())
    }
}

/// Resolve a single row's file content from disk.
fn resolve_row(props: &PropertyRow, roots: &[PathBuf]) -> Option<ColumnValue> {
    let file_path: String = props
        .get("file_path")
        .or_else(|| props.get("path"))
        .and_then(|v| v.coerce())?;

    let content = read_from_roots(roots, &file_path)?;

    let start_byte: Option<i64> = props.get("start_byte").and_then(|v| v.coerce());
    let end_byte: Option<i64> = props.get("end_byte").and_then(|v| v.coerce());

    Some(ColumnValue::String(
        slice_content(&content, start_byte, end_byte).to_string(),
    ))
}

/// Try each root until we find the file. Returns `None` if the path is
/// absolute, escapes the root via `..`, or the file can't be read as UTF-8.
fn read_from_roots(roots: &[PathBuf], relative_path: &str) -> Option<String> {
    let rel = Path::new(relative_path);
    if rel.is_absolute() {
        return None;
    }

    for root in roots {
        let full = root.join(rel);

        // Canonicalize to resolve any `..` segments, then verify the
        // resolved path is still inside the root.
        let Ok(canonical) = full.canonicalize() else {
            continue;
        };
        let Ok(canonical_root) = root.canonicalize() else {
            continue;
        };
        if !canonical.starts_with(&canonical_root) {
            continue;
        }

        if let Ok(content) = std::fs::read_to_string(&canonical) {
            return Some(content);
        }
    }
    None
}

/// Return the byte-range slice of `content`, or the full string when no
/// range is specified.
fn slice_content(content: &str, start_byte: Option<i64>, end_byte: Option<i64>) -> &str {
    match (start_byte, end_byte) {
        (Some(s), Some(e)) if s >= 0 && e >= s => {
            let s = s as usize;
            let e = (e as usize).min(content.len());
            if s >= content.len() {
                return "";
            }
            content.get(s..e).unwrap_or("")
        }
        _ => content,
    }
}

/// Build a [`LocalContentService`] and wrap it in a
/// [`ColumnResolverRegistry`] under the `"gitaly"` service name
/// (matching the ontology's `virtual.service` field).
pub fn local_resolver_registry(
    roots: Vec<PathBuf>,
) -> query_engine::shared::content::ColumnResolverRegistry {
    let mut registry = query_engine::shared::content::ColumnResolverRegistry::new();
    registry.register(
        "gitaly",
        std::sync::Arc::new(LocalContentService::new(roots)),
    );
    registry
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn slice_full_when_no_range() {
        assert_eq!(slice_content("hello world", None, None), "hello world");
    }

    #[test]
    fn slice_byte_range() {
        assert_eq!(slice_content("hello world", Some(6), Some(11)), "world");
    }

    #[test]
    fn slice_clamps_end() {
        assert_eq!(slice_content("hi", Some(0), Some(999)), "hi");
    }

    #[test]
    fn slice_empty_when_start_past_end_of_content() {
        assert_eq!(slice_content("hi", Some(100), Some(200)), "");
    }

    #[test]
    fn slice_empty_on_utf8_boundary() {
        assert_eq!(slice_content("é", Some(0), Some(1)), "");
    }

    #[test]
    fn resolve_row_from_path() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src/lib.rs"), "fn main() {}").unwrap();

        let mut props = HashMap::new();
        props.insert("path".into(), ColumnValue::String("src/lib.rs".into()));

        let result = resolve_row(&props, &[dir.path().to_path_buf()]);
        assert_eq!(result, Some(ColumnValue::String("fn main() {}".into())));
    }

    #[test]
    fn resolve_row_with_byte_range() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src/lib.rs"), "fn main() {}").unwrap();

        let mut props = HashMap::new();
        props.insert("file_path".into(), ColumnValue::String("src/lib.rs".into()));
        props.insert("start_byte".into(), ColumnValue::Int64(3));
        props.insert("end_byte".into(), ColumnValue::Int64(7));

        let result = resolve_row(&props, &[dir.path().to_path_buf()]);
        assert_eq!(result, Some(ColumnValue::String("main".into())));
    }

    #[test]
    fn resolve_row_missing_file() {
        let props = HashMap::from([("path".into(), ColumnValue::String("no/such/file.rs".into()))]);
        let result = resolve_row(&props, &[PathBuf::from("/nonexistent")]);
        assert_eq!(result, None);
    }

    #[test]
    fn resolve_row_missing_path_prop() {
        let props = HashMap::new();
        let result = resolve_row(&props, &[PathBuf::from("/tmp")]);
        assert_eq!(result, None);
    }

    #[test]
    fn rejects_absolute_path() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("secret.txt"), "oh no").unwrap();

        let abs = dir.path().join("secret.txt");
        let mut props = HashMap::new();
        props.insert(
            "path".into(),
            ColumnValue::String(abs.to_string_lossy().into()),
        );

        let result = resolve_row(&props, &[dir.path().to_path_buf()]);
        assert_eq!(result, None);
    }

    #[test]
    fn rejects_path_traversal() {
        let dir = tempfile::TempDir::new().unwrap();
        let inner = dir.path().join("repo");
        std::fs::create_dir_all(&inner).unwrap();
        std::fs::write(dir.path().join("secret.txt"), "oh no").unwrap();

        let mut props = HashMap::new();
        props.insert("path".into(), ColumnValue::String("../secret.txt".into()));

        let result = resolve_row(&props, std::slice::from_ref(&inner));
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn resolve_batch_integration() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.rs"), "aaa").unwrap();
        std::fs::write(dir.path().join("b.rs"), "bbb").unwrap();

        let svc = LocalContentService::new(vec![dir.path().to_path_buf()]);
        let row_a = HashMap::from([("path".into(), ColumnValue::String("a.rs".into()))]);
        let row_b = HashMap::from([("path".into(), ColumnValue::String("b.rs".into()))]);
        let rows: Vec<&PropertyRow> = vec![&row_a, &row_b];

        let results = svc
            .resolve_batch("blob_content", &rows, &ResolverContext::default())
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], Some(ColumnValue::String("aaa".into())));
        assert_eq!(results[1], Some(ColumnValue::String("bbb".into())));
    }
}

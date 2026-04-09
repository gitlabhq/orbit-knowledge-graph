//! Local filesystem content resolver for virtual columns.
//!
//! Replaces Gitaly in local mode: reads file content directly from disk
//! using the repo root paths passed at construction time.

use std::collections::HashMap;
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
        // Cache file reads so multiple rows referencing the same file
        // (e.g. several Definitions in one source file) hit disk once.
        let mut cache: HashMap<String, Option<String>> = HashMap::new();

        Ok(rows
            .iter()
            .map(|props| {
                let file_path: String = props
                    .get("file_path")
                    .or_else(|| props.get("path"))
                    .and_then(|v| v.coerce())?;

                let content = cache
                    .entry(file_path.clone())
                    .or_insert_with(|| read_from_roots(&self.roots, &file_path))
                    .as_ref()?;

                let start_byte: Option<i64> = props.get("start_byte").and_then(|v| v.coerce());
                let end_byte: Option<i64> = props.get("end_byte").and_then(|v| v.coerce());

                Some(ColumnValue::String(
                    slice_content(content, start_byte, end_byte).to_string(),
                ))
            })
            .collect())
    }
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
    use super::*;

    fn svc(roots: Vec<PathBuf>) -> LocalContentService {
        LocalContentService::new(roots)
    }

    async fn resolve_one(svc: &LocalContentService, props: &PropertyRow) -> Option<ColumnValue> {
        let rows: Vec<&PropertyRow> = vec![props];
        svc.resolve_batch("blob_content", &rows, &ResolverContext::default())
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }

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

    #[tokio::test]
    async fn resolves_file_from_path() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src/lib.rs"), "fn main() {}").unwrap();

        let s = svc(vec![dir.path().to_path_buf()]);
        let props = HashMap::from([("path".into(), ColumnValue::String("src/lib.rs".into()))]);

        assert_eq!(
            resolve_one(&s, &props).await,
            Some(ColumnValue::String("fn main() {}".into()))
        );
    }

    #[tokio::test]
    async fn resolves_with_byte_range() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src/lib.rs"), "fn main() {}").unwrap();

        let s = svc(vec![dir.path().to_path_buf()]);
        let props = HashMap::from([
            ("file_path".into(), ColumnValue::String("src/lib.rs".into())),
            ("start_byte".into(), ColumnValue::Int64(3)),
            ("end_byte".into(), ColumnValue::Int64(7)),
        ]);

        assert_eq!(
            resolve_one(&s, &props).await,
            Some(ColumnValue::String("main".into()))
        );
    }

    #[tokio::test]
    async fn returns_none_for_missing_file() {
        let s = svc(vec![PathBuf::from("/nonexistent")]);
        let props = HashMap::from([("path".into(), ColumnValue::String("no/such/file.rs".into()))]);
        assert_eq!(resolve_one(&s, &props).await, None);
    }

    #[tokio::test]
    async fn returns_none_for_missing_path_prop() {
        let s = svc(vec![PathBuf::from("/tmp")]);
        let props = HashMap::new();
        assert_eq!(resolve_one(&s, &props).await, None);
    }

    #[tokio::test]
    async fn rejects_absolute_path() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("secret.txt"), "oh no").unwrap();

        let s = svc(vec![dir.path().to_path_buf()]);
        let abs = dir.path().join("secret.txt");
        let props = HashMap::from([(
            "path".into(),
            ColumnValue::String(abs.to_string_lossy().into()),
        )]);

        assert_eq!(resolve_one(&s, &props).await, None);
    }

    #[tokio::test]
    async fn rejects_path_traversal() {
        let dir = tempfile::TempDir::new().unwrap();
        let inner = dir.path().join("repo");
        std::fs::create_dir_all(&inner).unwrap();
        std::fs::write(dir.path().join("secret.txt"), "oh no").unwrap();

        let s = svc(vec![inner]);
        let props = HashMap::from([("path".into(), ColumnValue::String("../secret.txt".into()))]);

        assert_eq!(resolve_one(&s, &props).await, None);
    }

    #[tokio::test]
    async fn deduplicates_file_reads() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("f.rs"), "fn foo() {}").unwrap();

        let s = svc(vec![dir.path().to_path_buf()]);
        let row_a = HashMap::from([
            ("file_path".into(), ColumnValue::String("f.rs".into())),
            ("start_byte".into(), ColumnValue::Int64(0)),
            ("end_byte".into(), ColumnValue::Int64(2)),
        ]);
        let row_b = HashMap::from([
            ("file_path".into(), ColumnValue::String("f.rs".into())),
            ("start_byte".into(), ColumnValue::Int64(3)),
            ("end_byte".into(), ColumnValue::Int64(6)),
        ]);
        let rows: Vec<&PropertyRow> = vec![&row_a, &row_b];

        let results = s
            .resolve_batch("blob_content", &rows, &ResolverContext::default())
            .await
            .unwrap();

        assert_eq!(results[0], Some(ColumnValue::String("fn".into())));
        assert_eq!(results[1], Some(ColumnValue::String("foo".into())));
    }
}

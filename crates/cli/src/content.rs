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
/// Maps `project_id` to the correct repo root so that files from
/// different repos (or worktrees) resolve from the right directory.
/// Supports `start_byte`/`end_byte` slicing for Definition entities.
pub struct LocalContentService {
    /// project_id -> canonical repo root
    project_roots: HashMap<i64, PathBuf>,
}

impl LocalContentService {
    pub fn new(project_roots: HashMap<i64, PathBuf>) -> Self {
        Self { project_roots }
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
        // Cache file reads keyed by (project_id, path) so multiple rows
        // referencing the same file hit disk once, and different repos
        // with the same relative path don't collide.
        let mut cache: HashMap<(i64, String), Option<String>> = HashMap::new();

        Ok(rows
            .iter()
            .map(|props| {
                let file_path: String = props
                    .get("file_path")
                    .or_else(|| props.get("path"))
                    .and_then(|v| v.coerce())?;

                let project_id: i64 = props.get("project_id").and_then(|v| v.coerce())?;

                let root = self.project_roots.get(&project_id)?;

                let content = cache
                    .entry((project_id, file_path.clone()))
                    .or_insert_with(|| read_from_root(root, &file_path))
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

/// Read a file from a single repo root. Returns `None` if the path is
/// absolute, escapes the root via `..`, or the file can't be read as UTF-8.
fn read_from_root(root: &Path, relative_path: &str) -> Option<String> {
    let rel = Path::new(relative_path);
    if rel.is_absolute() {
        return None;
    }

    let full = root.join(rel);

    // Canonicalize to resolve any `..` segments, then verify the
    // resolved path is still inside the root.
    let canonical = full.canonicalize().ok()?;
    let canonical_root = root.canonicalize().ok()?;
    if !canonical.starts_with(&canonical_root) {
        return None;
    }

    std::fs::read_to_string(&canonical).ok()
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
    project_roots: HashMap<i64, PathBuf>,
) -> query_engine::shared::content::ColumnResolverRegistry {
    let mut registry = query_engine::shared::content::ColumnResolverRegistry::new();
    registry.register(
        "gitaly",
        std::sync::Arc::new(LocalContentService::new(project_roots)),
    );
    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_PID: i64 = 42;

    fn svc(root: PathBuf) -> LocalContentService {
        LocalContentService::new(HashMap::from([(TEST_PID, root)]))
    }

    fn with_pid(mut props: HashMap<String, ColumnValue>) -> HashMap<String, ColumnValue> {
        props.insert("project_id".into(), ColumnValue::Int64(TEST_PID));
        props
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

        let s = svc(dir.path().to_path_buf());
        let props = with_pid(HashMap::from([(
            "path".into(),
            ColumnValue::String("src/lib.rs".into()),
        )]));

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

        let s = svc(dir.path().to_path_buf());
        let props = with_pid(HashMap::from([
            ("file_path".into(), ColumnValue::String("src/lib.rs".into())),
            ("start_byte".into(), ColumnValue::Int64(3)),
            ("end_byte".into(), ColumnValue::Int64(7)),
        ]));

        assert_eq!(
            resolve_one(&s, &props).await,
            Some(ColumnValue::String("main".into()))
        );
    }

    #[tokio::test]
    async fn returns_none_for_missing_file() {
        let s = svc(PathBuf::from("/nonexistent"));
        let props = with_pid(HashMap::from([(
            "path".into(),
            ColumnValue::String("no/such/file.rs".into()),
        )]));
        assert_eq!(resolve_one(&s, &props).await, None);
    }

    #[tokio::test]
    async fn returns_none_for_missing_path_prop() {
        let s = svc(PathBuf::from("/tmp"));
        let props = with_pid(HashMap::new());
        assert_eq!(resolve_one(&s, &props).await, None);
    }

    #[tokio::test]
    async fn returns_none_for_unknown_project_id() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.rs"), "content").unwrap();

        let s = svc(dir.path().to_path_buf());
        let mut props = HashMap::from([("path".into(), ColumnValue::String("a.rs".into()))]);
        props.insert("project_id".into(), ColumnValue::Int64(999));

        assert_eq!(resolve_one(&s, &props).await, None);
    }

    #[tokio::test]
    async fn rejects_absolute_path() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("secret.txt"), "oh no").unwrap();

        let s = svc(dir.path().to_path_buf());
        let abs = dir.path().join("secret.txt");
        let props = with_pid(HashMap::from([(
            "path".into(),
            ColumnValue::String(abs.to_string_lossy().into()),
        )]));

        assert_eq!(resolve_one(&s, &props).await, None);
    }

    #[tokio::test]
    async fn rejects_path_traversal() {
        let dir = tempfile::TempDir::new().unwrap();
        let inner = dir.path().join("repo");
        std::fs::create_dir_all(&inner).unwrap();
        std::fs::write(dir.path().join("secret.txt"), "oh no").unwrap();

        let s = svc(inner);
        let props = with_pid(HashMap::from([(
            "path".into(),
            ColumnValue::String("../secret.txt".into()),
        )]));

        assert_eq!(resolve_one(&s, &props).await, None);
    }

    #[tokio::test]
    async fn deduplicates_file_reads() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("f.rs"), "fn foo() {}").unwrap();

        let s = svc(dir.path().to_path_buf());
        let row_a = with_pid(HashMap::from([
            ("file_path".into(), ColumnValue::String("f.rs".into())),
            ("start_byte".into(), ColumnValue::Int64(0)),
            ("end_byte".into(), ColumnValue::Int64(2)),
        ]));
        let row_b = with_pid(HashMap::from([
            ("file_path".into(), ColumnValue::String("f.rs".into())),
            ("start_byte".into(), ColumnValue::Int64(3)),
            ("end_byte".into(), ColumnValue::Int64(6)),
        ]));
        let rows: Vec<&PropertyRow> = vec![&row_a, &row_b];

        let results = s
            .resolve_batch("blob_content", &rows, &ResolverContext::default())
            .await
            .unwrap();

        assert_eq!(results[0], Some(ColumnValue::String("fn".into())));
        assert_eq!(results[1], Some(ColumnValue::String("foo".into())));
    }

    #[tokio::test]
    async fn resolves_from_correct_project_root() {
        let dir = tempfile::TempDir::new().unwrap();
        let root_a = dir.path().join("repo-a");
        let root_b = dir.path().join("repo-b");
        std::fs::create_dir_all(&root_a).unwrap();
        std::fs::create_dir_all(&root_b).unwrap();
        std::fs::write(root_a.join("f.txt"), "from repo A").unwrap();
        std::fs::write(root_b.join("f.txt"), "from repo B").unwrap();

        let pid_a: i64 = 100;
        let pid_b: i64 = 200;
        let s = LocalContentService::new(HashMap::from([(pid_a, root_a), (pid_b, root_b)]));

        let props_a = HashMap::from([
            ("path".into(), ColumnValue::String("f.txt".into())),
            ("project_id".into(), ColumnValue::Int64(pid_a)),
        ]);
        let props_b = HashMap::from([
            ("path".into(), ColumnValue::String("f.txt".into())),
            ("project_id".into(), ColumnValue::Int64(pid_b)),
        ]);

        assert_eq!(
            resolve_one(&s, &props_a).await,
            Some(ColumnValue::String("from repo A".into()))
        );
        assert_eq!(
            resolve_one(&s, &props_b).await,
            Some(ColumnValue::String("from repo B".into()))
        );
    }
}

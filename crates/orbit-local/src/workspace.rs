use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use duckdb_client::DuckDbClient;
use gitalisk_core::repository::gitalisk_repository::CoreGitaliskRepository;
use gitalisk_core::workspace_folder::gitalisk_workspace::CoreGitaliskWorkspaceFolder;
use serde_json::json;
use strum::{AsRefStr, Display};

/// Repo indexing status, stored as a DuckDB `repo_status` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub enum RepoStatus {
    Indexing,
    Indexed,
    Error,
}

/// Manages the `~/.orbit/` workspace — graph database, repo discovery,
/// and manifest.
pub struct Workspace {
    root: PathBuf,
}

impl Workspace {
    pub fn open_default() -> Result<Self> {
        Self::open(Self::default_root()?)
    }

    pub fn default_root() -> Result<PathBuf> {
        if let Some(dir) = std::env::var("ORBIT_DATA_DIR")
            .ok()
            .filter(|s| !s.is_empty())
        {
            Ok(PathBuf::from(dir))
        } else {
            let home = dirs::home_dir().context("Could not determine home directory")?;
            Ok(home.join(".orbit"))
        }
    }

    pub fn open(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    pub fn db_path(&self) -> PathBuf {
        self.root.join("graph.duckdb")
    }

    /// Discover git repos in a directory, including nested repos when
    /// the path itself is a git repo. Returns canonical paths.
    pub fn resolve_repos(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let canonical = dunce::canonicalize(path)?;

        let discovered = discover_repos(&canonical);
        if discovered.is_empty() && is_git_repo(&canonical) {
            Ok(vec![canonical])
        } else {
            Ok(discovered)
        }
    }
}

/// Resolve the DuckDB path for a command: an explicit `--db` override wins,
/// otherwise the default workspace's `graph.duckdb`. The result is absolute so
/// that a later `set_current_dir` (e.g. `repo-map` anchoring at the repo root)
/// cannot change which file a relative `--db` or `ORBIT_DATA_DIR` points at.
pub fn resolve_db_path(db: Option<PathBuf>) -> Result<PathBuf> {
    let path = match db {
        Some(p) => p,
        None => Workspace::open_default()?.db_path(),
    };
    absolutize(path)
}

fn absolutize(path: PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()
            .context("failed to read current directory")?
            .join(path))
    }
}

const LOCAL_DDL_META_KEY: &str = "local_ddl";

pub fn ensure_graph_schema(db_path: &Path, ddl: &str) -> Result<()> {
    let client = DuckDbClient::open(db_path).context("failed to open DuckDB")?;
    if stored_local_ddl(&client)?.as_deref() == Some(ddl) {
        return Ok(());
    }
    let had_data = table_exists(&client, "_orbit_manifest")?;
    drop(client);

    if had_data {
        tracing::warn!(
            "local graph schema changed; rebuilding {} (previously indexed repositories must be re-indexed)",
            db_path.display()
        );
    }
    remove_db_files(db_path)?;

    let client = DuckDbClient::open(db_path).context("failed to open DuckDB")?;
    client
        .initialize_schema(ddl)
        .context("failed to create schema")?;
    client
        .execute(
            "INSERT INTO _orbit_meta (key, value) VALUES (?1, ?2)",
            &[json!(LOCAL_DDL_META_KEY), json!(ddl)],
        )
        .context("failed to record schema fingerprint")?;
    Ok(())
}

fn stored_local_ddl(client: &DuckDbClient) -> Result<Option<String>> {
    if !table_exists(client, "_orbit_meta")? {
        return Ok(None);
    }
    let batches = client
        .query_arrow_json(
            "SELECT value FROM _orbit_meta WHERE key = ?1",
            &[json!(LOCAL_DDL_META_KEY)],
        )
        .context("failed to read _orbit_meta")?;
    Ok(duckdb_client::string_column(&batches, "value")
        .into_iter()
        .next())
}

fn table_exists(client: &DuckDbClient, name: &str) -> Result<bool> {
    let batches = client
        .query_arrow_json(
            "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM information_schema.tables WHERE table_name = ?1",
            &[json!(name)],
        )
        .context("failed to query information_schema")?;
    Ok(duckdb_client::scalar_i64(&batches) > 0)
}

fn remove_db_files(db_path: &Path) -> Result<()> {
    let mut wal = db_path.as_os_str().to_owned();
    wal.push(".wal");
    for path in [db_path.to_path_buf(), PathBuf::from(wal)] {
        if let Err(e) = std::fs::remove_file(&path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            return Err(e).with_context(|| {
                format!(
                    "failed to remove outdated local graph {}; delete it manually and retry",
                    path.display()
                )
            });
        }
    }
    Ok(())
}

pub fn set_status(
    client: &DuckDbClient,
    repo_path: &str,
    project_id: i64,
    status: RepoStatus,
    error_message: Option<&str>,
    git: Option<&GitInfo>,
) -> Result<()> {
    let parent = git.map(|g| g.parent_repo_path.to_string_lossy().to_string());
    let branch = git.map(|g| g.branch.as_str());
    let commit = git.map(|g| g.commit_sha.as_str());

    client
        .execute(
            "INSERT INTO _orbit_manifest (repo_path, project_id, parent_repo_path, branch, commit_sha, status, error_message, last_indexed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6::repo_status, ?7, CASE WHEN ?6 = 'indexed' THEN now() ELSE NULL END)
             ON CONFLICT (repo_path) DO UPDATE SET
                 parent_repo_path = COALESCE(?3, parent_repo_path),
                 branch = COALESCE(?4, branch),
                 commit_sha = COALESCE(?5, commit_sha),
                 status = ?6::repo_status,
                 error_message = ?7,
                 last_indexed_at = CASE WHEN ?6 = 'indexed' THEN now() ELSE last_indexed_at END",
            &[
                json!(repo_path),
                json!(project_id),
                parent.map_or(serde_json::Value::Null, |s| json!(s)),
                branch.map_or(serde_json::Value::Null, |s| json!(s)),
                commit.map_or(serde_json::Value::Null, |s| json!(s)),
                json!(status.as_ref()),
                error_message.map_or(serde_json::Value::Null, |s| json!(s)),
            ],
        )
        .context("failed to upsert manifest")?;
    Ok(())
}

/// Best-effort `error` manifest row for a repo git_info could not open; logged, never propagated so it can't mask the original error.
pub fn record_git_info_failure(db_path: &Path, repo_path: &Path, error: &str) {
    let key = dunce::canonicalize(repo_path)
        .unwrap_or_else(|_| repo_path.to_path_buf())
        .to_string_lossy()
        .to_string();
    let client = match DuckDbClient::open(db_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("failed to open DuckDB to record git failure for {key}: {e}");
            return;
        }
    };
    if let Err(e) = set_status(
        &client,
        &key,
        project_id_from_path(&key),
        RepoStatus::Error,
        Some(error),
        None,
    ) {
        tracing::warn!("failed to record git failure status for {key}: {e}");
    }
}

pub struct GitInfo {
    pub repo_path: PathBuf,
    /// Deterministic project ID derived from `repo_path`.
    pub project_id: i64,
    pub branch: String,
    pub commit_sha: String,
    /// For worktrees, the parent repo's canonical path. For regular
    /// repos, same as `repo_path`.
    pub parent_repo_path: PathBuf,
}

pub fn git_info(repo_path: &Path) -> Result<GitInfo> {
    let canonical = dunce::canonicalize(repo_path)
        .with_context(|| format!("failed to canonicalize {}", repo_path.display()))?;
    let path_str = canonical.to_string_lossy().to_string();
    let project_id = project_id_from_path(&path_str);

    let repo = CoreGitaliskRepository::new(path_str.clone(), path_str);

    let branch = repo
        .get_current_branch()
        .context("failed to get current branch")?;
    let commit_sha = repo.get_current_commit_hash().map_err(|_| {
        anyhow::anyhow!(
            "{} has no commits yet. Make at least one commit before indexing.",
            canonical.display()
        )
    })?;
    let parent_repo_path = repo
        .parent_repo_path()
        .context("failed to resolve parent repo path")?;

    Ok(GitInfo {
        repo_path: canonical,
        project_id,
        branch,
        commit_sha,
        parent_repo_path,
    })
}

/// Resolve the working-tree root of the repo containing `path`.
///
/// `orbit index` walks and stores file paths relative to this root, so a
/// `repo-map` invocation pointed at a subdirectory must anchor here too;
/// otherwise `read_text` globs (resolved against CWD) miss the repo-root-relative
/// paths the graph holds and `api`/`class` silently degrade. This is the git
/// working-tree root (`git rev-parse --show-toplevel`), not
/// [`GitInfo::parent_repo_path`], which for a worktree points at the origin
/// repo instead of this checkout's own root.
pub fn git_toplevel(path: &Path) -> Result<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(path)
        .output()
        .with_context(|| format!("failed to run git in {}", path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "git rev-parse --show-toplevel failed in {}: {}",
            path.display(),
            stderr.trim()
        );
    }

    let top = String::from_utf8(output.stdout)
        .context("git output is not valid UTF-8")?
        .trim()
        .to_string();
    dunce::canonicalize(&top).with_context(|| format!("failed to canonicalize {top}"))
}

/// Mask clears the sign bit so the result is always a positive i64.
pub fn project_id_from_path(path: &str) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

fn is_git_repo(path: &Path) -> bool {
    let git = path.join(".git");
    git.is_dir() || git.is_file()
}

fn discover_repos(workspace_path: &Path) -> Vec<PathBuf> {
    let ws = CoreGitaliskWorkspaceFolder::new(workspace_path.to_string_lossy().to_string());
    if let Err(e) = ws.index_repositories() {
        let path = workspace_path.display();
        tracing::warn!("repository discovery failed in {path}: {e}");
        return vec![];
    }
    ws.get_repositories()
        .into_iter()
        .map(|r| PathBuf::from(&r.path))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_repo(path: &Path) {
        std::fs::create_dir_all(path).unwrap();
        Command::new("git")
            .args(["init"])
            .current_dir(path)
            .output()
            .unwrap();
    }

    const LOCAL_DDL: &str = include_str!(concat!(env!("CONFIG_DIR"), "/graph_local.sql"));

    #[test]
    fn record_git_info_failure_writes_error_row() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = tmp.path().join("graph.duckdb");
        let client = DuckDbClient::open(&db).unwrap();
        client.initialize_schema(LOCAL_DDL).unwrap();
        drop(client);

        let repo = tmp.path().join("unopenable");
        std::fs::create_dir_all(&repo).unwrap();
        record_git_info_failure(&db, &repo, "failed to get current branch");

        let client = DuckDbClient::open(&db).unwrap();
        let rows = client
            .query_arrow("SELECT CAST(status AS VARCHAR), error_message FROM _orbit_manifest")
            .unwrap();
        let batch = rows.first().expect("one row");
        assert_eq!(batch.num_rows(), 1);
        let status = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let msg = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(status.value(0), "error");
        assert_eq!(msg.value(0), "failed to get current branch");
    }

    #[test]
    fn remove_db_files_appends_wal_to_the_full_path() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = tmp.path().join("mydb");
        let wal = tmp.path().join("mydb.wal");
        std::fs::write(&db, b"").unwrap();
        std::fs::write(&wal, b"").unwrap();

        remove_db_files(&db).unwrap();

        assert!(!db.exists());
        assert!(!wal.exists());
    }

    #[test]
    fn ensure_graph_schema_is_idempotent_and_preserves_data() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = tmp.path().join("graph.duckdb");

        ensure_graph_schema(&db, LOCAL_DDL).unwrap();
        let client = DuckDbClient::open(&db).unwrap();
        client
            .execute(
                "INSERT INTO _orbit_manifest (repo_path, project_id) VALUES ('/r', 1)",
                &[],
            )
            .unwrap();
        drop(client);

        ensure_graph_schema(&db, LOCAL_DDL).unwrap();
        let client = DuckDbClient::open(&db).unwrap();
        let batches = client
            .query_arrow("SELECT count(*) FROM _orbit_manifest")
            .unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 1);
    }

    #[test]
    fn ensure_graph_schema_rebuilds_on_ddl_drift() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = tmp.path().join("graph.duckdb");
        let old_ddl = "CREATE TABLE IF NOT EXISTS _orbit_meta (key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL);
             CREATE TABLE IF NOT EXISTS old_table (id BIGINT)";

        ensure_graph_schema(&db, old_ddl).unwrap();
        ensure_graph_schema(&db, LOCAL_DDL).unwrap();

        let client = DuckDbClient::open(&db).unwrap();
        assert!(!table_exists(&client, "old_table").unwrap());
        assert!(table_exists(&client, "gl_definition").unwrap());
        assert_eq!(
            stored_local_ddl(&client).unwrap().as_deref(),
            Some(LOCAL_DDL)
        );
    }

    #[test]
    fn ensure_graph_schema_rebuilds_legacy_db_without_fingerprint() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = tmp.path().join("graph.duckdb");

        let client = DuckDbClient::open(&db).unwrap();
        client
            .initialize_schema(
                "CREATE TABLE IF NOT EXISTS _orbit_manifest (repo_path VARCHAR PRIMARY KEY, project_id BIGINT NOT NULL);
                 CREATE TABLE IF NOT EXISTS gl_definition (id BIGINT NOT NULL)",
            )
            .unwrap();
        client
            .execute(
                "INSERT INTO _orbit_manifest (repo_path, project_id) VALUES ('/r', 1)",
                &[],
            )
            .unwrap();
        drop(client);

        ensure_graph_schema(&db, LOCAL_DDL).unwrap();

        let client = DuckDbClient::open(&db).unwrap();
        let batches = client
            .query_arrow("SELECT count(*) FROM _orbit_manifest")
            .unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 0);

        let cols = client
            .query_arrow(
                "SELECT count(*) FROM information_schema.columns WHERE table_name = 'gl_definition'",
            )
            .unwrap();
        let col_count = cols[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert!(col_count.value(0) > 1);
    }

    #[test]
    fn test_project_id_deterministic() {
        let a = project_id_from_path("/Users/alice/repo");
        let b = project_id_from_path("/Users/alice/repo");
        assert_eq!(a, b);
        assert!(a > 0);
    }

    #[test]
    fn test_project_id_different_paths() {
        let a = project_id_from_path("/Users/alice/repo-a");
        let b = project_id_from_path("/Users/alice/repo-b");
        assert_ne!(a, b);
    }

    #[test]
    fn test_resolve_single_repo() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = Workspace::open(temp.path().join("orbit")).unwrap();

        let repo = temp.path().join("my-repo");
        init_repo(&repo);

        let repos = store.resolve_repos(&repo).unwrap();
        assert_eq!(repos.len(), 1);
    }

    #[test]
    fn test_resolve_workspace() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = Workspace::open(temp.path().join("orbit")).unwrap();

        let workspace = temp.path().join("workspace");
        init_repo(&workspace.join("repo-a"));
        init_repo(&workspace.join("repo-b"));
        std::fs::create_dir_all(workspace.join("not-a-repo")).unwrap();

        let repos = store.resolve_repos(&workspace).unwrap();
        assert_eq!(repos.len(), 2);
    }

    #[test]
    fn test_resolve_workspace_that_is_also_a_git_repo() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = Workspace::open(temp.path().join("orbit")).unwrap();

        let workspace = temp.path().join("workspace");
        init_repo(&workspace);
        init_repo(&workspace.join("nested-a"));
        init_repo(&workspace.join("nested-b"));

        let repos = store.resolve_repos(&workspace).unwrap();
        assert!(
            repos.len() >= 3,
            "expected at least 3 repos, got {}: {:?}",
            repos.len(),
            repos
        );
    }

    #[test]
    fn git_toplevel_climbs_from_a_subdirectory() {
        let temp = tempfile::TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        init_repo(&repo);
        let subdir = repo.join("src/deep");
        std::fs::create_dir_all(&subdir).unwrap();

        let top = git_toplevel(&subdir).unwrap();
        assert_eq!(top, dunce::canonicalize(&repo).unwrap());
    }
}

use std::path::Path;

use anyhow::{Context, Result};
use duckdb_client::DuckDbClient;
use serde_json::json;
use strum::{AsRefStr, Display};

use super::lock_database;
use crate::workspace::{GitInfo, project_id_from_path};

pub const MANIFEST_DDL: &str = include_str!(concat!(env!("CONFIG_DIR"), "/orbit_cli/manifest.sql"));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub enum RepoStatus {
    Indexing,
    Indexed,
    Error,
}

pub fn record_status(
    path: &Path,
    git: &GitInfo,
    status: RepoStatus,
    error: Option<&str>,
    database: &Path,
) -> Result<()> {
    let _lock = lock_database(path)?;
    let client = open_manifest(path)?;
    set_status(
        &client,
        &git.repo_path.to_string_lossy(),
        git.project_id,
        status,
        error,
        Some(git),
        Some(database),
    )
}

pub fn read(path: &Path, query: &str) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    if !path
        .try_exists()
        .with_context(|| format!("failed to check {}", path.display()))?
    {
        return Ok(Vec::new());
    }
    let _lock = lock_database(path)?;
    let client = DuckDbClient::open_read_only(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    if !super::schema::table_exists(&client, "_orbit_manifest")? {
        return Ok(Vec::new());
    }
    client
        .query_arrow(query)
        .context("failed to read _orbit_manifest")
}

fn set_status(
    client: &DuckDbClient,
    repo_path: &str,
    project_id: i64,
    status: RepoStatus,
    error_message: Option<&str>,
    git: Option<&GitInfo>,
    database_path: Option<&Path>,
) -> Result<()> {
    let parent = git.map(|g| g.parent_repo_path.to_string_lossy().to_string());
    let branch = git.map(|g| g.branch.as_str());
    let commit = git.map_or("", |g| g.commit_sha.as_str());

    client
        .execute(
            "INSERT INTO _orbit_manifest (repo_path, project_id, parent_repo_path, branch, commit_sha, status, error_message, last_indexed_at, database_path)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6::repo_status, ?7, CASE WHEN ?6 = 'indexed' THEN now() ELSE NULL END, ?8)
             ON CONFLICT (repo_path, commit_sha) DO UPDATE SET
                 parent_repo_path = COALESCE(?3, parent_repo_path),
                 branch = COALESCE(?4, branch),
                 database_path = COALESCE(?8, database_path),
                 status = ?6::repo_status,
                 error_message = ?7,
                 last_indexed_at = CASE WHEN ?6 = 'indexed' THEN now() ELSE last_indexed_at END",
            &[
                json!(repo_path),
                json!(project_id),
                parent.map_or(serde_json::Value::Null, |s| json!(s)),
                branch.map_or(serde_json::Value::Null, |s| json!(s)),
                json!(commit),
                json!(status.as_ref()),
                error_message.map_or(serde_json::Value::Null, |s| json!(s)),
                json!(database_path.map(|path| path.to_string_lossy().to_string())),
            ],
        )
        .context("failed to upsert manifest")?;
    Ok(())
}

pub fn record_git_info_failure(db_path: &Path, repo_path: &Path, error: &str) {
    let key = dunce::canonicalize(repo_path)
        .unwrap_or_else(|_| repo_path.to_path_buf())
        .to_string_lossy()
        .to_string();
    let _lock = match lock_database(db_path) {
        Ok(lock) => lock,
        Err(error) => {
            orbit_log_fields::warn!(expanded(ERROR_MESSAGE) = %error, "failed to lock manifest");
            return;
        }
    };
    let client = match open_manifest(db_path) {
        Ok(c) => c,
        Err(e) => {
            orbit_log_fields::warn!(
                expanded(LOCAL_PATH) = %key,
                expanded(ERROR_MESSAGE) = %e,
                "failed to open DuckDB to record git failure"
            );
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
        None,
    ) {
        orbit_log_fields::warn!(
            expanded(LOCAL_PATH) = %key,
            expanded(ERROR_MESSAGE) = %e,
            "failed to record git failure status"
        );
    }
}

fn open_manifest(path: &Path) -> Result<DuckDbClient> {
    let client = DuckDbClient::open(path)?;
    client.initialize_schema(MANIFEST_DDL)?;
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::schema::table_exists;

    #[test]
    fn record_git_info_failure_writes_error_row() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = tmp.path().join("manifest.duckdb");

        let repo = tmp.path().join("unopenable");
        std::fs::create_dir_all(&repo).unwrap();
        record_git_info_failure(&db, &repo, "failed to get current branch");

        let client = DuckDbClient::open(&db).unwrap();
        assert!(!table_exists(&client, "gl_file").unwrap());
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
}

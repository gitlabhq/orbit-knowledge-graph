use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb_client::DuckDbClient;
use serde_json::json;

use super::read_lock_database;

const LOCAL_DDL_META_KEY: &str = "local_ddl";
const CODE_INDEX_META_KEY: &str = "code_index_revision";
const CODE_INDEX_REVISION: &str = "4";

pub fn graph_lacks_commit(db: &Path, commit_sha: &str) -> Result<bool> {
    let Some(_lock) = read_lock_database(db)? else {
        return Ok(true);
    };
    let client = DuckDbClient::open_read_only(db)?;
    if stored_meta(&client, CODE_INDEX_META_KEY)?.as_deref() != Some(CODE_INDEX_REVISION) {
        return Ok(true);
    }
    Ok(stored_meta(&client, "indexed_commit")?.as_deref() != Some(commit_sha))
}

pub fn ensure_graph_schema(db_path: &Path, ddl: &str) -> Result<()> {
    let client = DuckDbClient::open(db_path).context("failed to open DuckDB")?;
    if stored_meta(&client, LOCAL_DDL_META_KEY)?.as_deref() == Some(ddl)
        && stored_meta(&client, CODE_INDEX_META_KEY)?.as_deref() == Some(CODE_INDEX_REVISION)
    {
        return Ok(());
    }
    let had_data = table_exists(&client, "gl_file")?;
    drop(client);

    if had_data {
        orbit_log_fields::warn!(
            expanded(LOCAL_PATH) = %db_path.display(),
            "local graph schema or code index changed; rebuilding (previously indexed repositories must be re-indexed)"
        );
    }
    remove_db_files(db_path)?;

    let client = DuckDbClient::open(db_path).context("failed to open DuckDB")?;
    client
        .initialize_schema(ddl)
        .context("failed to create schema")?;
    client
        .execute(
            "INSERT INTO _orbit_meta (key, value) VALUES (?1, ?2), (?3, ?4)",
            &[
                json!(LOCAL_DDL_META_KEY),
                json!(ddl),
                json!(CODE_INDEX_META_KEY),
                json!(CODE_INDEX_REVISION),
            ],
        )
        .context("failed to record schema fingerprint")?;
    Ok(())
}

pub(crate) fn stored_meta(client: &DuckDbClient, key: &str) -> Result<Option<String>> {
    if !table_exists(client, "_orbit_meta")? {
        return Ok(None);
    }
    let batches = client
        .query_arrow_json(
            "SELECT value FROM _orbit_meta WHERE key = ?1",
            &[json!(key)],
        )
        .context("failed to read _orbit_meta")?;
    Ok(duckdb_client::string_column(&batches, "value")
        .into_iter()
        .next())
}

pub(crate) fn table_exists(client: &DuckDbClient, name: &str) -> Result<bool> {
    let batches = client
        .query_arrow_json(
            "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?1",
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

#[cfg(test)]
mod tests {
    use super::*;

    const LOCAL_DDL: &str = include_str!(concat!(env!("ORBIT_CONFIG_DIR"), "/graph_local.sql"));

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
                "INSERT INTO _orbit_meta (key, value) VALUES ('test', 'preserved')",
                &[],
            )
            .unwrap();
        drop(client);

        ensure_graph_schema(&db, LOCAL_DDL).unwrap();
        let client = DuckDbClient::open(&db).unwrap();
        let batches = client
            .query_arrow("SELECT count(*) FROM _orbit_meta WHERE key = 'test'")
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
            stored_meta(&client, LOCAL_DDL_META_KEY).unwrap().as_deref(),
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
        assert!(!table_exists(&client, "_orbit_manifest").unwrap());

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
}

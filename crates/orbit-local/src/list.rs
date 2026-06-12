use crate::sql_format::{self, Format};
use crate::workspace;
use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use std::io::Write;
use std::path::{Path, PathBuf};

// Cast the enum and timestamp to text so JSON/CSV don't render them as a
// dictionary index or epoch-millis integer. The ORDER BY qualifies the
// table column so it sorts the TIMESTAMP, not the CAST alias.
const MANIFEST_QUERY: &str = "SELECT \
       repo_path, \
       branch, \
       commit_sha, \
       CAST(status AS VARCHAR) AS status, \
       CAST(last_indexed_at AS VARCHAR) AS last_indexed_at \
     FROM _orbit_manifest \
     ORDER BY _orbit_manifest.last_indexed_at DESC NULLS LAST, repo_path";

pub fn run(format: Format, db: Option<PathBuf>) -> Result<()> {
    let db_path = workspace::resolve_db_path(db)?;
    let stdout = std::io::stdout().lock();
    run_to(format, &db_path, stdout)
}

fn run_to<W: Write>(format: Format, db_path: &Path, out: W) -> Result<()> {
    let batches = load_manifest(db_path)?;
    // An empty table render is noise for humans, but structured formats
    // must stay parseable on empty input (`orbit list -F json | jq`), so
    // only the table view suppresses the writers.
    if matches!(format, Format::Table) && batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(());
    }
    sql_format::write(out, format, &batches)
}

fn load_manifest(db_path: &Path) -> Result<Vec<RecordBatch>> {
    // Unlike `sql`/`schema`, a missing graph is a normal state for `list`,
    // not an error. `try_exists` so an unreadable path still errors instead
    // of reading as empty.
    let present = db_path
        .try_exists()
        .with_context(|| format!("failed to check {}", db_path.display()))?;
    if !present {
        return Ok(Vec::new());
    }
    let client = duckdb_client::DuckDbClient::open_read_only(db_path)
        .with_context(|| format!("failed to open {}", db_path.display()))?;
    if !manifest_exists(&client)? {
        return Ok(Vec::new());
    }
    client
        .query_arrow(MANIFEST_QUERY)
        .context("failed to read _orbit_manifest")
}

fn manifest_exists(client: &duckdb_client::DuckDbClient) -> Result<bool> {
    let batches = client
        .query_arrow(
            "SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'main' AND table_name = '_orbit_manifest'",
        )
        .context("failed to check for _orbit_manifest")?;
    Ok(batches.iter().any(|b| b.num_rows() > 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::project_id_from_path;

    const LOCAL_DDL: &str = include_str!(concat!(env!("CONFIG_DIR"), "/graph_local.sql"));

    // Mirrors `set_status`: only `indexed` rows get a timestamp, so
    // non-indexed rows exercise the NULL `last_indexed_at` path.
    fn seed(db_path: &Path, rows: &[(&str, &str, &str, &str)]) {
        let client = duckdb_client::DuckDbClient::open(db_path).unwrap();
        client.initialize_schema(LOCAL_DDL).unwrap();
        for (repo, branch, commit, status) in rows {
            client
                .execute(
                    "INSERT INTO _orbit_manifest \
                       (repo_path, project_id, branch, commit_sha, status, last_indexed_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5::repo_status, \
                             CASE WHEN ?5 = 'indexed' THEN now() END)",
                    &[
                        serde_json::json!(repo),
                        serde_json::json!(project_id_from_path(repo)),
                        serde_json::json!(branch),
                        serde_json::json!(commit),
                        serde_json::json!(status),
                    ],
                )
                .unwrap();
        }
    }

    fn render(format: Format, db: &Path) -> String {
        let mut out = Vec::new();
        run_to(format, db, &mut out).unwrap();
        String::from_utf8(out).unwrap()
    }

    fn assert_empty_state_output(db: &Path) {
        assert!(render(Format::Table, db).is_empty());
        let json = render(Format::Json, db);
        let v: serde_json::Value = serde_json::from_str(json.trim()).unwrap();
        assert_eq!(v.as_array().map(Vec::len), Some(0));
        assert!(render(Format::Ndjson, db).is_empty());
    }

    #[test]
    fn missing_db_emits_valid_empty_output() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = tmp.path().join("absent.duckdb");
        assert_empty_state_output(&db);
        assert!(render(Format::Csv, &db).is_empty());
    }

    #[test]
    fn db_without_manifest_emits_valid_empty_output() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = tmp.path().join("empty.duckdb");
        duckdb_client::DuckDbClient::open(&db).unwrap();
        assert_empty_state_output(&db);
        assert!(render(Format::Csv, &db).is_empty());
    }

    #[test]
    fn empty_manifest_emits_valid_empty_output() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = tmp.path().join("graph.duckdb");
        seed(&db, &[]);
        assert_empty_state_output(&db);
        // DuckDB returns zero batches (not a 0-row batch) for an empty
        // result, so there is no schema to print a CSV header from.
        assert!(render(Format::Csv, &db).is_empty());
    }

    #[test]
    fn lists_indexed_repos_as_table() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = tmp.path().join("graph.duckdb");
        seed(
            &db,
            &[
                ("/tmp/repo-a", "main", "aaaaaaa", "indexed"),
                ("/tmp/repo-b", "dev", "bbbbbbb", "indexed"),
            ],
        );
        let mut out = Vec::new();
        run_to(Format::Table, &db, &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("/tmp/repo-a") && s.contains("/tmp/repo-b"));
        assert!(s.contains("repo_path") && s.contains("indexed"));
    }

    #[test]
    fn lists_indexed_repos_as_json() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = tmp.path().join("graph.duckdb");
        seed(
            &db,
            &[
                ("/tmp/repo-a", "main", "aaaaaaa", "indexed"),
                ("/tmp/repo-b", "dev", "bbbbbbb", "pending"),
            ],
        );
        let mut out = Vec::new();
        run_to(Format::Json, &db, &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        let v: serde_json::Value = serde_json::from_str(s.trim()).unwrap();
        let rows = v.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["repo_path"], "/tmp/repo-a");
        assert_eq!(rows[0]["status"], "indexed");
        // The pending row has no timestamp and must sort last (NULLS LAST).
        assert_eq!(rows[1]["repo_path"], "/tmp/repo-b");
        assert!(rows[1].get("last_indexed_at").is_none_or(|t| t.is_null()));
    }
}

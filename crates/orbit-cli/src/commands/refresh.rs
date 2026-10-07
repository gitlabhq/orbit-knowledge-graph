//! Keeps the local graph in step with the working tree. Files that `git status` reports as
//! edited, added, or deleted since the indexed commit are re-parsed on their own and swapped
//! into the graph before a query runs. Cross-file edges into an edited file return on the
//! next full index.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use duckdb_client::DuckDbClient;
use ontology::Ontology;
use orbit_utils::fs_walk::{Decision, FileInventory, FileInventoryEntry, step};
use serde_json::json;

use super::index::MAX_INDEXED_FILE_BYTES;
use crate::workspace::{self, GitInfo};

const NODE_FILE_COLUMNS: &[(&str, &str)] = &[
    ("gl_definition", "file_path"),
    ("gl_imported_symbol", "file_path"),
    ("gl_file", "path"),
];
const STAGED_TABLES: &[&str] = &[
    "gl_definition",
    "gl_imported_symbol",
    "gl_file",
    "gl_directory",
    "gl_edge",
];

type Fingerprints = BTreeMap<String, String>;

pub(crate) fn refresh_worktree(git: &GitInfo, db: &Path) -> Result<usize> {
    let key = format!("worktree:{}", git.project_id);
    let edited = edited_files(&git.repo_path)?;
    let stored: Fingerprints = {
        let client = DuckDbClient::open_read_only(db)?;
        workspace::stored_meta(&client, &key)?
            .and_then(|value| serde_json::from_str(&value).ok())
            .unwrap_or_default()
    };
    let current: Fingerprints = edited
        .iter()
        .map(|path| (path.clone(), fingerprint(&git.repo_path.join(path))))
        .collect();
    let stale: Vec<String> = current
        .keys()
        .chain(stored.keys())
        .filter(|path| current.get(*path) != stored.get(*path))
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    if stale.is_empty() {
        return Ok(current.len());
    }
    let batches = parse(git, &stale)?;
    let client = DuckDbClient::open(db).context("failed to open DuckDB to refresh edits")?;
    swap(&client, git, &stale, &batches)?;
    let readable: Vec<String> = stale
        .iter()
        .filter(|path| {
            std::fs::metadata(git.repo_path.join(path))
                .is_ok_and(|meta| duckdb_client::search::is_indexed_text_file(path, meta.len()))
        })
        .cloned()
        .collect();
    duckdb_client::search::append_text_lines(
        &client,
        &duckdb_client::search::text_line_table(git.project_id),
        &git.repo_path,
        &git.commit_sha,
        &readable,
    )?;
    client.execute(
        "INSERT OR REPLACE INTO _orbit_meta (key, value) VALUES (?1, ?2)",
        &[json!(key), json!(serde_json::to_string(&current)?)],
    )?;
    Ok(current.len())
}

fn edited_files(repo: &Path) -> Result<Vec<String>> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["status", "--porcelain=v1", "-z", "--untracked-files=all"])
        .output()
        .context("failed to run git status")?;
    anyhow::ensure!(output.status.success(), "git status failed");
    let status = String::from_utf8_lossy(&output.stdout);
    let mut fields = status.split('\0').filter(|f| f.len() > 3);
    let mut files = Vec::new();
    while let Some(entry) = fields.next() {
        let (code, path) = entry.split_at(3);
        if code.contains(['R', 'C']) {
            files.extend(fields.next().map(str::to_string));
        }
        files.push(path.to_string());
    }
    Ok(files)
}

fn fingerprint(path: &Path) -> String {
    std::fs::metadata(path)
        .map(|meta| {
            let modified = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(0, |d| d.as_nanos());
            format!("{modified}:{}", meta.len())
        })
        .unwrap_or_else(|_| "deleted".to_string())
}

fn parse(git: &GitInfo, files: &[String]) -> Result<Vec<(String, RecordBatch)>> {
    let mut filter = code_graph::v2::config::CodeFilter::new(
        Some(MAX_INDEXED_FILE_BYTES),
        None,
        code_graph::v2::config::detect_language_from_path,
    );
    let mut entries = Vec::new();
    let mut content = Vec::new();
    for path in files {
        let full = git.repo_path.join(path);
        let Ok(meta) = std::fs::metadata(&full) else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        let mut entry = FileInventoryEntry {
            path: path.clone(),
            size: meta.len(),
            decision: Decision::ListOnly,
            label: Default::default(),
        };
        (entry.decision, entry.label) = step(&mut filter, &entry, &mut content, |buf| {
            std::fs::File::open(&full)?.read_to_end(buf).map(drop)
        })?;
        if entry.decision != Decision::Drop {
            entries.push(entry);
        }
    }
    let batches = Arc::new(Mutex::new(Vec::new()));
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let sink = batches.clone();
    let on_batch: Arc<code_graph::v2::OnBatch> =
        Arc::new(move |table: &str, batch: RecordBatch| {
            sink.lock().unwrap().push((table.to_string(), batch));
            Ok(())
        });
    let converter: Arc<dyn code_graph::v2::GraphConverter> =
        Arc::new(duckdb_client::DuckDbConverter {
            project_id: git.project_id,
            branch: git.branch.clone(),
            commit_sha: git.commit_sha.clone(),
            ontology: Arc::new(Ontology::load_embedded().context("failed to load ontology")?),
        });
    let config = code_graph::v2::PipelineConfig {
        per_file_timeout: Some(Duration::from_secs(2)),
        per_file_parse_timeout: Some(Duration::from_millis(100)),
        per_file_walk_timeout: Some(Duration::from_millis(100)),
        per_file_ssa_timeout: Some(Duration::from_millis(100)),
        cross_file_resolve_timeout: Some(Duration::from_secs(10)),
        ..Default::default()
    };
    code_graph::v2::Pipeline::run_with_tracer(
        &git.repo_path,
        Arc::new(FileInventory::new(entries)),
        config,
        code_graph::v2::trace::Tracer::new(false),
        converter,
        on_batch,
    );
    Ok(std::mem::take(&mut *batches.lock().unwrap()))
}

fn swap(
    client: &DuckDbClient,
    git: &GitInfo,
    files: &[String],
    batches: &[(String, RecordBatch)],
) -> Result<()> {
    client.execute(
        "CREATE OR REPLACE TABLE _orbit_refresh_files (path VARCHAR)",
        &[],
    )?;
    for file in files {
        client.execute(
            "INSERT INTO _orbit_refresh_files VALUES (?1)",
            &[json!(file)],
        )?;
    }
    for table in STAGED_TABLES {
        client.execute(
            &format!(
                "CREATE OR REPLACE TABLE _orbit_refresh_{table} AS SELECT * FROM {table} LIMIT 0"
            ),
            &[],
        )?;
    }
    for (table, batch) in batches {
        if STAGED_TABLES.contains(&table.as_str()) {
            client.insert_batch(&format!("_orbit_refresh_{table}"), batch)?;
        }
    }
    let ids = |prefix: &str| {
        NODE_FILE_COLUMNS
            .iter()
            .map(|(table, column)| {
                format!(
                    "SELECT id FROM {prefix}{table} WHERE project_id = ?1 AND {column} IN (SELECT path FROM _orbit_refresh_files)"
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    };
    let project = [json!(git.project_id)];
    client.execute("BEGIN TRANSACTION", &[])?;
    let mut statements = vec![format!(
        "DELETE FROM gl_edge WHERE source_id IN ({old}) OR target_id IN ({old})",
        old = ids("")
    )];
    for (table, column) in NODE_FILE_COLUMNS {
        statements.push(format!(
            "DELETE FROM {table} WHERE project_id = ?1 AND {column} IN (SELECT path FROM _orbit_refresh_files)"
        ));
        statements.push(format!(
            "INSERT INTO {table} SELECT * FROM _orbit_refresh_{table} WHERE project_id = ?1 AND {column} IN (SELECT path FROM _orbit_refresh_files)"
        ));
    }
    statements.push(
        "INSERT INTO gl_directory SELECT * FROM _orbit_refresh_gl_directory r WHERE project_id = ?1 AND NOT EXISTS (SELECT 1 FROM gl_directory d WHERE d.id = r.id)".to_string(),
    );
    statements.push(format!(
        "INSERT INTO gl_edge SELECT * FROM _orbit_refresh_gl_edge WHERE source_id IN ({new}) OR target_id IN ({new})",
        new = ids("_orbit_refresh_")
    ));
    statements.push(format!(
        "DELETE FROM {} WHERE file_path IN (SELECT path FROM _orbit_refresh_files)",
        duckdb_client::search::text_line_table(git.project_id)
    ));
    for statement in &statements {
        let params: &[serde_json::Value] = match statement.contains("?1") {
            true => &project,
            false => &[],
        };
        if let Err(error) = client.execute(statement, params) {
            client.execute("ROLLBACK", &[]).ok();
            return Err(error).context("failed to swap edited files into the graph");
        }
    }
    client.execute("COMMIT", &[])?;
    for table in STAGED_TABLES {
        client.execute(&format!("DROP TABLE IF EXISTS _orbit_refresh_{table}"), &[])?;
    }
    client.execute("DROP TABLE IF EXISTS _orbit_refresh_files", &[])?;
    Ok(())
}

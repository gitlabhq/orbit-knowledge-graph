use crate::commands::setup::spec;
use crate::sql_format::{self, Format};
use crate::workspace;
use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use duckdb_client::{DuckDbClient, bool_column, sql_lit, string_column};
use std::io::{IsTerminal, Read};
use std::path::{Path, PathBuf};

const META_TABLE: &str = "_orbit_meta";

pub struct LocalGraphReader {
    client: DuckDbClient,
    _locks: Vec<std::fs::File>,
}

impl std::ops::Deref for LocalGraphReader {
    type Target = DuckDbClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

pub fn schema_introspection_sql() -> String {
    format!(
        "SELECT table_name, column_name, data_type \
         FROM information_schema.columns \
         WHERE table_schema = 'main' \
           AND table_name <> {} \
         GROUP BY table_name, column_name, data_type \
         ORDER BY table_name, min(ordinal_position)",
        sql_lit(META_TABLE)
    )
}

pub fn open_graph(db: Option<PathBuf>) -> Result<LocalGraphReader> {
    let Some(db_path) = db else {
        return open_workspace_graph();
    };
    let lock = workspace::read_lock_database(&db_path)?;
    if !db_path.exists() {
        anyhow::bail!(
            "no local graph found at {}. Index a repository first \
             (`{} index` inside it, or the `index` MCP tool).",
            db_path.display(),
            spec::launcher()
        );
    }
    let client = DuckDbClient::open_read_only(&db_path)
        .with_context(|| format!("failed to open {}", db_path.display()))?;
    if workspace::stored_meta(&client, "revision")?.is_some() {
        anyhow::ensure!(
            workspace::stored_meta(&client, "indexed_commit")?.is_some(),
            "revision at {} is not fully indexed; run `{} index` again",
            db_path.display(),
            spec::launcher()
        );
    }
    Ok(LocalGraphReader {
        client,
        _locks: vec![lock],
    })
}

fn open_workspace_graph() -> Result<LocalGraphReader> {
    let manifest = workspace::Workspace::open_default()?.manifest_path();
    if !manifest.exists() {
        anyhow::bail!(
            "no local graph found. Run `{} index` first.",
            spec::launcher()
        );
    }
    let mut reader = LocalGraphReader {
        client: DuckDbClient::open_in_memory()?,
        _locks: Vec::new(),
    };
    let client = &reader.client;
    let paths = {
        let _lock = workspace::lock_database(&manifest)?;
        client.execute(
            &format!(
                "ATTACH {} AS orbit_manifest (READ_ONLY)",
                sql_lit(&manifest.to_string_lossy())
            ),
            &[],
        )?;
        client.execute("CREATE TEMP TABLE _orbit_manifest AS SELECT * FROM orbit_manifest.main._orbit_manifest", &[])?;
        client.execute("DETACH orbit_manifest", &[])?;
        string_column(
            &client.query_arrow("SELECT DISTINCT database_path FROM _orbit_manifest WHERE status IN ('indexed', 'indexing') AND database_path IS NOT NULL ORDER BY database_path")?,
            "database_path",
        )
    };
    let mut catalogs = Vec::new();
    for (index, path) in paths.iter().enumerate() {
        reader
            ._locks
            .push(workspace::read_lock_database(Path::new(path))?);
        let catalog = format!("revision_{index}");
        client.execute(
            &format!("ATTACH {} AS {catalog} (READ_ONLY)", sql_lit(path)),
            &[],
        )?;
        let complete = client.query_arrow(&format!(
            "SELECT count(*) AS n FROM {catalog}.main._orbit_meta WHERE key = 'indexed_commit'"
        ))?;
        anyhow::ensure!(
            duckdb_client::scalar_i64(&complete) == 1,
            "revision at {path} is not fully indexed; run `{} index` again",
            spec::launcher()
        );
        catalogs.push(catalog);
    }
    let tables = string_column(
        &client.query_arrow("SELECT table_name FROM information_schema.tables WHERE table_catalog = 'revision_0' AND table_schema = 'main' AND table_name LIKE 'gl_%' ORDER BY table_name")?,
        "table_name",
    );
    for table in tables {
        let table = quote_ident(&table);
        let sources = catalogs
            .iter()
            .map(|catalog| format!("SELECT * FROM {catalog}.main.{table}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        client.execute(&format!("CREATE TEMP VIEW {table} AS {sources}"), &[])?;
    }
    client.execute(
        "CREATE MACRO def_name(fqn) AS regexp_replace(fqn, '^.*[:.#/]', '')",
        &[],
    )?;
    Ok(reader)
}

pub fn query(client: &DuckDbClient, sql: &str) -> Result<Vec<RecordBatch>> {
    client.query_arrow(sql).with_context(|| {
        let preview: String = sql.chars().take(120).collect();
        let suffix = if sql.chars().count() > 120 { "…" } else { "" };
        format!("query failed: {preview}{suffix}")
    })
}

pub fn run(
    query_arg: Option<String>,
    file: Option<PathBuf>,
    format: Format,
    db: Option<PathBuf>,
    repo: Option<PathBuf>,
    _all: bool,
) -> Result<()> {
    let sql = resolve_sql(query_arg.as_deref(), file)?;
    let sql = sql.trim();
    if sql.is_empty() {
        anyhow::bail!("empty SQL query");
    }

    let db = if db.is_none()
        && let Some(repo) = &repo
    {
        let git = workspace::git_info(&workspace::git_toplevel(repo)?)?;
        Some(workspace::resolve_db_path(None, &git)?)
    } else {
        db
    };
    let client = open_graph(db)?;
    if let Some(repo) = repo {
        scope_to_checkout(&client, &repo)?;
    }
    let batches = query(&client, sql)?;

    let stdout = std::io::stdout().lock();
    sql_format::write(stdout, format, &batches)
}

fn scope_to_checkout(client: &DuckDbClient, repo: &Path) -> Result<()> {
    let git = match workspace::git_toplevel(repo).and_then(|top| workspace::git_info(&top)) {
        Ok(git) => git,
        Err(_) => {
            eprintln!(
                "note: {} is not inside a git checkout; querying every indexed commit (as with --all)",
                repo.display()
            );
            return Ok(());
        }
    };
    if !scope_tables(client, git.project_id, &git.commit_sha)? {
        eprintln!(
            "note: commit {} of {} is not indexed; querying every indexed commit (as with --all). \
             `{} index {}` indexes it.",
            git.short_sha(),
            git.repo_path.display(),
            spec::launcher(),
            git.repo_path.display()
        );
    }
    Ok(())
}

fn scope_tables(client: &DuckDbClient, project_id: i64, commit_sha: &str) -> Result<bool> {
    let catalog = string_column(
        &client.query_arrow("SELECT current_database() AS catalog")?,
        "catalog",
    )
    .pop()
    .context("DuckDB reported no current database")?;
    let tables = client.query_arrow_json(
        "SELECT table_name,
                bool_or(column_name = 'source_id') AND bool_or(column_name = 'target_id') AS is_edge
         FROM information_schema.columns
         WHERE table_catalog = ?1 AND table_schema = 'main'
         GROUP BY table_name
         HAVING (bool_or(column_name = 'id')
                 AND bool_or(column_name = 'project_id')
                 AND bool_or(column_name = 'commit_sha'))
             OR (bool_or(column_name = 'source_id') AND bool_or(column_name = 'target_id'))
         ORDER BY table_name",
        &[catalog.clone().into()],
    )?;
    let names = string_column(&tables, "table_name");
    let is_edge = bool_column(&tables, "is_edge");
    let (edge_tables, node_tables): (Vec<_>, Vec<_>) = names
        .iter()
        .zip(&is_edge)
        .partition(|(_, is_edge)| **is_edge);
    if node_tables.is_empty() {
        return Ok(false);
    }

    let sha = sql_lit(commit_sha);
    let base = |table: &str| format!("{}.main.{}", quote_ident(&catalog), quote_ident(table));
    let indexed = node_tables
        .iter()
        .map(|(table, _)| {
            format!(
                "EXISTS (SELECT 1 FROM {} WHERE project_id = {project_id} AND commit_sha = {sha})",
                base(table)
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    if !bool_column(
        &client.query_arrow(&format!("SELECT ({indexed}) AS indexed"))?,
        "indexed",
    )
    .first()
    .copied()
    .unwrap_or(false)
    {
        return Ok(false);
    }

    for (table, _) in &node_tables {
        client.execute(
            &format!(
                "CREATE TEMP VIEW {} AS SELECT * FROM {} WHERE project_id = {project_id} AND commit_sha = {sha}",
                quote_ident(table),
                base(table)
            ),
            &[],
        )?;
    }
    let node_ids = node_tables
        .iter()
        .map(|(table, _)| format!("SELECT id FROM {}", quote_ident(table)))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    for (table, _) in edge_tables {
        client.execute(
            &format!(
                "CREATE TEMP VIEW {} AS SELECT DISTINCT * FROM {} WHERE source_id IN ({node_ids})",
                quote_ident(table),
                base(table)
            ),
            &[],
        )?;
    }
    Ok(true)
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn resolve_sql(query: Option<&str>, file: Option<PathBuf>) -> Result<String> {
    match (query, file) {
        (Some("-"), _) => read_stdin(),
        (Some(q), None) => Ok(q.to_string()),
        (None, Some(path)) => std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display())),
        (None, None) => {
            if std::io::stdin().is_terminal() {
                anyhow::bail!(
                    "no SQL provided. Pass a query, --file PATH, or pipe via stdin (`-`)."
                );
            }
            read_stdin()
        }
        (Some(_), Some(_)) => unreachable!("clap conflicts_with"),
    }
}

fn read_stdin() -> Result<String> {
    let mut buf = String::new();
    std::io::stdin()
        .read_to_string(&mut buf)
        .context("failed to read SQL from stdin")?;
    Ok(buf)
}

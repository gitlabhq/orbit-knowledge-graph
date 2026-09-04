use crate::commands::setup::spec;
use crate::sql_format::{self, Format};
use crate::workspace;
use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use duckdb_client::search::DEF_DOC_PREFIX;
use duckdb_client::{DuckDbClient, bool_column, sql_lit, string_column};
use std::io::{IsTerminal, Read};
use std::path::{Path, PathBuf};

const META_TABLE: &str = "_orbit_meta";

pub fn schema_introspection_sql() -> String {
    format!(
        "SELECT table_name, column_name, data_type \
         FROM information_schema.columns \
         WHERE table_schema = 'main' \
           AND table_name NOT LIKE {} \
           AND table_name <> {} \
         ORDER BY table_name, ordinal_position",
        sql_lit(&format!("{DEF_DOC_PREFIX}%")),
        sql_lit(META_TABLE)
    )
}

pub fn open_graph(db: Option<PathBuf>) -> Result<DuckDbClient> {
    let db_path = workspace::resolve_db_path(db)?;
    if !db_path.exists() {
        anyhow::bail!(
            "no local graph found at {}. Index a repository first \
             (`{} index <path>`, or the `index` MCP tool).",
            db_path.display(),
            spec::launcher()
        );
    }
    DuckDbClient::open_read_only(&db_path)
        .with_context(|| format!("failed to open {}", db_path.display()))
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
    all: bool,
) -> Result<()> {
    let sql = resolve_sql(query_arg.as_deref(), file)?;
    let sql = sql.trim();
    if sql.is_empty() {
        anyhow::bail!("empty SQL query");
    }

    let client = open_graph(db)?;
    if !all {
        scope_to_checkout(&client, repo.as_deref().unwrap_or(Path::new(".")))?;
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
    let tables_where = |edge: bool| -> Vec<&str> {
        names
            .iter()
            .zip(&is_edge)
            .filter(|(_, is_edge)| **is_edge == edge)
            .map(|(name, _)| name.as_str())
            .collect()
    };
    let node_tables = tables_where(false);
    let edge_tables = tables_where(true);
    if node_tables.is_empty() {
        return Ok(false);
    }

    let sha = sql_lit(commit_sha);
    let base = |table: &str| format!("{}.main.{}", quote_ident(&catalog), quote_ident(table));
    let indexed = node_tables
        .iter()
        .map(|table| {
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

    for table in &node_tables {
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
        .map(|table| format!("SELECT id FROM {}", quote_ident(table)))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    for table in edge_tables {
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

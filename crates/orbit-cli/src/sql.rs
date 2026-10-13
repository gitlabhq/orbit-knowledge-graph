use crate::database::{GraphReader, schema};
use crate::sql_format::{self, Format};
use crate::workspace;
use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use duckdb_client::{DuckDbClient, sql_lit};
use std::io::{IsTerminal, Read};
use std::path::PathBuf;

const META_TABLE: &str = "_orbit_meta";

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
) -> Result<()> {
    let sql = resolve_sql(query_arg.as_deref(), file)?;
    let sql = sql.trim();
    if sql.is_empty() {
        anyhow::bail!("empty SQL query");
    }

    let git = repo
        .map(|repo| workspace::git_info(&workspace::git_toplevel(&repo)?))
        .transpose()?;
    let db = match (db, &git) {
        (None, Some(git)) => Some(workspace::resolve_db_path(None, git)?),
        (db, _) => db,
    };
    let client = GraphReader::open(db)?;
    if let Some(git) = git {
        let expected = format!("{}:{}", git.project_id, git.commit_sha);
        anyhow::ensure!(
            schema::stored_meta(&client, "revision")?.as_deref() == Some(&expected),
            "selected database does not match the checkout revision; choose a matching --db or omit --repo"
        );
    }
    let batches = query(&client, sql)?;

    let stdout = std::io::stdout().lock();
    sql_format::write(stdout, format, &batches)
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

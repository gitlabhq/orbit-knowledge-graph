use crate::sql_format::{self, Format};
use crate::workspace;
use anyhow::{Context, Result};
use std::io::{IsTerminal, Read};
use std::path::PathBuf;

pub fn run(
    query: Option<String>,
    file: Option<PathBuf>,
    format: Format,
    db: Option<PathBuf>,
) -> Result<()> {
    let sql = resolve_sql(query.as_deref(), file)?;
    let sql = sql.trim();
    if sql.is_empty() {
        anyhow::bail!("empty SQL query");
    }

    let db_path = workspace::resolve_db_path(db)?;
    if !db_path.exists() {
        anyhow::bail!(
            "no local graph found at {}. Run `orbit index` first.",
            db_path.display()
        );
    }

    let client = duckdb_client::DuckDbClient::open_read_only(&db_path)
        .with_context(|| format!("failed to open {}", db_path.display()))?;
    let batches = client.query_arrow(sql).with_context(|| {
        let preview: String = sql.chars().take(120).collect();
        let suffix = if sql.chars().count() > 120 { "…" } else { "" };
        format!("query failed: {preview}{suffix}")
    })?;

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

use crate::commands::setup::spec;
use crate::sql_format::{self, Format};
use crate::workspace;
use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use duckdb_client::{DuckDbClient, sql_lit, string_column};
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

    let git = repo
        .map(|repo| workspace::git_info(&workspace::git_toplevel(&repo)?))
        .transpose()?;
    let db = match (db, &git) {
        (None, Some(git)) => Some(workspace::resolve_db_path(None, git)?),
        (db, _) => db,
    };
    let client = open_graph(db)?;
    if let Some(git) = git {
        let expected = format!("{}:{}", git.project_id, git.commit_sha);
        anyhow::ensure!(
            workspace::stored_meta(&client, "revision")?.as_deref() == Some(&expected),
            "selected database does not match the checkout revision; choose a matching --db or omit --repo"
        );
    }
    let batches = query(&client, sql)?;

    let stdout = std::io::stdout().lock();
    sql_format::write(stdout, format, &batches)
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

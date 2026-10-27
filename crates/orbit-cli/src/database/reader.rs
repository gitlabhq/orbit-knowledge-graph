use super::{lock_database, read_lock_database, schema};
use crate::commands::setup::spec;
use crate::workspace;
use anyhow::{Context, Result};
use duckdb_client::{DuckDbClient, sql_lit, string_column};
use std::path::{Path, PathBuf};

/// An index began this revision database and never finished it, such as after a killed index.
#[derive(Debug)]
pub struct IncompleteRevision(PathBuf);

impl std::fmt::Display for IncompleteRevision {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "revision at {} is not fully indexed; run `{} workspace index` again",
            self.0.display(),
            spec::launcher()
        )
    }
}

impl std::error::Error for IncompleteRevision {}

pub struct GraphReader {
    client: DuckDbClient,
    _locks: Vec<std::fs::File>,
}

impl std::ops::Deref for GraphReader {
    type Target = DuckDbClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl GraphReader {
    /// False when no complete revision could be attached, such as after a killed first index.
    pub fn has_revisions(&self) -> bool {
        !self._locks.is_empty()
    }

    pub fn open(db: Option<PathBuf>) -> Result<Self> {
        let Some(db_path) = db else {
            return Self::open_workspace();
        };
        let Some(lock) = read_lock_database(&db_path)? else {
            anyhow::bail!(
                "no local graph found at {}. Index a repository first \
             (`{} workspace index` inside it, or the `index` MCP tool).",
                db_path.display(),
                spec::launcher()
            );
        };
        let client = DuckDbClient::open_read_only(&db_path)
            .with_context(|| format!("failed to open {}", db_path.display()))?;
        if schema::stored_meta(&client, "revision")?.is_some()
            && schema::stored_meta(&client, "indexed_commit")?.is_none()
        {
            return Err(IncompleteRevision(db_path).into());
        }
        Ok(Self {
            client,
            _locks: vec![lock],
        })
    }

    fn open_workspace() -> Result<Self> {
        let manifest = workspace::Workspace::open_default()?.manifest_path();
        if !manifest.exists() {
            anyhow::bail!(
                "no local graph found. Run `{} workspace index` first.",
                spec::launcher()
            );
        }
        let mut reader = Self {
            client: DuckDbClient::open_in_memory()?,
            _locks: Vec::new(),
        };
        let client = &reader.client;
        let paths = {
            let _lock = lock_database(&manifest)?;
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
        let mut tables = std::collections::BTreeMap::<String, Vec<String>>::new();
        for (index, path) in paths.iter().enumerate() {
            let Some(lock) = read_lock_database(Path::new(path))? else {
                eprintln!("orbit: skipping missing revision {path}; reindex its checkout");
                continue;
            };
            reader._locks.push(lock);
            let catalog = format!("revision_{index}");
            client.execute(
                &format!("ATTACH {} AS {catalog} (READ_ONLY)", sql_lit(path)),
                &[],
            )?;
            let metadata = client.query_arrow_json(
            "SELECT count(*) AS n FROM information_schema.tables WHERE table_catalog = ?1 AND table_name = '_orbit_meta' AND table_schema = 'main'",
            &[catalog.clone().into()],
        )?;
            let complete = duckdb_client::scalar_i64(&metadata) == 1
            && duckdb_client::scalar_i64(&client.query_arrow(&format!(
                "SELECT count(*) AS n FROM {catalog}.main._orbit_meta WHERE key = 'indexed_commit'"
            ))?) == 1;
            if !complete {
                eprintln!("orbit: skipping incomplete revision {path}; reindex its checkout");
                client.execute(&format!("DETACH {catalog}"), &[])?;
                reader._locks.pop();
                continue;
            }
            for table in string_column(
            &client.query_arrow_json(
                "SELECT table_name FROM information_schema.tables WHERE table_catalog = ?1 AND table_schema = 'main' AND starts_with(table_name, 'gl_') ORDER BY table_name",
                &[catalog.clone().into()],
            )?,
            "table_name",
        ) {
            tables.entry(table).or_default().push(catalog.clone());
        }
        }
        for (table, catalogs) in tables {
            let table = quote_ident(&table);
            let sources = catalogs
                .iter()
                .map(|catalog| format!("SELECT * FROM {catalog}.main.{table}"))
                .collect::<Vec<_>>()
                .join(" UNION ALL BY NAME ");
            client.execute(&format!("CREATE TEMP VIEW {table} AS {sources}"), &[])?;
        }
        client.execute(
            "CREATE MACRO def_name(fqn) AS regexp_replace(fqn, '^.*[:.#/]', '')",
            &[],
        )?;
        Ok(reader)
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

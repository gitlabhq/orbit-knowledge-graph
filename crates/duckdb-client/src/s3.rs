//! A DuckDB connection that reads its tables from S3: as Parquet files under a key prefix, or
//! through a DuckLake catalog whose data sits there. Each table is a view of its current rows.

use orbit_server_config::{DuckDbConnection, DuckDbLocation};
use query_data_model::implementations::relational::Table;
use query_data_model::{Catalog, DuckDb};

use crate::client::{DuckDbClient, base_config};
use crate::error::{DuckDbError, Result};
use crate::helpers::sql_lit;

const LAKE: &str = "orbit_ducklake";
const RANK: &str = "orbit_rank";

/// How one table's rows become current: its writer's lifecycle columns and the identity key.
struct Lifecycle<'a> {
    key: &'a [String],
    version: &'a str,
    deleted: &'a str,
}

impl DuckDbClient {
    /// Opens connection `name` with every table of `catalog` as a view of its current rows.
    pub fn open_s3(
        name: &str,
        connection: &DuckDbConnection,
        catalog: &Catalog<DuckDb>,
    ) -> Result<Self> {
        connection
            .validate(name)
            .map_err(|error| DuckDbError::Connection(error.to_string()))?;
        let setup = Setup { name, connection };
        let conn = duckdb::Connection::open_in_memory_with_flags(base_config(
            duckdb::AccessMode::ReadWrite,
        )?)?;
        let directory = connection.extension_directory.display().to_string();
        setup.run(
            &conn,
            "load the httpfs and ducklake extensions",
            &format!(
                "SET extension_directories = [{}]; LOAD httpfs; LOAD ducklake;",
                sql_lit(&directory)
            ),
        )?;
        setup.run(&conn, "create the S3 secret", &setup.secret())?;
        let root = connection.location.root();
        if let DuckDbLocation::Ducklake { catalog: path, .. } = &connection.location {
            let attach = format!(
                "ATTACH {} AS {LAKE} (READ_ONLY, DATA_PATH {})",
                sql_lit(&format!("ducklake:{}", path.display())),
                sql_lit(&root)
            );
            setup.run(&conn, "attach the DuckLake catalog", &attach)?;
        }
        for table in catalog.tables() {
            let lifecycle = lifecycle(name, catalog, table)?;
            let rows = match &connection.location {
                DuckDbLocation::Parquet { .. } => format!(
                    "read_parquet({})",
                    sql_lit(&format!("{root}{}/*.parquet", table.name))
                ),
                DuckDbLocation::Ducklake { .. } => format!("{LAKE}.{}", ident(&table.name)),
            };
            let view = current_rows_view(table, &rows, &lifecycle);
            setup.run(&conn, &format!("resolve table {}", table.name), &view)?;
            check_columns(name, &conn, table, &lifecycle)?;
        }
        Ok(Self { conn })
    }
}

struct Setup<'a> {
    name: &'a str,
    connection: &'a DuckDbConnection,
}

impl Setup<'_> {
    fn secret(&self) -> String {
        let (s3, credentials) = (&self.connection.s3, &self.connection.credentials);
        let token = credentials
            .session_token
            .as_deref()
            .map(|token| format!(", SESSION_TOKEN {}", sql_lit(token)))
            .unwrap_or_default();
        format!(
            "CREATE SECRET orbit_s3 (TYPE s3, PROVIDER config, KEY_ID {}, SECRET {}{token}, \
             REGION {}, ENDPOINT {}, URL_STYLE {}, USE_SSL {}, SCOPE {})",
            sql_lit(&credentials.access_key_id),
            sql_lit(&credentials.secret_access_key),
            sql_lit(&s3.region),
            sql_lit(&s3.endpoint),
            sql_lit(s3.url_style.as_str()),
            s3.use_ssl,
            sql_lit(&format!("s3://{}/", self.connection.location.bucket())),
        )
    }

    /// Runs one setup statement; its error never carries a secret the statement held.
    fn run(&self, conn: &duckdb::Connection, step: &str, sql: &str) -> Result<()> {
        conn.execute_batch(sql).map_err(|error| {
            let credentials = &self.connection.credentials;
            let secrets = std::iter::once(credentials.secret_access_key.as_str())
                .chain(credentials.session_token.as_deref());
            let message = secrets.fold(error.to_string(), |message, secret| {
                message.replace(secret, "[redacted]")
            });
            DuckDbError::Connection(format!("connection {} cannot {step}: {message}", self.name))
        })
    }
}

/// The writer of the table's entity lands its rows in this very table on this connection, and
/// declares the columns that tell its current rows apart.
fn lifecycle<'c>(name: &str, catalog: &'c Catalog<DuckDb>, table: &Table) -> Result<Lifecycle<'c>> {
    let unresolved = |problem: String| {
        DuckDbError::Connection(format!(
            "connection {name} cannot read table {}: {problem}",
            table.name
        ))
    };
    let entity = table
        .entity
        .ok_or_else(|| unresolved("an S3 connection reads entity tables only".into()))?;
    let (_, source) = catalog
        .sources(entity)
        .find(|(_, source)| source.table.as_deref() == Some(table.name.as_str()))
        .ok_or_else(|| unresolved("no store on the connection binds it".into()))?;
    let sources = catalog.model().sources();
    let writers: Vec<_> = sources.entity_pipelines(entity).collect();
    let [pipeline] = writers.as_slice() else {
        return Err(unresolved(format!(
            "its entity has {} writer pipelines, not one",
            writers.len()
        )));
    };
    let writer = sources.data_source(pipeline);
    if writer.connection != name || pipeline.tables != [table.name.clone()] {
        return Err(unresolved(format!(
            "its writer {} lands rows in {:?} on connection {}",
            pipeline.name, pipeline.tables, writer.connection
        )));
    }
    Ok(Lifecycle {
        key: &source.key,
        version: &writer.version,
        deleted: &writer.deleted,
    })
}

fn current_rows_view(table: &Table, rows: &str, lifecycle: &Lifecycle<'_>) -> String {
    let key: Vec<String> = lifecycle.key.iter().map(|column| ident(column)).collect();
    format!(
        "CREATE VIEW {table} AS SELECT * EXCLUDE ({RANK}) FROM (\
         SELECT *, row_number() OVER (PARTITION BY {key} ORDER BY {version} DESC) AS {RANK} \
         FROM {rows}) WHERE {RANK} = 1 AND NOT {deleted}",
        table = ident(&table.name),
        key = key.join(", "),
        version = ident(lifecycle.version),
        deleted = ident(lifecycle.deleted),
    )
}

/// Every declared column has its declared type, and the lifecycle columns exist; nothing casts.
fn check_columns(
    name: &str,
    conn: &duckdb::Connection,
    table: &Table,
    lifecycle: &Lifecycle<'_>,
) -> Result<()> {
    let mut statement = conn.prepare(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?",
    )?;
    let found: Vec<(String, String)> = statement
        .query_map([&table.name], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<std::result::Result<_, _>>()?;
    let type_of = |column: &str| {
        found
            .iter()
            .find(|(name, _)| name == column)
            .map(|(_, data_type)| data_type.as_str())
    };
    let declared = table
        .columns
        .iter()
        .map(|column| (column.name.as_str(), Some(column.storage_type.as_str())))
        .chain([
            (lifecycle.version, None),
            (lifecycle.deleted, Some("BOOLEAN")),
        ]);
    for (column, expected) in declared {
        let problem = match (type_of(column), expected) {
            (None, _) => format!("has no column {column}"),
            (Some(found), Some(expected)) if !found.eq_ignore_ascii_case(expected) => {
                format!("stores {column} as {found}, but the package declares {expected}")
            }
            _ => continue,
        };
        return Err(DuckDbError::Connection(format!(
            "connection {name} table {} {problem}",
            table.name
        )));
    }
    Ok(())
}

fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use orbit_server_config::{S3Credentials, S3Endpoint, S3UrlStyle};
    use query_data_model::Model;

    use super::*;

    fn connection(extension_directory: &std::path::Path) -> DuckDbConnection {
        DuckDbConnection {
            extension_directory: extension_directory.to_path_buf(),
            s3: S3Endpoint {
                endpoint: "127.0.0.1:9".into(),
                region: "us-east-1".into(),
                url_style: S3UrlStyle::Path,
                use_ssl: false,
            },
            credentials: S3Credentials {
                access_key_id: "test-key".into(),
                secret_access_key: "test-secret-0042".into(),
                session_token: None,
            },
            location: DuckDbLocation::Parquet {
                bucket: "orbit-lake".into(),
                prefix: "jira/".into(),
            },
        }
    }

    fn jira() -> Catalog<DuckDb> {
        let model: Arc<Model> = query_data_model::testkit::with_demo()
            .deployment("demo")
            .unwrap();
        model
            .physical::<DuckDb>(model.names().connection("jira_lake").unwrap())
            .unwrap()
    }

    #[test]
    fn a_missing_extension_fails_the_open_instead_of_installing() {
        let empty = tempfile::tempdir().unwrap();
        let error = DuckDbClient::open_s3("jira_lake", &connection(empty.path()), &jira())
            .err()
            .unwrap()
            .to_string();
        assert!(
            error.starts_with(
                "connection jira_lake cannot load the httpfs and ducklake extensions: "
            ),
            "{error}"
        );
        assert_eq!(std::fs::read_dir(empty.path()).unwrap().count(), 0);
    }

    #[test]
    fn a_setup_error_never_carries_the_secret() {
        let empty = tempfile::tempdir().unwrap();
        let connection = connection(empty.path());
        let setup = Setup {
            name: "jira_lake",
            connection: &connection,
        };
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let error = setup
            .run(
                &conn,
                "echo the secret",
                "SELECT * FROM \"test-secret-0042\"",
            )
            .unwrap_err()
            .to_string();
        assert!(
            error.starts_with("connection jira_lake cannot echo the secret: "),
            "{error}"
        );
        assert!(error.contains("[redacted]"), "{error}");
        assert!(!error.contains("test-secret-0042"), "{error}");
    }

    #[test]
    fn the_secret_is_scoped_to_the_bucket_and_never_persists() {
        let empty = tempfile::tempdir().unwrap();
        let connection = connection(empty.path());
        let secret = Setup {
            name: "jira_lake",
            connection: &connection,
        }
        .secret();
        assert_eq!(
            secret,
            "CREATE SECRET orbit_s3 (TYPE s3, PROVIDER config, KEY_ID 'test-key', SECRET 'test-secret-0042', \
             REGION 'us-east-1', ENDPOINT '127.0.0.1:9', URL_STYLE 'path', USE_SSL false, SCOPE 's3://orbit-lake/')"
        );
    }

    #[test]
    fn each_table_reads_the_latest_live_version_of_each_identity() {
        let catalog = jira();
        let table = catalog
            .tables()
            .find(|table| table.name == "jira_user")
            .unwrap();
        let lifecycle = lifecycle("jira_lake", &catalog, table).unwrap();
        assert_eq!(
            current_rows_view(
                table,
                "read_parquet('s3://orbit-lake/jira/jira_user/*.parquet')",
                &lifecycle
            ),
            "CREATE VIEW \"jira_user\" AS SELECT * EXCLUDE (orbit_rank) FROM (SELECT *, row_number() OVER \
             (PARTITION BY \"account_id\" ORDER BY \"_siphon_replicated_at\" DESC) AS orbit_rank FROM \
             read_parquet('s3://orbit-lake/jira/jira_user/*.parquet')) WHERE orbit_rank = 1 AND NOT \"_siphon_deleted\""
        );
    }

    #[test]
    fn a_table_whose_writer_lands_elsewhere_fails() {
        let catalog = jira();
        let table = catalog
            .tables()
            .find(|table| table.name == "jira_user")
            .unwrap();
        let error = match lifecycle("another_lake", &catalog, table) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("the table resolved on another connection"),
        };
        assert_eq!(
            error,
            "connection another_lake cannot read table jira_user: its writer JiraUser lands rows in [\"jira_user\"] on connection jira_lake"
        );
    }
}

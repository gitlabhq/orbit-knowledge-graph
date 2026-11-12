//! A DuckDB connection's tables landed on a local S3 server the way the Data Extraction Service
//! lands them: one Parquet file per table, every row with its writer's lifecycle columns, either
//! under a key prefix or registered in a DuckLake catalog.

use orbit_server_config::{DuckDbConnection, DuckDbLocation};
use query_data_model::{Catalog, DuckDb};

use crate::s3::{BUCKET, S3Lake};
use crate::scenario::Seed;
use crate::scenario::seed::{group_by_columns, values_insert};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Parquet,
    Ducklake,
}

/// The seeded lake and the connection config that reads it; the DuckLake catalog lives in a
/// directory this owns.
pub struct Lake {
    pub s3: S3Lake,
    pub connection: DuckDbConnection,
    _scratch: tempfile::TempDir,
}

impl Lake {
    /// Lands `seed`'s rows for every table of `catalog`; `storage_type` gives each column's
    /// Parquet type from its table, name and declared type, so a test can store another one.
    pub async fn seed(
        mode: Mode,
        catalog: &Catalog<DuckDb>,
        seed: &Seed,
        storage_type: impl Fn(&str, &str, &str) -> String,
    ) -> Self {
        let s3 = S3Lake::start().await;
        let scratch = tempfile::tempdir().expect("a scratch directory");
        let prefix = match mode {
            Mode::Parquet => "lake/",
            Mode::Ducklake => "ducklake/",
        };
        let seeder = s3.seeder();
        let ducklake = scratch.path().join("catalog.ducklake");
        if mode == Mode::Ducklake {
            seeder
                .execute_batch(&format!(
                    "ATTACH 'ducklake:{}' AS lake (DATA_PATH 's3://{BUCKET}/{prefix}')",
                    ducklake.display()
                ))
                .expect("create the DuckLake catalog");
        }
        let sources = catalog.model().sources();
        for table in catalog.tables() {
            let name = &table.name;
            let entity = table.entity.expect("a lake table holds an entity");
            let writer = sources
                .entity_pipelines(entity)
                .map(|pipeline| sources.data_source(pipeline))
                .next()
                .unwrap_or_else(|| panic!("{name} has no writer pipeline"));
            let columns: Vec<String> = table
                .columns
                .iter()
                .map(|column| {
                    let stored = storage_type(name, &column.name, &column.storage_type);
                    format!("\"{}\" {stored}", column.name)
                })
                .chain([
                    format!("\"{}\" TIMESTAMP", writer.version),
                    format!("\"{}\" TIMESTAMP", writer.watermark),
                    format!("\"{}\" BOOLEAN", writer.deleted),
                ])
                .collect();
            seeder
                .execute_batch(&format!("CREATE TABLE \"{name}\" ({})", columns.join(", ")))
                .unwrap_or_else(|error| panic!("create {name}: {error}"));
            let rows = seed.get(name).cloned().unwrap_or_default();
            for (column_names, group) in group_by_columns(rows) {
                let quoted: Vec<String> = column_names.iter().map(|c| format!("\"{c}\"")).collect();
                seeder
                    .execute_batch(&values_insert(name, &quoted, &column_names, &group, name))
                    .unwrap_or_else(|error| panic!("seed {name}: {error}"));
            }
            let local = scratch.path().join(format!("{name}.parquet"));
            seeder
                .execute_batch(&format!(
                    "COPY \"{name}\" TO '{}' (FORMAT parquet)",
                    local.display()
                ))
                .unwrap_or_else(|error| panic!("write {name}: {error}"));
            let key = match mode {
                Mode::Parquet => format!("{prefix}{name}/part-0.parquet"),
                Mode::Ducklake => format!("{prefix}main/{name}/part-0.parquet"),
            };
            s3.put(&key, std::fs::read(&local).expect("read the Parquet file"))
                .await;
            if mode == Mode::Ducklake {
                let file = format!("s3://{BUCKET}/{key}");
                seeder
                    .execute_batch(&format!(
                        "CREATE TABLE lake.\"{name}\" AS SELECT * FROM read_parquet('{file}') LIMIT 0;
                         CALL ducklake_add_data_files('lake', '{name}', '{file}');"
                    ))
                    .unwrap_or_else(|error| panic!("register {name} in the lake: {error}"));
            }
        }
        drop(seeder);
        let location = match mode {
            Mode::Parquet => DuckDbLocation::Parquet {
                bucket: BUCKET.into(),
                prefix: prefix.into(),
            },
            Mode::Ducklake => DuckDbLocation::Ducklake {
                catalog: ducklake,
                bucket: BUCKET.into(),
                prefix: prefix.into(),
            },
        };
        let connection = s3.connection(location);
        Self {
            s3,
            connection,
            _scratch: scratch,
        }
    }
}

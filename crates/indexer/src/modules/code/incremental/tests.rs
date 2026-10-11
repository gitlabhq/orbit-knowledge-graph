use super::super::repository::service::test_utils::build_tar_gz;
use super::*;
use orbit_utils::vfs::Decision;

use super::super::{
    arrow_converter::IndexerEnvelope, config::CodeTableNames, pipeline::IndexingReport,
};
use arrow::record_batch::RecordBatch;
use code_graph::v2::{OnBatch, SinkError};
use orbit_utils::arrow::ArrowUtils;
use std::sync::Mutex;

struct IndexFixture {
    ontology: Arc<ontology::Ontology>,
    tables: Arc<CodeTableNames>,
    version: chrono::DateTime<chrono::Utc>,
}

impl IndexFixture {
    fn new() -> Self {
        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        Self {
            tables: Arc::new(CodeTableNames::from_ontology(&ontology).unwrap()),
            ontology,
            version: chrono::Utc::now(),
        }
    }

    async fn run(
        &self,
        files: &[(&str, &[u8])],
        options: Options,
        sink: Arc<OnBatch>,
    ) -> Result<IndexingReport, SinkError> {
        let repository = load(stream(build_tar_gz(files)), &config()).await.unwrap();
        run(
            repository,
            options,
            IndexerEnvelope::new(
                orbit_utils::traversal_path::TraversalPath::new_unchecked("1/42/"),
                7,
                "main".into(),
                "abc123".into(),
                self.version,
            ),
            self.ontology.clone(),
            self.tables.clone(),
            sink,
        )
    }
}

fn collected_rows(
    batches: &[(String, RecordBatch)],
    table: &str,
) -> Vec<std::collections::HashMap<String, orbit_utils::arrow::ColumnValue>> {
    batches
        .iter()
        .filter(|(name, _)| name == table)
        .flat_map(|(_, batch)| {
            let columns: Vec<_> = batch
                .schema()
                .fields()
                .iter()
                .zip(batch.columns())
                .map(|(field, column)| {
                    let column = match field.data_type() {
                        arrow::datatypes::DataType::Dictionary(_, value) => {
                            arrow::compute::cast(column, value).unwrap()
                        }
                        _ => column.clone(),
                    };
                    (field.name().clone(), column)
                })
                .collect();
            (0..batch.num_rows()).map(move |row| {
                columns
                    .iter()
                    .map(|(name, column)| {
                        (
                            name.clone(),
                            ArrowUtils::extract_value(column.as_ref(), row),
                        )
                    })
                    .collect()
            })
        })
        .collect()
}

#[tokio::test]
async fn archive_indexes_multiple_families_into_server_batches() {
    let files: &[(&str, &[u8])] = &[
        ("root/src/main.py", b"from helper import greet\ngreet()\n"),
        ("root/src/helper.py", b"def greet():\n    pass\n"),
        ("root/web/a.ts", b"export function hello() {}\n"),
        ("root/web/b.js", b"import { hello } from './a';\nhello();\n"),
        ("root/README.md", b"readme"),
        ("root/logo.png", b"image"),
    ];
    let fixture = IndexFixture::new();
    let tables = &fixture.tables;
    let batches = Arc::new(Mutex::new(Vec::new()));
    let capture = batches.clone();
    let sink = Arc::new(move |table: &str, batch: RecordBatch| {
        capture.lock().unwrap().push((table.to_owned(), batch));
        Ok(())
    });
    let result = fixture.run(files, options(), sink).await.unwrap();
    assert_eq!(result.stats.files_indexed, 6);
    assert_eq!(result.stats.files_parsed, 4);
    assert_eq!(result.stats.files_skipped, 1);
    let batches = batches.lock().unwrap();
    let files = collected_rows(&batches, &tables.file);
    let paths: std::collections::HashSet<_> = files
        .iter()
        .map(|row| row["path"].coerce::<String>().unwrap())
        .collect();
    assert_eq!(files.len(), paths.len());
    let definitions = collected_rows(&batches, &tables.definition);
    for name in ["greet", "hello"] {
        assert!(
            definitions
                .iter()
                .any(|row| row["name"].coerce::<String>().as_deref() == Some(name)),
            "missing {name}"
        );
    }
    assert_eq!(collected_rows(&batches, &tables.branch).len(), 1);
    let directories = collected_rows(&batches, &tables.directory);
    assert_eq!(
        directories.len(),
        directories
            .iter()
            .map(|row| row["id"].coerce::<i64>().unwrap())
            .collect::<std::collections::HashSet<_>>()
            .len()
    );
    for (_, batch) in batches.iter().filter(|(_, batch)| batch.num_rows() > 0) {
        for values in collected_rows(&[("batch".into(), batch.clone())], "batch") {
            assert_eq!(
                values["traversal_path"].coerce::<String>().as_deref(),
                Some("1/42/")
            );
            assert_eq!(values["_deleted"].coerce::<bool>(), Some(false));
        }
        let versions = batch
            .column_by_name("_version")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert!(
            versions
                .values()
                .iter()
                .all(|value| *value == fixture.version.timestamp_micros())
        );
    }
    let edges: Vec<_> = tables
        .edge_table_names()
        .iter()
        .flat_map(|table| collected_rows(&batches, table))
        .collect();
    assert!(
        edges
            .iter()
            .any(|row| row["relationship_kind"].coerce::<String>().as_deref() == Some("CALLS"))
    );
    assert_eq!(
        edges
            .iter()
            .filter(
                |row| row["relationship_kind"].coerce::<String>().as_deref() == Some("ON_BRANCH")
            )
            .count(),
        files.len() + directories.len()
    );
}

#[tokio::test]
async fn cancelled_run_emits_no_batches() {
    let mut options = options();
    options.cancelled = Arc::new(|| true);
    let result = IndexFixture::new()
        .run(
            &[("root/file.py", b"def f(): pass")],
            options,
            Arc::new(|_, _| panic!("cancelled run must not emit")),
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn repositories_without_source_files_still_emit_inventory() {
    let fixture = IndexFixture::new();
    let batches = Arc::new(Mutex::new(Vec::new()));
    let capture = batches.clone();
    let result = fixture
        .run(
            &[
                ("root/docs/README.md", b"readme"),
                ("root/image.png", b"image"),
            ],
            options(),
            Arc::new(move |table, batch| {
                capture.lock().unwrap().push((table.into(), batch));
                Ok(())
            }),
        )
        .await
        .unwrap();
    assert_eq!(result.stats.files_parsed, 0);
    assert_eq!(result.stats.files_indexed, 2);
    assert_eq!(result.stats.files_skipped, 1);
    assert_eq!(result.stats.definitions_count, 0);
    assert_eq!(
        collected_rows(&batches.lock().unwrap(), &fixture.tables.file).len(),
        2
    );
}

#[tokio::test]
async fn sink_failure_aborts_the_run() {
    let error = IndexFixture::new()
        .run(
            &[("root/file.py", b"def f(): pass")],
            options(),
            Arc::new(|_, _| Err(code_graph::v2::SinkError("write failed".into()))),
        )
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("write failed"));
}

fn stream(bytes: Vec<u8>) -> ByteStream {
    Box::pin(futures::stream::once(async move { Ok(bytes.into()) }))
}

fn config() -> CodeIndexingPipelineConfig {
    orbit_server_config::AppConfig::embedded_defaults()
        .engine
        .handlers
        .code_indexing_task
        .pipeline
}

fn options() -> Options {
    Options {
        limits: code_graph_incremental::Limits::UNLIMITED,
        worker_threads: 2,
        cancelled: Arc::new(|| false),
        family_finished: Arc::new(|| {}),
    }
}

#[tokio::test]
async fn loading_retains_filtered_metadata_and_spills_at_the_memory_limit() {
    let archive = build_tar_gz(&[
        ("root/src/main.rs", b"fn main() {}"),
        ("root/Cargo.toml", b"[package]"),
        ("root/image.png", b"image"),
        ("root/binary.rs", b"\0binary"),
    ]);
    let mut config = config();
    for budget in [0, 1024] {
        config.source_memory_budget_bytes = budget;
        let repository = load(stream(archive.clone()), &config).await.unwrap();
        assert_eq!(repository.usage().files, 4);
        assert_eq!(&*repository.read("src/main.rs").unwrap(), b"fn main() {}");
        assert_eq!(&*repository.read("Cargo.toml").unwrap(), b"[package]");
        for (path, reason) in [("image.png", "excluded_extension"), ("binary.rs", "binary")] {
            assert_eq!(
                repository.stat(path).unwrap().decision,
                Some(Decision::List(reason))
            );
            assert_eq!(
                repository.read(path).unwrap_err().kind(),
                std::io::ErrorKind::Unsupported
            );
        }
        assert_eq!(repository.usage().resident == 0, budget == 0);
        assert_eq!(repository.usage().spilled > 0, budget == 0);
    }
}

#[tokio::test]
async fn loading_distinguishes_empty_archives_and_total_byte_limits() {
    let mut config = config();
    config.max_total_bytes = 4;
    assert!(matches!(
        load(stream(Vec::new()), &config).await,
        Err(RepositoryCacheError::EmptyArchive)
    ));
    let archive = build_tar_gz(&[("root/file.png", b"oversize")]);
    assert!(matches!(
        load(stream(archive), &config).await,
        Err(RepositoryCacheError::RepositoryTooLarge)
    ));
}

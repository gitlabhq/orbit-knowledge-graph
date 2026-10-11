use super::super::repository::service::test_utils::build_tar_gz;
use super::*;
use orbit_utils::vfs::Decision;

use arrow::record_batch::RecordBatch;
use orbit_utils::arrow::ArrowUtils;
use std::sync::Mutex;

fn collected_rows(
    batches: &[(String, RecordBatch)],
    table: &str,
) -> Vec<std::collections::HashMap<String, orbit_utils::arrow::ColumnValue>> {
    batches
        .iter()
        .filter(|(name, _)| name == table)
        .flat_map(|(_, batch)| {
            let columns = batch
                .schema()
                .fields()
                .iter()
                .zip(batch.columns())
                .map(|(field, column)| {
                    if matches!(
                        field.data_type(),
                        arrow::datatypes::DataType::Dictionary(_, _)
                    ) {
                        arrow::compute::cast(column, &arrow::datatypes::DataType::Utf8).unwrap()
                    } else {
                        column.clone()
                    }
                })
                .collect::<Vec<_>>();
            let fields = batch
                .schema()
                .fields()
                .iter()
                .zip(&columns)
                .map(|(field, column)| {
                    arrow::datatypes::Field::new(
                        field.name(),
                        column.data_type().clone(),
                        field.is_nullable(),
                    )
                })
                .collect::<Vec<_>>();
            let plain =
                RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)
                    .unwrap();
            (0..plain.num_rows())
                .map(|row| ArrowUtils::extract_row(&plain, row))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn archive_indexes_multiple_families_into_server_batches() {
    use super::super::arrow_converter::IndexerEnvelope;
    use super::super::config::CodeTableNames;
    use orbit_utils::traversal_path::TraversalPath;

    let archive = build_tar_gz(&[
        ("root/src/main.py", b"from helper import greet\ngreet()\n"),
        ("root/src/helper.py", b"def greet():\n    pass\n"),
        ("root/web/a.ts", b"export function hello() {}\n"),
        ("root/web/b.js", b"import { hello } from './a';\nhello();\n"),
        ("root/README.md", b"readme"),
        ("root/logo.png", b"image"),
    ]);
    let repository = load(stream(archive), &config()).await.unwrap();
    let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
    let tables = Arc::new(CodeTableNames::from_ontology(&ontology).unwrap());
    let batches = Arc::new(Mutex::new(Vec::new()));
    let capture = batches.clone();
    let sink = Arc::new(move |table: &str, batch: RecordBatch| {
        capture.lock().unwrap().push((table.to_owned(), batch));
        Ok(())
    });
    let version = chrono::Utc::now();
    let result = run(
        repository,
        options(),
        IndexerEnvelope::new(
            TraversalPath::new_unchecked("1/42/"),
            7,
            "main".into(),
            "abc123".into(),
            version,
        ),
        ontology,
        tables.clone(),
        sink,
    )
    .unwrap();
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
                .all(|value| *value == version.timestamp_micros())
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
    let repository = load(
        stream(build_tar_gz(&[("root/file.py", b"def f(): pass")])),
        &config(),
    )
    .await
    .unwrap();
    let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
    let tables = Arc::new(super::super::config::CodeTableNames::from_ontology(&ontology).unwrap());
    let mut options = options();
    options.cancelled = Arc::new(|| true);
    let result = run(
        repository,
        options,
        super::super::arrow_converter::IndexerEnvelope::new(
            orbit_utils::traversal_path::TraversalPath::new_unchecked("1/42/"),
            7,
            "main".into(),
            "abc".into(),
            chrono::Utc::now(),
        ),
        ontology,
        tables,
        Arc::new(|_, _| panic!("cancelled run must not emit")),
    );
    assert!(result.is_err());
}

#[tokio::test]
async fn repositories_without_source_files_still_emit_inventory() {
    let repository = load(
        stream(build_tar_gz(&[
            ("root/docs/README.md", b"readme"),
            ("root/image.png", b"image"),
        ])),
        &config(),
    )
    .await
    .unwrap();
    let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
    let tables = Arc::new(super::super::config::CodeTableNames::from_ontology(&ontology).unwrap());
    let batches = Arc::new(Mutex::new(Vec::new()));
    let capture = batches.clone();
    let result = run(
        repository,
        options(),
        super::super::arrow_converter::IndexerEnvelope::new(
            orbit_utils::traversal_path::TraversalPath::new_unchecked("1/42/"),
            7,
            "main".into(),
            "abc".into(),
            chrono::Utc::now(),
        ),
        ontology,
        tables.clone(),
        Arc::new(move |table, batch| {
            capture.lock().unwrap().push((table.into(), batch));
            Ok(())
        }),
    )
    .unwrap();
    assert_eq!(result.stats.files_parsed, 0);
    assert_eq!(result.stats.files_indexed, 2);
    assert_eq!(result.stats.files_skipped, 1);
    assert_eq!(result.stats.definitions_count, 0);
    assert_eq!(
        collected_rows(&batches.lock().unwrap(), &tables.file).len(),
        2
    );
}

#[tokio::test]
async fn sink_failure_aborts_the_run() {
    let repository = load(
        stream(build_tar_gz(&[("root/file.py", b"def f(): pass")])),
        &config(),
    )
    .await
    .unwrap();
    let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
    let tables = Arc::new(super::super::config::CodeTableNames::from_ontology(&ontology).unwrap());
    let error = run(
        repository,
        options(),
        super::super::arrow_converter::IndexerEnvelope::new(
            orbit_utils::traversal_path::TraversalPath::new_unchecked("1/42/"),
            7,
            "main".into(),
            "abc".into(),
            chrono::Utc::now(),
        ),
        ontology,
        tables,
        Arc::new(|_, _| Err(code_graph::v2::SinkError("write failed".into()))),
    )
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
        max_files: 0,
        worker_threads: 2,
        parse_timeout: None,
        rewrite_timeout: None,
        link_timeout: None,
        resolve_timeout: None,
        cross_file_timeout: None,
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

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

struct Timings {
    languages: Vec<(String, u64)>,
    slowest_paths: Vec<String>,
    errors: usize,
    skipped_paths: Vec<String>,
}

async fn timings_for(files: &[(&str, &[u8])], limits: code_graph_incremental::Limits) -> Timings {
    let mut options = options();
    options.limits = limits;
    let sink = Arc::new(|_: &str, _: RecordBatch| Ok(()));
    let report = IndexFixture::new().run(files, options, sink).await.unwrap();
    let mut languages: Vec<_> = report
        .stats
        .language_timings
        .iter()
        .map(|timing| (timing.language.clone(), timing.file_count as u64))
        .collect();
    languages.sort();
    let mut slowest_paths: Vec<_> = report
        .stats
        .slowest_files
        .iter()
        .map(|file| file.path.clone())
        .collect();
    slowest_paths.sort();
    let mut skipped_paths: Vec<_> = report
        .skipped
        .iter()
        .map(|file| file.path.clone())
        .collect();
    skipped_paths.sort();
    Timings {
        languages,
        slowest_paths,
        errors: report.errors.len(),
        skipped_paths,
    }
}

#[tokio::test]
async fn language_timings_skip_excluded_and_binary_files() {
    let files: &[(&str, &[u8])] = &[
        ("root/a.ts", b"export function hello() {}\n"),
        ("root/vendor.min.js", b"function v(){}\n"),
        ("root/blob.js", b"function b(){}\0\0\0"),
    ];
    let timings = timings_for(files, code_graph_incremental::Limits::UNLIMITED).await;
    assert_eq!(timings.languages, [("javascript".to_owned(), 1)]);
    assert_eq!(timings.slowest_paths, ["a.ts"]);
}

#[tokio::test]
async fn language_timings_exclude_files_beyond_max_files() {
    let files: &[(&str, &[u8])] = &[
        ("root/a.js", b"function a() {}\n"),
        ("root/b.js", b"function b() {}\n"),
        ("root/c.js", b"function c() {}\n"),
    ];
    let limits = code_graph_incremental::Limits {
        max_files: 1,
        ..code_graph_incremental::Limits::UNLIMITED
    };
    let timings = timings_for(files, limits).await;
    assert_eq!(timings.languages, [("javascript".to_owned(), 1)]);
    assert_eq!(timings.slowest_paths.len(), 1);
    assert!(["a.js", "b.js", "c.js"].contains(&timings.slowest_paths[0].as_str()));
}

#[tokio::test]
async fn language_timings_exclude_files_killed_by_the_parse_budget() {
    let files: &[(&str, &[u8])] = &[
        ("root/a.js", b"function a() {}\n"),
        ("root/b.js", b"function b() {}\n"),
    ];
    let limits = code_graph_incremental::Limits {
        file_parse_ms: 0,
        ..code_graph_incremental::Limits::UNLIMITED
    };
    let timings = timings_for(files, limits).await;
    assert_eq!(timings.errors, 0);
    assert_eq!(timings.skipped_paths, ["a.js", "b.js"]);
    assert!(timings.languages.is_empty());
    assert!(timings.slowest_paths.is_empty());
}

#[tokio::test]
async fn language_timings_keep_files_killed_by_the_resolve_budget() {
    let files: &[(&str, &[u8])] = &[
        ("root/a.js", b"function a() {}\n"),
        ("root/b.js", b"function b() {}\n"),
    ];
    let limits = code_graph_incremental::Limits {
        file_resolve_ms: 0,
        ..code_graph_incremental::Limits::UNLIMITED
    };
    let timings = timings_for(files, limits).await;
    assert_eq!(timings.errors, 0);
    assert_eq!(timings.skipped_paths, ["a.js", "b.js"]);
    assert_eq!(timings.languages, [("javascript".to_owned(), 2)]);
    assert_eq!(timings.slowest_paths, ["a.js", "b.js"]);
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
async fn tables_over_the_batch_byte_budget_index_the_same_rows() {
    let files: &[(&str, &[u8])] = &[
        ("root/src/main.py", b"from helper import greet\ngreet()\n"),
        ("root/src/helper.py", b"def greet():\n    pass\n"),
        ("root/web/a.ts", b"export function hello() {}\n"),
        ("root/web/b.js", b"import { hello } from './a';\nhello();\n"),
    ];
    let fixture = IndexFixture::new();
    let index = async |batch_bytes| {
        let batches = Arc::new(Mutex::new(Vec::new()));
        let capture = batches.clone();
        let mut options = options();
        options.batch_bytes = batch_bytes;
        let report = fixture
            .run(
                files,
                options,
                Arc::new(move |table: &str, batch: RecordBatch| {
                    capture.lock().unwrap().push((table.to_owned(), batch));
                    Ok(())
                }),
            )
            .await
            .unwrap();
        let batches = std::mem::take(&mut *batches.lock().unwrap());
        (report.stats, batches)
    };
    let rows = |batches: &[(String, RecordBatch)]| {
        let mut tables = std::collections::BTreeMap::<_, Vec<_>>::new();
        for (table, _) in batches {
            tables.entry(table.clone()).or_insert_with(|| {
                let mut rows: Vec<_> = collected_rows(batches, table)
                    .into_iter()
                    .map(|row| {
                        format!(
                            "{:?}",
                            row.into_iter()
                                .collect::<std::collections::BTreeMap<_, _>>()
                        )
                    })
                    .collect();
                rows.sort();
                rows
            });
        }
        tables
    };
    let (whole_stats, whole) = index(orbit_utils::arrow::BATCH_BYTES).await;
    let (split_stats, split) = index(1).await;
    assert!(split.len() > whole.len());
    assert_eq!(rows(&split), rows(&whole));
    assert_eq!(split_stats.files_indexed, whole_stats.files_indexed);
    assert_eq!(split_stats.definitions_count, whole_stats.definitions_count);
    assert_eq!(split_stats.edges_count, whole_stats.edges_count);
}

#[tokio::test]
async fn a_family_panic_is_a_fatal_error() {
    let report = IndexFixture::new()
        .run(
            &[("root/file.py", b"def f(): pass")],
            options(),
            Arc::new(|_, _| panic!("sink panicked")),
        )
        .await
        .unwrap();
    assert_eq!(report.errors.len(), 1);
    assert!(report.errors[0].fatal);
    assert!(
        report.errors[0].to_string().contains("panic"),
        "{}",
        report.errors[0]
    );
}

#[tokio::test]
async fn cancelled_run_emits_no_batches_and_no_errors() {
    let mut options = options();
    options.cancelled = Arc::new(|| true);
    let report = IndexFixture::new()
        .run(
            &[("root/file.py", b"def f(): pass")],
            options,
            Arc::new(|_, _| panic!("cancelled run must not emit")),
        )
        .await
        .unwrap();
    assert!(report.errors.is_empty());
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

#[tokio::test]
async fn an_ontology_the_export_cannot_map_is_a_fatal_error() {
    let fixture = IndexFixture {
        ontology: Arc::new(ontology::Ontology::new()),
        ..IndexFixture::new()
    };
    let report = fixture
        .run(
            &[("root/file.py", b"def f(): pass")],
            options(),
            Arc::new(|_, _| Ok(())),
        )
        .await
        .unwrap();
    assert_eq!(report.errors.len(), 1);
    assert!(report.errors[0].fatal);
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
        worker_threads: config().worker_threads,
        concurrent_families: code_graph::v2::PipelineConfig::default().concurrent_languages(),
        small_family_files: config().small_repo_max_files,
        batch_bytes: orbit_utils::arrow::BATCH_BYTES,
        cancelled: Arc::new(|| false),
        family_finished: Arc::new(|| {}),
    }
}

#[test]
fn engine_limits_come_from_the_server_config_and_zero_means_no_limit() {
    let configured = config();
    let mapped = limits(&configured);
    assert_eq!(
        [
            mapped.file_parse_ms,
            mapped.file_rewrite_ms,
            mapped.file_link_ms,
            mapped.file_resolve_ms,
            mapped.cross_file_resolve_ms,
        ],
        [
            configured.per_file_parse_timeout_ms,
            configured.per_file_walk_timeout_ms,
            configured.per_file_ssa_timeout_ms,
            configured.per_file_timeout_ms,
            configured.cross_file_resolve_timeout_ms,
        ]
    );
    assert_eq!(mapped.max_files, configured.max_files);

    let unlimited = limits(&CodeIndexingPipelineConfig {
        max_files: 0,
        per_file_timeout_ms: 0,
        per_file_parse_timeout_ms: 0,
        per_file_walk_timeout_ms: 0,
        per_file_ssa_timeout_ms: 0,
        cross_file_resolve_timeout_ms: 0,
        ..configured
    });
    assert_eq!(unlimited, code_graph_incremental::Limits::UNLIMITED);
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

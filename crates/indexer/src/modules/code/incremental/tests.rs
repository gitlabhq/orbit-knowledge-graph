use super::super::repository::service::test_utils::build_tar_gz;
use super::*;
use orbit_utils::vfs::Decision;

use super::super::{
    arrow_converter::IndexerEnvelope, config::CodeTableNames, pipeline::IndexingReport,
};
use arrow::record_batch::RecordBatch;
use code_graph::v2::{OnBatch, SinkError};
use orbit_arrow::ArrowUtils;
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
        self.run_with(&config(), files, options, sink).await
    }

    async fn run_with(
        &self,
        config: &CodeIndexingPipelineConfig,
        files: &[(&str, &[u8])],
        options: Options,
        sink: Arc<OnBatch>,
    ) -> Result<IndexingReport, SinkError> {
        let repository = load(stream(build_tar_gz(files)), config).await.unwrap();
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
) -> Vec<std::collections::HashMap<String, orbit_arrow::ColumnValue>> {
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
    skipped_reasons: Vec<&'static str>,
    skipped_families: Vec<Option<String>>,
    limited_reasons: Vec<&'static str>,
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
    let mut skipped_reasons: Vec<_> = report
        .skipped
        .iter()
        .map(|file| file.kind.as_metric_label())
        .collect();
    skipped_reasons.sort();
    let mut limited_reasons: Vec<_> = report
        .limited
        .iter()
        .map(|file| file.kind.as_metric_label())
        .collect();
    limited_reasons.sort();
    Timings {
        languages,
        slowest_paths,
        errors: report.errors.len(),
        skipped_paths,
        skipped_reasons,
        skipped_families: report
            .skipped
            .iter()
            .map(|file| file.family.clone())
            .collect(),
        limited_reasons,
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
async fn files_nested_past_the_default_depth_limit_are_skipped() {
    let nested = format!(
        "function nested(c) {{\n{}{}}}\n",
        "if (c) {\n".repeat(5_000),
        "}\n".repeat(5_000)
    );
    let files: &[(&str, &[u8])] = &[
        ("root/nested.js", nested.as_bytes()),
        ("root/flat.js", b"function flat() {}\n"),
    ];
    let limits = code_graph_incremental::Limits {
        max_tree_depth: limits(&config()).max_tree_depth,
        ..code_graph_incremental::Limits::UNLIMITED
    };
    let timings = timings_for(files, limits).await;
    assert_eq!(timings.skipped_paths, ["nested.js"]);
    assert_eq!(timings.skipped_reasons, ["deeply_nested"]);
    assert_eq!(timings.languages, [("javascript".to_owned(), 1)]);
}

#[tokio::test]
async fn a_long_operator_chain_is_indexed_under_the_default_depth_limit() {
    let terms = vec!["c"; 5_000].join(" +\n");
    let source = format!("function t() {{}}\nfunction f(c) {{\nt();\nreturn {terms};\n}}\n");
    let files: &[(&str, &[u8])] = &[("root/sum.js", source.as_bytes())];
    let limits = code_graph_incremental::Limits {
        max_tree_depth: limits(&config()).max_tree_depth,
        ..code_graph_incremental::Limits::UNLIMITED
    };
    let timings = timings_for(files, limits).await;
    assert!(timings.skipped_paths.is_empty());
    assert_eq!(timings.languages, [("javascript".to_owned(), 1)]);
}

#[tokio::test]
async fn a_long_or_pattern_is_indexed_under_the_default_depth_limit() {
    let codes: Vec<_> = (0..2_000).map(|i| format!("Code.E{i}")).collect();
    let source = format!(
        "enum Code {{ {} }}\nclass Facts {{\n    static bool IsWarning(Code code) => code switch {{\n        {} => true,\n        _ => false,\n    }};\n}}\n",
        (0..2_000)
            .map(|i| format!("E{i}"))
            .collect::<Vec<_>>()
            .join(", "),
        codes.join("\n            or ")
    );
    let files: &[(&str, &[u8])] = &[("root/Facts.cs", source.as_bytes())];
    let limits = code_graph_incremental::Limits {
        max_tree_depth: limits(&config()).max_tree_depth,
        ..code_graph_incremental::Limits::UNLIMITED
    };
    let timings = timings_for(files, limits).await;
    assert!(timings.skipped_paths.is_empty());
    assert_eq!(timings.languages, [("csharp".to_owned(), 1)]);
}

#[tokio::test]
async fn a_deep_relative_import_stays_inside_the_rewrite_budget() {
    let deep = format!(
        "from {} import x\n\ndef f():\n    pass\n",
        ".".repeat(60_000)
    );
    let files: &[(&str, &[u8])] = &[("root/main.py", deep.as_bytes())];
    let limits = code_graph_incremental::Limits {
        file_rewrite_ms: 2_000,
        ..code_graph_incremental::Limits::UNLIMITED
    };
    let timings = timings_for(files, limits).await;
    assert_eq!(timings.skipped_paths, Vec::<String>::new());
    assert_eq!(timings.languages, [("python".to_owned(), 1)]);
}

fn one_package(files: usize, path: &str, header: &str, class: &str) -> Vec<(String, Vec<u8>)> {
    (0..files)
        .map(|file| {
            let classes: String = (0..30)
                .map(|n| class.replace("NAME", &format!("C{file}_{n}")))
                .collect();
            let path = format!("root/{}", path.replace("{}", &file.to_string()));
            (path, format!("{header}\n{classes}").into_bytes())
        })
        .collect()
}

fn as_files(files: &[(String, Vec<u8>)]) -> Vec<(&str, &[u8])> {
    files
        .iter()
        .map(|(path, body)| (path.as_str(), body.as_slice()))
        .collect()
}

#[tokio::test]
async fn the_cross_file_budget_stops_package_reexports() {
    let files = one_package(
        600,
        "Big/F{}.cs",
        "namespace Big;",
        "public class NAME {}\n",
    );
    let limits = code_graph_incremental::Limits {
        cross_file_resolve_ms: 1,
        ..code_graph_incremental::Limits::UNLIMITED
    };
    let timings = timings_for(&as_files(&files), limits).await;
    assert_eq!(timings.skipped_paths, [""]);
    assert_eq!(timings.skipped_reasons, ["timeout_cross_file"]);
    assert_eq!(timings.skipped_families, [Some("csharp".to_owned())]);
    assert_eq!(timings.languages, [("csharp".to_owned(), 600)]);
}

#[tokio::test]
async fn cross_file_resolution_stops_at_the_name_limit() {
    let files = one_package(
        200,
        "Big/F{}.cs",
        "namespace Big;",
        "public class NAME {}\n",
    );
    let limits = code_graph_incremental::Limits {
        max_cross_file_names: 10_000,
        ..code_graph_incremental::Limits::UNLIMITED
    };
    let timings = timings_for(&as_files(&files), limits).await;
    assert_eq!(timings.skipped_paths, [""]);
    assert_eq!(timings.skipped_reasons, ["too_many_names"]);
    assert_eq!(timings.skipped_families, [Some("csharp".to_owned())]);
    assert_eq!(timings.languages, [("csharp".to_owned(), 200)]);
}

#[tokio::test]
async fn a_package_shared_by_many_files_resolves_inside_the_cross_file_budget() {
    let limits = code_graph_incremental::Limits {
        cross_file_resolve_ms: 60_000,
        ..code_graph_incremental::Limits::UNLIMITED
    };
    for (path, header, class) in [
        ("Big/F{}.cs", "namespace Big;", "public class NAME {}\n"),
        ("big/F{}.scala", "package big", "class NAME\n"),
    ] {
        let files = one_package(300, path, header, class);
        let timings = timings_for(&as_files(&files), limits).await;
        assert_eq!(timings.skipped_paths, Vec::<String>::new(), "{path}");
        assert_eq!(timings.languages.len(), 1, "{path}");
        assert_eq!(timings.languages[0].1, 300, "{path}");
    }
}

#[tokio::test]
async fn a_read_inside_deeply_nested_loops_links_inside_the_budget() {
    let nested = format!(
        "function t() {{}}\nfunction m() {{\n{}t();\n{}}}\n",
        "while (true) {\n".repeat(2_000),
        "}\n".repeat(2_000)
    );
    let files: &[(&str, &[u8])] = &[("root/loops.js", nested.as_bytes())];
    let limits = code_graph_incremental::Limits {
        file_link_ms: 5_000,
        ..code_graph_incremental::Limits::UNLIMITED
    };
    let timings = timings_for(files, limits).await;
    assert_eq!(timings.skipped_paths, Vec::<String>::new());
}

#[tokio::test]
async fn a_value_copied_into_its_own_fields_links_without_a_copy_limit() {
    let copies = "a.x = a;\na.y = a;\n".repeat(5_000);
    let body = format!("fn target() {{}}\nfn self_fields(mut a: Node) {{\n{copies}target();\n}}\n");
    let files: &[(&str, &[u8])] = &[
        ("root/src/lib.rs", body.as_bytes()),
        ("root/src/flat.rs", b"fn flat() {}\n"),
    ];
    let timings = timings_for(files, code_graph_incremental::Limits::UNLIMITED).await;
    assert!(timings.skipped_paths.is_empty());
    assert!(timings.limited_reasons.is_empty());
}

#[tokio::test]
async fn a_file_with_more_edges_than_the_limit_is_skipped() {
    let functions: String = (0..50).map(|i| format!("function a{i}() {{}}\n")).collect();
    let assigns: String = (0..50)
        .map(|i| format!("if (c) {{ f = a{i}; }}\n"))
        .collect();
    let calls = "f();\n".repeat(50);
    let wide = format!("{functions}function m(c) {{\nlet f = a0;\n{assigns}{calls}}}\n");
    let files: &[(&str, &[u8])] = &[
        ("root/wide.js", wide.as_bytes()),
        ("root/flat.js", b"function flat() {}\n"),
    ];
    let limits = code_graph_incremental::Limits {
        max_file_edges: 1_000,
        ..code_graph_incremental::Limits::UNLIMITED
    };
    assert_eq!(timings_for(files, limits).await.skipped_paths, ["wide.js"]);
    let unlimited = timings_for(files, code_graph_incremental::Limits::UNLIMITED).await;
    assert!(unlimited.skipped_paths.is_empty());
    let timings = timings_for(files, limits).await;
    assert_eq!(timings.skipped_reasons, ["too_many_edges"]);
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
async fn a_file_under_thousands_of_directories_is_indexed() {
    let deep = format!("root/{}notes.txt", "d/".repeat(3_000));
    let files: &[(&str, &[u8])] = &[(deep.as_str(), b"notes")];
    let fixture = IndexFixture::new();
    let batches = Arc::new(Mutex::new(Vec::new()));
    let capture = batches.clone();
    let sink = Arc::new(move |table: &str, batch: RecordBatch| {
        capture.lock().unwrap().push((table.to_owned(), batch));
        Ok(())
    });
    fixture.run(files, options(), sink).await.unwrap();
    let batches = batches.lock().unwrap();
    let paths: Vec<_> = collected_rows(&batches, &fixture.tables.file)
        .iter()
        .map(|row| row["path"].coerce::<String>().unwrap())
        .collect();
    assert_eq!(paths, [&deep["root/".len()..]]);
    assert_eq!(
        collected_rows(&batches, &fixture.tables.directory).len(),
        3_001
    );
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
    let (whole_stats, whole) = index(orbit_arrow::BATCH_BYTES).await;
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
async fn a_panic_without_a_parsed_family_is_a_fatal_error() {
    let report = IndexFixture::new()
        .run(
            &[("root/notes.txt", b"notes")],
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
        batch_bytes: orbit_arrow::BATCH_BYTES,
        cancelled: Arc::new(|| false),
        family_finished: Arc::new(|| {}),
        phase_started: Arc::new(|_, _, _| {}),
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
    assert_eq!(mapped.max_tree_depth, configured.max_tree_depth);
    assert_eq!(mapped.max_cross_file_names, configured.max_cross_file_names);
    assert_eq!(mapped.max_manifest_bytes, configured.max_manifest_bytes);
    assert_eq!(mapped.max_file_edges, configured.max_file_edges);

    let unlimited = limits(&CodeIndexingPipelineConfig {
        max_files: 0,
        per_file_timeout_ms: 0,
        per_file_parse_timeout_ms: 0,
        per_file_walk_timeout_ms: 0,
        per_file_ssa_timeout_ms: 0,
        cross_file_resolve_timeout_ms: 0,
        max_tree_depth: 0,
        max_cross_file_names: 0,
        max_manifest_bytes: 0,
        max_file_edges: 0,
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

#[tokio::test]
async fn loading_rejects_repositories_with_more_kept_files_than_the_limit() {
    let mut config = config();
    config.max_repository_files = 3;
    let files: &[(&str, &[u8])] = &[
        ("root/a.txt", b""),
        ("root/b.txt", b""),
        ("root/c.txt", b""),
        ("root/d.txt", b""),
    ];
    assert!(matches!(
        load(stream(build_tar_gz(files)), &config).await,
        Err(RepositoryCacheError::RepositoryTooLarge)
    ));
    config.max_repository_files = 4;
    assert!(load(stream(build_tar_gz(files)), &config).await.is_ok());
}

#[tokio::test]
async fn excluded_files_past_the_file_limit_do_not_fail_the_repository() {
    let mut config = config();
    config.max_repository_files = 3;
    let assets: Vec<(String, &[u8])> = (0..10)
        .map(|i| {
            (
                format!("root/node_modules/pkg/icon{i}.png"),
                &b"\x89PNG"[..],
            )
        })
        .collect();
    let files: Vec<(&str, &[u8])> = [
        ("root/src/a.js", &b"export function a() {}\n"[..]),
        ("root/src/b.js", &b"import { a } from './a';\na();\n"[..]),
    ]
    .into_iter()
    .chain(assets.iter().map(|(path, bytes)| (path.as_str(), *bytes)))
    .collect();
    let repository = load(stream(build_tar_gz(&files)), &config).await.unwrap();
    assert_eq!(repository.usage().files, 5);
    assert_eq!(repository.usage().dropped_listed, 7);
    let logs = Logs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .finish();
    let _logging = tracing::subscriber::set_default(subscriber);
    let sink = Arc::new(|_: &str, _: RecordBatch| Ok(()));
    let report = IndexFixture::new()
        .run_with(&config, &files, options(), sink)
        .await
        .unwrap();
    assert!(report.errors.is_empty());
    assert_eq!(report.stats.files_parsed, 2);
    assert_eq!(report.stats.files_discovered, 12);
    assert_eq!(report.stats.files_skipped, 10);
    let logs = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("WARN"), "{logs}");
    assert!(logs.contains("dropped=7"), "{logs}");
}

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn each_engine_phase_of_a_family_reports_its_start() {
    let started = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = started.clone();
    let mut options = options();
    options.phase_started = Arc::new(move |family, phase, files| {
        seen.lock()
            .unwrap()
            .push(format!("{family} {phase} {files}"));
    });
    let files: &[(&str, &[u8])] = &[
        ("root/a.js", b"export function a() {}\n"),
        ("root/b.js", b"import { a } from './a';\na();\n"),
    ];
    let sink = Arc::new(|_: &str, _: RecordBatch| Ok(()));
    IndexFixture::new().run(files, options, sink).await.unwrap();
    let started = started.lock().unwrap().clone();
    assert_eq!(
        started,
        [
            "js_ts prepare 2",
            "js_ts parse+rewrite+canonicalize+link 2",
            "js_ts insert 2",
            "js_ts resolve 2",
            "js_ts display 2",
            "js_ts export 2",
        ]
    );
}

#[tokio::test]
async fn manifests_past_the_byte_limit_are_reported_and_not_read() {
    let manifest = br#"{ "name": "pkg", "dependencies": { "left-pad": "^1.0.0" } }"#;
    let files: &[(&str, &[u8])] = &[
        ("root/a/package.json", manifest),
        ("root/b/package.json", manifest),
        ("root/c/package.json", manifest),
        ("root/a/index.js", b"export function a() {}\n"),
    ];
    let limits = code_graph_incremental::Limits {
        max_manifest_bytes: 2 * manifest.len() as u64,
        ..code_graph_incremental::Limits::UNLIMITED
    };
    let timings = timings_for(files, limits).await;
    assert!(timings.skipped_paths.is_empty());
    assert_eq!(timings.limited_reasons, ["too_many_manifest_bytes"]);
    assert_eq!(timings.errors, 0);
}

#[tokio::test]
async fn nested_definitions_index_without_a_member_limit() {
    let files: Vec<(String, String)> = (0..26)
        .map(|file| {
            let opens: String = (0..500)
                .map(|level| format!("function f{file}_{level}() {{\n"))
                .collect();
            (
                format!("root/nested{file:02}.js"),
                format!(
                    "function target() {{}}\n{opens}target();\n{}",
                    "}\n".repeat(500)
                ),
            )
        })
        .collect();
    let files: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(path, source)| (path.as_str(), source.as_bytes()))
        .collect();
    let timings = timings_for(&files, code_graph_incremental::Limits::UNLIMITED).await;
    assert!(timings.skipped_paths.is_empty());
    assert_eq!(timings.limited_reasons, vec!["fqn_truncated"; 26]);
    assert_eq!(timings.errors, 0);
}

#[tokio::test]
async fn qualified_names_past_the_segment_limit_keep_their_innermost_segments() {
    let opens: String = (0..70)
        .map(|level| format!("function f{level}() {{\n"))
        .collect();
    let source = format!("{opens}{}", "}\n".repeat(70));
    let files: &[(&str, &[u8])] = &[("root/deep.js", source.as_bytes())];
    let timings = timings_for(files, code_graph_incremental::Limits::UNLIMITED).await;
    assert!(timings.skipped_paths.is_empty());
    assert_eq!(timings.limited_reasons, ["fqn_truncated"]);
    assert_eq!(timings.errors, 0);
}

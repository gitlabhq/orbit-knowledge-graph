use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use chrono::{TimeZone, Utc};
use clickhouse_client::{ClickHouseConfigurationExt, FromArrowColumn};
use indexer::handler::Handler;
use indexer::locking::LockService;
use indexer::modules::code::{
    ClickHouseStaleDataCleaner, CodeCheckpointStore, StaleDataCleaner, config::CodeTableNames,
    project_lock_key,
};
use indexer::testkit::MockLockService;
use indexer::topic::CodeIndexingTaskRequest;
use indexer::types::{Envelope, Event};
use integration_testkit::{assert_edge_count_for_traversal_path, t};
use orbit_arrow::ArrowUtils;

use super::helpers::*;
use orbit_utils::traversal_path::TraversalPath;

#[tokio::test]
async fn incremental_flag_indexes_archives_and_replaces_stale_definitions() {
    enable_incremental("1/2790/");
    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;
    let mock = MockGitlabServer::start().await;
    let project = 2790;
    let traversal = "1/2790/";
    mock.add_project(
        project,
        "main",
        &[
            ("src/main.py", "from helper import greet\ngreet()\n"),
            ("src/helper.py", "def greet():\n    pass\n"),
            ("web/a.ts", "export function hello() {}\n"),
            ("web/b.js", "import { hello } from './a';\nhello();\n"),
            ("README.md", "readme"),
            ("logo.png", "image"),
        ],
    );
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();
    index_code(&handler, &clickhouse, project, "first", 1, traversal).await;
    assert_code_indexed(&clickhouse, project).await;
    for (kind, source, target, count) in [
        ("IN_PROJECT", "Branch", "Project", 1),
        ("CONTAINS", "Project", "Branch", 1),
        ("CONTAINS", "Branch", "Directory", 2),
        ("CONTAINS", "Branch", "File", 2),
        ("ON_BRANCH", "File", "Branch", 6),
    ] {
        assert_edge_count_for_traversal_path(
            &clickhouse,
            kind,
            source,
            target,
            &TraversalPath::new_unchecked(traversal),
            count,
        )
        .await;
    }
    let files = active_file_rows(&clickhouse, project).await;
    assert_eq!(files.len(), 6);
    assert_eq!(
        files
            .iter()
            .map(|row| &row.0)
            .collect::<BTreeSet<_>>()
            .len(),
        6
    );
    assert_eq!(std::fs::read_dir(deps.cache_dir_path()).unwrap().count(), 0);
    assert_contains_edge_between_directory_and_file(&clickhouse, project, "web", "web/a.ts").await;
    mock.add_project(
        project,
        "main",
        &[("replacement.py", "def replacement():\n    pass\n")],
    );
    index_code(&handler, &clickhouse, project, "second", 2, traversal).await;
    let files = active_file_rows(&clickhouse, project).await;
    assert_eq!(
        files.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
        ["replacement.py"]
    );
}

#[tokio::test]
async fn incremental_flag_records_language_timing_metrics() {
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());
    enable_incremental("1/2801/");
    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;
    let mock = MockGitlabServer::start().await;
    mock.add_project(
        2801,
        "main",
        &[
            ("src/main.py", "from helper import greet\ngreet()\n"),
            ("src/helper.py", "def greet():\n    pass\n"),
            ("web/a.ts", "export function hello() {}\n"),
            ("README.md", "readme"),
        ],
    );
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();
    index_code(&handler, &clickhouse, 2801, "first", 1, "1/2801/").await;
    provider.force_flush().unwrap();

    let metrics = exporter.get_finished_metrics().unwrap();
    let by_language = |name: &str| -> BTreeMap<String, u64> {
        metrics
            .iter()
            .flat_map(|resource| resource.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .filter(|metric| metric.name() == name)
            .flat_map(|metric| match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                    .data_points()
                    .map(|point| {
                        let language = point
                            .attributes()
                            .find(|kv| kv.key.as_str() == "language")
                            .map(|kv| kv.value.to_string())
                            .unwrap();
                        (language, point.value())
                    })
                    .collect::<Vec<_>>(),
                _ => Vec::new(),
            })
            .collect()
    };
    assert_eq!(
        by_language("gkg.indexer.code.language.files"),
        BTreeMap::from([("python".to_string(), 2), ("javascript".to_string(), 1)])
    );
    assert_eq!(
        by_language("gkg.indexer.code.language.bytes"),
        BTreeMap::from([("python".to_string(), 55), ("javascript".to_string(), 27)])
    );
}

#[tokio::test]
async fn incremental_flag_counts_listed_entries_dropped_past_the_file_limit() {
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());
    enable_incremental("1/2803/");
    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;
    let mock = MockGitlabServer::start().await;
    let icons: Vec<String> = (0..10).map(|i| format!("assets/icon{i}.png")).collect();
    let mut files = vec![
        ("src/a.js", "export function a() {}\n"),
        ("src/b.js", "import { a } from './a';\na();\n"),
    ];
    files.extend(icons.iter().map(|path| (path.as_str(), "png")));
    mock.add_project(2803, "main", &files);
    let mut config = indexer::testkit::test_pipeline_configuration();
    config.max_repository_files = 3;
    let deps = CodeIndexingDeps::new_with_pipeline_config(&mock, &clickhouse, config);
    let handler = deps.code_indexing_task_handler();
    index_code(&handler, &clickhouse, 2803, "first", 1, "1/2803/").await;
    provider.force_flush().unwrap();

    let metrics = exporter.get_finished_metrics().unwrap();
    let skipped_by_reason: BTreeMap<String, u64> = metrics
        .iter()
        .flat_map(|resource| resource.scope_metrics())
        .flat_map(|scope| scope.metrics())
        .filter(|metric| metric.name() == "gkg.indexer.code.archive.entries.skipped")
        .flat_map(|metric| match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                .data_points()
                .map(|point| {
                    let reason = point
                        .attributes()
                        .find(|kv| kv.key.as_str() == "reason")
                        .map(|kv| kv.value.to_string())
                        .unwrap();
                    (reason, point.value())
                })
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect();
    assert_eq!(
        skipped_by_reason.get("file_limit"),
        Some(&7),
        "listed-only entries past the file limit need a skip reason, got {skipped_by_reason:?}"
    );
}

#[tokio::test]
async fn incremental_flag_dead_letters_an_export_the_ontology_cannot_map() {
    enable_incremental("1/2802/");
    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;
    let mock = MockGitlabServer::start().await;
    mock.add_project(2802, "main", &[("src/main.py", "def f():\n    pass\n")]);
    let deps = CodeIndexingDeps::new_with_ontology(
        &mock,
        &clickhouse,
        indexer::testkit::test_pipeline_configuration(),
        ontology::Ontology::new(),
    );
    let result = deps
        .code_indexing_task_handler()
        .handle(
            handler_context(),
            code_indexing_task_envelope(2802, "first", 1, "1/2802/"),
        )
        .await;
    assert!(
        matches!(
            result,
            Err(indexer::handler::HandlerError::Permanent {
                action: indexer::handler::PermanentAction::DeadLetter,
                ..
            })
        ),
        "got {result:?}"
    );
}

#[tokio::test]
async fn indexes_repository() {
    let project_id: i64 = 1;
    let commit_sha = "abc123";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[(
            "src/Main.java",
            "public class Main {
            public void save() { validate(); }
            public void validate() {}
        }",
        )],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();
    let context = handler_context();
    let traversal_path = "1/1/";
    let envelope = code_indexing_task_envelope(project_id, commit_sha, 1, traversal_path);

    let result = handler.handle(context, envelope).await;
    assert!(result.is_ok(), "handler failed: {:?}", result);

    assert_code_indexed(&clickhouse, project_id).await;
    assert_branch_indexed(&clickhouse, project_id, "main", traversal_path).await;

    // Nested files should not have direct Branch --CONTAINS--> File edges
    assert_edge_count_for_traversal_path(
        &clickhouse,
        "CONTAINS",
        "Branch",
        "File",
        &TraversalPath::new_unchecked(traversal_path),
        0,
    )
    .await;
}

#[tokio::test]
async fn indexes_file_nodes_for_all_archive_files() {
    let project_id: i64 = 8;
    let commit_sha = "allfiles";
    let traversal_path = "1/8/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[
            ("src/main.py", "def hello():\n    return 1\n"),
            ("README.md", "# Project\n"),
            ("config/app.yml", "enabled: true\n"),
            (
                ".gitlab-ci.yml",
                "stages: [test]\nvariables:\n  CARGO_HOME: .cargo\nunit-test:\n  script: [true]\n",
            ),
            ("Dockerfile", "FROM scratch\n"),
            (".gitignore", "target/\n"),
            ("assets/logo.png", "fake png bytes"),
            ("docs/only/README.md", "# Nested\n"),
        ],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();
    index_code(
        &handler,
        &clickhouse,
        project_id,
        commit_sha,
        1,
        traversal_path,
    )
    .await;

    let files = active_file_rows(&clickhouse, project_id).await;
    let paths: Vec<_> = files.iter().map(|row| row.0.clone()).collect();
    let unique_paths: BTreeSet<_> = paths.iter().cloned().collect();
    assert_eq!(
        paths.len(),
        unique_paths.len(),
        "duplicate File nodes: {paths:?}"
    );
    assert_eq!(
        unique_paths,
        BTreeSet::from([
            ".gitignore".to_string(),
            ".gitlab-ci.yml".to_string(),
            "Dockerfile".to_string(),
            "README.md".to_string(),
            "assets/logo.png".to_string(),
            "config/app.yml".to_string(),
            "docs/only/README.md".to_string(),
            "src/main.py".to_string(),
        ])
    );
    assert_eq!(
        language_for(&files, "README.md"),
        Some("unknown"),
        "non-parsable markdown should use the stable unknown language"
    );
    assert_eq!(language_for(&files, "assets/logo.png"), Some("unknown"));
    assert_eq!(language_for(&files, "src/main.py"), Some("python"));
    assert_eq!(language_for(&files, "config/app.yml"), Some("yaml"));
    assert_eq!(language_for(&files, ".gitlab-ci.yml"), Some("yaml"));

    assert_eq!(
        file_size_bytes(&clickhouse, project_id, "src/main.py").await,
        Some(26),
        "size_bytes must record the file's uncompressed byte length"
    );

    assert_directory_is_active(&clickhouse, project_id, "config").await;
    assert_directory_is_active(&clickhouse, project_id, "docs/only").await;
    assert_contains_edge_between_directory_and_file(
        &clickhouse,
        project_id,
        "config",
        "config/app.yml",
    )
    .await;
    assert_contains_edge_between_directory_and_file(
        &clickhouse,
        project_id,
        "docs/only",
        "docs/only/README.md",
    )
    .await;

    assert_no_active_definitions(&clickhouse, project_id, "README.md").await;
    assert_no_active_definitions(&clickhouse, project_id, "assets/logo.png").await;
    assert_no_active_definitions(&clickhouse, project_id, "config/app.yml").await;
    assert_active_definitions(
        &clickhouse,
        project_id,
        ".gitlab-ci.yml",
        &["test", "CARGO_HOME", "unit-test"],
    )
    .await;
    assert_active_definitions(&clickhouse, project_id, "src/main.py", &["hello"]).await;
}

#[tokio::test]
async fn skips_oversized_go_parser_input_and_indexes_repository() {
    let project_id: i64 = 12;
    let commit_sha = "biggo";
    let traversal_path = "1/12/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mut oversized_go = String::from("package generated\n\n");
    oversized_go.push_str("/*\n");
    while oversized_go.len() <= 10 * 1024 * 1024 {
        oversized_go.push_str("generated generated generated generated generated generated\n");
    }
    oversized_go.push_str("*/\n");

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[
            ("proto.gen.go", oversized_go.as_str()),
            ("main.go", "package main\n\nfunc main() {}\n"),
        ],
    );

    let deps = CodeIndexingDeps::new_with_pipeline_config(
        &mock,
        &clickhouse,
        orbit_server_config::CodeIndexingPipelineConfig {
            max_file_size_bytes: u64::MAX,
            ..indexer::testkit::test_pipeline_configuration()
        },
    );
    let handler = deps.code_indexing_task_handler();
    index_code(
        &handler,
        &clickhouse,
        project_id,
        commit_sha,
        1,
        traversal_path,
    )
    .await;

    let files = active_file_rows(&clickhouse, project_id).await;
    let paths: BTreeSet<_> = files.iter().map(|row| row.0.clone()).collect();
    assert_eq!(
        paths,
        BTreeSet::from(["main.go".to_string(), "proto.gen.go".to_string()])
    );
    assert_eq!(language_for(&files, "main.go"), Some("go"));
    assert_eq!(language_for(&files, "proto.gen.go"), Some("go"));

    assert_no_active_definitions(&clickhouse, project_id, "proto.gen.go").await;
    assert_active_definitions(&clickhouse, project_id, "main.go", &["main"]).await;
}

#[tokio::test]
async fn indexes_calls_and_extends_edges() {
    let project_id: i64 = 99;
    let commit_sha = "callsdef";
    let traversal_path = "1/99/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[(
            "src/Zoo.java",
            "public class Animal {
                public void speak() {}
            }
            public class Dog extends Animal {
                public void fetch() { speak(); }
            }",
        )],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();
    index_code(
        &handler,
        &clickhouse,
        project_id,
        commit_sha,
        1,
        traversal_path,
    )
    .await;

    // Both edges land in gl_code_edge per ontology routing
    // (the schema version prefix is added at runtime).
    let ontology = integration_testkit::load_ontology();
    let calls_table = ontology.edge_table_for_relationship("CALLS").unwrap();
    let extends_table = ontology.edge_table_for_relationship("EXTENDS").unwrap();
    assert!(
        calls_table.ends_with("gl_code_edge"),
        "CALLS must route to gl_code_edge, got {calls_table}"
    );
    assert!(
        extends_table.ends_with("gl_code_edge"),
        "EXTENDS must route to gl_code_edge, got {extends_table}"
    );

    let calls = count_active_edges(&clickhouse, project_id, "CALLS").await;
    assert!(
        calls >= 1,
        "expected at least one CALLS edge for fetch()->speak(), got {calls}"
    );

    let extends = count_active_edges(&clickhouse, project_id, "EXTENDS").await;
    assert!(
        extends >= 1,
        "expected at least one EXTENDS edge for Dog extends Animal, got {extends}"
    );

    // Now run a real query through the compiler against the indexed data:
    // CALLS traversal Definition -> Definition. The compiler resolves the
    // CALLS edge through the embedded ontology (proves schema.yaml
    // registration works) and the SQL must hit gl_code_edge.
    let json = format!(
        r#"{{
        "query_type": "traversal",
        "nodes": [
            {{"id": "caller", "entity": "Definition", "filters": {{"project_id": {project_id}}}, "columns": ["name", "fqn"]}},
            {{"id": "callee", "entity": "Definition", "columns": ["name", "fqn"]}}
        ],
        "relationships": [{{"type": "CALLS", "from": "caller", "to": "callee"}}],
        "limit": 25
    }}"#
    );
    let json = json.as_str();
    let security_ctx = compiler::SecurityContext::new(1, vec!["1/".into()])
        .expect("security context")
        .with_role(true, None);
    let compiled = compiler::compile(json, compiler::Frontend::JsonDsl, &ontology, &security_ctx)
        .expect("CALLS query compiles");
    let sql = compiled.base.render();
    assert!(
        sql.contains("gl_code_edge"),
        "compiled CALLS query must scan gl_code_edge (table: {calls_table}): {sql}"
    );
    let rows = clickhouse.query(&sql).await;
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert!(
        total >= 1,
        "compiled CALLS query should return at least one row, got {total}"
    );
}

#[tokio::test]
async fn soft_deletes_stale_code_data_after_reindexing() {
    let project_id: i64 = 2;

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[(
            "src/Main.java",
            "public class Main {
            public void save() { validate(); }
            public void validate() {}
        }",
        )],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();

    index_code(&handler, &clickhouse, project_id, "commit1", 1, "1/2/").await;

    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
    assert_active_definitions(
        &clickhouse,
        project_id,
        "src/Main.java",
        &["Main", "save", "validate"],
    )
    .await;
    assert_eq!(
        count_active_edges(&clickhouse, project_id, "DEFINES").await,
        2,
        "Main→save and Main→validate DEFINES edges should exist"
    );

    mock.replace_archive(
        project_id,
        &[(
            "src/Other.java",
            "public class Other {
            public void run() {}
        }",
        )],
    );
    index_code(&handler, &clickhouse, project_id, "commit2", 2, "1/2/").await;

    assert_file_not_active(&clickhouse, project_id, "src/Main.java").await;
    assert_no_active_definitions(&clickhouse, project_id, "src/Main.java").await;

    assert_file_is_active(&clickhouse, project_id, "src/Other.java").await;
    assert_active_definitions(&clickhouse, project_id, "src/Other.java", &["Other", "run"]).await;

    assert_eq!(
        count_active_edges(&clickhouse, project_id, "DEFINES").await,
        1,
        "only Other→run DEFINES edge should remain active"
    );
}

#[tokio::test]
async fn first_index_skips_stale_cleanup_and_the_next_reindex_sweeps() {
    let project_id: i64 = 13;
    let traversal_path = "1/13/";
    let branch = "main";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        branch,
        &[("src/Main.java", "public class Main { public void v1() {} }")],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();

    insert_stale_canary_file(&clickhouse, project_id, traversal_path, branch).await;
    assert_file_is_active(&clickhouse, project_id, "src/Canary.java").await;

    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit1",
        1,
        traversal_path,
    )
    .await;

    assert_file_is_active(&clickhouse, project_id, "src/Canary.java").await;
    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;

    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit2",
        2,
        traversal_path,
    )
    .await;

    assert_file_not_active(&clickhouse, project_id, "src/Canary.java").await;
    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
}

#[tokio::test]
async fn identical_reindex_keeps_all_content_active() {
    let project_id: i64 = 14;
    let traversal_path = "1/14/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[("src/Main.java", "public class Main { public void v1() {} }")],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();

    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit1",
        1,
        traversal_path,
    )
    .await;
    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit2",
        2,
        traversal_path,
    )
    .await;

    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
    assert_active_definitions(&clickhouse, project_id, "src/Main.java", &["Main", "v1"]).await;
    assert_eq!(
        count_active_edges(&clickhouse, project_id, "DEFINES").await,
        1,
        "the re-index cleanup tombstones must not outrank the freshly written rows"
    );
}

#[tokio::test]
async fn stale_cleanup_tombstones_must_not_outrank_rows_versioned_at_the_watermark() {
    let project_id: i64 = 21;
    let traversal_path = "1/21/";
    let branch = "main";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    insert_file_row_with_version(
        &clickhouse,
        project_id,
        traversal_path,
        branch,
        101,
        "src/Kept.java",
        "2026-01-01 00:00:00.000000",
    )
    .await;
    insert_file_row_with_version(
        &clickhouse,
        project_id,
        traversal_path,
        branch,
        202,
        "src/Stale.java",
        "2026-01-01 00:00:00.000000",
    )
    .await;

    let graph_client = Arc::new(clickhouse.config.build_client("graph"));
    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
    let table_names = CodeTableNames::from_ontology(&ontology).expect("code tables must resolve");
    let cleaner = ClickHouseStaleDataCleaner::new(graph_client, &table_names);

    let watermark = Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap();
    cleaner
        .delete_stale_data(
            &TraversalPath::new_unchecked(traversal_path),
            project_id,
            branch,
            watermark,
        )
        .await
        .expect("stale data cleanup failed");

    insert_file_row_with_version(
        &clickhouse,
        project_id,
        traversal_path,
        branch,
        101,
        "src/Kept.java",
        "2026-01-02 00:00:00.000000",
    )
    .await;

    assert_file_is_active(&clickhouse, project_id, "src/Kept.java").await;
    assert_file_not_active(&clickhouse, project_id, "src/Stale.java").await;
}

#[tokio::test]
async fn reindex_tombstones_only_the_vanished_keys() {
    let project_id: i64 = 23;
    let traversal_path = "1/23/";
    let kept = (
        "src/Kept.java",
        "public class Kept { public void keep() {} }",
    );

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[
            kept,
            (
                "src/Gone.java",
                "public class Gone { public void vanish() {} }",
            ),
        ],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();

    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit1",
        1,
        traversal_path,
    )
    .await;
    mock.replace_archive(project_id, &[kept]);
    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit2",
        2,
        traversal_path,
    )
    .await;

    let file_ids = |path: &str| {
        format!(
            "SELECT id FROM {} WHERE project_id = {project_id} AND path = '{path}'",
            t("gl_file")
        )
    };
    let definition_ids = |path: &str| {
        format!(
            "SELECT id FROM {} WHERE project_id = {project_id} AND file_path = '{path}'",
            t("gl_definition")
        )
    };
    let gone_file = format!("id IN ({})", file_ids("src/Gone.java"));
    let kept_file = format!("id IN ({})", file_ids("src/Kept.java"));
    let gone_definitions = format!("id IN ({})", definition_ids("src/Gone.java"));
    let kept_definitions = format!("id IN ({})", definition_ids("src/Kept.java"));

    assert_eq!(
        count_tombstones(&clickhouse, "gl_file", project_id, &gone_file).await,
        1,
        "the vanished file must get exactly one tombstone"
    );
    assert_eq!(
        count_tombstones(&clickhouse, "gl_file", project_id, &kept_file).await,
        0,
        "a file re-emitted by the new snapshot must not be tombstoned"
    );
    assert_eq!(
        count_tombstones(&clickhouse, "gl_directory", project_id, "true").await,
        0,
        "directories re-emitted by the new snapshot must not be tombstoned"
    );
    assert_eq!(
        count_tombstones(&clickhouse, "gl_definition", project_id, &gone_definitions).await,
        2,
        "each vanished definition must get exactly one tombstone"
    );
    assert_eq!(
        count_tombstones(&clickhouse, "gl_definition", project_id, &kept_definitions).await,
        0,
        "definitions re-emitted by the new snapshot must not be tombstoned"
    );

    let kept_ids = format!(
        "{} UNION ALL {} UNION ALL SELECT id FROM {} WHERE project_id = {project_id}",
        file_ids("src/Kept.java"),
        definition_ids("src/Kept.java"),
        t("gl_directory")
    );
    assert!(
        count_tombstones(&clickhouse, "gl_code_edge", project_id, "true").await > 0,
        "edges of the vanished file must be tombstoned"
    );
    assert_eq!(
        count_tombstones(
            &clickhouse,
            "gl_code_edge",
            project_id,
            &format!("source_id IN ({kept_ids}) AND target_id IN ({kept_ids})")
        )
        .await,
        0,
        "edges between re-emitted nodes must not be tombstoned"
    );
}

#[tokio::test]
async fn disk_is_clean_after_successful_indexing() {
    let project_id: i64 = 4;
    let commit_sha = "abc123";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[(
            "src/Main.java",
            "public class Main {
            public void save() {}
        }",
        )],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let cache_dir = deps.cache_dir_path().to_path_buf();
    let handler = deps.code_indexing_task_handler();

    index_code(&handler, &clickhouse, project_id, commit_sha, 1, "1/4/").await;

    assert_code_indexed(&clickhouse, project_id).await;

    let remaining: Vec<_> = std::fs::read_dir(&cache_dir)
        .into_iter()
        .flatten()
        .flatten()
        .collect();
    assert!(
        remaining.is_empty(),
        "cache dir should be empty after indexing, found: {remaining:?}"
    );
}

#[tokio::test]
async fn disk_is_clean_after_multiple_reindexes() {
    let project_id: i64 = 5;

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[("src/Main.java", "public class Main { public void v1() {} }")],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let cache_dir = deps.cache_dir_path().to_path_buf();
    let handler = deps.code_indexing_task_handler();

    index_code(&handler, &clickhouse, project_id, "commit1", 1, "1/5/").await;

    mock.replace_archive(
        project_id,
        &[("src/Main.java", "public class Main { public void v2() {} }")],
    );
    index_code(&handler, &clickhouse, project_id, "commit2", 2, "1/5/").await;

    mock.replace_archive(
        project_id,
        &[("src/Main.java", "public class Main { public void v3() {} }")],
    );
    index_code(&handler, &clickhouse, project_id, "commit3", 3, "1/5/").await;

    assert_active_definitions(&clickhouse, project_id, "src/Main.java", &["Main", "v3"]).await;

    let remaining: Vec<_> = std::fs::read_dir(&cache_dir)
        .into_iter()
        .flatten()
        .flatten()
        .collect();
    assert!(
        remaining.is_empty(),
        "cache dir should be empty after repeated indexing, found: {remaining:?}"
    );
}

#[tokio::test]
async fn does_not_checkpoint_or_stale_delete_when_writer_fails() {
    let project_id: i64 = 6;
    let traversal_path = "1/6/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[(
            "src/Main.java",
            "public class Main { public void keep() {} }",
        )],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();
    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit1",
        1,
        traversal_path,
    )
    .await;

    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        Some(1)
    );

    mock.replace_archive(
        project_id,
        &[(
            "src/Other.java",
            "public class Other { public void run() {} }",
        )],
    );
    let failing_handler = deps.code_indexing_task_handler_with_writer(failing_writer());
    let context = handler_context();
    let envelope = code_indexing_task_envelope(project_id, "commit2", 2, traversal_path);
    let error = failing_handler
        .handle(context, envelope)
        .await
        .expect_err("a task whose rows never land must fail so NATS redelivers it");
    assert!(
        matches!(error, indexer::handler::HandlerError::Processing(_)),
        "unexpected error: {error}"
    );

    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
    assert_file_not_active(&clickhouse, project_id, "src/Other.java").await;
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        Some(1),
        "failed reindex must not advance the checkpoint"
    );
}

#[tokio::test]
async fn a_finished_job_acks_without_a_flush_timer() {
    let project_id: i64 = 61;
    let traversal_path = "1/61/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[(
            "src/Main.java",
            "public class Main { public void keep() {} }",
        )],
    );

    let production = orbit_server_config::CodeIndexingPipelineConfig {
        small_indexing_slots: Some(indexer::testkit::TEST_SMALL_INDEXING_SLOTS),
        big_indexing_slots: Some(indexer::testkit::TEST_BIG_INDEXING_SLOTS),
        ..orbit_server_config::AppConfig::embedded_defaults()
            .engine
            .handlers
            .code_indexing_task
            .pipeline
    };
    let deps = CodeIndexingDeps::new_with_pipeline_config(&mock, &clickhouse, production);
    let handler = deps.code_indexing_task_handler();

    let envelope = code_indexing_task_envelope(project_id, "commit1", 1, traversal_path);
    tokio::time::timeout(
        std::time::Duration::from_secs(60),
        handler.handle(handler_context(), envelope),
    )
    .await
    .expect("a job acks once its own rows land, not after a flush timer")
    .expect("indexing succeeds");

    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        Some(1)
    );
}

fn spawn_with_work_slot(
    handler: &Arc<indexer::modules::code::CodeIndexingTaskHandler>,
    project_id: i64,
    traversal_path: &'static str,
) -> (
    tokio::task::JoinHandle<Result<(), indexer::handler::HandlerError>>,
    Arc<tokio::sync::Semaphore>,
) {
    let slot = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = Arc::clone(&slot)
        .try_acquire_owned()
        .expect("a fresh slot has its permit");
    let context = handler_context().with_work_slot(Some(permit));
    let handler = Arc::clone(handler);
    let task = tokio::spawn(async move {
        handler
            .handle(
                context,
                code_indexing_task_envelope(project_id, "commit1", 1, traversal_path),
            )
            .await
    });
    (task, slot)
}

async fn released(slot: &tokio::sync::Semaphore) {
    slot.acquire().await.expect("slot stays open").forget();
}

#[tokio::test]
async fn a_job_frees_its_tree_and_slot_but_acks_only_after_its_rows_land() {
    let project_id: i64 = 63;
    let traversal_path = "1/63/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[(
            "src/Main.java",
            "public class Main { public void keep() {} }",
        )],
    );

    let gate = GatedClickHouse::start(&clickhouse.config.url).await;
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = Arc::new(deps.gated_handler(&gate));
    let cache_dir = deps.cache_dir_path().to_path_buf();

    let (mut job, slot) = spawn_with_work_slot(&handler, project_id, traversal_path);
    gate.insert_arrived().await;
    released(&slot).await;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while std::fs::read_dir(&cache_dir).is_ok_and(|mut d| d.next().is_some()) {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the extraction tree must be gone while the job waits for its rows to land");
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut job)
            .await
            .is_err(),
        "the job must not ack before ClickHouse has its rows"
    );
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        None
    );

    gate.open();
    job.await.expect("join").expect("indexing succeeds");
    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        Some(1)
    );
    assert!(gate.arrived() >= 6, "one insert per code table");
    assert_eq!(
        gate.max_in_flight(),
        gate.arrived(),
        "a small job sends every table's insert at once, so it waits one server flush"
    );
}

#[tokio::test]
async fn a_job_keeps_at_most_write_max_concurrent_inserts_in_flight() {
    let project_id: i64 = 65;
    let traversal_path = "1/65/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[(
            "src/Main.java",
            "public class Main { public void a() {} public void b() {} public void c() {} }",
        )],
    );

    let gate = GatedClickHouse::start(&clickhouse.config.url).await;
    let deps = CodeIndexingDeps::new_with_pipeline_config(
        &mock,
        &clickhouse,
        orbit_server_config::CodeIndexingPipelineConfig {
            write_slice_rows: 1,
            write_max_concurrent: 2,
            ..indexer::testkit::test_pipeline_configuration()
        },
    );
    let handler = deps.gated_handler(&gate);
    let job = tokio::spawn(async move {
        handler
            .handle(
                handler_context(),
                code_indexing_task_envelope(project_id, "commit1", 1, traversal_path),
            )
            .await
    });

    gate.insert_arrived().await;
    gate.insert_arrived().await;
    gate.open();
    job.await.expect("join").expect("indexing succeeds");

    assert!(
        gate.arrived() > 2,
        "one-row slices must have produced more inserts than the bound"
    );
    assert_eq!(
        gate.max_in_flight(),
        2,
        "the parse must pause until one of the two in-flight inserts lands"
    );
    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
}

#[tokio::test]
async fn a_stopped_engine_waits_for_a_job_whose_rows_are_still_in_flight() {
    let project_id: i64 = 64;
    let traversal_path = "1/64/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;
    let (_nats, nats_url) = super::super::common::dispatch::start_nats().await;
    let broker = Arc::new(
        indexer::nats::NatsBroker::connect(&orbit_server_config::NatsConfiguration {
            url: nats_url,
            ..orbit_server_config::AppConfig::embedded_defaults().nats
        })
        .await
        .expect("nats"),
    );
    broker
        .ensure_kv_bucket_exists(
            indexer::locking::INDEXING_LOCKS_BUCKET,
            indexer::nats::KvBucketConfig::default(),
        )
        .await
        .expect("locks bucket");

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[(
            "src/Main.java",
            "public class Main { public void keep() {} }",
        )],
    );

    let gate = GatedClickHouse::start(&clickhouse.config.url).await;
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.gated_handler(&gate);
    let (engine, configuration) = indexer::testkit::TestEngineBuilder::new(Arc::clone(&broker))
        .with_handler(Box::new(handler))
        .build();

    let request = CodeIndexingTaskRequest {
        task_id: 1,
        project_id,
        branch: Some("main".to_string()),
        commit_sha: Some("commit1".to_string()),
        traversal_path: TraversalPath::new_unchecked(traversal_path),
        dispatch_id: uuid::Uuid::new_v4(),
        campaign_id: None,
    };
    broker
        .ensure_streams(&[CodeIndexingTaskRequest::subscription()])
        .await
        .expect("stream");
    broker
        .publish(
            &request.publish_subscription(),
            &Envelope::new(&request).expect("envelope"),
        )
        .await
        .expect("publish");

    let mut running = tokio::spawn({
        let engine = Arc::clone(&engine);
        async move { engine.run(&configuration).await }
    });
    gate.insert_arrived().await;
    engine.stop();

    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), &mut running)
            .await
            .is_err(),
        "a stopped engine must keep running until the in-flight job lands its rows"
    );
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        None
    );

    gate.open();
    tokio::time::timeout(std::time::Duration::from_secs(60), running)
        .await
        .expect("the engine stops once the job lands")
        .expect("join")
        .expect("engine run");
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        Some(1),
        "the checkpoint is saved before the listener returns"
    );
    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
}

#[tokio::test]
async fn a_persistent_handled_failure_parks_the_project_at_the_sweep_threshold() {
    let project_id: i64 = 64;
    let traversal_path = "1/64/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[("src/Main.java", "public class Main {}")],
    );
    mock.set_archive_status(
        project_id,
        Some(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
    );
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();

    let handler = &handler;
    let fail_once = move |attempt: u32| async move {
        let mut envelope = code_indexing_task_envelope(project_id, "abc123", 11, traversal_path);
        envelope.attempt = attempt;
        handler
            .handle(handler_context_redelivered(attempt < 5), envelope)
            .await
            .expect_err("the archive endpoint keeps failing")
    };

    for cycle in 1..=5i64 {
        for attempt in 1..=5 {
            let error = fail_once(attempt).await;
            assert!(
                matches!(error, indexer::handler::HandlerError::Processing(_)),
                "cycle {cycle}, attempt {attempt}: {error}"
            );
        }
        assert_eq!(
            attempts(&clickhouse, traversal_path, project_id).await,
            [cycle],
            "each exhausted cycle keeps one attempt; the redelivered ones give theirs back"
        );
    }
    assert_eq!(
        attempts(&clickhouse, traversal_path, project_id).await,
        [indexer::orchestrator::dispatch::code_backfill::MAX_CODE_ATTEMPTS],
        "the project is parked at the sweep's threshold"
    );
}

#[tokio::test]
async fn a_lost_lock_does_not_refund_over_the_new_holders_attempt() {
    let project_id: i64 = 66;
    let traversal_path = "1/66/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project_with_slow_archive(project_id, "main");
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler_with_lock_ttl(std::time::Duration::from_secs(3));
    let locks = Arc::new(MockLockService::new());

    let new_holder = {
        let locks = Arc::clone(&locks);
        let store = Arc::clone(&deps.checkpoint_store);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
            locks.steal_leases();
            let mut checkpoint = indexer::modules::code::CodeCheckpoint::new(
                TraversalPath::new_unchecked(traversal_path),
                project_id,
                "main",
            );
            checkpoint.attempts = 7;
            store.save(&checkpoint).await.expect("new holder's start");
        })
    };

    let envelope = code_indexing_task_envelope(project_id, "abc123", 11, traversal_path);
    let error = handler
        .handle(
            handler_context_with_lock_service(locks).with_redelivery(true),
            envelope,
        )
        .await
        .expect_err("a lost lock cancels the run");
    assert!(
        matches!(
            error,
            indexer::handler::HandlerError::Processing(_)
                | indexer::handler::HandlerError::Backpressure(_)
        ),
        "unexpected error: {error}"
    );
    new_holder.await.expect("new holder");

    assert_eq!(
        attempts(&clickhouse, traversal_path, project_id).await,
        [7],
        "a run that lost its lock must not refund over the new holder's checkpoint"
    );
}

async fn a_held_lock_defers_the_delivery(project_id: i64, attempt: u32) {
    let traversal_path = format!("1/{project_id}/");

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[("src/Main.java", "public class Main {}")],
    );
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let lock_ttl = std::time::Duration::from_secs(3);
    let renew_interval = lock_ttl / 3;
    let handler = deps.code_indexing_task_handler_with_lock_ttl(lock_ttl);
    let held_for = std::time::Duration::from_secs(2);
    let locks = Arc::new(MockLockService::new());
    locks.set_lock_for(&project_lock_key(project_id, "main"), held_for);

    let mut envelope = code_indexing_task_envelope(project_id, "abc123", 11, &traversal_path);
    envelope.attempt = attempt;
    let error = handler
        .handle(handler_context_with_lock_service(locks), envelope)
        .await
        .expect_err("a held lock defers the delivery");

    let indexer::handler::HandlerError::Deferred { delay, .. } = error else {
        panic!("a held lock must defer delivery {attempt}, got {error}");
    };
    assert!(
        delay >= renew_interval && delay <= held_for + renew_interval,
        "delivery {attempt} waits out the rest of the lease plus one renew interval, got {delay:?}"
    );
    assert!(
        attempts(&clickhouse, &traversal_path, project_id)
            .await
            .is_empty(),
        "a deferred delivery starts no attempt"
    );
}

#[tokio::test]
async fn a_held_lock_defers_a_first_delivery() {
    a_held_lock_defers_the_delivery(68, 1).await;
}

#[tokio::test]
async fn a_held_lock_defers_a_redelivery() {
    a_held_lock_defers_the_delivery(71, 2).await;
}

#[tokio::test]
async fn a_deferred_delivery_runs_once_the_lock_frees() {
    let project_id: i64 = 69;
    let traversal_path = "1/69/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[("src/Main.java", "public class Main {}")],
    );
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler =
        deps.code_indexing_task_handler_with_lock_ttl(std::time::Duration::from_millis(900));
    let key = project_lock_key(project_id, "main");
    let locks = Arc::new(MockLockService::new());
    locks.set_lock_for(&key, std::time::Duration::from_secs(60));

    let envelope = code_indexing_task_envelope(project_id, "abc123", 11, traversal_path);
    let error = handler
        .handle(
            handler_context_with_lock_service(Arc::clone(&locks)),
            envelope,
        )
        .await
        .expect_err("a held lock defers the delivery");
    assert!(
        matches!(error, indexer::handler::HandlerError::Deferred { .. }),
        "a held lock must defer the delivery, got {error}"
    );

    locks.release(&key).await.expect("release");
    let mut envelope = code_indexing_task_envelope(project_id, "abc123", 11, traversal_path);
    envelope.attempt = 2;
    handler
        .handle(handler_context_with_lock_service(locks), envelope)
        .await
        .expect("the delivery after the lease expires runs");

    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        Some(11)
    );
    assert_eq!(attempts(&clickhouse, traversal_path, project_id).await, [0]);
    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
}

#[tokio::test]
async fn a_transient_renew_error_still_refunds_a_redelivered_attempt() {
    let project_id: i64 = 70;
    let traversal_path = "1/70/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[("src/Main.java", "public class Main {}")],
    );
    mock.set_archive_status(
        project_id,
        Some(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
    );
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();
    let locks = Arc::new(MockLockService::new());
    locks.fail_renews_transiently();

    let envelope = code_indexing_task_envelope(project_id, "abc123", 11, traversal_path);
    let error = handler
        .handle(handler_context_with_lock_service(locks), envelope)
        .await
        .expect_err("the archive endpoint fails");

    assert!(
        matches!(error, indexer::handler::HandlerError::Processing(_)),
        "unexpected error: {error}"
    );
    assert_eq!(
        attempts(&clickhouse, traversal_path, project_id).await,
        [0],
        "only a lost lock keeps the attempt; a backend error on renew does not prove another holder"
    );
}

#[tokio::test]
async fn empty_200_archive_checkpoints_as_empty_repository() {
    let project_id: i64 = 7;
    let traversal_path = "1/7/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project_with_empty_archive(project_id, "main");

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();
    let context = handler_context();
    let envelope = code_indexing_backfill_envelope(project_id, 11, traversal_path);

    let result = handler.handle(context, envelope).await;
    assert!(
        result.is_ok(),
        "empty 200 archive should ack on first attempt, got {:?}",
        result
    );

    let checkpoint_rows = clickhouse
        .query(&format!(
            "SELECT last_task_id, last_commit FROM {} FINAL \
             WHERE traversal_path = '{traversal_path}' AND project_id = {project_id} \
             AND branch = 'main' AND _deleted = false",
            t("code_indexing_checkpoint")
        ))
        .await;
    let batch = checkpoint_rows
        .first()
        .expect("checkpoint row must exist after empty-archive ack");
    assert_eq!(batch.num_rows(), 1, "expected exactly one checkpoint row");
    let task_ids = ArrowUtils::get_column_by_name::<Int64Array>(batch, "last_task_id")
        .expect("last_task_id column");
    assert_eq!(task_ids.value(0), 11);
    let last_commits = ArrowUtils::get_column_by_name::<StringArray>(batch, "last_commit")
        .expect("last_commit column");
    assert!(
        last_commits.is_null(0) || last_commits.value(0).is_empty(),
        "empty-archive checkpoint should record no commit, got {:?}",
        last_commits.value(0)
    );

    let files = clickhouse
        .query(&format!(
            "SELECT path FROM {} WHERE project_id = {project_id}",
            t("gl_file")
        ))
        .await;
    assert!(
        files.first().is_none_or(|b| b.num_rows() == 0),
        "no files should be written for an empty-archive project"
    );
    let definitions = clickhouse
        .query(&format!(
            "SELECT name FROM {} WHERE project_id = {project_id}",
            t("gl_definition")
        ))
        .await;
    assert!(
        definitions.first().is_none_or(|b| b.num_rows() == 0),
        "no definitions should be written for an empty-archive project"
    );
    let ontology = integration_testkit::load_ontology();
    let defines_edges = clickhouse
        .query(&format!(
            "SELECT source_id FROM {} \
             WHERE relationship_kind = 'DEFINES' \
             AND source_id IN (SELECT id FROM {} WHERE project_id = {project_id})",
            ontology.edge_table_for_relationship("DEFINES").unwrap(),
            t("gl_definition")
        ))
        .await;
    assert!(
        defines_edges.first().is_none_or(|b| b.num_rows() == 0),
        "no DEFINES edges should be written for an empty-archive project"
    );
}

#[tokio::test]
async fn archive_overload_on_backfill_is_retried_not_checkpointed_as_empty() {
    let project_id: i64 = 7;
    let traversal_path = "1/7/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[("src/Main.java", "public class Main {}")],
    );
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();

    let max_attempts = indexer::modules::code::code_indexing_task_topic_policy()
        .max_attempts
        .expect("the code topic sets max_attempts");
    for delivery in 1..=max_attempts {
        let status = if delivery % 2 == 1 {
            axum::http::StatusCode::BAD_GATEWAY
        } else {
            axum::http::StatusCode::TOO_MANY_REQUESTS
        };
        mock.set_archive_status(project_id, Some(status));
        let mut envelope = code_indexing_backfill_envelope(project_id, 11, traversal_path);
        envelope.attempt = delivery;
        let result = handler.handle(handler_context(), envelope).await;
        assert!(
            matches!(&result, Err(indexer::handler::HandlerError::Processing(msg)) if msg.contains("overloaded")),
            "{status} on a backfill task must nack as overload, got {result:?}"
        );
    }

    let spent = attempts(&clickhouse, traversal_path, project_id).await;
    assert_eq!(spent, [1], "a whole dispatch of overload costs one attempt");
    assert!(
        spent[0] < indexer::orchestrator::dispatch::code_backfill::MAX_CODE_ATTEMPTS,
        "the dispatcher must still re-send the project after CODE_RETRY_AFTER"
    );
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        None,
        "overload must not write an indexed-empty checkpoint"
    );

    mock.set_archive_status(project_id, None);
    let envelope = code_indexing_backfill_envelope(project_id, 11, traversal_path);
    handler
        .handle(handler_context(), envelope)
        .await
        .expect("the redelivered task indexes once the endpoint recovers");
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        Some(11)
    );
}

#[tokio::test]
async fn an_empty_archive_on_a_redelivered_attempt_is_not_checkpointed() {
    let project_id: i64 = 67;
    let traversal_path = "1/67/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project_with_empty_archive(project_id, "main");
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();

    let envelope = code_indexing_task_envelope(project_id, "abc123", 11, traversal_path);
    let error = handler
        .handle(handler_context_redelivered(true), envelope)
        .await
        .expect_err("an empty archive for a commit that exists waits for the next delivery");
    assert!(
        matches!(error, indexer::handler::HandlerError::Processing(_)),
        "unexpected error: {error}"
    );
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        None
    );
    assert_eq!(attempts(&clickhouse, traversal_path, project_id).await, [0]);
}

#[tokio::test]
async fn an_empty_archive_on_the_last_delivery_is_checkpointed_empty() {
    let project_id: i64 = 7;
    let traversal_path = "1/7/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project_with_empty_archive(project_id, "main");
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();

    let envelope = code_indexing_task_envelope(project_id, "abc123", 11, traversal_path);
    handler
        .handle(handler_context_redelivered(true), envelope)
        .await
        .expect_err("earlier deliveries keep the redelivery window open");

    let mut envelope = code_indexing_task_envelope(project_id, "abc123", 11, traversal_path);
    envelope.attempt = 5;
    handler
        .handle(handler_context_redelivered(false), envelope)
        .await
        .expect(
            "the last delivery checkpoints the project as indexed-empty instead of dead-lettering",
        );
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        Some(11)
    );
    assert_eq!(attempts(&clickhouse, traversal_path, project_id).await, [0]);

    mock.replace_archive(project_id, &[("src/Main.java", "public class Main {}")]);
    let envelope = code_indexing_task_envelope(project_id, "abc123", 12, traversal_path);
    handler
        .handle(handler_context(), envelope)
        .await
        .expect("the next task indexes the project");

    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        Some(12)
    );
    assert_eq!(attempts(&clickhouse, traversal_path, project_id).await, [0]);
    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
}

#[tokio::test]
async fn a_transiently_empty_archive_is_retried_before_the_download_gives_up() {
    let project_id: i64 = 65;
    let traversal_path = "1/65/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project_with_transient_empty_archive(
        project_id,
        "main",
        &[("src/Main.java", "public class Main {}")],
        1,
    );
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();

    let envelope = code_indexing_task_envelope(project_id, "abc123", 11, traversal_path);
    handler
        .handle(handler_context(), envelope)
        .await
        .expect("the download retry sees the archive once Gitaly serves it");
    assert_file_is_active(&clickhouse, project_id, "src/Main.java").await;
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, traversal_path, project_id, "main").await,
        Some(11)
    );
}

async fn attempts(
    clickhouse: &integration_testkit::TestContext,
    traversal_path: &str,
    project_id: i64,
) -> Vec<i64> {
    let result = clickhouse
        .query(&format!(
            "SELECT attempts FROM {} FINAL \
             WHERE traversal_path = '{traversal_path}' AND project_id = {project_id} \
             AND branch = 'main' AND _deleted = false",
            t("code_indexing_checkpoint")
        ))
        .await;
    i64::extract_column(&result, 0).expect("attempts")
}

/// Verifies that stale edge cleanup is scoped to the specific project+branch
/// and does not delete edges belonging to other projects in the same namespace.
///
/// This is the regression test for the fix in `stale_data_cleaner.rs` that
/// replaced the namespace-wide `source_kind IN (...)` filter with a
/// `source_id IN (subquery)` that pins to `project_id` and `branch`.
#[tokio::test]
async fn stale_edge_cleanup_does_not_affect_other_projects_in_namespace() {
    let project_a: i64 = 20;
    let project_b: i64 = 21;
    let traversal_path_a = "1/20/";
    let traversal_path_b = "1/21/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;

    mock.add_project(
        project_a,
        "main",
        &[(
            "src/Alpha.java",
            "public class Alpha {
                public void run() {}
            }",
        )],
    );

    mock.add_project(
        project_b,
        "main",
        &[(
            "src/Beta.java",
            "public class Beta {
                public void execute() {}
            }",
        )],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();

    index_code(
        &handler,
        &clickhouse,
        project_a,
        "commit_a1",
        1,
        traversal_path_a,
    )
    .await;
    index_code(
        &handler,
        &clickhouse,
        project_b,
        "commit_b1",
        2,
        traversal_path_b,
    )
    .await;

    let edges_a_before = count_active_edges(&clickhouse, project_a, "DEFINES").await;
    let edges_b_before = count_active_edges(&clickhouse, project_b, "DEFINES").await;
    assert!(
        edges_a_before >= 1,
        "project A should have DEFINES edges before reindex, got {edges_a_before}"
    );
    assert!(
        edges_b_before >= 1,
        "project B should have DEFINES edges before reindex, got {edges_b_before}"
    );

    mock.replace_archive(
        project_a,
        &[(
            "src/AlphaV2.java",
            "public class AlphaV2 {
                public void runV2() {}
            }",
        )],
    );
    index_code(
        &handler,
        &clickhouse,
        project_a,
        "commit_a2",
        3,
        traversal_path_a,
    )
    .await;

    assert_no_active_definitions(&clickhouse, project_a, "src/Alpha.java").await;
    assert_active_definitions(
        &clickhouse,
        project_a,
        "src/AlphaV2.java",
        &["AlphaV2", "runV2"],
    )
    .await;

    let edges_b_after = count_active_edges(&clickhouse, project_b, "DEFINES").await;
    assert_eq!(
        edges_b_after, edges_b_before,
        "stale edge cleanup for project A must not delete edges belonging to project B \
         (same namespace). Before: {edges_b_before}, after: {edges_b_after}"
    );
    assert_active_definitions(
        &clickhouse,
        project_b,
        "src/Beta.java",
        &["Beta", "execute"],
    )
    .await;
}

#[tokio::test]
async fn stale_rows_in_the_code_edge_table_are_cleaned_on_reindex() {
    let project_id: i64 = 22;
    let traversal_path = "1/22/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[("src/Alpha.java", "public class Alpha {}")],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();

    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit1",
        1,
        traversal_path,
    )
    .await;

    let directory_id = first_active_directory_id(&clickhouse, project_id).await;
    insert_stale_canary_edge(
        &clickhouse,
        traversal_path,
        project_id,
        "main",
        directory_id,
    )
    .await;
    assert_eq!(count_canary_edges(&clickhouse, directory_id).await, 1);

    mock.replace_archive(
        project_id,
        &[("src/AlphaV2.java", "public class AlphaV2 {}")],
    );
    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit2",
        2,
        traversal_path,
    )
    .await;

    assert_eq!(
        count_canary_edges(&clickhouse, directory_id).await,
        0,
        "the reindex must tombstone the stale code edge so a _deleted = false FINAL \
         read no longer returns it"
    );
}

#[tokio::test]
async fn edges_from_a_directory_removed_by_a_reindex_are_cleaned() {
    let project_id: i64 = 23;
    let traversal_path = "1/23/";

    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[
            ("src/Alpha.java", "public class Alpha {}"),
            ("src/legacy/Old.java", "public class Old {}"),
        ],
    );

    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler = deps.code_indexing_task_handler();

    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit1",
        1,
        traversal_path,
    )
    .await;
    assert_eq!(
        count_active_directories_at_path(&clickhouse, project_id, "src/legacy").await,
        1,
        "the fixture must produce a directory that the reindex can then remove"
    );

    mock.replace_archive(project_id, &[("src/Alpha.java", "public class Alpha {}")]);
    index_code(
        &handler,
        &clickhouse,
        project_id,
        "commit2",
        2,
        traversal_path,
    )
    .await;

    assert_eq!(
        count_active_directories_at_path(&clickhouse, project_id, "src/legacy").await,
        0,
        "the removed directory's own row should be gone"
    );
    assert_eq!(
        edges_from_directories_that_no_longer_exist(&clickhouse).await,
        0,
        "a shared edge whose source directory the same reindex removed can never be \
         matched again once that directory's id is no longer resolvable"
    );
}

async fn count_active_directories_at_path(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    path: &str,
) -> usize {
    let result = clickhouse
        .query(&format!(
            "SELECT id FROM {} FINAL \
             WHERE project_id = {project_id} AND path = '{path}' AND _deleted = false",
            t("gl_directory")
        ))
        .await;
    result.first().map_or(0, |b| b.num_rows())
}

async fn edges_from_directories_that_no_longer_exist(
    clickhouse: &integration_testkit::TestContext,
) -> usize {
    let ontology = integration_testkit::load_ontology();
    let edge_table = ontology
        .edge_table_for_variant("CONTAINS", "Directory", "Directory")
        .unwrap();
    let result = clickhouse
        .query(&format!(
            "SELECT source_id FROM {edge_table} FINAL \
             WHERE _deleted = false AND source_kind = 'Directory' \
             AND source_id NOT IN (SELECT id FROM {} FINAL WHERE _deleted = false)",
            t("gl_directory")
        ))
        .await;
    result.first().map_or(0, |b| b.num_rows())
}

const CANARY_TARGET_ID: i64 = 999_999_998;

async fn first_active_directory_id(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
) -> i64 {
    let result = clickhouse
        .query(&format!(
            "SELECT id FROM {} FINAL \
             WHERE project_id = {project_id} AND _deleted = false LIMIT 1",
            t("gl_directory")
        ))
        .await;
    let batch = result
        .first()
        .expect("directory query should return a batch");
    assert_eq!(
        batch.num_rows(),
        1,
        "indexing must produce directory nodes for the canary to hang off"
    );
    ArrowUtils::get_column_by_name::<Int64Array>(batch, "id")
        .expect("id column")
        .value(0)
}

async fn insert_stale_canary_edge(
    clickhouse: &integration_testkit::TestContext,
    traversal_path: &str,
    project_id: i64,
    branch: &str,
    source_id: i64,
) {
    let ontology = integration_testkit::load_ontology();
    let edge_table = ontology
        .edge_table_for_variant("CONTAINS", "Directory", "File")
        .unwrap();
    let sql = format!(
        "INSERT INTO {edge_table} \
         (traversal_path, project_id, branch, source_id, source_kind, relationship_kind, \
          target_id, target_kind, _version) \
         VALUES ('{traversal_path}', {project_id}, '{branch}', {source_id}, 'Directory', 'CONTAINS', \
                 {CANARY_TARGET_ID}, 'File', '2020-01-01 00:00:00.000000')"
    );
    clickhouse.execute(&sql).await;
}

async fn count_canary_edges(
    clickhouse: &integration_testkit::TestContext,
    source_id: i64,
) -> usize {
    let ontology = integration_testkit::load_ontology();
    let edge_table = ontology
        .edge_table_for_variant("CONTAINS", "Directory", "File")
        .unwrap();
    let result = clickhouse
        .query(&format!(
            "SELECT target_id FROM {edge_table} FINAL \
             WHERE source_id = {source_id} AND target_id = {CANARY_TARGET_ID} \
             AND _deleted = false"
        ))
        .await;
    result.first().map_or(0, |b| b.num_rows())
}

#[tokio::test]
async fn code_job_inserts_send_the_default_insert_settings() {
    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;
    let mock = MockGitlabServer::start().await;
    let project = 2791;
    mock.add_project(
        project,
        "main",
        &[
            ("src/main.py", "from helper import greet\ngreet()\n"),
            ("src/helper.py", "def greet():\n    pass\n"),
        ],
    );
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    index_code(
        &deps.code_indexing_task_handler(),
        &clickhouse,
        project,
        "first",
        1,
        "1/2791/",
    )
    .await;

    clickhouse.execute("SYSTEM FLUSH LOGS").await;
    let batches = clickhouse
        .query(&format!(
            "SELECT DISTINCT tables[1], toString(ProfileEvents['AsyncInsertQuery']), \
                    if(Settings['wait_for_async_insert'] = '', \
                       toString(toUInt8(getSetting('wait_for_async_insert'))), \
                       Settings['wait_for_async_insert']), \
                    Settings['async_insert_busy_timeout_min_ms'], \
                    Settings['async_insert_busy_timeout_max_ms'] \
             FROM system.query_log \
             WHERE type = 'QueryFinish' AND query_kind = 'Insert' \
               AND current_database = '{}' AND length(tables) = 1",
            clickhouse.config.database
        ))
        .await;
    let columns: Vec<Vec<String>> = (0..5)
        .map(|index| String::extract_column(&batches, index).expect("string column"))
        .collect();
    let sent: Vec<(String, [String; 4])> = (0..columns[0].len())
        .map(|row| {
            (
                columns[0][row].clone(),
                [1, 2, 3, 4].map(|column| columns[column][row].clone()),
            )
        })
        .collect();

    let table = |name: &str| format!("{}.{}", clickhouse.config.database, t(name));
    for expected in [table("code_indexing_checkpoint"), table("gl_file")] {
        assert!(sent.iter().any(|(name, _)| *name == expected), "{sent:?}");
    }
    for (name, settings) in &sent {
        assert_eq!(settings, &["1", "1", "1000", "1000"], "{name}: {sent:?}");
    }
}

async fn index_code(
    handler: &indexer::modules::code::CodeIndexingTaskHandler,
    _clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    commit_sha: &str,
    task_id: i64,
    traversal_path: &str,
) {
    let context = handler_context();
    let envelope = code_indexing_task_envelope(project_id, commit_sha, task_id, traversal_path);

    handler
        .handle(context, envelope)
        .await
        .unwrap_or_else(|e| panic!("indexing commit {commit_sha} (task {task_id}) failed: {e}"));
}

async fn insert_file_row_with_version(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    traversal_path: &str,
    branch: &str,
    id: i64,
    path: &str,
    version: &str,
) {
    let name = path.rsplit('/').next().unwrap_or(path);
    let sql = format!(
        "INSERT INTO {} \
         (id, traversal_path, project_id, branch, path, name, extension, language, _version) \
         VALUES ({id}, '{traversal_path}', {project_id}, '{branch}', \
                 '{path}', '{name}', 'java', 'java', '{version}')",
        t("gl_file")
    );
    clickhouse.execute(&sql).await;
}

async fn insert_stale_canary_file(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    traversal_path: &str,
    branch: &str,
) {
    let sql = format!(
        "INSERT INTO {} \
         (id, traversal_path, project_id, branch, path, name, extension, language, _version) \
         VALUES (999999999, '{traversal_path}', {project_id}, '{branch}', \
                 'src/Canary.java', 'Canary.java', 'java', 'java', \
                 '2020-01-01 00:00:00.000000')",
        t("gl_file")
    );
    clickhouse.execute(&sql).await;
}

fn failing_writer() -> Arc<indexer::clickhouse::ClickHouseWriter> {
    writer_for(orbit_server_config::ClickHouseConfiguration {
        url: "http://127.0.0.1:1".into(),
        ..orbit_server_config::AppConfig::embedded_defaults().graph
    })
}

async fn latest_checkpoint_task_id(
    clickhouse: &integration_testkit::TestContext,
    traversal_path: &str,
    project_id: i64,
    branch: &str,
) -> Option<i64> {
    let result = clickhouse
        .query(&format!(
            "SELECT last_task_id FROM {} FINAL \
             WHERE traversal_path = '{traversal_path}' AND project_id = {project_id} \
             AND branch = '{branch}' AND indexed_at IS NOT NULL AND _deleted = false",
            t("code_indexing_checkpoint")
        ))
        .await;
    let batch = result.first()?;
    if batch.num_rows() == 0 {
        return None;
    }
    let task_ids = ArrowUtils::get_column_by_name::<Int64Array>(batch, "last_task_id")
        .expect("task id column");
    Some(task_ids.value(0))
}

fn code_indexing_task_envelope(
    project_id: i64,
    commit_sha: &str,
    task_id: i64,
    traversal_path: &str,
) -> Envelope {
    Envelope::new(&CodeIndexingTaskRequest {
        task_id,
        project_id,
        branch: Some("main".to_string()),
        commit_sha: Some(commit_sha.to_string()),
        traversal_path: TraversalPath::new_unchecked(traversal_path),
        dispatch_id: uuid::Uuid::new_v4(),
        campaign_id: None,
    })
    .expect("failed to create envelope")
}

fn code_indexing_backfill_envelope(
    project_id: i64,
    task_id: i64,
    traversal_path: &str,
) -> Envelope {
    Envelope::new(&CodeIndexingTaskRequest {
        task_id,
        project_id,
        branch: Some("main".to_string()),
        commit_sha: None,
        traversal_path: TraversalPath::new_unchecked(traversal_path),
        dispatch_id: uuid::Uuid::new_v4(),
        campaign_id: None,
    })
    .expect("failed to create envelope")
}

async fn assert_file_not_active(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    path: &str,
) {
    let active_rows = clickhouse
        .query(&format!(
            "SELECT id FROM {} FINAL \
             WHERE project_id = {project_id} AND path = '{path}' AND _deleted = false",
            t("gl_file")
        ))
        .await;
    assert!(
        active_rows.first().is_none_or(|b| b.num_rows() == 0),
        "file '{path}' should not be active after soft-deletion"
    );
}

async fn assert_file_is_active(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    path: &str,
) {
    let result = clickhouse
        .query(&format!(
            "SELECT id FROM {} FINAL \
             WHERE project_id = {project_id} AND path = '{path}' AND _deleted = false",
            t("gl_file")
        ))
        .await;
    assert!(
        result.first().is_some_and(|b| b.num_rows() > 0),
        "file '{path}' should be active"
    );
}

async fn active_file_rows(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
) -> Vec<(String, String, String)> {
    let result = clickhouse
        .query(&format!(
            "SELECT path, extension, language FROM {} FINAL \
             WHERE project_id = {project_id} AND _deleted = false \
             ORDER BY path",
            t("gl_file")
        ))
        .await;

    let mut rows = Vec::new();
    for batch in result {
        let paths =
            ArrowUtils::get_column_by_name::<StringArray>(&batch, "path").expect("path column");
        let extensions = ArrowUtils::get_column_by_name::<StringArray>(&batch, "extension")
            .expect("extension column");
        let languages = ArrowUtils::get_column_by_name::<StringArray>(&batch, "language")
            .expect("language column");
        for row in 0..batch.num_rows() {
            rows.push((
                paths.value(row).to_string(),
                extensions.value(row).to_string(),
                languages.value(row).to_string(),
            ));
        }
    }
    rows
}

fn language_for<'a>(rows: &'a [(String, String, String)], path: &str) -> Option<&'a str> {
    rows.iter()
        .find(|row| row.0 == path)
        .map(|row| row.2.as_str())
}

async fn file_size_bytes(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    path: &str,
) -> Option<i64> {
    let result = clickhouse
        .query(&format!(
            "SELECT size_bytes FROM {} FINAL \
             WHERE project_id = {project_id} AND path = '{path}' AND _deleted = false",
            t("gl_file")
        ))
        .await;
    let batch = result.into_iter().find(|b| b.num_rows() > 0)?;
    let col = ArrowUtils::get_column_by_name::<arrow::array::Int64Array>(&batch, "size_bytes")
        .expect("size_bytes column");
    Some(col.value(0))
}

async fn assert_directory_is_active(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    path: &str,
) {
    let result = clickhouse
        .query(&format!(
            "SELECT id FROM {} FINAL \
             WHERE project_id = {project_id} AND path = '{path}' AND _deleted = false",
            t("gl_directory")
        ))
        .await;
    assert!(
        result.first().is_some_and(|b| b.num_rows() > 0),
        "directory '{path}' should be active"
    );
}

async fn assert_contains_edge_between_directory_and_file(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    directory_path: &str,
    file_path: &str,
) {
    let ontology = integration_testkit::load_ontology();
    let edge_table = ontology
        .edge_table_for_variant("CONTAINS", "Directory", "File")
        .unwrap();
    let result = clickhouse
        .query(&format!(
            "SELECT e.source_id FROM {edge_table} AS e FINAL \
             INNER JOIN {} AS d FINAL ON e.source_id = d.id \
             INNER JOIN {} AS f FINAL ON e.target_id = f.id \
             WHERE e.relationship_kind = 'CONTAINS' \
               AND e.source_kind = 'Directory' \
               AND e.target_kind = 'File' \
               AND d.project_id = {project_id} \
               AND f.project_id = {project_id} \
               AND d.path = '{directory_path}' \
               AND f.path = '{file_path}' \
               AND d._deleted = false \
               AND f._deleted = false \
               AND e._deleted = false",
            t("gl_directory"),
            t("gl_file")
        ))
        .await;
    let row_count: usize = result.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(
        row_count, 1,
        "expected exactly one CONTAINS edge from directory '{directory_path}' to file '{file_path}'"
    );
}

async fn query_active_definition_names(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    file_path: &str,
) -> Vec<String> {
    let result = clickhouse
        .query(&format!(
            "SELECT name FROM {} FINAL \
             WHERE project_id = {project_id} AND file_path = '{file_path}' AND _deleted = false",
            t("gl_definition")
        ))
        .await;
    let Some(batch) = result.first() else {
        return Vec::new();
    };
    let names = ArrowUtils::get_column_by_name::<StringArray>(batch, "name").expect("name column");
    (0..batch.num_rows())
        .map(|i| names.value(i).to_string())
        .collect()
}

async fn assert_active_definitions(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    file_path: &str,
    expected_names: &[&str],
) {
    let mut actual = query_active_definition_names(clickhouse, project_id, file_path).await;
    actual.sort();
    let mut expected: Vec<&str> = expected_names.to_vec();
    expected.sort();
    assert_eq!(
        actual, expected,
        "active definitions in '{file_path}' should be {expected:?}, got {actual:?}"
    );
}

async fn count_active_edges(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    relationship_kind: &str,
) -> usize {
    let ontology = integration_testkit::load_ontology();
    let edge_table = ontology
        .edge_table_for_relationship(relationship_kind)
        .unwrap();
    let result = clickhouse
        .query(&format!(
            "SELECT source_id FROM {edge_table} FINAL \
             WHERE relationship_kind = '{relationship_kind}' AND _deleted = false \
             AND source_id IN (SELECT id FROM {} FINAL WHERE project_id = {project_id} AND _deleted = false)",
            t("gl_definition")
        ))
        .await;
    result.first().map_or(0, |b| b.num_rows())
}

async fn count_tombstones(
    clickhouse: &integration_testkit::TestContext,
    table: &str,
    project_id: i64,
    predicate: &str,
) -> usize {
    let result = clickhouse
        .query(&format!(
            "SELECT _version FROM {} \
             WHERE project_id = {project_id} AND _deleted = true AND {predicate}",
            t(table)
        ))
        .await;
    result.iter().map(|b| b.num_rows()).sum()
}

async fn assert_no_active_definitions(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    file_path: &str,
) {
    let active = query_active_definition_names(clickhouse, project_id, file_path).await;
    assert!(
        active.is_empty(),
        "definitions in '{file_path}' should not be active (soft-deleted), but found: {active:?}"
    );
}

async fn assert_branch_indexed(
    clickhouse: &integration_testkit::TestContext,
    project_id: i64,
    expected_name: &str,
    expected_traversal_path: &str,
) {
    let result = clickhouse
        .query(&format!(
            "SELECT name, is_default, traversal_path, project_id \
             FROM {} FINAL \
             WHERE project_id = {project_id} AND _deleted = false",
            t("gl_branch")
        ))
        .await;

    let batch = result
        .first()
        .expect("gl_branch should have rows after indexing");
    assert_eq!(batch.num_rows(), 1, "expected exactly one branch row");

    let names = ArrowUtils::get_column_by_name::<StringArray>(batch, "name").expect("name column");
    assert_eq!(names.value(0), expected_name);

    let is_default = ArrowUtils::get_column_by_name::<BooleanArray>(batch, "is_default")
        .expect("is_default column");
    assert!(is_default.value(0), "branch should be marked as default");

    let traversal_paths = ArrowUtils::get_column_by_name::<StringArray>(batch, "traversal_path")
        .expect("traversal_path column");
    assert_eq!(traversal_paths.value(0), expected_traversal_path);

    let project_ids = ArrowUtils::get_column_by_name::<Int64Array>(batch, "project_id")
        .expect("project_id column");
    assert_eq!(project_ids.value(0), project_id);

    assert_edge_count_for_traversal_path(
        clickhouse,
        "IN_PROJECT",
        "Branch",
        "Project",
        &TraversalPath::new_unchecked(expected_traversal_path),
        1,
    )
    .await;

    assert_edge_count_for_traversal_path(
        clickhouse,
        "CONTAINS",
        "Project",
        "Branch",
        &TraversalPath::new_unchecked(expected_traversal_path),
        1,
    )
    .await;

    assert_edge_count_for_traversal_path(
        clickhouse,
        "CONTAINS",
        "Branch",
        "Directory",
        &TraversalPath::new_unchecked(expected_traversal_path),
        1,
    )
    .await;

    assert_edge_count_for_traversal_path(
        clickhouse,
        "ON_BRANCH",
        "File",
        "Branch",
        &TraversalPath::new_unchecked(expected_traversal_path),
        1,
    )
    .await;
}

async fn timed_out_job_writes_no_data(incremental: bool) {
    if incremental {
        enable_incremental("99/99/");
    }
    let project_id: i64 = 99;
    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project_with_slow_archive(project_id, "main");

    let deps = CodeIndexingDeps::new_with_pipeline_config(
        &mock,
        &clickhouse,
        orbit_server_config::CodeIndexingPipelineConfig {
            job_timeout_secs: 1,
            ..indexer::testkit::test_pipeline_configuration()
        },
    );
    let handler = deps.code_indexing_task_handler();

    // First timeout: transient, so NATS redelivers for one retry.
    let mut first = code_indexing_task_envelope(project_id, "abc123", 1, "99/99/");
    first.attempt = 1;
    let result = handler.handle(handler_context(), first).await;
    assert!(
        matches!(result, Err(indexer::handler::HandlerError::Processing(_))),
        "first timeout must retry (transient), not dead-letter; got {result:?}"
    );

    // Second timeout: structurally stuck, so it dead-letters.
    let mut second = code_indexing_task_envelope(project_id, "abc123", 1, "99/99/");
    second.attempt = 2;
    let result = handler.handle(handler_context(), second).await;
    assert!(
        matches!(
            result,
            Err(indexer::handler::HandlerError::Permanent {
                action: indexer::handler::PermanentAction::DeadLetter,
                ..
            })
        ),
        "a job that times out twice must dead-letter; got {result:?}"
    );

    for table in ["gl_file", "gl_definition"] {
        let rows = clickhouse
            .query(&format!(
                "SELECT project_id FROM {} WHERE project_id = {project_id}",
                t(table)
            ))
            .await;
        assert!(
            rows.iter().all(|b| b.num_rows() == 0),
            "a timed-out job must write no rows to {table}",
        );
    }
}

async fn heartbeat_keeps_a_slow_job_alive_and_indexes_it(incremental: bool) {
    if incremental {
        enable_incremental("4242/4242/");
    }
    let project_id: i64 = 4242;
    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project_with_slow_archive(project_id, "main");

    let lock_ttl_forcing_ticks_during_fetch = std::time::Duration::from_millis(900);
    let deps = CodeIndexingDeps::new(&mock, &clickhouse);
    let handler =
        deps.code_indexing_task_handler_with_lock_ttl(lock_ttl_forcing_ticks_during_fetch);

    let locks = Arc::new(MockLockService::new());
    let context = handler_context_with_lock_service(locks.clone());
    let envelope = code_indexing_task_envelope(project_id, "slow-sha", 1, "4242/4242/");

    let result = handler.handle(context, envelope).await;
    assert!(
        result.is_ok(),
        "a slow job must complete under the heartbeat, not fail: {result:?}"
    );

    assert_code_indexed(&clickhouse, project_id).await;
    assert!(
        locks.renew_count() >= 3,
        "the heartbeat must renew the lock during the slow fetch; got {} renews",
        locks.renew_count(),
    );
}

async fn disk_is_clean_after_a_timed_out_job(incremental: bool) {
    if incremental {
        enable_incremental("991/991/");
    }
    let project_id: i64 = 991;
    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project_with_slow_archive(project_id, "main");

    // 1s budget vs a 3s fetch: the job is dropped mid-run, so cleanup runs via the TempDir drop.
    let deps = CodeIndexingDeps::new_with_pipeline_config(
        &mock,
        &clickhouse,
        orbit_server_config::CodeIndexingPipelineConfig {
            job_timeout_secs: 1,
            ..indexer::testkit::test_pipeline_configuration()
        },
    );
    let cache_dir = deps.cache_dir_path().to_path_buf();
    let handler = deps.code_indexing_task_handler();

    let mut envelope = code_indexing_task_envelope(project_id, "abc123", 1, "991/991/");
    envelope.attempt = 1;
    let _ = handler.handle(handler_context(), envelope).await;

    let remaining: Vec<_> = std::fs::read_dir(&cache_dir)
        .into_iter()
        .flatten()
        .flatten()
        .collect();
    assert!(
        remaining.is_empty(),
        "a dropped job must not leak its extraction dir, found: {remaining:?}"
    );
}

#[derive(Debug, PartialEq)]
enum CancelledRun {
    FailedProcessing,
    Backpressure,
    Other,
}

async fn run_with_cancelled_token(
    incremental: bool,
    leasing: bool,
) -> (
    CancelledRun,
    integration_testkit::TestContext,
    CodeIndexingDeps,
) {
    if incremental {
        enable_incremental("993/993/");
    }
    let project_id: i64 = 993;
    let traversal_path = "993/993/";
    let clickhouse = integration_testkit::TestContext::new(&[
        integration_testkit::SIPHON_SCHEMA_SQL,
        *integration_testkit::GRAPH_SCHEMA_SQL,
    ])
    .await;

    let mock = MockGitlabServer::start().await;
    mock.add_project(
        project_id,
        "main",
        &[("src/Main.java", "public class Main { void a() {} }")],
    );

    let mut pipeline_config = indexer::testkit::test_pipeline_configuration();
    if !leasing {
        pipeline_config.archive_storage_budget = 0;
        pipeline_config.archive_tenant_budget = 0;
    }
    let deps = CodeIndexingDeps::new_with_pipeline_config(&mock, &clickhouse, pipeline_config);
    let request = indexer::modules::code::IndexingRequest {
        project_id,
        branch: "main".to_string(),
        traversal_path: TraversalPath::new_unchecked(traversal_path),
        task_id: 1,
        commit_sha: Some("abc123".to_string()),
        checkpoint: indexer::modules::code::CodeCheckpoint::new(
            TraversalPath::new_unchecked(traversal_path),
            project_id,
            "main",
        ),
        repository_storage: None,
    };

    let cancel = code_graph::v2::CancellationToken::new();
    cancel.cancel();
    let mut observer = indexer::observer::NoOpObserver;
    let lock = indexer::locking::LockGuard::acquire(
        Arc::new(MockLockService::new()),
        "project.9911.cancelled",
        std::time::Duration::from_secs(300),
    )
    .await
    .expect("lock service")
    .expect("lock free");
    let result = deps
        .pipeline
        .index_project(&handler_context(), &request, &mut observer, cancel, &lock)
        .await;
    let outcome = match result {
        Err(indexer::modules::code::IndexError::Failed(
            indexer::handler::HandlerError::Processing(_),
        )) => CancelledRun::FailedProcessing,
        Err(indexer::modules::code::IndexError::NoLane { .. }) => CancelledRun::Backpressure,
        _ => CancelledRun::Other,
    };
    (outcome, clickhouse, deps)
}

async fn cancelled_run_that_finishes_writes_no_checkpoint(incremental: bool) {
    let (outcome, clickhouse, _deps) = run_with_cancelled_token(incremental, false).await;
    assert_eq!(
        outcome,
        CancelledRun::FailedProcessing,
        "a cancelled run must fail with Processing so it is retried, not checkpointed"
    );

    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, "993/993/", 993, "main").await,
        None,
        "a cancelled run must not write a checkpoint"
    );
}

async fn cancelled_lease_wait_is_backpressure(incremental: bool) {
    let (outcome, clickhouse, _deps) = run_with_cancelled_token(incremental, true).await;
    assert_eq!(
        outcome,
        CancelledRun::Backpressure,
        "a lost project lock must stop the lease wait and return NoLane"
    );
    assert_eq!(
        latest_checkpoint_task_id(&clickhouse, "993/993/", 993, "main").await,
        None
    );
}

both_engines! {
    timed_out_job_writes_no_data,
    heartbeat_keeps_a_slow_job_alive_and_indexes_it,
    disk_is_clean_after_a_timed_out_job,
    cancelled_run_that_finishes_writes_no_checkpoint,
    cancelled_lease_wait_is_backpressure,
}

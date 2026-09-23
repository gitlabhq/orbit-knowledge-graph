use std::fmt::Write;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

use anyhow::Context;
use code_graph::v2::dispatch_by_tag;
use code_graph::v2::trace::Tracer;
use code_graph::v2::{
    BatchTx, Decision, FileInventory, FileInventoryEntry, GraphStatsCounters, OnBatch, Pipeline,
    PipelineConfig, PipelineContext,
};
use duckdb_client::DuckDbClient;

use super::assertions::{Severity, TestSuite};
use super::validator::run_suite;

const LOCAL_DDL: &str = include_str!(concat!(env!("CONFIG_DIR"), "/graph_local.sql"));

pub fn create_test_db() -> anyhow::Result<DuckDbClient> {
    let client =
        DuckDbClient::open(Path::new(":memory:")).context("failed to open in-memory DuckDB")?;
    client
        .initialize_schema(LOCAL_DDL)
        .context("failed to initialize local DDL")?;
    Ok(client)
}

fn on_batch_for(client: &Arc<Mutex<DuckDbClient>>) -> Arc<OnBatch> {
    let client = Arc::clone(client);
    Arc::new(
        move |table: &str, batch: arrow::record_batch::RecordBatch| {
            if batch.num_rows() == 0 {
                return Ok(());
            }
            client
                .lock()
                .unwrap()
                .insert_batch(table, &batch)
                .map_err(|e| code_graph::v2::SinkError(format!("DuckDB write to {table}: {e}")))
        },
    )
}

fn workspace_root() -> std::path::PathBuf {
    let output = std::process::Command::new("cargo")
        .args(["metadata", "--format-version=1", "--no-deps"])
        .output()
        .expect("Failed to run cargo metadata");
    let meta: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("Failed to parse cargo metadata");
    std::path::PathBuf::from(meta["workspace_root"].as_str().unwrap())
}

fn copy_dir_recursive(
    src_dir: &std::path::Path,
    dst_dir: &std::path::Path,
    inventory: &mut Vec<FileInventoryEntry>,
) {
    for entry in walkdir::WalkDir::new(src_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let rel = entry.path().strip_prefix(src_dir).unwrap();
        let dst = dst_dir.join(rel);
        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&dst).ok();
        } else {
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            std::fs::copy(entry.path(), &dst)
                .unwrap_or_else(|e| panic!("Failed to copy {}: {e}", entry.path().display()));
            inventory.push(FileInventoryEntry {
                path: rel.to_string_lossy().to_string(),
                size: entry.metadata().map_or(0, |metadata| metadata.len()),
                decision: Decision::Parse,
                label: Default::default(),
            });
        }
    }
}

pub fn run_yaml_suite(yaml: &str) {
    let suite: TestSuite = orbit_utils::yaml::from_str(yaml).expect("Failed to parse YAML suite");
    assert!(
        suite.steps.is_empty(),
        "suite {:?} has incremental steps; only the tree-dsl runner executes them",
        suite.name
    );

    if suite.tests.iter().all(|t| t.skip) {
        eprintln!(
            "[PASS] Suite: {} ({} tests, all skipped)",
            suite.name,
            suite.tests.len()
        );
        return;
    }

    let tmp = tempfile::tempdir().expect("Failed to create temp dir");
    let mut file_inventory = Vec::new();

    if let Some(dir) = &suite.fixture_dir {
        let root = workspace_root();
        let src = root.join(dir);
        assert!(src.is_dir(), "fixture_dir not found: {}", src.display());
        copy_dir_recursive(&src, tmp.path(), &mut file_inventory);
    }

    for fixture in &suite.fixtures {
        let path = tmp.path().join(&fixture.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("Failed to create dir {}: {e}", parent.display()));
        }
        std::fs::write(&path, &fixture.content)
            .unwrap_or_else(|e| panic!("Failed to write {}: {e}", path.display()));
        file_inventory.push(FileInventoryEntry {
            path: fixture.path.clone(),
            size: fixture.content.len() as u64,
            decision: Decision::Parse,
            label: Default::default(),
        });
    }

    let root = tmp.path().to_string_lossy().to_string();

    let trace_any = suite.trace || suite.tests.iter().any(|t| t.debug);
    let tracer = Tracer::new(trace_any);

    let pool = if trace_any {
        Some(
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap(),
        )
    } else {
        None
    };

    let client = Arc::new(Mutex::new(
        create_test_db().expect("Failed to create test DuckDB"),
    ));
    let ontology =
        std::sync::Arc::new(ontology::Ontology::load_embedded().expect("embedded ontology"));
    let converter: Arc<dyn code_graph::v2::GraphConverter> =
        Arc::new(duckdb_client::DuckDbConverter {
            project_id: 1,
            branch: "main".to_string(),
            commit_sha: "test".to_string(),
            ontology: ontology.clone(),
        });

    let pipeline_ctx = match suite.pipeline.as_deref() {
        None | Some("generic") => {
            let config = PipelineConfig::default();
            let on_batch = on_batch_for(&client);
            let inventory: Arc<FileInventory> =
                Arc::new(FileInventory::new(file_inventory.clone()));
            let result = if let Some(pool) = &pool {
                let c = converter.clone();
                let ob = on_batch.clone();
                let inventory = inventory.clone();
                pool.install(move || {
                    Pipeline::run_with_tracer(tmp.path(), inventory, config, tracer, c, ob)
                })
            } else {
                Pipeline::run_with_tracer(
                    tmp.path(),
                    inventory,
                    config,
                    tracer,
                    converter.clone(),
                    on_batch,
                )
            };
            assert!(
                result.errors.is_empty(),
                "Pipeline errors: {:?}",
                result.errors
            );
            result.ctx.clone()
        }
        Some(tag) => {
            let files: Vec<String> = suite
                .fixtures
                .iter()
                .map(|f| format!("{root}/{}", f.path))
                .collect();
            let ctx = Arc::new(PipelineContext {
                config: PipelineConfig::default(),
                tracer,
                root_path: root.clone(),
                skipped: Mutex::new(Vec::new()),
                faults: Mutex::new(Vec::new()),
                file_timings: Mutex::new(Vec::new()),
                language_timings: Mutex::new(Vec::new()),
            });
            let (tx, rx) = crossbeam_channel::unbounded();
            let on_batch = {
                let tx = tx.clone();
                move |table: &str, batch: arrow::record_batch::RecordBatch| {
                    tx.send((table.to_string(), batch))
                        .map_err(|_| code_graph::v2::SinkError("channel closed".into()))
                }
            };
            let dirs = AtomicUsize::new(0);
            let files_count = AtomicUsize::new(0);
            let defs = AtomicUsize::new(0);
            let imps = AtomicUsize::new(0);
            let edgs = AtomicUsize::new(0);
            {
                let errors = Mutex::new(Vec::new());
                let on_batch_ref: &OnBatch = &on_batch;
                let btx = BatchTx::new(
                    on_batch_ref,
                    converter.as_ref(),
                    &errors,
                    GraphStatsCounters::new(&dirs, &files_count, &defs, &imps, &edgs),
                );
                dispatch_by_tag(tag, &files, &ctx, &btx)
                    .unwrap_or_else(|| panic!("unknown pipeline tag: {tag}"))
                    .unwrap_or_else(|e| panic!("pipeline {tag} failed: {e:?}"));
            }
            drop(tx);
            let db = client.lock().unwrap();
            for (table, batch) in rx.try_iter() {
                if batch.num_rows() > 0 {
                    db.insert_batch(&table, &batch)
                        .unwrap_or_else(|e| panic!("insert into {table}: {e}"));
                }
            }
            drop(db);
            ctx
        }
    };

    pipeline_ctx.tracer.dump(&suite.name);

    let db = client.lock().unwrap();
    let failures = run_suite(&suite, &db, &ontology);
    drop(db);

    if failures.is_empty() {
        eprintln!("[PASS] Suite: {} ({} tests)", suite.name, suite.tests.len());
        return;
    }

    let mut msg = format!(
        "\n[FAIL] Suite: {} ({} failures)\n",
        suite.name,
        failures.len()
    );
    for f in &failures {
        writeln!(msg, "  [{}] \"{}\" — {}", f.severity, f.test, f.message).unwrap();
    }

    if failures.iter().any(|f| f.severity == Severity::Error) {
        panic!("{msg}");
    } else {
        eprintln!("{msg}");
    }
}

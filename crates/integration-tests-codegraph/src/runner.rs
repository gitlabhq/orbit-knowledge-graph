use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

use anyhow::Context;
use code_graph::v2::dispatch_by_tag;
use code_graph::v2::trace::Tracer;
use code_graph::v2::{
    BatchTx, GraphStatsCounters, OnBatch, Pipeline, PipelineConfig, PipelineContext,
};
use duckdb_client::DuckDbClient;

use super::validator::{load_suite, report, run_suite, write_suite_files};

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

pub fn run_yaml_suite(yaml: &str) {
    let Some(suite) = load_suite(yaml) else {
        return;
    };
    assert!(
        suite.steps.is_empty(),
        "suite {:?} has incremental steps; only the incremental runner executes them",
        suite.name
    );

    let tmp = tempfile::tempdir().expect("Failed to create temp dir");
    write_suite_files(&suite, tmp.path());

    let vfs = Arc::new(
        orbit_utils::vfs::Vfs::load(
            orbit_utils::vfs::sources::Checkout(tmp.path()),
            code_graph::v2::config::CodeFilter::new(
                None,
                None,
                code_graph::v2::config::detect_language_from_path,
            ),
            orbit_utils::vfs::Limits::default(),
            orbit_utils::vfs::Options::default(),
        )
        .expect("fixture repository"),
    );

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
            let inventory = vfs.clone();
            let result = if let Some(pool) = &pool {
                let c = converter.clone();
                let ob = on_batch.clone();
                let inventory = inventory.clone();
                pool.install(move || Pipeline::run_with_tracer(inventory, config, tracer, c, ob))
            } else {
                Pipeline::run_with_tracer(inventory, config, tracer, converter.clone(), on_batch)
            };
            assert!(
                result.errors.is_empty(),
                "Pipeline errors: {:?}",
                result.errors
            );
            result.ctx.clone()
        }
        Some(tag) => {
            let files: Vec<String> = suite.fixtures.iter().map(|f| f.path.clone()).collect();
            let ctx = Arc::new(PipelineContext::new(vfs, PipelineConfig::default(), tracer));
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
    report(&suite, &failures);
}

use std::path::Path;
use std::sync::{Arc, Mutex};

use compiler::dag::Peer;
use duckdb_client::DuckDbClient;
use duckdb_client::dag::DuckDbExecutor;
use query_data_model::{DuckDb, Packages};

use super::{
    PresetOr, QueryStep, assert_compilation, assert_output, build_redaction, build_security,
    execute_output, read_scenarios, resolve_preset, seed_preset, step_label,
};
use crate::context::TestContext;
use crate::lake::{Lake, Mode};
use crate::scenario::discover;

/// Runs every scenario under `root` the way the server does, on a deployment of the testkit
/// shape `shape`: the ClickHouse graph of `ctx`, and `connection`'s DuckDB peer with its tables
/// landed on a local S3 server in `mode`, seeded once from the `seed` preset.
pub async fn run_lake_dir(
    ctx: &TestContext,
    (root, presets): (&str, &str),
    packages: Packages,
    (shape, connection, seed): (&str, &str, &str),
    mode: Mode,
) {
    let (root, presets) = (Path::new(root), Path::new(presets));
    let fragments = packages
        .fragments()
        .and_then(|fragments| {
            fragments.deployment(&query_data_model::testkit::deployments(), shape)
        })
        .unwrap();
    let graph = crate::versioned_data_model(fragments);
    let lake_catalog = Arc::new(graph.shared_model().physical::<DuckDb>(connection).unwrap());
    let lake = Lake::seed(
        mode,
        &lake_catalog,
        &seed_preset(presets, seed),
        |_, _, declared| declared.to_string(),
    )
    .await;
    let client = DuckDbClient::open_s3(connection, &lake.connection, &lake_catalog)
        .unwrap_or_else(|error| panic!("open {connection}: {error}"));
    let executor = DuckDbExecutor {
        client: Arc::new(Mutex::new(client)),
    };
    let peers = [(Peer::DuckDb(lake_catalog), Arc::new(executor) as _)];
    let mut files = Vec::new();
    discover(root, &mut files);
    files.sort();
    let mut ran = 0;
    for (name, scenario) in files.iter().flat_map(|file| read_scenarios(root, file)) {
        let (config, steps) = scenario.into_steps();
        assert!(
            config.extra_seed.is_empty() && steps.iter().all(|step| step.seed.is_empty()),
            "{name}: lake scenarios read the shared seeds"
        );
        if config.skip.is_some() {
            continue;
        }
        let security = build_security(&resolve_preset(
            "security",
            &config.security,
            presets,
            &name,
        ));
        let redaction = config
            .redaction
            .clone()
            .unwrap_or(PresetOr::Preset("allow_all".into()));
        let redaction = build_redaction(&resolve_preset(
            "redaction",
            &Some(redaction),
            presets,
            &name,
        ));
        for (index, step) in steps.iter().enumerate() {
            let label = format!("{} [{mode:?}]", step_label(&name, index, steps.len()));
            run_step(ctx, &graph, &security, &redaction, &peers, step, &label).await;
        }
        ran += 1;
    }
    assert!(ran > 0, "no lake scenarios under {}", root.display());
}

async fn run_step(
    ctx: &TestContext,
    graph: &Arc<query_data_model::Catalog<query_data_model::ClickHouse>>,
    security: &query_engine::pipeline::SecurityContext,
    redaction: &crate::mock_redaction::MockRedactionService,
    peers: &[(Peer, Arc<dyn query_engine::shared::dag::Executor>)],
    step: &QueryStep,
    label: &str,
) {
    let expect = &step.expect;
    assert!(
        expect.pages.is_empty() && !expect.compile_only,
        "{label}: lake scenarios run one page"
    );
    let output = execute_output(ctx, &step.query, graph, security, redaction, peers).await;
    assert_compilation(Ok::<_, String>(Arc::clone(&output.compiled)), expect, label);
    assert_output(&output, expect, label);
}

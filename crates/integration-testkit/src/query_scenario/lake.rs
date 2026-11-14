use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::{Arc, Mutex};

use futures::FutureExt;

use compiler::dag::{Backend, Connections, Peer};
use orbit_server::pipeline::PeerExecutors;
use orbit_server_config::ConnectionsConfig;
use query_data_model::plan::live::{Workspaces, workspace};
use query_data_model::{Catalog, DuckDb, Packages};
use query_engine::shared::dag::Executor;

use super::{
    PresetOr, QueryStep, assert_compilation, assert_output, build_redaction, build_security,
    read_scenarios, resolve_preset, seed_preset, settle, step_label, try_execute_output,
};
use crate::context::TestContext;
use crate::dag_fakes::{Recorded, seeded_live};
use crate::lake::{Lake, Mode};
use crate::scenario::discover;

/// Runs every scenario under `root` the way the server does, on a deployment of the testkit
/// shape `shape`: the ClickHouse graph of `ctx`, each DuckDB peer with its tables landed on one
/// local S3 server in `mode`, seeded once from the `seed` preset, and the live peer on the
/// seeded Data Extraction Service, signed in as `alice`, recording the calls each step sends.
pub async fn run_lake_dir(
    ctx: &TestContext,
    (root, presets): (&str, &str),
    packages: Packages,
    (shape, seed): (&str, &str),
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
    let catalogs: Vec<Catalog<DuckDb>> = Connections::of(graph.shared_model())
        .unwrap()
        .peers()
        .iter()
        .filter(|peer| peer.backend() == Backend::DuckDb)
        .map(|peer| {
            graph
                .shared_model()
                .physical::<DuckDb>(peer.connection())
                .unwrap()
        })
        .collect();
    let lake = Lake::seed(
        mode,
        &catalogs.iter().collect::<Vec<_>>(),
        &seed_preset(presets, seed),
        |_, _, declared| declared.to_string(),
    )
    .await;
    let config = ConnectionsConfig {
        duckdb: lake.connections.clone(),
    };
    let live = Arc::new(Recorded {
        inner: seeded_live("alice"),
        calls: Mutex::default(),
    });
    let remote = |peer: &Peer| {
        (peer.backend() == Backend::Live).then(|| Arc::clone(&live) as Arc<dyn Executor>)
    };
    let peers = PeerExecutors::open_with(graph.shared_model(), &config, remote)
        .unwrap_or_else(|error| panic!("open the {shape} peers: {error}"));
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
        let label = format!("{name} [{mode:?}]");
        let outcome = AssertUnwindSafe(async {
            for (index, step) in steps.iter().enumerate() {
                let label = format!("{} [{mode:?}]", step_label(&name, index, steps.len()));
                run_step(
                    ctx,
                    &graph,
                    &security,
                    &redaction,
                    (&peers, &live),
                    step,
                    &label,
                )
                .await;
            }
        })
        .catch_unwind()
        .await;
        settle(outcome, config.expected_failure.as_ref(), &label);
        ran += 1;
    }
    assert!(ran > 0, "no lake scenarios under {}", root.display());
}

async fn run_step(
    ctx: &TestContext,
    graph: &Arc<query_data_model::Catalog<query_data_model::ClickHouse>>,
    security: &query_engine::pipeline::SecurityContext,
    redaction: &crate::mock_redaction::MockRedactionService,
    (peers, live): (&PeerExecutors, &Recorded),
    step: &QueryStep,
    label: &str,
) {
    let expect = &step.expect;
    assert!(
        expect.pages.is_empty() && !expect.compile_only,
        "{label}: lake scenarios run one page"
    );
    live.calls.lock().unwrap().clear();
    let output = try_execute_output(ctx, &step.query, graph, security, redaction, peers).await;
    let compiled = output.as_ref().map(|output| Arc::clone(&output.compiled));
    if assert_compilation(compiled, expect, label).is_some() {
        assert_output(&output.expect("the query ran"), expect, label);
    }
    if let Some(expected) = &expect.live_calls {
        let sent: Vec<Vec<serde_json::Value>> = live
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| match &call.workspaces {
                Workspaces::Only(keys) => keys.iter().map(workspace).collect(),
                Workspaces::Any => panic!("{label}: a live call names no workspace"),
            })
            .collect();
        assert_eq!(&sent, expected, "{label}: live calls mismatch");
    }
}

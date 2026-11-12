use std::path::Path;
use std::sync::{Arc, Mutex};

use duckdb_client::DuckDbClient;
use orbit_server::redaction::QueryResult;
use query_data_model::testkit::limits;
use query_data_model::{Catalog, DuckDb};
use query_engine::compiler::orbit_authorization::Grants;

use super::{
    QueryStep, assert_compilation, assert_output, build_security, pipeline_output, read_scenarios,
    resolve_preset, seed_preset, step_label,
};
use crate::lake::{Lake, Mode};
use crate::scenario::discover;

/// Runs every scenario under `root` against `connection`'s tables landed on a local S3 server in
/// `mode`, seeded once from the `seed` preset; each scenario's caller comes from its security
/// preset.
pub async fn run_lake_dir(root: &str, presets: &str, seed: &str, connection: &str, mode: Mode) {
    let (root, presets) = (Path::new(root), Path::new(presets));
    let model = query_data_model::testkit::with_demo().load().unwrap();
    let catalog = Arc::new(model.physical::<DuckDb>(connection).unwrap());
    let lake = Lake::seed(
        mode,
        &catalog,
        &seed_preset(presets, seed),
        |_, _, declared| declared.to_string(),
    )
    .await;
    let client = DuckDbClient::open_s3(connection, &lake.connection, &catalog)
        .unwrap_or_else(|error| panic!("open {connection}: {error}"));
    let client = Arc::new(Mutex::new(client));
    let authorization = orbit_server_config::AppConfig::embedded_defaults().authorization;
    let mut files = Vec::new();
    discover(root, &mut files);
    files.sort();
    let mut ran = 0;
    for (name, scenario) in files.iter().flat_map(|file| read_scenarios(root, file)) {
        let (config, steps) = scenario.into_steps();
        assert!(
            config.extra_seed.is_empty() && steps.iter().all(|step| step.seed.is_empty()),
            "{name}: lake scenarios read the shared seed"
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
        let grants = orbit_server::authorization::gitlab::session_grants(
            &authorization,
            catalog.model(),
            security,
        )
        .unwrap_or_else(|error| panic!("{name}: grants: {error}"));
        for (index, step) in steps.iter().enumerate() {
            let label = format!("{} [{mode:?}]", step_label(&name, index, steps.len()));
            run_step(&client, &catalog, &grants, step, &label).await;
        }
        ran += 1;
    }
    assert!(ran > 0, "no lake scenarios under {}", root.display());
}

async fn run_step(
    client: &Arc<Mutex<DuckDbClient>>,
    catalog: &Arc<Catalog<DuckDb>>,
    grants: &Grants,
    step: &QueryStep,
    label: &str,
) {
    let expect = &step.expect;
    assert!(
        expect.pages.is_empty(),
        "{label}: lake scenarios read one page"
    );
    let outcome = duckdb_client::dag::run(client, catalog, &step.query, grants, limits()).await;
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(error) => {
            assert_compilation(Err::<Arc<_>, _>(error), expect, label);
            return;
        }
    };
    let compiled = Arc::new(outcome.compiled);
    if assert_compilation(Ok::<_, String>(Arc::clone(&compiled)), expect, label).is_none()
        || expect.compile_only
    {
        return;
    }
    let result =
        QueryResult::from_batches(&outcome.executed.batches, &compiled.base.result_context)
            .unwrap();
    let output = pipeline_output(
        &compiled,
        result,
        0,
        compiled.base.result_context.clone(),
        compiled.base.render(),
    );
    assert_output(&output, expect, label);
}

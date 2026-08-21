//! Dispatcher coverage lives in the YAML scenarios under
//! `tests/indexer/scenarios/dispatch/`, executed by `dispatch_scenarios`.

use std::sync::Arc;

use super::common;

#[tokio::test]
async fn dispatch_scenarios() {
    let ctx =
        common::TestContext::new(&[common::SIPHON_SCHEMA_SQL, *common::GRAPH_SCHEMA_SQL]).await;
    let (_nats, nats_url) = common::dispatch::start_nats().await;
    integration_testkit::scenario::run_dir(
        &ctx,
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/indexer/scenarios/dispatch"
        ),
        Arc::new(common::dispatch::DispatchScenarioHandlers::new(nats_url)),
    )
    .await;
}

#[tokio::test]
async fn dispatch_scenarios_with_a_mismatch_between_downstream_schema_and_orbit() {
    let sbom_without_traversal_path = "
        DROP TABLE siphon_sbom_component_versions;
        CREATE TABLE siphon_sbom_component_versions (
            id Int64,
            _siphon_replicated_at DateTime64(6, 'UTC') DEFAULT now64(6, 'UTC'),
            _siphon_watermark DateTime64(6, 'UTC') DEFAULT _siphon_replicated_at,
            _siphon_deleted Bool DEFAULT FALSE
        ) ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted) ORDER BY id;
    ";
    let ctx = common::TestContext::new(&[
        common::SIPHON_SCHEMA_SQL,
        *common::GRAPH_SCHEMA_SQL,
        sbom_without_traversal_path,
    ])
    .await;
    let (_nats, nats_url) = common::dispatch::start_nats().await;
    integration_testkit::scenario::run_dir(
        &ctx,
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/indexer/scenarios/dispatch_degraded"
        ),
        Arc::new(common::dispatch::DispatchScenarioHandlers::new(nats_url)),
    )
    .await;
}

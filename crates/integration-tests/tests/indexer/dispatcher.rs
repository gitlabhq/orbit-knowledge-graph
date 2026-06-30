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

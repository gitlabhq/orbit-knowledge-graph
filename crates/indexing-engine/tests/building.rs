mod support;

use std::time::Duration;

use async_trait::async_trait;
use indexing_engine::prelude::*;
use indexing_engine::{BuildError, Engine, EngineConfig, LaneConfig, PoolConfig};
use support::{Script, StatusBoard, SummaryPlanner, fixtures, register};

struct SecondDocStatus;

#[async_trait]
impl Reconciler for SecondDocStatus {
    const NAME: &'static str = "doc.status.second";
    const WRITES: &'static [&'static str] = &["doc_status"];
    const EVERY: Duration = Duration::from_secs(60);

    async fn reconcile(&self, _: &ReconcileContext) -> Result<(), ReconcileError> {
        Ok(())
    }
}

fn register_docs(engine: &mut EngineBuilder) {
    register(
        engine,
        &fixtures(Script::default(), vec![], StatusBoard::default()),
    );
}

fn build_with(config: EngineConfig) -> Result<Engine, BuildError> {
    Engine::builder()
        .config(config)
        .module(register_docs)
        .build()
}

fn lane() -> LaneConfig {
    LaneConfig::new(4, Duration::from_secs(60))
}

#[test]
fn builds_a_valid_engine() {
    let config = EngineConfig::default()
        .with_lane("doc.index/default", lane().on_timeout("doc.index/big"))
        .with_lane("doc.index/big", lane());

    assert!(build_with(config).is_ok());
}

#[test]
fn rejects_two_workers_with_one_name() {
    let built = Engine::builder()
        .module(register_docs)
        .module(register_docs)
        .build();

    assert!(matches!(
        built,
        Err(BuildError::DuplicateName { kind: "worker", .. })
    ));
}

#[test]
fn rejects_a_config_lane_of_an_unknown_worker() {
    let built = build_with(EngineConfig::default().with_lane("doc.unknown/default", lane()));

    assert!(matches!(built, Err(BuildError::UnknownLaneWorker { .. })));
}

#[test]
fn rejects_a_config_lane_without_a_worker() {
    let built = build_with(EngineConfig::default().with_lane("default", lane()));

    assert!(matches!(built, Err(BuildError::InvalidLane { .. })));
}

#[test]
fn rejects_an_on_timeout_lane_of_another_worker() {
    let lane = lane().on_timeout("summary.build/big");
    let built = build_with(EngineConfig::default().with_lane("doc.index/default", lane));

    assert!(matches!(built, Err(BuildError::UnknownTimeoutLane { .. })));
}

#[test]
fn rejects_a_config_pool_that_no_module_registered() {
    let config = EngineConfig::default().with_pool("unknown", PoolConfig::Pod(4));

    assert!(matches!(
        build_with(config),
        Err(BuildError::UnregisteredPool { .. })
    ));
}

#[test]
fn rejects_a_done_trigger_on_a_worker_that_no_module_registered() {
    let built = Engine::builder()
        .module(|engine| {
            engine.planner(SummaryPlanner);
        })
        .build();

    assert!(matches!(
        built,
        Err(BuildError::UnregisteredDoneSource { .. })
    ));
}

#[test]
fn rejects_two_reconcilers_that_write_one_table() {
    let built = Engine::builder()
        .module(register_docs)
        .module(|engine| {
            engine.reconciler(SecondDocStatus);
        })
        .build();

    assert!(matches!(
        built,
        Err(BuildError::SharedWrite {
            table: "doc_status",
            ..
        })
    ));
}

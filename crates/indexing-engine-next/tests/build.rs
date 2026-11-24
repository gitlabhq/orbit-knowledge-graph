mod support;

use std::time::Duration;

use async_trait::async_trait;
use indexing_engine_next::prelude::*;
use indexing_engine_next::{BuildError, Engine, EngineConfig, LaneConfig, PoolConfig};
use support::{DownstreamPlanner, TestPlanner, TestWorker, nonzero};

struct FirstStatusWriter;

struct SecondStatusWriter;

#[async_trait]
impl Reconciler for FirstStatusWriter {
    const NAME: &'static str = "first.status.writer";
    const WRITES: &'static [&'static str] = &["task_status"];
    const EVERY: Duration = Duration::from_secs(60);

    async fn reconcile(&self, _: &ReconcileContext) -> Result<(), ReconcileError> {
        Ok(())
    }
}

#[async_trait]
impl Reconciler for SecondStatusWriter {
    const NAME: &'static str = "second.status.writer";
    const WRITES: &'static [&'static str] = &["task_status"];
    const EVERY: Duration = Duration::from_secs(60);

    async fn reconcile(&self, _: &ReconcileContext) -> Result<(), ReconcileError> {
        Ok(())
    }
}

fn build_test_flow(config: EngineConfig) -> Option<BuildError> {
    Engine::builder()
        .config(config)
        .module(|engine| {
            engine
                .worker(TestWorker::default())
                .planner(TestPlanner::default());
        })
        .build()
        .err()
}

fn lane() -> LaneConfig {
    LaneConfig::new(nonzero(4), Duration::from_secs(60))
}

#[test]
fn a_name_must_not_be_empty_or_hold_a_slash_or_a_space() {
    for name in ["", "parse/big", "parse big"] {
        let built = Engine::builder()
            .module(|engine| {
                engine.pool(name);
            })
            .build();

        assert_eq!(
            built.err(),
            Some(BuildError::InvalidName {
                kind: "pool",
                name: name.to_owned()
            })
        );
    }
}

#[test]
fn two_workers_must_not_share_a_name() {
    let built = Engine::builder()
        .module(|engine| {
            engine
                .worker(TestWorker::default())
                .worker(TestWorker::default());
        })
        .build();

    assert_eq!(
        built.err(),
        Some(BuildError::DuplicateName {
            kind: "worker",
            name: "test.worker".to_owned()
        })
    );
}

#[test]
fn a_config_lane_must_look_like_worker_slash_lane() {
    let built = build_test_flow(EngineConfig::default().with_lane("default", lane()));

    assert_eq!(
        built,
        Some(BuildError::InvalidLane {
            lane: "default".to_owned()
        })
    );
}

#[test]
fn a_config_lane_must_belong_to_a_registered_worker() {
    let config = EngineConfig::default().with_lane("unknown.worker/default", lane());

    assert_eq!(
        build_test_flow(config),
        Some(BuildError::UnknownLaneWorker {
            lane: "unknown.worker/default".to_owned()
        })
    );
}

#[test]
fn an_on_timeout_lane_must_belong_to_the_same_worker() {
    let config = EngineConfig::default().with_lane(
        "test.worker/default",
        lane().on_timeout("downstream.worker/big"),
    );

    assert_eq!(
        build_test_flow(config),
        Some(BuildError::UnknownTimeoutLane {
            lane: "test.worker/default".to_owned(),
            on_timeout: "downstream.worker/big".to_owned()
        })
    );
}

#[test]
fn a_config_pool_must_be_registered_by_a_module() {
    let config = EngineConfig::default().with_pool("parse", PoolConfig::Pod(nonzero(4)));

    assert_eq!(
        build_test_flow(config),
        Some(BuildError::UnregisteredPool {
            pool: "parse".to_owned()
        })
    );
}

#[test]
fn a_done_trigger_must_wait_on_a_registered_worker() {
    let built = Engine::builder()
        .module(|engine| {
            engine.planner(DownstreamPlanner::default());
        })
        .build();

    assert_eq!(
        built.err(),
        Some(BuildError::UnregisteredDoneSource {
            planner: "downstream.planner",
            worker: "test.worker"
        })
    );
}

#[test]
fn two_reconcilers_must_not_write_one_table() {
    let built = Engine::builder()
        .module(|engine| {
            engine
                .reconciler(FirstStatusWriter)
                .reconciler(SecondStatusWriter);
        })
        .build();

    assert_eq!(
        built.err(),
        Some(BuildError::SharedWrite {
            table: "task_status",
            first: "first.status.writer",
            second: "second.status.writer"
        })
    );
}

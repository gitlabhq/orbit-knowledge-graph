mod support;

use std::time::Duration;

use async_trait::async_trait;
use indexing_engine::prelude::*;
use indexing_engine::{BuildError, Engine, EngineConfig, LaneConfig, PoolConfig};
use support::{Flows, register_summaries};

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

fn build_docs(config: EngineConfig) -> Option<BuildError> {
    let flows = Flows::default();
    Engine::builder()
        .config(config)
        .module(|engine| flows.register_docs(engine))
        .build()
        .err()
}

fn lane() -> LaneConfig {
    LaneConfig::new(4, Duration::from_secs(60))
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
    let flows = Flows::default();

    let built = Engine::builder()
        .module(|engine| flows.register_docs(engine))
        .module(|engine| flows.register_docs(engine))
        .build();

    assert_eq!(
        built.err(),
        Some(BuildError::DuplicateName {
            kind: "worker",
            name: "doc.index".to_owned()
        })
    );
}

#[test]
fn a_config_lane_must_look_like_worker_slash_lane() {
    let built = build_docs(EngineConfig::default().with_lane("default", lane()));

    assert_eq!(
        built,
        Some(BuildError::InvalidLane {
            lane: "default".to_owned()
        })
    );
}

#[test]
fn a_config_lane_must_belong_to_a_registered_worker() {
    let built = build_docs(EngineConfig::default().with_lane("doc.unknown/default", lane()));

    assert_eq!(
        built,
        Some(BuildError::UnknownLaneWorker {
            lane: "doc.unknown/default".to_owned()
        })
    );
}

#[test]
fn an_on_timeout_lane_must_belong_to_the_same_worker() {
    let config = EngineConfig::default()
        .with_lane("doc.index/default", lane().on_timeout("summary.build/big"));

    assert_eq!(
        build_docs(config),
        Some(BuildError::UnknownTimeoutLane {
            lane: "doc.index/default".to_owned(),
            on_timeout: "summary.build/big".to_owned()
        })
    );
}

#[test]
fn a_config_pool_must_be_registered_by_a_module() {
    let config = EngineConfig::default().with_pool("unknown", PoolConfig::Pod(4));

    assert_eq!(
        build_docs(config),
        Some(BuildError::UnregisteredPool {
            pool: "unknown".to_owned()
        })
    );
}

#[test]
fn a_done_trigger_must_wait_on_a_registered_worker() {
    let built = Engine::builder().module(register_summaries).build();

    assert_eq!(
        built.err(),
        Some(BuildError::UnregisteredDoneSource {
            planner: "summary.planner",
            worker: "doc.index"
        })
    );
}

#[test]
fn two_reconcilers_must_not_write_one_table() {
    let flows = Flows::default();

    let built = Engine::builder()
        .module(|engine| flows.register_docs(engine))
        .module(|engine| {
            engine.reconciler(SecondDocStatus);
        })
        .build();

    assert_eq!(
        built.err(),
        Some(BuildError::SharedWrite {
            table: "doc_status",
            first: "doc.status",
            second: "doc.status.second"
        })
    );
}

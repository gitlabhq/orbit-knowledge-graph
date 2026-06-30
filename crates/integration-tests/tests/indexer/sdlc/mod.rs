//! Entity-ETL coverage lives in the YAML scenarios under
//! `tests/indexer/scenarios/sdlc/`, executed by `scenario_indexing`. The Rust
//! subtests below cover the partitioning mechanics the scenario format does not
//! model: per-run partition count and filtered checkpoint assertions (live vs
//! tombstoned rows by key prefix).
//!
//! Each `#[tokio::test]` starts a single ClickHouse container and runs all
//! subtests in parallel, forking an isolated database per subtest to avoid
//! cross-test contamination while eliminating per-test container startup overhead.

mod partitioning;

use std::sync::Arc;

use super::common::scenarios::SdlcScenarioHandlers;
use super::common::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext};
use integration_testkit::run_subtests;

#[tokio::test]
async fn scenario_indexing() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    integration_testkit::scenario::run_dir(
        &ctx,
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/indexer/scenarios/sdlc"),
        Arc::new(SdlcScenarioHandlers),
    )
    .await;
}

#[tokio::test]
async fn global_indexing() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    run_subtests!(
        &ctx,
        partitioning::partitioned_initial_load_indexes_all_rows_and_consolidates,
        partitioning::retry_skips_completed_resumes_in_progress_and_pins_watermark,
        partitioning::unfinished_partition_blocks_parent_consolidation,
        partitioning::present_parent_takes_single_pull_path_and_honors_floor,
        partitioning::span_smaller_than_partition_count_falls_back_to_single_run,
    );
}

#[tokio::test]
async fn namespace_indexing() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    run_subtests!(
        &ctx,
        partitioning::namespaced_entities_partition_by_id_within_scope,
    );
}

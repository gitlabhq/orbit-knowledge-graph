//! Consolidated SDLC integration tests.
//!
//! Entity-ETL coverage lives in the YAML scenarios under
//! `tests/indexer/scenarios/sdlc/`, executed by `scenario_indexing`. The
//! Rust subtests below cover behavioral mechanics the scenario format does
//! not model: watermarks, cursors, partitioning, and checkpoint paging.
//!
//! Each `#[tokio::test]` starts a single ClickHouse container and runs all
//! subtests in parallel, forking an isolated database per subtest to avoid
//! cross-test contamination while eliminating per-test container startup overhead.

mod global;
mod partitioning;
mod system_notes;
mod watermarking;
mod work_items;

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
        global::uses_watermark_for_incremental_processing,
        global::resumes_from_saved_cursor_skipping_processed_users,
        global::incomplete_checkpoint_does_not_advance_watermark_on_resume,
        global::resume_is_bounded_by_window_floor,
        global::resume_is_bounded_by_window_floor_for_partitioned_entity,
        partitioning::partitioned_initial_load_indexes_all_rows_and_consolidates,
        partitioning::incomplete_partition_checkpoint_does_not_advance_watermark_on_resume,
        partitioning::unfinished_partition_blocks_parent_consolidation,
        partitioning::second_run_after_consolidation_skips_partitioning,
        partitioning::skips_already_completed_partitions_on_retry,
        partitioning::all_partitions_completed_runs_consolidate_only,
        partitioning::span_smaller_than_partition_count_falls_back_to_single_run,
    );
}

#[tokio::test]
async fn namespace_indexing() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    run_subtests!(
        &ctx,
        system_notes::checkpoint_advances_after_draining_paged_window,
        system_notes::incremental_run_skips_already_processed_notes,
        work_items::clamps_out_of_range_due_date_to_null,
        watermarking::uses_watermark_for_incremental_processing,
        watermarking::resumes_from_saved_cursor_skipping_processed_groups,
        partitioning::namespaced_entity_partitions_by_id_within_scope,
        partitioning::query_etl_entity_partitions_by_id_within_scope,
    );
}

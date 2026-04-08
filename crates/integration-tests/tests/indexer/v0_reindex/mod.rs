//! Integration tests verifying that post-V0-reset re-indexing works via the existing pipeline.
//!
//! After a drop-and-recreate schema reset all graph tables are empty and checkpoints are gone.
//! These tests prove that the standard dispatch pipeline handles this state correctly — no new
//! re-indexing machinery is needed. Re-enabling namespaces in Rails is indistinguishable from
//! enabling them for the first time, so the pipeline handles both cases identically.
//!
//! Test coverage:
//! - SDLC re-indexing starts from scratch when checkpoints are absent (namespace data)
//! - Global re-indexing (Users) works correctly after reset
//! - Namespace deletion schedule rebuilds on next scheduler cycle after reset
//! - End-to-end: index → schema reset → re-enable → re-index → all data present

use crate::indexer::common::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext};
use integration_testkit::run_subtests;

mod deletion_schedule_rebuild;
mod end_to_end;
mod global_reindex;
mod sdlc_reindex;

/// SDLC re-indexing starts from watermark epoch-zero when the checkpoint table is empty,
/// so all datalake data is picked up after a reset.
#[tokio::test]
async fn sdlc_reindex_after_reset() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    run_subtests!(
        &ctx,
        sdlc_reindex::namespace_data_reindexed_with_empty_checkpoints,
        sdlc_reindex::checkpoint_written_after_reindex,
        sdlc_reindex::second_reindex_cycle_is_incremental,
    );
}

/// Global (non-namespaced) entities are re-indexed on the next dispatch cycle regardless of
/// checkpoint state — the GlobalHandler starts from epoch-zero when no checkpoint exists.
#[tokio::test]
async fn global_reindex_after_reset() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    run_subtests!(
        &ctx,
        global_reindex::user_data_reindexed_with_empty_checkpoints,
    );
}

/// After a reset, the namespace_deletion_schedule table is empty. The NamespaceDeletionScheduler
/// uses a checkpoint to track newly-disabled namespaces, and that checkpoint is also gone. The
/// scheduler therefore scans from epoch-zero on the next cycle, picking up any namespaces that
/// became disabled after the reset.
///
/// This test runs sequentially (not subtested) because `siphon_knowledge_graph_enabled_namespaces`
/// is defined with an explicit `test.` database prefix in `siphon.sql`, which prevents it from
/// being created in forked databases by the `run_subtests!` macro.
#[tokio::test]
async fn deletion_schedule_rebuild_after_reset() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    deletion_schedule_rebuild::scheduler_scans_from_epoch_zero_after_reset(&ctx).await;
}

/// Full end-to-end V0 cycle: index → reset → namespace re-enable → re-index → verify.
/// This test runs sequentially (not subtested) because it exercises stateful stages.
#[tokio::test]
async fn end_to_end_v0_cycle() {
    end_to_end::full_v0_cycle().await;
}

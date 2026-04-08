//! Verify that global (non-namespaced) entities are re-indexed after a V0 reset.
//!
//! The GlobalHandler runs unconditionally on every dispatch cycle and reads from
//! `checkpoint` with key `global.<Entity>`. After a reset the checkpoint table is empty,
//! so the handler starts from the Unix epoch and picks up all rows in the datalake.

use crate::indexer::common::{
    TestContext, assert_node_count, global_envelope, global_handler, handler_context,
};

/// After a V0 reset (checkpoint table empty), the GlobalHandler re-indexes all User rows.
pub async fn user_data_reindexed_with_empty_checkpoints(ctx: &TestContext) {
    // Populate the datalake with users.
    ctx.execute(
        "INSERT INTO siphon_users \
         (id, username, email, name, first_name, last_name, state, \
          public_email, preferred_language, last_activity_on, private_profile, \
          admin, auditor, external, user_type, created_at, updated_at, _siphon_replicated_at) \
         VALUES \
         (1, 'alice', 'alice@example.com', 'Alice', 'Alice', '', 'active', \
          '', 'en', '2024-01-01', false, false, false, false, 0, \
          '2023-01-01', '2024-01-01', '2024-01-20 12:00:00'), \
         (2, 'bob', 'bob@example.com', 'Bob', 'Bob', '', 'active', \
          '', 'en', '2024-01-01', false, false, false, false, 0, \
          '2023-01-01', '2024-01-01', '2024-01-20 12:00:00')",
    )
    .await;

    // No checkpoint row exists (simulating post-reset state). Run the GlobalHandler.
    global_handler(ctx)
        .await
        .handle(handler_context(ctx), global_envelope())
        .await
        .expect("global handler should succeed with empty checkpoints");

    // Both users must be indexed — the handler started from epoch-zero.
    assert_node_count(ctx, "gl_user", 2).await;
}

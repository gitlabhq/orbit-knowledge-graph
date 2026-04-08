//! Verify that the namespace deletion schedule rebuilds correctly after a V0 reset.
//!
//! After a reset the `namespace_deletion_schedule` and `checkpoint` tables are both empty.
//! The `NamespaceDeletionScheduler` uses a checkpoint keyed `namespace_deletion_scheduler`
//! to track its last scan watermark. With no checkpoint row it defaults to the Unix epoch,
//! which means it will scan for namespaces that became disabled at any point in time.
//!
//! The key invariant: before a V0 reset all namespaces must be disabled. After the reset,
//! namespaces are re-enabled fresh. So on the first post-reset scheduler cycle there should
//! be no newly-deleted namespace events to schedule (they were all re-enabled), which means
//! the schedule starts empty and is rebuilt organically as namespaces are eventually disabled.

use std::sync::Arc;

use clickhouse_client::ClickHouseConfigurationExt;
use gkg_server_config::NamespaceDeletionSchedulerConfig;
use indexer::checkpoint::ClickHouseCheckpointStore;
use indexer::modules::namespace_deletion::{
    ClickHouseNamespaceDeletionStore, NamespaceDeletionScheduler,
};
use indexer::scheduler::{ScheduledTask, ScheduledTaskMetrics};
use indexer::testkit::MockNatsServices;

use crate::indexer::common::TestContext;

/// After a V0 reset, the scheduler runs from epoch-zero with no prior checkpoint.
/// When all namespaces are currently enabled (post-re-enable), the scan finds no
/// newly-deleted namespaces and the schedule table remains empty.
pub async fn scheduler_scans_from_epoch_zero_after_reset(ctx: &TestContext) {
    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");

    // Simulate datalake state: namespace 100 is enabled (not deleted).
    ctx.execute(
        "INSERT INTO siphon_namespaces (id, name, path, organization_id, created_at, updated_at, _siphon_replicated_at) \
         VALUES (100, 'test-ns', 'test-ns', 1, now(), now(), now())",
    )
    .await;
    ctx.execute(
        "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
         (id, root_namespace_id, _siphon_deleted, _siphon_replicated_at, created_at, updated_at) \
         VALUES (1, 100, false, now(), now(), now())",
    )
    .await;

    // No checkpoint exists (simulating post-reset state).
    let graph = Arc::new(ctx.config.build_client());
    let datalake = Arc::new(ctx.config.build_client());

    let store = Arc::new(ClickHouseNamespaceDeletionStore::new(
        datalake,
        graph.clone(),
        &ontology,
    ));
    let checkpoint_store = Arc::new(ClickHouseCheckpointStore::new(graph));
    let nats = Arc::new(MockNatsServices::new());

    let scheduler = NamespaceDeletionScheduler::new(
        store,
        checkpoint_store,
        nats.clone(),
        ScheduledTaskMetrics::new(),
        NamespaceDeletionSchedulerConfig::default(),
    );

    // Run must succeed without error.
    scheduler
        .run()
        .await
        .expect("scheduler should run successfully after reset with no prior checkpoint");

    // Since the namespace is currently enabled (not deleted), no deletion should be scheduled.
    let result = ctx
        .query("SELECT count() AS cnt FROM namespace_deletion_schedule FINAL")
        .await;
    let count = result[0]
        .column_by_name("cnt")
        .expect("cnt column")
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("UInt64")
        .value(0);
    assert_eq!(
        count, 0,
        "no deletion schedule entries expected when namespace is currently enabled"
    );

    // A checkpoint must be written so the next cycle is incremental.
    let cp_result = ctx
        .query(
            "SELECT count() AS cnt FROM checkpoint FINAL \
             WHERE key = 'namespace_deletion_scheduler' AND _deleted = false",
        )
        .await;
    let cp_count = cp_result[0]
        .column_by_name("cnt")
        .expect("cnt column")
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("UInt64")
        .value(0);
    assert_eq!(
        cp_count, 1,
        "checkpoint must be written after scheduler run so next cycle is incremental"
    );
}

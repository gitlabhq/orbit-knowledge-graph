use std::sync::Arc;

use clickhouse_client::{ClickHouseConfigurationExt, FromArrowColumn};
use gkg_server_config::StaleEdgeReconciliationConfig;
use indexer::checkpoint::ClickHouseCheckpointStore;
use indexer::orchestrator::scheduled::stale_edge_reconciliation::StaleEdgeReconciliation;
use indexer::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics};
use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, run_subtests, t};

// Edge `_version` is in the past so the reconcile tombstone (now64()) supersedes
// it under ReplacingMergeTree; the owner `_version` is in the future so it always
// lands in the `_version >= cursor` changed set, including the idempotency re-run
// after the cursor has advanced.
const PAST: &str = "2024-01-01 00:00:00.000000";
const FUTURE: &str = "2099-01-01 00:00:00.000000";
const TRAVERSAL_PATH: &str = "1/9970/";

#[tokio::test]
async fn stale_edge_reconciliation() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    run_subtests!(
        &ctx,
        reconciles_has_latest_diff,
        reconciles_has_head_pipeline,
        reconciles_last_edited_by,
        reconciles_updated_by,
        reconciles_in_milestone_for_merge_request,
        reconciles_in_milestone_for_work_item,
        reconcile_is_idempotent,
        leaves_unrelated_owner_edges_untouched,
    );
}

#[derive(Clone)]
struct Case {
    relationship_kind: &'static str,
    owner_table: &'static str,
    owner_id: i64,
    owner_fk_column: &'static str,
    edge_table: &'static str,
    source_kind: &'static str,
    target_kind: &'static str,
    /// Which endpoint the owner's FK value occupies: `true` = source, `false` = target.
    owner_is_source: bool,
    current_fk: i64,
    stale_fk: i64,
}

impl Case {
    fn endpoints(&self, other: i64) -> (i64, i64) {
        if self.owner_is_source {
            (self.owner_id, other)
        } else {
            (other, self.owner_id)
        }
    }

    fn owner_endpoint_column(&self) -> &str {
        if self.owner_is_source {
            "source_id"
        } else {
            "target_id"
        }
    }

    fn other_endpoint_column(&self) -> &str {
        if self.owner_is_source {
            "target_id"
        } else {
            "source_id"
        }
    }
}

fn task(context: &TestContext) -> StaleEdgeReconciliation {
    let checkpoint_store = Arc::new(ClickHouseCheckpointStore::new(Arc::new(
        context.config.build_client(),
    )));
    StaleEdgeReconciliation::new(
        context.config.build_client(),
        &ontology::Ontology::load_embedded().unwrap(),
        checkpoint_store,
        ScheduledTaskMetrics::new(),
        StaleEdgeReconciliationConfig::default(),
    )
}

async fn seed(context: &TestContext, case: &Case) {
    context
        .execute(&format!(
            "INSERT INTO {} (id, traversal_path, {}, _version, _deleted) \
             VALUES ({}, '{TRAVERSAL_PATH}', {}, '{FUTURE}', false)",
            t(case.owner_table),
            case.owner_fk_column,
            case.owner_id,
            case.current_fk,
        ))
        .await;

    for other in [case.stale_fk, case.current_fk] {
        let (source_id, target_id) = case.endpoints(other);
        context
            .execute(&format!(
                "INSERT INTO {} \
                 (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted) \
                 VALUES ('{TRAVERSAL_PATH}', {source_id}, '{}', '{}', {target_id}, '{}', '{PAST}', false)",
                t(case.edge_table),
                case.source_kind,
                case.relationship_kind,
                case.target_kind,
            ))
            .await;
    }
}

async fn live_other_endpoints(context: &TestContext, case: &Case) -> Vec<i64> {
    let result = context
        .query(&format!(
            "SELECT {other} FROM {table} FINAL \
             WHERE {owner} = {owner_id} AND relationship_kind = '{kind}' \
               AND source_kind = '{sk}' AND target_kind = '{tk}' AND _deleted = false \
             ORDER BY {other}",
            other = case.other_endpoint_column(),
            table = t(case.edge_table),
            owner = case.owner_endpoint_column(),
            owner_id = case.owner_id,
            kind = case.relationship_kind,
            sk = case.source_kind,
            tk = case.target_kind,
        ))
        .await;
    i64::extract_column(&result, 0).unwrap()
}

async fn assert_reconciles(ctx: &TestContext, case: Case) {
    seed(ctx, &case).await;

    task(ctx).run().await.unwrap();

    assert_eq!(
        live_other_endpoints(ctx, &case).await,
        vec![case.current_fk],
        "{}: stale edge ({}) must be tombstoned and current ({}) kept",
        case.relationship_kind,
        case.stale_fk,
        case.current_fk,
    );
}

async fn reconciles_has_latest_diff(ctx: &TestContext) {
    assert_reconciles(
        ctx,
        Case {
            relationship_kind: "HAS_LATEST_DIFF",
            owner_table: "gl_merge_request",
            owner_id: 10,
            owner_fk_column: "latest_merge_request_diff_id",
            edge_table: "gl_diff_edge",
            source_kind: "MergeRequest",
            target_kind: "MergeRequestDiff",
            owner_is_source: true,
            current_fk: 200,
            stale_fk: 100,
        },
    )
    .await;
}

async fn reconciles_has_head_pipeline(ctx: &TestContext) {
    assert_reconciles(
        ctx,
        Case {
            relationship_kind: "HAS_HEAD_PIPELINE",
            owner_table: "gl_merge_request",
            owner_id: 10,
            owner_fk_column: "head_pipeline_id",
            edge_table: "gl_ci_edge",
            source_kind: "MergeRequest",
            target_kind: "Pipeline",
            owner_is_source: true,
            current_fk: 500,
            stale_fk: 400,
        },
    )
    .await;
}

async fn reconciles_last_edited_by(ctx: &TestContext) {
    assert_reconciles(
        ctx,
        Case {
            relationship_kind: "LAST_EDITED_BY",
            owner_table: "gl_merge_request",
            owner_id: 10,
            owner_fk_column: "last_edited_by_id",
            edge_table: "gl_edge",
            source_kind: "User",
            target_kind: "MergeRequest",
            owner_is_source: false,
            current_fk: 7,
            stale_fk: 6,
        },
    )
    .await;
}

async fn reconciles_updated_by(ctx: &TestContext) {
    assert_reconciles(
        ctx,
        Case {
            relationship_kind: "UPDATED_BY",
            owner_table: "gl_merge_request",
            owner_id: 10,
            owner_fk_column: "updated_by_id",
            edge_table: "gl_edge",
            source_kind: "User",
            target_kind: "MergeRequest",
            owner_is_source: false,
            current_fk: 7,
            stale_fk: 6,
        },
    )
    .await;
}

async fn reconciles_in_milestone_for_merge_request(ctx: &TestContext) {
    assert_reconciles(
        ctx,
        Case {
            relationship_kind: "IN_MILESTONE",
            owner_table: "gl_merge_request",
            owner_id: 10,
            owner_fk_column: "milestone_id",
            edge_table: "gl_edge",
            source_kind: "MergeRequest",
            target_kind: "Milestone",
            owner_is_source: true,
            current_fk: 900,
            stale_fk: 800,
        },
    )
    .await;
}

async fn reconciles_in_milestone_for_work_item(ctx: &TestContext) {
    assert_reconciles(
        ctx,
        Case {
            relationship_kind: "IN_MILESTONE",
            owner_table: "gl_work_item",
            owner_id: 55,
            owner_fk_column: "milestone_id",
            edge_table: "gl_edge",
            source_kind: "WorkItem",
            target_kind: "Milestone",
            owner_is_source: true,
            current_fk: 900,
            stale_fk: 800,
        },
    )
    .await;
}

async fn reconcile_is_idempotent(ctx: &TestContext) {
    let case = Case {
        relationship_kind: "HAS_LATEST_DIFF",
        owner_table: "gl_merge_request",
        owner_id: 10,
        owner_fk_column: "latest_merge_request_diff_id",
        edge_table: "gl_diff_edge",
        source_kind: "MergeRequest",
        target_kind: "MergeRequestDiff",
        owner_is_source: true,
        current_fk: 200,
        stale_fk: 100,
    };
    seed(ctx, &case).await;

    task(ctx).run().await.unwrap();
    task(ctx).run().await.unwrap();

    assert_eq!(
        live_other_endpoints(ctx, &case).await,
        vec![case.current_fk],
        "re-running the sweep must not change an already-reconciled owner",
    );
}

async fn leaves_unrelated_owner_edges_untouched(ctx: &TestContext) {
    let reconciled = Case {
        relationship_kind: "HAS_LATEST_DIFF",
        owner_table: "gl_merge_request",
        owner_id: 10,
        owner_fk_column: "latest_merge_request_diff_id",
        edge_table: "gl_diff_edge",
        source_kind: "MergeRequest",
        target_kind: "MergeRequestDiff",
        owner_is_source: true,
        current_fk: 200,
        stale_fk: 100,
    };
    let healthy = Case {
        owner_id: 11,
        current_fk: 300,
        stale_fk: 300,
        ..reconciled.clone()
    };
    seed(ctx, &reconciled).await;
    seed(ctx, &healthy).await;

    task(ctx).run().await.unwrap();

    assert_eq!(live_other_endpoints(ctx, &reconciled).await, vec![200]);
    assert_eq!(
        live_other_endpoints(ctx, &healthy).await,
        vec![300],
        "an MR whose edge already matches its FK must be untouched",
    );
}

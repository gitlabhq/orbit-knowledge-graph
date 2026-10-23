use std::sync::Arc;

use clickhouse_client::FromArrowColumn;
use integration_testkit::{load_ontology, t};
use orbit_server::active_schema::{ActiveSchema, SchemaSnapshot};
use orbit_server::item_counts::{ItemCountService, ScopeItemCounts};
use orbit_server_config::AppConfig;
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::{AuthorizedPath, SecurityContext};

use crate::common::TestContext;

const NO_CURSOR: &str = "null";
const FIRST_PASS_CURSOR: &str = r#"{"c":["1/100/","42"]}"#;
const INCREMENTAL_CURSOR: &str = r#"{"c":["1/100/","42"],"f":"2026-09-22T00:00:00Z"}"#;

#[derive(Clone, Copy)]
pub struct CheckpointRow {
    watermark: &'static str,
    cursor: &'static str,
    indexed_at: &'static str,
    attempts: i64,
    written_at: &'static str,
}

pub const COMPLETED: CheckpointRow = CheckpointRow {
    watermark: "now()",
    cursor: NO_CURSOR,
    indexed_at: "now()",
    attempts: 0,
    written_at: "now64(6)",
};
pub const SOURCE_UNAVAILABLE: CheckpointRow = CheckpointRow {
    watermark: "'1970-01-01 00:00:00'",
    ..COMPLETED
};
pub const COMPLETED_ON_NEW_YEAR: CheckpointRow = CheckpointRow {
    indexed_at: "'2026-01-01 00:00:00'",
    ..COMPLETED
};
pub const INCREMENTAL: CheckpointRow = CheckpointRow {
    cursor: INCREMENTAL_CURSOR,
    ..COMPLETED
};
pub const PAGING_FIRST_PASS: CheckpointRow = CheckpointRow {
    watermark: "now()",
    cursor: FIRST_PASS_CURSOR,
    indexed_at: "NULL",
    attempts: 1,
    written_at: "now64(6)",
};
pub const STARTED_FIRST_PASS: CheckpointRow = CheckpointRow {
    cursor: NO_CURSOR,
    ..PAGING_FIRST_PASS
};
pub const FAILED_FIRST_PASS: CheckpointRow = CheckpointRow {
    attempts: 5,
    ..PAGING_FIRST_PASS
};
pub const STALE_FIRST_PASS: CheckpointRow = CheckpointRow {
    written_at: "now64(6) - INTERVAL 3 HOUR",
    ..PAGING_FIRST_PASS
};

pub fn use_production_query_settings() {
    orbit_server_config::query::init(AppConfig::embedded_defaults().query);
}

pub async fn query_cache_usage(ctx: &TestContext) -> Vec<String> {
    read_cached_query_log(ctx, "toString(query_cache_usage)").await
}

pub async fn query_cache_ttls(ctx: &TestContext) -> Vec<String> {
    read_cached_query_log(ctx, "Settings['query_cache_ttl']").await
}

pub async fn read_cached_query_log(ctx: &TestContext, column: &str) -> Vec<String> {
    let read = |select: &str| {
        format!(
            "SELECT {select} FROM system.query_log \
             WHERE current_database = '{}' AND Settings['use_query_cache'] = '1'",
            ctx.config.database
        )
    };
    let all_finished =
        read("toString(countIf(type = 'QueryStart') = countIf(type != 'QueryStart'))");
    for _ in 0..100 {
        ctx.execute("SYSTEM FLUSH LOGS").await;
        let batches = ctx.query(&all_finished).await;
        if String::extract_column(&batches, 0).expect("query log state") == ["1"] {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let batches = ctx
        .query(&format!(
            "{} AND type = 'QueryFinish' ORDER BY event_time_microseconds",
            read(column)
        ))
        .await;
    String::extract_column(&batches, 0).expect("query log column")
}

pub async fn count_items(
    ctx: &TestContext,
    security_context: &SecurityContext,
    scopes: &[&str],
) -> Vec<ScopeItemCounts> {
    let scopes: Vec<TraversalPath> = scopes
        .iter()
        .map(|scope| TraversalPath::new_unchecked(*scope))
        .collect();
    ItemCountService::new(Arc::new(ctx.create_client()))
        .count_items(&load_ontology(), security_context, &scopes)
        .await
}

pub fn admin_context() -> SecurityContext {
    SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/", 50)])
        .unwrap()
        .with_role(true, Some(50))
}

pub fn pinned_schema() -> Arc<SchemaSnapshot> {
    ActiveSchema::pinned(load_ontology())
        .snapshot()
        .expect("pinned schema is installed")
}

pub async fn seed_namespaces(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (100, 'Public Group', 'public', '1/100/'),
         (101, 'Private Group', 'private', '1/101/')",
        t("gl_group")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (1000, 'Public Project', 'public', '1/100/1000/'),
         (1001, 'Private Project', 'private', '1/101/1001/'),
         (1002, 'Internal Project', 'internal', '1/100/1002/'),
         (1070, 'Code Indexed Project', 'public', '1/107/1070/'),
         (1120, 'Ready Project', 'public', '1/112/1120/')",
        t("gl_project")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, project_id, branch, last_task_id, indexed_at, is_default_branch) VALUES
         ('1/100/1000/', 1000, 'main', 1, now(), true),
         ('1/101/1001/', 1001, 'main', 2, now(), true),
         ('1/100/1999/', 1999, 'main', 3, now(), true),
         ('1/107/1070/', 1070, 'main', 4, now(), true),
         ('1/112/1120/', 1120, 'main', 5, now(), true)",
        t("code_indexing_checkpoint")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, traversal_path) VALUES
         (2000, 1, 'Add feature A', 'opened', 'feature-a', 'main', '1/100/1000/'),
         (2001, 2, 'Fix bug B', 'opened', 'fix-b', 'main', '1/101/1001/')",
        t("gl_merge_request")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, title, state, severity, report_type, traversal_path) VALUES
         (5000, 'SQL Injection', 'detected', 'critical', 'sast', '1/101/1001/')",
        t("gl_vulnerability")
    ))
    .await;

    seed_plans(ctx, 100, &[("Note", INCREMENTAL)]).await;
    seed_plans(ctx, 101, &[("MergeRequest", PAGING_FIRST_PASS)]).await;
    seed_plans(ctx, 104, &[("Job.p1of5", PAGING_FIRST_PASS)]).await;
    seed_plans(ctx, 105, &[("MEMBER_OF_siphon_members", PAGING_FIRST_PASS)]).await;
    seed_plans(ctx, 107, &[("Commit", PAGING_FIRST_PASS)]).await;
    seed_plans(ctx, 108, &[("SystemNote", PAGING_FIRST_PASS)]).await;
    seed_plans(ctx, 109, &[("MergeRequest", STARTED_FIRST_PASS)]).await;
    seed_plans(ctx, 112, &[]).await;
    seed_plans(ctx, 113, &[("MergeRequest", COMPLETED_ON_NEW_YEAR)]).await;
    seed_plans(
        ctx,
        141,
        &[
            ("AgentSession", SOURCE_UNAVAILABLE),
            (
                "CREATED_MR_siphon_duo_workflows_workflow_merge_requests",
                SOURCE_UNAVAILABLE,
            ),
        ],
    )
    .await;
    seed_plans(ctx, 143, &[("AgentSession", SOURCE_UNAVAILABLE)]).await;
    insert_checkpoints(ctx, &[("ns.106.Project".to_string(), COMPLETED)]).await;

    ctx.optimize_all().await;
}

pub async fn seed_plans(ctx: &TestContext, root: i64, overrides: &[(&str, CheckpointRow)]) {
    let overridden = |plan: &str| overrides.iter().any(|(key, _)| *key == plan);
    let completed_plans = load_ontology()
        .pipeline_descriptors()
        .into_iter()
        .filter(|plan| plan.scope == ontology::EtlScope::Namespaced && !overridden(&plan.name))
        .map(|plan| (plan.name, COMPLETED));
    let overrides = overrides.iter().map(|(plan, row)| (plan.to_string(), *row));

    let rows: Vec<(String, CheckpointRow)> = completed_plans
        .chain(overrides)
        .map(|(plan, row)| (format!("ns.{root}.{plan}"), row))
        .collect();
    insert_checkpoints(ctx, &rows).await;
}

pub async fn insert_checkpoints(ctx: &TestContext, rows: &[(String, CheckpointRow)]) {
    let values: Vec<String> = rows
        .iter()
        .map(|(key, row)| {
            format!(
                "('{key}', {}, '{}', {}, {}, {})",
                row.watermark, row.cursor, row.indexed_at, row.attempts, row.written_at
            )
        })
        .collect();
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values, indexed_at, attempts, _version) VALUES {}",
        t("checkpoint"),
        values.join(", ")
    ))
    .await;
}

pub async fn seed_project_gap(ctx: &TestContext, root: i64, project: i64) {
    let path = format!("2/{root}/{project}/");
    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         ({project}, 'Failing Project', 'public', '{path}')",
        t("gl_project")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, project_id, branch, last_task_id, indexed_at, attempts, is_default_branch) VALUES
         ('{path}', {project}, 'main', 1, NULL, 5, true)",
        t("code_indexing_checkpoint")
    ))
    .await;
}

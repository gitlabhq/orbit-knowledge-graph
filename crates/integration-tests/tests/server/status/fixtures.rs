use std::sync::Arc;

use integration_testkit::{load_ontology, t};
use orbit_server::active_schema::{ActiveSchema, SchemaSnapshot};
use query_engine::compiler::{AuthorizedPath, SecurityContext};

use crate::common::TestContext;

const NO_CURSOR: &str = "null";
const FIRST_PASS_CURSOR: &str = r#"{"c":["1/100/","42"]}"#;
const INCREMENTAL_CURSOR: &str = r#"{"c":["1/100/","42"],"f":"2026-09-22T00:00:00Z"}"#;

#[derive(Clone, Copy)]
pub struct CheckpointRow {
    cursor: &'static str,
    indexed_at: &'static str,
}

pub const COMPLETED: CheckpointRow = CheckpointRow {
    cursor: NO_CURSOR,
    indexed_at: "now()",
};
pub const INCREMENTAL: CheckpointRow = CheckpointRow {
    cursor: INCREMENTAL_CURSOR,
    indexed_at: "now()",
};
pub const PAGING_FIRST_PASS: CheckpointRow = CheckpointRow {
    cursor: FIRST_PASS_CURSOR,
    indexed_at: "NULL",
};
pub const STARTED_FIRST_PASS: CheckpointRow = CheckpointRow {
    cursor: NO_CURSOR,
    indexed_at: "NULL",
};

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
        "INSERT INTO {} (traversal_path, project_id, branch, last_task_id, indexed_at) VALUES
         ('1/100/1000/', 1000, 'main', 1, now()),
         ('1/101/1001/', 1001, 'main', 2, now()),
         ('1/100/1999/', 1999, 'main', 3, now()),
         ('1/107/1070/', 1070, 'main', 4, now()),
         ('1/112/1120/', 1120, 'main', 5, now())",
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
        .map(|(key, row)| format!("('{key}', now(), '{}', {})", row.cursor, row.indexed_at))
        .collect();
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values, indexed_at) VALUES {}",
        t("checkpoint"),
        values.join(", ")
    ))
    .await;
}

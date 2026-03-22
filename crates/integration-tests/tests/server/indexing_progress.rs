use std::sync::Arc;

use crate::common::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext};
use gkg_server::indexing_progress::IndexingProgressService;
use integration_testkit::run_subtests;
use ontology::Ontology;

async fn seed_base_data(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO namespace_traversal_paths (id, traversal_path) VALUES
         (100, '1/100/'),
         (101, '1/101/')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_project (id, name, visibility_level, traversal_path) VALUES
         (1000, 'Project A', 'public', '1/100/1000/'),
         (1001, 'Project B', 'public', '1/100/1001/'),
         (1002, 'Project C', 'public', '1/101/1002/')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_group (id, name, visibility_level, traversal_path) VALUES
         (100, 'Group A', 'public', '1/100/'),
         (101, 'Group B', 'public', '1/101/')",
    )
    .await;

    ctx.optimize_all().await;
}

fn build_service(ctx: &TestContext) -> IndexingProgressService {
    let client = Arc::new(ctx.create_client());
    let datalake_client = Arc::new(ctx.create_client());
    let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
    IndexingProgressService::new(client, datalake_client, ontology)
}

#[tokio::test]
async fn indexing_progress() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;

    run_subtests!(
        &ctx,
        namespace_with_no_checkpoints_returns_queued,
        namespace_with_partial_checkpoints_returns_indexing,
        namespace_with_partial_checkpoints_and_prior_completion_returns_re_indexing,
        namespace_with_all_completed_returns_completed,
        unknown_namespace_returns_not_found,
    );
}

async fn namespace_with_no_checkpoints_returns_queued(ctx: &TestContext) {
    seed_base_data(ctx).await;

    let service = build_service(ctx);

    let traversal_path = service.resolve_traversal_path(100).await.unwrap();
    assert_eq!(traversal_path, "1/100/");

    let response = service.get_progress(100, &traversal_path).await.unwrap();
    let response = response.into_inner();

    assert_eq!(response.namespace_id, 100);
    assert_eq!(response.status, "queued");
    assert!(!response.domains.is_empty());
}

async fn namespace_with_partial_checkpoints_returns_indexing(ctx: &TestContext) {
    seed_base_data(ctx).await;

    ctx.execute(
        "INSERT INTO checkpoint (key, watermark, cursor_values) VALUES
         ('ns.100.Project', '1970-01-01 00:00:00.000000', ''),
         ('ns.100.Group', '1970-01-01 00:00:00.000000', '[\"1/100/\",\"42\"]')",
    )
    .await;
    ctx.optimize_all().await;

    let service = build_service(ctx);
    let traversal_path = service.resolve_traversal_path(100).await.unwrap();
    let response = service.get_progress(100, &traversal_path).await.unwrap();
    let response = response.into_inner();

    assert_eq!(response.status, "indexing");

    let core = response
        .domains
        .iter()
        .find(|d| d.name == "core")
        .expect("should have core domain");

    let project = core.items.iter().find(|i| i.name == "Project").unwrap();
    assert_eq!(project.status, "completed");
    assert_eq!(project.count, 2, "two projects under 1/100/");

    let group = core.items.iter().find(|i| i.name == "Group").unwrap();
    assert_eq!(group.status, "in_progress");
}

async fn namespace_with_partial_checkpoints_and_prior_completion_returns_re_indexing(
    ctx: &TestContext,
) {
    seed_base_data(ctx).await;

    ctx.execute(
        "INSERT INTO checkpoint (key, watermark, cursor_values) VALUES
         ('ns.100.Project', '2024-06-15 12:00:00.000000', ''),
         ('ns.100.Group', '2024-06-15 12:00:00.000000', '[\"1/100/\",\"42\"]')",
    )
    .await;
    ctx.optimize_all().await;

    let service = build_service(ctx);
    let traversal_path = service.resolve_traversal_path(100).await.unwrap();
    let response = service.get_progress(100, &traversal_path).await.unwrap();
    let response = response.into_inner();

    assert_eq!(response.status, "re_indexing");
}

async fn namespace_with_all_completed_returns_completed(ctx: &TestContext) {
    seed_base_data(ctx).await;

    let ontology = Ontology::load_embedded().unwrap();
    let plan_names: Vec<String> = ontology
        .nodes()
        .filter(|n| n.etl.is_some() && n.domain != "source_code")
        .map(|n| n.name.clone())
        .collect();

    let values: Vec<String> = plan_names
        .iter()
        .map(|name| format!("('ns.101.{name}', '2024-06-15 12:00:00.000000', '')"))
        .collect();

    ctx.execute(&format!(
        "INSERT INTO checkpoint (key, watermark, cursor_values) VALUES {}",
        values.join(", ")
    ))
    .await;

    ctx.execute(
        "INSERT INTO code_indexing_checkpoint \
         (traversal_path, project_id, branch, last_task_id, indexed_at, _version) VALUES \
         ('1/101/1002/', 1002, 'main', 1, '2024-06-15 12:00:00.000000', 1)",
    )
    .await;

    ctx.optimize_all().await;

    let service = build_service(ctx);
    let traversal_path = service.resolve_traversal_path(101).await.unwrap();
    let response = service.get_progress(101, &traversal_path).await.unwrap();
    let response = response.into_inner();

    assert_eq!(response.status, "completed");

    for domain in &response.domains {
        if domain.name == "source_code" {
            for item in &domain.items {
                assert_eq!(
                    item.status, "completed",
                    "source_code item {} should be completed",
                    item.name
                );
            }
        }
    }
}

async fn unknown_namespace_returns_not_found(ctx: &TestContext) {
    let service = build_service(ctx);

    let traversal_path = service.resolve_traversal_path(999).await.unwrap();
    assert!(
        traversal_path.is_empty(),
        "unknown namespace should resolve to an empty traversal path"
    );
}

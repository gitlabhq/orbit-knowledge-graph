//! Behavior tests for the CTE-shape namespace and project ETL.
//!
//! The previous shape (LEFT JOIN over `siphon_routes` + INNER JOIN over
//! `siphon_namespace_details`) full-scanned the source tables when the target
//! subtree was small but the surrounding datalake was large. The current shape
//! in `config/ontology/nodes/core/{group,project}.yaml` pre-aggregates each
//! source in its own CTE and drives the join from the small `subtree` CTE.
//!
//! These tests assert the *behavior* that follows from the new shape — rows
//! outside the requested subtree do not surface, soft-deleted routes are
//! filtered out, and the namespace handler still produces the same `gl_group`
//! and `gl_project` rows. The SQL-shape regression is asserted by a unit test
//! on the plan builder (see `crates/indexer/src/modules/sdlc/plan/`).

use crate::indexer::common::{
    TestContext, assert_node_count, create_namespace_with_path, create_project_with_path,
    create_route, handler_context, namespace_envelope, namespace_handler,
};

pub async fn namespace_handler_ignores_sibling_subtree(ctx: &TestContext) {
    create_namespace_with_path(ctx, 100, None, 0, "1/100/", Some("target")).await;
    create_namespace_with_path(ctx, 101, Some(100), 0, "1/100/101/", Some("child")).await;
    create_route(ctx, 100, 100, "Namespace", "target", 100, "1/100/").await;
    create_route(
        ctx,
        101,
        101,
        "Namespace",
        "target/child",
        101,
        "1/100/101/",
    )
    .await;

    create_namespace_with_path(ctx, 200, None, 0, "2/200/", Some("sibling")).await;
    create_route(ctx, 200, 200, "Namespace", "sibling", 200, "2/200/").await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_group", 2).await;
}

pub async fn project_handler_ignores_sibling_subtree(ctx: &TestContext) {
    create_namespace_with_path(ctx, 100, None, 0, "1/100/", Some("target")).await;
    create_namespace_with_path(ctx, 200, None, 0, "2/200/", Some("sibling")).await;
    create_project_with_path(ctx, 1000, 100, 1, 0, "1/100/1000/", Some("p")).await;
    create_route(ctx, 1000, 1000, "Project", "target/p", 100, "1/100/1000/").await;
    create_project_with_path(ctx, 2000, 200, 1, 0, "2/200/2000/", Some("sib")).await;
    create_route(
        ctx,
        2000,
        2000,
        "Project",
        "sibling/sib",
        200,
        "2/200/2000/",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_project", 1).await;
}

pub async fn namespace_handler_excludes_soft_deleted_routes(ctx: &TestContext) {
    create_namespace_with_path(ctx, 100, None, 0, "1/100/", Some("ns")).await;

    let client = ctx.create_client();
    client
        .execute(
            "INSERT INTO siphon_routes \
             (id, source_id, source_type, path, namespace_id, traversal_path, \
              _siphon_replicated_at, _siphon_deleted) \
             VALUES (100, 100, 'Namespace', 'old-name', 100, '1/100/', \
                     '2024-01-01 00:00:00', true)",
        )
        .await
        .expect("seed soft-deleted route");

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    let result = ctx
        .query(&format!(
            "SELECT full_path FROM {} FINAL WHERE id = 100",
            integration_testkit::t("gl_group")
        ))
        .await;
    let paths = gkg_utils::arrow::ArrowUtils::get_column_by_name::<arrow::array::StringArray>(
        &result[0],
        "full_path",
    )
    .expect("full_path column");

    assert_eq!(
        paths.value(0),
        "ns",
        "soft-deleted route 'old-name' must not surface; full_path should fall back to namespace.path"
    );
}

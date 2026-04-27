//! Integration coverage for multi-emit FK edges (one source column emitting
//! more than one edge mapping).
//!
//! The embedded ontology does not yet declare any multi-emit columns, so the
//! test mutates a loaded ontology in-process to add a second mapping to
//! `Job.stage_id`. This exercises the full handler pipeline (extract ->
//! transform -> load) against a real ClickHouse container and proves both
//! emissions land in `gl_edge` from a single source-row scan.

use ontology::etl::{EdgeDirection, EdgeMapping, EdgeTarget, EtlConfig};

use crate::indexer::common::{
    TestContext, assert_edge_count_for_traversal_path, create_namespace, create_project,
    create_user, handler_context, namespace_envelope, namespace_handler_with_ontology,
};

/// Synthetic relationship kind that does not exist in the embedded ontology.
/// Unknown relationships fall back to `gl_edge`, so the second emission is
/// observable by the same assertion path as production edges.
const SECONDARY_KIND: &str = "MULTI_EMIT_TEST_INVERSE";

fn add_inverse_stage_mapping(ontology: &mut ontology::Ontology) {
    let job = ontology
        .get_node_mut("Job")
        .expect("Job node should exist in embedded ontology");
    let etl = job
        .etl
        .as_mut()
        .expect("Job node should declare an etl block");

    let edges = match etl {
        EtlConfig::Table { edges, .. } | EtlConfig::Query { edges, .. } => edges,
    };

    edges
        .get_mut("stage_id")
        .expect("Job.stage_id should already declare an edge")
        .push(EdgeMapping {
            target: EdgeTarget::Literal("Stage".to_string()),
            relationship_kind: SECONDARY_KIND.to_string(),
            direction: EdgeDirection::Outgoing,
            delimiter: None,
            array_field: None,
            array: false,
        });
}

pub async fn multi_emit_fk_writes_both_edges(ctx: &TestContext) {
    let mut ontology = ontology::Ontology::load_embedded().expect("ontology must load");
    add_inverse_stage_mapping(&mut ontology);

    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;
    create_user(ctx, 1).await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_stages (id, partition_id, pipeline_id, project_id, name, status, position, traversal_path, _siphon_replicated_at)
        VALUES (6001, 1, 5001, 1000, 'build', 3, 0, '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_builds (id, partition_id, stage_id, project_id, user_id, name, status, ref, tag, allow_failure, environment, `when`, retried, created_at, started_at, finished_at, queued_at, traversal_path, _siphon_replicated_at)
        VALUES
        (7001, 1, 6001, 1000, 1, 'compile', 'success', 'main', false, false, NULL, 'on_success', false, '2024-01-15 10:00:00', '2024-01-15 10:00:30', '2024-01-15 10:01:00', '2024-01-15 10:00:00', '1/100/1000/', '2024-01-20 12:00:00'),
        (7002, 1, 6001, 1000, 1, 'lint', 'success', 'main', false, true, NULL, 'on_success', false, '2024-01-15 10:00:00', '2024-01-15 10:00:30', '2024-01-15 10:01:00', '2024-01-15 10:00:00', '1/100/1000/', '2024-01-20 12:00:00'),
        (7003, 1, 6001, 1000, 1, 'test', 'success', 'main', false, false, NULL, 'on_success', false, '2024-01-15 10:00:00', '2024-01-15 10:00:30', '2024-01-15 10:01:00', '2024-01-15 10:00:00', '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler_with_ontology(ctx, &ontology)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    // Original mapping: Stage -[HAS_JOB]-> Job, one edge per source row.
    assert_edge_count_for_traversal_path(ctx, "HAS_JOB", "Stage", "Job", "1/100/1000/", 3).await;

    // Second mapping piggybacks on the same extract: Job -[SECONDARY_KIND]-> Stage.
    assert_edge_count_for_traversal_path(ctx, SECONDARY_KIND, "Job", "Stage", "1/100/1000/", 3)
        .await;

    // Other FK edges declared on Job (single-emission) must still produce
    // exactly one edge per source row — the multi-emit code path must not
    // duplicate or drop unrelated mappings.
    assert_edge_count_for_traversal_path(ctx, "IN_PROJECT", "Job", "Project", "1/100/1000/", 3)
        .await;
    assert_edge_count_for_traversal_path(ctx, "TRIGGERED", "User", "Job", "1/100/1000/", 3).await;
}

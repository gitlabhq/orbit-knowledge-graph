use indexer::modules::sdlc::dispatch::partitioning::{DatalakePartitioner, Partitioner};
use indexer::topic::{IndexingScope, PartitionBounds};

use crate::indexer::common::TestContext;

async fn seed_merge_requests(ctx: &TestContext, traversal_path: &str, count: i64) {
    for i in 1..=count {
        ctx.execute(&format!(
            "INSERT INTO merge_requests \
             (id, iid, title, target_branch, source_branch, target_project_id, \
              traversal_path, _siphon_replicated_at, _siphon_deleted) \
             VALUES ({i}, {i}, 'MR {i}', 'main', 'feature-{i}', 1000, \
                     '{traversal_path}', '2024-01-20 12:00:00', false)"
        ))
        .await;
    }
}

async fn seed_users(ctx: &TestContext, count: i64) {
    for i in 1..=count {
        ctx.execute(&format!(
            "INSERT INTO siphon_users \
             (id, email, username, name, state, organization_id, _siphon_replicated_at) \
             VALUES ({i}, 'user{i}@test.com', 'user{i}', 'User {i}', 'active', 1, \
                     '2024-01-20 12:00:00')"
        ))
        .await;
    }
}

fn assert_contiguous_boundaries(boundaries: &[PartitionBounds], expected_count: usize) {
    assert_eq!(
        boundaries.len(),
        expected_count,
        "expected {expected_count} partitions, got {}",
        boundaries.len()
    );

    for i in 0..boundaries.len() - 1 {
        let PartitionBounds::Range { upper_bound, .. } = &boundaries[i];
        let PartitionBounds::Range { lower_bound, .. } = &boundaries[i + 1];
        assert_eq!(
            upper_bound,
            lower_bound,
            "gap between partition {i} and {}: upper={upper_bound}, lower={lower_bound}",
            i + 1
        );
    }
}

pub async fn computes_boundaries_for_namespaced_scope(ctx: &TestContext) {
    seed_merge_requests(ctx, "42/100/", 100).await;

    let partitioner = DatalakePartitioner::new(ctx.create_client());
    let scope = IndexingScope::Namespace {
        namespace_id: 100,
        traversal_path: "42/100/".to_string(),
    };

    let boundaries = partitioner
        .compute_boundaries("merge_requests", "id", 4, &scope)
        .await
        .expect("compute_boundaries should succeed");

    assert_contiguous_boundaries(&boundaries, 4);

    let PartitionBounds::Range { lower_bound, .. } = &boundaries[0];
    assert_eq!(lower_bound, "1", "first lower bound should be min id");

    let PartitionBounds::Range { upper_bound, .. } = &boundaries[3];
    assert_eq!(upper_bound, "100", "last upper bound should be max id");
}

pub async fn computes_boundaries_for_global_scope(ctx: &TestContext) {
    seed_users(ctx, 100).await;

    let partitioner = DatalakePartitioner::new(ctx.create_client());

    let boundaries = partitioner
        .compute_boundaries("siphon_users", "id", 4, &IndexingScope::Global)
        .await
        .expect("compute_boundaries should succeed");

    assert_contiguous_boundaries(&boundaries, 4);

    let PartitionBounds::Range { lower_bound, .. } = &boundaries[0];
    assert_eq!(lower_bound, "1", "first lower bound should be min id");

    let PartitionBounds::Range { upper_bound, .. } = &boundaries[3];
    assert_eq!(upper_bound, "100", "last upper bound should be max id");
}

pub async fn namespace_filter_excludes_other_namespaces(ctx: &TestContext) {
    seed_merge_requests(ctx, "42/100/", 50).await;
    seed_merge_requests(ctx, "42/200/", 50).await;

    let partitioner = DatalakePartitioner::new(ctx.create_client());
    let scope = IndexingScope::Namespace {
        namespace_id: 100,
        traversal_path: "42/100/".to_string(),
    };

    let boundaries = partitioner
        .compute_boundaries("merge_requests", "id", 2, &scope)
        .await
        .expect("compute_boundaries should succeed");

    assert_contiguous_boundaries(&boundaries, 2);

    let PartitionBounds::Range { lower_bound, .. } = &boundaries[0];
    let PartitionBounds::Range { upper_bound, .. } = &boundaries[1];
    assert_eq!(lower_bound, "1");
    assert_eq!(upper_bound, "50");
}

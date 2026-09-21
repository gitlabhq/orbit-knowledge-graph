use std::sync::Arc;

use indexer::modules::sdlc::jobs::NAMESPACE_DATA;
use jobs::{JobLedger, JobState};
use orbit_utils::traversal_path::TraversalPath;

use crate::indexer::common::{
    TestContext, create_namespace, handler_context, namespace_envelope, namespace_handler,
};

pub async fn namespace_run_records_a_succeeded_job_per_pipeline(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(), namespace_envelope(1, 100))
        .await
        .expect("namespace handlers should succeed");
    ctx.flush_async_inserts().await;

    let runs = JobLedger::new(Arc::new(ctx.create_client()))
        .latest_runs(&TraversalPath::from("1/100/"), &NAMESPACE_DATA)
        .await
        .expect("ledger should be readable");

    assert!(runs.iter().any(|run| run.key == "MergeRequest"), "{runs:?}");
    for run in &runs {
        assert_eq!(run.state, JobState::Succeeded, "{run:?}");
        assert_eq!(run.namespace_id, 100);
        assert_eq!(run.traversal_path.as_str(), "1/100/");
        assert!(
            run.completed_at
                .is_some_and(|completed| completed >= run.started_at),
            "{run:?}"
        );
    }
}

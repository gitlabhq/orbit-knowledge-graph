use arrow::array::Int64Array;
use integration_testkit::t;
use orbit_arrow::ArrowUtils;

use crate::indexer::common::{
    TestContext, entity_handler_with_batch_size, handler_context, namespace_envelope,
};

const PAGE_SIZE: u64 = 2;

// Within `1/100/1000/`, ids 1/2 and 3/4 share a timestamp, 5/6 differ by one microsecond, and
// id 0 is the latest session. Id order and (started_at, id) order disagree, so a query ordered
// differently from the cursor's sort key skips id 0.
const SESSIONS: [(i64, &str, &str); 9] = [
    (0, "1/100/1000/", "2024-01-15 10:00:01.000000"),
    (1, "1/100/1000/", "2024-01-15 10:00:00.000000"),
    (2, "1/100/1000/", "2024-01-15 10:00:00.000000"),
    (3, "1/100/1000/", "2024-01-15 10:00:00.500000"),
    (4, "1/100/1000/", "2024-01-15 10:00:00.500000"),
    (5, "1/100/1000/", "2024-01-15 10:00:00.500001"),
    (6, "1/100/1000/", "2024-01-15 10:00:00.500002"),
    (8, "1/100/1001/", "2024-01-14 09:00:00.250000"),
    (7, "1/100/1001/", "2024-01-14 09:00:00.250000"),
];

async fn seed_sessions(ctx: &TestContext) {
    for (id, traversal_path, started_at) in SESSIONS {
        ctx.execute(&format!(
            "INSERT INTO siphon_ai_governance_sessions \
             (id, namespace_id, project_id, workflow_id, source, flow_type, session_started_at, \
              created_at, updated_at, traversal_path, _siphon_replicated_at, _siphon_watermark) \
             VALUES ({id}, 100, 1000, {id}, 0, 'chat', '{started_at}', '{started_at}', \
                     '{started_at}', '{traversal_path}', '2024-01-15 11:00:00', '2024-01-15 11:00:00')"
        ))
        .await;
    }
}

async fn indexed_session_ids(ctx: &TestContext) -> Vec<i64> {
    let result = ctx
        .query(&format!(
            "SELECT id FROM {} ORDER BY id",
            t("gl_agent_session")
        ))
        .await;
    let ids = ArrowUtils::get_column_by_name::<Int64Array>(&result[0], "id").expect("id column");
    (0..ids.len()).map(|i| ids.value(i)).collect()
}

async fn index_agent_sessions(ctx: &TestContext) {
    entity_handler_with_batch_size(ctx, "AgentSession", PAGE_SIZE)
        .await
        .handle(handler_context(), namespace_envelope(1, 100))
        .await
        .expect("AgentSession handler should succeed");
}

fn expected_ids() -> Vec<i64> {
    let mut ids: Vec<i64> = SESSIONS.iter().map(|(id, ..)| *id).collect();
    ids.sort();
    ids
}

pub async fn agent_sessions_page_by_sort_key_without_skips_or_duplicates(ctx: &TestContext) {
    seed_sessions(ctx).await;

    index_agent_sessions(ctx).await;

    let indexed = indexed_session_ids(ctx).await;
    assert_eq!(
        indexed,
        expected_ids(),
        "paging 9 rows two at a time must neither skip nor repeat a session"
    );
}

pub async fn agent_sessions_resume_from_a_saved_cursor(ctx: &TestContext) {
    seed_sessions(ctx).await;
    // The last saved page ended at (1/100/1000/, 10:00:00.5, 3).
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values, _version) \
         VALUES ('ns.100.AgentSession', '2024-01-20 12:00:00.000000', \
                 '{{\"c\":[\"1/100/1000/\",\"2024-01-15 10:00:00.500000\",\"3\"]}}', \
                 '2024-01-20 12:00:00.000000')",
        t("checkpoint")
    ))
    .await;

    index_agent_sessions(ctx).await;

    assert_eq!(
        indexed_session_ids(ctx).await,
        vec![0, 4, 5, 6, 7, 8],
        "resume must continue strictly after the checkpointed (path, started_at, id)"
    );
}

pub async fn agent_sessions_restart_when_the_saved_cursor_has_the_old_sort_key_arity(
    ctx: &TestContext,
) {
    seed_sessions(ctx).await;
    // Saved while AgentSession paged by [traversal_path, id].
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values, _version) \
         VALUES ('ns.100.AgentSession', '2024-01-20 12:00:00.000000', \
                 '{{\"c\":[\"1/100/1000/\",\"3\"]}}', '2024-01-20 12:00:00.000000')",
        t("checkpoint")
    ))
    .await;

    index_agent_sessions(ctx).await;

    assert_eq!(
        indexed_session_ids(ctx).await,
        expected_ids(),
        "a cursor that no longer fits the sort key must restart the window, not panic"
    );
}

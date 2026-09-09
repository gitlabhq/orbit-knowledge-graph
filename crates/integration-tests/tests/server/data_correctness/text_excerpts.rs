use super::helpers::*;
use integration_testkit::{query_scenario, run_subtests_shared, t};
use query_engine::formatters::{GOON_OUTPUT_FORMAT_VERSION, goon_encode};
use serde_json::json;

const RESPONSE_LIMIT_BYTES: usize = 8 * 1024 * 1024;
const OVERSIZED_NOTE_PATTERN: &str = "abcd";
const OVERSIZED_NOTE_REPETITIONS: usize =
    RESPONSE_LIMIT_BYTES / 3 / OVERSIZED_NOTE_PATTERN.len() + 1;

#[tokio::test]
async fn returned_text_excerpts() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    query_scenario::load_yaml_seed(&ctx, super::PRESETS, "text_excerpts").await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, note, noteable_type, noteable_id, traversal_path)
         SELECT 31000 + number,
                if(number < 3, repeat('{OVERSIZED_NOTE_PATTERN}', {OVERSIZED_NOTE_REPETITIONS}), 'ordinary note'),
                'MergeRequest', 40000, '1/100/1000/' FROM numbers(1000)",
        t("gl_note")
    ))
    .await;
    ctx.optimize_all().await;

    run_subtests_shared!(
        &ctx,
        goon_length_describes_the_excerpt,
        cursor_order_uses_complete_text,
        node_aggregation_returns_excerpts,
        scalar_grouping_and_aggregation_use_complete_text,
        oversized_note_page_keeps_every_row,
    );
}

fn permissions() -> MockRedactionService {
    let mut permissions = MockRedactionService::new();
    permissions.allow("note", &(30000..32000).collect::<Vec<_>>());
    permissions
}

async fn goon_length_describes_the_excerpt(ctx: &TestContext) {
    let response = run_query(
        ctx,
        &json!({
            "query_type": "traversal",
            "nodes": [{"id": "note", "entity": "Note", "node_ids": [30003], "columns": ["note"]}]
        })
        .to_string(),
        &permissions(),
    )
    .await;
    response.assert_node_count(1);
    response.assert_node_ids("Note", &[30003]);
    let goon = goon_encode(&response.response, &GOON_OUTPUT_FORMAT_VERSION);
    assert!(goon.contains("note_len=2060"));
    assert!(goon.contains("..."));
    assert!(!goon.contains(&"a".repeat(2048)));
}

async fn cursor_order_uses_complete_text(ctx: &TestContext) {
    let mut query = json!({
        "query_type": "traversal",
        "nodes": [{"id": "note", "entity": "Note", "node_ids": [30006, 30007], "columns": ["note"]}],
        "order_by": "note.note",
        "cursor": {"page_size": 1}
    });
    for expected_id in [30007, 30006] {
        let response = run_query(ctx, &query.to_string(), &permissions()).await;
        response.assert_node_count(1);
        response.assert_node_order("Note", &[expected_id]);
        let pagination = response.response.pagination.as_ref().unwrap();
        assert_eq!(pagination.has_more, expected_id == 30007);
        if expected_id == 30007 {
            query["cursor"]["after"] = json!(pagination.next_cursor.as_ref().unwrap());
        } else {
            assert!(pagination.next_cursor.is_none());
        }
    }
}

async fn node_aggregation_returns_excerpts(ctx: &TestContext) {
    let response = run_query(ctx, &json!({
        "query_type": "aggregation",
        "nodes": [{"id": "note", "entity": "Note", "node_ids": [30006, 30007], "columns": ["note"]}],
        "group_by": ["note"], "aggregations": [{"count": "note", "as": "count"}]
    }).to_string(), &permissions()).await;
    response.assert_group_node_ids("note", "Note", &[30006, 30007]);
    response.assert_row_count(2);
    for note_id in [30006, 30007] {
        response.assert_group_node_property_str(
            "note",
            "Note",
            note_id,
            "note",
            &format!("{} [truncated]", "x".repeat(2048)),
        );
        response.assert_group_row_value_i64("note", "Note", note_id, "count", 1);
    }
}

async fn scalar_grouping_and_aggregation_use_complete_text(ctx: &TestContext) {
    let response = run_query(ctx, &json!({
        "query_type": "aggregation",
        "nodes": [{"id": "note", "entity": "Note", "id_range": {"start": 30006, "end": 30007}}],
        "group_by": [{"key": "note.note", "as": "body"}],
        "aggregations": [{"count": "note", "as": "count"}, {"min": "note.note", "as": "minimum"}]
    }).to_string(), &permissions()).await;
    response.assert_row_count(2);
    for suffix in ["a", "z"] {
        let complete_text = format!("{}{suffix}", "x".repeat(2048));
        let row_index = response
            .rows()
            .iter()
            .position(|row| row["body"] == complete_text)
            .unwrap();
        response.assert_row_value_str(row_index, "minimum", &complete_text);
        response.assert_row_value_i64(row_index, "count", 1);
    }
}

async fn oversized_note_page_keeps_every_row(ctx: &TestContext) {
    let response = run_query(ctx, &json!({
        "query_type": "traversal",
        "nodes": [{"id": "note", "entity": "Note", "id_range": {"start": 31000, "end": 31999}, "columns": ["note"]}],
        "cursor": {"page_size": 1000}
    }).to_string(), &permissions()).await;
    response.assert_node_count(1000);
    response.assert_node_ids("Note", &(31000..32000).collect::<Vec<_>>());
    let pagination = response.response.pagination.as_ref().unwrap();
    assert!(!pagination.has_more);
    assert!(pagination.next_cursor.is_none());
    assert!(serde_json::to_vec(&response.response).unwrap().len() < RESPONSE_LIMIT_BYTES);
    for note_id in 31000..31003 {
        response.find_node("Note", note_id).unwrap().assert_str(
            "note",
            &format!(
                "{} [truncated]",
                OVERSIZED_NOTE_PATTERN.repeat(2048 / OVERSIZED_NOTE_PATTERN.len())
            ),
        );
    }
}

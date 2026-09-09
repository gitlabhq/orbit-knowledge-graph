use super::helpers::*;
use integration_testkit::{run_subtests_shared, t};
use query_engine::formatters::{GOON_OUTPUT_FORMAT_VERSION, goon_encode};
use serde_json::json;

const RESPONSE_LIMIT_BYTES: usize = 8 * 1024 * 1024;
const OVERSIZED_NOTE_PATTERN: &str = "abcd";
const OVERSIZED_NOTE_REPETITIONS: usize =
    RESPONSE_LIMIT_BYTES / 3 / OVERSIZED_NOTE_PATTERN.len() + 1;

#[tokio::test]
async fn returned_text_excerpts() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, title, state, traversal_path) VALUES
         (40000, repeat('m', 3000), 'opened', '1/100/1000/')",
        t("gl_merge_request")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, note, discussion_id, noteable_type, noteable_id,
                         author_id, created_at, internal, traversal_path)
         SELECT 30000 + number,
                arrayElement(['', repeat('a', 2047), repeat('a', 2048), repeat('a', 2049),
                              repeat('🙂', 2048), repeat('🙂', 2049),
                              concat(repeat('x', 2048), 'z'), concat(repeat('x', 2048), 'a'),
                              repeat(concat(char(34), char(10), '🙂'), 1024)], number + 1) AS body,
                if(number IN (0, 5), NULL, body), 'MergeRequest', 40000, 1,
                toDateTime('2026-01-01 00:00:00', 'UTC'), false, '1/100/1000/'
         FROM numbers(9)",
        t("gl_note")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, note, noteable_type, noteable_id, traversal_path)
         SELECT 31000 + number,
                if(number < 3, repeat('{OVERSIZED_NOTE_PATTERN}', {OVERSIZED_NOTE_REPETITIONS}), 'ordinary note'),
                'MergeRequest', 40000, '1/100/1000/' FROM numbers(1000)",
        t("gl_note")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind)
         SELECT '1/100/1000/', 40000, 'MergeRequest', 'HAS_NOTE', id, 'Note' FROM {}",
        t("gl_edge"),
        t("gl_note")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, project_id, branch, path, name, traversal_path)
         VALUES (50000, 1000, repeat('b', 3000), repeat('p', 3000), repeat('n', 3000), '1/100/1000/')",
        t("gl_file")
    ))
    .await;
    ctx.optimize_all().await;

    run_subtests_shared!(
        &ctx,
        excerpts_preserve_boundaries_unicode_nulls_and_types,
        explicit_default_and_wildcard_columns_return_excerpts,
        static_and_dynamic_hydration_return_excerpts,
        filtering_and_cursor_order_use_complete_text,
        redaction_still_removes_inaccessible_notes,
        virtual_lookup_inputs_remain_complete,
        node_aggregation_returns_excerpts,
        scalar_grouping_and_aggregation_use_complete_text,
        oversized_note_page_keeps_every_row,
    );
}

fn permissions() -> MockRedactionService {
    let mut permissions = MockRedactionService::new();
    permissions.allow("merge_request", &[40000]);
    permissions.allow("note", &(30000..32000).collect::<Vec<_>>());
    permissions.allow("project", &[1000]);
    permissions
}

async fn excerpts_preserve_boundaries_unicode_nulls_and_types(ctx: &TestContext) {
    let response = run_query(
        ctx,
        &json!({
            "query_type": "traversal",
            "nodes": [{"id": "note", "entity": "Note", "node_ids": (30000..30009).collect::<Vec<_>>(),
                "columns": ["note", "discussion_id", "noteable_type", "author_id", "created_at", "internal"]}]
        }).to_string(),
        &permissions(),
    ).await;
    response.assert_node_count(9);
    response.assert_node_ids("Note", &(30000..30009).collect::<Vec<_>>());
    for (note_id, expected) in [
        (30000, String::new()),
        (30001, "a".repeat(2047)),
        (30002, "a".repeat(2048)),
        (30003, format!("{} [truncated]", "a".repeat(2048))),
        (30004, "🙂".repeat(2048)),
        (30005, format!("{} [truncated]", "🙂".repeat(2048))),
        (30006, format!("{} [truncated]", "x".repeat(2048))),
        (30007, format!("{} [truncated]", "x".repeat(2048))),
        (30008, format!("{}\"\n [truncated]", "\"\n🙂".repeat(682))),
    ] {
        let note = response.find_node("Note", note_id).unwrap();
        note.assert_str("note", &expected);
        note.assert_str("noteable_type", "MergeRequest");
        note.assert_i64("author_id", 1);
        note.assert_prop("internal", &json!(false));
        assert!(
            note.prop_str("created_at")
                .unwrap()
                .starts_with("2026-01-01")
        );
        note.assert_prop(
            "discussion_id",
            &if matches!(note_id, 30000 | 30005) {
                Value::Null
            } else {
                json!(expected)
            },
        );
    }
    let goon = goon_encode(&response.response, &GOON_OUTPUT_FORMAT_VERSION);
    assert!(goon.contains("note_len=2060"));
    assert!(goon.contains("..."));
    assert!(!goon.contains(&"a".repeat(2048)));
}

async fn explicit_default_and_wildcard_columns_return_excerpts(ctx: &TestContext) {
    for columns in [None, Some(json!(["title", "state"])), Some(json!("*"))] {
        let mut query = json!({
            "query_type": "traversal",
            "nodes": [{"id": "mr", "entity": "MergeRequest", "node_ids": [40000]}]
        });
        if let Some(columns) = columns {
            query["nodes"][0]["columns"] = columns;
        }
        let response = run_query(ctx, &query.to_string(), &permissions()).await;
        response.assert_node_count(1);
        response.assert_node_ids("MergeRequest", &[40000]);
        let merge_request = response.find_node("MergeRequest", 40000).unwrap();
        merge_request.assert_str("title", &format!("{} [truncated]", "m".repeat(2048)));
        merge_request.assert_str("state", "opened");
    }
}

async fn static_and_dynamic_hydration_return_excerpts(ctx: &TestContext) {
    for query in [
        json!({
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "node_ids": [40000]},
                {"id": "note", "entity": "Note", "node_ids": [30005], "columns": ["note", "internal", "discussion_id"]}
            ],
            "relationships": [{"type": "HAS_NOTE", "from": "mr", "to": "note"}]
        }),
        json!({
            "query_type": "neighbors",
            "options": {"dynamic_columns": "*"},
            "nodes": [{"id": "note", "entity": "Note", "node_ids": [30005], "columns": "*"}],
            "neighbors": {"direction": "incoming", "rel_types": ["HAS_NOTE"]}
        }),
        json!({
            "query_type": "path_finding",
            "options": {"dynamic_columns": "*"},
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "node_ids": [40000], "columns": "*"},
                {"id": "note", "entity": "Note", "node_ids": [30005], "columns": "*"}
            ],
            "path": {"type": "shortest", "from": "mr", "to": "note", "max_depth": 1, "rel_types": ["HAS_NOTE"]}
        }),
    ] {
        let response = run_query(ctx, &query.to_string(), &permissions()).await;
        response.assert_node_count(2);
        response.assert_node_ids("Note", &[30005]);
        response.assert_edge_set("HAS_NOTE", &[(40000, 30005)]);
        if query["query_type"] == "path_finding" {
            assert_eq!(response.path_ids().len(), 1);
        }
        let note = response.find_node("Note", 30005).unwrap();
        note.assert_str("note", &format!("{} [truncated]", "🙂".repeat(2048)));
        note.assert_prop("internal", &json!(false));
        assert!(matches!(
            note.prop("discussion_id"),
            None | Some(Value::Null)
        ));
    }
}

async fn filtering_and_cursor_order_use_complete_text(ctx: &TestContext) {
    let filtered = run_query(
        ctx,
        &json!({
            "query_type": "traversal",
            "nodes": [{"id": "note", "entity": "Note", "node_ids": [30006, 30007],
            "columns": ["note"], "filters": {"note": {"ends_with": "xxz"}}}]
        })
        .to_string(),
        &permissions(),
    )
    .await;
    filtered.assert_node_count(1);
    filtered.assert_node_ids("Note", &[30006]);
    filtered.assert_filter("Note", "note", |note| {
        note.prop_str("note") == Some(&format!("{} [truncated]", "x".repeat(2048)))
    });

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

async fn redaction_still_removes_inaccessible_notes(ctx: &TestContext) {
    let mut permissions = permissions();
    permissions.deny("note", &[30005]);
    let response = run_query(ctx, &json!({
        "query_type": "traversal",
        "nodes": [{"id": "note", "entity": "Note", "node_ids": [30003, 30005], "columns": ["note"]}]
    }).to_string(), &permissions).await;
    response.assert_node_count(1);
    response.assert_node_ids("Note", &[30003]);
    response.assert_node_absent("Note", 30005);
}

async fn virtual_lookup_inputs_remain_complete(ctx: &TestContext) {
    let response = run_query(ctx, &json!({
        "query_type": "traversal",
        "nodes": [{"id": "file", "entity": "File", "node_ids": [50000], "columns": ["branch", "path", "name"]}]
    }).to_string(), &permissions()).await;
    response.assert_node_count(1);
    response.assert_node_ids("File", &[50000]);
    let file = response.find_node("File", 50000).unwrap();
    file.assert_str("branch", &"b".repeat(3000));
    file.assert_str("path", &"p".repeat(3000));
    file.assert_str("name", &format!("{} [truncated]", "n".repeat(2048)));
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

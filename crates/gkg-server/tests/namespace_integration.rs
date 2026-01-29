//! Integration tests for the namespace handler (groups, projects, notes, and merge requests).
//!
//! These tests require a Docker-compatible runtime (Docker, Colima, etc).

mod common;

use arrow::array::{BooleanArray, StringArray, UInt64Array};
use chrono::{DateTime, Utc};
use etl_engine::module::Module;
use etl_engine::testkit::TestEnvelopeFactory;
use gkg_server::indexer::modules::SdlcModule;
use serial_test::serial;

use common::{TestContext, create_namespace_payload};

#[tokio::test]
#[serial]
async fn namespace_handler_processes_and_transforms_groups() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES
            (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00'),
            (101, 'team-a', 'team-a', 10, 100, 2, '2023-06-01', '2024-01-10', '2024-01-20 12:00:00'),
            (102, 'team-b', 'team-b', 20, 100, NULL, '2023-09-01', '2024-01-05', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_namespace_details (namespace_id, description)
            VALUES
            (100, 'Organization 1'),
            (101, 'Team A under org1'),
            (102, NULL)",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES
            (100, '1/100/'),
            (101, '1/100/101/'),
            (102, '1/100/102/')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config)
        .await
        .expect("failed to create SDLC module");

    let handlers = sdlc_module.handlers();
    let namespace_handler = handlers
        .iter()
        .find(|h| h.name() == "namespace-handler")
        .expect("namespace-handler not found");

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT * FROM gl_groups ORDER BY id").await;
    assert!(!result.is_empty(), "groups result should not be empty");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3);

    let visibility_column = batch
        .column_by_name("visibility_level")
        .expect("visibility_level column should exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("visibility_level should be StringArray");

    assert_eq!(visibility_column.value(0), "private");
    assert_eq!(visibility_column.value(1), "internal");
    assert_eq!(visibility_column.value(2), "public");
}

#[tokio::test]
#[serial]
async fn namespace_handler_creates_group_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES
            (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00'),
            (101, 'team-a', 'team-a', 10, 100, 2, '2023-06-01', '2024-01-10', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_namespace_details (namespace_id, description)
            VALUES (100, 'Org'), (101, 'Team')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/'), (101, '1/100/101/')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config)
        .await
        .expect("failed to create SDLC module");

    let handlers = sdlc_module.handlers();
    let namespace_handler = handlers
        .iter()
        .find(|h| h.name() == "namespace-handler")
        .expect("namespace-handler not found");

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    // Verify owner edges
    let owner_edges = context
        .query("SELECT source_id, target_id FROM gl_edges WHERE relationship_kind = 'owner' ORDER BY target_id")
        .await;

    assert!(!owner_edges.is_empty(), "owner edges should exist");
    let batch = &owner_edges[0];
    assert_eq!(batch.num_rows(), 2, "should have 2 owner edges");

    // Verify parent-child edges
    let parent_edges = context
        .query("SELECT source_id, target_id FROM gl_edges WHERE relationship_kind = 'contains' AND source_kind = 'Group' AND target_kind = 'Group'")
        .await;

    assert!(!parent_edges.is_empty(), "parent edges should exist");
    let batch = &parent_edges[0];
    assert_eq!(
        batch.num_rows(),
        1,
        "should have 1 parent-child edge (100 contains 101)"
    );
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_projects() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_namespace_details (namespace_id, description)
            VALUES (100, 'Organization 1')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, description, visibility_level, path, namespace_id, creator_id, created_at, updated_at, archived, star_count, last_activity_at, _siphon_replicated_at)
            VALUES
            (1000, 'project-alpha', 'Alpha project', 0, 'project-alpha', 100, 1, '2023-01-01', '2024-01-15', false, 42, '2024-01-15', '2024-01-20 12:00:00'),
            (1001, 'project-beta', 'Beta project', 20, 'project-beta', 100, 2, '2023-06-01', '2024-01-10', true, 10, '2024-01-10', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/'), (1001, '1/100/1001/')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config)
        .await
        .expect("failed to create SDLC module");

    let handlers = sdlc_module.handlers();
    let namespace_handler = handlers
        .iter()
        .find(|h| h.name() == "namespace-handler")
        .expect("namespace-handler not found");

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT * FROM gl_projects ORDER BY id").await;
    assert!(!result.is_empty(), "projects result should not be empty");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    let visibility_column = batch
        .column_by_name("visibility_level")
        .expect("visibility_level column should exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("visibility_level should be StringArray");

    assert_eq!(visibility_column.value(0), "private");
    assert_eq!(visibility_column.value(1), "public");

    // Verify project edges (User/Group is source, Project is target - incoming direction)
    let creator_edges = context
        .query("SELECT source_id, target_id FROM gl_edges WHERE relationship_kind = 'creator' AND source_kind = 'User' AND target_kind = 'Project' ORDER BY target_id")
        .await;

    assert!(!creator_edges.is_empty(), "creator edges should exist");
    assert_eq!(creator_edges[0].num_rows(), 2);

    let contains_edges = context
        .query("SELECT source_id, target_id FROM gl_edges WHERE relationship_kind = 'contains' AND source_kind = 'Group' AND target_kind = 'Project'")
        .await;

    assert!(!contains_edges.is_empty(), "contains edges should exist");
    assert_eq!(contains_edges[0].num_rows(), 2);
}

#[tokio::test]
#[serial]
async fn namespace_handler_uses_watermark_for_incremental_processing() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO namespace_indexing_watermark (namespace, watermark)
            VALUES (100, '2024-01-19 00:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES
            (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-18 12:00:00'),
            (101, 'new-team', 'new-team', 10, 100, NULL, '2024-01-19', '2024-01-20', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_namespace_details (namespace_id, description)
            VALUES (100, 'Old org'), (101, 'New team')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/'), (101, '1/100/101/')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config)
        .await
        .expect("failed to create SDLC module");

    let handlers = sdlc_module.handlers();
    let namespace_handler = handlers
        .iter()
        .find(|h| h.name() == "namespace-handler")
        .expect("namespace-handler not found");

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT count() as cnt FROM gl_groups").await;
    let count_array = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("expected UInt64Array");

    assert_eq!(
        count_array.value(0),
        1,
        "should only process new-team, not org1"
    );

    let names = context.query("SELECT name FROM gl_groups").await;
    let name_array = names[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name should be StringArray");

    assert_eq!(name_array.value(0), "new-team");
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_notes_with_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_notes (id, note, noteable_type, noteable_id, author_id, system, internal, traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES
            (1, 'MR comment', 'MergeRequest', 100, 1, false, false, '1/100/', '2024-01-15', '2024-01-15', '2024-01-20 12:00:00'),
            (2, 'Work item note', 'WorkItem', 200, 2, false, false, '1/100/', '2024-01-16', '2024-01-16', '2024-01-20 12:00:00'),
            (3, 'Vuln comment', 'Vulnerability', 300, 1, false, true, '1/100/', '2024-01-17', '2024-01-17', '2024-01-20 12:00:00')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config)
        .await
        .expect("failed to create SDLC module");

    let handlers = sdlc_module.handlers();
    let namespace_handler = handlers
        .iter()
        .find(|h| h.name() == "namespace-handler")
        .expect("namespace-handler not found");

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT * FROM gl_notes ORDER BY id").await;
    assert!(!result.is_empty(), "notes result should not be empty");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3);

    // Verify author edges (User -> Note via authored)
    let author_edges = context
        .query("SELECT source_id, target_id FROM gl_edges WHERE relationship_kind = 'authored' AND source_kind = 'User' AND target_kind = 'Note' ORDER BY target_id")
        .await;

    assert!(!author_edges.is_empty(), "author edges should exist");
    assert_eq!(author_edges[0].num_rows(), 3, "should have 3 author edges");

    // Verify has_note edges (MergeRequest/WorkItem -> Note)
    let has_note_edges = context
        .query("SELECT source_id, source_kind, target_id FROM gl_edges WHERE relationship_kind = 'has_note' ORDER BY target_id")
        .await;

    assert!(!has_note_edges.is_empty(), "has_note edges should exist");
    let batch = &has_note_edges[0];
    assert_eq!(batch.num_rows(), 3, "should have 3 has_note edges");

    let source_kind = batch
        .column_by_name("source_kind")
        .expect("source_kind column should exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("source_kind should be StringArray");

    assert_eq!(source_kind.value(0), "MergeRequest");
    assert_eq!(source_kind.value(1), "WorkItem");
    assert_eq!(source_kind.value(2), "Vulnerability");
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_merge_requests_with_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO hierarchy_merge_requests
                (id, iid, title, description, source_branch, target_branch, state_id, merge_status,
                 draft, squash, target_project_id, author_id, assignee_id, merge_user_id,
                 traversal_path, version)
            VALUES
                (1, 101, 'Add feature X', 'Implements feature X', 'feature-x', 'main', 1, 'can_be_merged',
                 false, true, 1000, 1, 2, NULL, '1/100/', '2024-01-20 12:00:00'),
                (2, 102, 'Fix bug Y', 'Fixes critical bug', 'fix-y', 'main', 3, 'merged',
                 false, false, 1000, 2, NULL, 1, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config)
        .await
        .expect("failed to create SDLC module");

    let handlers = sdlc_module.handlers();
    let namespace_handler = handlers
        .iter()
        .find(|h| h.name() == "namespace-handler")
        .expect("namespace-handler not found");

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context
        .query("SELECT id, title, state, merge_status, draft, squash FROM gl_merge_requests ORDER BY id")
        .await;
    assert!(!result.is_empty(), "merge requests should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    let titles = batch
        .column_by_name("title")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(titles.value(0), "Add feature X");
    assert_eq!(titles.value(1), "Fix bug Y");

    let states = batch
        .column_by_name("state")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(states.value(0), "opened");
    assert_eq!(states.value(1), "merged");

    let in_project_edges = context
        .query(
            "SELECT source_id, target_id FROM gl_edges
             WHERE relationship_kind = 'in_project' AND source_kind = 'MergeRequest'",
        )
        .await;
    assert_eq!(
        in_project_edges[0].num_rows(),
        2,
        "both MRs should have in_project edges"
    );

    let authored_edges = context
        .query(
            "SELECT source_id, target_id FROM gl_edges
             WHERE relationship_kind = 'authored' AND target_kind = 'MergeRequest'
             ORDER BY target_id",
        )
        .await;
    assert_eq!(
        authored_edges[0].num_rows(),
        2,
        "both MRs should have author edges"
    );

    let assigned_edges = context
        .query(
            "SELECT target_id FROM gl_edges
             WHERE relationship_kind = 'assigned' AND target_kind = 'MergeRequest'",
        )
        .await;
    assert_eq!(assigned_edges[0].num_rows(), 1, "only MR 1 has an assignee");

    let merged_by_edges = context
        .query(
            "SELECT target_id FROM gl_edges
             WHERE relationship_kind = 'merged_by' AND target_kind = 'MergeRequest'",
        )
        .await;
    assert_eq!(merged_by_edges[0].num_rows(), 1, "only MR 2 was merged");
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_merge_request_diffs_with_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO hierarchy_merge_requests
                (id, iid, title, source_branch, target_branch, state_id, merge_status,
                 draft, squash, target_project_id, traversal_path, version)
            VALUES
                (1, 101, 'Add feature X', 'feature-x', 'main', 1, 'can_be_merged',
                 false, true, 1000, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_merge_request_diffs
                (id, merge_request_id, state, base_commit_sha, head_commit_sha, start_commit_sha,
                 commits_count, files_count, traversal_path, _siphon_replicated_at)
            VALUES
                (10, 1, 'collected', 'abc123', 'def456', 'ghi789', 3, 5, '1/100/', '2024-01-20 12:00:00'),
                (11, 1, 'collected', 'abc123', 'jkl012', 'ghi789', 4, 6, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config)
        .await
        .expect("failed to create SDLC module");

    let handlers = sdlc_module.handlers();
    let namespace_handler = handlers
        .iter()
        .find(|h| h.name() == "namespace-handler")
        .expect("namespace-handler not found");

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context
        .query("SELECT id, merge_request_id, state, commits_count, files_count FROM gl_merge_request_diffs ORDER BY id")
        .await;
    assert!(!result.is_empty(), "merge request diffs should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    let states = batch
        .column_by_name("state")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(states.value(0), "collected");
    assert_eq!(states.value(1), "collected");

    let has_diff_edges = context
        .query(
            "SELECT source_id, target_id FROM gl_edges
             WHERE relationship_kind = 'has_diff' AND source_kind = 'MergeRequest' AND target_kind = 'MergeRequestDiff'
             ORDER BY target_id",
        )
        .await;
    assert_eq!(
        has_diff_edges[0].num_rows(),
        2,
        "both diffs should have has_diff edges to the MR"
    );
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_merge_request_diff_files_with_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO hierarchy_merge_requests
                (id, iid, title, source_branch, target_branch, state_id, merge_status,
                 draft, squash, target_project_id, traversal_path, version)
            VALUES
                (1, 101, 'Add feature X', 'feature-x', 'main', 1, 'can_be_merged',
                 false, true, 1000, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_merge_request_diffs
                (id, merge_request_id, state, traversal_path, _siphon_replicated_at)
            VALUES
                (10, 1, 'collected', '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_merge_request_diff_files
                (merge_request_diff_id, relative_order, old_path, new_path, new_file, renamed_file,
                 deleted_file, too_large, binary, a_mode, b_mode, _siphon_replicated_at)
            VALUES
                (10, 0, 'src/main.rs', 'src/main.rs', false, false, false, false, false, '100644', '100644', '2024-01-20 12:00:00'),
                (10, 1, '', 'src/new_file.rs', true, false, false, false, false, '000000', '100644', '2024-01-20 12:00:00'),
                (10, 2, 'src/old_file.rs', '', false, false, true, false, false, '100644', '000000', '2024-01-20 12:00:00')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config)
        .await
        .expect("failed to create SDLC module");

    let handlers = sdlc_module.handlers();
    let namespace_handler = handlers
        .iter()
        .find(|h| h.name() == "namespace-handler")
        .expect("namespace-handler not found");

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context
        .query("SELECT merge_request_diff_id, old_path, new_path, new_file, deleted_file FROM gl_merge_request_diff_files ORDER BY old_path")
        .await;
    assert!(!result.is_empty(), "merge request diff files should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3);

    let new_file_flags = batch
        .column_by_name("new_file")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();

    let has_new_file = (0..batch.num_rows()).any(|i| new_file_flags.value(i));
    assert!(has_new_file, "should have at least one new file");

    let has_file_edges = context
        .query(
            "SELECT source_id, target_id FROM gl_edges
             WHERE relationship_kind = 'has_file' AND source_kind = 'MergeRequestDiff' AND target_kind = 'MergeRequestDiffFile'",
        )
        .await;
    assert_eq!(
        has_file_edges[0].num_rows(),
        3,
        "all diff files should have has_file edges to the diff"
    );
}

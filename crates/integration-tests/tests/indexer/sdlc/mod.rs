//! Consolidated SDLC integration tests.
//!
//! Each `#[tokio::test]` starts a single ClickHouse container and runs all
//! subtests in parallel, forking an isolated database per subtest to avoid
//! cross-test contamination while eliminating per-test container startup overhead.

mod ci;
mod global;
mod groups;
mod labels;
mod merge_request_diffs;
mod merge_requests;
mod milestones;
mod notes;
mod projects;
mod security;
mod watermarking;
mod work_items;

use super::common::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext};
use integration_testkit::run_subtests;

#[tokio::test]
async fn global_indexing() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    run_subtests!(
        &ctx,
        global::processes_and_transforms_users,
        global::uses_watermark_for_incremental_processing,
    );
}

#[tokio::test]
async fn namespace_indexing() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    run_subtests!(
        &ctx,
        projects::processes_projects,
        projects::computes_full_path_for_projects,
        projects::project_route_update_changes_full_path,
        projects::creates_member_of_edges_for_projects,
        groups::processes_and_transforms_groups,
        groups::computes_full_path_for_top_level_group,
        groups::computes_full_path_for_nested_subgroups,
        groups::creates_group_edges,
        groups::route_rename_updates_full_path,
        groups::child_route_reflects_parent_rename,
        groups::no_route_falls_back_to_slug,
        groups::creates_member_of_edges_for_groups,
        labels::processes_labels_with_edges,
        milestones::processes_milestones_with_edges,
        merge_requests::processes_merge_requests_with_edges,
        merge_requests::processes_merge_requests_closing_issues,
        merge_request_diffs::processes_merge_request_diffs_with_edges,
        merge_request_diffs::processes_merge_request_diff_files_with_edges,
        notes::processes_notes_with_edges,
        notes::filters_out_system_notes,
        work_items::processes_work_items_with_edges,
        work_items::processes_work_item_single_value_edges,
        work_items::processes_work_item_multi_target_edges,
        work_items::processes_work_item_parent_links,
        work_items::processes_issue_links,
        ci::processes_pipelines,
        ci::processes_stages,
        ci::processes_jobs,
        ci::processes_ci_hierarchy,
        security::processes_vulnerabilities,
        security::processes_scanners,
        security::processes_vulnerability_identifiers,
        security::processes_findings,
        security::processes_vulnerability_with_user_edges,
        security::processes_vulnerability_finding_edge,
        security::processes_vulnerability_occurrences,
        security::processes_vulnerability_merge_request_links,
        security::processes_vulnerability_occurrence_identifiers,
        security::processes_security_scans,
        security::processes_security_scan_finding_edges,
        watermarking::uses_watermark_for_incremental_processing,
    );
}

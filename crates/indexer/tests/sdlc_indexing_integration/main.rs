//! Consolidated SDLC integration tests.
//!
//! Each `#[tokio::test]` starts a single ClickHouse container and runs all
//! subtests sequentially, truncating tables between them to avoid cross-test
//! contamination while eliminating per-test container startup overhead.

#[path = "../common/mod.rs"]
mod common;

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

use common::{TestContext, run_subtest};
use serial_test::serial;

#[tokio::test]
#[serial]
async fn global_indexing() {
    let context = TestContext::new().await;

    run_subtest!(
        "processes_and_transforms_users",
        &context,
        global::processes_and_transforms_users
    );
    run_subtest!(
        "uses_watermark_for_incremental_processing",
        &context,
        global::uses_watermark_for_incremental_processing
    );
}

#[tokio::test]
#[serial]
async fn namespace_indexing() {
    let context = TestContext::new().await;

    // Projects
    run_subtest!("processes_projects", &context, projects::processes_projects);
    run_subtest!(
        "creates_member_of_edges_for_projects",
        &context,
        projects::creates_member_of_edges_for_projects
    );

    // Groups
    run_subtest!(
        "processes_and_transforms_groups",
        &context,
        groups::processes_and_transforms_groups
    );
    run_subtest!("creates_group_edges", &context, groups::creates_group_edges);
    run_subtest!(
        "creates_member_of_edges_for_groups",
        &context,
        groups::creates_member_of_edges_for_groups
    );

    // Labels
    run_subtest!(
        "processes_labels_with_edges",
        &context,
        labels::processes_labels_with_edges
    );

    // Milestones
    run_subtest!(
        "processes_milestones_with_edges",
        &context,
        milestones::processes_milestones_with_edges
    );

    // Merge requests
    run_subtest!(
        "processes_merge_requests_with_edges",
        &context,
        merge_requests::processes_merge_requests_with_edges
    );
    run_subtest!(
        "processes_merge_requests_closing_issues",
        &context,
        merge_requests::processes_merge_requests_closing_issues
    );

    // Merge request diffs
    run_subtest!(
        "processes_merge_request_diffs_with_edges",
        &context,
        merge_request_diffs::processes_merge_request_diffs_with_edges
    );
    run_subtest!(
        "processes_merge_request_diff_files_with_edges",
        &context,
        merge_request_diffs::processes_merge_request_diff_files_with_edges
    );

    // Notes
    run_subtest!(
        "processes_notes_with_edges",
        &context,
        notes::processes_notes_with_edges
    );
    run_subtest!(
        "filters_out_system_notes",
        &context,
        notes::filters_out_system_notes
    );

    // Work items
    run_subtest!(
        "processes_work_items_with_edges",
        &context,
        work_items::processes_work_items_with_edges
    );
    run_subtest!(
        "processes_work_item_single_value_edges",
        &context,
        work_items::processes_work_item_single_value_edges
    );
    run_subtest!(
        "processes_work_item_multi_target_edges",
        &context,
        work_items::processes_work_item_multi_target_edges
    );
    run_subtest!(
        "processes_work_item_parent_links",
        &context,
        work_items::processes_work_item_parent_links
    );
    run_subtest!(
        "processes_issue_links",
        &context,
        work_items::processes_issue_links
    );

    // CI
    run_subtest!("processes_pipelines", &context, ci::processes_pipelines);
    run_subtest!("processes_stages", &context, ci::processes_stages);
    run_subtest!("processes_jobs", &context, ci::processes_jobs);
    run_subtest!(
        "processes_ci_hierarchy",
        &context,
        ci::processes_ci_hierarchy
    );

    // Security
    run_subtest!(
        "processes_vulnerabilities",
        &context,
        security::processes_vulnerabilities
    );
    run_subtest!("processes_scanners", &context, security::processes_scanners);
    run_subtest!(
        "processes_vulnerability_identifiers",
        &context,
        security::processes_vulnerability_identifiers
    );
    run_subtest!("processes_findings", &context, security::processes_findings);
    run_subtest!(
        "processes_vulnerability_with_user_edges",
        &context,
        security::processes_vulnerability_with_user_edges
    );
    run_subtest!(
        "processes_vulnerability_finding_edge",
        &context,
        security::processes_vulnerability_finding_edge
    );
    run_subtest!(
        "processes_vulnerability_occurrences",
        &context,
        security::processes_vulnerability_occurrences
    );
    run_subtest!(
        "processes_vulnerability_merge_request_links",
        &context,
        security::processes_vulnerability_merge_request_links
    );
    run_subtest!(
        "processes_vulnerability_occurrence_identifiers",
        &context,
        security::processes_vulnerability_occurrence_identifiers
    );
    run_subtest!(
        "processes_security_scans",
        &context,
        security::processes_security_scans
    );
    run_subtest!(
        "processes_security_scan_finding_edges",
        &context,
        security::processes_security_scan_finding_edges
    );

    // Watermarking
    run_subtest!(
        "uses_watermark_for_incremental_processing",
        &context,
        watermarking::uses_watermark_for_incremental_processing
    );
}

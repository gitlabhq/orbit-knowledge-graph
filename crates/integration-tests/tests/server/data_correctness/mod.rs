mod aggregation;
mod dedup;
mod edge_cases;
mod helpers;
mod neighbors;
mod pagination;
mod path_finding;
mod search;
mod security;
mod traversal;
mod work_items;

use helpers::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, seed};
use integration_testkit::{run_subtests, run_subtests_shared};

#[tokio::test]
async fn data_correctness() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    seed(&ctx).await;

    run_subtests_shared!(
        &ctx,
        // search: column value correctness
        search::search_returns_correct_user_properties,
        search::search_returns_correct_project_properties,
        search::search_returns_correct_group_full_path,
        search::search_returns_correct_project_full_path,
        search::search_default_columns_include_full_path,
        search::search_filter_eq_returns_matching_rows,
        search::search_filter_in_returns_matching_rows,
        search::search_filter_starts_with_returns_matching_rows,
        search::search_filter_contains_returns_substring_matches,
        search::search_filter_is_null_matches_unset_columns,
        search::search_node_ids_returns_only_specified,
        search::search_with_order_by_desc,
        search::search_no_auth_returns_empty,
        search::search_redaction_returns_only_allowed_ids,
        search::search_unicode_properties_survive_pipeline,
        search::search_wildcard_columns_returns_all_ontology_fields,
        search::search_boolean_columns_have_correct_values,
        search::search_datetime_columns_serialize_as_strings,
        search::search_nullable_datetime_returns_null_when_unset,
        // search: limits
        search::search_limit_truncates_results,
        search::search_filter_no_match_returns_empty,
        search::search_combined_filter_node_ids_order_by,
        search::search_filter_gte_on_datetime_returns_matching_rows,
        search::search_filter_lte_on_datetime_returns_matching_rows,
        search::search_filter_lt_on_datetime_excludes_same_day_after_midnight,
        search::search_filter_is_not_null_on_datetime_returns_merged_rows,
        // traversal
        traversal::traversal_user_group_returns_correct_pairs_and_edges,
        traversal::traversal_three_hop_returns_all_user_group_project_paths,
        traversal::traversal_user_authored_mr_returns_correct_edges,
        traversal::traversal_user_approved_mr_returns_correct_edges,
        traversal::traversal_wildcard_user_to_mr_infers_relationship_kinds,
        traversal::traversal_redaction_removes_unauthorized_data,
        traversal::traversal_with_order_by,
        traversal::traversal_variable_length_reaches_depth_2,
        traversal::traversal_incoming_direction,
        traversal::traversal_with_filter_narrows_results,
        traversal::traversal_variable_length_min_hops_skips_shallow,
        traversal::traversal_variable_length_includes_depth_2_path_to_project,
        traversal::aggregation_variable_length_counts_all_depths,
        traversal::traversal_variable_length_with_redaction_at_depth,
        traversal::traversal_deduplicates_shared_nodes,
        traversal::traversal_shared_target_fan_in,
        traversal::traversal_order_by_node_property,
        traversal::traversal_order_by_source_node_property,
        traversal::traversal_order_by_with_node_ids_filter,
        // traversal: code graph cascades
        traversal::traversal_code_graph_calls_without_node_ids,
        traversal::traversal_code_graph_calls_with_node_ids,
        // aggregation
        aggregation::aggregation_count_returns_correct_values,
        aggregation::aggregation_wildcard_user_to_mr_counts_inferred_edges,
        aggregation::aggregation_count_group_contains_projects,
        aggregation::aggregation_sort_orders_by_aggregate_value,
        aggregation::aggregation_sum_produces_correct_totals,
        aggregation::aggregation_redaction_excludes_unauthorized_from_counts,
        aggregation::aggregation_avg_produces_correct_values,
        aggregation::aggregation_min_max_produce_correct_values,
        aggregation::aggregation_min_on_string_column,
        aggregation::aggregation_multiple_functions_in_one_query,
        // aggregation: traversal path authorization
        aggregation::aggregation_path_single_nested_group,
        aggregation::aggregation_path_multiple_groups,
        aggregation::aggregation_sum_with_restricted_path,
        aggregation::aggregation_nested_path_includes_child_projects,
        aggregation::aggregation_non_nested_path_only,
        aggregation::aggregation_group_by_non_default_redaction_id_column,
        aggregation::aggregation_three_node_with_cascade_intermediate,
        aggregation::aggregation_empty_security_context_rejects_at_compile,
        aggregation::aggregation_no_group_by_with_filtered_other_node,
        aggregation::aggregation_no_group_by_preserves_relationship_kind,
        // path finding
        path_finding::path_finding_returns_valid_complete_paths,
        path_finding::path_finding_filtered_start_endpoint_reaches_project,
        path_finding::path_finding_wildcard_keeps_intermediate_hops_unconstrained,
        path_finding::path_finding_multiple_destinations_returns_distinct_paths,
        path_finding::path_finding_consecutive_edges_connect,
        path_finding::path_finding_max_depth_too_shallow_returns_empty,
        path_finding::path_finding_redaction_blocks_intermediate_node,
        path_finding::path_finding_all_shortest_returns_valid_paths,
        path_finding::path_finding_any_returns_at_least_one_path,
        path_finding::path_finding_rel_types_restricts_traversal,
        path_finding::path_finding_step_indices_are_sequential,
        path_finding::path_finding_target_entity_constrains_results,
        path_finding::path_finding_entity_filter_excludes_wrong_types,
        path_finding::path_finding_code_filtered_endpoints_stay_on_same_traversal_path,
        // neighbors
        neighbors::neighbors_outgoing_returns_correct_targets,
        neighbors::neighbors_incoming_returns_correct_sources,
        neighbors::neighbors_rel_types_filter_works,
        neighbors::neighbors_both_direction_returns_all_connected,
        neighbors::neighbors_mixed_entity_types,
        neighbors::neighbors_redaction_removes_unauthorized_targets,
        neighbors::neighbors_dynamic_columns_all_returns_properties,
        neighbors::neighbors_center_node_properties_hydrated,
        neighbors::neighbors_both_direction_preserves_edge_direction,
        // edge cases
        edge_cases::giant_string_survives_pipeline,
        edge_cases::sql_injection_string_preserved,
        edge_cases::empty_result_has_valid_schema,
        // SIP pre-filter correctness
        edge_cases::sip_prefilter_with_node_ids_returns_correct_results,
        edge_cases::sip_prefilter_with_filter_returns_correct_results,
        edge_cases::sip_prefilter_multi_hop_returns_correct_results,
        edge_cases::sip_target_aggregation_with_filter_returns_correct_counts,
        // cross-namespace correctness
        edge_cases::cross_namespace_user_authors_mr_in_different_group,
        edge_cases::cross_namespace_group_containment_across_depth,
        edge_cases::cross_namespace_isolation_no_leakage,
        edge_cases::cross_namespace_narrow_scope_returns_all_authors,
        edge_cases::cross_namespace_aggregation_respects_scope,
        edge_cases::neighbors_cross_namespace_no_false_positives,
        // non-default redaction id_column
        edge_cases::non_default_redaction_id_entity_traversal,
        edge_cases::non_default_redaction_id_denies_unauthorized,
        edge_cases::non_default_redaction_id_with_multiple_mrs,
        // referential integrity
        edge_cases::traversal_referential_integrity_on_complex_query,
        // LIKE data correctness
        edge_cases::like_contains_returns_matching_rows,
        edge_cases::like_contains_matches_multiple,
        edge_cases::like_contains_no_match_returns_empty,
        edge_cases::like_starts_with_returns_matching_rows,
        edge_cases::like_starts_with_no_match,
        edge_cases::like_ends_with_returns_matching_rows,
        edge_cases::like_percent_matched_literally,
        edge_cases::like_underscore_matched_literally,
        edge_cases::like_equality_on_email_returns_correct_row,
        edge_cases::like_in_filter_on_email_works,
        // filterable: false data correctness
        edge_cases::filterable_traversal_path_readable_as_column,
        edge_cases::filterable_traversal_path_readable_on_project,
        edge_cases::filterable_other_filters_still_work_alongside_traversal_path_column,
        // security: traversal path scoping for search
        security::search_scoped_path_excludes_other_namespaces,
        security::search_scoped_to_single_project_namespace,
        security::search_multi_path_returns_union_of_scopes,
        security::search_scoped_mr_excludes_other_namespaces,
        security::search_with_filter_respects_scope,
        // security: traversal path scoping for path finding
        security::path_finding_scoped_excludes_paths_through_other_namespaces,
        security::path_finding_multi_path_scope_finds_both,
        security::path_finding_narrow_scope_excludes_all_targets,
        // security: admin_only field restriction (RestrictPass)
        security::admin_only_non_admin_filter_rejects_at_compile,
        security::admin_only_non_admin_order_by_rejects_at_compile,
        security::admin_only_non_admin_max_aggregation_rejects_at_compile,
        security::admin_only_non_admin_count_aggregation_on_auditor_rejects_at_compile,
        security::admin_only_non_admin_wildcard_columns_excludes_admin_fields,
        security::admin_only_non_admin_explicit_columns_silently_stripped,
        security::admin_only_admin_filter_compiles,
        security::admin_only_admin_order_by_compiles,
        security::admin_only_admin_aggregation_compiles,
        security::admin_only_admin_wildcard_columns_includes_admin_fields,
        // security: admin_only on dynamic hydration (Neighbors / PathFinding)
        security::admin_only_non_admin_neighbors_dynamic_wildcard_strips_admin_fields,
        security::admin_only_non_admin_neighbors_dynamic_center_node_strips_admin_fields,
        security::admin_only_non_admin_path_finding_dynamic_wildcard_strips_admin_fields,
        security::admin_only_admin_neighbors_dynamic_wildcard_includes_admin_fields,
        // security: cross-organization isolation
        security::cross_org_search_excludes_other_org,
        security::cross_org_traversal_excludes_other_org,
        security::cross_org_aggregation_excludes_other_org,
        security::cross_org_inverse_isolation,
        // security: aggregation SQL assertions
        security::aggregation_sql_contains_traversal_path_filter,
        security::aggregation_multi_path_sql_contains_both_filters,
        security::aggregation_multi_path_returns_union_of_scopes,
        // security: globally-scoped entity guard (work_items/347)
        security::aggregation_user_only_rejects_at_compile,
        security::aggregation_user_only_with_pii_filter_rejects_at_compile,
        security::aggregation_user_joined_to_scoped_group_compiles,
        security::aggregation_user_only_admin_still_compiles,
        security::aggregation_user_only_rejection_happens_before_sql_compile,
        security::aggregation_user_only_neighbors_query_is_not_blocked,
        security::aggregation_user_joined_runtime_returns_expected_counts,
        security::aggregation_user_disconnected_scoped_node_rejects_at_compile,
        security::aggregation_user_reachable_via_path_compiles,
        // security: per-entity role scoping on aggregation target nodes
        security::aggregation_vulnerability_reporter_only_sees_zero_counts,
        security::aggregation_vulnerability_mixed_roles_only_surfaces_developer_paths,
        security::aggregation_vulnerability_security_manager_meets_the_required_floor,
        security::aggregation_vulnerability_developer_everywhere_sees_all_counts,
        security::search_vulnerability_reporter_only_returns_empty,
        security::aggregation_vulnerability_filter_oracle_is_neutralized,
        security::aggregation_vulnerability_sql_drops_reporter_paths,
        // cursor pagination
        pagination::cursor_first_page,
        pagination::cursor_second_page,
        pagination::cursor_last_page_partial,
        pagination::cursor_offset_beyond_data,
        pagination::cursor_with_filter,
        pagination::cursor_with_filter_second_page,
        pagination::cursor_with_redaction,
        pagination::cursor_with_redaction_second_page,
        pagination::cursor_pages_cover_all_data,
        pagination::cursor_traversal,
        pagination::cursor_without_order_by_is_deterministic,
        pagination::cursor_without_order_by_pages_cover_all_data,
        pagination::cursor_traversal_without_order_by_is_deterministic,
        pagination::cursor_aggregation_without_sort_is_deterministic,
        pagination::cursor_path_finding_pages_cover_all_paths,
        pagination::cursor_path_finding_is_deterministic,
        // work items: search
        work_items::search_returns_correct_work_item_properties,
        work_items::search_filter_work_item_type_returns_matching_rows,
        // work items: traversal (all 7 edge types)
        work_items::traversal_user_authored_work_item_returns_correct_edges,
        work_items::traversal_work_item_in_group_returns_correct_edges,
        work_items::traversal_work_item_in_project_returns_correct_edges,
        work_items::traversal_user_closed_work_item_returns_correct_edges,
        work_items::traversal_work_item_in_milestone_returns_correct_edges,
        work_items::traversal_user_assigned_work_item_returns_correct_edges,
        work_items::traversal_work_item_has_label_returns_correct_edges,
    );

    // Dedup tests INSERT extra rows, so they run in forked (isolated) databases
    // to avoid cross-test data interference.
    run_subtests!(
        &ctx,
        dedup::search_returns_latest_version,
        dedup::search_excludes_deleted_rows,
        dedup::search_filter_returns_latest_matching_version,
        dedup::search_filter_excludes_stale_match,
        dedup::aggregation_dedup_counts_unique_entities,
        dedup::aggregation_filter_excludes_stale_mutable_match,
        dedup::traversal_dedup_returns_single_edge,
        dedup::traversal_filter_excludes_stale_version,
        dedup::traversal_deleted_node_visible_via_edge,
        dedup::neighbors_dedup_returns_unique_edges,
        dedup::neighbors_deleted_node_visible_via_edge,
        dedup::hydration_returns_latest_properties,
        dedup::traversal_excludes_deleted_edge,
        dedup::search_three_versions_returns_latest,
        dedup::aggregation_excludes_deleted_from_count,
    );
}

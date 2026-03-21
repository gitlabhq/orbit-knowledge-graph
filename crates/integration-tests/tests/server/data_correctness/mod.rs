mod aggregation;
mod edge_cases;
mod helpers;
mod neighbors;
mod path_finding;
mod search;
mod traversal;

use helpers::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, seed};
use integration_testkit::run_subtests_shared;

#[tokio::test]
async fn data_correctness() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    seed(&ctx).await;

    run_subtests_shared!(
        &ctx,
        // search: column value correctness
        search::search_returns_correct_user_properties,
        search::search_returns_correct_project_properties,
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
        // search: pagination, limits, combined
        search::search_range_returns_paginated_results,
        search::search_limit_truncates_results,
        search::search_filter_no_match_returns_empty,
        search::search_combined_filter_node_ids_order_by,
        // traversal
        traversal::traversal_user_group_returns_correct_pairs_and_edges,
        traversal::traversal_three_hop_returns_all_user_group_project_paths,
        traversal::traversal_user_authored_mr_returns_correct_edges,
        traversal::traversal_redaction_removes_unauthorized_data,
        traversal::traversal_with_order_by,
        traversal::traversal_variable_length_reaches_depth_2,
        traversal::traversal_incoming_direction,
        traversal::traversal_with_filter_narrows_results,
        traversal::traversal_variable_length_min_hops_skips_shallow,
        traversal::traversal_variable_length_with_redaction_at_depth,
        traversal::traversal_deduplicates_shared_nodes,
        traversal::traversal_shared_target_fan_in,
        // aggregation
        aggregation::aggregation_count_returns_correct_values,
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
        aggregation::aggregation_empty_security_context_rejects_at_compile,
        // optimizer: target elimination and edge-only aggregation correctness
        aggregation::aggregation_count_with_target_elimination,
        aggregation::aggregation_count_with_edge_only_root_elimination,
        // path finding
        path_finding::path_finding_returns_valid_complete_paths,
        path_finding::path_finding_multiple_destinations_returns_distinct_paths,
        path_finding::path_finding_consecutive_edges_connect,
        path_finding::path_finding_max_depth_too_shallow_returns_empty,
        path_finding::path_finding_redaction_blocks_intermediate_node,
        path_finding::path_finding_all_shortest_returns_valid_paths,
        path_finding::path_finding_any_returns_at_least_one_path,
        path_finding::path_finding_rel_types_restricts_traversal,
        path_finding::path_finding_step_indices_are_sequential,
        // neighbors
        neighbors::neighbors_outgoing_returns_correct_targets,
        neighbors::neighbors_incoming_returns_correct_sources,
        neighbors::neighbors_rel_types_filter_works,
        neighbors::neighbors_both_direction_returns_all_connected,
        neighbors::neighbors_mixed_entity_types,
        neighbors::neighbors_redaction_removes_unauthorized_targets,
        neighbors::neighbors_dynamic_columns_all_returns_properties,
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
        // referential integrity
        edge_cases::traversal_referential_integrity_on_complex_query,
    );
}

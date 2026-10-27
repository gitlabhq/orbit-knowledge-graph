use orbit_server_config::QueryConfig;

use crate::ast::visit::{visit_queries, visit_relations};
use crate::ast::{Node, TableRef};
use crate::error::Result;
use crate::input::{Input, QueryType};
use crate::passes::plan::QueryPlan;

// Deployment config can tighten these compiler-enforced ceilings but cannot raise them.
const PATHFINDING_EXECUTION_TIME_CEILING_SECONDS: u64 = 15;
const PATHFINDING_MEMORY_CEILING_BYTES: u64 = 15 * 1024 * 1024 * 1024;

const IN_SUBQUERY_INDEX_MAX_VALUES: u64 = 1_000_000;

pub fn resolve(input: &Input, node: &Node, plan: &QueryPlan) -> Result<QueryConfig> {
    let query_type: &str = input.query_type.into();
    let mut config = orbit_server_config::query::for_query_type(query_type);

    if let Node::Query(query) = node {
        let derived = &mut config.compiler_derived;
        derived.null_in_is_unknown = input.has_negation();
        visit_queries(query, &mut |query| {
            derived.enable_materialized_cte |= query.ctes.iter().any(|cte| cte.materialized);
            visit_relations(&query.from, &mut |relation| {
                let final_scan = matches!(relation, TableRef::Scan { final_: true, .. });
                derived.optimize_move_to_prewhere_if_final |= final_scan;
                derived.use_skip_indexes_if_final_exact |= final_scan;
            });
            if !query.ctes.is_empty() {
                derived.use_index_for_in_with_subqueries_max_values =
                    Some(IN_SUBQUERY_INDEX_MAX_VALUES);
            }
            Ok(())
        })?;
    }

    if plan.relations().len() >= 3 {
        config.compiler_derived.join_order_algorithm = Some("dpsize".into());
    }
    if plan.is_point_lookup() && config.use_query_cache == Some(true) {
        config.use_query_cache = Some(false);
    }
    if input.query_type == QueryType::PathFinding {
        if config.max_execution_time.is_none()
            || config.max_execution_time > Some(PATHFINDING_EXECUTION_TIME_CEILING_SECONDS)
        {
            config.max_execution_time = Some(PATHFINDING_EXECUTION_TIME_CEILING_SECONDS);
        }
        if config.max_memory_usage.is_none()
            || config.max_memory_usage > Some(PATHFINDING_MEMORY_CEILING_BYTES)
        {
            config.max_memory_usage = Some(PATHFINDING_MEMORY_CEILING_BYTES);
        }
    }
    Ok(config)
}

use orbit_server_config::QueryConfig;

use crate::ast::visit::{visit_queries, visit_relations};
use crate::ast::{Node, TableRef};
use crate::error::Result;
use crate::input::{Input, QueryType};
use crate::passes::plan::QueryPlan;

const PATHFINDING_MAX_EXECUTION_TIME: u64 = 15;
const PATHFINDING_MAX_MEMORY_USAGE: u64 = 16_106_127_360;
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
                derived.optimize_move_to_prewhere_if_final |=
                    matches!(relation, TableRef::Scan { final_: true, .. });
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
            || config.max_execution_time > Some(PATHFINDING_MAX_EXECUTION_TIME)
        {
            config.max_execution_time = Some(PATHFINDING_MAX_EXECUTION_TIME);
        }
        if config.max_memory_usage.is_none()
            || config.max_memory_usage > Some(PATHFINDING_MAX_MEMORY_USAGE)
        {
            config.max_memory_usage = Some(PATHFINDING_MAX_MEMORY_USAGE);
        }
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_returns_default_for_unknown_type() {
        let cfg = orbit_server_config::query::for_query_type("nonexistent");
        assert_eq!(cfg, QueryConfig::default());
    }
}

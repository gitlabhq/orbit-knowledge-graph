use orbit_server_config::QueryConfig;

use query_data_model::implementations::relational::plan::{Operation, ReadMode, Sharing};

use crate::passes::codegen::ClickHousePlan;
use crate::types::QueryType;

// Deployment config can tighten these compiler-enforced ceilings but cannot raise them.
const PATHFINDING_EXECUTION_TIME_CEILING_SECONDS: u64 = 15;
const PATHFINDING_MEMORY_CEILING_BYTES: u64 = 15 * 1024 * 1024 * 1024;

const IN_SUBQUERY_INDEX_MAX_VALUES: u64 = 1_000_000;

pub fn resolve(
    query_type: QueryType,
    negation: bool,
    relational: &ClickHousePlan,
    relationships: usize,
    point_lookup: bool,
) -> QueryConfig {
    let mut config = orbit_server_config::query::for_query_type(query_type.into());

    let derived = &mut config.compiler_derived;
    derived.null_in_is_unknown = negation;
    derived.enable_materialized_cte = relational
        .bodies
        .iter()
        .any(|body| body.sharing == Sharing::Shared { materialized: true });
    derived.final_scan = relational.plan.nodes().any(|node| {
        matches!(
            relational.plan.operation(node),
            Operation::Scan {
                read: ReadMode::Current,
                ..
            }
        )
    });
    let shared = relational
        .bodies
        .iter()
        .any(|body| matches!(body.sharing, Sharing::Shared { .. }));
    let membership = relational
        .plan
        .nodes()
        .any(|node| !relational.plan.operation(node).memberships().is_empty());
    if shared || membership {
        derived.use_index_for_in_with_subqueries_max_values = Some(IN_SUBQUERY_INDEX_MAX_VALUES);
    }

    if relationships >= 3 {
        config.compiler_derived.join_order_algorithm = Some("dpsize".into());
    }
    if point_lookup && config.use_query_cache == Some(true) {
        config.use_query_cache = Some(false);
    }
    if query_type == QueryType::PathFinding {
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
    config
}

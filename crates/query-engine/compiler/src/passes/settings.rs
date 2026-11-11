use orbit_server_config::{QueryConfig, QueryLimits};

use query_data_model::implementations::relational::plan::{Operation, ReadMode, Sharing};

use crate::passes::codegen::ClickHousePlan;
use crate::types::QueryType;

pub fn resolve(
    query_type: QueryType,
    negation: bool,
    relational: &ClickHousePlan,
    relationships: usize,
    point_lookup: bool,
    limits: &QueryLimits,
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
        derived.use_index_for_in_with_subqueries_max_values = Some(limits.in_subquery_index_values);
    }

    if relationships >= 3 {
        config.compiler_derived.join_order_algorithm = Some("dpsize".into());
    }
    if point_lookup && config.use_query_cache == Some(true) {
        config.use_query_cache = Some(false);
    }
    if query_type == QueryType::PathFinding {
        let time = limits.path_finding_max_execution_time;
        if config
            .max_execution_time
            .is_none_or(|seconds| seconds > time)
        {
            config.max_execution_time = Some(time);
        }
        let memory = limits.path_finding_max_memory_usage;
        if config.max_memory_usage.is_none_or(|bytes| bytes > memory) {
            config.max_memory_usage = Some(memory);
        }
    }
    config
}

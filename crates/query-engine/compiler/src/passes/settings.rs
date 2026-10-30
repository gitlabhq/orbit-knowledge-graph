use orbit_server_config::QueryConfig;

use crate::ast::visit::{visit_queries, visit_relations};
use crate::ast::{Node, TableRef};
use crate::error::Result;
use crate::input::Input;
use crate::passes::plan::QueryPlan;

pub fn resolve(input: &Input, node: &Node, plan: &QueryPlan) -> Result<QueryConfig> {
    let query_type: &str = input.query_type.into();
    let mut config = orbit_server_config::query::for_query_type(query_type);
    let when = orbit_server_config::query::shape_settings();

    let (mut materialized, mut final_scan, mut ctes) = (false, false, false);
    if let Node::Query(query) = node {
        visit_queries(query, &mut |query| {
            materialized |= query.ctes.iter().any(|cte| cte.materialized);
            ctes |= !query.ctes.is_empty();
            visit_relations(&query.from, &mut |relation| {
                final_scan |= matches!(relation, TableRef::Scan { final_: true, .. });
            });
            Ok(())
        })?;
    }

    if final_scan {
        config = config.merge(&when.final_scan);
    }
    if ctes {
        config = config.merge(&when.with_ctes);
    }
    if plan.relations().len() >= 3 {
        config = config.merge(&when.multi_join);
    }
    if materialized {
        config.set("enable_materialized_cte", true);
    }
    if input.has_negation() {
        config.set("transform_null_in", false);
    }
    if plan.is_point_lookup() && config.get_bool("use_query_cache") == Some(true) {
        config.set("use_query_cache", false);
    }
    Ok(config)
}

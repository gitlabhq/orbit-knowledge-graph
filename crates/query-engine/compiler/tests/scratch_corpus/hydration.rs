use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

use compiler::{
    CompiledQueryContext, HydrationArm, HydrationCompileOptions, HydrationPlan, HydrationRequest,
    QueryType, SecurityContext, compile_hydration,
};
use query_data_model::{Catalog, ClickHouse};

pub fn dump(
    out: &mut String,
    compiled: &CompiledQueryContext,
    model: &Arc<Catalog<ClickHouse>>,
    ctx: &SecurityContext,
) {
    let scope: Vec<_> = ctx.traversal_paths.iter().map(|p| p.path.clone()).collect();
    let specs: Vec<(String, Vec<String>, bool)> = match &compiled.hydration {
        HydrationPlan::None => return,
        HydrationPlan::Static(t) => t
            .iter()
            .map(|t| {
                (
                    t.entity_type.clone(),
                    t.columns.clone(),
                    t.has_traversal_path,
                )
            })
            .collect(),
        HydrationPlan::Dynamic(d) => d
            .iter()
            .map(|d| {
                (
                    d.entity_type.clone(),
                    d.columns.clone(),
                    d.has_traversal_path,
                )
            })
            .collect(),
    };
    let cap = match &compiled.hydration {
        HydrationPlan::Dynamic(_) => 3,
        _ => usize::MAX,
    };
    let mut arms = Vec::new();
    for (entity, columns, tp) in specs
        .into_iter()
        .filter(|(_, columns, _)| !columns.is_empty())
        .take(cap)
    {
        let paths = if tp { scope.clone() } else { Vec::new() };
        match HydrationArm::new(model.model(), &entity, &[1, 2], &columns, paths) {
            Ok(arm) => arms.push(arm),
            Err(error) => {
                let _ = writeln!(out, "hydration error: {error}");
                return;
            }
        }
    }
    let options = HydrationCompileOptions {
        dynamic: matches!(
            compiled.query_type,
            QueryType::Neighbors | QueryType::PathFinding
        ),
        path_segment_budget: Some(orbit_utils::clickhouse::MAX_BOUND_PATH_SEGMENTS),
    };
    match compile_hydration(HydrationRequest { arms, options }, model, ctx) {
        Ok(h) => {
            let _ = writeln!(out, "hydration sql: {}", h.sql);
            let params: BTreeMap<_, _> = h.params.iter().collect();
            for (key, value) in params {
                let _ = writeln!(out, "hydration param {key}: {value:?}");
            }
        }
        Err(error) => {
            let _ = writeln!(out, "hydration error: {error}");
        }
    }
}

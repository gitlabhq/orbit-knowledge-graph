use std::collections::HashSet;

use query_data_model::{ClickHouseDataModel, QueryBackendCatalog, QueryDataModel};

use super::{HydrationCompileOptions, QueryPlan, context::PlanningContext};
use crate::{Input, Result, input::QueryType};

pub fn plan(
    input: &Input,
    model: &ClickHouseDataModel,
    hydration_options: HydrationCompileOptions,
    table_scans: &HashSet<String>,
) -> Result<QueryPlan> {
    let context = PlanningContext::new(input, model);
    match input.query_type {
        QueryType::Traversal | QueryType::Aggregation => {
            super::edge_chain::plan(context, table_scans)
        }
        QueryType::Neighbors => super::neighbors::plan_neighbors(context).map(QueryPlan::Neighbors),
        QueryType::PathFinding => {
            super::pathfinding::plan_pathfinding(context).map(QueryPlan::PathFinding)
        }
        QueryType::Hydration => {
            super::hydration::plan_hydration(context, hydration_options).map(QueryPlan::Hydration)
        }
    }
}

pub(super) fn select_read_layout(
    model: &ClickHouseDataModel,
    canonical: &str,
    equality_columns: &[&str],
) -> String {
    let score = |table: &str| {
        model
            .table_sort_key(table)
            .unwrap_or_default()
            .iter()
            .skip_while(|column| column.as_str() == ontology::TRAVERSAL_PATH_COLUMN)
            .take_while(|column| equality_columns.contains(&column.as_str()))
            .count()
    };
    let mut selected = canonical;
    let mut best = score(canonical);
    for table in model.query_backend().equivalent_layouts(canonical) {
        let candidate = score(table);
        if candidate > best {
            selected = table;
            best = candidate;
        }
    }
    selected.to_string()
}

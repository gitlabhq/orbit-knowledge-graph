use super::*;
use crate::passes::logical_v3::{LogicalOp, LogicalPlan, LogicalSource, Plan, RelationId};

pub fn plan_clickhouse(
    logical: LogicalPlan,
    catalog: &PhysicalCatalog<'_>,
) -> PhysicalPlan<ClickHouse> {
    map(logical.root, catalog, &|relation, source, catalog| match source {
        LogicalSource::Node { .. } => ClickHouseAccess::Table(
            catalog.node_table(relation).unwrap_or_default().to_string(),
        ),
        LogicalSource::Edge { .. } => {
            ClickHouseAccess::EdgeTables(catalog.edge_tables(relation).to_vec())
        }
    })
}

pub fn plan_duckdb(
    logical: LogicalPlan,
    catalog: &PhysicalCatalog<'_>,
) -> PhysicalPlan<DuckDb> {
    map(logical.root, catalog, &|relation, source, catalog| match source {
        LogicalSource::Node { .. } => DuckDbAccess::Table(
            catalog.node_table(relation).unwrap_or_default().to_string(),
        ),
        LogicalSource::Edge { .. } => DuckDbAccess::Table(
            catalog.edge_tables(relation).first().cloned().unwrap_or_default(),
        ),
    })
}

fn map<B: Backend>(
    logical: crate::passes::logical_v3::Rel,
    catalog: &PhysicalCatalog<'_>,
    access: &impl Fn(RelationId, &LogicalSource, &PhysicalCatalog<'_>) -> B::Access,
) -> PhysicalPlan<B> {
    let inputs: Vec<_> = logical
        .inputs
        .into_iter()
        .map(|input| map(input, catalog, access))
        .collect();
    match logical.op {
        LogicalOp::Scan(source) => {
            let relation = match &source {
                LogicalSource::Node { relation, .. } | LogicalSource::Edge { relation, .. } => {
                    *relation
                }
            };
            let alias = match &source {
                LogicalSource::Node { alias, .. } | LogicalSource::Edge { alias, .. } => {
                    alias.clone()
                }
            };
            let scan = Plan::leaf(PhysicalOp::Scan {
                relation,
                alias,
                access: access(relation, &source, catalog),
            });
            scan
        }
        LogicalOp::LatestBy(keys) => Plan::nary(
            PhysicalOp::Deduplicate {
                keys,
                strategy: B::latest_by(),
            },
            inputs,
        ),
        op => Plan {
            op: match op {
                LogicalOp::Filter(predicate) => PhysicalOp::Filter(predicate),
                LogicalOp::Project(columns) => PhysicalOp::Project(columns),
                LogicalOp::Join(conditions) => PhysicalOp::Join {
                    conditions,
                    strategy: JoinStrategy::Default,
                },
                LogicalOp::SemiJoin(condition) => PhysicalOp::SemiJoin(condition),
                LogicalOp::Aggregate { groups, metrics } => {
                    PhysicalOp::Aggregate { groups, metrics }
                }
                LogicalOp::Union => PhysicalOp::Union,
                LogicalOp::Alias { relation, alias } => PhysicalOp::Alias { relation, alias },
                LogicalOp::Sort(keys) => PhysicalOp::Sort(keys),
                LogicalOp::Limit(limit) => PhysicalOp::Limit(limit),
                LogicalOp::Scan(_) | LogicalOp::LatestBy(_) => unreachable!(),
            },
            inputs,
        },
    }
}

use super::sql::latest_row_dedup;
use crate::ast::{Cte, Expr, Query, SelectExpr, TableRef};
use crate::passes::plan::physical::{ExecutionPlan, PhysicalPlan, PhysicalSource};

use super::requirements::{column, predicate, projections};
use super::{EmitOutput, NodeBinding};

pub(super) fn execute(plan: &ExecutionPlan) -> EmitOutput {
    let source = emit_source(&plan.source);
    EmitOutput {
        from: source.from,
        where_parts: source.predicates,
        select: projections(&plan.outputs),
        ctes: plan
            .subplans
            .iter()
            .map(|subplan| {
                let mut cte = Cte::new(subplan_name(subplan.id), query(&subplan.plan));
                cte.materialized = subplan.materialized;
                cte
            })
            .collect(),
        nodes: plan
            .bindings
            .iter()
            .map(|binding| {
                (
                    binding.node.clone(),
                    NodeBinding::source(
                        &binding.alias,
                        &binding.column,
                        binding.joined.then(|| binding.node.clone()),
                    ),
                )
            })
            .collect(),
    }
}

fn subplan_name(id: crate::passes::plan::physical::SubplanId) -> String {
    format!("_orbit_subplan_{}", id.0)
}

pub(super) fn query(plan: &PhysicalPlan) -> Query {
    let (source, outputs, distinct) = match plan {
        PhysicalPlan::Project { source, outputs } => (source, outputs, false),
        PhysicalPlan::Distinct { source, outputs } => (source, outputs, true),
    };
    let (source, order_by, limit_by, condition) = match source {
        PhysicalSource::Latest {
            alias,
            sort_key,
            input,
            aggregate_condition,
        } => {
            let (order_by, limit_by) = latest_row_dedup(alias, sort_key);
            (
                input.as_ref(),
                order_by,
                limit_by,
                aggregate_condition.as_slice(),
            )
        }
        source => (source, vec![], None, [].as_slice()),
    };
    let mut output = emit_source(source);
    output.predicates.extend(condition.iter().map(predicate));
    Query {
        distinct,
        select: projections(outputs),
        from: output.from,
        where_clause: Expr::conjoin(output.predicates),
        order_by,
        limit_by,
        ..Default::default()
    }
}

struct SourceOutput {
    from: TableRef,
    predicates: Vec<Expr>,
}

fn emit_source(plan: &PhysicalSource) -> SourceOutput {
    match plan {
        PhysicalSource::Union {
            alias,
            arms,
            relationship,
        } => {
            let queries = arms.iter().map(query).collect();
            SourceOutput {
                from: TableRef::union_all(queries, alias).with_relationship(*relationship),
                predicates: vec![],
            }
        }
        PhysicalSource::Scan {
            table,
            alias,
            final_,
            relationship,
        } => SourceOutput {
            from: TableRef::Scan {
                table: table.clone(),
                alias: alias.clone(),
                final_: *final_,
                relationship: *relationship,
            },
            predicates: vec![],
        },
        PhysicalSource::Use { id, alias } => SourceOutput {
            from: TableRef::cte(subplan_name(*id), alias),
            predicates: vec![],
        },
        PhysicalSource::Filter { predicates, input } => {
            let mut output = emit_source(input);
            output.predicates.extend(predicates.iter().map(predicate));
            output
        }
        PhysicalSource::Project {
            alias,
            outputs,
            input,
        } => {
            let output = emit_source(input);
            SourceOutput {
                from: TableRef::subquery(
                    Query {
                        select: projections(outputs),
                        from: output.from,
                        where_clause: Expr::conjoin(output.predicates),
                        ..Default::default()
                    },
                    alias,
                ),
                predicates: vec![],
            }
        }
        PhysicalSource::Scope { alias, input } | PhysicalSource::Latest { alias, input, .. } => {
            let mut output = emit_source(input);
            let (order_by, limit_by) = match plan {
                PhysicalSource::Latest {
                    sort_key,
                    aggregate_condition,
                    ..
                } => {
                    let outer = aggregate_condition.iter().map(predicate).collect();
                    let (order_by, limit_by) = latest_row_dedup(alias, sort_key);
                    output.from = TableRef::subquery(
                        Query {
                            select: vec![SelectExpr::star()],
                            from: output.from,
                            where_clause: Expr::conjoin(std::mem::take(&mut output.predicates)),
                            order_by,
                            limit_by,
                            ..Default::default()
                        },
                        alias,
                    );
                    output.predicates = outer;
                    return output;
                }
                _ => (vec![], None),
            };
            output.from = TableRef::subquery(
                Query {
                    select: vec![SelectExpr::star()],
                    from: output.from,
                    where_clause: Expr::conjoin(std::mem::take(&mut output.predicates)),
                    order_by,
                    limit_by,
                    ..Default::default()
                },
                alias,
            );
            output
        }
        PhysicalSource::Join {
            endpoints,
            predicates,
            left,
            right,
        } => {
            let mut left = emit_source(left);
            let right = emit_source(right);
            let condition = predicates.iter().map(predicate).fold(
                Expr::eq(column(&endpoints.0), column(&endpoints.1)),
                Expr::and,
            );
            left.from = TableRef::join(
                crate::ast::JoinType::Inner,
                left.from,
                right.from,
                condition,
            );
            left.predicates.extend(right.predicates);
            left
        }
    }
}

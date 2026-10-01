use crate::ast::{Cte, Expr, Query, SelectExpr, TableRef};
use crate::passes::plan::physical::{ExecutionPlan, PhysicalPlan, PhysicalSource};
use crate::passes::shared::latest_row_dedup;

use super::{EmitOutput, NodeBinding};

pub(super) fn execute(plan: &ExecutionPlan) -> EmitOutput {
    let source = emit_source(&plan.source);
    EmitOutput {
        from: source.from,
        where_parts: source.predicates,
        select: plan.outputs.clone(),
        edge_aliases: plan.edge_aliases.clone(),
        edge_if_predicates: plan.edge_if_predicates.clone(),
        ctes: plan
            .definitions
            .iter()
            .map(|(name, keys)| Cte::new(name, query(keys)))
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

pub(super) fn query(plan: &PhysicalPlan) -> Query {
    let (source, order_by, limit_by) = match &plan.source {
        PhysicalSource::Latest {
            alias,
            sort_key,
            input,
        } => {
            let (order_by, limit_by) = latest_row_dedup(alias, sort_key);
            (input.as_ref(), order_by, limit_by)
        }
        source => (source, vec![], None),
    };
    let output = emit_source(source);
    Query {
        select: plan.outputs.clone(),
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
        PhysicalSource::Filter { predicate, input } => {
            let mut output = emit_source(input);
            output.predicates.push(predicate.clone());
            output
        }
        PhysicalSource::KeyFilter { value, keys, input } => {
            let mut output = emit_source(input);
            output.predicates.push(Expr::InSelect {
                expr: Box::new(value.clone()),
                query: Box::new(query(keys)),
            });
            output
        }
        PhysicalSource::Scope { alias, input } | PhysicalSource::Latest { alias, input, .. } => {
            let mut output = emit_source(input);
            let (order_by, limit_by) = match plan {
                PhysicalSource::Latest { sort_key, .. } => latest_row_dedup(alias, sort_key),
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
            kind,
            condition,
            left,
            right,
        } => {
            let mut left = emit_source(left);
            let right = emit_source(right);
            left.from = TableRef::join(*kind, left.from, right.from, condition.clone());
            left.predicates.extend(right.predicates);
            left
        }
    }
}

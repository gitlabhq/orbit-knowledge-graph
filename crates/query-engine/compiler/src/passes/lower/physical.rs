use std::collections::HashMap;

use crate::ast::{Expr, Query, SelectExpr, TableRef};
use crate::error::Result;
use crate::passes::plan::physical::{PhysicalPlan, PhysicalSource};
use crate::passes::shared::latest_row_dedup;

use super::{EmitOutput, NodeBinding};

pub(super) fn emit(plan: &PhysicalPlan) -> Result<EmitOutput> {
    let mut output = emit_source(&plan.source);
    output.select = plan.outputs.clone();
    Ok(output)
}

pub(super) fn query(plan: &PhysicalPlan) -> Query {
    if let PhysicalSource::Latest {
        alias,
        sort_key,
        input,
    } = &plan.source
    {
        let output = emit_source(input);
        let (order_by, limit_by) = latest_row_dedup(alias, sort_key);
        return Query {
            select: plan.outputs.clone(),
            from: output.from,
            where_clause: Expr::conjoin(output.where_parts),
            order_by,
            limit_by,
            ..Default::default()
        };
    }
    let output = emit_source(&plan.source);
    Query {
        select: plan.outputs.clone(),
        from: output.from,
        where_clause: Expr::conjoin(output.where_parts),
        ..Default::default()
    }
}

pub(super) fn emit_source(plan: &PhysicalSource) -> EmitOutput {
    match plan {
        PhysicalSource::Union {
            alias,
            arms,
            relationship,
        } => {
            let queries = arms.iter().map(query).collect();
            EmitOutput {
                from: TableRef::union_all(queries, alias).with_relationship(*relationship),
                nodes: HashMap::new(),
                edge_aliases: vec![],
                where_parts: vec![],
                select: vec![],
                ctes: vec![],
                edge_if_predicates: None,
            }
        }
        PhysicalSource::Scan {
            table,
            alias,
            final_,
        } => EmitOutput {
            from: if *final_ {
                TableRef::scan_final(table, alias)
            } else {
                TableRef::scan(table, alias)
            },
            nodes: HashMap::from([(alias.clone(), NodeBinding::table(alias))]),
            edge_aliases: vec![],
            where_parts: vec![],
            select: vec![],
            ctes: vec![],
            edge_if_predicates: None,
        },
        PhysicalSource::Filter { predicate, input } => {
            let mut output = emit_source(input);
            output.where_parts.push(predicate.clone());
            output
        }
        PhysicalSource::Scope { alias, input } => {
            let mut output = emit_source(input);
            output.from = TableRef::subquery(
                Query {
                    select: vec![SelectExpr::star()],
                    from: output.from,
                    where_clause: Expr::conjoin(std::mem::take(&mut output.where_parts)),
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
            left.where_parts.extend(right.where_parts);
            left.nodes.extend(right.nodes);
            left
        }
        PhysicalSource::Latest {
            sort_key,
            alias,
            input,
        } => {
            let mut output = emit_source(input);
            let (order_by, limit_by) = latest_row_dedup(alias, sort_key);
            output.from = TableRef::subquery(
                Query {
                    select: vec![SelectExpr::star()],
                    from: output.from,
                    where_clause: Expr::conjoin(std::mem::take(&mut output.where_parts)),
                    order_by,
                    limit_by,
                    ..Default::default()
                },
                alias,
            );
            output
        }
    }
}

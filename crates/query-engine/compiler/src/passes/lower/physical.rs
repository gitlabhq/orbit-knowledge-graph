use std::collections::HashMap;

use crate::ast::TableRef;
use crate::error::{QueryError, Result};
use crate::passes::plan::physical::PhysicalPlan;

use super::{EmitOutput, NodeBinding};

pub(super) fn emit(plan: &PhysicalPlan) -> Result<EmitOutput> {
    match plan {
        PhysicalPlan::Scan {
            table,
            alias,
            final_,
        } => Ok(EmitOutput {
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
        }),
        PhysicalPlan::Filter { predicate, input } => {
            let mut output = emit(input)?;
            if !output.select.is_empty() {
                return Err(QueryError::Lowering(
                    "filter above a projection needs a query scope".into(),
                ));
            }
            output.where_parts.push(predicate.clone());
            Ok(output)
        }
        PhysicalPlan::Project { columns, input } => {
            let mut output = emit(input)?;
            if !output.select.is_empty() {
                return Err(QueryError::Lowering(
                    "nested projection needs a query scope".into(),
                ));
            }
            output.select = columns.clone();
            Ok(output)
        }
    }
}

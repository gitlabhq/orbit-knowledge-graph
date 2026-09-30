use std::collections::HashMap;

use crate::ast::TableRef;
use crate::error::Result;
use crate::passes::plan::physical::{PhysicalPlan, PhysicalSource};

use super::{EmitOutput, NodeBinding};

pub(super) fn emit(plan: &PhysicalPlan) -> Result<EmitOutput> {
    let mut output = emit_source(&plan.source);
    output.select = plan.outputs.clone();
    Ok(output)
}

fn emit_source(plan: &PhysicalSource) -> EmitOutput {
    match plan {
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
    }
}

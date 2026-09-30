use crate::ast::{Expr, SelectExpr};
use crate::error::{QueryError, Result};
use crate::passes::shared::{latest_node_predicates, node_select_columns};

use super::NodePlan;

pub struct PhysicalPlan {
    pub source: PhysicalSource,
    pub outputs: Vec<SelectExpr>,
}

pub enum PhysicalSource {
    Scan {
        table: String,
        alias: String,
        final_: bool,
    },
    Filter {
        predicate: Expr,
        input: Box<Self>,
    },
}

impl PhysicalPlan {
    pub fn single_node(node: &NodePlan) -> Result<Self> {
        let table = node
            .table
            .clone()
            .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", node.alias)))?;
        Ok(Self {
            outputs: node_select_columns(&node.alias, node),
            source: PhysicalSource::Filter {
                predicate: Expr::conjoin(latest_node_predicates(&node.alias, node))
                    .expect("current-row scan has a deletion predicate"),
                input: Box::new(PhysicalSource::Scan {
                    table,
                    alias: node.alias.clone(),
                    final_: true,
                }),
            },
        })
    }
}

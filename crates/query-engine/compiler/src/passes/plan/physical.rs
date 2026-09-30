use crate::ast::{Expr, SelectExpr};
use crate::error::{QueryError, Result};
use crate::passes::shared::{latest_node_predicates, node_select_columns};

use super::NodePlan;

pub enum PhysicalPlan {
    Scan {
        table: String,
        alias: String,
        final_: bool,
    },
    Filter {
        predicate: Expr,
        input: Box<Self>,
    },
    Project {
        columns: Vec<SelectExpr>,
        input: Box<Self>,
    },
}

impl PhysicalPlan {
    pub fn single_node(node: &NodePlan) -> Result<Self> {
        let table = node
            .table
            .clone()
            .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", node.alias)))?;
        Ok(Self::Project {
            columns: node_select_columns(&node.alias, node),
            input: Box::new(Self::Filter {
                predicate: Expr::conjoin(latest_node_predicates(&node.alias, node))
                    .expect("current-row scan has a deletion predicate"),
                input: Box::new(Self::Scan {
                    table,
                    alias: node.alias.clone(),
                    final_: true,
                }),
            }),
        })
    }
}

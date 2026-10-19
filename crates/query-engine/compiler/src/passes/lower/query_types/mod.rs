pub mod aggregation;
pub mod hydration;
pub mod neighbors;
pub mod pathfinding;
pub mod traversal;

use std::collections::HashMap;

use super::NodeBinding;
use crate::ast::{Node, OrderExpr};

pub(crate) struct Emitted {
    pub ast: Node,
    pub nodes: HashMap<String, NodeBinding>,
    pub stable_order: Vec<OrderExpr>,
}

impl Emitted {
    pub fn new(ast: Node) -> Self {
        Self {
            ast,
            nodes: HashMap::new(),
            stable_order: Vec::new(),
        }
    }
}

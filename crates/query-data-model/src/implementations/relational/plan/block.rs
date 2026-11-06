use super::{Operation, RelationalPlan};
use crate::plan::tracking::NodeId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub select: Option<NodeId>,
    pub limit: Option<NodeId>,
    pub limit_by: Option<NodeId>,
    pub order: Option<NodeId>,
    pub compute: Option<NodeId>,
    pub havings: Vec<NodeId>,
    pub aggregate: Option<NodeId>,
    pub wheres: Vec<NodeId>,
    pub base: NodeId,
}

impl<B> RelationalPlan<B> {
    pub fn block(&self, top: NodeId) -> Block {
        let mut node = top;
        let select = self.peel(&mut node, |op| matches!(op, Operation::Select(_)));
        let limit = self.peel(&mut node, |op| matches!(op, Operation::Limit { .. }));
        let limit_by = self.peel(&mut node, |op| {
            matches!(op, Operation::LimitBy { .. } | Operation::Latest { .. })
        });
        let order = self.peel(&mut node, |op| matches!(op, Operation::Order(_)));
        let mut block = Block {
            select,
            limit,
            limit_by,
            order,
            compute: None,
            havings: Vec::new(),
            aggregate: None,
            wheres: Vec::new(),
            base: node,
        };
        if select.is_none() {
            return block;
        }
        block.compute = self.peel(&mut node, |op| matches!(op, Operation::Compute(_)));
        let filtered = self.filters(&mut node);
        match self.peel(&mut node, |op| matches!(op, Operation::Aggregate { .. })) {
            Some(aggregate) => {
                block.aggregate = Some(aggregate);
                block.havings = filtered;
                block.wheres = self.filters(&mut node);
            }
            None => block.wheres = filtered,
        }
        block.base = node;
        block
    }

    fn peel(&self, node: &mut NodeId, test: fn(&Operation) -> bool) -> Option<NodeId> {
        let current = *node;
        test(self.plan.operation(current)).then(|| {
            *node = self.plan.inputs(current)[0];
            current
        })
    }

    fn filters(&self, node: &mut NodeId) -> Vec<NodeId> {
        let mut filters = Vec::new();
        while let Some(filter) = self.peel(node, |op| matches!(op, Operation::Filter(_))) {
            filters.insert(0, filter);
        }
        filters
    }
}

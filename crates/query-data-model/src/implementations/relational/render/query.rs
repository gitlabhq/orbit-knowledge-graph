use super::{Dialect, Renderer};
use crate::implementations::relational::plan::{Operation, Sharing};
use crate::plan::tracking::{BodyId, NodeId};

#[derive(Default)]
pub(super) struct Block {
    select: Option<NodeId>,
    compute: Option<NodeId>,
    wheres: Vec<NodeId>,
    aggregate: Option<NodeId>,
    havings: Vec<NodeId>,
    pub(super) order: Option<NodeId>,
    pub(super) limit_by: Option<NodeId>,
    pub(super) limit: Option<NodeId>,
}

impl<D: Dialect> Renderer<'_, D> {
    pub(super) fn statement(&mut self, with: &[BodyId], root: NodeId) -> String {
        if with.is_empty() {
            return self.query(root);
        }
        let shared: Vec<_> = with
            .iter()
            .map(|body| {
                let (name, root) = self.plan.plan.body(*body);
                let inner = self.plan.body(*body).with.clone();
                let sql = self.statement(&inner, root);
                let materialized = matches!(
                    self.plan.body(*body).sharing,
                    Sharing::Shared { materialized: true }
                );
                self.dialect.shared(name, materialized, &sql)
            })
            .collect();
        format!("WITH {} {}", shared.join(", "), self.query(root))
    }

    pub(super) fn subquery(&mut self, body: BodyId) -> String {
        let (_, root) = self.plan.plan.body(body);
        let with = self.plan.body(body).with.clone();
        self.statement(&with, root)
    }

    fn query(&mut self, top: NodeId) -> String {
        let (block, base) = self.block(top);
        let mut parts = Vec::new();
        match block.select {
            Some(select) => {
                parts.push(self.select_list(select, block.compute));
                let from = self.from(base);
                parts.push(format!("FROM {from}"));
                self.conjunction("WHERE", &block.wheres, &mut parts);
                if let Some(Operation::Aggregate { groups, .. }) =
                    block.aggregate.map(|node| self.plan.plan.operation(node))
                    && !groups.is_empty()
                {
                    let groups = self.list(groups);
                    parts.push(format!("GROUP BY {groups}"));
                }
                self.conjunction("HAVING", &block.havings, &mut parts);
            }
            None => {
                let arms: Vec<_> = self.plan.plan.inputs(base).to_vec();
                let arms: Vec<_> = arms.into_iter().map(|arm| self.query(arm)).collect();
                parts.push(arms.join(" UNION ALL "));
                let tail = block.order.or(block.limit_by).or(block.limit);
                if D::UNION_TAIL_WRAPS && tail.is_some() {
                    let body = std::mem::take(&mut parts).join(" ");
                    parts.push(format!("SELECT * FROM ({body})"));
                }
            }
        }
        self.tail(&block, &mut parts);
        parts.join(" ")
    }

    fn block(&self, top: NodeId) -> (Block, NodeId) {
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
            ..Block::default()
        };
        if select.is_none() {
            return (block, node);
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
        (block, node)
    }

    fn peel(&self, node: &mut NodeId, test: fn(&Operation) -> bool) -> Option<NodeId> {
        let current = *node;
        test(self.plan.plan.operation(current)).then(|| {
            *node = self.plan.plan.inputs(current)[0];
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

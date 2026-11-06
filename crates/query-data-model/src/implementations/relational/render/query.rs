use super::{Dialect, Renderer};
use crate::implementations::relational::plan::{Operation, Sharing};
use crate::plan::tracking::{BodyId, NodeId};

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
        let block = self.plan.block(top);
        let base = block.base;
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
}

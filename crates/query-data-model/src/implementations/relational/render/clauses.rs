use super::{Dialect, Renderer};
use crate::implementations::relational::plan::Block;
use crate::implementations::relational::plan::{JoinKind, Operation, Selection, Sharing};
use crate::plan::tracking::NodeId;

impl<D: Dialect> Renderer<'_, D> {
    pub(super) fn select_list(&mut self, select: NodeId, compute: Option<NodeId>) -> String {
        let plan = self.plan;
        let Operation::Select(Selection::Columns(columns)) = plan.plan.operation(select) else {
            return "SELECT *".to_string();
        };
        let items: Vec<_> = columns
            .iter()
            .map(|column| match self.owners[column] {
                (owner, position) if Some(owner) == compute => {
                    let value = self.computed(owner, position);
                    let value = self.expr(value);
                    match plan.plan.name(*column) {
                        Some(name) => format!("{value} AS {name}"),
                        None => value,
                    }
                }
                _ => self.column(*column),
            })
            .collect();
        format!("SELECT {}", items.join(", "))
    }

    pub(super) fn conjunction(
        &mut self,
        keyword: &str,
        filters: &[NodeId],
        parts: &mut Vec<String>,
    ) {
        let plan = self.plan;
        let rendered = filters
            .iter()
            .filter_map(|filter| match plan.plan.operation(*filter) {
                Operation::Filter(predicate) => Some(self.expr(predicate)),
                _ => None,
            });
        let combined = rendered.reduce(|left, right| format!("({left} AND {right})"));
        if let Some(predicate) = combined {
            parts.push(format!("{keyword} {predicate}"));
        }
    }

    pub(super) fn tail(&mut self, block: &Block, parts: &mut Vec<String>) {
        let plan = self.plan;
        if let Some(Operation::Order(orders)) = block.order.map(|node| plan.plan.operation(node)) {
            let orders: Vec<_> = orders
                .iter()
                .map(|order| self.direction(&order.value, order.descending))
                .collect();
            parts.push(format!("ORDER BY {}", orders.join(", ")));
        }
        match block.limit_by.map(|node| plan.plan.operation(node)) {
            Some(Operation::LimitBy { count, by }) => {
                let by = self.list(by);
                parts.push(format!("LIMIT {count} BY {by}"));
            }
            Some(Operation::Latest { key, version }) => {
                let mut orders: Vec<_> = key.iter().map(|key| self.column(*key) + " ASC").collect();
                orders.push(self.column(*version) + " DESC");
                parts.push(format!("ORDER BY {}", orders.join(", ")));
                let key: Vec<_> = key.iter().map(|key| self.column(*key)).collect();
                parts.push(format!("LIMIT 1 BY {}", key.join(", ")));
            }
            _ => {}
        }
        if let Some(Operation::Limit { count, offset }) =
            block.limit.map(|node| plan.plan.operation(node))
        {
            parts.extend(count.map(|count| format!("LIMIT {count}")));
            parts.extend(offset.map(|offset| format!("OFFSET {offset}")));
        }
    }

    pub(super) fn from(&mut self, node: NodeId) -> String {
        let plan = self.plan;
        match plan.plan.operation(node) {
            Operation::Scan { table, read, alias } => {
                let table = self.table(table.store(), table.index());
                self.dialect.scan(table, alias, *read)
            }
            Operation::Read { body, alias } => match (plan.body(*body).sharing, alias) {
                (Sharing::Inline, alias) => {
                    let alias = alias.as_deref().unwrap_or_default();
                    format!("({}) AS {alias}", self.subquery(*body))
                }
                (Sharing::Shared { .. }, Some(alias)) => {
                    format!("{} AS {alias}", plan.plan.body(*body).0)
                }
                (Sharing::Shared { .. }, None) => plan.plan.body(*body).0.to_string(),
            },
            Operation::Join(kind) => {
                let inputs = plan.plan.inputs(node);
                let left = self.from(inputs[0]);
                let right = self.from(inputs[1]);
                match kind {
                    JoinKind::Inner(on) => {
                        format!("{left} INNER JOIN {right} ON {}", self.expr(on))
                    }
                    JoinKind::Left(on) => format!("{left} LEFT JOIN {right} ON {}", self.expr(on)),
                    JoinKind::Cross => self.dialect.cross_join(&left, &right),
                }
            }
            operation => unreachable!("{operation:?} is not a relation source"),
        }
    }
}

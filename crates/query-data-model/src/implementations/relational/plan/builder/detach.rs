use super::RelationalBuilder;
use crate::implementations::relational::RelationalBackend;
use crate::implementations::relational::plan::{Operation, Relation, RelationalError, Selection};
use crate::plan::tracking::{Declare, NodeId, Ref, Rejected};

#[derive(Debug)]
pub struct Detached {
    pub operation: Operation,
    pub inputs: Vec<Relation>,
    outputs: Vec<Ref>,
}

impl<B: RelationalBackend> RelationalBuilder<'_, B> {
    pub fn original(&self) -> Option<NodeId> {
        self.construction.visiting()
    }

    pub fn detach(&mut self, rows: Relation) -> Result<Detached, RelationalError> {
        let outputs = self.construction.outputs(&rows.rows)?;
        let (operation, inputs) = self.construction.detach(rows.rows)?;
        let inputs = inputs
            .into_iter()
            .map(|rows| {
                let view = self.views.get(&rows.node()).cloned();
                let view = view.ok_or(RelationalError::Rejected(Rejected::Failed))?;
                Ok(view.relation(rows))
            })
            .collect::<Result<_, RelationalError>>();
        Ok(Detached {
            operation,
            inputs: self.guard(inputs)?,
            outputs,
        })
    }

    pub fn reattach(&mut self, detached: Detached) -> Result<Relation, RelationalError> {
        let Detached {
            operation,
            mut inputs,
            outputs,
        } = detached;
        let rows = match operation {
            Operation::Filter(predicate) => self.filter(first(&mut inputs)?, predicate)?,
            Operation::Order(orders) => self.order(first(&mut inputs)?, orders)?,
            Operation::Limit { count, offset } => self.limit(first(&mut inputs)?, count, offset)?,
            Operation::LimitBy { count, by } => self.limit_by(first(&mut inputs)?, count, by)?,
            Operation::Latest { key, version } => {
                let key = key.into_iter().map(|output| self.column(output));
                let key = key.collect::<Result<_, _>>()?;
                let version = self.column(version)?;
                self.latest(first(&mut inputs)?, key, version)?
            }
            Operation::Aggregate { groups, measures } => {
                self.aggregate(first(&mut inputs)?, groups, measures)?.rows
            }
            Operation::Compute(values) => {
                let fresh = &outputs[outputs.len() - values.len()..];
                let mut items = Vec::new();
                for (output, value) in fresh.iter().zip(values) {
                    let (name, _) = self.construction.value(*output)?;
                    items.push(Declare {
                        name: name.map(str::to_string),
                        value,
                    });
                }
                self.compute(first(&mut inputs)?, items)?.0
            }
            Operation::Select(Selection::All) => self.select_all(first(&mut inputs)?)?,
            Operation::Select(Selection::Columns(columns)) => {
                let columns = columns.into_iter().map(|output| self.column(output));
                let columns = columns.collect::<Result<_, _>>()?;
                self.select(first(&mut inputs)?, columns)?
            }
            Operation::Join(kind) => {
                let left = first(&mut inputs)?;
                self.join(left, first(&mut inputs)?, kind)?
            }
            Operation::Union => self.union(inputs)?,
            Operation::Scan { .. } | Operation::Read { .. } => {
                return self.guard(Err(RelationalError::Rejected(Rejected::NotDetachable)));
            }
        };
        let fresh = self.construction.outputs(&rows.rows)?;
        for (old, new) in outputs.into_iter().zip(fresh) {
            if old != new {
                self.reconnect(old, new)?;
            }
        }
        Ok(rows)
    }
}

fn first(inputs: &mut Vec<Relation>) -> Result<Relation, RelationalError> {
    match inputs.is_empty() {
        true => Err(RelationalError::Rejected(Rejected::NotDetachable)),
        false => Ok(inputs.remove(0)),
    }
}

use super::{RelationalBuilder, complete, supported};
use crate::implementations::relational::plan::stage::Stage;
use crate::implementations::relational::plan::{
    Column, Expr, Operation, Order, Relation, RelationalError, Selection,
};
use crate::implementations::relational::{RelationalBackend, RelationalFacts};
use crate::plan::tracking::{Declare, Output, Outputs};

impl<B: RelationalBackend> RelationalBuilder<'_, B> {
    pub fn order(
        &mut self,
        rows: Relation,
        orders: Vec<Order>,
    ) -> Result<Relation, RelationalError> {
        let stage = self.guard(rows.stage.order())?;
        for order in &orders {
            self.check(&order.value)?;
        }
        self.pass(Operation::Order(orders), rows, stage)
    }

    pub fn latest(
        &mut self,
        rows: Relation,
        key: Vec<Column>,
        version: Column,
    ) -> Result<Relation, RelationalError> {
        let stage = self.guard(rows.stage.latest())?;
        let (name, _) = self.construction.value(version.output)?;
        let name = name.unwrap_or_default().to_string();
        let versioned = self.catalog.stores().any(|(_, facts)| {
            facts
                .relational()
                .versioning
                .as_ref()
                .is_some_and(|versioning| versioning.version == name)
        });
        self.guard(match versioned {
            true => Ok(()),
            false => Err(RelationalError::NotVersion(name)),
        })?;
        let operation = Operation::Latest {
            key: key.into_iter().map(Column::output).collect(),
            version: version.output,
        };
        self.pass(operation, rows, stage)
    }

    pub fn limit_by(
        &mut self,
        rows: Relation,
        count: u32,
        by: Vec<Expr>,
    ) -> Result<Relation, RelationalError> {
        self.guard(supported(B::CAPABILITIES.limit_by, "limit by"))?;
        let stage = self.guard(rows.stage.limit_by())?;
        for value in &by {
            self.check(value)?;
        }
        self.pass(Operation::LimitBy { count, by }, rows, stage)
    }

    pub fn limit(
        &mut self,
        rows: Relation,
        count: Option<u32>,
        offset: Option<u32>,
    ) -> Result<Relation, RelationalError> {
        let stage = self.guard(rows.stage.limit())?;
        self.pass(Operation::Limit { count, offset }, rows, stage)
    }

    pub fn select(
        &mut self,
        rows: Relation,
        columns: Vec<Column>,
    ) -> Result<Relation, RelationalError> {
        let stage = self.guard(rows.stage.select())?;
        let dropped = rows
            .computed
            .iter()
            .find(|(output, _)| !columns.iter().any(|column| column.output == *output));
        if let Some((_, name)) = dropped {
            let name = name.clone().unwrap_or_default();
            return self.guard(Err(RelationalError::Unselected(name)));
        }
        let refs: Vec<_> = columns.iter().map(|column| column.output).collect();
        let outputs = Outputs::replace(refs.iter().copied().map(Output::Keep));
        let operation = Operation::Select(Selection::Columns(refs.clone()));
        let (mut relation, _) = self.connect(operation, vec![rows], stage, outputs)?;
        relation.visible = self.named(&refs, None)?;
        Ok(relation)
    }

    pub fn select_all(&mut self, rows: Relation) -> Result<Relation, RelationalError> {
        let stage = self.guard(rows.stage.select())?;
        let operation = Operation::Select(Selection::All);
        let (relation, _) = self.connect(operation, vec![rows], stage, Outputs::preserve())?;
        Ok(relation)
    }

    pub fn union(&mut self, arms: Vec<Relation>) -> Result<Relation, RelationalError> {
        let arms = self.guard(
            arms.into_iter()
                .map(|arm| complete(arm, "a union arm"))
                .collect::<Result<Vec<_>, _>>(),
        )?;
        let mut widths = Vec::new();
        for arm in &arms {
            widths.push(self.construction.outputs(&arm.rows)?);
        }
        let first = widths.first().cloned().unwrap_or_default();
        if let Some(other) = widths.iter().find(|outputs| outputs.len() != first.len()) {
            return self.guard(Err(RelationalError::Width {
                expected: first.len(),
                found: other.len(),
            }));
        }
        let mut declared = Vec::new();
        for output in &first {
            let (name, value) = self.construction.value(*output)?;
            declared.push(Declare {
                name: name.map(str::to_string),
                value: *value,
            });
        }
        let outputs = Outputs::replace(declared.into_iter().map(Output::Declare));
        let (mut relation, fresh) = self.connect(Operation::Union, arms, Stage::UNITED, outputs)?;
        relation.visible = self.named(&fresh, None)?;
        Ok(relation)
    }
}

use super::{RelationalBuilder, complete};
use crate::implementations::relational::RelationalBackend;
use crate::implementations::relational::plan::stage::Stage;
use crate::implementations::relational::plan::{
    Column, Expr, Operation, Order, Relation, RelationalError, Selection,
};
use crate::plan::tracking::{Declare, Output, Outputs, Ref};

impl<B: RelationalBackend> RelationalBuilder<'_, B> {
    pub fn order(
        &mut self,
        rows: Relation,
        orders: Vec<Order>,
    ) -> Result<Relation, RelationalError> {
        self.apply(Operation::Order(orders), vec![rows], Outputs::preserve())
    }

    pub fn latest(
        &mut self,
        rows: Relation,
        key: Vec<Column>,
        version: Column,
    ) -> Result<Relation, RelationalError> {
        let key = key.into_iter().map(Column::output).collect();
        let operation = Operation::Latest {
            key,
            version: version.output,
        };
        self.apply(operation, vec![rows], Outputs::preserve())
    }

    pub fn limit_by(
        &mut self,
        rows: Relation,
        count: u32,
        by: Vec<Expr>,
    ) -> Result<Relation, RelationalError> {
        self.apply(
            Operation::LimitBy { count, by },
            vec![rows],
            Outputs::preserve(),
        )
    }

    pub fn limit(
        &mut self,
        rows: Relation,
        count: Option<u32>,
        offset: Option<u32>,
    ) -> Result<Relation, RelationalError> {
        self.apply(
            Operation::Limit { count, offset },
            vec![rows],
            Outputs::preserve(),
        )
    }

    pub fn select(
        &mut self,
        rows: Relation,
        columns: Vec<Column>,
    ) -> Result<Relation, RelationalError> {
        let columns = columns.into_iter().map(Column::output).collect();
        let operation = Operation::Select(Selection::Columns(columns));
        self.apply(operation, vec![rows], Outputs::preserve())
    }

    pub fn select_all(&mut self, rows: Relation) -> Result<Relation, RelationalError> {
        let operation = Operation::Select(Selection::All);
        self.apply(operation, vec![rows], Outputs::preserve())
    }

    pub fn union(&mut self, arms: Vec<Relation>) -> Result<Relation, RelationalError> {
        self.apply(Operation::Union, arms, Outputs::preserve())
    }

    pub(super) fn selected(
        &mut self,
        rows: Relation,
        refs: Vec<Ref>,
    ) -> Result<Relation, RelationalError> {
        let stage = self.guard(rows.stage.select())?;
        let dropped = rows
            .computed
            .iter()
            .find(|(output, _)| !refs.contains(output));
        if let Some((_, name)) = dropped {
            let name = name.clone().unwrap_or_default();
            return self.guard(Err(RelationalError::Unselected(name)));
        }
        let outputs = Outputs::replace(refs.iter().copied().map(Output::Keep));
        let operation = Operation::Select(Selection::Columns(refs.clone()));
        let (mut relation, _) = self.connect(operation, vec![rows], stage, outputs)?;
        relation.visible = self.named(&refs, None)?;
        Ok(relation)
    }

    pub(super) fn united(&mut self, arms: Vec<Relation>) -> Result<Relation, RelationalError> {
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

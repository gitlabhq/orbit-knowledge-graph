use super::RelationalBuilder;
use crate::DataType;
use crate::implementations::relational::RelationalBackend;
use crate::implementations::relational::plan::stage::Stage;
use crate::implementations::relational::plan::{
    Column, Expr, Measure, Operation, Relation, RelationalError,
};
use crate::plan::tracking::{Declare, Output, Outputs};

pub struct Aggregated {
    pub rows: Relation,
    pub groups: Vec<Column>,
    pub measures: Vec<Column>,
}

impl<B: RelationalBackend> RelationalBuilder<'_, B> {
    pub fn filter(&mut self, rows: Relation, predicate: Expr) -> Result<Relation, RelationalError> {
        self.apply(
            Operation::Filter(predicate),
            vec![rows],
            Outputs::preserve(),
        )
    }

    pub fn aggregate(
        &mut self,
        rows: Relation,
        groups: Vec<Expr>,
        measures: Vec<Measure>,
    ) -> Result<Aggregated, RelationalError> {
        let width = groups.len();
        let operation = Operation::Aggregate { groups, measures };
        let rows = self.apply(operation, vec![rows], Outputs::preserve())?;
        let mut columns = self.columns(&rows)?;
        let measures = columns.split_off(width);
        Ok(Aggregated {
            rows,
            groups: columns,
            measures,
        })
    }

    pub fn compute(
        &mut self,
        rows: Relation,
        items: Vec<Declare<Expr>>,
    ) -> Result<(Relation, Vec<Column>), RelationalError> {
        let (names, values): (Vec<_>, Vec<_>) = items
            .into_iter()
            .map(|item| {
                (
                    Declare::<Option<DataType>> {
                        name: item.name,
                        value: None,
                    },
                    item.value,
                )
            })
            .unzip();
        let width = values.len();
        let rows = self.apply(
            Operation::Compute(values),
            vec![rows],
            Outputs::extend(names),
        )?;
        let mut columns = self.columns(&rows)?;
        let computed = columns.split_off(columns.len() - width);
        Ok((rows, computed))
    }

    pub(super) fn pass(
        &mut self,
        operation: Operation,
        rows: Relation,
        stage: Stage,
    ) -> Result<Relation, RelationalError> {
        let computed = rows.computed.clone();
        let (mut relation, _) = self.connect(operation, vec![rows], stage, Outputs::preserve())?;
        relation.computed = computed;
        Ok(relation)
    }

    fn columns(&self, rows: &Relation) -> Result<Vec<Column>, RelationalError> {
        let outputs = self.construction.outputs(&rows.rows)?;
        outputs
            .into_iter()
            .map(|output| self.column(output))
            .collect()
    }

    pub(super) fn aggregated(
        &mut self,
        rows: Relation,
        groups: Vec<Expr>,
        measures: Vec<Measure>,
    ) -> Result<Relation, RelationalError> {
        let stage = self.guard(rows.stage.aggregate())?;
        for group in &groups {
            self.check(group)?;
        }
        for measure in &measures {
            self.check_measure(measure)?;
        }
        let declared = || {
            Output::Declare(Declare {
                name: None,
                value: None,
            })
        };
        let outputs = groups
            .iter()
            .map(|group| match group {
                Expr::Column(output) => Output::Keep(*output),
                _ => declared(),
            })
            .chain(measures.iter().map(|_| declared()))
            .collect::<Vec<_>>();
        let width = groups.len();
        let operation = Operation::Aggregate { groups, measures };
        let (mut relation, _) =
            self.connect(operation, vec![rows], stage, Outputs::replace(outputs))?;
        let mut kept = self.columns(&relation)?;
        kept.truncate(width);
        relation
            .visible
            .retain(|visible| kept.contains(&visible.column));
        Ok(relation)
    }

    pub(super) fn computed(
        &mut self,
        rows: Relation,
        values: Vec<Expr>,
        outputs: Outputs<Option<DataType>>,
    ) -> Result<Relation, RelationalError> {
        let stage = self.guard(rows.stage.compute())?;
        let names = match outputs {
            Outputs::Preserve(names) if names.len() == values.len() => names,
            _ => return self.guard(Err(RelationalError::Incomplete("computed names"))),
        };
        let mut declared = Vec::new();
        for (name, value) in names.into_iter().zip(&values) {
            self.check(value)?;
            let data_type = match value {
                Expr::Column(output) => self.column(*output)?.data_type,
                _ => None,
            };
            declared.push(Declare {
                name: name.name,
                value: data_type,
            });
        }
        let (mut relation, outputs) = self.connect(
            Operation::Compute(values),
            vec![rows],
            stage,
            Outputs::extend(declared),
        )?;
        for output in &outputs {
            let (name, _) = self.construction.value(*output)?;
            relation.computed.push((*output, name.map(str::to_string)));
        }
        relation.visible.extend(self.named(&outputs, None)?);
        Ok(relation)
    }
}

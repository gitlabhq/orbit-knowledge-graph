use super::RelationalBuilder;
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
        let stage = self.guard(rows.stage.filter())?;
        self.check(&predicate)?;
        self.pass(Operation::Filter(predicate), rows, stage)
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

    pub fn aggregate(
        &mut self,
        rows: Relation,
        groups: Vec<Expr>,
        measures: Vec<Measure>,
    ) -> Result<Aggregated, RelationalError> {
        let stage = self.guard(rows.stage.aggregate())?;
        for group in &groups {
            self.check(group)?;
        }
        for measure in &measures {
            self.check_measure(measure)?;
        }
        let outputs = groups
            .iter()
            .map(|group| match group {
                Expr::Column(output) => Output::Keep(*output),
                _ => Output::Declare(Declare {
                    name: None,
                    value: None,
                }),
            })
            .chain(measures.iter().map(|_| {
                Output::Declare(Declare {
                    name: None,
                    value: None,
                })
            }))
            .collect::<Vec<_>>();
        let operation = Operation::Aggregate {
            groups: groups.clone(),
            measures,
        };
        let (mut relation, declared) =
            self.connect(operation, vec![rows], stage, Outputs::replace(outputs))?;
        let mut fresh = declared.into_iter();
        let mut columns = Vec::new();
        for group in &groups {
            let output = match group {
                Expr::Column(output) => *output,
                _ => fresh
                    .next()
                    .expect("one declared output per computed group"),
            };
            columns.push(self.column(output)?);
        }
        relation
            .visible
            .retain(|visible| columns.contains(&visible.column));
        let measures = fresh
            .map(|output| self.column(output))
            .collect::<Result<_, _>>()?;
        Ok(Aggregated {
            rows: relation,
            groups: columns,
            measures,
        })
    }

    pub fn compute(
        &mut self,
        rows: Relation,
        items: Vec<Declare<Expr>>,
    ) -> Result<(Relation, Vec<Column>), RelationalError> {
        let stage = self.guard(rows.stage.compute())?;
        let mut declared = Vec::new();
        for item in &items {
            self.check(&item.value)?;
            let data_type = match &item.value {
                Expr::Column(output) => self.column(*output)?.data_type,
                _ => None,
            };
            declared.push(Declare {
                name: item.name.clone(),
                value: data_type,
            });
        }
        let values = items.into_iter().map(|item| item.value).collect();
        let (mut relation, outputs) = self.connect(
            Operation::Compute(values),
            vec![rows],
            stage,
            Outputs::extend(declared),
        )?;
        let columns = outputs
            .iter()
            .map(|output| self.column(*output))
            .collect::<Result<Vec<_>, _>>()?;
        for output in &outputs {
            let (name, _) = self.construction.value(*output)?;
            relation.computed.push((*output, name.map(str::to_string)));
        }
        relation.visible.extend(self.named(&outputs, None)?);
        Ok((relation, columns))
    }
}

use super::RelationalBuilder;
use crate::implementations::relational::RelationalBackend;
use crate::implementations::relational::plan::relation::Visible;
use crate::implementations::relational::plan::stage::Stage;
use crate::implementations::relational::plan::{Operation, Relation, RelationalError, Selection};
use crate::plan::tracking::{Ref, Rows};

#[derive(Debug, Clone)]
pub(super) struct View {
    stage: Stage,
    visible: Vec<Visible>,
    computed: Vec<(Ref, Option<String>)>,
}

impl View {
    fn of(relation: &Relation) -> Self {
        Self {
            stage: relation.stage,
            visible: relation.visible.clone(),
            computed: relation.computed.clone(),
        }
    }

    pub(super) fn relation(self, rows: Rows) -> Relation {
        Relation {
            rows,
            stage: self.stage,
            visible: self.visible,
            computed: self.computed,
        }
    }

    fn from(stage: Stage, visible: Vec<Visible>) -> Self {
        Self {
            stage,
            visible,
            computed: Vec::new(),
        }
    }
}

impl<B: RelationalBackend> RelationalBuilder<'_, B> {
    pub fn operation(&self, rows: &Relation) -> Result<&Operation, RelationalError> {
        Ok(self.construction.operation(&rows.rows)?)
    }

    pub fn reconnect(&mut self, from: Ref, to: Ref) -> Result<(), RelationalError> {
        Ok(self.construction.reconnect(from, to)?)
    }

    pub(super) fn remember(&mut self, relation: &Relation) {
        self.views.insert(relation.rows.node(), View::of(relation));
    }

    pub(super) fn view(&mut self, rows: Rows) -> Result<Relation, RelationalError> {
        let view = self.derive(&rows);
        Ok(self.guard(view)?.relation(rows))
    }

    fn input(&self, rows: &Rows, index: usize) -> Result<View, RelationalError> {
        let input = self.construction.inputs(rows)?[index];
        self.views
            .get(&input)
            .cloned()
            .ok_or(RelationalError::Rejected(
                crate::plan::tracking::Rejected::Failed,
            ))
    }

    fn derive(&self, rows: &Rows) -> Result<View, RelationalError> {
        let outputs = self.construction.outputs(rows)?;
        let pass = |stage: fn(Stage) -> Result<Stage, RelationalError>| {
            let input = self.input(rows, 0)?;
            Ok::<_, RelationalError>(View {
                stage: stage(input.stage)?,
                ..input
            })
        };
        Ok(match self.construction.operation(rows)? {
            Operation::Scan { alias, .. } => {
                View::from(Stage::FROM, self.named(&outputs, Some(alias))?)
            }
            Operation::Read { body, alias } => {
                let range = alias.as_deref().unwrap_or(&self.names[body.index()]);
                View::from(Stage::FROM, self.named(&outputs, Some(range))?)
            }
            Operation::Join(_) => {
                let mut visible = self.input(rows, 0)?.visible;
                visible.extend(self.input(rows, 1)?.visible);
                View::from(Stage::FROM, visible)
            }
            Operation::Filter(_) => pass(Stage::filter)?,
            Operation::Order(_) => pass(Stage::order)?,
            Operation::Latest { .. } => pass(Stage::latest)?,
            Operation::LimitBy { .. } => pass(Stage::limit_by)?,
            Operation::Limit { .. } => pass(Stage::limit)?,
            Operation::Aggregate { groups, .. } => {
                let input = self.input(rows, 0)?;
                let groups = &outputs[..groups.len()];
                let mut visible = input.visible;
                visible.retain(|visible| groups.contains(&visible.column.output));
                View::from(input.stage.aggregate()?, visible)
            }
            Operation::Compute(values) => {
                let mut view = pass(Stage::compute)?;
                let fresh = &outputs[outputs.len() - values.len()..];
                for output in fresh {
                    let (name, _) = self.construction.value(*output)?;
                    view.computed.push((*output, name.map(str::to_string)));
                }
                view.visible.extend(self.named(fresh, None)?);
                view
            }
            Operation::Select(Selection::Columns(columns)) => {
                let stage = self.input(rows, 0)?.stage.select()?;
                View::from(stage, self.named(columns, None)?)
            }
            Operation::Select(Selection::All) => {
                let input = self.input(rows, 0)?;
                View::from(input.stage.select()?, input.visible)
            }
            Operation::Union => View::from(Stage::UNITED, self.named(&outputs, None)?),
        })
    }
}

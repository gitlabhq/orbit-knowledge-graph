mod bodies;
mod check;
mod clauses;
mod sources;
mod tail;

use ontology::DataType;

pub use bodies::Subquery;
pub use clauses::Aggregated;

use super::relation::Visible;
use super::stage::Stage;
use super::{BodyUse, Column, Operation, Relation, Relational, RelationalError, RelationalPlan};
use crate::catalog::Catalog;
use crate::implementations::relational::RelationalBackend;
use crate::plan::tracking::{BodyId, Construction, Outputs, Ref};

pub struct RelationalBuilder<'c, B: RelationalBackend> {
    catalog: &'c Catalog<B>,
    construction: Construction<Relational<B>>,
    bodies: Vec<BodyUse>,
    widths: Vec<usize>,
    frames: Vec<Vec<BodyId>>,
    settings: Vec<(String, String)>,
}

impl<'c, B: RelationalBackend> RelationalBuilder<'c, B> {
    pub(in super::super) fn new(catalog: &'c Catalog<B>) -> Self {
        Self {
            catalog,
            construction: Construction::new(catalog.model().id()),
            bodies: Vec::new(),
            widths: Vec::new(),
            frames: vec![Vec::new()],
            settings: Vec::new(),
        }
    }

    pub(in super::super) fn finish(
        mut self,
        root: Result<Relation, RelationalError>,
    ) -> Result<RelationalPlan<B>, RelationalError> {
        let root = self.guard(complete(root?, "a plan"))?;
        let with = self.frames.pop().unwrap_or_default();
        Ok(RelationalPlan {
            plan: self.construction.finish(root.rows)?,
            with,
            bodies: self.bodies,
            settings: self.settings,
        })
    }

    pub fn settings(
        &mut self,
        settings: impl IntoIterator<Item = (String, String)>,
    ) -> Result<(), RelationalError> {
        self.guard(supported(B::CAPABILITIES.settings, "query settings"))?;
        self.settings.extend(settings);
        Ok(())
    }

    fn guard<T>(&mut self, result: Result<T, RelationalError>) -> Result<T, RelationalError> {
        self.construction.attempt(|_| result)
    }

    fn connect(
        &mut self,
        operation: Operation,
        inputs: Vec<Relation>,
        stage: Stage,
        outputs: Outputs<Option<DataType>>,
    ) -> Result<(Relation, Vec<Ref>), RelationalError> {
        self.construction.attempt(|construction| {
            let mut visible = Vec::new();
            let mut rows = Vec::new();
            for input in inputs {
                visible.extend(input.visible);
                rows.push(input.rows);
            }
            let connected = construction.connect(operation, rows, outputs)?;
            let relation = Relation {
                rows: connected.rows,
                stage,
                visible,
                computed: Vec::new(),
            };
            Ok((relation, connected.declared))
        })
    }

    fn column(&self, output: Ref) -> Result<Column, RelationalError> {
        let (_, data_type) = self.construction.value(output)?;
        Ok(Column {
            output,
            data_type: *data_type,
        })
    }

    fn named(&self, outputs: &[Ref], range: Option<&str>) -> Result<Vec<Visible>, RelationalError> {
        let mut visible = Vec::new();
        for output in outputs {
            if let (Some(name), _) = self.construction.value(*output)? {
                visible.push(Visible {
                    range: range.map(str::to_string),
                    name: name.to_string(),
                    column: self.column(*output)?,
                });
            }
        }
        Ok(visible)
    }
}

fn complete(relation: Relation, purpose: &'static str) -> Result<Relation, RelationalError> {
    match relation.stage.complete() {
        true => Ok(relation),
        false => Err(RelationalError::Incomplete(purpose)),
    }
}

fn supported(capability: bool, feature: &'static str) -> Result<(), RelationalError> {
    match capability {
        true => Ok(()),
        false => Err(RelationalError::Unsupported(feature)),
    }
}

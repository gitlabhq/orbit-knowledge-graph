use super::{Calls, Condition, Field, LiveBuilder, LiveError, LiveLanguage, LiveOperation};
use super::{LivePlan, LiveValue};
use crate::DataType;
use crate::plan::tracking::{self, Construction, Declare, Output, Outputs, Rebuilder, Ref};

impl LiveBuilder<'_> {
    /// Checks `operation` on `inputs` and appends it.
    pub fn apply(
        &mut self,
        operation: LiveOperation,
        inputs: Vec<Calls>,
    ) -> Result<Calls, LiveError> {
        let outputs = self.outputs(&operation);
        let rows = self.construction.attempt(|construction| {
            let outputs = outputs?;
            let inputs = inputs.into_iter().map(Calls::into_inner).collect();
            Ok::<_, LiveError>(construction.connect(operation, inputs, outputs)?.rows)
        })?;
        self.view(rows)
    }

    fn outputs(&self, operation: &LiveOperation) -> Result<Outputs<LiveValue>, LiveError> {
        match operation {
            LiveOperation::Call(operation) => {
                let graph = self.catalog.graph();
                let store = &self.catalog.model().stores()[operation.store];
                let declared = graph
                    .entity(operation.entity)
                    .properties
                    .iter()
                    .filter(|property| store.column(**property).is_some())
                    .map(|property| {
                        let declared = graph.property(*property);
                        let value = LiveValue {
                            property: *property,
                            data_type: declared.data_type,
                        };
                        Declare::named(&declared.name, value)
                    });
                Ok(Outputs::extend(declared.collect::<Vec<_>>()))
            }
            LiveOperation::Filter(conditions) => {
                for condition in conditions {
                    match condition {
                        Condition::Equals { field, value } => {
                            let field = self.field(*field)?;
                            self.pushed(&field, false, "equality")?;
                            Self::typed(&field, value)?;
                        }
                        Condition::Contains { field, .. } => {
                            self.pushed(&self.field(*field)?, true, "contains")?
                        }
                        Condition::OneOf { field, values } => {
                            let field = self.field(*field)?;
                            self.pushed(&field, false, "a key set")?;
                            for value in values {
                                Self::typed(&field, value)?;
                            }
                        }
                    }
                }
                Ok(Outputs::preserve())
            }
            LiveOperation::Window { field, from, to } => {
                let field = self.field(*field)?;
                let timed = matches!(field.value.data_type, DataType::Date | DataType::DateTime)
                    && (from.is_some() || to.is_some());
                match timed {
                    true => self.pushed(&field, false, "a time window")?,
                    false => return Err(LiveError::Window),
                }
                Ok(Outputs::preserve())
            }
            LiveOperation::Workspaces(workspaces) if workspaces.is_empty() => {
                Err(LiveError::NoWorkspace)
            }
            LiveOperation::Limit(0) => Err(LiveError::EmptyLimit),
            LiveOperation::Workspaces(_) | LiveOperation::Limit(_) => Ok(Outputs::preserve()),
            LiveOperation::Select(refs) => {
                let mut outputs = Vec::new();
                for output in refs {
                    let (name, value) = self.construction.value(*output)?;
                    let name = name.map(str::to_string);
                    outputs.push(Output::Declare(Declare {
                        name,
                        value: *value,
                    }));
                }
                Ok(Outputs::replace(outputs))
            }
        }
    }

    fn field(&self, output: Ref) -> Result<Field, LiveError> {
        let (name, value) = self.construction.value(output)?;
        Ok(Field {
            output,
            name: name.unwrap_or_default().to_string(),
            value: *value,
        })
    }
}

impl Rebuilder for LiveBuilder<'_> {
    type Language = LiveLanguage;
    type Old = LivePlan;
    type Rows = Calls;
    type Error = LiveError;

    fn plan(old: &LivePlan) -> &LivePlan {
        old
    }

    fn construction(&mut self) -> &mut Construction<LiveLanguage> {
        &mut self.construction
    }

    fn rows(rows: &Calls) -> &tracking::Rows {
        &rows.rows
    }

    fn apply(
        &mut self,
        operation: LiveOperation,
        inputs: Vec<Calls>,
        _outputs: Outputs<LiveValue>,
    ) -> Result<Calls, LiveError> {
        LiveBuilder::apply(self, operation, inputs)
    }
}

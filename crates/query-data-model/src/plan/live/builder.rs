use super::{Condition, LiveError, LiveLanguage, LiveOperation, LivePlan, LiveValue, Operation};
use crate::DataType;
use crate::catalog::Catalog;
use crate::implementations::live::Live;
use crate::plan::logical::Literal;
use crate::plan::tracking::{self, Construction, Declare, Declared, Output, Outputs, Ref};

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    output: Ref,
    name: String,
    value: LiveValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Conditions(Vec<(Field, Condition)>);

impl Field {
    pub fn eq(&self, value: impl Into<Literal>) -> Conditions {
        let condition = Condition::Equals {
            field: self.output,
            value: value.into(),
        };
        Conditions(vec![(self.clone(), condition)])
    }

    pub fn contains(&self, text: impl Into<String>) -> Conditions {
        let condition = Condition::Contains {
            field: self.output,
            text: text.into(),
        };
        Conditions(vec![(self.clone(), condition)])
    }
}

impl Conditions {
    #[must_use]
    pub fn and(mut self, other: Conditions) -> Conditions {
        self.0.extend(other.0);
        self
    }
}

#[derive(Debug)]
pub struct Calls {
    rows: tracking::Rows,
    fields: Vec<Field>,
}

impl Calls {
    pub fn field(&self, name: &str) -> Result<Field, LiveError> {
        self.fields
            .iter()
            .find(|field| field.name == name)
            .cloned()
            .ok_or_else(|| LiveError::UnknownField {
                name: name.to_string(),
                available: self.fields.iter().map(|field| field.name.clone()).collect(),
            })
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub(super) fn into_inner(self) -> tracking::Rows {
        self.rows
    }
}

pub struct LiveBuilder<'c> {
    catalog: &'c Catalog<Live>,
    construction: Construction<LiveLanguage>,
}

impl<'c> LiveBuilder<'c> {
    pub(super) fn new(
        catalog: &'c Catalog<Live>,
        construction: Construction<LiveLanguage>,
    ) -> Self {
        Self {
            catalog,
            construction,
        }
    }

    pub(super) fn construction(&mut self) -> &mut Construction<LiveLanguage> {
        &mut self.construction
    }

    pub(super) fn finish(
        self,
        root: Result<tracking::Rows, LiveError>,
    ) -> Result<LivePlan, LiveError> {
        Ok(self.construction.finish(root?)?)
    }

    pub(super) fn view(&self, rows: tracking::Rows) -> Result<Calls, LiveError> {
        let mut fields = Vec::new();
        for output in self.construction.outputs(&rows)? {
            if let (Some(name), value) = self.construction.value(output)? {
                fields.push(Field {
                    output,
                    name: name.to_string(),
                    value: *value,
                });
            }
        }
        Ok(Calls { rows, fields })
    }

    pub fn operation(&self, calls: &Calls) -> Result<&LiveOperation, LiveError> {
        Ok(self.construction.operation(&calls.rows)?)
    }

    fn connect(
        &mut self,
        operation: LiveOperation,
        inputs: Vec<Calls>,
        check: impl FnOnce() -> Result<Outputs<LiveValue>, LiveError>,
    ) -> Result<Calls, LiveError> {
        let rows = self.construction.attempt(|construction| {
            let outputs = check()?;
            let inputs = inputs.into_iter().map(Calls::into_inner).collect();
            Ok::<_, LiveError>(construction.connect(operation, inputs, outputs)?.rows)
        })?;
        self.view(rows)
    }

    pub fn call(&mut self, operation: Declared<Operation>) -> Result<Calls, LiveError> {
        let operation = self
            .construction
            .attempt(|construction| construction.declared(operation))?;
        let graph = self.catalog.graph();
        let store = &self.catalog.model().stores()[operation.store];
        let declared: Vec<Declare<LiveValue>> = graph
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
            })
            .collect();
        self.connect(LiveOperation::Call(operation), Vec::new(), || {
            Ok(Outputs::extend(declared))
        })
    }

    fn pushed(&self, field: &Field, text: bool, condition: &'static str) -> Result<(), LiveError> {
        let graph = self.catalog.graph();
        let property = graph.property(field.value.property);
        let endpoint = self
            .catalog
            .endpoint(property.entity)
            .ok_or(LiveError::Incomplete("an operation"))?;
        let pushed = match text {
            true => endpoint.text == Some(field.value.property),
            false => endpoint.filters.contains(&field.value.property),
        };
        match pushed {
            true => Ok(()),
            false => Err(LiveError::NotPushedDown {
                operation: endpoint.operation.clone(),
                field: format!("{}.{}", graph.entity(property.entity).name, property.name),
                condition,
                filters: endpoint
                    .filters
                    .iter()
                    .map(|filter| graph.property(*filter).name.clone())
                    .chain(
                        endpoint
                            .text
                            .map(|text| format!("{} (contains)", graph.property(text).name)),
                    )
                    .collect(),
            }),
        }
    }

    fn typed(field: &Field, value: &Literal) -> Result<(), LiveError> {
        let fits = matches!(
            (field.value.data_type, value),
            (
                DataType::String | DataType::Enum | DataType::Uuid,
                Literal::String(_)
            ) | (DataType::Int, Literal::Int(_))
                | (DataType::Float, Literal::Float(_) | Literal::Int(_))
                | (DataType::Bool, Literal::Bool(_))
                | (
                    DataType::Date | DataType::DateTime,
                    Literal::Temporal(_) | Literal::String(_)
                )
        );
        match fits {
            true => Ok(()),
            false => Err(LiveError::Value {
                field: field.name.clone(),
                value: value.clone(),
                expected: format!("{:?}", field.value.data_type).to_lowercase(),
            }),
        }
    }

    pub fn filter(&mut self, calls: Calls, conditions: Conditions) -> Result<Calls, LiveError> {
        let checked: Result<(), LiveError> =
            conditions
                .0
                .iter()
                .try_for_each(|(field, condition)| match condition {
                    Condition::Equals { value, .. } => {
                        self.pushed(field, false, "equality")?;
                        Self::typed(field, value)
                    }
                    Condition::Contains { .. } => self.pushed(field, true, "contains"),
                });
        let conditions = conditions
            .0
            .into_iter()
            .map(|(_, condition)| condition)
            .collect();
        self.connect(LiveOperation::Filter(conditions), vec![calls], || {
            checked?;
            Ok(Outputs::preserve())
        })
    }

    pub fn window(
        &mut self,
        calls: Calls,
        field: &Field,
        from: Option<String>,
        to: Option<String>,
    ) -> Result<Calls, LiveError> {
        let timed = matches!(field.value.data_type, DataType::Date | DataType::DateTime)
            && (from.is_some() || to.is_some());
        let checked = match timed {
            true => self.pushed(field, false, "a time window"),
            false => Err(LiveError::Window),
        };
        let operation = LiveOperation::Window {
            field: field.output,
            from,
            to,
        };
        self.connect(operation, vec![calls], || {
            checked?;
            Ok(Outputs::preserve())
        })
    }

    pub fn workspaces(
        &mut self,
        calls: Calls,
        workspaces: impl IntoIterator<Item = String>,
    ) -> Result<Calls, LiveError> {
        let mut workspaces: Vec<String> = workspaces.into_iter().collect();
        workspaces.sort();
        workspaces.dedup();
        let empty = workspaces.is_empty();
        self.connect(
            LiveOperation::Workspaces(workspaces),
            vec![calls],
            || match empty {
                true => Err(LiveError::NoWorkspace),
                false => Ok(Outputs::preserve()),
            },
        )
    }

    pub fn limit(&mut self, calls: Calls, rows: u64) -> Result<Calls, LiveError> {
        self.connect(LiveOperation::Limit(rows), vec![calls], || match rows {
            0 => Err(LiveError::EmptyLimit),
            _ => Ok(Outputs::preserve()),
        })
    }

    pub fn select(
        &mut self,
        calls: Calls,
        fields: impl IntoIterator<Item = Field>,
    ) -> Result<Calls, LiveError> {
        let (refs, outputs): (Vec<_>, Vec<_>) = fields
            .into_iter()
            .map(|field| {
                let declare = Declare::named(field.name, field.value);
                (field.output, Output::Declare(declare))
            })
            .unzip();
        self.connect(LiveOperation::Select(refs), vec![calls], || {
            Ok(Outputs::replace(outputs))
        })
    }
}

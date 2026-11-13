use super::{
    Condition, LiveError, LiveLanguage, LiveOperation, LivePlan, LiveValue, Operation, Workspaces,
};
use crate::catalog::Catalog;
use crate::implementations::live::Live;
use crate::plan::logical::Literal;
use crate::plan::tracking::{self, Construction, Declared, Ref};
use crate::{DataType, PropertyId};

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub(super) output: Ref,
    pub(super) name: String,
    pub(super) value: LiveValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Conditions(Vec<Condition>);

impl Field {
    pub fn eq(&self, value: impl Into<Literal>) -> Conditions {
        let condition = Condition::Equals {
            field: self.output,
            value: value.into(),
        };
        Conditions(vec![condition])
    }

    pub fn one_of(&self, values: impl IntoIterator<Item = Literal>) -> Conditions {
        let condition = Condition::OneOf {
            field: self.output,
            values: values.into_iter().collect(),
        };
        Conditions(vec![condition])
    }

    pub fn contains(&self, text: impl Into<String>) -> Conditions {
        let condition = Condition::Contains {
            field: self.output,
            text: text.into(),
        };
        Conditions(vec![condition])
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
    pub(super) rows: tracking::Rows,
    fields: Vec<Field>,
}

impl Calls {
    /// The field the call returns `property` in.
    pub fn field(&self, property: PropertyId) -> Result<Field, LiveError> {
        self.fields
            .iter()
            .find(|field| field.value.property == property)
            .cloned()
            .ok_or_else(|| LiveError::UnknownField {
                name: format!("{property:?}"),
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
    pub(super) catalog: &'c Catalog<Live>,
    pub(super) construction: Construction<LiveLanguage>,
}

impl<'c> LiveBuilder<'c> {
    pub(super) fn new(catalog: &'c Catalog<Live>) -> Self {
        Self {
            catalog,
            construction: Construction::new(catalog.model().id()),
        }
    }

    pub(super) fn finish(self, root: Result<Calls, LiveError>) -> Result<LivePlan, LiveError> {
        Ok(self.construction.finish(root?.rows)?)
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

    pub fn call(&mut self, operation: Declared<Operation>) -> Result<Calls, LiveError> {
        let operation = self
            .construction
            .attempt(|construction| construction.declared(operation))?;
        self.apply(LiveOperation::Call(operation), Vec::new())
    }

    pub fn filter(&mut self, calls: Calls, conditions: Conditions) -> Result<Calls, LiveError> {
        self.apply(LiveOperation::Filter(conditions.0), vec![calls])
    }

    pub fn window(
        &mut self,
        calls: Calls,
        field: &Field,
        from: Option<String>,
        to: Option<String>,
    ) -> Result<Calls, LiveError> {
        let operation = LiveOperation::Window {
            field: field.output,
            from,
            to,
        };
        self.apply(operation, vec![calls])
    }

    pub fn workspaces(&mut self, calls: Calls, workspaces: Workspaces) -> Result<Calls, LiveError> {
        let workspaces = match workspaces {
            Workspaces::Any => Workspaces::Any,
            Workspaces::Only(mut listed) => {
                listed.sort();
                listed.dedup();
                Workspaces::Only(listed)
            }
        };
        self.apply(LiveOperation::Workspaces(workspaces), vec![calls])
    }

    pub fn limit(&mut self, calls: Calls, rows: u64) -> Result<Calls, LiveError> {
        self.apply(LiveOperation::Limit(rows), vec![calls])
    }

    pub fn select(
        &mut self,
        calls: Calls,
        fields: impl IntoIterator<Item = Field>,
    ) -> Result<Calls, LiveError> {
        let refs = fields.into_iter().map(|field| field.output).collect();
        self.apply(LiveOperation::Select(refs), vec![calls])
    }

    pub(super) fn pushed(
        &self,
        field: &Field,
        text: bool,
        condition: &'static str,
    ) -> Result<(), LiveError> {
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

    pub(super) fn typed(field: &Field, value: &Literal) -> Result<(), LiveError> {
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
}

use std::convert::Infallible;

use query_data_model::ClickHouseDataModel;

use crate::ast::{Expr, Query, TableRef};
use crate::error::{QueryError, Result};
use crate::lowering::{Context, EmitOperation, SqlFragment};
use crate::planning::bind::Source;
use crate::planning::generic::{Node, Operation, Schema, Values};
use crate::planning::physical::{self, CurrentRows, Read, Scalar};

#[derive(Clone, PartialEq)]
pub struct Scan {
    read: Read,
    deletion_column: String,
    binding: Option<String>,
    relationship: Option<usize>,
    unique_key: Option<Schema>,
}

pub fn select(
    source: Source,
    model: &ClickHouseDataModel,
    values: &mut Values,
) -> Result<Node<Scan, Scalar, Infallible>> {
    let (binding, relationship) = match &source {
        Source::Entity { binding, .. } => (Some(binding.clone()), None),
        Source::Edge { relationship, .. } => (None, Some(*relationship)),
    };
    let plan = physical::select_source(source, model, CurrentRows::Final, values)?;

    plan.map_sources(&mut |read| {
        let deletion_column = model
            .backend()
            .table(&read.table)
            .map(|table| table.deletion_column())
            .ok_or_else(|| {
                QueryError::ReferenceError(format!(
                    "{} has no current-row deletion column",
                    read.table
                ))
            })?
            .to_string();

        let unique_key = model.backend().table(&read.table).and_then(|table| {
            (!table.sort_key.is_empty())
                .then(|| {
                    table
                        .sort_key
                        .iter()
                        .map(|column| {
                            read.columns
                                .iter()
                                .find(|(_, name)| name == column)
                                .map(|(value, _)| *value)
                        })
                        .collect::<Option<Vec<_>>>()
                })
                .flatten()
        });

        Ok(Scan {
            read,
            deletion_column,
            binding: binding.clone(),
            relationship,
            unique_key,
        })
    })
}

impl Scan {
    pub fn explain(&self) -> crate::planning::explain::SExpression {
        use crate::planning::explain::SExpression;

        SExpression::node(
            "CurrentRows",
            [
                self.read.explain(),
                SExpression::node("Deleted", [SExpression::atom(&self.deletion_column)]),
                SExpression::node("Binding", self.binding.iter().map(SExpression::atom)),
                SExpression::node(
                    "Relationship",
                    self.relationship.iter().map(SExpression::atom),
                ),
            ],
        )
    }
}

impl Operation for Scan {
    fn retain_outputs(&mut self, required: &Schema) -> bool {
        let changed = self.read.retain_outputs(required);
        if self
            .unique_key
            .as_ref()
            .is_some_and(|key| key.iter().any(|value| !required.contains(value)))
        {
            self.unique_key = None;
        }

        changed
    }

    fn map_values(&mut self, map: &mut impl FnMut(&mut crate::planning::generic::ValueId)) {
        self.read.map_values(map);
        for value in self.unique_key.iter_mut().flatten() {
            map(value);
        }
    }

    fn unique_keys(&self) -> Vec<Schema> {
        self.unique_key.iter().cloned().collect()
    }

    fn key_coverage(&self) -> crate::planning::generic::facts::KeyCoverage {
        crate::planning::generic::facts::KeyCoverage::Exact
    }

    fn output(&self, inputs: &[Schema], values: &Values) -> Result<Schema> {
        self.read.output(inputs, values)
    }
}

impl EmitOperation for Scan {
    fn emit(&self, _: Vec<SqlFragment>, context: &mut Context) -> Result<SqlFragment> {
        let alias = context.alias();
        if let Some(binding) = &self.binding {
            context
                .source_bindings
                .insert(alias.clone(), binding.clone());
        }

        let mut from = TableRef::scan_final(&self.read.table, &alias);
        if let Some(index) = self.relationship {
            from = from.with_relationship(index);
        }

        Ok(SqlFragment {
            query: Query {
                from,
                where_clause: Some(Expr::eq(
                    Expr::col(&alias, &self.deletion_column),
                    Expr::lit(false),
                )),
                ..Default::default()
            },
            exports: self
                .read
                .columns
                .iter()
                .map(|(value, column)| (*value, Expr::col(&alias, column)))
                .collect(),
        })
    }
}

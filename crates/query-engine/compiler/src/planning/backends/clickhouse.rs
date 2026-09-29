use std::convert::Infallible;

use query_data_model::ClickHouseDataModel;

use crate::ast::{Expr, Query, TableRef};
use crate::error::{QueryError, Result};
use crate::lowering::{Context, EmitOperation, SqlFragment};
use crate::planning::bind::Source;
use crate::planning::generic::{Node, Operation, Schema, Values};
use crate::planning::physical::{self, CurrentRows, Read, Scalar};

#[derive(Clone)]
pub struct Scan {
    read: Read,
    deletion_column: String,
    binding: Option<String>,
    relationship: Option<usize>,
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

        Ok(Scan {
            read,
            deletion_column,
            binding: binding.clone(),
            relationship,
        })
    })
}

impl Operation for Scan {
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

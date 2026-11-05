mod aggregate;
mod convert;
mod expr;
mod query;
mod scope;
mod sources;
mod tail;

use std::collections::HashMap;

use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::implementations::duckdb::DuckDb;
use query_data_model::implementations::relational::plan::RelationalError;
use query_data_model::implementations::relational::render::ParameterizedSql;
use query_data_model::{Catalog, ClickHouseDataModel, DuckDbDataModel};

use crate::ast::Node;
use crate::passes::codegen::{CompiledQueryContext, ParamValue, SqlDialect};

pub type Rendered = Result<ParameterizedSql, RelationalError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Identical,
    Undeclared(String),
}

pub fn clickhouse(
    ast: &Node,
    catalog: &Catalog<ClickHouse>,
    settings: Vec<(String, String)>,
) -> Rendered {
    let Node::Query(root) = ast else {
        return Err(RelationalError::Unsupported(
            "inserts outside the trusted DML path",
        ));
    };
    let plan = catalog.build(|q| {
        q.settings(settings)?;
        query::Importer::new(SqlDialect::ClickHouse).statement(q, root)
    })?;
    Ok(catalog.render(&plan))
}

pub fn duckdb(ast: &Node, catalog: &Catalog<DuckDb>) -> Rendered {
    let Node::Query(root) = ast else {
        return Err(RelationalError::Unsupported(
            "inserts outside the trusted DML path",
        ));
    };
    let plan = catalog.build(|q| query::Importer::new(SqlDialect::DuckDb).statement(q, root))?;
    Ok(catalog.render(&plan))
}

pub fn check_clickhouse(
    compiled: &CompiledQueryContext,
    model: &ClickHouseDataModel,
) -> Result<Outcome, String> {
    let settings = compiled.base.query_config.to_clickhouse_settings()?;
    let rendered = clickhouse(&compiled.ast, model.catalog(), settings);
    compare(&compiled.base.sql, &compiled.base.params, rendered)
}

pub fn check_duckdb(
    compiled: &CompiledQueryContext,
    model: &DuckDbDataModel,
) -> Result<Outcome, String> {
    let rendered = duckdb(&compiled.ast, model.catalog());
    compare(&compiled.base.sql, &compiled.base.params, rendered)
}

pub fn compare(
    sql: &str,
    parameters: &HashMap<String, ParamValue>,
    rendered: Rendered,
) -> Result<Outcome, String> {
    let rendered = match rendered {
        Ok(rendered) => rendered,
        Err(RelationalError::UnknownColumn { name, available })
            if declared_range(&name, &available) =>
        {
            return Ok(Outcome::Undeclared(name));
        }
        Err(error) => return Err(format!("{error}\n codegen: {sql}")),
    };
    if rendered.sql != sql {
        return Err(format!(
            "SQL differs\n codegen: {sql}\nrenderer: {}",
            rendered.sql
        ));
    }
    if rendered.parameters != *parameters {
        return Err(format!(
            "parameters differ for {sql}\n codegen: {parameters:?}\nrenderer: {:?}",
            rendered.parameters
        ));
    }
    Ok(Outcome::Identical)
}

fn declared_range(name: &str, available: &[String]) -> bool {
    name.split_once('.').is_some_and(|(range, _)| {
        available.iter().any(|column| {
            column
                .split_once('.')
                .is_some_and(|(other, _)| other == range)
        })
    })
}

#[cfg(test)]
pub(crate) fn assert_matches(
    compiled: &crate::Result<CompiledQueryContext>,
    check: impl FnOnce(&CompiledQueryContext) -> Result<Outcome, String>,
) {
    if let Ok(compiled) = compiled
        && let Err(mismatch) = check(compiled)
    {
        panic!("renderer parity: {mismatch}");
    }
}

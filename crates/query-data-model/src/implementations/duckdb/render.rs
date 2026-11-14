use std::collections::HashMap;

use orbit_utils::query_types::{ParamValue, SqlType};
use serde_json::Value;

use super::DuckDb;
use crate::catalog::Catalog;
use crate::implementations::relational::plan::{
    AggregateFunction, Expr, Function, Measure, ReadMode, RelationalPlan, TextMatch, TimeUnit,
};
use crate::implementations::relational::render::{Dialect, ParameterizedSql, Renderer};

#[derive(Default)]
pub struct DuckDbSql {
    parameters: HashMap<String, ParamValue>,
}

impl Catalog<DuckDb> {
    pub fn render(&self, plan: &RelationalPlan<DuckDb>) -> ParameterizedSql {
        Renderer::render(self, plan, DuckDbSql::default())
    }
}

pub fn type_name(data_type: SqlType) -> String {
    match data_type {
        SqlType::String => "VARCHAR".into(),
        SqlType::Int64 => "BIGINT".into(),
        SqlType::UInt32 => "UINTEGER".into(),
        SqlType::Float64 => "DOUBLE".into(),
        SqlType::Bool => "BOOLEAN".into(),
        SqlType::Date => "DATE".into(),
        SqlType::Timestamp { timezone, .. } => match timezone {
            Some(_) => "TIMESTAMPTZ".into(),
            None => "TIMESTAMP".into(),
        },
        SqlType::Array(element) => format!("{}[]", type_name(element.into())),
    }
}

impl DuckDbSql {
    fn bind(&mut self, data_type: SqlType, value: &Value) -> String {
        let position = self.parameters.len() + 1;
        let parameter = ParamValue {
            data_type,
            value: value.clone(),
        };
        self.parameters.insert(format!("p{position}"), parameter);
        format!("${position}")
    }
}

impl Dialect for DuckDbSql {
    type Backend = DuckDb;

    const UNION_TAIL_WRAPS: bool = false;

    fn parameter(&mut self, data_type: SqlType, value: &Value) -> String {
        match value {
            Value::Array(items) => {
                let element = match data_type {
                    SqlType::Array(element) => element.into(),
                    scalar => scalar,
                };
                let items: Vec<_> = items.iter().map(|item| self.bind(element, item)).collect();
                format!("({})", items.join(", "))
            }
            value => {
                let placeholder = self.bind(data_type, value);
                match data_type {
                    SqlType::Date | SqlType::Timestamp { .. } => {
                        format!("CAST({placeholder} AS {})", type_name(data_type))
                    }
                    _ => placeholder,
                }
            }
        }
    }

    fn scan(&self, table: &str, alias: &str, _read: ReadMode) -> String {
        format!("{table} AS {alias}")
    }

    fn cross_join(&self, left: &str, right: &str) -> String {
        format!("{left} CROSS JOIN {right}")
    }

    fn shared(&self, name: &str, _materialized: bool, body: &str) -> String {
        format!("{name} AS ({body})")
    }

    fn call(renderer: &mut Renderer<'_, Self>, function: Function, args: &[Expr]) -> String {
        super::functions::call(renderer, function, args)
    }

    fn measure(renderer: &mut Renderer<'_, Self>, measure: &Measure) -> String {
        let name = match measure.function {
            AggregateFunction::Count => "COUNT",
            AggregateFunction::Sum => "SUM",
            AggregateFunction::Avg => "AVG",
            AggregateFunction::Min => "MIN",
            AggregateFunction::Max => "MAX",
            AggregateFunction::Collect => "array_agg",
            AggregateFunction::ArgMax | AggregateFunction::ArgMaxOrNull => {
                return format!("arg_max({})", renderer.list(&measure.args));
            }
        };
        let argument = match &measure.args[..] {
            [] => "*".to_string(),
            [argument] => renderer.expr(argument),
            arguments => format!("({})", renderer.list(arguments)),
        };
        let distinct = if measure.distinct { "DISTINCT " } else { "" };
        let expression = format!("{name}({distinct}{argument})");
        let condition = measure
            .condition
            .as_ref()
            .map(|condition| renderer.expr(condition));
        if measure.function == AggregateFunction::Collect {
            let present = format!("{argument} IS NOT NULL");
            let condition = match condition {
                Some(condition) => format!("({condition}) AND {present}"),
                None => present,
            };
            return format!("coalesce({expression} FILTER (WHERE {condition}), [])");
        }
        match condition {
            Some(condition) => format!("{expression} FILTER (WHERE {condition})"),
            None => expression,
        }
    }

    fn time_bucket(&self, unit: TimeUnit, value: &str) -> String {
        let bucket = match unit {
            TimeUnit::Week => {
                format!("(date_trunc('week', {value} + INTERVAL 1 DAY) - INTERVAL 1 DAY)")
            }
            unit => format!("date_trunc('{}', {value})", unit.name()),
        };
        match unit.keeps_time() {
            true => bucket,
            false => format!("CAST({bucket} AS DATE)"),
        }
    }

    fn text_search(&self, mode: TextMatch, value: &str, query: &str) -> String {
        match mode {
            TextMatch::Contains => format!("contains({value}, {query})"),
            _ => unreachable!("DuckDB plans reject token search at construction"),
        }
    }

    fn lambda(&self, param: &str, body: &str) -> String {
        format!("lambda {param}: {body}")
    }

    fn empty_array(&self, types: &[SqlType]) -> String {
        let fields: Vec<_> = types
            .iter()
            .enumerate()
            .map(|(index, field)| format!("_{} {}", index + 1, type_name(*field)))
            .collect();
        format!("CAST([] AS STRUCT({})[])", fields.join(", "))
    }

    fn into_parameters(self) -> HashMap<String, ParamValue> {
        self.parameters
    }
}

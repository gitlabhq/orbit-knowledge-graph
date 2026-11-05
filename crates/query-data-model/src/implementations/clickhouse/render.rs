use std::collections::HashMap;

use orbit_utils::clickhouse::type_name;
use orbit_utils::query_types::{ParamBindings, ParamValue, SqlType};
use serde_json::Value;

use super::ClickHouse;
use crate::catalog::Catalog;
use crate::implementations::relational::plan::{
    AggregateFunction, Expr, Function, Measure, ReadMode, RelationalPlan, TextMatch, TimeUnit,
};
use crate::implementations::relational::render::{Dialect, ParameterizedSql, Renderer};

#[derive(Default)]
pub struct ClickHouseSql {
    parameters: ParamBindings,
}

impl Catalog<ClickHouse> {
    pub fn render(&self, plan: &RelationalPlan<ClickHouse>) -> ParameterizedSql {
        let mut rendered = Renderer::render(self, plan, ClickHouseSql::default());
        if !rendered.settings.is_empty() {
            let settings: Vec<_> = rendered
                .settings
                .iter()
                .map(|(name, value)| format!("{name} = {value}"))
                .collect();
            rendered.sql = format!("{} SETTINGS {}", rendered.sql, settings.join(", "));
        }
        rendered
    }
}

impl Dialect for ClickHouseSql {
    type Backend = ClickHouse;

    const UNION_TAIL_WRAPS: bool = true;

    fn parameter(&mut self, data_type: SqlType, value: &Value) -> String {
        let name = self.parameters.intern(data_type, value);
        format!("{{{name}:{}}}", type_name(data_type))
    }

    fn scan(&self, table: &str, alias: &str, read: ReadMode) -> String {
        match read {
            ReadMode::Current => format!("{table} AS {alias} FINAL"),
            ReadMode::Raw => format!("{table} AS {alias}"),
        }
    }

    fn cross_join(&self, left: &str, right: &str) -> String {
        format!("{left} INNER JOIN {right} ON 1")
    }

    fn shared(&self, name: &str, materialized: bool, body: &str) -> String {
        match materialized {
            true => format!("{name} AS MATERIALIZED ({body})"),
            false => format!("{name} AS ({body})"),
        }
    }

    fn call(renderer: &mut Renderer<'_, Self>, function: Function, args: &[Expr]) -> String {
        format!("{}({})", function_name(function), renderer.list(args))
    }

    fn measure(renderer: &mut Renderer<'_, Self>, measure: &Measure) -> String {
        let base = match measure.function {
            AggregateFunction::Count => "count",
            AggregateFunction::Sum => "sum",
            AggregateFunction::Avg => "avg",
            AggregateFunction::Min => "min",
            AggregateFunction::Max => "max",
            AggregateFunction::Collect => "groupArray",
            AggregateFunction::ArgMax => "argMax",
            AggregateFunction::ArgMaxOrNull => "argMaxOrNull",
        };
        let filtered = measure.condition.is_some();
        let name = match (measure.distinct, filtered, measure.function) {
            (
                false,
                false,
                AggregateFunction::Collect
                | AggregateFunction::ArgMax
                | AggregateFunction::ArgMaxOrNull,
            ) => base.to_string(),
            (false, false, _) => base.to_uppercase(),
            (distinct, filtered, _) => format!(
                "{base}{}{}",
                if distinct { "Distinct" } else { "" },
                if filtered { "If" } else { "" }
            ),
        };
        let mut args: Vec<_> = measure.args.iter().map(|arg| renderer.expr(arg)).collect();
        if let Some(condition) = &measure.condition {
            args.push(renderer.expr(condition));
        }
        format!("{name}({})", args.join(", "))
    }

    fn time_bucket(&self, unit: TimeUnit, value: &str) -> String {
        let function = match unit {
            TimeUnit::Minute => "toStartOfMinute",
            TimeUnit::Hour => "toStartOfHour",
            TimeUnit::Day => "toStartOfDay",
            TimeUnit::Week => "toStartOfWeek",
            TimeUnit::Month => "toStartOfMonth",
            TimeUnit::Quarter => "toStartOfQuarter",
            TimeUnit::Year => "toStartOfYear",
        };
        match unit.keeps_time() {
            true => format!("toDateTime64({function}({value}), 0)"),
            false => format!("toDate32({function}({value}))"),
        }
    }

    fn text_search(&self, mode: TextMatch, value: &str, query: &str) -> String {
        match mode {
            TextMatch::Contains => format!("multiSearchAny({value}, [{query}])"),
            TextMatch::Token => format!("hasToken({value}, {query})"),
            TextMatch::AllTokens => format!("hasAllTokens({value}, {query})"),
            TextMatch::AnyTokens => format!("hasAnyTokens({value}, {query})"),
        }
    }

    fn lambda(&self, param: &str, body: &str) -> String {
        format!("{param} -> {body}")
    }

    fn empty_array(&self, types: &[SqlType]) -> String {
        let types: Vec<_> = types.iter().map(|field| type_name(*field)).collect();
        format!("CAST([], 'Array(Tuple({}))')", types.join(", "))
    }

    fn into_parameters(self) -> HashMap<String, ParamValue> {
        self.parameters.into_map()
    }
}

fn function_name(function: Function) -> &'static str {
    match function {
        Function::StartsWith => "startsWith",
        Function::EndsWith => "endsWith",
        Function::Lower => "lower",
        Function::ToString => "toString",
        Function::ToJson => "toJSONString",
        Function::Object => "map",
        Function::If => "if",
        Function::Coalesce => "coalesce",
        Function::NullIf => "nullIf",
        Function::ByteLength => "length",
        Function::Substring => "substringUTF8",
        Function::Concat => "concat",
        Function::CountSubstrings => "countSubstrings",
        Function::Array => "array",
        Function::Tuple => "tuple",
        Function::ArrayConcat => "arrayConcat",
        Function::ArrayReverse => "arrayReverse",
        Function::ArrayResize => "arrayResize",
        Function::ArrayContains => "has",
        Function::ArrayContainsAny => "hasAny",
        Function::ArrayContainsAll => "hasAll",
        Function::ArrayFilter => "arrayFilter",
        Function::ArrayMap => "arrayMap",
        Function::ArrayExists => "arrayExists",
        Function::Unnest => "arrayJoin",
        Function::TupleElement => "tupleElement",
    }
}

use serde_json::Value;

use crate::query_types::{ParamValue, SqlType, TimeZone};

pub const MAX_BOUND_PATH_SEGMENTS: usize = 2000;

pub fn type_name(data_type: SqlType) -> String {
    match data_type {
        SqlType::String => "String".into(),
        SqlType::Int64 => "Int64".into(),
        SqlType::UInt32 => "UInt32".into(),
        SqlType::Float64 => "Float64".into(),
        SqlType::Bool => "Bool".into(),
        SqlType::Date => "Date32".into(),
        SqlType::Timestamp {
            precision,
            timezone,
        } => match timezone {
            Some(TimeZone::Utc) => format!("DateTime64({precision}, 'UTC')"),
            None => format!("DateTime64({precision})"),
        },
        SqlType::Array(element) => format!("Array({})", type_name(element.into())),
    }
}

impl ParamValue {
    pub fn render_clickhouse_literal(&self) -> String {
        match (&self.data_type, &self.value) {
            (
                SqlType::Timestamp {
                    precision,
                    timezone,
                },
                Value::String(value),
            ) => {
                let value = value.strip_suffix('Z').unwrap_or(value).replace('\'', "''");
                let zone = if timezone.is_some() { ", 'UTC'" } else { "" };
                format!("toDateTime64('{value}', {precision}{zone})")
            }
            (SqlType::Date, Value::String(value)) => {
                format!("toDate32({})", render_value(&Value::String(value.clone())))
            }
            _ => render_value(&self.value),
        }
    }

    pub fn render_http_param(&self) -> String {
        render_http_value(&self.value)
    }
}

pub fn render_value(value: &Value) -> String {
    match value {
        Value::String(value) => format!("'{}'", value.replace('\'', "''")),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Null => "NULL".into(),
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(render_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}

fn render_http_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => "\\N".into(),
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(|value| match value {
                    Value::String(value) => format!("'{}'", value.replace('\'', "\\'")),
                    other => render_http_value(other),
                })
                .collect::<Vec<_>>()
                .join(",")
        ),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_types::ScalarType;
    use serde_json::json;

    #[test]
    fn parameters_render_clickhouse_values_and_temporal_types() {
        for (data_type, value, expected) in [
            (SqlType::String, json!("hello"), "'hello'"),
            (SqlType::String, json!("it's a test"), "'it''s a test'"),
            (SqlType::Int64, json!(42), "42"),
            (SqlType::Bool, json!(true), "true"),
            (SqlType::String, Value::Null, "NULL"),
            (
                SqlType::Array(ScalarType::String),
                json!(["active", "blocked"]),
                "['active', 'blocked']",
            ),
            (
                SqlType::Array(ScalarType::Int64),
                json!([1, 2, 3]),
                "[1, 2, 3]",
            ),
            (SqlType::Array(ScalarType::String), json!([]), "[]"),
            (SqlType::Date, json!("2026-10-05"), "toDate32('2026-10-05')"),
            (
                SqlType::Timestamp {
                    precision: 6,
                    timezone: Some(TimeZone::Utc),
                },
                json!("2026-10-05T01:02:03.123456Z"),
                "toDateTime64('2026-10-05T01:02:03.123456', 6, 'UTC')",
            ),
        ] {
            assert_eq!(
                ParamValue { data_type, value }.render_clickhouse_literal(),
                expected
            );
        }
    }
}

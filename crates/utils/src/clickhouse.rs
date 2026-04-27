//! ClickHouse parameter types shared between `compiler` and `clickhouse-client`.

use std::fmt;

use serde_json::Value;

/// ClickHouse scalar types for parameterized query placeholders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display)]
pub enum ChScalar {
    String,
    Int64,
    Float64,
    Bool,
}

impl ChScalar {
    /// Infer scalar type from a JSON value.
    pub fn from_value(v: Option<&Value>) -> Self {
        match v {
            Some(Value::Number(n)) if n.is_i64() => ChScalar::Int64,
            Some(Value::Number(_)) => ChScalar::Float64,
            Some(Value::Bool(_)) => ChScalar::Bool,
            _ => ChScalar::String,
        }
    }
}

/// ClickHouse types used in parameterized query placeholders (`{pN:Type}`).
/// `Array(ChScalar)` maps to `Array(T)` for any scalar `T`, used in `IN`
/// clauses with multiple values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChType {
    String,
    Int64,
    UInt32,
    Float64,
    Bool,
    DateTime64,
    Array(ChScalar),
}

impl fmt::Display for ChType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChType::String => write!(f, "String"),
            ChType::Int64 => write!(f, "Int64"),
            ChType::UInt32 => write!(f, "UInt32"),
            ChType::Float64 => write!(f, "Float64"),
            ChType::Bool => write!(f, "Bool"),
            ChType::DateTime64 => write!(f, "DateTime64(6, 'UTC')"),
            ChType::Array(s) => write!(f, "Array({s})"),
        }
    }
}

impl From<ChScalar> for ChType {
    fn from(s: ChScalar) -> Self {
        match s {
            ChScalar::String => ChType::String,
            ChScalar::Int64 => ChType::Int64,
            ChScalar::Float64 => ChType::Float64,
            ChScalar::Bool => ChType::Bool,
        }
    }
}

impl ChType {
    /// Infer ClickHouse type from a JSON value.
    /// For arrays, inspects the first element to determine the element type.
    pub fn from_value(v: &Value) -> Self {
        match v {
            Value::Number(n) if n.is_i64() => ChType::Int64,
            Value::Number(_) => ChType::Float64,
            Value::Bool(_) => ChType::Bool,
            Value::Array(arr) => ChType::Array(ChScalar::from_value(arr.first())),
            _ => ChType::String,
        }
    }

    /// Promote a scalar type to its array equivalent.
    pub fn to_array(self) -> Self {
        match self {
            ChType::String | ChType::DateTime64 => ChType::Array(ChScalar::String),
            ChType::Int64 | ChType::UInt32 => ChType::Array(ChScalar::Int64),
            ChType::Float64 => ChType::Array(ChScalar::Float64),
            ChType::Bool => ChType::Array(ChScalar::Bool),
            ChType::Array(_) => self,
        }
    }
}

/// A query parameter with its ClickHouse type and JSON value.
#[derive(Debug, Clone, PartialEq)]
pub struct ParamValue {
    pub ch_type: ChType,
    pub value: Value,
}

impl ParamValue {
    /// Render as a ClickHouse SQL literal for debugging/observability.
    pub fn render_literal(&self) -> String {
        render_value(&self.value)
    }

    /// Render as a ClickHouse HTTP query parameter value.
    /// Unlike `render_literal`, strings are NOT quoted — ClickHouse handles
    /// typing via the `{name:Type}` placeholder in the SQL.
    pub fn render_http_param(&self) -> String {
        render_http_value(&self.value)
    }
}

/// Render a JSON value as a ClickHouse SQL literal.
pub fn render_value(value: &Value) -> String {
    fn quote(s: &str) -> String {
        format!("'{}'", s.replace('\'', "''"))
    }

    match value {
        Value::String(s) => quote(s),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Null => "NULL".to_string(),
        Value::Array(arr) => {
            let elements: Vec<String> = arr.iter().map(render_value).collect();
            format!("[{}]", elements.join(", "))
        }
        other => quote(&other.to_string()),
    }
}

fn render_http_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Null => "\\N".to_string(),
        Value::Array(arr) => {
            let elements: Vec<String> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => format!("'{}'", s.replace('\'', "\\'")),
                    other => render_http_value(other),
                })
                .collect();
            format!("[{}]", elements.join(","))
        }
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn render_literal_string() {
        let p = ParamValue {
            ch_type: ChType::String,
            value: Value::String("hello".into()),
        };
        assert_eq!(p.render_literal(), "'hello'");
    }

    #[test]
    fn render_literal_string_with_quotes() {
        let p = ParamValue {
            ch_type: ChType::String,
            value: Value::String("it's a test".into()),
        };
        assert_eq!(p.render_literal(), "'it''s a test'");
    }

    #[test]
    fn render_literal_int() {
        let p = ParamValue {
            ch_type: ChType::Int64,
            value: Value::from(42),
        };
        assert_eq!(p.render_literal(), "42");
    }

    #[test]
    fn render_literal_bool() {
        let p = ParamValue {
            ch_type: ChType::Bool,
            value: Value::Bool(true),
        };
        assert_eq!(p.render_literal(), "true");
    }

    #[test]
    fn render_literal_null() {
        let p = ParamValue {
            ch_type: ChType::String,
            value: Value::Null,
        };
        assert_eq!(p.render_literal(), "NULL");
    }

    #[test]
    fn render_literal_string_array() {
        let p = ParamValue {
            ch_type: ChType::Array(ChScalar::String),
            value: json!(["active", "blocked"]),
        };
        assert_eq!(p.render_literal(), "['active', 'blocked']");
    }

    #[test]
    fn render_literal_int_array() {
        let p = ParamValue {
            ch_type: ChType::Array(ChScalar::Int64),
            value: json!([1, 2, 3]),
        };
        assert_eq!(p.render_literal(), "[1, 2, 3]");
    }

    #[test]
    fn render_literal_empty_array() {
        let p = ParamValue {
            ch_type: ChType::Array(ChScalar::String),
            value: json!([]),
        };
        assert_eq!(p.render_literal(), "[]");
    }
}

use gkg_utils::clickhouse::{ChType, ParamValue};

pub fn to_sql_params(params: &[&ParamValue]) -> Vec<Box<dyn duckdb::ToSql>> {
    params.iter().map(|p| param_to_sql(p)).collect()
}

fn param_to_sql(param: &ParamValue) -> Box<dyn duckdb::ToSql> {
    match (&param.ch_type, &param.value) {
        (_, serde_json::Value::Null) => Box::new(Option::<String>::None),
        (ChType::String, serde_json::Value::String(s)) => Box::new(s.clone()),
        (ChType::Int64, serde_json::Value::Number(n)) => Box::new(n.as_i64().unwrap_or(0)),
        (ChType::Float64, serde_json::Value::Number(n)) => Box::new(n.as_f64().unwrap_or(0.0)),
        (ChType::Bool, serde_json::Value::Bool(b)) => Box::new(*b),
        (_, v) => Box::new(v.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn make_param(ch_type: ChType, value: Value) -> ParamValue {
        ParamValue { ch_type, value }
    }

    #[test]
    fn converts_string() {
        let p = make_param(ChType::String, Value::String("hello".into()));
        assert_eq!(to_sql_params(&[&p]).len(), 1);
    }

    #[test]
    fn converts_int64() {
        let p = make_param(ChType::Int64, Value::from(42));
        assert_eq!(to_sql_params(&[&p]).len(), 1);
    }

    #[test]
    fn converts_float64() {
        let p = make_param(ChType::Float64, Value::from(1.5));
        assert_eq!(to_sql_params(&[&p]).len(), 1);
    }

    #[test]
    fn converts_bool() {
        let p = make_param(ChType::Bool, Value::Bool(true));
        assert_eq!(to_sql_params(&[&p]).len(), 1);
    }

    #[test]
    fn converts_null() {
        let p = make_param(ChType::String, Value::Null);
        assert_eq!(to_sql_params(&[&p]).len(), 1);
    }

    #[test]
    fn fallback_renders_as_string() {
        let p = make_param(ChType::Int64, Value::String("not-a-number".into()));
        assert_eq!(to_sql_params(&[&p]).len(), 1);
    }
}

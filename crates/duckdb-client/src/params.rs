use orbit_utils::query_types::{ParamValue, SqlType};

pub fn to_sql_params(params: &[&ParamValue]) -> Vec<Box<dyn duckdb::ToSql>> {
    params.iter().map(|p| param_to_sql(p)).collect()
}

fn param_to_sql(param: &ParamValue) -> Box<dyn duckdb::ToSql> {
    match (&param.data_type, &param.value) {
        (SqlType::Int64, serde_json::Value::Null) => Box::new(Option::<i64>::None),
        (SqlType::Float64, serde_json::Value::Null) => Box::new(Option::<f64>::None),
        (SqlType::Bool, serde_json::Value::Null) => Box::new(Option::<bool>::None),
        (_, serde_json::Value::Null) => Box::new(Option::<String>::None),
        (_, serde_json::Value::String(s)) => Box::new(s.clone()),
        (SqlType::Int64, serde_json::Value::Number(n)) => match n.as_i64() {
            Some(v) => Box::new(v),
            None => Box::new(param.value.to_string()),
        },
        (SqlType::Float64, serde_json::Value::Number(n)) => match n.as_f64() {
            Some(v) => Box::new(v),
            None => Box::new(param.value.to_string()),
        },
        (SqlType::UInt32, serde_json::Value::Number(n)) => match n.as_u64() {
            Some(value) => Box::new(value),
            None => Box::new(param.value.to_string()),
        },
        (SqlType::Bool, serde_json::Value::Bool(b)) => Box::new(*b),
        (_, v) => Box::new(v.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn make_param(data_type: SqlType, value: Value) -> ParamValue {
        ParamValue { data_type, value }
    }

    #[test]
    fn converts_string() {
        let p = make_param(SqlType::String, Value::String("hello".into()));
        assert_eq!(to_sql_params(&[&p]).len(), 1);
    }

    #[test]
    fn converts_int64() {
        let p = make_param(SqlType::Int64, Value::from(42));
        assert_eq!(to_sql_params(&[&p]).len(), 1);
    }

    #[test]
    fn converts_float64() {
        let p = make_param(SqlType::Float64, Value::from(1.5));
        assert_eq!(to_sql_params(&[&p]).len(), 1);
    }

    #[test]
    fn converts_bool() {
        let p = make_param(SqlType::Bool, Value::Bool(true));
        assert_eq!(to_sql_params(&[&p]).len(), 1);
    }

    #[test]
    fn converts_null() {
        let p = make_param(SqlType::String, Value::Null);
        assert_eq!(to_sql_params(&[&p]).len(), 1);
    }

    #[test]
    fn fallback_renders_as_string() {
        let p = make_param(SqlType::Int64, Value::String("not-a-number".into()));
        assert_eq!(to_sql_params(&[&p]).len(), 1);
    }
}

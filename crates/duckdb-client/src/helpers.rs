use arrow::array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};

pub fn sql_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

pub fn scalar_i64(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .map(|arr| arr.value(0))
        .unwrap_or(0)
}

pub fn string_column(batches: &[RecordBatch], name: &str) -> Vec<String> {
    batches
        .iter()
        .filter_map(|b| {
            b.column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        })
        .flat_map(|arr| arr.iter().flatten().map(String::from))
        .collect()
}

pub fn i64_column(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    batches
        .iter()
        .filter_map(|b| {
            b.column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        })
        .flat_map(|arr| arr.iter().map(|v| v.unwrap_or(0)))
        .collect()
}

pub fn f64_column(batches: &[RecordBatch], name: &str) -> Vec<f64> {
    batches
        .iter()
        .filter_map(|b| {
            b.column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        })
        .flat_map(|arr| arr.iter().map(|v| v.unwrap_or(0.0)))
        .collect()
}

pub fn bool_column(batches: &[RecordBatch], name: &str) -> Vec<bool> {
    batches
        .iter()
        .filter_map(|b| {
            b.column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
        })
        .flat_map(|arr| arr.iter().map(|v| v.unwrap_or(false)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_lit_doubles_single_quotes() {
        assert_eq!(sql_lit("O'Brien"), "'O''Brien'");
        assert_eq!(sql_lit("plain"), "'plain'");
    }
}

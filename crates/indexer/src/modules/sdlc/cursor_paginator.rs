use std::fmt;

use arrow::array::{Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::DataType as ArrowDataType;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CursorValue {
    Int64(i64),
    UInt64(u64),
    String(String),
}

impl fmt::Display for CursorValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CursorValue::Int64(v) => write!(f, "{v}"),
            CursorValue::UInt64(v) => write!(f, "{v}"),
            CursorValue::String(v) => write!(f, "'{v}'"),
        }
    }
}

impl CursorValue {
    fn clickhouse_type(&self) -> &'static str {
        match self {
            CursorValue::Int64(_) => "Int64",
            CursorValue::UInt64(_) => "UInt64",
            CursorValue::String(_) => "String",
        }
    }
}

/// Wraps an extract query with PK-ordered pagination.
///
/// When `cursor_columns` is non-empty, appends `ORDER BY ... LIMIT ...` and
/// composite-key cursor filtering. When empty, passes the query through unchanged
/// and treats every result set as the final page.
#[derive(Debug, Clone)]
pub struct CursorPaginator {
    cursor_columns: Vec<String>,
    page_size: u64,
    cursor_values: Option<Vec<CursorValue>>,
}

impl CursorPaginator {
    pub fn new(cursor_columns: Vec<String>, page_size: u64) -> Self {
        Self {
            cursor_columns,
            page_size,
            cursor_values: None,
        }
    }

    pub fn with_cursor(mut self, cursor_values: Vec<CursorValue>) -> Self {
        if !cursor_values.is_empty() {
            self.cursor_values = Some(cursor_values);
        }
        self
    }

    pub fn cursor_values(&self) -> Option<&[CursorValue]> {
        self.cursor_values.as_deref()
    }

    pub fn build_page_query(&self, base_query: &str) -> String {
        if self.cursor_columns.is_empty() {
            return base_query.to_string();
        }

        let order_clause = self.cursor_columns.join(", ");

        match &self.cursor_values {
            None => {
                format!("{base_query} ORDER BY {order_clause} LIMIT {}", self.page_size)
            }
            Some(values) => {
                let cursor_where = build_composite_cursor_where(&self.cursor_columns, values);
                format!(
                    "{base_query} AND ({cursor_where}) ORDER BY {order_clause} LIMIT {}",
                    self.page_size
                )
            }
        }
    }

    pub fn is_last_page(&self, rows_returned: u64) -> bool {
        if self.cursor_columns.is_empty() {
            return true;
        }
        rows_returned < self.page_size
    }

    pub fn advance(&mut self, batch: &RecordBatch) -> Option<Vec<CursorValue>> {
        if batch.num_rows() == 0 || self.cursor_columns.is_empty() {
            return self.cursor_values.clone();
        }

        let last_row = batch.num_rows() - 1;
        let mut values = Vec::with_capacity(self.cursor_columns.len());

        for column_name in &self.cursor_columns {
            let column_index = batch.schema().index_of(column_name).ok()?;
            let value = extract_cursor_value(batch.column(column_index).as_ref(), last_row)?;
            values.push(value);
        }

        self.cursor_values = Some(values.clone());
        Some(values)
    }
}

fn extract_cursor_value(column: &dyn Array, row: usize) -> Option<CursorValue> {
    if column.is_null(row) {
        return None;
    }

    match column.data_type() {
        ArrowDataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>()?;
            Some(CursorValue::Int64(array.value(row)))
        }
        ArrowDataType::UInt64 => {
            let array = column.as_any().downcast_ref::<UInt64Array>()?;
            Some(CursorValue::UInt64(array.value(row)))
        }
        ArrowDataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>()?;
            Some(CursorValue::String(array.value(row).to_string()))
        }
        ArrowDataType::LargeUtf8 => {
            let array = column
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()?;
            Some(CursorValue::String(array.value(row).to_string()))
        }
        _ => None,
    }
}

fn build_composite_cursor_where(columns: &[String], values: &[CursorValue]) -> String {
    assert_eq!(columns.len(), values.len());

    let mut disjuncts = Vec::with_capacity(columns.len());

    for depth in 0..columns.len() {
        let mut conjuncts = Vec::with_capacity(depth + 1);

        for equal_index in 0..depth {
            let param = format!("__cursor_{equal_index}");
            conjuncts.push(format!(
                "{} = {{{param}:{}}}",
                columns[equal_index],
                values[equal_index].clickhouse_type()
            ));
        }

        let param = format!("__cursor_{depth}");
        conjuncts.push(format!(
            "{} > {{{param}:{}}}",
            columns[depth],
            values[depth].clickhouse_type()
        ));

        disjuncts.push(conjuncts.join(" AND "));
    }

    disjuncts
        .iter()
        .map(|d| format!("({d})"))
        .collect::<Vec<_>>()
        .join(" OR ")
}

pub fn cursor_params(values: &[CursorValue]) -> Vec<(String, serde_json::Value)> {
    values
        .iter()
        .enumerate()
        .map(|(i, value)| {
            let key = format!("__cursor_{i}");
            let json_value = match value {
                CursorValue::Int64(v) => serde_json::Value::Number((*v).into()),
                CursorValue::UInt64(v) => serde_json::Value::Number((*v).into()),
                CursorValue::String(v) => serde_json::Value::String(v.clone()),
            };
            (key, json_value)
        })
        .collect()
}

pub fn serialize_cursor(values: &[CursorValue]) -> String {
    serde_json::to_string(values).expect("cursor serialization should not fail")
}

pub fn deserialize_cursor(json: &str) -> Result<Vec<CursorValue>, serde_json::Error> {
    serde_json::from_str(json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};

    #[test]
    fn single_column_cursor_where() {
        let columns = vec!["id".to_string()];
        let values = vec![CursorValue::Int64(42)];

        let result = build_composite_cursor_where(&columns, &values);
        assert_eq!(result, "(id > {__cursor_0:Int64})");
    }

    #[test]
    fn two_column_composite_cursor_where() {
        let columns = vec!["traversal_path".to_string(), "id".to_string()];
        let values = vec![
            CursorValue::String("1/2/".to_string()),
            CursorValue::Int64(100),
        ];

        let result = build_composite_cursor_where(&columns, &values);
        assert_eq!(
            result,
            "(traversal_path > {__cursor_0:String}) OR (traversal_path = {__cursor_0:String} AND id > {__cursor_1:Int64})"
        );
    }

    #[test]
    fn three_column_composite_cursor_where() {
        let columns = vec![
            "traversal_path".to_string(),
            "source_id".to_string(),
            "target_id".to_string(),
        ];
        let values = vec![
            CursorValue::String("1/2/".to_string()),
            CursorValue::Int64(10),
            CursorValue::Int64(20),
        ];

        let result = build_composite_cursor_where(&columns, &values);
        assert!(result.contains("(traversal_path > {__cursor_0:String})"));
        assert!(result.contains(
            "(traversal_path = {__cursor_0:String} AND source_id > {__cursor_1:Int64})"
        ));
        assert!(result.contains("(traversal_path = {__cursor_0:String} AND source_id = {__cursor_1:Int64} AND target_id > {__cursor_2:Int64})"));
    }

    #[test]
    fn first_page_query_has_no_cursor_where() {
        let paginator =
            CursorPaginator::new(vec!["traversal_path".to_string(), "id".to_string()], 1000);

        let query = paginator.build_page_query("SELECT * FROM t WHERE watermark > '2024-01-01'");
        assert_eq!(
            query,
            "SELECT * FROM t WHERE watermark > '2024-01-01' ORDER BY traversal_path, id LIMIT 1000"
        );
    }

    #[test]
    fn subsequent_page_query_includes_cursor_where() {
        let paginator = CursorPaginator::new(
            vec!["traversal_path".to_string(), "id".to_string()],
            500,
        )
        .with_cursor(vec![
            CursorValue::String("1/2/".to_string()),
            CursorValue::Int64(100),
        ]);

        let query = paginator.build_page_query("SELECT * FROM t WHERE watermark > '2024-01-01'");
        assert!(query.starts_with("SELECT * FROM t WHERE watermark > '2024-01-01' AND ("));
        assert!(query.contains("ORDER BY traversal_path, id LIMIT 500"));
    }

    #[test]
    fn is_last_page_when_fewer_rows_than_page_size() {
        let paginator = CursorPaginator::new(vec!["id".to_string()], 1000);
        assert!(paginator.is_last_page(999));
        assert!(paginator.is_last_page(0));
        assert!(!paginator.is_last_page(1000));
    }

    #[test]
    fn empty_cursor_columns_passes_query_through() {
        let paginator = CursorPaginator::new(vec![], 1000);
        let query = paginator.build_page_query("SELECT * FROM t");
        assert_eq!(query, "SELECT * FROM t");
    }

    #[test]
    fn empty_cursor_columns_always_last_page() {
        let paginator = CursorPaginator::new(vec![], 1000);
        assert!(paginator.is_last_page(5000));
        assert!(paginator.is_last_page(0));
    }

    #[test]
    fn advance_extracts_last_row_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("traversal_path", ArrowDataType::Utf8, false),
            Field::new("id", ArrowDataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["1/2/", "3/4/", "5/6/"])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let mut paginator =
            CursorPaginator::new(vec!["traversal_path".to_string(), "id".to_string()], 100);

        let values = paginator.advance(&batch).unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], CursorValue::String("5/6/".to_string()));
        assert_eq!(values[1], CursorValue::Int64(30));
    }

    #[test]
    fn advance_empty_batch_returns_previous_cursor() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![] as Vec<i64>))],
        )
        .unwrap();

        let mut paginator = CursorPaginator::new(vec!["id".to_string()], 100);
        let values = paginator.advance(&batch);
        assert!(values.is_none());
    }

    #[test]
    fn cursor_params_produces_correct_json() {
        let values = vec![
            CursorValue::String("1/2/".to_string()),
            CursorValue::Int64(42),
        ];

        let params = cursor_params(&values);
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].0, "__cursor_0");
        assert_eq!(params[0].1, serde_json::Value::String("1/2/".to_string()));
        assert_eq!(params[1].0, "__cursor_1");
        assert_eq!(params[1].1, serde_json::json!(42));
    }

    #[test]
    fn serialize_deserialize_cursor_roundtrip() {
        let values = vec![
            CursorValue::String("path".to_string()),
            CursorValue::Int64(99),
        ];

        let json = serialize_cursor(&values);
        let restored = deserialize_cursor(&json).unwrap();
        assert_eq!(values, restored);
    }

    #[test]
    fn with_cursor_empty_vec_stays_none() {
        let paginator = CursorPaginator::new(vec!["id".to_string()], 100).with_cursor(vec![]);
        assert_eq!(
            paginator.build_page_query("SELECT * FROM t"),
            "SELECT * FROM t ORDER BY id LIMIT 100"
        );
    }
}

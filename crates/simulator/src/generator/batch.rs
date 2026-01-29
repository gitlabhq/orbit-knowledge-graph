//! Dynamic RecordBatch builder from ontology definitions.

use super::fake_data::{FakeValue, FakeValueGenerator};
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use ontology::{DataType, Field, NodeEntity};
use std::collections::HashSet;
use std::sync::Arc;

/// Builds Arrow RecordBatches dynamically from ontology node definitions.
///
/// Accumulates rows and flushes to batches when `batch_size` is reached.
pub struct BatchBuilder {
    /// Primary key field names (from ontology).
    primary_keys: HashSet<String>,
    /// Maximum rows per batch before flushing.
    batch_size: usize,
    /// Schema for building batches.
    schema: Arc<Schema>,
    fake_gen: FakeValueGenerator,
    organization_ids: Vec<u32>,
    traversal_ids: Vec<String>,
    columns: Vec<ColumnData>,
    batches: Vec<RecordBatch>,
}

/// Holds data for a single column during batch building.
struct ColumnData {
    field: Field,
    values: ColumnValues,
}

/// Enum to hold different column value types.
enum ColumnValues {
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
    String(Vec<Option<String>>),
    Date32(Vec<Option<i32>>),
}

impl BatchBuilder {
    pub fn new(node: &NodeEntity, schema: Arc<Schema>, batch_size: usize) -> Self {
        let columns: Vec<ColumnData> = node
            .fields
            .iter()
            .map(|field| ColumnData {
                field: field.clone(),
                values: ColumnValues::new(&field.data_type),
            })
            .collect();

        let primary_keys: HashSet<String> = node.primary_keys.iter().cloned().collect();

        Self {
            primary_keys,
            batch_size,
            schema,
            fake_gen: FakeValueGenerator::new_fast(), // Fast mode with non-predictable patterns
            organization_ids: Vec::with_capacity(batch_size),
            traversal_ids: Vec::with_capacity(batch_size),
            columns,
            batches: Vec::new(),
        }
    }

    pub fn add_row(&mut self, organization_id: u32, traversal_id: String, id: i64) {
        self.organization_ids.push(organization_id);
        self.traversal_ids.push(traversal_id);

        for col_data in &mut self.columns {
            if self.primary_keys.contains(&col_data.field.name) {
                col_data.values.push_int64(Some(id));
            } else {
                let value = self.fake_gen.generate(&col_data.field);
                col_data
                    .values
                    .push_value(&value, &col_data.field.data_type);
            }
        }

        if self.organization_ids.len() >= self.batch_size {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.organization_ids.is_empty() {
            return;
        }

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(2 + self.columns.len());

        arrays.push(Arc::new(UInt32Array::from(std::mem::take(
            &mut self.organization_ids,
        ))));

        arrays.push(Arc::new(StringArray::from(std::mem::take(
            &mut self.traversal_ids,
        ))));

        for col_data in &mut self.columns {
            let array = col_data.values.drain_to_array();
            arrays.push(array);
        }

        if let Ok(batch) = RecordBatch::try_new(self.schema.clone(), arrays) {
            self.batches.push(batch);
        }

        self.organization_ids = Vec::with_capacity(self.batch_size);
        self.traversal_ids = Vec::with_capacity(self.batch_size);
    }

    pub fn finish(mut self) -> Vec<RecordBatch> {
        self.flush();
        self.batches
    }
}

impl ColumnValues {
    fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Int | DataType::DateTime => ColumnValues::Int64(Vec::new()),
            DataType::Float => ColumnValues::Float64(Vec::new()),
            DataType::Bool => ColumnValues::Bool(Vec::new()),
            DataType::String | DataType::Enum => ColumnValues::String(Vec::new()),
            DataType::Date => ColumnValues::Date32(Vec::new()),
        }
    }

    fn push_value(&mut self, value: &FakeValue, _data_type: &DataType) {
        match (self, value) {
            (ColumnValues::Int64(vec), FakeValue::Int(v)) => vec.push(Some(*v)),
            (ColumnValues::Int64(vec), FakeValue::DateTime(v)) => vec.push(Some(*v)),
            (ColumnValues::Int64(vec), FakeValue::Null) => vec.push(None),
            (ColumnValues::Float64(vec), FakeValue::Float(v)) => vec.push(Some(*v)),
            (ColumnValues::Float64(vec), FakeValue::Null) => vec.push(None),
            (ColumnValues::Bool(vec), FakeValue::Bool(v)) => vec.push(Some(*v)),
            (ColumnValues::Bool(vec), FakeValue::Null) => vec.push(None),
            (ColumnValues::String(vec), FakeValue::String(v)) => vec.push(Some(v.clone())),
            (ColumnValues::String(vec), FakeValue::Null) => vec.push(None),
            (ColumnValues::Date32(vec), FakeValue::Date(v)) => vec.push(Some(*v)),
            (ColumnValues::Date32(vec), FakeValue::Null) => vec.push(None),
            // Type mismatch fallback
            (ColumnValues::Int64(vec), _) => vec.push(None),
            (ColumnValues::Float64(vec), _) => vec.push(None),
            (ColumnValues::Bool(vec), _) => vec.push(None),
            (ColumnValues::String(vec), _) => vec.push(None),
            (ColumnValues::Date32(vec), _) => vec.push(None),
        }
    }

    fn push_int64(&mut self, value: Option<i64>) {
        if let ColumnValues::Int64(vec) = self {
            vec.push(value);
        }
    }

    fn drain_to_array(&mut self) -> ArrayRef {
        match self {
            ColumnValues::Int64(vec) => {
                let data = std::mem::take(vec);
                Arc::new(Int64Array::from(data))
            }
            ColumnValues::Float64(vec) => {
                let data = std::mem::take(vec);
                Arc::new(Float64Array::from(data))
            }
            ColumnValues::Bool(vec) => {
                let data = std::mem::take(vec);
                Arc::new(BooleanArray::from(data))
            }
            ColumnValues::String(vec) => {
                let data = std::mem::take(vec);
                Arc::new(StringArray::from(data))
            }
            ColumnValues::Date32(vec) => {
                let data = std::mem::take(vec);
                Arc::new(Date32Array::from(data))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_node() -> NodeEntity {
        NodeEntity {
            name: "TestNode".to_string(),
            fields: vec![
                Field {
                    name: "id".to_string(),
                    source: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                    enum_values: None,
                },
                Field {
                    name: "name".to_string(),
                    source: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    enum_values: None,
                },
                Field {
                    name: "active".to_string(),
                    source: "active".to_string(),
                    data_type: DataType::Bool,
                    nullable: false,
                    enum_values: None,
                },
            ],
            primary_keys: vec!["id".to_string()],
            destination_table: "gl_test_nodes".to_string(),
            etl: None,
        }
    }

    #[test]
    fn test_batch_builder_single_batch() {
        use crate::arrow_schema::ToArrowSchema;

        let node = test_node();
        let schema = Arc::new(node.to_arrow_schema());
        let mut builder = BatchBuilder::new(&node, schema, 100);

        // Add some rows (less than batch_size)
        for i in 0..10 {
            builder.add_row(1, format!("1/{}", i), i + 1);
        }

        let batches = builder.finish();
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 10);
        assert_eq!(batch.num_columns(), 5); // organization_id + traversal_path + 3 fields

        // Check organization_id column
        let org_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert!(org_ids.iter().all(|v| v == Some(1)));

        // Check traversal_path column
        let traversal_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(traversal_ids.iter().all(|v| v.is_some()));

        // Check id column
        let ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let id_values: Vec<i64> = ids.iter().flatten().collect();
        assert_eq!(id_values, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_batch_builder_multiple_batches() {
        use crate::arrow_schema::ToArrowSchema;

        let node = test_node();
        let schema = Arc::new(node.to_arrow_schema());
        let mut builder = BatchBuilder::new(&node, schema, 5); // Small batch size

        // Add 12 rows - should create 3 batches (5, 5, 2)
        for i in 0..12 {
            builder.add_row(1, format!("1/{}", i), i + 1);
        }

        let batches = builder.finish();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[1].num_rows(), 5);
        assert_eq!(batches[2].num_rows(), 2);
    }
}

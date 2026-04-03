//! Dynamic RecordBatch builder from ontology definitions.

use super::fake_data::{FakeDataPools, FakeValue, FakeValueGenerator, FieldKind};
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use ontology::constants::TRAVERSAL_PATH_COLUMN;
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
    traversal_paths: Vec<String>,
    columns: Vec<ColumnData>,
    batches: Vec<RecordBatch>,
}

/// Holds data for a single column during batch building.
struct ColumnData {
    field: Field,
    /// Pre-computed field kind for fast generation.
    kind: FieldKind,
    /// Cached enum values for enum fields (uses i64 keys as per ontology).
    enum_values: Option<std::collections::BTreeMap<i64, String>>,
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
    pub fn new(
        node: &NodeEntity,
        schema: Arc<Schema>,
        batch_size: usize,
        pools: &'static FakeDataPools,
    ) -> Self {
        Self::with_seed(node, schema, batch_size, None, pools)
    }

    pub fn with_seed(
        node: &NodeEntity,
        schema: Arc<Schema>,
        batch_size: usize,
        seed: Option<u64>,
        pools: &'static FakeDataPools,
    ) -> Self {
        // Skip traversal_path - it's a system column handled separately
        // Pre-compute FieldKind once per field to avoid runtime string matching
        let columns: Vec<ColumnData> = node
            .fields
            .iter()
            .filter(|field| field.name != TRAVERSAL_PATH_COLUMN)
            .map(|field| ColumnData {
                kind: FieldKind::classify(field, pools),
                enum_values: field.enum_values.clone(),
                field: field.clone(),
                values: ColumnValues::new(&field.data_type),
            })
            .collect();

        let primary_keys: HashSet<String> = node.primary_keys.iter().cloned().collect();

        let fake_gen = match seed {
            Some(s) => FakeValueGenerator::fast_with_seed(s, pools),
            None => FakeValueGenerator::new_fast(pools),
        };

        Self {
            primary_keys,
            batch_size,
            schema,
            fake_gen,
            traversal_paths: Vec::with_capacity(batch_size),
            columns,
            batches: Vec::new(),
        }
    }

    pub fn add_row(&mut self, traversal_path: String, id: i64) {
        self.traversal_paths.push(traversal_path);

        for col_data in &mut self.columns {
            if self.primary_keys.contains(&col_data.field.name) {
                col_data.values.push_int64(Some(id));
            } else {
                // Use pre-computed FieldKind for fast generation
                let value = self.fake_gen.generate_with_kind(
                    col_data.kind,
                    col_data.field.nullable,
                    col_data.enum_values.as_ref(),
                );
                col_data
                    .values
                    .push_value(&value, &col_data.field.data_type);
            }
        }

        if self.traversal_paths.len() >= self.batch_size {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.traversal_paths.is_empty() {
            return;
        }

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(1 + self.columns.len());

        arrays.push(Arc::new(StringArray::from(std::mem::take(
            &mut self.traversal_paths,
        ))));

        for col_data in &mut self.columns {
            let array = col_data.values.drain_to_array();
            arrays.push(array);
        }

        if let Ok(batch) = RecordBatch::try_new(self.schema.clone(), arrays) {
            self.batches.push(batch);
        }

        self.traversal_paths = Vec::with_capacity(self.batch_size);
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
            DataType::String | DataType::Enum | DataType::Uuid => ColumnValues::String(Vec::new()),
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
            (ColumnValues::String(vec), FakeValue::String(v)) => vec.push(Some(v.to_string())),
            (ColumnValues::String(vec), FakeValue::Null) => vec.push(None),
            (ColumnValues::Date32(vec), FakeValue::Date(v)) => vec.push(Some(*v)),
            (ColumnValues::Date32(vec), FakeValue::Null) => vec.push(None),
            (col, val) => panic!(
                "type mismatch: column {:?} got {:?}",
                std::mem::discriminant(col),
                val
            ),
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
    use ontology::FieldSource;

    fn fake_data_path() -> String {
        crate::synth::fixture_path(crate::synth::constants::DEFAULT_FAKE_DATA_PATH)
    }

    fn test_node() -> NodeEntity {
        NodeEntity {
            name: "TestNode".to_string(),
            domain: "core".to_string(),
            description: "Test node entity".to_string(),
            label: "name".to_string(),
            fields: vec![
                Field {
                    name: "id".to_string(),
                    source: FieldSource::DatabaseColumn("id".to_string()),
                    data_type: DataType::Int,
                    ..Default::default()
                },
                Field {
                    name: "name".to_string(),
                    source: FieldSource::DatabaseColumn("name".to_string()),
                    data_type: DataType::String,
                    nullable: true,
                    ..Default::default()
                },
                Field {
                    name: "active".to_string(),
                    source: FieldSource::DatabaseColumn("active".to_string()),
                    data_type: DataType::Bool,
                    ..Default::default()
                },
            ],
            destination_table: "gl_test_nodes".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn test_batch_builder_single_batch() {
        use crate::synth::arrow_schema::ToArrowSchema;
        use crate::synth::config::FakeDataConfig;

        let pools = FakeDataPools::intern(FakeDataConfig::load(fake_data_path()).unwrap());
        let node = test_node();
        let schema = Arc::new(node.to_arrow_schema());
        let mut builder = BatchBuilder::new(&node, schema, 100, pools);

        // Add some rows (less than batch_size)
        for i in 0..10 {
            builder.add_row(format!("1/{}/", i), i + 1);
        }

        let batches = builder.finish();
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 10);
        assert_eq!(batch.num_columns(), 4); // traversal_path + 3 fields

        // Check traversal_path column
        let traversal_paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(traversal_paths.iter().all(|v| v.is_some()));

        // Check id column
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let id_values: Vec<i64> = ids.iter().flatten().collect();
        assert_eq!(id_values, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_batch_builder_multiple_batches() {
        use crate::synth::arrow_schema::ToArrowSchema;
        use crate::synth::config::FakeDataConfig;

        let pools = FakeDataPools::intern(FakeDataConfig::load(fake_data_path()).unwrap());
        let node = test_node();
        let schema = Arc::new(node.to_arrow_schema());
        let mut builder = BatchBuilder::new(&node, schema, 5, pools); // Small batch size

        // Add 12 rows - should create 3 batches (5, 5, 2)
        for i in 0..12 {
            builder.add_row(format!("1/{}/", i), i + 1);
        }

        let batches = builder.finish();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[1].num_rows(), 5);
        assert_eq!(batches[2].num_rows(), 2);
    }
}

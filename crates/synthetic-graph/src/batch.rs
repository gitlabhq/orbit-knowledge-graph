use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use ontology::{DataType, Field, NodeEntity};

use crate::fake_values::{FakeValue, FakeValueGenerator, FieldKind};

/// Builds Arrow RecordBatches dynamically from ontology node definitions.
///
/// Accumulates rows and auto-flushes to batches at `batch_size`. Each row
/// requires a traversal path and entity ID; all other fields are generated
/// from the ontology field definitions using [`FakeValueGenerator`].
pub struct BatchBuilder {
    primary_keys: HashSet<String>,
    batch_size: usize,
    schema: Arc<Schema>,
    fake_gen: FakeValueGenerator,
    traversal_paths: Vec<String>,
    columns: Vec<ColumnData>,
    batches: Vec<RecordBatch>,
}

struct ColumnData {
    field: Field,
    kind: FieldKind,
    enum_values: Option<std::collections::BTreeMap<i64, String>>,
    values: ColumnValues,
}

enum ColumnValues {
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
    String(Vec<Option<String>>),
    Date32(Vec<Option<i32>>),
}

impl BatchBuilder {
    pub fn new(node: &NodeEntity, schema: Arc<Schema>, batch_size: usize) -> Self {
        Self::with_seed(node, schema, batch_size, None)
    }

    pub fn with_seed(
        node: &NodeEntity,
        schema: Arc<Schema>,
        batch_size: usize,
        seed: Option<u64>,
    ) -> Self {
        let columns: Vec<ColumnData> = node
            .fields
            .iter()
            .filter(|field| field.name != "traversal_path")
            .map(|field| ColumnData {
                kind: FieldKind::classify(field),
                enum_values: field.enum_values.clone(),
                field: field.clone(),
                values: ColumnValues::new(&field.data_type),
            })
            .collect();

        let primary_keys: HashSet<String> = node.primary_keys.iter().cloned().collect();

        let fake_gen = match seed {
            Some(s) => FakeValueGenerator::with_seed(s),
            None => FakeValueGenerator::new(),
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

/// Arrow schema conversion utilities for ontology types.
pub mod arrow_schema {
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
    use ontology::{DataType, NodeEntity};

    /// Convert an ontology node entity to an Arrow schema.
    ///
    /// Prepends a `traversal_path` column, then maps each ontology field.
    pub fn node_to_arrow_schema(node: &NodeEntity) -> Schema {
        let mut fields = vec![ArrowField::new(
            "traversal_path",
            ArrowDataType::Utf8,
            false,
        )];
        for f in &node.fields {
            fields.push(ArrowField::new(
                &f.name,
                ontology_to_arrow_type(&f.data_type),
                f.nullable,
            ));
        }
        Schema::new(fields)
    }

    /// Fixed schema for edge records: traversal_path, relationship_kind,
    /// source_id, source_kind, target_id, target_kind.
    pub fn edge_schema() -> Schema {
        Schema::new(vec![
            ArrowField::new("traversal_path", ArrowDataType::Utf8, false),
            ArrowField::new("relationship_kind", ArrowDataType::Utf8, false),
            ArrowField::new("source_id", ArrowDataType::Int64, false),
            ArrowField::new("source_kind", ArrowDataType::Utf8, false),
            ArrowField::new("target_id", ArrowDataType::Int64, false),
            ArrowField::new("target_kind", ArrowDataType::Utf8, false),
        ])
    }

    /// Map ontology data type to Arrow data type.
    pub fn ontology_to_arrow_type(dt: &DataType) -> ArrowDataType {
        match dt {
            DataType::String | DataType::Enum | DataType::Uuid => ArrowDataType::Utf8,
            DataType::Int => ArrowDataType::Int64,
            DataType::Float => ArrowDataType::Float64,
            DataType::Bool => ArrowDataType::Boolean,
            DataType::Date => ArrowDataType::Date32,
            DataType::DateTime => ArrowDataType::Int64,
        }
    }

    /// Map an Arrow data type to its ClickHouse DDL type string.
    pub fn arrow_to_clickhouse_type(arrow_type: &ArrowDataType, nullable: bool) -> String {
        let base = match arrow_type {
            ArrowDataType::Int64 => "Int64",
            ArrowDataType::Float64 => "Float64",
            ArrowDataType::Utf8 => "String",
            ArrowDataType::Boolean => "Bool",
            ArrowDataType::Date32 => "Date",
            _ => "String",
        };
        if nullable {
            format!("Nullable({})", base)
        } else {
            base.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::arrow_schema::node_to_arrow_schema;

    fn test_node() -> NodeEntity {
        NodeEntity {
            name: "TestNode".to_string(),
            domain: "core".to_string(),
            description: "Test node entity".to_string(),
            label: "name".to_string(),
            fields: vec![
                Field {
                    name: "id".to_string(),
                    source: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                    enum_values: None,
                    enum_type: ontology::EnumType::default(),
                },
                Field {
                    name: "name".to_string(),
                    source: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    enum_values: None,
                    enum_type: ontology::EnumType::default(),
                },
                Field {
                    name: "active".to_string(),
                    source: "active".to_string(),
                    data_type: DataType::Bool,
                    nullable: false,
                    enum_values: None,
                    enum_type: ontology::EnumType::default(),
                },
            ],
            primary_keys: vec!["id".to_string()],
            destination_table: "gl_test_nodes".to_string(),
            style: Default::default(),
            etl: None,
            redaction: None,
        }
    }

    #[test]
    fn test_batch_builder_single_batch() {
        let node = test_node();
        let schema = Arc::new(node_to_arrow_schema(&node));
        let mut builder = BatchBuilder::new(&node, schema, 100);

        for i in 0..10 {
            builder.add_row(format!("1/{}/", i), i + 1);
        }

        let batches = builder.finish();
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 10);
        assert_eq!(batch.num_columns(), 4);

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
        let node = test_node();
        let schema = Arc::new(node_to_arrow_schema(&node));
        let mut builder = BatchBuilder::new(&node, schema, 5);

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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{
    BooleanBuilder, Date32Builder, Float64Builder, Int8Builder, Int64Builder, ListBuilder,
    RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Schema};

use super::fake_values::{ColumnKind, SiphonFakeValueGenerator, SiphonValue};

pub enum FieldStrategy<'a> {
    System(SiphonValue),
    Pool(&'a [serde_json::Value]),
    Generate(GenerateStrategy),
}

pub struct GenerateStrategy {
    data_type: DataType,
    nullable: bool,
    kind: ColumnKind,
    is_datetime: bool,
}

pub fn is_system_column(name: &str) -> bool {
    matches!(
        name,
        "_siphon_replicated_at" | "_siphon_deleted" | "version" | "deleted"
    )
}

pub fn system_column_value(name: &str, watermark_micros: i64) -> SiphonValue {
    match name {
        "_siphon_replicated_at" | "version" => SiphonValue::Int64(watermark_micros),
        "_siphon_deleted" | "deleted" => SiphonValue::Bool(false),
        _ => SiphonValue::Null,
    }
}

fn build_field_strategies<'a>(
    schema: &Schema,
    watermark_micros: i64,
    field_overrides: Option<&'a HashMap<String, Vec<serde_json::Value>>>,
) -> Vec<FieldStrategy<'a>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let field_name = field.name().as_str();

            if is_system_column(field_name) {
                return FieldStrategy::System(system_column_value(field_name, watermark_micros));
            }

            if let Some(pool) = field_overrides.and_then(|o| o.get(field_name))
                && !pool.is_empty()
            {
                return FieldStrategy::Pool(pool.as_slice());
            }

            let kind = ColumnKind::classify(field_name);
            let is_datetime = matches!(field.data_type(), DataType::Int64)
                && (kind == ColumnKind::DateTime
                    || field_name.ends_with("_at")
                    || field_name == "created_at"
                    || field_name == "updated_at");

            FieldStrategy::Generate(GenerateStrategy {
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                kind,
                is_datetime,
            })
        })
        .collect()
}

// Generates a value and appends it directly to the column builder.
// For string columns, this avoids allocating an intermediate String
// by writing the generator's internal buffer straight into the StringBuilder.
#[inline]
fn generate_and_append(
    generator: &mut SiphonFakeValueGenerator,
    strategy: &GenerateStrategy,
    column: &mut ColumnBuilder,
) {
    if strategy.data_type == DataType::Utf8
        && let ColumnBuilder::Utf8(builder) = column
    {
        generator.generate_string_into(strategy.kind, strategy.nullable, builder);
        return;
    }
    let value = generate_value(generator, strategy);
    column.append_value(&value);
}

#[inline]
pub fn generate_value(
    generator: &mut SiphonFakeValueGenerator,
    strategy: &GenerateStrategy,
) -> SiphonValue {
    match &strategy.data_type {
        DataType::Int64 => {
            if strategy.is_datetime {
                generator.generate_datetime64(strategy.nullable)
            } else {
                generator.generate_int64(strategy.kind, strategy.nullable)
            }
        }
        DataType::Int8 => generator.generate_int8(strategy.nullable),
        DataType::Utf8 => generator.generate_string(strategy.kind, strategy.nullable),
        DataType::Boolean => generator.generate_bool(strategy.nullable),
        DataType::Float64 => generator.generate_float64(strategy.nullable),
        DataType::Date32 => generator.generate_date32(strategy.nullable),
        DataType::List(_) => generator.generate_list_int64(strategy.nullable),
        _ => SiphonValue::Null,
    }
}

#[derive(Debug)]
pub enum ColumnBuilder {
    Int64(Int64Builder),
    Int8(Int8Builder),
    Utf8(StringBuilder),
    Boolean(BooleanBuilder),
    Float64(Float64Builder),
    Date32(Date32Builder),
    ListInt64(ListBuilder<Int64Builder>),
}

impl ColumnBuilder {
    pub fn new(data_type: DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Int64 => ColumnBuilder::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Int8 => ColumnBuilder::Int8(Int8Builder::with_capacity(capacity)),
            DataType::Utf8 => {
                ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 16))
            }
            DataType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Float64 => ColumnBuilder::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Date32 => ColumnBuilder::Date32(Date32Builder::with_capacity(capacity)),
            DataType::List(field) => ColumnBuilder::ListInt64(
                ListBuilder::new(Int64Builder::new()).with_field(field.clone()),
            ),
            _ => ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 16)),
        }
    }

    pub fn finish(self) -> Arc<dyn arrow::array::Array> {
        match self {
            ColumnBuilder::Int64(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Int8(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Utf8(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Boolean(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Date32(mut b) => Arc::new(b.finish()),
            ColumnBuilder::ListInt64(mut b) => Arc::new(b.finish()),
        }
    }

    pub fn append_value(&mut self, value: &SiphonValue) {
        match (self, value) {
            (ColumnBuilder::Int64(b), SiphonValue::Int64(v)) => b.append_value(*v),
            (ColumnBuilder::Int64(b), SiphonValue::DateTime64(v)) => b.append_value(*v),
            (ColumnBuilder::Int64(b), SiphonValue::Null) => b.append_null(),
            (ColumnBuilder::Int8(b), SiphonValue::Int8(v)) => b.append_value(*v),
            (ColumnBuilder::Int8(b), SiphonValue::Null) => b.append_null(),
            (ColumnBuilder::Utf8(b), SiphonValue::String(v)) => b.append_value(v),
            (ColumnBuilder::Utf8(b), SiphonValue::Null) => b.append_null(),
            (ColumnBuilder::Boolean(b), SiphonValue::Bool(v)) => b.append_value(*v),
            (ColumnBuilder::Boolean(b), SiphonValue::Null) => b.append_null(),
            (ColumnBuilder::Float64(b), SiphonValue::Float64(v)) => b.append_value(*v),
            (ColumnBuilder::Float64(b), SiphonValue::Null) => b.append_null(),
            (ColumnBuilder::Date32(b), SiphonValue::Date32(v)) => b.append_value(*v),
            (ColumnBuilder::Date32(b), SiphonValue::Null) => b.append_null(),
            (ColumnBuilder::ListInt64(b), SiphonValue::ListInt64(v)) => {
                let values = b.values();
                for item in v {
                    values.append_value(*item);
                }
                b.append(true);
            }
            (ColumnBuilder::ListInt64(b), SiphonValue::Null) => b.append(false),
            // Cross-type fallbacks
            (ColumnBuilder::Int64(b), SiphonValue::Bool(v)) => {
                b.append_value(if *v { 1 } else { 0 })
            }
            (ColumnBuilder::Int8(b), SiphonValue::Int64(v)) => b.append_value(*v as i8),
            (ColumnBuilder::Utf8(b), SiphonValue::Int64(v)) => b.append_value(v.to_string()),
            (ColumnBuilder::Int64(b), _) => b.append_null(),
            (ColumnBuilder::Int8(b), _) => b.append_null(),
            (ColumnBuilder::Utf8(b), _) => b.append_null(),
            (ColumnBuilder::Boolean(b), _) => b.append_null(),
            (ColumnBuilder::Float64(b), _) => b.append_null(),
            (ColumnBuilder::Date32(b), _) => b.append_null(),
            (ColumnBuilder::ListInt64(b), _) => b.append(false),
        }
    }
}

pub struct DirectBatchBuilder<'a> {
    schema: Arc<Schema>,
    columns: Vec<ColumnBuilder>,
    strategies: Vec<FieldStrategy<'a>>,
    field_name_to_index: HashMap<String, usize>,
    overridden: Vec<bool>,
}

impl<'a> DirectBatchBuilder<'a> {
    pub fn new(
        schema: Arc<Schema>,
        capacity: usize,
        watermark_micros: i64,
        field_overrides: Option<&'a HashMap<String, Vec<serde_json::Value>>>,
    ) -> Self {
        let field_count = schema.fields().len();

        let columns: Vec<ColumnBuilder> = schema
            .fields()
            .iter()
            .map(|field| ColumnBuilder::new(field.data_type().clone(), capacity))
            .collect();

        let strategies = build_field_strategies(&schema, watermark_micros, field_overrides);

        let field_name_to_index: HashMap<String, usize> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().clone(), i))
            .collect();

        Self {
            schema,
            columns,
            strategies,
            field_name_to_index,
            overridden: vec![false; field_count],
        }
    }

    pub fn set_int64(&mut self, name: &str, value: i64) {
        let idx = self.field_name_to_index[name];
        self.overridden[idx] = true;
        match &mut self.columns[idx] {
            ColumnBuilder::Int64(b) => b.append_value(value),
            other => panic!("expected Int64 column for '{name}', got {other:?}"),
        }
    }

    pub fn set_string(&mut self, name: &str, value: &str) {
        let idx = self.field_name_to_index[name];
        self.overridden[idx] = true;
        match &mut self.columns[idx] {
            ColumnBuilder::Utf8(b) => b.append_value(value),
            other => panic!("expected Utf8 column for '{name}', got {other:?}"),
        }
    }

    pub fn set_bool(&mut self, name: &str, value: bool) {
        let idx = self.field_name_to_index[name];
        self.overridden[idx] = true;
        match &mut self.columns[idx] {
            ColumnBuilder::Boolean(b) => b.append_value(value),
            other => panic!("expected Boolean column for '{name}', got {other:?}"),
        }
    }

    pub fn fill_unset_fields(&mut self, generator: &mut SiphonFakeValueGenerator) {
        for (field_index, strategy) in self.strategies.iter().enumerate() {
            if self.overridden[field_index] {
                self.overridden[field_index] = false;
                continue;
            }

            let column = &mut self.columns[field_index];
            match strategy {
                FieldStrategy::System(value) => {
                    column.append_value(value);
                }
                FieldStrategy::Pool(pool) => {
                    let value = generator.pick_from_pool(pool);
                    column.append_value(&value);
                }
                FieldStrategy::Generate(strat) => {
                    generate_and_append(generator, strat, column);
                }
            }
        }
    }

    pub fn finish_and_reset(&mut self, next_capacity: usize) -> Result<RecordBatch> {
        let new_columns: Vec<ColumnBuilder> = self
            .schema
            .fields()
            .iter()
            .map(|field| ColumnBuilder::new(field.data_type().clone(), next_capacity))
            .collect();
        let old_columns = std::mem::replace(&mut self.columns, new_columns);

        let mut arrow_columns: Vec<Arc<dyn arrow::array::Array>> =
            Vec::with_capacity(old_columns.len());
        for column in old_columns {
            arrow_columns.push(column.finish());
        }
        Ok(RecordBatch::try_new(
            Arc::clone(&self.schema),
            arrow_columns,
        )?)
    }

    pub fn finish(self) -> Result<RecordBatch> {
        let mut arrow_columns: Vec<Arc<dyn arrow::array::Array>> =
            Vec::with_capacity(self.columns.len());
        for column in self.columns {
            arrow_columns.push(column.finish());
        }
        Ok(RecordBatch::try_new(self.schema, arrow_columns)?)
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.field_name_to_index.get(name).copied()
    }

    pub fn set_int64_by_index(&mut self, index: usize, value: i64) {
        self.overridden[index] = true;
        match &mut self.columns[index] {
            ColumnBuilder::Int64(b) => b.append_value(value),
            other => panic!("expected Int64 column at index {index}, got {other:?}"),
        }
    }

    pub fn set_string_by_index(&mut self, index: usize, value: &str) {
        self.overridden[index] = true;
        match &mut self.columns[index] {
            ColumnBuilder::Utf8(b) => b.append_value(value),
            other => panic!("expected Utf8 column at index {index}, got {other:?}"),
        }
    }
}

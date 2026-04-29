//! Arrow array utilities: extraction helpers and RecordBatch builder.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, Int64Builder, LargeStringArray, ListArray, PrimitiveArray,
    StringArray, StringBuilder, StructArray, TimestampMicrosecondArray,
    TimestampMicrosecondBuilder, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone, PartialEq, enum_as_inner::EnumAsInner)]
pub enum ColumnValue {
    Int64(i64),
    Float64(f64),
    String(String),
    Null,
}

/// Types that can be extracted from a [`ColumnValue`], with fallback
/// parsing from the string representation. Useful because ClickHouse
/// hydration (`toJSONString(map(...))`) stringifies all values.
pub trait FromColumnValue: Sized {
    fn from_column_value(v: &ColumnValue) -> Option<Self>;
}

/// Implement [`FromColumnValue`] for a type. Tries the native accessor
/// first, then falls back to parsing from the string variant.
macro_rules! impl_coerce {
    // Numeric: try native variant, then parse from string
    ($ty:ty, native: $accessor:ident) => {
        impl FromColumnValue for $ty {
            fn from_column_value(v: &ColumnValue) -> Option<Self> {
                v.$accessor()
                    .copied()
                    .or_else(|| v.as_string().and_then(|s| s.parse().ok()))
            }
        }
    };
    // String-only: extract or parse from string variant
    ($ty:ty, from_str: $parse:expr) => {
        impl FromColumnValue for $ty {
            fn from_column_value(v: &ColumnValue) -> Option<Self> {
                v.as_string().and_then($parse)
            }
        }
    };
}

impl_coerce!(i64, native: as_int64);
impl_coerce!(f64, native: as_float64);
impl_coerce!(String, from_str: |s| Some(s.clone()));
impl_coerce!(bool, from_str: |s| match s.trim().to_ascii_lowercase().as_str() {
    "true" | "1" => Some(true),
    "false" | "0" => Some(false),
    _ => None,
});

impl ColumnValue {
    /// Extract as the requested type, parsing from string if needed.
    ///
    /// ```
    /// # use gkg_utils::arrow::ColumnValue;
    /// assert_eq!(ColumnValue::Int64(42).coerce::<i64>(), Some(42));
    /// assert_eq!(ColumnValue::String("42".into()).coerce::<i64>(), Some(42));
    /// assert_eq!(ColumnValue::String("hello".into()).coerce::<i64>(), None);
    /// assert_eq!(ColumnValue::String("hello".into()).coerce::<String>(), Some("hello".into()));
    /// ```
    pub fn coerce<T: FromColumnValue>(&self) -> Option<T> {
        T::from_column_value(self)
    }
}

impl From<serde_json::Value> for ColumnValue {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::String(s) => Self::String(s),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Self::Int64(i)
                } else if let Some(f) = n.as_f64() {
                    Self::Float64(f)
                } else {
                    Self::String(n.to_string())
                }
            }
            serde_json::Value::Bool(b) => Self::String(b.to_string()),
            serde_json::Value::Null => Self::Null,
            other => Self::String(other.to_string()),
        }
    }
}

/// Stateless helper for extracting typed values from Arrow [`RecordBatch`]es.
///
/// All methods are associated functions — no instance required.
pub struct ArrowUtils;

impl ArrowUtils {
    /// Extract every column value from a single row of a [`RecordBatch`],
    /// keyed by the field name as it appears in the schema.
    pub fn extract_row(batch: &RecordBatch, row_idx: usize) -> HashMap<String, ColumnValue> {
        let schema = batch.schema();
        let mut map = HashMap::with_capacity(schema.fields().len());
        for (col_idx, field) in schema.fields().iter().enumerate() {
            map.insert(
                field.name().clone(),
                Self::extract_value(batch.column(col_idx).as_ref(), row_idx),
            );
        }
        map
    }

    /// Look up a column by name and return its primitive value at the given row,
    /// or `None` if the column is missing, cannot be downcast, or is null.
    pub fn get_column<T: ArrowPrimitiveType>(
        batch: &RecordBatch,
        col_name: &str,
        row: usize,
    ) -> Option<T::Native> {
        let idx = batch.schema().index_of(col_name).ok()?;
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()?;
        if arr.is_null(row) {
            return None;
        }
        Some(arr.value(row))
    }

    /// Look up a column by name and return its `String` value at the given row,
    /// or `None` if the column is missing, not a `StringArray`, or null.
    pub fn get_column_string(batch: &RecordBatch, col_name: &str, row: usize) -> Option<String> {
        let idx = batch.schema().index_of(col_name).ok()?;
        let arr = batch.column(idx).as_any().downcast_ref::<StringArray>()?;
        if arr.is_null(row) {
            return None;
        }
        Some(arr.value(row).to_string())
    }

    /// Look up a `List<String>` column by name and collect its non-null elements
    /// at the given row. Returns an empty vec if the column is missing, not a
    /// `ListArray`, null at this row, or contains a non-`StringArray` inner type.
    pub fn get_string_list(batch: &RecordBatch, col_name: &str, row: usize) -> Vec<String> {
        let Some(idx) = batch.schema().index_of(col_name).ok() else {
            return Vec::new();
        };
        let Some(list) = batch.column(idx).as_any().downcast_ref::<ListArray>() else {
            return Vec::new();
        };
        if list.is_null(row) {
            return Vec::new();
        }
        let values = list.value(row);
        let Some(arr) = values.as_any().downcast_ref::<StringArray>() else {
            return Vec::new();
        };
        (0..arr.len())
            .filter(|&i| !arr.is_null(i))
            .map(|i| arr.value(i).to_string())
            .collect()
    }

    /// Look up a `List<Struct<Int64, String>>` column by name and collect its
    /// non-null `(i64, String)` pairs at the given row. Returns an empty vec if the
    /// column is missing, the list is null, or the inner struct doesn't have the
    /// expected layout (at least two columns: `Int64Array` then `StringArray`).
    pub fn get_i64_string_pairs(
        batch: &RecordBatch,
        col_name: &str,
        row: usize,
    ) -> Vec<(i64, String)> {
        let Some(idx) = batch.schema().index_of(col_name).ok() else {
            return Vec::new();
        };
        let Some(list) = batch.column(idx).as_any().downcast_ref::<ListArray>() else {
            return Vec::new();
        };
        if list.is_null(row) {
            return Vec::new();
        }
        let values = list.value(row);
        let Some(structs) = values.as_any().downcast_ref::<StructArray>() else {
            return Vec::new();
        };
        if structs.num_columns() < 2 {
            return Vec::new();
        }
        let Some(ids) = structs.column(0).as_any().downcast_ref::<Int64Array>() else {
            return Vec::new();
        };
        let Some(types) = structs.column(1).as_any().downcast_ref::<StringArray>() else {
            return Vec::new();
        };
        (0..ids.len())
            .filter(|&i| !ids.is_null(i) && !types.is_null(i))
            .map(|i| (ids.value(i), types.value(i).to_string()))
            .collect()
    }

    /// Downcast a column by positional index to the requested Arrow array type.
    ///
    /// Returns `None` if the index is out of range or the column cannot be
    /// downcast to `A`.
    pub fn get_column_by_index<A: 'static>(batch: &RecordBatch, col: usize) -> Option<&A> {
        batch.column(col).as_any().downcast_ref::<A>()
    }

    /// Downcast a column by name to the requested Arrow array type.
    ///
    /// Returns `None` if the column is missing or cannot be downcast to `A`.
    pub fn get_column_by_name<'a, A: 'static>(batch: &'a RecordBatch, name: &str) -> Option<&'a A> {
        batch.column_by_name(name)?.as_any().downcast_ref::<A>()
    }

    /// Convert a single Arrow array cell to its string representation.
    ///
    /// Covers all integer widths (Int8–Int64, UInt8–UInt64), Utf8,
    /// LargeUtf8, Float64, Boolean, and all timestamp precisions.
    /// Returns `None` for null cells or unsupported types.
    pub fn array_value_to_string(array: &dyn Array, row: usize) -> Option<String> {
        match Self::extract_value(array, row) {
            ColumnValue::Int64(v) => Some(v.to_string()),
            ColumnValue::Float64(v) => Some(v.to_string()),
            ColumnValue::String(v) => Some(v),
            ColumnValue::Null => None,
        }
    }

    /// Extract a typed `ColumnValue` from an Arrow array at the given row index.
    pub fn extract_value(array: &dyn Array, idx: usize) -> ColumnValue {
        macro_rules! downcast {
            ($arr_ty:ty, $val:ident => $expr:expr) => {
                if let Some(arr) = array.as_any().downcast_ref::<$arr_ty>() {
                    let $val = arr.value(idx);
                    return $expr;
                }
            };
        }

        if array.is_null(idx) {
            return ColumnValue::Null;
        }

        downcast!(Int8Array, v => ColumnValue::Int64(i64::from(v)));
        downcast!(Int16Array, v => ColumnValue::Int64(i64::from(v)));
        downcast!(Int32Array, v => ColumnValue::Int64(i64::from(v)));
        downcast!(Int64Array, v => ColumnValue::Int64(v));
        downcast!(UInt8Array, v => ColumnValue::Int64(i64::from(v)));
        downcast!(UInt16Array, v => ColumnValue::Int64(i64::from(v)));
        downcast!(UInt32Array, v => ColumnValue::Int64(i64::from(v)));
        downcast!(UInt64Array, v => ColumnValue::Int64(i64::try_from(v).unwrap_or(i64::MAX)));
        downcast!(StringArray, v => ColumnValue::String(v.to_string()));
        downcast!(LargeStringArray, v => ColumnValue::String(v.to_string()));
        downcast!(Float64Array, v => ColumnValue::Float64(v));
        downcast!(BooleanArray, v => ColumnValue::String(v.to_string()));

        if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
            return timestamp_to_string(arr.value_as_datetime(idx));
        }
        if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
            return timestamp_to_string(arr.value_as_datetime(idx));
        }
        if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            return timestamp_to_string(arr.value_as_datetime(idx));
        }
        if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
            return timestamp_to_string(arr.value_as_datetime(idx));
        }

        ColumnValue::Null
    }
}

// ── RecordBatch builder ──────────────────────────────────────────────────────

/// Column type for [`NodeBatch`] builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Str,
    /// Dictionary-encoded string. Uses integer indices internally,
    /// stores each unique value once. Ideal for low-cardinality columns
    /// like edge_kind, definition_type, language.
    DictStr,
    Int,
    Bool,
    /// Microsecond-precision UTC timestamp.
    TimestampMicros,
    /// `Array(String)` — variable-length list of strings.
    StrList,
}

/// Column definition for [`NodeBatch`] builder.
#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub col_type: ColumnType,
    pub nullable: bool,
}

enum Col {
    Str(StringBuilder, bool),
    DictStr(
        arrow::array::StringDictionaryBuilder<arrow::datatypes::Int32Type>,
        bool,
    ),
    Int(Int64Builder, bool),
    Bool(BooleanBuilder, bool),
    Timestamp(TimestampMicrosecondBuilder, bool),
    StrList(arrow::array::ListBuilder<StringBuilder>, bool),
}

impl std::fmt::Debug for Col {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Col::{}", self.kind())
    }
}

impl Col {
    fn len(&self) -> usize {
        match self {
            Self::Str(b, _) => b.len(),
            Self::DictStr(b, _) => b.len(),
            Self::Int(b, _) => b.len(),
            Self::Bool(b, _) => b.len(),
            Self::Timestamp(b, _) => b.len(),
            Self::StrList(b, _) => b.len(),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Str(..) => "Str",
            Self::DictStr(..) => "DictStr",
            Self::Int(..) => "Int",
            Self::Bool(..) => "Bool",
            Self::Timestamp(..) => "Timestamp",
            Self::StrList(..) => "StrList",
        }
    }

    fn finish(self) -> (DataType, bool, ArrayRef) {
        match self {
            Self::Str(mut b, nullable) => (DataType::Utf8, nullable, Arc::new(b.finish())),
            Self::DictStr(mut b, nullable) => (
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                nullable,
                Arc::new(b.finish()),
            ),
            Self::Int(mut b, nullable) => (DataType::Int64, nullable, Arc::new(b.finish())),
            Self::Bool(mut b, nullable) => (DataType::Boolean, nullable, Arc::new(b.finish())),
            Self::Timestamp(mut b, nullable) => (
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
                nullable,
                Arc::new(b.finish().with_timezone("UTC")),
            ),
            Self::StrList(mut b, nullable) => {
                let arr = b.finish();
                (arr.data_type().clone(), nullable, Arc::new(arr))
            }
        }
    }
}

/// A mutable handle to a single column builder. Returned by [`BatchBuilder::col`].
///
/// Returns `Err` if you call the wrong push variant for the column type.
pub struct ColRef<'a> {
    name: &'a str,
    col: &'a mut Col,
}

type BatchResult<T> = std::result::Result<T, arrow::error::ArrowError>;

fn batch_err(msg: impl Into<String>) -> arrow::error::ArrowError {
    arrow::error::ArrowError::InvalidArgumentError(msg.into())
}

/// Error for nodes without assigned IDs passed to [`AsRecordBatch::write_row`].
pub fn missing_id(node_type: &str) -> arrow::error::ArrowError {
    batch_err(format!("{} has no assigned ID", node_type))
}

impl ColRef<'_> {
    pub fn push_str(&mut self, v: impl AsRef<str>) -> BatchResult<()> {
        match &mut *self.col {
            Col::Str(b, _) => {
                b.append_value(v);
                Ok(())
            }
            Col::DictStr(b, _) => {
                b.append_value(v);
                Ok(())
            }
            other => Err(batch_err(format!(
                "push_str on {} column '{}'",
                other.kind(),
                self.name
            ))),
        }
    }

    pub fn push_int(&mut self, v: i64) -> BatchResult<()> {
        match &mut *self.col {
            Col::Int(b, _) => {
                b.append_value(v);
                Ok(())
            }
            other => Err(batch_err(format!(
                "push_int on {} column '{}'",
                other.kind(),
                self.name
            ))),
        }
    }

    pub fn push_bool(&mut self, v: bool) -> BatchResult<()> {
        match &mut *self.col {
            Col::Bool(b, _) => {
                b.append_value(v);
                Ok(())
            }
            other => Err(batch_err(format!(
                "push_bool on {} column '{}'",
                other.kind(),
                self.name
            ))),
        }
    }

    pub fn push_timestamp_micros(&mut self, v: i64) -> BatchResult<()> {
        match &mut *self.col {
            Col::Timestamp(b, _) => {
                b.append_value(v);
                Ok(())
            }
            other => Err(batch_err(format!(
                "push_timestamp_micros on {} column '{}'",
                other.kind(),
                self.name
            ))),
        }
    }

    /// Push an empty `[]` to a `StrList` column.
    pub fn push_empty_str_list(&mut self) -> BatchResult<()> {
        match &mut *self.col {
            Col::StrList(b, _) => {
                b.append(true);
                Ok(())
            }
            other => Err(batch_err(format!(
                "push_empty_str_list on {} column '{}'",
                other.kind(),
                self.name
            ))),
        }
    }

    pub fn push_opt_str<S: AsRef<str>>(&mut self, v: Option<S>) -> BatchResult<()> {
        match &mut *self.col {
            Col::Str(b, _) => {
                match v {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
                Ok(())
            }
            Col::DictStr(b, _) => {
                match v {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
                Ok(())
            }
            other => Err(batch_err(format!(
                "push_opt_str on {} column '{}'",
                other.kind(),
                self.name
            ))),
        }
    }
}

/// Schema-driven Arrow RecordBatch builder.
///
/// All columns are declared via `&[ColumnSpec]`. The builder manages typed
/// column builders and produces a RecordBatch. No opinions about which
/// columns exist or their order -- the caller controls the full schema.
///
/// ```ignore
/// use gkg_utils::arrow::{BatchBuilder, ColumnSpec, ColumnType};
///
/// let specs = vec![
///     ColumnSpec { name: "id".into(), col_type: ColumnType::Int, nullable: false },
///     ColumnSpec { name: "path".into(), col_type: ColumnType::Str, nullable: false },
///     ColumnSpec { name: "name".into(), col_type: ColumnType::Str, nullable: false },
/// ];
/// let batch = BatchBuilder::new(&specs, nodes.len())?
///     .build(&nodes, |n, b| {
///         b.col("id")?.push_int(n.id)?;
///         b.col("path")?.push_str(&n.path)?;
///         b.col("name")?.push_str(&n.name)?;
///         Ok(())
///     })?;
/// ```
#[derive(Debug)]
pub struct BatchBuilder {
    names: Vec<String>,
    cols: Vec<Col>,
    index: HashMap<String, usize>,
}

impl BatchBuilder {
    /// Create a builder from column specs, pre-allocating for `cap` rows.
    pub fn new(specs: &[ColumnSpec], cap: usize) -> BatchResult<Self> {
        let mut names = Vec::with_capacity(specs.len());
        let mut cols = Vec::with_capacity(specs.len());
        let mut index = HashMap::with_capacity(specs.len());

        for spec in specs {
            if index.contains_key(&spec.name) {
                return Err(batch_err(format!("duplicate column name '{}'", spec.name)));
            }
            let col = match spec.col_type {
                ColumnType::Int => Col::Int(Int64Builder::with_capacity(cap), spec.nullable),
                ColumnType::Str => {
                    Col::Str(StringBuilder::with_capacity(cap, cap * 8), spec.nullable)
                }
                ColumnType::DictStr => Col::DictStr(
                    arrow::array::StringDictionaryBuilder::<arrow::datatypes::Int32Type>::new(),
                    spec.nullable,
                ),
                ColumnType::Bool => Col::Bool(BooleanBuilder::with_capacity(cap), spec.nullable),
                ColumnType::TimestampMicros => Col::Timestamp(
                    TimestampMicrosecondBuilder::with_capacity(cap),
                    spec.nullable,
                ),
                ColumnType::StrList => Col::StrList(
                    arrow::array::ListBuilder::new(StringBuilder::with_capacity(cap, cap * 8)),
                    spec.nullable,
                ),
            };
            index.insert(spec.name.clone(), names.len());
            names.push(spec.name.clone());
            cols.push(col);
        }

        Ok(Self { names, cols, index })
    }

    /// Get a mutable column handle by name.
    pub fn col(&mut self, name: &str) -> BatchResult<ColRef<'_>> {
        let idx = *self.index.get(name).ok_or_else(|| {
            batch_err(format!(
                "column '{name}' not found; available: {:?}",
                self.names
            ))
        })?;
        Ok(ColRef {
            name: &self.names[idx],
            col: &mut self.cols[idx],
        })
    }

    /// Iterate over items, fill columns via the closure, and produce a
    /// RecordBatch.
    ///
    /// Every call to `fill` must push exactly one value to each column.
    pub fn build<T>(
        mut self,
        items: &[T],
        fill: impl Fn(&T, &mut Self) -> BatchResult<()>,
    ) -> BatchResult<RecordBatch> {
        for item in items {
            fill(item, &mut self)?;
        }

        // Validate all columns have the same length.
        if let Some(expected) = self.cols.first().map(Col::len) {
            for (i, col) in self.cols.iter().enumerate().skip(1) {
                let actual = col.len();
                if actual != expected {
                    return Err(batch_err(format!(
                        "column '{}' has {} rows but '{}' has {}",
                        self.names[i], actual, self.names[0], expected,
                    )));
                }
            }
        }

        let mut fields = Vec::with_capacity(self.names.len());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.cols.len());

        for (name, col) in self.names.into_iter().zip(self.cols) {
            let (dtype, nullable, array) = col.finish();
            fields.push(Field::new(name, dtype, nullable));
            columns.push(array);
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }
}

// ── AsRecordBatch trait ──────────────────────────────────────────────────────

/// Types that can serialize a slice of themselves into an Arrow
/// [`RecordBatch`] via [`BatchBuilder`].
///
/// The `Ctx` type parameter carries pipeline-level values (e.g.
/// project ID, branch) that aren't part of the domain struct.
/// Defaults to `()` for types that don't need external context.
///
/// Nodes without assigned IDs should return `false` from
/// [`should_include`](Self::should_include) and will be filtered out
/// before building the batch.
///
/// ```ignore
/// let specs = ontology.local_entity_specs("File");
/// let batch = FileNode::to_record_batch(&nodes, &specs, &ctx)?;
/// ```
/// Trait for row envelope columns that surround entity-specific data.
/// Each consumer (DuckDB, ClickHouse) implements this with their own
/// header columns (id, project_id, branch, traversal_path, _version, etc).
///
/// Row types call `ctx.write_header(b, id)` to emit the envelope,
/// then write their own entity columns.
pub trait RowEnvelope {
    /// Write envelope columns for a node row.
    fn write_header(&self, b: &mut BatchBuilder, id: i64) -> BatchResult<()>;

    /// Column specs for the envelope (prepended to entity specs).
    fn header_specs(&self) -> Vec<ColumnSpec>;
}

pub trait AsRecordBatch<Ctx = ()>: Sized {
    /// Whether this item should be included in the batch.
    /// Default: always include.
    fn should_include(&self) -> bool {
        true
    }

    /// Write one row into `builder`. Must push exactly one value to
    /// every column declared in the specs that were used to create the
    /// builder. Returns `Err` if a referenced column is missing.
    fn write_row(&self, builder: &mut BatchBuilder, ctx: &Ctx) -> BatchResult<()>;

    /// Build a [`RecordBatch`] from a slice of items using the given
    /// column specs, filtering out any where
    /// [`should_include`](Self::should_include) returns `false`.
    fn to_record_batch(
        items: &[Self],
        specs: &[ColumnSpec],
        ctx: &Ctx,
    ) -> BatchResult<RecordBatch> {
        let included: Vec<&Self> = items.iter().filter(|i| i.should_include()).collect();
        BatchBuilder::new(specs, included.len())?.build(&included, |item, b| {
            item.write_row(b, ctx)?;
            Ok(())
        })
    }
}

fn timestamp_to_string(dt: Option<chrono::NaiveDateTime>) -> ColumnValue {
    dt.map(|d| ColumnValue::String(d.format("%Y-%m-%dT%H:%M:%SZ").to_string()))
        .unwrap_or(ColumnValue::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Int64Builder, ListBuilder, StringBuilder, StructBuilder, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, Field, Int64Type, Schema, UInt64Type};
    use std::sync::Arc;

    // ── coerce tests ─────────────────────────────────────────────────

    #[test]
    fn coerce_i64_from_int64() {
        assert_eq!(ColumnValue::Int64(42).coerce::<i64>(), Some(42));
    }

    #[test]
    fn coerce_i64_from_string() {
        assert_eq!(ColumnValue::String("42".into()).coerce::<i64>(), Some(42));
    }

    #[test]
    fn coerce_i64_from_bad_string() {
        assert_eq!(ColumnValue::String("abc".into()).coerce::<i64>(), None);
    }

    #[test]
    fn coerce_i64_from_null() {
        assert_eq!(ColumnValue::Null.coerce::<i64>(), None);
    }

    #[test]
    fn coerce_f64_from_float64() {
        assert_eq!(ColumnValue::Float64(2.72).coerce::<f64>(), Some(2.72));
    }

    #[test]
    fn coerce_f64_from_string() {
        assert_eq!(
            ColumnValue::String("2.72".into()).coerce::<f64>(),
            Some(2.72)
        );
    }

    #[test]
    fn coerce_string_from_string() {
        assert_eq!(
            ColumnValue::String("hello".into()).coerce::<String>(),
            Some("hello".into())
        );
    }

    #[test]
    fn coerce_string_from_int64() {
        assert_eq!(ColumnValue::Int64(42).coerce::<String>(), None);
    }

    #[test]
    fn coerce_string_from_null() {
        assert_eq!(ColumnValue::Null.coerce::<String>(), None);
    }

    #[test]
    fn coerce_bool_from_string_true() {
        assert_eq!(
            ColumnValue::String("true".into()).coerce::<bool>(),
            Some(true)
        );
        assert_eq!(ColumnValue::String("1".into()).coerce::<bool>(), Some(true));
    }

    #[test]
    fn coerce_bool_from_string_false() {
        assert_eq!(
            ColumnValue::String("false".into()).coerce::<bool>(),
            Some(false)
        );
        assert_eq!(
            ColumnValue::String("0".into()).coerce::<bool>(),
            Some(false)
        );
    }

    #[test]
    fn coerce_bool_from_bad_string() {
        assert_eq!(ColumnValue::String("yes".into()).coerce::<bool>(), None);
    }

    // ── arrow extraction tests ──────────────────────────────────────

    fn make_batch(columns: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<Arc<dyn Array>> = columns.into_iter().map(|(_, arr)| arr).collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    /// Build a `List<Struct<Int64, Utf8>>` column with the given rows.
    /// Each row is a slice of `(i64, &str)` pairs; `None` produces a null list entry.
    fn make_i64_string_list(rows: &[Option<&[(i64, &str)]>]) -> ListArray {
        let fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ];
        let mut builder = ListBuilder::new(StructBuilder::new(
            fields,
            vec![
                Box::new(Int64Builder::new()),
                Box::new(StringBuilder::new()),
            ],
        ));
        for row in rows {
            match row {
                Some(pairs) => {
                    for &(id, s) in *pairs {
                        builder
                            .values()
                            .field_builder::<Int64Builder>(0)
                            .unwrap()
                            .append_value(id);
                        builder
                            .values()
                            .field_builder::<StringBuilder>(1)
                            .unwrap()
                            .append_value(s);
                        builder.values().append(true);
                    }
                    builder.append(true);
                }
                None => builder.append(false),
            }
        }
        builder.finish()
    }

    fn assert_ts(arr: Arc<dyn Array>) {
        let batch = make_batch(vec![("ts", arr)]);
        assert_eq!(
            ArrowUtils::extract_row(&batch, 0).get("ts"),
            Some(&ColumnValue::String("2024-01-01T00:00:00Z".to_string())),
        );
    }

    // -- ColumnValue enum --

    #[test]
    fn column_value_accessors() {
        let i = ColumnValue::Int64(42);
        assert_eq!(i.as_int64().copied(), Some(42));
        assert!(i.as_string().is_none());

        let s = ColumnValue::String("hello".into());
        assert_eq!(s.as_string().map(|s| s.as_str()), Some("hello"));
        assert!(s.as_int64().is_none());

        let n = ColumnValue::Null;
        assert!(n.as_int64().is_none());
        assert!(n.as_string().is_none());
    }

    #[test]
    fn column_value_equality() {
        assert_eq!(ColumnValue::Int64(1), ColumnValue::Int64(1));
        assert_ne!(ColumnValue::Int64(1), ColumnValue::Int64(2));
        assert_eq!(
            ColumnValue::String("a".into()),
            ColumnValue::String("a".into())
        );
        assert_ne!(ColumnValue::Null, ColumnValue::Int64(0));
    }

    // -- extract_value / extract_row --

    #[test]
    fn extract_row_returns_all_columns() {
        let batch = make_batch(vec![
            ("id", Arc::new(Int64Array::from(vec![1]))),
            ("name", Arc::new(StringArray::from(vec!["alice"]))),
        ]);
        let row = ArrowUtils::extract_row(&batch, 0);
        assert_eq!(row.len(), 2);
        assert_eq!(row.get("id"), Some(&ColumnValue::Int64(1)));
        assert_eq!(row.get("name"), Some(&ColumnValue::String("alice".into())));
    }

    #[test]
    fn extract_uint64_as_int64() {
        let batch = make_batch(vec![(
            "n",
            Arc::new(UInt64Array::from(vec![100u64, 200, 300])),
        )]);
        for (i, expected) in [100, 200, 300].iter().enumerate() {
            assert_eq!(
                ArrowUtils::extract_row(&batch, i).get("n"),
                Some(&ColumnValue::Int64(*expected)),
            );
        }
    }

    #[test]
    fn extract_uint64_overflow_clamps_to_max() {
        let batch = make_batch(vec![("big", Arc::new(UInt64Array::from(vec![u64::MAX])))]);
        assert_eq!(
            ArrowUtils::extract_row(&batch, 0).get("big"),
            Some(&ColumnValue::Int64(i64::MAX)),
        );
    }

    #[test]
    fn extract_all_timestamp_precisions() {
        // 2024-01-01T00:00:00Z at each resolution
        assert_ts(Arc::new(TimestampSecondArray::new(
            vec![1_704_067_200].into(),
            None,
        )));
        assert_ts(Arc::new(TimestampMillisecondArray::new(
            vec![1_704_067_200_000].into(),
            None,
        )));
        assert_ts(Arc::new(TimestampMicrosecondArray::new(
            vec![1_704_067_200_000_000].into(),
            None,
        )));
        assert_ts(Arc::new(TimestampNanosecondArray::new(
            vec![1_704_067_200_000_000_000].into(),
            None,
        )));
    }

    #[test]
    fn extract_null_timestamp_returns_null() {
        let arr: TimestampSecondArray = vec![Some(1_704_067_200i64), None].into_iter().collect();
        let batch = make_batch(vec![("ts", Arc::new(arr))]);
        assert_eq!(
            ArrowUtils::extract_row(&batch, 0).get("ts"),
            Some(&ColumnValue::String("2024-01-01T00:00:00Z".into())),
        );
        assert_eq!(
            ArrowUtils::extract_row(&batch, 1).get("ts"),
            Some(&ColumnValue::Null)
        );
    }

    // -- typed column getters --

    #[test]
    fn get_column_i64_and_string() {
        let batch = make_batch(vec![
            ("id", Arc::new(Int64Array::from(vec![42]))),
            ("name", Arc::new(StringArray::from(vec!["bob"]))),
        ]);
        assert_eq!(
            ArrowUtils::get_column::<Int64Type>(&batch, "id", 0),
            Some(42)
        );
        assert_eq!(
            ArrowUtils::get_column_string(&batch, "name", 0),
            Some("bob".into())
        );
        assert_eq!(
            ArrowUtils::get_column::<Int64Type>(&batch, "missing", 0),
            None
        );
        assert_eq!(ArrowUtils::get_column_string(&batch, "missing", 0), None);
    }

    #[test]
    fn get_column_null_returns_none() {
        let batch = make_batch(vec![(
            "id",
            Arc::new(Int64Array::from(vec![Option::<i64>::None])),
        )]);
        assert_eq!(ArrowUtils::get_column::<Int64Type>(&batch, "id", 0), None);
    }

    // -- list column getters --

    #[test]
    fn get_string_list_returns_values() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("a");
        builder.values().append_value("b");
        builder.append(true);

        let batch = make_batch(vec![("tags", Arc::new(builder.finish()))]);
        assert_eq!(
            ArrowUtils::get_string_list(&batch, "tags", 0),
            vec!["a", "b"]
        );
        assert!(ArrowUtils::get_string_list(&batch, "missing", 0).is_empty());
    }

    #[test]
    fn get_i64_string_pairs_returns_pairs() {
        let list = make_i64_string_list(&[Some(&[(10, "User"), (20, "Project")]), None]);
        let batch = make_batch(vec![("path", Arc::new(list))]);

        assert_eq!(
            ArrowUtils::get_i64_string_pairs(&batch, "path", 0),
            vec![(10, "User".into()), (20, "Project".into())],
        );
        assert!(ArrowUtils::get_i64_string_pairs(&batch, "path", 1).is_empty());
        assert!(ArrowUtils::get_i64_string_pairs(&batch, "missing", 0).is_empty());
    }

    // -- get_column_by_index --

    #[test]
    fn get_column_by_index_returns_typed_ref() {
        let batch = make_batch(vec![
            ("a", Arc::new(Int64Array::from(vec![7]))),
            ("b", Arc::new(StringArray::from(vec!["x"]))),
        ]);
        let col: &Int64Array = ArrowUtils::get_column_by_index(&batch, 0).unwrap();
        assert_eq!(col.value(0), 7);
        let col: &StringArray = ArrowUtils::get_column_by_index(&batch, 1).unwrap();
        assert_eq!(col.value(0), "x");
        assert!(ArrowUtils::get_column_by_index::<UInt64Array>(&batch, 0).is_none());
    }

    // -- get_column (uint64) --

    #[test]
    fn get_column_uint64_returns_value() {
        let batch = make_batch(vec![("n", Arc::new(UInt64Array::from(vec![42u64])))]);
        assert_eq!(
            ArrowUtils::get_column::<UInt64Type>(&batch, "n", 0),
            Some(42)
        );
        assert_eq!(
            ArrowUtils::get_column::<UInt64Type>(&batch, "missing", 0),
            None
        );
    }

    #[test]
    fn get_column_uint64_null_returns_none() {
        let batch = make_batch(vec![(
            "n",
            Arc::new(UInt64Array::from(vec![Option::<u64>::None])),
        )]);
        assert_eq!(ArrowUtils::get_column::<UInt64Type>(&batch, "n", 0), None);
    }

    // -- extract_value with small integer widths --

    #[test]
    fn extract_small_integer_widths() {
        let batch = make_batch(vec![
            ("i8", Arc::new(Int8Array::from(vec![i8::MIN]))),
            ("i16", Arc::new(Int16Array::from(vec![1000i16]))),
            ("i32", Arc::new(Int32Array::from(vec![100_000i32]))),
            ("u8", Arc::new(UInt8Array::from(vec![255u8]))),
            ("u16", Arc::new(UInt16Array::from(vec![60_000u16]))),
            ("u32", Arc::new(UInt32Array::from(vec![4_000_000_000u32]))),
        ]);
        let row = ArrowUtils::extract_row(&batch, 0);
        assert_eq!(row.get("i8"), Some(&ColumnValue::Int64(i64::from(i8::MIN))));
        assert_eq!(row.get("i16"), Some(&ColumnValue::Int64(1000)));
        assert_eq!(row.get("i32"), Some(&ColumnValue::Int64(100_000)));
        assert_eq!(row.get("u8"), Some(&ColumnValue::Int64(255)));
        assert_eq!(row.get("u16"), Some(&ColumnValue::Int64(60_000)));
        assert_eq!(row.get("u32"), Some(&ColumnValue::Int64(4_000_000_000)));
    }

    #[test]
    fn extract_large_string() {
        let batch = make_batch(vec![(
            "ls",
            Arc::new(LargeStringArray::from(vec!["large"])),
        )]);
        assert_eq!(
            ArrowUtils::extract_row(&batch, 0).get("ls"),
            Some(&ColumnValue::String("large".into())),
        );
    }

    // -- array_value_to_string --

    #[test]
    fn array_value_to_string_returns_formatted() {
        let arr = Int64Array::from(vec![42]);
        assert_eq!(
            ArrowUtils::array_value_to_string(&arr, 0),
            Some("42".to_string()),
        );

        let arr = StringArray::from(vec!["hello"]);
        assert_eq!(
            ArrowUtils::array_value_to_string(&arr, 0),
            Some("hello".to_string()),
        );

        let arr = UInt8Array::from(vec![255u8]);
        assert_eq!(
            ArrowUtils::array_value_to_string(&arr, 0),
            Some("255".to_string()),
        );
    }

    #[test]
    fn array_value_to_string_null_returns_none() {
        let arr = Int64Array::from(vec![Option::<i64>::None]);
        assert_eq!(ArrowUtils::array_value_to_string(&arr, 0), None);
    }

    // ── BatchBuilder tests ──────────────────────────────────────────

    fn test_specs() -> Vec<ColumnSpec> {
        vec![
            ColumnSpec {
                name: "id".into(),
                col_type: ColumnType::Int,
                nullable: false,
            },
            ColumnSpec {
                name: "name".into(),
                col_type: ColumnType::Str,
                nullable: false,
            },
        ]
    }

    #[test]
    fn batch_builder_produces_correct_schema() {
        let batch = BatchBuilder::new(&test_specs(), 2)
            .unwrap()
            .build(&[(1i64, "alice"), (2, "bob")], |item, b| {
                b.col("id")?.push_int(item.0)?;
                b.col("name")?.push_str(item.1)?;
                Ok(())
            })
            .unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name"]);
    }

    #[test]
    fn batch_builder_empty_items() {
        let items: Vec<(i64, &str)> = vec![];
        let batch = BatchBuilder::new(&test_specs(), 0)
            .unwrap()
            .build(&items, |item, b| {
                b.col("id")?.push_int(item.0)?;
                b.col("name")?.push_str(item.1)?;
                Ok(())
            })
            .unwrap();

        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn batch_builder_nullable_column() {
        let specs = vec![ColumnSpec {
            name: "val".into(),
            col_type: ColumnType::Str,
            nullable: true,
        }];
        let batch = BatchBuilder::new(&specs, 2)
            .unwrap()
            .build(&[Some("hello"), None], |item, b| {
                b.col("val")?.push_opt_str(*item)?;
                Ok(())
            })
            .unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert!(batch.schema().field(0).is_nullable());
    }

    #[test]
    fn batch_builder_duplicate_column_name_errors() {
        let specs = vec![
            ColumnSpec {
                name: "x".into(),
                col_type: ColumnType::Int,
                nullable: false,
            },
            ColumnSpec {
                name: "x".into(),
                col_type: ColumnType::Str,
                nullable: false,
            },
        ];
        let err = BatchBuilder::new(&specs, 0).unwrap_err();
        assert!(err.to_string().contains("duplicate column name 'x'"));
    }

    #[test]
    fn batch_builder_unknown_column_errors() {
        let items = vec![1i64];
        let err = BatchBuilder::new(&test_specs(), 1)
            .unwrap()
            .build(&items, |_, b| {
                b.col("nonexistent")?.push_int(1)?;
                Ok(())
            })
            .unwrap_err();
        assert!(err.to_string().contains("column 'nonexistent' not found"));
    }

    #[test]
    fn batch_builder_type_mismatch_push_str_on_int() {
        let items = vec![1i64];
        let err = BatchBuilder::new(&test_specs(), 1)
            .unwrap()
            .build(&items, |_, b| {
                b.col("id")?.push_str("oops")?;
                Ok(())
            })
            .unwrap_err();
        assert!(err.to_string().contains("push_str on Int column 'id'"));
    }

    #[test]
    fn batch_builder_type_mismatch_push_int_on_str() {
        let items = vec![1i64];
        let err = BatchBuilder::new(&test_specs(), 1)
            .unwrap()
            .build(&items, |_, b| {
                b.col("name")?.push_int(42)?;
                Ok(())
            })
            .unwrap_err();
        assert!(err.to_string().contains("push_int on Str column 'name'"));
    }

    #[test]
    fn batch_builder_column_length_mismatch_errors() {
        let items = vec![1i64, 2];
        let err = BatchBuilder::new(&test_specs(), 2)
            .unwrap()
            .build(&items, |item, b| {
                b.col("id")?.push_int(*item)?;
                // deliberately skip "name" on second row
                if *item == 1 {
                    b.col("name")?.push_str("alice")?;
                }
                Ok(())
            })
            .unwrap_err();
        assert!(err.to_string().contains("has 1 rows but"));
    }

    #[test]
    fn batch_builder_preserves_column_order() {
        let specs = vec![
            ColumnSpec {
                name: "c".into(),
                col_type: ColumnType::Str,
                nullable: false,
            },
            ColumnSpec {
                name: "a".into(),
                col_type: ColumnType::Int,
                nullable: false,
            },
            ColumnSpec {
                name: "b".into(),
                col_type: ColumnType::Str,
                nullable: false,
            },
        ];
        let batch = BatchBuilder::new(&specs, 1)
            .unwrap()
            .build(&[()], |_, b| {
                b.col("c")?.push_str("x")?;
                b.col("a")?.push_int(1)?;
                b.col("b")?.push_str("y")?;
                Ok(())
            })
            .unwrap();

        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["c", "a", "b"]);
    }
}

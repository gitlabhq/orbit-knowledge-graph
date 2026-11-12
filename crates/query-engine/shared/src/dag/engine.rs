//! Runs an engine plan over the rows earlier stages returned: hash joins on typed keys, then
//! order, result columns and the page window.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt32Array, new_null_array};
use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take};
use arrow::datatypes::{DataType as ArrowType, Field, Schema};
use arrow::record_batch::RecordBatch;
use compiler::dag::StageId;
use compiler::orbit_authorization::Key;
use query_data_model::DataType;
use query_data_model::plan::engine::{EngineOperation, EnginePlan};
use query_data_model::plan::logical::Order;
use query_data_model::plan::tracking::{NodeId, Ref};

use super::executor::ExecutorError;
use super::rows::concat;

struct Table {
    columns: Vec<(Ref, ArrayRef)>,
    rows: usize,
}

impl Table {
    fn array(&self, output: Ref) -> Result<&ArrayRef, ExecutorError> {
        self.columns
            .iter()
            .find(|(declared, _)| *declared == output)
            .map(|(_, array)| array)
            .ok_or_else(|| ExecutorError("an engine operation reads an unknown column".into()))
    }

    fn take(&self, indices: &UInt32Array) -> Result<Self, ExecutorError> {
        let columns = self
            .columns
            .iter()
            .map(|(output, array)| Ok((*output, take(array, indices, None).map_err(failed)?)))
            .collect::<Result<_, ExecutorError>>()?;
        Ok(Self {
            columns,
            rows: indices.len(),
        })
    }
}

fn failed(error: arrow::error::ArrowError) -> ExecutorError {
    ExecutorError(error.to_string())
}

/// The key of one row across join columns, each read as its declared type; a null part matches
/// nothing.
fn join_key(
    arrays: &[(&ArrayRef, DataType)],
    row: usize,
) -> Result<Option<Vec<Key>>, ExecutorError> {
    arrays
        .iter()
        .map(|(array, data_type)| {
            super::rows::scalar(array, row, compiler::KeyKind::of(*data_type))
        })
        .collect()
}

/// Text values cut to `chars` characters, each cut one followed by `suffix`.
fn excerpt(array: &ArrayRef, chars: usize, suffix: &str) -> Result<ArrayRef, ExecutorError> {
    let text = arrow::compute::cast(array, &ArrowType::Utf8).map_err(failed)?;
    let text = text
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| ExecutorError("an excerpt reads a text column".into()))?;
    let cut: arrow::array::StringArray = text
        .iter()
        .map(|value| {
            value.map(|value| match value.char_indices().nth(chars) {
                Some((end, _)) => format!("{}{suffix}", &value[..end]),
                None => value.to_string(),
            })
        })
        .collect();
    Ok(Arc::new(cut))
}

pub fn run(
    plan: &EnginePlan,
    stages: &HashMap<StageId, Vec<RecordBatch>>,
) -> Result<RecordBatch, ExecutorError> {
    let mut tables: HashMap<NodeId, Table> = HashMap::new();
    for node in plan.nodes() {
        let inputs = plan.inputs(node);
        let input = |index: usize| {
            inputs
                .get(index)
                .and_then(|node| tables.get(node))
                .ok_or_else(|| ExecutorError("an engine operation lacks its input".into()))
        };
        let table = match plan.operation(node) {
            EngineOperation::Rows { stage } => {
                let batch = concat(stages.get(stage).map_or(&[][..], Vec::as_slice))?;
                let columns = plan
                    .outputs(node)
                    .map(|output| {
                        let name = plan.name(output).unwrap_or_default();
                        let column = name.split_once('.').map_or(name, |(_, column)| column);
                        let array = match &batch {
                            Some(batch) => {
                                batch.column_by_name(column).cloned().ok_or_else(|| {
                                    ExecutorError(format!(
                                        "stage {stage} returned no column {column}"
                                    ))
                                })?
                            }
                            None => new_null_array(&ArrowType::Null, 0),
                        };
                        Ok((output, array))
                    })
                    .collect::<Result<_, ExecutorError>>()?;
                Table {
                    columns,
                    rows: batch.map_or(0, |batch| batch.num_rows()),
                }
            }
            EngineOperation::Join { on } => {
                let (left, right) = (input(0)?, input(1)?);
                let near = on
                    .iter()
                    .map(|(left_key, _, data_type)| Ok((left.array(*left_key)?, *data_type)))
                    .collect::<Result<Vec<_>, ExecutorError>>()?;
                let far = on
                    .iter()
                    .map(|(_, right_key, data_type)| Ok((right.array(*right_key)?, *data_type)))
                    .collect::<Result<Vec<_>, ExecutorError>>()?;
                let mut built: HashMap<Vec<Key>, Vec<u32>> = HashMap::new();
                for row in 0..right.rows {
                    if let Some(key) = join_key(&far, row)? {
                        built.entry(key).or_default().push(row as u32);
                    }
                }
                let (mut lefts, mut rights) = (Vec::new(), Vec::new());
                for row in 0..left.rows {
                    let Some(key) = join_key(&near, row)? else {
                        continue;
                    };
                    for matched in built.get(&key).into_iter().flatten() {
                        lefts.push(row as u32);
                        rights.push(*matched);
                    }
                }
                let left = left.take(&UInt32Array::from(lefts))?;
                let right = right.take(&UInt32Array::from(rights))?;
                Table {
                    rows: left.rows,
                    columns: left.columns.into_iter().chain(right.columns).collect(),
                }
            }
            EngineOperation::Order(keys) => {
                let table = input(0)?;
                let sort = keys
                    .iter()
                    .map(|(key, order)| {
                        Ok(SortColumn {
                            values: table.array(*key)?.clone(),
                            options: Some(SortOptions {
                                descending: *order == Order::Descending,
                                nulls_first: false,
                            }),
                        })
                    })
                    .collect::<Result<Vec<_>, ExecutorError>>()?;
                table.take(&lexsort_to_indices(&sort, None).map_err(failed)?)?
            }
            EngineOperation::Limit { rows, skip } => {
                let table = input(0)?;
                let start = (*skip as usize).min(table.rows);
                let end = start.saturating_add(*rows as usize).min(table.rows);
                table.take(&UInt32Array::from_iter_values(start as u32..end as u32))?
            }
            EngineOperation::Tag(tags) => {
                let table = input(0)?;
                let outputs: Vec<Ref> = plan.outputs(node).collect();
                let declared = &outputs[outputs.len() - tags.len()..];
                let mut columns = table.columns.clone();
                for (output, (_, value)) in declared.iter().zip(tags) {
                    let constant: ArrayRef = Arc::new(arrow::array::StringArray::from(vec![
                        value.as_str();
                        table.rows
                    ]));
                    columns.push((*output, constant));
                }
                Table {
                    columns,
                    rows: table.rows,
                }
            }
            EngineOperation::Excerpt {
                columns,
                chars,
                suffix,
            } => {
                let table = input(0)?;
                let cut = table
                    .columns
                    .iter()
                    .map(|(output, array)| match columns.contains(output) {
                        true => Ok((*output, excerpt(array, *chars as usize, suffix)?)),
                        false => Ok((*output, array.clone())),
                    })
                    .collect::<Result<_, ExecutorError>>()?;
                Table {
                    columns: cut,
                    rows: table.rows,
                }
            }
            EngineOperation::Select(selected) => {
                let table = input(0)?;
                let columns = plan
                    .outputs(node)
                    .zip(selected)
                    .map(|(output, source)| Ok((output, table.array(*source)?.clone())))
                    .collect::<Result<_, ExecutorError>>()?;
                Table {
                    columns,
                    rows: table.rows,
                }
            }
        };
        tables.insert(node, table);
    }
    let root = tables
        .remove(&plan.root())
        .ok_or_else(|| ExecutorError("the engine plan has no root".into()))?;
    let fields: Vec<Field> = root
        .columns
        .iter()
        .map(|(output, array)| {
            Field::new(
                plan.name(*output).unwrap_or_default(),
                array.data_type().clone(),
                true,
            )
        })
        .collect();
    let arrays = root.columns.into_iter().map(|(_, array)| array).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(failed)
}

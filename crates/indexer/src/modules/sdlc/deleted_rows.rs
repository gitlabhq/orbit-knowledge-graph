//! Splits transformed pages into live rows to INSERT and lightweight
//! `DELETE FROM` statements for rows the datalake marked `_deleted`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, LargeStringArray, StringArray};
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::handler::HandlerError;
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};

const MAX_KEYS_PER_DELETE: usize = 10_000;
const DELETE_MAX_EXECUTION_TIME_SECS: u64 = 600;

#[derive(Debug)]
pub(super) struct SplitBatches {
    pub live: Vec<RecordBatch>,
    pub delete_statements: Vec<String>,
}

pub(super) struct DeletedRowSplitter {
    sort_keys_by_table: HashMap<String, Vec<String>>,
}

impl DeletedRowSplitter {
    pub fn from_ontology(ontology: &ontology::Ontology) -> Self {
        let mut sort_keys_by_table = HashMap::new();
        for node in ontology.nodes() {
            let prefixed = prefixed_table_name(&node.destination_table, *SCHEMA_VERSION);
            let sort_key = ontology
                .sort_key_for_table(&node.destination_table)
                .unwrap_or(&node.sort_key)
                .to_vec();
            sort_keys_by_table.insert(prefixed, sort_key);
        }
        for edge_table in ontology.edge_tables() {
            if let Some(config) = ontology.edge_table_config(edge_table) {
                let prefixed = prefixed_table_name(edge_table, *SCHEMA_VERSION);
                sort_keys_by_table.insert(prefixed, config.sort_key.clone());
            }
        }
        Self { sort_keys_by_table }
    }

    /// Tables without a known sort key pass through untouched: their deleted
    /// rows keep flowing as `_deleted = true` inserts rather than vanishing.
    pub fn split(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<SplitBatches, HandlerError> {
        let Some(sort_key) = self.sort_keys_by_table.get(table) else {
            return Ok(SplitBatches {
                live: batches,
                delete_statements: Vec::new(),
            });
        };

        let mut live = Vec::with_capacity(batches.len());
        let mut deleted_key_tuples = Vec::new();

        for batch in batches {
            let Some(deleted) = deleted_column(&batch) else {
                live.push(batch);
                continue;
            };
            if deleted.true_count() == 0 {
                live.push(batch);
                continue;
            }

            let live_mask = compute::not(deleted).expect("boolean NOT cannot fail");
            let live_batch = compute::filter_record_batch(&batch, &live_mask)
                .expect("filter with same-length mask cannot fail");
            if live_batch.num_rows() > 0 {
                live.push(live_batch);
            }

            let deleted_batch = compute::filter_record_batch(&batch, deleted)
                .expect("filter with same-length mask cannot fail");
            let key_columns = sort_key_columns(&deleted_batch, sort_key)?;
            for row in 0..deleted_batch.num_rows() {
                deleted_key_tuples.push(key_tuple_literal(&key_columns, row)?);
            }
        }

        let delete_statements = deleted_key_tuples
            .chunks(MAX_KEYS_PER_DELETE)
            .map(|chunk| build_delete_statement(table, sort_key, chunk))
            .collect();

        Ok(SplitBatches {
            live,
            delete_statements,
        })
    }
}

fn deleted_column(batch: &RecordBatch) -> Option<&BooleanArray> {
    batch
        .column_by_name(ontology::DELETED_COLUMN)?
        .as_any()
        .downcast_ref::<BooleanArray>()
}

fn build_delete_statement(table: &str, sort_key: &[String], tuples: &[String]) -> String {
    format!(
        "DELETE FROM {table} WHERE ({}) IN ({}) \
         SETTINGS lightweight_deletes_sync = 0, \
         max_execution_time = {DELETE_MAX_EXECUTION_TIME_SECS}",
        sort_key.join(", "),
        tuples.join(", "),
    )
}

/// Resolves each sort key column to a plain array, casting dictionary-encoded
/// columns (how transforms emit low-cardinality kinds) to their value type.
fn sort_key_columns(
    batch: &RecordBatch,
    sort_key: &[String],
) -> Result<Vec<ArrayRef>, HandlerError> {
    sort_key
        .iter()
        .map(|column| {
            let array = batch.column_by_name(column).ok_or_else(|| {
                HandlerError::Processing(format!("sort key column '{column}' missing from batch"))
            })?;
            let plain = match array.data_type() {
                DataType::Dictionary(_, value_type) => {
                    compute::cast(array, value_type).map_err(|e| {
                        HandlerError::Processing(format!(
                            "failed to cast dictionary sort key column '{column}': {e}"
                        ))
                    })?
                }
                _ => Arc::clone(array),
            };
            match plain.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Int64 => Ok(plain),
                other => Err(HandlerError::Processing(format!(
                    "unsupported sort key type {other:?} for column '{column}'"
                ))),
            }
        })
        .collect()
}

fn key_tuple_literal(key_columns: &[ArrayRef], row: usize) -> Result<String, HandlerError> {
    let values = key_columns
        .iter()
        .map(|column| sql_literal(column, row))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("({})", values.join(", ")))
}

fn sql_literal(array: &ArrayRef, row: usize) -> Result<String, HandlerError> {
    let downcast_failed = || {
        HandlerError::Processing(format!(
            "sort key column does not match its declared type {:?}",
            array.data_type()
        ))
    };
    match array.data_type() {
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(downcast_failed)?;
            Ok(quote_string_literal(array.value(row)))
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(downcast_failed)?;
            Ok(quote_string_literal(array.value(row)))
        }
        DataType::Int64 => {
            let array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(downcast_failed)?;
            Ok(array.value(row).to_string())
        }
        other => Err(HandlerError::Processing(format!(
            "unsupported sort key type {other:?}"
        ))),
    }
}

fn quote_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn splitter_for(table: &str, sort_key: &[&str]) -> DeletedRowSplitter {
        DeletedRowSplitter {
            sort_keys_by_table: HashMap::from([(
                table.to_string(),
                sort_key.iter().map(|s| s.to_string()).collect(),
            )]),
        }
    }

    fn batch(paths: &[&str], ids: &[i64], deleted: &[bool]) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
            Field::new(ontology::DELETED_COLUMN, DataType::Boolean, false),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(paths.to_vec())),
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(BooleanArray::from(deleted.to_vec())),
            ],
        )
        .expect("valid batch")
    }

    #[test]
    fn unknown_table_passes_batches_through() {
        let splitter = splitter_for("known", &["id"]);
        let batches = vec![batch(&["1/"], &[1], &[true])];

        let split = splitter.split("unknown", batches).unwrap();

        assert_eq!(split.live.len(), 1);
        assert_eq!(split.live[0].num_rows(), 1);
        assert!(split.delete_statements.is_empty());
    }

    #[test]
    fn batch_without_deleted_column_passes_through() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let no_deleted_column = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let splitter = splitter_for("t", &["id"]);

        let split = splitter.split("t", vec![no_deleted_column]).unwrap();

        assert_eq!(split.live.len(), 1);
        assert!(split.delete_statements.is_empty());
    }

    #[test]
    fn all_live_batch_passes_through_unfiltered() {
        let splitter = splitter_for("t", &["traversal_path", "id"]);
        let batches = vec![batch(&["1/", "1/"], &[1, 2], &[false, false])];

        let split = splitter.split("t", batches).unwrap();

        assert_eq!(split.live.len(), 1);
        assert_eq!(split.live[0].num_rows(), 2);
        assert!(split.delete_statements.is_empty());
    }

    #[test]
    fn mixed_batch_splits_live_rows_and_builds_one_delete() {
        let splitter = splitter_for("v99_gl_project", &["traversal_path", "id"]);
        let batches = vec![batch(&["1/", "1/", "2/"], &[1, 2, 3], &[false, true, true])];

        let split = splitter.split("v99_gl_project", batches).unwrap();

        assert_eq!(split.live.len(), 1);
        assert_eq!(split.live[0].num_rows(), 1);
        assert_eq!(
            split.delete_statements,
            vec![
                "DELETE FROM v99_gl_project \
                 WHERE (traversal_path, id) IN (('1/', 2), ('2/', 3)) \
                 SETTINGS lightweight_deletes_sync = 0, max_execution_time = 600"
                    .to_string()
            ]
        );
    }

    #[test]
    fn deleted_keys_chunk_into_multiple_statements() {
        let rows = MAX_KEYS_PER_DELETE + 1;
        let paths = vec!["1/"; rows];
        let ids: Vec<i64> = (0..rows as i64).collect();
        let deleted = vec![true; rows];
        let splitter = splitter_for("t", &["id"]);

        let split = splitter
            .split("t", vec![batch(&paths, &ids, &deleted)])
            .unwrap();

        assert!(split.live.is_empty());
        assert_eq!(split.delete_statements.len(), 2);
        assert!(split.delete_statements[1].contains(&format!("({})", rows - 1)));
    }

    #[test]
    fn string_literals_escape_backslashes_and_quotes() {
        assert_eq!(quote_string_literal(r"a\"), r"'a\\'");
        assert_eq!(quote_string_literal("a'b"), r"'a\'b'");
        assert_eq!(quote_string_literal(r"a\'b"), r"'a\\\'b'");
    }

    #[test]
    fn dictionary_encoded_sort_key_columns_cast_to_their_value_type() {
        use arrow::array::{Array, DictionaryArray};
        use arrow::datatypes::Int32Type;

        let kinds: DictionaryArray<Int32Type> = vec!["HAS_ITEM", "MENTIONS"].into_iter().collect();
        let schema = Schema::new(vec![
            Field::new("relationship_kind", kinds.data_type().clone(), false),
            Field::new("source_id", DataType::Int64, false),
            Field::new(ontology::DELETED_COLUMN, DataType::Boolean, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(kinds),
                Arc::new(Int64Array::from(vec![7_i64, 8])),
                Arc::new(BooleanArray::from(vec![true, true])),
            ],
        )
        .unwrap();
        let splitter = splitter_for("t", &["relationship_kind", "source_id"]);

        let split = splitter.split("t", vec![batch]).unwrap();

        assert!(split.live.is_empty());
        assert!(
            split.delete_statements[0].contains("(('HAS_ITEM', 7), ('MENTIONS', 8))"),
            "{}",
            split.delete_statements[0]
        );
    }

    #[test]
    fn unsupported_sort_key_type_is_an_error() {
        let schema = Schema::new(vec![
            Field::new("score", DataType::Float64, false),
            Field::new(ontology::DELETED_COLUMN, DataType::Boolean, false),
        ]);
        let float_keyed = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Float64Array::from(vec![1.5])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
        )
        .unwrap();
        let splitter = splitter_for("t", &["score"]);

        let error = splitter.split("t", vec![float_keyed]).unwrap_err();

        assert!(error.to_string().contains("unsupported sort key type"));
    }

    #[test]
    fn missing_sort_key_column_is_an_error() {
        let splitter = splitter_for("t", &["not_a_column"]);

        let error = splitter
            .split("t", vec![batch(&["1/"], &[1], &[true])])
            .unwrap_err();

        assert!(error.to_string().contains("not_a_column"));
    }

    #[test]
    fn from_ontology_maps_prefixed_node_and_edge_tables() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let splitter = DeletedRowSplitter::from_ontology(&ontology);

        let project = prefixed_table_name("gl_project", *SCHEMA_VERSION);
        let edge = prefixed_table_name(ontology.edge_table(), *SCHEMA_VERSION);
        assert!(!splitter.sort_keys_by_table[&project].is_empty());
        assert!(!splitter.sort_keys_by_table[&edge].is_empty());
    }
}

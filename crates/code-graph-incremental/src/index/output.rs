use arrow::{
    array::UInt32Array, datatypes::Int64Type, error::ArrowError, record_batch::RecordBatch,
};
use orbit_utils::arrow::ArrowUtils;
use rustc_hash::FxHashSet;

use super::Outcome;

#[derive(Default)]
pub(super) struct Output {
    directories: FxHashSet<i64>,
}

impl Output {
    pub fn select(
        &mut self,
        mut batches: Vec<(String, RecordBatch)>,
        ontology: &ontology::Ontology,
        owns_file: impl Fn(&str) -> bool,
        outcome: &mut Outcome,
    ) -> Result<Vec<(String, RecordBatch)>, ArrowError> {
        let table = |kind| {
            ontology
                .table_name(kind)
                .map_err(|error| ArrowError::SchemaError(error.to_string()))
        };
        let directories = table("Directory")?;
        let files = table("File")?;
        let definitions = table("Definition")?;
        let imports = table("ImportedSymbol")?;
        let mut emitted = FxHashSet::default();
        for (table, batch) in &mut batches {
            if table == directories || table == files {
                let mut rows = Vec::new();
                for row in 0..batch.num_rows() {
                    let id = ArrowUtils::get_column::<Int64Type>(batch, "id", row)
                        .ok_or_else(|| ArrowError::SchemaError("missing entity id".into()))?;
                    let keep = if table == directories {
                        self.directories.insert(id)
                    } else {
                        let path = ArrowUtils::get_column_string(batch, "path", row)
                            .ok_or_else(|| ArrowError::SchemaError("missing file path".into()))?;
                        owns_file(&path)
                    };
                    if keep {
                        rows.push(row as u32);
                        emitted.insert(id);
                    }
                }
                *batch = arrow::compute::take_record_batch(batch, &UInt32Array::from(rows))?;
            }
            if table == directories {
                outcome.directories += batch.num_rows();
            } else if table == files {
                outcome.files += batch.num_rows();
            } else if table == definitions {
                outcome.definitions += batch.num_rows();
            } else if table == imports {
                outcome.imports += batch.num_rows();
            }
        }
        for (table, batch) in &mut batches {
            if table != ontology.edge_table() {
                continue;
            }
            let mut rows = Vec::new();
            for row in 0..batch.num_rows() {
                let kind = ArrowUtils::get_column_string(batch, "relationship_kind", row)
                    .ok_or_else(|| ArrowError::SchemaError("missing relationship kind".into()))?;
                let target = ArrowUtils::get_column::<Int64Type>(batch, "target_id", row)
                    .ok_or_else(|| ArrowError::SchemaError("missing target id".into()))?;
                if kind != "CONTAINS" || emitted.contains(&target) {
                    rows.push(row as u32);
                }
            }
            *batch = arrow::compute::take_record_batch(batch, &UInt32Array::from(rows))?;
            outcome.edges += batch.num_rows();
        }
        Ok(batches)
    }
}

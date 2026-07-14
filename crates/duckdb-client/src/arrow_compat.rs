//! RecordBatch conversion between the workspace arrow version and the older
//! one duckdb requires, via an arrow IPC round-trip (same pattern as the
//! `arrow_56` alias in integration-tests-codegraph). `arrow_58` resolves to
//! the exact crate version duckdb depends on, so its types unify with the
//! duckdb API. Delete this module once duckdb-rs catches up to the workspace
//! arrow major.

use std::io::Cursor;

use arrow::array::RecordBatch;

use crate::error::{DuckDbError, Result};

pub(crate) fn to_duck(batch: &RecordBatch) -> Result<arrow_58::record_batch::RecordBatch> {
    let mut buf = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, batch.schema_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    let mut reader = arrow_58::ipc::reader::StreamReader::try_new(Cursor::new(buf), None)
        .map_err(|e| DuckDbError::Conversion(e.to_string()))?;
    reader
        .next()
        .transpose()
        .map_err(|e| DuckDbError::Conversion(e.to_string()))?
        .ok_or_else(|| DuckDbError::Conversion("empty IPC stream".into()))
}

pub(crate) fn from_duck(batch: &arrow_58::record_batch::RecordBatch) -> Result<RecordBatch> {
    let mut buf = Vec::new();
    let mut writer =
        arrow_58::ipc::writer::StreamWriter::try_new(&mut buf, batch.schema_ref().as_ref())
            .map_err(|e| DuckDbError::Conversion(e.to_string()))?;
    writer
        .write(batch)
        .map_err(|e| DuckDbError::Conversion(e.to_string()))?;
    writer
        .finish()
        .map_err(|e| DuckDbError::Conversion(e.to_string()))?;
    let mut reader = arrow::ipc::reader::StreamReader::try_new(Cursor::new(buf), None)?;
    reader
        .next()
        .transpose()?
        .ok_or_else(|| DuckDbError::Conversion("empty IPC stream".into()))
}

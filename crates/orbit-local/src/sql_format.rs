use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use std::io::Write;

#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum Format {
    #[default]
    Table,
    Json,
    Ndjson,
    Csv,
}

pub fn write<W: Write>(out: W, format: Format, batches: &[RecordBatch]) -> Result<()> {
    match format {
        Format::Table => write_table(out, batches),
        Format::Json => write_json(out, batches),
        Format::Ndjson => write_ndjson(out, batches),
        Format::Csv => write_csv(out, batches),
    }
}

pub fn write_table<W: Write>(mut out: W, batches: &[RecordBatch]) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let pretty =
        arrow::util::pretty::pretty_format_batches(batches).context("failed to format batches")?;
    writeln!(out, "{pretty}").context("failed to write output")
}

pub fn write_json<W: Write>(out: W, batches: &[RecordBatch]) -> Result<()> {
    let refs: Vec<&RecordBatch> = batches.iter().collect();
    let mut writer = arrow::json::ArrayWriter::new(out);
    writer
        .write_batches(&refs)
        .context("failed to write JSON")?;
    writer.finish().context("failed to finish JSON")?;
    let mut out = writer.into_inner();
    writeln!(out).context("failed to write newline")
}

pub fn write_ndjson<W: Write>(out: W, batches: &[RecordBatch]) -> Result<()> {
    let refs: Vec<&RecordBatch> = batches.iter().collect();
    let mut writer = arrow::json::LineDelimitedWriter::new(out);
    writer
        .write_batches(&refs)
        .context("failed to write NDJSON")?;
    writer.finish().context("failed to finish NDJSON")
}

pub fn write_csv<W: Write>(out: W, batches: &[RecordBatch]) -> Result<()> {
    let mut writer = arrow::csv::WriterBuilder::new()
        .with_header(true)
        .build(out);
    for batch in batches {
        writer.write(batch).context("failed to write CSV row")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("alice"), None])),
                Arc::new(BooleanArray::from(vec![true, false])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn table_renders_columns_and_rows() {
        let mut out = Vec::new();
        write_table(&mut out, &[sample_batch()]).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("id") && s.contains("name") && s.contains("active"));
        assert!(s.contains("alice") && s.contains("true") && s.contains("false"));
    }

    #[test]
    fn json_emits_array_of_objects() {
        let mut out = Vec::new();
        write_json(&mut out, &[sample_batch()]).unwrap();
        let s = String::from_utf8(out).unwrap();
        let v: serde_json::Value = serde_json::from_str(s.trim()).unwrap();
        let rows = v.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["name"], "alice");
        assert!(rows[1].get("name").is_none() || rows[1]["name"].is_null());
    }

    #[test]
    fn ndjson_emits_one_object_per_line() {
        let mut out = Vec::new();
        write_ndjson(&mut out, &[sample_batch()]).unwrap();
        let s = String::from_utf8(out).unwrap();
        let lines: Vec<_> = s.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 2);
        for line in lines {
            serde_json::from_str::<serde_json::Value>(line).unwrap();
        }
    }

    #[test]
    fn csv_emits_header_and_rows() {
        let mut out = Vec::new();
        write_csv(&mut out, &[sample_batch()]).unwrap();
        let s = String::from_utf8(out).unwrap();
        let mut lines = s.lines();
        assert_eq!(lines.next().unwrap(), "id,name,active");
        assert_eq!(lines.next().unwrap(), "1,alice,true");
        assert_eq!(lines.next().unwrap(), "2,,false");
    }

    #[test]
    fn table_empty_batches_is_noop() {
        let mut out = Vec::new();
        write_table(&mut out, &[]).unwrap();
        assert!(out.is_empty());
    }

    // `orbit list` depends on this for parseable empty output.
    #[test]
    fn json_empty_batches_emit_empty_array() {
        let mut out = Vec::new();
        write_json(&mut out, &[]).unwrap();
        let s = String::from_utf8(out).unwrap();
        let v: serde_json::Value = serde_json::from_str(s.trim()).unwrap();
        assert_eq!(v.as_array().map(Vec::len), Some(0));
    }
}

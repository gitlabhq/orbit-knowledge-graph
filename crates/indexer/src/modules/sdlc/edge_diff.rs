use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Int64Array, StringArray, TimestampMicrosecondArray, downcast_array,
};
use arrow::compute::{cast, filter_record_batch, interleave_record_batch};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use futures::{StreamExt, TryStreamExt, stream};
use indexmap::IndexSet;
use ontology::{EdgeMapping, NodeEntity, NodeRef, NodeRefKind, Ontology, OntologyError};
use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};

use super::plan::{Transformation, fk_edge_key_transform};
use super::transform::BlockTransform;
use crate::clickhouse::{ArrowClickHouseClient, ArrowQuery};
use crate::handler::HandlerError;

const STORED_ROW_READ_CHUNK: usize = 10_000;

const VERSION: DataType = DataType::Timestamp(TimeUnit::Microsecond, None);

const EDGE_KEY: [(&str, DataType); 7] = [
    ("traversal_path", DataType::Utf8),
    ("relationship_kind", DataType::Utf8),
    ("source_id", DataType::Int64),
    ("source_kind", DataType::Utf8),
    ("target_id", DataType::Int64),
    ("target_kind", DataType::Utf8),
    ("_version", VERSION),
];

#[derive(Clone)]
pub(in crate::modules::sdlc) struct EdgeDiffClient {
    graph: ArrowClickHouseClient,
    max_concurrent_reads: usize,
}

impl EdgeDiffClient {
    pub(in crate::modules::sdlc) fn new(
        graph: ArrowClickHouseClient,
        max_concurrent_reads: usize,
    ) -> Self {
        Self {
            graph,
            max_concurrent_reads: max_concurrent_reads.max(1),
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct EdgeDiff {
    node_table: String,
    columns: IndexSet<StoredColumn>,
    edge_keys: Vec<Transformation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StoredColumn {
    stored: String,
    source: String,
}

type RowKey = (String, i64);
type StoredRow = (usize, usize);

pub(in crate::modules::sdlc) struct PageRows(BTreeMap<RowKey, i64>);

impl EdgeDiff {
    pub(in crate::modules::sdlc) fn for_node(
        node: &NodeEntity,
        mappings: &[EdgeMapping],
        ontology: &Ontology,
    ) -> Result<Option<Self>, OntologyError> {
        let mut columns = IndexSet::new();
        let mut edge_keys = Vec::new();
        for mapping in mappings {
            let Some(mapping_columns) = stored_columns(node, mapping) else {
                continue;
            };
            columns.extend(mapping_columns);
            edge_keys.extend(fk_edge_key_transform(mapping, !node.global, ontology)?);
        }
        Ok((!edge_keys.is_empty()).then(|| Self {
            node_table: prefixed_table_name(&node.destination_table, *SCHEMA_VERSION),
            columns,
            edge_keys,
        }))
    }

    pub(in crate::modules::sdlc) fn edge_keys(&self) -> &[Transformation] {
        &self.edge_keys
    }

    pub(in crate::modules::sdlc) async fn previous_edges(
        &self,
        client: &EdgeDiffClient,
        edge_keys: &dyn BlockTransform,
        page: PageRows,
    ) -> Result<Vec<(String, RecordBatch)>, HandlerError> {
        let stored = self.read_stored(client, &page).await?;
        edges_before(&stored, &page, edge_keys).await
    }

    async fn read_stored(
        &self,
        client: &EdgeDiffClient,
        page: &PageRows,
    ) -> Result<Vec<RecordBatch>, HandlerError> {
        let (Some(((lo_path, _), _)), Some(((hi_path, _), _))) =
            (page.0.first_key_value(), page.0.last_key_value())
        else {
            return Ok(Vec::new());
        };
        let mut ids: Vec<i64> = page.0.keys().map(|(_, id)| *id).collect();
        ids.sort_unstable();
        ids.dedup();
        let sql = self.stored_rows_sql();
        let queries: Vec<ArrowQuery> = ids
            .chunks(STORED_ROW_READ_CHUNK)
            .map(|ids| {
                client
                    .graph
                    .query(&sql)
                    .param("lo_path", lo_path.as_str())
                    .param("hi_path", hi_path.as_str())
                    .param("ids", ids)
            })
            .collect();
        let batches: Vec<Vec<RecordBatch>> = stream::iter(queries)
            .map(ArrowQuery::fetch_arrow)
            .buffer_unordered(client.max_concurrent_reads)
            .try_collect()
            .await
            .map_err(|err| HandlerError::Processing(format!("edge diff read failed: {err}")))?;
        Ok(batches.into_iter().flatten().collect())
    }

    fn stored_rows_sql(&self) -> String {
        let select = |column: fn(&StoredColumn) -> String| -> String {
            self.columns
                .iter()
                .map(|stored| format!(", {}", column(stored)))
                .collect()
        };
        let sources = select(|column| {
            if column.stored == column.source {
                column.stored.clone()
            } else {
                format!("{} AS {}", column.stored, column.source)
            }
        });
        format!(
            "SELECT traversal_path, id, _version, _deleted{sources} FROM ( \
               SELECT traversal_path, id, _version, _deleted{stored} \
               FROM {table} \
               WHERE traversal_path >= {{lo_path:String}} AND traversal_path <= {{hi_path:String}} \
                 AND id IN {{ids:Array(Int64)}} \
               ORDER BY traversal_path, id, _version DESC \
               LIMIT 1 BY traversal_path, id, _version) \
             ORDER BY traversal_path, id, _version DESC \
             LIMIT 2 BY traversal_path, id",
            stored = select(|column| column.stored.clone()),
            table = self.node_table,
        )
    }

    pub(in crate::modules::sdlc) fn tombstones(
        &self,
        previous: &[(String, RecordBatch)],
        outputs: &[String],
        grouped: &[Vec<RecordBatch>],
    ) -> Result<Vec<(String, Vec<RecordBatch>)>, HandlerError> {
        if previous.is_empty() {
            return Ok(Vec::new());
        }
        let converter = RowConverter::new(
            EDGE_KEY
                .iter()
                .map(|(_, data_type)| SortField::new(data_type.clone()))
                .collect(),
        )
        .map_err(|err| HandlerError::Processing(format!("edge diff keys: {err}")))?;
        let page = outputs
            .iter()
            .zip(grouped)
            .filter(|(table, _)| **table != self.node_table)
            .flat_map(|(table, batches)| batches.iter().map(move |batch| (table.as_str(), batch)))
            .map(|(table, batch)| Ok((table, edge_rows(&converter, batch)?)))
            .collect::<Result<Vec<_>, HandlerError>>()?;
        let emitted: HashSet<_> = page
            .iter()
            .flat_map(|(table, rows)| rows.iter().map(move |row| (*table, row)))
            .collect();
        let mut retired: BTreeMap<&str, Vec<RecordBatch>> = BTreeMap::new();
        for (table, batch) in previous {
            let dropped: BooleanArray = edge_rows(&converter, batch)?
                .iter()
                .map(|row| Some(!emitted.contains(&(table.as_str(), row))))
                .collect();
            let deleted = Arc::new(BooleanArray::from(vec![true; batch.num_rows()]));
            let tombstones = replace_column(batch.clone(), "_deleted", deleted)
                .and_then(|batch| filter_record_batch(&batch, &dropped))
                .map_err(|err| HandlerError::Processing(format!("edge diff tombstones: {err}")))?;
            if tombstones.num_rows() > 0 {
                retired.entry(table).or_default().push(tombstones);
            }
        }
        Ok(retired
            .into_iter()
            .map(|(table, batches)| (table.to_owned(), batches))
            .collect())
    }
}

pub(in crate::modules::sdlc) fn page_rows(
    extracted: &[RecordBatch],
) -> Result<PageRows, HandlerError> {
    let mut page = BTreeMap::new();
    for batch in extracted {
        for (key, version) in versions(batch)? {
            if page.get(&key).is_none_or(|kept| *kept < version) {
                page.insert(key, version);
            }
        }
    }
    Ok(PageRows(page))
}

async fn edges_before(
    stored: &[RecordBatch],
    page: &PageRows,
    edge_keys: &dyn BlockTransform,
) -> Result<Vec<(String, RecordBatch)>, HandlerError> {
    let (rows, versions): (Vec<_>, Vec<_>) = previous_rows(stored, page)?.into_iter().unzip();
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let stored: Vec<&RecordBatch> = stored.iter().collect();
    let previous = interleave_record_batch(&stored, &rows)
        .and_then(|previous| stamped(previous, versions))
        .map_err(|err| HandlerError::Processing(format!("edge diff previous rows: {err}")))?;
    Ok(edge_keys
        .transform(&previous)
        .await?
        .into_iter()
        .map(|edges| (edge_keys.outputs()[edges.output_index].clone(), edges.batch))
        .collect())
}

fn stored_columns(node: &NodeEntity, mapping: &EdgeMapping) -> Option<Vec<StoredColumn>> {
    let binds_node = |end: &NodeRef| {
        matches!(&end.kind, NodeRefKind::Literal(kind) if *kind == node.name) && end.field == "id"
    };
    let parent = if binds_node(&mapping.source) {
        &mapping.target
    } else if binds_node(&mapping.target) {
        &mapping.source
    } else {
        return None;
    };
    let stored = |source: &str| {
        node.fields
            .iter()
            .find(|field| field.column_name() == Some(source))
            .map(|field| StoredColumn {
                stored: field.name.clone(),
                source: source.to_owned(),
            })
    };
    let ids = stored(&parent.field)?;
    let kind = match &parent.kind {
        NodeRefKind::Literal(_) => None,
        NodeRefKind::Derived { column, .. } => Some(stored(column)?),
    };
    Some(kind.into_iter().chain([ids]).collect())
}

fn previous_rows(
    stored: &[RecordBatch],
    page: &PageRows,
) -> Result<Vec<(StoredRow, i64)>, HandlerError> {
    let mut before: HashMap<RowKey, (i64, bool, StoredRow)> = HashMap::new();
    for (index, batch) in stored.iter().enumerate() {
        let deleted: BooleanArray = downcast_array(&column(batch, "_deleted", &DataType::Boolean)?);
        for (row, (key, version)) in versions(batch)?.enumerate() {
            let older = page.0.get(&key).is_some_and(|page| version < *page);
            if older && before.get(&key).is_none_or(|(kept, ..)| *kept < version) {
                before.insert(key, (version, deleted.value(row), (index, row)));
            }
        }
    }
    Ok(page
        .0
        .iter()
        .filter_map(|(key, version)| match before.get(key) {
            Some((_, false, row)) => Some((*row, *version)),
            _ => None,
        })
        .collect())
}

fn stamped(previous: RecordBatch, versions: Vec<i64>) -> Result<RecordBatch, ArrowError> {
    let schema = previous.schema();
    let data_type = schema.field_with_name("_version")?.data_type();
    let timezone = match data_type {
        DataType::Timestamp(_, timezone) => timezone.clone(),
        _ => None,
    };
    let versions: ArrayRef =
        Arc::new(TimestampMicrosecondArray::from(versions).with_timezone_opt(timezone));
    replace_column(previous, "_version", cast(&versions, data_type)?)
}

fn replace_column(
    batch: RecordBatch,
    name: &str,
    array: ArrayRef,
) -> Result<RecordBatch, ArrowError> {
    let schema = batch.schema();
    let mut columns = batch.columns().to_vec();
    columns[schema.index_of(name)?] = array;
    RecordBatch::try_new(schema, columns)
}

fn edge_rows(converter: &RowConverter, batch: &RecordBatch) -> Result<Rows, HandlerError> {
    let columns = EDGE_KEY
        .iter()
        .map(|(name, data_type)| column(batch, name, data_type))
        .collect::<Result<Vec<_>, _>>()?;
    converter
        .convert_columns(&columns)
        .map_err(|err| HandlerError::Processing(format!("edge diff keys: {err}")))
}

fn versions(batch: &RecordBatch) -> Result<impl Iterator<Item = (RowKey, i64)>, HandlerError> {
    let paths: StringArray = downcast_array(&column(batch, "traversal_path", &DataType::Utf8)?);
    let ids: Int64Array = downcast_array(&column(batch, "id", &DataType::Int64)?);
    let versions: TimestampMicrosecondArray = downcast_array(&column(batch, "_version", &VERSION)?);
    Ok((0..batch.num_rows()).map(move |row| {
        (
            (paths.value(row).to_owned(), ids.value(row)),
            versions.value(row),
        )
    }))
}

fn column(batch: &RecordBatch, name: &str, data_type: &DataType) -> Result<ArrayRef, HandlerError> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| HandlerError::Processing(format!("edge diff: page has no column {name}")))?;
    cast(array, data_type)
        .map_err(|err| HandlerError::Processing(format!("edge diff: column {name}: {err}")))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{Field, Schema};

    use super::*;

    fn stored(rows: &[(i64, i64, bool)]) -> RecordBatch {
        let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("traversal_path", DataType::Utf8, false),
                Field::new("id", DataType::Int64, false),
                Field::new("_version", utc, false),
                Field::new("_deleted", DataType::Boolean, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["1/100/"; rows.len()])),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(
                    TimestampMicrosecondArray::from_iter_values(rows.iter().map(|row| row.1))
                        .with_timezone("UTC"),
                ),
                Arc::new(BooleanArray::from_iter(rows.iter().map(|row| Some(row.2)))),
            ],
        )
        .expect("stored batch builds")
    }

    fn edges(rows: &[(i64, i64)]) -> RecordBatch {
        let text =
            |value: &str| -> ArrayRef { Arc::new(StringArray::from(vec![value; rows.len()])) };
        RecordBatch::try_from_iter([
            ("traversal_path", text("1/100/")),
            ("relationship_kind", text("TRIGGERED")),
            (
                "source_id",
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))) as ArrayRef,
            ),
            ("source_kind", text("User")),
            ("target_id", Arc::new(Int64Array::from(vec![7; rows.len()]))),
            ("target_kind", text("Job")),
            (
                "_version",
                Arc::new(TimestampMicrosecondArray::from_iter_values(
                    rows.iter().map(|row| row.1),
                )),
            ),
            (
                "_deleted",
                Arc::new(BooleanArray::from(vec![false; rows.len()])),
            ),
        ])
        .expect("edge batch builds")
    }

    #[test]
    fn a_replayed_page_diffs_against_the_row_before_its_own() {
        let page = PageRows(BTreeMap::from([
            (("1/100/".to_string(), 7), 20),
            (("1/100/".to_string(), 8), 20),
            (("1/100/".to_string(), 9), 20),
        ]));
        let stored = stored(&[
            (7, 20, false),
            (7, 10, false),
            (8, 10, true),
            (9, 20, false),
        ]);
        let rows = previous_rows(&[stored], &page).expect("rows select");
        assert_eq!(rows, vec![((0, 1), 20)]);
    }

    #[test]
    fn only_dropped_keys_of_the_latest_page_row_are_retired() {
        let table = "v1_gl_edge".to_string();
        let previous = vec![(table.clone(), edges(&[(1, 20), (2, 20), (3, 20)]))];
        let page = edges(&[(1, 20), (2, 10)]);
        let tombstones = diff_for("Job")
            .tombstones(&previous, std::slice::from_ref(&table), &[vec![page]])
            .expect("tombstones build");
        let [(retired_table, batches)] = tombstones.as_slice() else {
            panic!("one table retires edges: {tombstones:?}");
        };
        assert_eq!(retired_table, &table);
        let read =
            |name: &str, data_type: &DataType| column(&batches[0], name, data_type).expect("reads");
        let sources: Int64Array = downcast_array(&read("source_id", &DataType::Int64));
        let versions: TimestampMicrosecondArray = downcast_array(&read("_version", &VERSION));
        let deleted: BooleanArray = downcast_array(&read("_deleted", &DataType::Boolean));
        assert_eq!(sources.values().to_vec(), vec![2, 3]);
        assert_eq!(versions.values().to_vec(), vec![20, 20]);
        assert!(deleted.iter().all(|flag| flag == Some(true)));
    }

    #[test]
    fn the_stored_read_selects_source_names_and_two_versions_per_key() {
        let keys = "traversal_path, id, _version, _deleted, project_id, stage_id, user_id";
        let rest = "runner_id, upstream_pipeline_id, auto_canceled_by_id";
        assert_eq!(
            diff_for("Job").stored_rows_sql(),
            format!(
                "SELECT {keys}, pipeline_id AS commit_id, {rest} FROM ( \
                   SELECT {keys}, pipeline_id, {rest} \
                   FROM {table} \
                   WHERE traversal_path >= {{lo_path:String}} AND traversal_path <= {{hi_path:String}} \
                     AND id IN {{ids:Array(Int64)}} \
                   ORDER BY traversal_path, id, _version DESC \
                   LIMIT 1 BY traversal_path, id, _version) \
                 ORDER BY traversal_path, id, _version DESC \
                 LIMIT 2 BY traversal_path, id",
                table = prefixed_table_name("gl_job", *SCHEMA_VERSION),
            )
        );
    }

    fn diff_for(node: &str) -> EdgeDiff {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let node = ontology.get_node(node).expect("node exists");
        EdgeDiff::for_node(node, node.pipelines[0].transform.edges(), &ontology)
            .expect("plans")
            .expect("node has FK edges")
    }
}

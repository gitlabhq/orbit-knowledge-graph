use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, DynComparator, Int64Array, ListArray, StringArray,
    TimestampMicrosecondArray, downcast_array, make_comparator,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::{
    SortOptions, cast, concat_batches, filter, filter_record_batch, interleave_record_batch,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
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

const ROW_KEY: [&str; 4] = ["traversal_path", "id", "_version", "_deleted"];

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
    key_transforms: Vec<Transformation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StoredColumn {
    stored: String,
    source: String,
    shape: Shape,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Shape {
    Id,
    Kind,
    Elements(String),
}

pub(in crate::modules::sdlc) struct PageRows(Vec<RecordBatch>);

#[derive(Default)]
pub(in crate::modules::sdlc) struct PreviousEdges {
    ids: HashSet<i64>,
    edges: Vec<(String, RecordBatch)>,
}

type RowKey<'a> = (&'a str, i64);
type RowIndex = (usize, usize);

struct KeyColumns {
    paths: StringArray,
    ids: Int64Array,
    versions: TimestampMicrosecondArray,
    deleted: BooleanArray,
}

struct PageRow {
    row: RowIndex,
    version: i64,
    deleted: bool,
    previous: Option<StoredRow>,
}

#[derive(Clone, Copy)]
struct StoredRow {
    row: RowIndex,
    version: i64,
    deleted: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Edge<'a> {
    traversal_path: &'a str,
    relationship_kind: &'a str,
    source_id: i64,
    source_kind: &'a str,
    target_id: i64,
    target_kind: &'a str,
}

struct EdgeKeys {
    paths: StringArray,
    relationships: StringArray,
    source_ids: Int64Array,
    source_kinds: StringArray,
    target_ids: Int64Array,
    target_kinds: StringArray,
    versions: TimestampMicrosecondArray,
}

impl EdgeDiff {
    pub(in crate::modules::sdlc) fn for_node(
        node: &NodeEntity,
        mappings: &[EdgeMapping],
        ontology: &Ontology,
    ) -> Result<Option<Self>, OntologyError> {
        let mut columns = IndexSet::new();
        let mut key_transforms = Vec::new();
        for mapping in mappings {
            let Some(mapping_columns) = stored_columns(node, mapping) else {
                continue;
            };
            columns.extend(mapping_columns);
            key_transforms.extend(fk_edge_key_transform(mapping, !node.global, ontology)?);
        }
        Ok((!key_transforms.is_empty()).then(|| Self {
            node_table: prefixed_table_name(&node.destination_table, *SCHEMA_VERSION),
            columns,
            key_transforms,
        }))
    }

    pub(in crate::modules::sdlc) fn key_transforms(&self) -> &[Transformation] {
        &self.key_transforms
    }

    pub(in crate::modules::sdlc) fn page_rows(
        &self,
        extracted: &[RecordBatch],
    ) -> Result<PageRows, HandlerError> {
        let names: IndexSet<&str> = ROW_KEY
            .into_iter()
            .chain(self.columns.iter().map(|column| column.source.as_str()))
            .collect();
        extracted
            .iter()
            .map(|batch| {
                let schema = batch.schema();
                let indices = names
                    .iter()
                    .map(|name| schema.index_of(name))
                    .collect::<Result<Vec<_>, _>>()?;
                batch.project(&indices)
            })
            .collect::<Result<Vec<_>, ArrowError>>()
            .map(PageRows)
            .map_err(|err| HandlerError::Processing(format!("edge diff page rows: {err}")))
    }

    pub(in crate::modules::sdlc) async fn previous_edges(
        &self,
        client: &EdgeDiffClient,
        key_transform: &dyn BlockTransform,
        page: PageRows,
    ) -> Result<PreviousEdges, HandlerError> {
        let stored = self.read_stored(client, &page).await?;
        self.edges_before(&stored, &page, key_transform).await
    }

    async fn edges_before(
        &self,
        stored: &[RecordBatch],
        page: &PageRows,
        key_transform: &dyn BlockTransform,
    ) -> Result<PreviousEdges, HandlerError> {
        let candidates = self.candidates(stored, page)?;
        if candidates.is_empty() {
            return Ok(PreviousEdges::default());
        }
        let ids = candidates.iter().map(|(_, _, id)| *id).collect();
        let (rows, versions): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .map(|(row, version, _)| (row, version))
            .unzip();
        let stored: Vec<&RecordBatch> = stored.iter().collect();
        let previous = interleave_record_batch(&stored, &rows)
            .and_then(|previous| stamped(previous, versions))
            .and_then(|previous| self.source_named(previous))
            .map_err(|err| HandlerError::Processing(format!("edge diff previous rows: {err}")))?;
        let edges = key_transform
            .transform(&previous)
            .await?
            .into_iter()
            .map(|edges| {
                (
                    key_transform.outputs()[edges.output_index].clone(),
                    edges.batch,
                )
            })
            .collect();
        Ok(PreviousEdges { ids, edges })
    }

    fn candidates(
        &self,
        stored: &[RecordBatch],
        page: &PageRows,
    ) -> Result<Vec<(RowIndex, i64, i64)>, HandlerError> {
        let page_keys = page
            .0
            .iter()
            .map(KeyColumns::read)
            .collect::<Result<Vec<_>, _>>()?;
        let stored_keys = stored
            .iter()
            .map(KeyColumns::read)
            .collect::<Result<Vec<_>, _>>()?;
        let mut latest: HashMap<RowKey, PageRow> = HashMap::new();
        for (batch, keys) in page_keys.iter().enumerate() {
            for row in 0..keys.ids.len() {
                let page_row = PageRow {
                    row: (batch, row),
                    version: keys.versions.value(row),
                    deleted: keys.deleted.value(row),
                    previous: None,
                };
                match latest.entry(keys.key(row)) {
                    Entry::Vacant(entry) => {
                        entry.insert(page_row);
                    }
                    Entry::Occupied(mut entry) if entry.get().version < page_row.version => {
                        entry.insert(page_row);
                    }
                    Entry::Occupied(_) => {}
                }
            }
        }
        for (batch, keys) in stored_keys.iter().enumerate() {
            for row in 0..keys.ids.len() {
                let Some(page_row) = latest.get_mut(&keys.key(row)) else {
                    continue;
                };
                let version = keys.versions.value(row);
                if version < page_row.version
                    && page_row.previous.is_none_or(|kept| kept.version < version)
                {
                    page_row.previous = Some(StoredRow {
                        row: (batch, row),
                        version,
                        deleted: keys.deleted.value(row),
                    });
                }
            }
        }

        let page_values = page
            .0
            .iter()
            .map(|batch| self.key_values(batch, |column| &column.source))
            .collect::<Result<Vec<_>, _>>()?;
        let stored_values = stored
            .iter()
            .map(|batch| self.key_values(batch, |column| &column.stored))
            .collect::<Result<Vec<_>, _>>()?;
        let mut comparators: HashMap<(usize, usize), Vec<DynComparator>> = HashMap::new();
        let mut candidates = Vec::new();
        for ((_, id), page_row) in &latest {
            let Some(previous) = page_row.previous.filter(|previous| !previous.deleted) else {
                continue;
            };
            let ((page_batch, page_index), (stored_batch, stored_index)) =
                (page_row.row, previous.row);
            let compare = match comparators.entry((page_batch, stored_batch)) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => entry.insert(
                    page_values[page_batch]
                        .iter()
                        .zip(&stored_values[stored_batch])
                        .map(|(page, stored)| make_comparator(page, stored, SortOptions::default()))
                        .collect::<Result<_, _>>()
                        .map_err(|err| {
                            HandlerError::Processing(format!("edge diff key compare: {err}"))
                        })?,
                ),
            };
            let changed = compare
                .iter()
                .any(|compare| compare(page_index, stored_index) != Ordering::Equal);
            if page_row.deleted || changed {
                candidates.push((previous.row, page_row.version, *id));
            }
        }
        Ok(candidates)
    }

    fn key_values(
        &self,
        batch: &RecordBatch,
        name: fn(&StoredColumn) -> &String,
    ) -> Result<Vec<ArrayRef>, HandlerError> {
        self.columns
            .iter()
            .map(|column| match &column.shape {
                Shape::Id => self::column(batch, name(column), &DataType::Int64),
                Shape::Kind => self::column(batch, name(column), &DataType::Utf8),
                Shape::Elements(field) => element_ids(batch, name(column), field),
            })
            .collect()
    }

    fn source_named(&self, batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
        let fields: Vec<Field> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                let source = self
                    .columns
                    .iter()
                    .find(|column| column.stored == *field.name())
                    .map_or(field.name().as_str(), |column| column.source.as_str());
                field.as_ref().clone().with_name(source)
            })
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())
    }

    async fn read_stored(
        &self,
        client: &EdgeDiffClient,
        page: &PageRows,
    ) -> Result<Vec<RecordBatch>, HandlerError> {
        let keys = page
            .0
            .iter()
            .map(KeyColumns::read)
            .collect::<Result<Vec<_>, _>>()?;
        let paths = || keys.iter().flat_map(|keys| keys.paths.iter().flatten());
        let (Some(lo_path), Some(hi_path)) = (paths().min(), paths().max()) else {
            return Ok(Vec::new());
        };
        let mut ids: Vec<i64> = keys
            .iter()
            .flat_map(|keys| keys.ids.values().iter().copied())
            .collect();
        ids.sort_unstable();
        ids.dedup();
        let sql = self.stored_rows_sql();
        let queries: Vec<ArrowQuery> = ids
            .chunks(STORED_ROW_READ_CHUNK)
            .map(|ids| {
                client
                    .graph
                    .query(&sql)
                    .param("lo_path", lo_path)
                    .param("hi_path", hi_path)
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
        let columns: String = self
            .columns
            .iter()
            .map(|column| format!(", {}", column.stored))
            .collect();
        format!(
            "SELECT traversal_path, id, _version, _deleted{columns} FROM ( \
               SELECT traversal_path, id, _version, _deleted{columns} \
               FROM {table} \
               WHERE traversal_path >= {{lo_path:String}} AND traversal_path <= {{hi_path:String}} \
                 AND id IN {{ids:Array(Int64)}} \
               ORDER BY traversal_path, id, _version DESC \
               LIMIT 1 BY traversal_path, id, _version) \
             ORDER BY traversal_path, id, _version DESC \
             LIMIT 2 BY traversal_path, id",
            table = self.node_table,
        )
    }

    pub(in crate::modules::sdlc) fn tombstones(
        &self,
        previous: &PreviousEdges,
        outputs: &[String],
        grouped: &[Vec<RecordBatch>],
    ) -> Result<Vec<(String, RecordBatch)>, HandlerError> {
        if previous.edges.is_empty() {
            return Ok(Vec::new());
        }
        let old = previous
            .edges
            .iter()
            .map(|(table, batch)| Ok((table.as_str(), EdgeKeys::read(batch)?)))
            .collect::<Result<Vec<_>, HandlerError>>()?;
        let page = outputs
            .iter()
            .zip(grouped)
            .filter(|(table, _)| old.iter().any(|(retiring, _)| retiring == table))
            .flat_map(|(table, batches)| batches.iter().map(move |batch| (table.as_str(), batch)))
            .map(|(table, batch)| {
                Ok((
                    table,
                    EdgeKeys::read(&candidate_edges(batch, &previous.ids)?)?,
                ))
            })
            .collect::<Result<Vec<_>, HandlerError>>()?;
        let emitted: HashSet<(&str, Edge, i64)> = page
            .iter()
            .flat_map(|(table, keys)| {
                keys.rows()
                    .map(move |(edge, version)| (*table, edge, version))
            })
            .collect();
        let mut retired: BTreeMap<&str, Vec<RecordBatch>> = BTreeMap::new();
        for (table, keys) in &old {
            let dropped: BooleanArray = keys
                .rows()
                .map(|(edge, version)| Some(!emitted.contains(&(*table, edge, version))))
                .collect();
            let tombstones = keys
                .tombstones(&dropped)
                .map_err(|err| HandlerError::Processing(format!("edge diff tombstones: {err}")))?;
            if tombstones.num_rows() > 0 {
                retired.entry(table).or_default().push(tombstones);
            }
        }
        retired
            .into_iter()
            .map(|(table, batches)| {
                concat_batches(&batches[0].schema(), &batches)
                    .map(|batch| (table.to_owned(), batch))
                    .map_err(|err| HandlerError::Processing(format!("edge diff tombstones: {err}")))
            })
            .collect()
    }
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
    let stored = |source: &str, shape: Shape| {
        node.fields
            .iter()
            .find(|field| field.column_name() == Some(source))
            .map(|field| StoredColumn {
                stored: field.name.clone(),
                source: source.to_owned(),
                shape,
            })
    };
    let ids = stored(
        &parent.field,
        mapping
            .array_field
            .clone()
            .map_or(Shape::Id, Shape::Elements),
    )?;
    let kind = match &parent.kind {
        NodeRefKind::Literal(_) => None,
        NodeRefKind::Derived { column, .. } => Some(stored(column, Shape::Kind)?),
    };
    Some(kind.into_iter().chain([ids]).collect())
}

fn stamped(previous: RecordBatch, versions: Vec<i64>) -> Result<RecordBatch, ArrowError> {
    let schema = previous.schema();
    let index = schema.index_of("_version")?;
    let data_type = schema.field(index).data_type();
    let timezone = match data_type {
        DataType::Timestamp(_, timezone) => timezone.clone(),
        _ => None,
    };
    let versions: ArrayRef =
        Arc::new(TimestampMicrosecondArray::from(versions).with_timezone_opt(timezone));
    let mut columns = previous.columns().to_vec();
    columns[index] = cast(&versions, data_type)?;
    RecordBatch::try_new(schema, columns)
}

fn candidate_edges(batch: &RecordBatch, ids: &HashSet<i64>) -> Result<RecordBatch, HandlerError> {
    let sources: Int64Array = downcast_array(&column(batch, "source_id", &DataType::Int64)?);
    let targets: Int64Array = downcast_array(&column(batch, "target_id", &DataType::Int64)?);
    let candidates: BooleanArray = sources
        .values()
        .iter()
        .zip(targets.values())
        .map(|(source, target)| Some(ids.contains(source) || ids.contains(target)))
        .collect();
    filter_record_batch(batch, &candidates)
        .map_err(|err| HandlerError::Processing(format!("edge diff page edges: {err}")))
}

fn element_ids(batch: &RecordBatch, name: &str, field: &str) -> Result<ArrayRef, HandlerError> {
    let not_a_tuple_list = || {
        HandlerError::Processing(format!(
            "edge diff: column {name} is not a tuple list with {field}"
        ))
    };
    let array = batch.column_by_name(name).ok_or_else(not_a_tuple_list)?;
    let lists = array.as_list_opt::<i32>().ok_or_else(not_a_tuple_list)?;
    let offsets = lists.offsets();
    let start = offsets[0];
    let items = lists.values().slice(
        start as usize,
        (offsets[offsets.len() - 1] - start) as usize,
    );
    let ids = items
        .as_struct_opt()
        .and_then(|items| items.column_by_name(field))
        .ok_or_else(not_a_tuple_list)?;
    let ids = cast(ids, &DataType::Int64)
        .map_err(|err| HandlerError::Processing(format!("edge diff: {field}: {err}")))?;
    ListArray::try_new(
        Arc::new(Field::new_list_field(DataType::Int64, true)),
        OffsetBuffer::new(offsets.iter().map(|offset| offset - start).collect()),
        ids,
        lists.nulls().cloned(),
    )
    .map(|lists| Arc::new(lists) as ArrayRef)
    .map_err(|err| HandlerError::Processing(format!("edge diff: {field}: {err}")))
}

impl KeyColumns {
    fn read(batch: &RecordBatch) -> Result<Self, HandlerError> {
        Ok(Self {
            paths: downcast_array(&column(batch, "traversal_path", &DataType::Utf8)?),
            ids: downcast_array(&column(batch, "id", &DataType::Int64)?),
            versions: downcast_array(&column(batch, "_version", &VERSION)?),
            deleted: downcast_array(&column(batch, "_deleted", &DataType::Boolean)?),
        })
    }

    fn key(&self, row: usize) -> RowKey<'_> {
        (self.paths.value(row), self.ids.value(row))
    }
}

impl EdgeKeys {
    fn read(batch: &RecordBatch) -> Result<Self, HandlerError> {
        let text = |name| column(batch, name, &DataType::Utf8).map(|array| downcast_array(&array));
        let int = |name| column(batch, name, &DataType::Int64).map(|array| downcast_array(&array));
        Ok(Self {
            paths: text("traversal_path")?,
            relationships: text("relationship_kind")?,
            source_ids: int("source_id")?,
            source_kinds: text("source_kind")?,
            target_ids: int("target_id")?,
            target_kinds: text("target_kind")?,
            versions: downcast_array(&column(batch, "_version", &VERSION)?),
        })
    }

    fn rows(&self) -> impl Iterator<Item = (Edge<'_>, i64)> {
        (0..self.paths.len()).map(|row| {
            let edge = Edge {
                traversal_path: self.paths.value(row),
                relationship_kind: self.relationships.value(row),
                source_id: self.source_ids.value(row),
                source_kind: self.source_kinds.value(row),
                target_id: self.target_ids.value(row),
                target_kind: self.target_kinds.value(row),
            };
            (edge, self.versions.value(row))
        })
    }

    fn tombstones(&self, dropped: &BooleanArray) -> Result<RecordBatch, ArrowError> {
        let keep = |array: &dyn Array| filter(array, dropped);
        let versions: TimestampMicrosecondArray = downcast_array(&keep(&self.versions)?);
        let deleted = BooleanArray::from(vec![true; dropped.true_count()]);
        RecordBatch::try_from_iter_with_nullable([
            ("traversal_path", keep(&self.paths)?, false),
            ("relationship_kind", keep(&self.relationships)?, false),
            ("source_id", keep(&self.source_ids)?, false),
            ("source_kind", keep(&self.source_kinds)?, false),
            ("target_id", keep(&self.target_ids)?, false),
            ("target_kind", keep(&self.target_kinds)?, false),
            ("_version", Arc::new(versions.with_timezone("UTC")), false),
            ("_deleted", Arc::new(deleted), false),
        ])
    }
}

fn column(batch: &RecordBatch, name: &str, data_type: &DataType) -> Result<ArrayRef, HandlerError> {
    let array = batch.column_by_name(name).ok_or_else(|| {
        HandlerError::Processing(format!("edge diff: batch has no column {name}"))
    })?;
    cast(array, data_type)
        .map_err(|err| HandlerError::Processing(format!("edge diff: column {name}: {err}")))
}

#[cfg(test)]
mod tests {
    use arrow::array::StructArray;
    use arrow::datatypes::Fields;

    use super::*;

    struct Row {
        id: i64,
        version: i64,
        deleted: bool,
        owner: Option<i64>,
        kind: &'static str,
        members: &'static [i64],
    }

    fn row(id: i64, version: i64) -> Row {
        Row {
            id,
            version,
            deleted: false,
            owner: Some(1),
            kind: "Issue",
            members: &[4, 5],
        }
    }

    fn diff() -> EdgeDiff {
        let column = |stored: &str, source: &str, shape| StoredColumn {
            stored: stored.to_string(),
            source: source.to_string(),
            shape,
        };
        EdgeDiff {
            node_table: "v1_gl_node".to_string(),
            columns: IndexSet::from([
                column("owner_id", "owner", Shape::Id),
                column("kind", "kind", Shape::Kind),
                column("members", "members", Shape::Elements("user_id".to_string())),
            ]),
            key_transforms: Vec::new(),
        }
    }

    fn batch(rows: &[Row], stored: bool) -> RecordBatch {
        let items = Fields::from(vec![Field::new("user_id", DataType::UInt64, false)]);
        let members: Vec<u64> = rows
            .iter()
            .flat_map(|row| row.members.iter().map(|member| *member as u64))
            .collect();
        let members = ListArray::new(
            Arc::new(Field::new("item", DataType::Struct(items.clone()), false)),
            OffsetBuffer::from_lengths(rows.iter().map(|row| row.members.len())),
            Arc::new(StructArray::new(
                items,
                vec![Arc::new(arrow::array::UInt64Array::from(members))],
                None,
            )),
            None,
        );
        RecordBatch::try_from_iter([
            (
                "traversal_path",
                Arc::new(StringArray::from(vec!["1/100/"; rows.len()])) as ArrayRef,
            ),
            (
                "id",
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.id))),
            ),
            (
                "_version",
                Arc::new(
                    TimestampMicrosecondArray::from_iter_values(rows.iter().map(|row| row.version))
                        .with_timezone("UTC"),
                ),
            ),
            (
                "_deleted",
                Arc::new(BooleanArray::from_iter(
                    rows.iter().map(|row| Some(row.deleted)),
                )),
            ),
            (
                if stored { "owner_id" } else { "owner" },
                Arc::new(Int64Array::from_iter(rows.iter().map(|row| row.owner))),
            ),
            (
                "kind",
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|row| row.kind),
                )),
            ),
            ("members", Arc::new(members)),
        ])
        .expect("batch builds")
    }

    fn candidates(page: &[Row], stored: &[Row]) -> Vec<(RowIndex, i64, i64)> {
        let diff = diff();
        let page = diff.page_rows(&[batch(page, false)]).expect("page rows");
        let mut candidates = diff
            .candidates(&[batch(stored, true)], &page)
            .expect("candidates");
        candidates.sort_by_key(|(_, _, id)| *id);
        candidates
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
    fn only_rows_whose_keys_changed_or_that_were_deleted_are_candidates() {
        let page = [
            row(1, 20),
            Row {
                owner: Some(2),
                ..row(2, 20)
            },
            Row {
                owner: None,
                ..row(3, 20)
            },
            Row {
                kind: "MergeRequest",
                ..row(4, 20)
            },
            Row {
                members: &[4],
                ..row(5, 20)
            },
            Row {
                deleted: true,
                ..row(6, 20)
            },
            row(7, 20),
        ];
        let stored: Vec<Row> = (1..=6).map(|id| row(id, 10)).collect();
        let ids: Vec<i64> = candidates(&page, &stored)
            .into_iter()
            .map(|(_, _, id)| id)
            .collect();
        assert_eq!(ids, vec![2, 3, 4, 5, 6]);
    }

    #[test]
    fn a_replayed_page_diffs_against_the_row_before_its_own() {
        let page = [row(7, 20), row(8, 20), row(9, 20)];
        let stored = [
            row(7, 20),
            Row {
                owner: Some(2),
                ..row(7, 10)
            },
            Row {
                deleted: true,
                owner: Some(2),
                ..row(8, 10)
            },
            row(9, 20),
        ];
        assert_eq!(candidates(&page, &stored), vec![((0, 1), 20, 7)]);
    }

    #[test]
    fn only_dropped_keys_of_the_latest_page_row_are_retired() {
        let table = "v1_gl_edge".to_string();
        let retyped = |batch: RecordBatch| {
            let kinds = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
            let schema = batch.schema();
            let columns = schema
                .fields()
                .iter()
                .zip(batch.columns())
                .map(|(field, array)| {
                    let array = match field.name().as_str() {
                        "source_id" => cast(array, &DataType::UInt64).expect("casts"),
                        "source_kind" => cast(array, &kinds).expect("casts"),
                        _ => array.clone(),
                    };
                    (field.name().clone(), array)
                });
            RecordBatch::try_from_iter(columns).expect("retypes")
        };
        let previous = PreviousEdges {
            ids: HashSet::from([1, 2, 3]),
            edges: vec![
                (table.clone(), edges(&[(1, 20), (2, 20)])),
                (table.clone(), retyped(edges(&[(3, 20)]))),
            ],
        };
        let page = edges(&[(1, 20), (2, 10)]);
        let tombstones = diff()
            .tombstones(&previous, std::slice::from_ref(&table), &[vec![page]])
            .expect("tombstones build");
        let [(retired_table, batch)] = tombstones.as_slice() else {
            panic!("one table retires edges: {tombstones:?}");
        };
        assert_eq!(retired_table, &table);
        let text = |name: &str| Field::new(name, DataType::Utf8, false);
        let int = |name: &str| Field::new(name, DataType::Int64, false);
        assert_eq!(
            *batch.schema(),
            Schema::new(vec![
                text("traversal_path"),
                text("relationship_kind"),
                int("source_id"),
                text("source_kind"),
                int("target_id"),
                text("target_kind"),
                Field::new(
                    "_version",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
                Field::new("_deleted", DataType::Boolean, false),
            ])
        );
        let read =
            |name: &str, data_type: &DataType| column(batch, name, data_type).expect("reads");
        let sources: Int64Array = downcast_array(&read("source_id", &DataType::Int64));
        let versions: TimestampMicrosecondArray = downcast_array(&read("_version", &VERSION));
        let deleted: BooleanArray = downcast_array(&read("_deleted", &DataType::Boolean));
        assert_eq!(sources.values().to_vec(), vec![2, 3]);
        assert_eq!(versions.values().to_vec(), vec![20, 20]);
        assert!(deleted.iter().all(|flag| flag == Some(true)));
    }

    #[test]
    fn the_stored_read_keeps_two_distinct_versions_per_key() {
        let columns = "traversal_path, id, _version, _deleted, project_id, stage_id, user_id, \
                       pipeline_id, runner_id, upstream_pipeline_id, auto_canceled_by_id";
        assert_eq!(
            diff_for("Job").stored_rows_sql(),
            format!(
                "SELECT {columns} FROM ( \
                   SELECT {columns} \
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

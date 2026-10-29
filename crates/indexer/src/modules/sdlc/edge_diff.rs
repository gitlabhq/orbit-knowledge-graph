use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, DynComparator, Int64Array, StringArray,
    TimestampMicrosecondArray, downcast_array, make_comparator,
};
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
use rustc_hash::{FxHashMap, FxHashSet};

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
    keys: Vec<KeyTransform>,
}

#[derive(Debug, Clone)]
struct KeyTransform {
    node_is_source: bool,
    columns: u64,
    probes: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StoredColumn {
    stored: String,
    source: String,
}

pub(in crate::modules::sdlc) struct PageRows(Vec<RecordBatch>);

#[derive(Default)]
pub(in crate::modules::sdlc) struct PreviousEdges {
    candidates: Candidates,
    edges: Vec<(usize, RecordBatch)>,
}

#[derive(Default)]
struct Candidates {
    paths: Vec<String>,
    rows: FxHashMap<(usize, i64), Candidate>,
}

#[derive(Clone, Copy)]
struct Candidate {
    deleted: bool,
    changed: u64,
}

type RowKey<'a> = (&'a str, i64);
type RowIndex = (usize, usize);

struct KeyColumns {
    paths: StringArray,
    ids: Int64Array,
    versions: TimestampMicrosecondArray,
    deleted: BooleanArray,
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
        let node_table = prefixed_table_name(&node.destination_table, *SCHEMA_VERSION);
        let mut outputs = vec![(String::new(), node_table.clone())];
        let mut columns = IndexSet::new();
        let mut key_transforms = Vec::new();
        let mut planned = Vec::new();
        for mapping in mappings {
            let transforms = fk_edge_key_transform(mapping, !node.global, ontology)?;
            let first = outputs.len();
            outputs.extend(
                transforms
                    .iter()
                    .map(|transform| (mapping.label.clone(), transform.destination_table.clone())),
            );
            let Some((node_is_source, mapping_columns)) = stored_columns(node, mapping) else {
                continue;
            };
            let mask = mapping_columns
                .into_iter()
                .map(|column| columns.insert_full(column).0)
                .fold(0u64, |mask, index| {
                    mask | 1u64.checked_shl(index as u32).unwrap_or(0)
                });
            planned.extend((first..outputs.len()).map(|output| (output, node_is_source, mask)));
            key_transforms.extend(transforms);
        }
        if columns.len() > u64::BITS as usize {
            return Err(OntologyError::Validation(format!(
                "{} stores {} FK key columns; the edge diff tracks at most 64",
                node.name,
                columns.len()
            )));
        }
        let keys = planned
            .into_iter()
            .map(|(output, node_is_source, columns)| KeyTransform {
                node_is_source,
                columns,
                probes: (0..outputs.len())
                    .filter(|probe| outputs[*probe] == outputs[output])
                    .collect(),
            })
            .collect();
        Ok((!key_transforms.is_empty()).then_some(Self {
            node_table,
            columns,
            key_transforms,
            keys,
        }))
    }

    pub(in crate::modules::sdlc) fn probes_match(&self, outputs: &[String]) -> bool {
        self.keys
            .iter()
            .zip(&self.key_transforms)
            .all(|(key, transform)| {
                key.probes
                    .iter()
                    .all(|probe| outputs.get(*probe) == Some(&transform.destination_table))
            })
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
        let (previous, candidates) = self.candidates(stored, page)?;
        if previous.is_empty() {
            return Ok(PreviousEdges::default());
        }
        let (rows, versions): (Vec<_>, Vec<_>) = previous.into_iter().unzip();
        let stored: Vec<&RecordBatch> = stored.iter().collect();
        let previous = interleave_record_batch(&stored, &rows)
            .and_then(|previous| stamped(previous, versions))
            .and_then(|previous| self.source_named(previous))
            .map_err(|err| HandlerError::Processing(format!("edge diff previous rows: {err}")))?;
        let edges = key_transform
            .transform(&previous)
            .await?
            .into_iter()
            .map(|edges| (edges.output_index, edges.batch))
            .collect();
        Ok(PreviousEdges { candidates, edges })
    }

    fn candidates(
        &self,
        stored: &[RecordBatch],
        page: &PageRows,
    ) -> Result<(Vec<(RowIndex, i64)>, Candidates), HandlerError> {
        let Some(first) = stored.first() else {
            return Ok(Default::default());
        };
        let page_keys = page
            .0
            .iter()
            .map(KeyColumns::read)
            .collect::<Result<Vec<_>, _>>()?;
        let stored_keys = stored
            .iter()
            .map(KeyColumns::read)
            .collect::<Result<Vec<_>, _>>()?;
        let page_version = |(batch, row): RowIndex| page_keys[batch].versions.value(row);
        let stored_version = |(batch, row): RowIndex| stored_keys[batch].versions.value(row);
        let page_size = page_keys.iter().map(|keys| keys.ids.len()).sum();
        let mut latest: FxHashMap<RowKey, (RowIndex, Option<RowIndex>)> =
            FxHashMap::with_capacity_and_hasher(page_size, Default::default());
        for (batch, keys) in page_keys.iter().enumerate() {
            for row in 0..keys.ids.len() {
                match latest.entry(keys.key(row)) {
                    Entry::Vacant(entry) => {
                        entry.insert(((batch, row), None));
                    }
                    Entry::Occupied(mut entry)
                        if page_version(entry.get().0) < keys.versions.value(row) =>
                    {
                        entry.insert(((batch, row), None));
                    }
                    Entry::Occupied(_) => {}
                }
            }
        }
        for (batch, keys) in stored_keys.iter().enumerate() {
            for row in 0..keys.ids.len() {
                let Some((page_row, previous)) = latest.get_mut(&keys.key(row)) else {
                    continue;
                };
                let version = keys.versions.value(row);
                if version < page_version(*page_row)
                    && previous.is_none_or(|kept| stored_version(kept) < version)
                {
                    *previous = Some((batch, row));
                }
            }
        }

        let types = self
            .columns
            .iter()
            .map(|column| {
                first
                    .column_by_name(&column.stored)
                    .map(|array| array.data_type().clone())
                    .ok_or_else(|| {
                        HandlerError::Processing(format!(
                            "edge diff: batch has no column {}",
                            column.stored
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let key_values = |batch: &RecordBatch, name: fn(&StoredColumn) -> &String| {
            self.columns
                .iter()
                .zip(&types)
                .map(|(column, data_type)| self::column(batch, name(column), data_type))
                .collect::<Result<Vec<_>, _>>()
        };
        let page_values = page
            .0
            .iter()
            .map(|batch| key_values(batch, |column| &column.source))
            .collect::<Result<Vec<_>, _>>()?;
        let stored_values = stored
            .iter()
            .map(|batch| key_values(batch, |column| &column.stored))
            .collect::<Result<Vec<_>, _>>()?;
        let mut comparators: FxHashMap<(usize, usize), Vec<DynComparator>> = FxHashMap::default();
        let mut interned: FxHashMap<&str, usize> = FxHashMap::default();
        let mut previous_rows = Vec::new();
        let mut candidates = Candidates::default();
        let mut rows = Vec::new();
        for ((path, id), (page_row, previous)) in &latest {
            let Some(previous) =
                previous.filter(|(batch, row)| !stored_keys[*batch].deleted.value(*row))
            else {
                continue;
            };
            let ((page_batch, page_index), (stored_batch, stored_index)) = (*page_row, previous);
            let candidate = if page_keys[page_batch].deleted.value(page_index) {
                Candidate {
                    deleted: true,
                    changed: 0,
                }
            } else {
                let compare = match comparators.entry((page_batch, stored_batch)) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => entry.insert(
                        page_values[page_batch]
                            .iter()
                            .zip(&stored_values[stored_batch])
                            .map(|(page, stored)| {
                                make_comparator(page, stored, SortOptions::default())
                            })
                            .collect::<Result<_, _>>()
                            .map_err(|err| {
                                HandlerError::Processing(format!("edge diff key compare: {err}"))
                            })?,
                    ),
                };
                let changed = compare
                    .iter()
                    .enumerate()
                    .filter(|(_, compare)| compare(page_index, stored_index) != Ordering::Equal)
                    .fold(0u64, |changed, (column, _)| changed | 1 << column);
                if changed == 0 {
                    continue;
                }
                Candidate {
                    deleted: false,
                    changed,
                }
            };
            let path = *interned.entry(path).or_insert_with(|| {
                candidates.paths.push((*path).to_owned());
                candidates.paths.len() - 1
            });
            previous_rows.push((previous, page_version(*page_row)));
            rows.push(((path, *id), candidate));
        }
        candidates.rows = rows.into_iter().collect();
        Ok((previous_rows, candidates))
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
        let paths: FxHashMap<&str, usize> = previous
            .candidates
            .paths
            .iter()
            .enumerate()
            .map(|(index, path)| (path.as_str(), index))
            .collect();
        let old = previous
            .edges
            .iter()
            .map(|(transform, batch)| Ok((*transform, EdgeKeys::read(batch)?)))
            .collect::<Result<Vec<_>, HandlerError>>()?;
        let mut retired: Vec<Vec<bool>> = Vec::with_capacity(old.len());
        let mut pending = Vec::new();
        let mut probes: FxHashMap<usize, FxHashSet<i64>> = FxHashMap::default();
        for (batch, (transform, keys)) in old.iter().enumerate() {
            let key = &self.keys[*transform];
            let mut retire = vec![false; keys.len()];
            for (row, retire) in retire.iter_mut().enumerate() {
                let (edge, _) = keys.row(row);
                let id = if key.node_is_source {
                    edge.source_id
                } else {
                    edge.target_id
                };
                let candidate = paths
                    .get(edge.traversal_path)
                    .and_then(|path| previous.candidates.rows.get(&(*path, id)));
                match candidate {
                    Some(candidate) if candidate.deleted => *retire = true,
                    Some(candidate) if candidate.changed & key.columns != 0 => {
                        pending.push((batch, row));
                        for probe in &key.probes {
                            probes.entry(*probe).or_default().insert(id);
                        }
                    }
                    _ => {}
                }
            }
            retired.push(retire);
        }
        let page = probes
            .iter()
            .flat_map(|(probe, ids)| {
                grouped[*probe]
                    .iter()
                    .map(move |batch| (*probe, ids, batch))
            })
            .map(|(probe, ids, batch)| {
                Ok((
                    outputs[probe].as_str(),
                    EdgeKeys::read(&candidate_edges(batch, ids)?)?,
                ))
            })
            .collect::<Result<Vec<_>, HandlerError>>()?;
        let emitted: FxHashSet<(&str, Edge, i64)> = page
            .iter()
            .flat_map(|(table, keys)| (0..keys.len()).map(move |row| (*table, keys.row(row))))
            .map(|(table, (edge, version))| (table, edge, version))
            .collect();
        for (batch, row) in pending {
            let (transform, keys) = &old[batch];
            let (edge, version) = keys.row(row);
            let table = self.key_transforms[*transform].destination_table.as_str();
            retired[batch][row] = !emitted.contains(&(table, edge, version));
        }
        let mut by_table: BTreeMap<&str, Vec<RecordBatch>> = BTreeMap::new();
        for ((transform, keys), retire) in old.iter().zip(retired) {
            let tombstones = keys
                .tombstones(&BooleanArray::from(retire))
                .map_err(|err| HandlerError::Processing(format!("edge diff tombstones: {err}")))?;
            if tombstones.num_rows() > 0 {
                by_table
                    .entry(&self.key_transforms[*transform].destination_table)
                    .or_default()
                    .push(tombstones);
            }
        }
        by_table
            .into_iter()
            .map(|(table, batches)| {
                concat_batches(&batches[0].schema(), &batches)
                    .map(|batch| (table.to_owned(), batch))
                    .map_err(|err| HandlerError::Processing(format!("edge diff tombstones: {err}")))
            })
            .collect()
    }
}

fn stored_columns(node: &NodeEntity, mapping: &EdgeMapping) -> Option<(bool, Vec<StoredColumn>)> {
    let binds_node = |end: &NodeRef| {
        matches!(&end.kind, NodeRefKind::Literal(kind) if *kind == node.name) && end.field == "id"
    };
    let (node_is_source, parent) = if binds_node(&mapping.source) {
        (true, &mapping.target)
    } else if binds_node(&mapping.target) {
        (false, &mapping.source)
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
    Some((node_is_source, kind.into_iter().chain([ids]).collect()))
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

fn candidate_edges(batch: &RecordBatch, ids: &FxHashSet<i64>) -> Result<RecordBatch, HandlerError> {
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

    fn len(&self) -> usize {
        self.paths.len()
    }

    fn row(&self, row: usize) -> (Edge<'_>, i64) {
        let edge = Edge {
            traversal_path: self.paths.value(row),
            relationship_kind: self.relationships.value(row),
            source_id: self.source_ids.value(row),
            source_kind: self.source_kinds.value(row),
            target_id: self.target_ids.value(row),
            target_kind: self.target_kinds.value(row),
        };
        (edge, self.versions.value(row))
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
    use arrow::array::{ListArray, StructArray};
    use arrow::buffer::OffsetBuffer;
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
        let column = |stored: &str, source: &str| StoredColumn {
            stored: stored.to_string(),
            source: source.to_string(),
        };
        EdgeDiff {
            node_table: "v1_gl_node".to_string(),
            columns: IndexSet::from([
                column("owner_id", "owner"),
                column("kind", "kind"),
                column("members", "members"),
            ]),
            key_transforms: vec![Transformation {
                sql: String::new(),
                destination_table: "v1_gl_edge".to_string(),
                dict_encode_columns: std::collections::HashSet::new(),
            }],
            keys: vec![KeyTransform {
                node_is_source: false,
                columns: 1,
                probes: vec![0],
            }],
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

    fn candidates(page: &[Row], stored: &[Row]) -> (Vec<(RowIndex, i64)>, Candidates) {
        let diff = diff();
        let page = diff.page_rows(&[batch(page, false)]).expect("page rows");
        diff.candidates(&[batch(stored, true)], &page)
            .expect("candidates")
    }

    fn changes((_, candidates): &(Vec<(RowIndex, i64)>, Candidates)) -> Vec<(i64, bool, u64)> {
        let mut changes: Vec<(i64, bool, u64)> = candidates
            .rows
            .iter()
            .map(|((_, id), candidate)| (*id, candidate.deleted, candidate.changed))
            .collect();
        changes.sort_unstable();
        changes
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
        assert_eq!(
            changes(&candidates(&page, &stored)),
            vec![
                (2, false, 0b001),
                (3, false, 0b001),
                (4, false, 0b010),
                (5, false, 0b100),
                (6, true, 0),
            ]
        );
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
        let candidates = candidates(&page, &stored);
        assert_eq!(candidates.0, vec![((0, 1), 20)]);
        assert_eq!(changes(&candidates), vec![(7, false, 0b001)]);
    }

    fn retire(
        candidate: Candidate,
        previous: Vec<RecordBatch>,
        page: RecordBatch,
    ) -> Vec<(String, RecordBatch)> {
        let previous = PreviousEdges {
            candidates: Candidates {
                paths: vec!["1/100/".to_string()],
                rows: FxHashMap::from_iter([((0, 7), candidate)]),
            },
            edges: previous.into_iter().map(|batch| (0, batch)).collect(),
        };
        diff()
            .tombstones(&previous, &["v1_gl_edge".to_string()], &[vec![page]])
            .expect("tombstones build")
    }

    fn retired_sources(tombstones: &[(String, RecordBatch)]) -> Vec<i64> {
        tombstones
            .iter()
            .flat_map(|(_, batch)| {
                let sources: Int64Array =
                    downcast_array(&column(batch, "source_id", &DataType::Int64).expect("reads"));
                sources.values().to_vec()
            })
            .collect()
    }

    #[test]
    fn only_dropped_keys_of_the_latest_page_row_are_retired() {
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
        let changed = Candidate {
            deleted: false,
            changed: 1,
        };
        let tombstones = retire(
            changed,
            vec![edges(&[(1, 20), (2, 20)]), retyped(edges(&[(3, 20)]))],
            edges(&[(1, 20), (2, 10)]),
        );
        let [(retired_table, batch)] = tombstones.as_slice() else {
            panic!("one table retires edges: {tombstones:?}");
        };
        assert_eq!(retired_table, "v1_gl_edge");
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
        let versions: TimestampMicrosecondArray = downcast_array(&read("_version", &VERSION));
        let deleted: BooleanArray = downcast_array(&read("_deleted", &DataType::Boolean));
        assert_eq!(retired_sources(&tombstones), vec![2, 3]);
        assert_eq!(versions.values().to_vec(), vec![20, 20]);
        assert!(deleted.iter().all(|flag| flag == Some(true)));
    }

    #[test]
    fn a_deleted_row_retires_every_edge_and_an_unchanged_key_retires_none() {
        let previous = || vec![edges(&[(1, 20), (2, 20)])];
        let deleted = Candidate {
            deleted: true,
            changed: 0,
        };
        let unrelated = Candidate {
            deleted: false,
            changed: 0b10,
        };
        assert_eq!(
            retired_sources(&retire(deleted, previous(), edges(&[(1, 20)]))),
            vec![1, 2]
        );
        assert!(retire(unrelated, previous(), edges(&[])).is_empty());
    }

    #[test]
    fn every_node_plan_probes_its_own_outputs() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let overrides = std::collections::HashMap::new();
        let plans = crate::modules::sdlc::plan::build_plans(
            &ontology,
            crate::modules::sdlc::plan::Sizing {
                global_batch_size: 1,
                namespaced_batch_size: 1,
                overrides: &overrides,
            },
        )
        .expect("plans build");
        for plan in plans.namespaced {
            let (Some(diff), crate::modules::sdlc::plan::TransformSpec::DataFusion(transforms)) =
                (&plan.edge_diff, &plan.transform)
            else {
                continue;
            };
            let outputs: Vec<String> = transforms
                .iter()
                .map(|transform| transform.destination_table.clone())
                .collect();
            assert!(diff.probes_match(&outputs), "{}", plan.name);
        }
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

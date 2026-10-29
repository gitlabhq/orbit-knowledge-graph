use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayData, ArrayRef, AsArray, BooleanArray, Int64Array, ListArray, StringArray,
    TimestampMicrosecondArray, downcast_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Int64Type, TimeUnit};
use arrow::record_batch::RecordBatch;
use futures::{StreamExt, TryStreamExt, stream};
use indexmap::IndexSet;
use ontology::{EdgeMapping, NodeEntity, NodeRef, NodeRefKind, Ontology};
use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};

use crate::clickhouse::{ArrowClickHouseClient, ArrowQuery};
use crate::handler::HandlerError;

const STORED_ROW_READ_CHUNK: usize = 10_000;

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
    columns: IndexSet<Column>,
    keyers: Vec<Keyer>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Column {
    name: String,
    shape: Shape,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Shape {
    Id,
    Kind,
    Elements(String),
}

#[derive(Debug, Clone)]
struct Keyer {
    relationship_kind: String,
    node_kind: String,
    node_is_source: bool,
    parent_column: usize,
    parent_kind: ParentKind,
    routes: Vec<Route>,
}

#[derive(Debug, Clone)]
enum ParentKind {
    Literal(String),
    Column {
        column: usize,
        mapping: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone)]
struct Route {
    source_kind: String,
    target_kind: String,
    table: String,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeKey<'a> {
    table: &'a str,
    traversal_path: &'a str,
    relationship_kind: &'a str,
    source_id: i64,
    source_kind: &'a str,
    target_id: i64,
    target_kind: &'a str,
}

type RowKey = (String, i64);

#[derive(Debug, Clone, PartialEq)]
struct NodeRow {
    version: i64,
    deleted: bool,
    values: Vec<Option<Value>>,
}

#[derive(Debug, Clone, PartialEq)]
enum Value {
    Int(i64),
    Text(String),
    Ids(Vec<i64>),
}

pub(in crate::modules::sdlc) struct PageKeys {
    lo_path: String,
    hi_path: String,
    ids: Vec<i64>,
}

#[derive(Default)]
pub(in crate::modules::sdlc) struct PreviousRows(HashMap<RowKey, Vec<NodeRow>>);

impl PreviousRows {
    fn before(&self, key: &RowKey, row: &NodeRow) -> Option<&NodeRow> {
        self.0
            .get(key)?
            .iter()
            .filter(|stored| stored.version < row.version)
            .max_by_key(|stored| stored.version)
    }
}

impl EdgeDiff {
    pub(in crate::modules::sdlc) fn for_node(
        node: &NodeEntity,
        mappings: &[EdgeMapping],
        ontology: &Ontology,
    ) -> Option<Self> {
        let mut columns = IndexSet::new();
        let keyers: Vec<Keyer> = mappings
            .iter()
            .filter_map(|mapping| Keyer::plan(node, mapping, ontology, &mut columns))
            .collect();
        (!keyers.is_empty()).then(|| Self {
            node_table: prefixed_table_name(&node.destination_table, *SCHEMA_VERSION),
            columns,
            keyers,
        })
    }

    pub(in crate::modules::sdlc) fn node_table(&self) -> &str {
        &self.node_table
    }

    pub(in crate::modules::sdlc) async fn read_previous(
        &self,
        client: &EdgeDiffClient,
        page: PageKeys,
    ) -> Result<PreviousRows, HandlerError> {
        let sql = self.stored_rows_sql();
        let queries: Vec<ArrowQuery> = page
            .ids
            .chunks(STORED_ROW_READ_CHUNK)
            .map(|ids| {
                client
                    .graph
                    .query(&sql)
                    .param("lo_path", page.lo_path.as_str())
                    .param("hi_path", page.hi_path.as_str())
                    .param("ids", ids)
            })
            .collect();
        let batches: Vec<Vec<RecordBatch>> = stream::iter(queries)
            .map(ArrowQuery::fetch_arrow)
            .buffer_unordered(client.max_concurrent_reads)
            .try_collect()
            .await
            .map_err(|err| HandlerError::Processing(format!("edge diff read failed: {err}")))?;
        let mut previous = PreviousRows::default();
        for batch in batches.iter().flatten() {
            for (key, row) in self.rows(batch)? {
                previous.0.entry(key).or_default().push(row);
            }
        }
        Ok(previous)
    }

    fn stored_rows_sql(&self) -> String {
        let columns: String = self
            .columns
            .iter()
            .map(|column| format!(", {}", column.name))
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
        node_rows: &[RecordBatch],
        previous: &PreviousRows,
    ) -> Result<Vec<(String, RecordBatch)>, HandlerError> {
        let mut page: BTreeMap<RowKey, NodeRow> = BTreeMap::new();
        for batch in node_rows {
            for (key, row) in self.rows(batch)? {
                if page.get(&key).is_none_or(|kept| kept.version < row.version) {
                    page.insert(key, row);
                }
            }
        }

        let mut retired: BTreeMap<&str, Vec<(EdgeKey, i64)>> = BTreeMap::new();
        for (key, row) in &page {
            let Some(previous) = previous.before(key, row) else {
                continue;
            };
            for edge in self.retired(key, previous, row) {
                retired
                    .entry(edge.table)
                    .or_default()
                    .push((edge, row.version));
            }
        }
        retired
            .into_iter()
            .map(|(table, edges)| Ok((table.to_owned(), tombstone_batch(&edges)?)))
            .collect()
    }

    fn retired<'a>(
        &'a self,
        key: &'a RowKey,
        previous: &'a NodeRow,
        row: &'a NodeRow,
    ) -> BTreeSet<EdgeKey<'a>> {
        &self.edge_keys(key, previous) - &self.edge_keys(key, row)
    }

    fn edge_keys<'a>(&'a self, key: &'a RowKey, row: &'a NodeRow) -> BTreeSet<EdgeKey<'a>> {
        if row.deleted {
            return BTreeSet::new();
        }
        self.keyers
            .iter()
            .flat_map(|keyer| keyer.keys(key, row))
            .collect()
    }

    fn rows(
        &self,
        batch: &RecordBatch,
    ) -> Result<impl Iterator<Item = (RowKey, NodeRow)>, HandlerError> {
        let (paths, ids) = key_columns(batch)?;
        let versions: TimestampMicrosecondArray = typed(
            batch,
            "_version",
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )?;
        let deleted: BooleanArray = typed(batch, "_deleted", &DataType::Boolean)?;
        let arrays = self
            .columns
            .iter()
            .map(|column| column.read(batch))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((0..batch.num_rows()).map(move |row| {
            let values = self
                .columns
                .iter()
                .zip(&arrays)
                .map(|(column, array)| column.value(array, row))
                .collect();
            (
                (paths.value(row).to_owned(), ids.value(row)),
                NodeRow {
                    version: versions.value(row),
                    deleted: deleted.value(row),
                    values,
                },
            )
        }))
    }
}

impl Keyer {
    fn plan(
        node: &NodeEntity,
        mapping: &EdgeMapping,
        ontology: &Ontology,
        columns: &mut IndexSet<Column>,
    ) -> Option<Self> {
        let binds_node = |end: &NodeRef| {
            matches!(&end.kind, NodeRefKind::Literal(kind) if *kind == node.name)
                && end.field == "id"
        };
        let (node_is_source, parent) = if binds_node(&mapping.source) {
            (true, &mapping.target)
        } else if binds_node(&mapping.target) {
            (false, &mapping.source)
        } else {
            return None;
        };
        let stored = |source_field: &str| {
            node.fields
                .iter()
                .find(|field| field.column_name() == Some(source_field))
                .map(|field| field.name.clone())
        };
        let parent_column = Column {
            name: stored(&parent.field)?,
            shape: mapping
                .array_field
                .clone()
                .map_or(Shape::Id, Shape::Elements),
        };
        let parent_kind = match &parent.kind {
            NodeRefKind::Literal(kind) => ParentKind::Literal(kind.clone()),
            NodeRefKind::Derived { column, mapping } => ParentKind::Column {
                column: columns
                    .insert_full(Column {
                        name: stored(column)?,
                        shape: Shape::Kind,
                    })
                    .0,
                mapping: mapping.clone(),
            },
        };
        let routes = ontology
            .edge_routes_for_mapping(mapping)
            .into_iter()
            .map(|edge| Route {
                source_kind: edge.source_kind.clone(),
                target_kind: edge.target_kind.clone(),
                table: prefixed_table_name(&edge.destination_table, *SCHEMA_VERSION),
            })
            .collect();
        Some(Self {
            relationship_kind: mapping.label.clone(),
            node_kind: node.name.clone(),
            node_is_source,
            parent_column: columns.insert_full(parent_column).0,
            parent_kind,
            routes,
        })
    }

    fn keys<'a>(
        &'a self,
        (traversal_path, id): &'a RowKey,
        row: &'a NodeRow,
    ) -> impl Iterator<Item = EdgeKey<'a>> {
        let parent_ids: &[i64] = match &row.values[self.parent_column] {
            Some(Value::Int(id)) => std::slice::from_ref(id),
            Some(Value::Ids(ids)) => ids,
            _ => &[],
        };
        let parent_kind = match &self.parent_kind {
            ParentKind::Literal(kind) => Some(kind),
            ParentKind::Column { column, mapping } => match &row.values[*column] {
                Some(Value::Text(raw)) => Some(mapping.get(raw).unwrap_or(raw)),
                _ => None,
            },
        };
        let route = parent_kind.and_then(|parent_kind| {
            let (source_kind, target_kind) = self.oriented(&self.node_kind, parent_kind);
            self.routes.iter().find(|route| {
                route.source_kind == *source_kind && route.target_kind == *target_kind
            })
        });
        route.into_iter().flat_map(move |route| {
            parent_ids.iter().map(move |&parent_id| {
                let (source_id, target_id) = self.oriented(*id, parent_id);
                EdgeKey {
                    table: &route.table,
                    traversal_path,
                    relationship_kind: &self.relationship_kind,
                    source_id,
                    source_kind: &route.source_kind,
                    target_id,
                    target_kind: &route.target_kind,
                }
            })
        })
    }

    fn oriented<T>(&self, node: T, parent: T) -> (T, T) {
        if self.node_is_source {
            (node, parent)
        } else {
            (parent, node)
        }
    }
}

impl Column {
    fn read(&self, batch: &RecordBatch) -> Result<ArrayRef, HandlerError> {
        match &self.shape {
            Shape::Id => cast_column(batch, &self.name, &DataType::Int64),
            Shape::Kind => cast_column(batch, &self.name, &DataType::Utf8),
            Shape::Elements(field) => {
                let not_a_tuple_list = || {
                    HandlerError::Processing(format!(
                        "edge diff: column {} is not a tuple list with {field}",
                        self.name
                    ))
                };
                let lists = column(batch, &self.name)?
                    .as_list_opt::<i32>()
                    .ok_or_else(not_a_tuple_list)?;
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
                Ok(Arc::new(ListArray::new(
                    Arc::new(Field::new_list_field(DataType::Int64, true)),
                    OffsetBuffer::new(offsets.iter().map(|offset| offset - start).collect()),
                    cast(ids, &DataType::Int64).map_err(|err| {
                        HandlerError::Processing(format!("edge diff: {field}: {err}"))
                    })?,
                    lists.nulls().cloned(),
                )))
            }
        }
    }

    fn value(&self, array: &ArrayRef, row: usize) -> Option<Value> {
        if array.is_null(row) {
            return None;
        }
        Some(match self.shape {
            Shape::Id => Value::Int(array.as_primitive::<Int64Type>().value(row)),
            Shape::Kind => Value::Text(array.as_string::<i32>().value(row).to_owned()),
            Shape::Elements(_) => Value::Ids(
                array
                    .as_list::<i32>()
                    .value(row)
                    .as_primitive::<Int64Type>()
                    .iter()
                    .flatten()
                    .collect(),
            ),
        })
    }
}

pub(in crate::modules::sdlc) fn page_keys(
    extracted: &[RecordBatch],
) -> Result<PageKeys, HandlerError> {
    let keys = extracted
        .iter()
        .map(key_columns)
        .collect::<Result<Vec<_>, _>>()?;
    let paths = || {
        keys.iter()
            .flat_map(|(paths, _)| (0..paths.len()).map(|row| paths.value(row)))
    };
    let mut ids: Vec<i64> = keys
        .iter()
        .flat_map(|(_, ids)| ids.values().iter().copied())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    Ok(PageKeys {
        lo_path: paths().min().unwrap_or_default().to_owned(),
        hi_path: paths().max().unwrap_or_default().to_owned(),
        ids,
    })
}

fn key_columns(batch: &RecordBatch) -> Result<(StringArray, Int64Array), HandlerError> {
    Ok((
        typed(batch, "traversal_path", &DataType::Utf8)?,
        typed(batch, "id", &DataType::Int64)?,
    ))
}

fn typed<A: From<ArrayData>>(
    batch: &RecordBatch,
    name: &str,
    data_type: &DataType,
) -> Result<A, HandlerError> {
    Ok(downcast_array(&cast_column(batch, name, data_type)?))
}

fn cast_column(
    batch: &RecordBatch,
    name: &str,
    data_type: &DataType,
) -> Result<ArrayRef, HandlerError> {
    cast(column(batch, name)?, data_type)
        .map_err(|err| HandlerError::Processing(format!("edge diff: column {name}: {err}")))
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, HandlerError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| HandlerError::Processing(format!("edge diff: page has no column {name}")))
}

fn tombstone_batch<'a>(edges: &[(EdgeKey<'a>, i64)]) -> Result<RecordBatch, HandlerError> {
    let text = |pick: fn(&EdgeKey<'a>) -> &'a str| -> ArrayRef {
        Arc::new(StringArray::from_iter_values(
            edges.iter().map(|(key, _)| pick(key)),
        ))
    };
    let int = |pick: fn(&EdgeKey<'a>) -> i64| -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(
            edges.iter().map(|(key, _)| pick(key)),
        ))
    };
    let versions = TimestampMicrosecondArray::from_iter_values(edges.iter().map(|(_, v)| *v));
    RecordBatch::try_from_iter([
        ("traversal_path", text(|key| key.traversal_path)),
        ("relationship_kind", text(|key| key.relationship_kind)),
        ("source_id", int(|key| key.source_id)),
        ("source_kind", text(|key| key.source_kind)),
        ("target_id", int(|key| key.target_id)),
        ("target_kind", text(|key| key.target_kind)),
        ("_version", Arc::new(versions.with_timezone("UTC"))),
        (
            "_deleted",
            Arc::new(BooleanArray::from(vec![true; edges.len()])),
        ),
    ])
    .map_err(|err| HandlerError::Processing(format!("edge diff tombstones: {err}")))
}

#[cfg(test)]
mod tests {
    use arrow::array::{StructArray, UInt64Array};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{Fields, Schema};

    use super::*;

    fn diff_for(node: &str) -> EdgeDiff {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let node = ontology.get_node(node).expect("node exists");
        EdgeDiff::for_node(node, node.pipelines[0].transform.edges(), &ontology)
            .expect("node has FK edges")
    }

    fn key() -> RowKey {
        ("1/100/1000/".to_string(), 7001)
    }

    fn row(diff: &EdgeDiff, version: i64, deleted: bool, values: &[(&str, Value)]) -> NodeRow {
        NodeRow {
            version,
            deleted,
            values: diff
                .columns
                .iter()
                .map(|column| {
                    values
                        .iter()
                        .find(|(name, _)| *name == column.name)
                        .map(|(_, value)| value.clone())
                })
                .collect(),
        }
    }

    fn job(diff: &EdgeDiff, version: i64, user: i64) -> NodeRow {
        row(
            diff,
            version,
            false,
            &[
                ("project_id", Value::Int(1000)),
                ("stage_id", Value::Int(6001)),
                ("pipeline_id", Value::Int(5001)),
                ("user_id", Value::Int(user)),
            ],
        )
    }

    fn deleted_job(diff: &EdgeDiff, version: i64) -> NodeRow {
        NodeRow {
            deleted: true,
            ..job(diff, version, 1)
        }
    }

    fn job_batch(diff: &EdgeDiff, version: i64, user: i64) -> RecordBatch {
        let (path, id) = key();
        let mut fields = vec![
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
            Field::new(
                "_version",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("_deleted", DataType::Boolean, false),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![path])),
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(TimestampMicrosecondArray::from(vec![version]).with_timezone("UTC")),
            Arc::new(BooleanArray::from(vec![false])),
        ];
        for (column, value) in diff.columns.iter().zip(job(diff, version, user).values) {
            let id = match value {
                Some(Value::Int(id)) => Some(id),
                _ => None,
            };
            fields.push(Field::new(&column.name, DataType::Int64, true));
            columns.push(Arc::new(Int64Array::from(vec![id])));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("batch builds")
    }

    fn retired(diff: &EdgeDiff, previous: &NodeRow, row: &NodeRow) -> Vec<(String, i64, i64)> {
        let key = key();
        diff.retired(&key, previous, row)
            .into_iter()
            .map(|edge| {
                (
                    edge.relationship_kind.to_string(),
                    edge.source_id,
                    edge.target_id,
                )
            })
            .collect()
    }

    #[test]
    fn a_deleted_row_retires_every_edge_it_emitted() {
        let diff = diff_for("Job");
        let retired = retired(&diff, &job(&diff, 1, 1), &deleted_job(&diff, 2));
        for expected in [
            ("HAS_JOB".to_string(), 6001, 7001),
            ("HAS_JOB".to_string(), 5001, 7001),
            ("TRIGGERED".to_string(), 1, 7001),
            ("IN_PROJECT".to_string(), 7001, 1000),
            ("IN_PIPELINE".to_string(), 7001, 5001),
        ] {
            assert!(
                retired.contains(&expected),
                "{expected:?} not in {retired:?}"
            );
        }
    }

    #[test]
    fn an_unchanged_row_retires_nothing() {
        let diff = diff_for("Job");
        assert!(retired(&diff, &job(&diff, 1, 1), &job(&diff, 2, 1)).is_empty());
    }

    #[test]
    fn a_changed_key_retires_only_the_old_edge() {
        let diff = diff_for("Job");
        assert_eq!(
            retired(&diff, &job(&diff, 1, 1), &job(&diff, 2, 2)),
            vec![("TRIGGERED".to_string(), 1, 7001)]
        );
    }

    #[test]
    fn nothing_is_retired_without_a_live_previous_row() {
        let diff = diff_for("Job");
        let tombstones = diff
            .tombstones(&[job_batch(&diff, 2, 2)], &PreviousRows::default())
            .expect("tombstones build");
        assert!(tombstones.is_empty());
        assert!(retired(&diff, &deleted_job(&diff, 1), &job(&diff, 2, 2)).is_empty());
    }

    #[test]
    fn a_replayed_page_diffs_against_the_row_before_its_own() {
        let diff = diff_for("Job");
        let older = job(&diff, 1, 1);
        let page_row = job(&diff, 2, 2);
        let previous = PreviousRows(HashMap::from([(
            key(),
            vec![page_row.clone(), older.clone()],
        )]));
        let before = previous.before(&key(), &page_row).expect("older row");
        assert_eq!(before, &older);
        assert_eq!(
            retired(&diff, before, &page_row),
            vec![("TRIGGERED".to_string(), 1, 7001)]
        );
    }

    #[test]
    fn a_polymorphic_parent_takes_its_kind_from_the_stored_type() {
        let diff = diff_for("Note");
        let key = key();
        let note = |kind: &str| {
            row(
                &diff,
                1,
                false,
                &[
                    ("noteable_type", Value::Text(kind.to_string())),
                    ("noteable_id", Value::Int(200)),
                    ("author_id", Value::Int(1)),
                ],
            )
        };
        let deleted = row(&diff, 2, true, &[]);
        let has_note = |kind: &str| -> Vec<String> {
            diff.retired(&key, &note(kind), &deleted)
                .into_iter()
                .filter(|edge| edge.relationship_kind == "HAS_NOTE")
                .map(|edge| edge.source_kind.to_string())
                .collect()
        };
        assert_eq!(has_note("Issue"), vec!["WorkItem".to_string()]);
        assert_eq!(has_note("MergeRequest"), vec!["MergeRequest".to_string()]);
        assert!(has_note("Commit").is_empty());
    }

    #[test]
    fn a_removed_array_element_retires_only_its_edge() {
        let diff = diff_for("MergeRequest");
        let reviewers = |version: i64, ids: &[i64]| {
            row(
                &diff,
                version,
                false,
                &[("reviewers", Value::Ids(ids.to_vec()))],
            )
        };
        assert_eq!(
            retired(&diff, &reviewers(1, &[4, 5]), &reviewers(2, &[4])),
            vec![("REVIEWER".to_string(), 5, 7001)]
        );
    }

    #[test]
    fn array_columns_read_the_mapped_tuple_field() {
        let fields = Fields::from(vec![Field::new("user_id", DataType::UInt64, false)]);
        let items = StructArray::new(
            fields.clone(),
            vec![Arc::new(UInt64Array::from(vec![1u64, 4, 5]))],
            None,
        );
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Struct(fields), false)),
            OffsetBuffer::from_lengths([1, 0, 0, 2]),
            Arc::new(items),
            Some(NullBuffer::from(vec![true, false, true, true])),
        );
        let batch = RecordBatch::try_from_iter([("reviewers", Arc::new(list) as ArrayRef)])
            .expect("batch builds")
            .slice(1, 3);
        let column = Column {
            name: "reviewers".to_string(),
            shape: Shape::Elements("user_id".to_string()),
        };
        let array = column.read(&batch).expect("reads");
        assert_eq!(
            (0..3)
                .map(|row| column.value(&array, row))
                .collect::<Vec<_>>(),
            vec![None, Some(Value::Ids(vec![])), Some(Value::Ids(vec![4, 5]))]
        );
    }

    #[test]
    fn group_and_project_parents_are_stored() {
        let labels = |node: &str| -> Vec<String> {
            diff_for(node)
                .keyers
                .iter()
                .map(|keyer| keyer.relationship_kind.clone())
                .collect()
        };
        assert!(labels("Group").contains(&"OWNER".to_string()));
        assert!(labels("Group").contains(&"CONTAINS".to_string()));
        assert!(labels("Project").contains(&"CREATOR".to_string()));
        assert!(labels("Project").contains(&"CONTAINS".to_string()));
    }

    #[test]
    fn the_stored_read_keeps_two_distinct_versions_per_key() {
        let sql = diff_for("Note").stored_rows_sql();
        assert!(
            sql.starts_with(
                "SELECT traversal_path, id, _version, _deleted, \
                 author_id, project_id, noteable_type, noteable_id FROM ("
            ),
            "{sql}"
        );
        assert!(
            sql.contains("LIMIT 1 BY traversal_path, id, _version"),
            "{sql}"
        );
        assert!(sql.contains("LIMIT 2 BY traversal_path, id"), "{sql}");
        assert!(sql.contains("id IN {ids:Array(Int64)}"), "{sql}");
    }

    #[test]
    fn tombstones_carry_the_page_row_version() {
        let diff = diff_for("Job");
        let (key, stored) = diff
            .rows(&job_batch(&diff, 1, 1))
            .expect("stored rows decode")
            .next()
            .expect("one stored row");
        let previous = PreviousRows(HashMap::from([(key, vec![stored])]));
        let tombstones = diff
            .tombstones(
                &[job_batch(&diff, 42, 2), job_batch(&diff, 41, 3)],
                &previous,
            )
            .expect("tombstones build");
        assert_eq!(tombstones.len(), 1);
        let batch = &tombstones[0].1;
        assert_eq!(batch.num_rows(), 1);
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
        let versions = batch
            .column_by_name("_version")
            .and_then(|column| column.as_any().downcast_ref::<TimestampMicrosecondArray>())
            .expect("version column");
        assert_eq!(versions.value(0), 42);
    }
}

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, ListArray, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use futures::{StreamExt, TryStreamExt, stream};
use ontology::{EdgeMapping, NodeEntity, NodeRef, NodeRefKind, Ontology};
use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};

use crate::clickhouse::ArrowClickHouseClient;
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
    columns: Vec<Column>,
    keyers: Vec<EdgeKeyer>,
}

#[derive(Debug, Clone, PartialEq)]
struct Column {
    name: String,
    shape: Shape,
}

#[derive(Debug, Clone, PartialEq)]
enum Shape {
    Id,
    Kind,
    Elements(String),
}

#[derive(Debug, Clone, PartialEq)]
pub(in crate::modules::sdlc) struct DiffPage {
    lo_path: String,
    hi_path: String,
    ids: Vec<i64>,
}

pub(in crate::modules::sdlc) type StoredRows = HashMap<(String, i64), Vec<NodeRow>>;

#[derive(Debug, Clone)]
struct EdgeKeyer {
    relationship_kind: String,
    emitter_kind: String,
    emitter_is_source: bool,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeKey {
    table: String,
    traversal_path: String,
    relationship_kind: String,
    source_id: i64,
    source_kind: String,
    target_id: i64,
    target_kind: String,
}

#[derive(Debug, Clone, PartialEq)]
enum Value {
    Int(i64),
    Text(String),
    Ids(Vec<i64>),
}

#[derive(Debug, Clone, PartialEq)]
pub(in crate::modules::sdlc) struct NodeRow {
    traversal_path: String,
    id: i64,
    version: i64,
    deleted: bool,
    values: Vec<Option<Value>>,
}

impl EdgeDiff {
    pub(in crate::modules::sdlc) fn for_node(
        node: &NodeEntity,
        mappings: &[EdgeMapping],
        ontology: &Ontology,
    ) -> Option<Self> {
        let mut columns: Vec<Column> = Vec::new();
        let mut column_index = |name: String, shape: Shape| {
            let column = Column { name, shape };
            columns
                .iter()
                .position(|c| *c == column)
                .unwrap_or_else(|| {
                    columns.push(column);
                    columns.len() - 1
                })
        };
        let stored = |source_field: &str| {
            node.fields
                .iter()
                .find(|field| field.column_name() == Some(source_field))
                .map(|field| field.name.clone())
        };

        let mut keyers = Vec::new();
        for mapping in mappings {
            let binds_node = |end: &NodeRef| {
                matches!(&end.kind, NodeRefKind::Literal(kind) if *kind == node.name)
                    && end.field == "id"
            };
            let (emitter_is_source, parent) = if binds_node(&mapping.source) {
                (true, &mapping.target)
            } else if binds_node(&mapping.target) {
                (false, &mapping.source)
            } else {
                continue;
            };
            let Some(parent_column) = stored(&parent.field) else {
                continue;
            };
            let parent_kind = match &parent.kind {
                NodeRefKind::Literal(kind) => ParentKind::Literal(kind.clone()),
                NodeRefKind::Derived { column, mapping } => {
                    let Some(kind_column) = stored(column) else {
                        continue;
                    };
                    ParentKind::Column {
                        column: column_index(kind_column, Shape::Kind),
                        mapping: mapping.clone(),
                    }
                }
            };
            let shape = match &mapping.array_field {
                Some(element) => Shape::Elements(element.clone()),
                None => Shape::Id,
            };
            keyers.push(EdgeKeyer {
                relationship_kind: mapping.label.clone(),
                emitter_kind: node.name.clone(),
                emitter_is_source,
                parent_column: column_index(parent_column, shape),
                parent_kind,
                routes: ontology
                    .edge_routes_for_mapping(mapping)
                    .into_iter()
                    .map(|edge| Route {
                        source_kind: edge.source_kind.clone(),
                        target_kind: edge.target_kind.clone(),
                        table: prefixed_table_name(&edge.destination_table, *SCHEMA_VERSION),
                    })
                    .collect(),
            });
        }

        (!keyers.is_empty()).then(|| Self {
            node_table: prefixed_table_name(&node.destination_table, *SCHEMA_VERSION),
            columns,
            keyers,
        })
    }

    pub(in crate::modules::sdlc) fn node_table(&self) -> &str {
        &self.node_table
    }

    pub(in crate::modules::sdlc) fn page(
        &self,
        extracted: &[RecordBatch],
    ) -> Result<Option<DiffPage>, HandlerError> {
        let mut paths: BTreeSet<String> = BTreeSet::new();
        let mut ids: Vec<i64> = Vec::new();
        for batch in extracted {
            let batch_paths = column(batch, "traversal_path", &DataType::Utf8)?;
            let batch_paths = downcast::<StringArray>(&batch_paths)?;
            let batch_ids = column(batch, "id", &DataType::Int64)?;
            let batch_ids = downcast::<Int64Array>(&batch_ids)?;
            for row in 0..batch.num_rows() {
                paths.insert(batch_paths.value(row).to_string());
                ids.push(batch_ids.value(row));
            }
        }
        let (Some(lo_path), Some(hi_path)) = (paths.first().cloned(), paths.last().cloned()) else {
            return Ok(None);
        };
        ids.sort_unstable();
        ids.dedup();
        Ok(Some(DiffPage {
            lo_path,
            hi_path,
            ids,
        }))
    }

    pub(in crate::modules::sdlc) async fn read_stored(
        &self,
        client: &EdgeDiffClient,
        page: &DiffPage,
    ) -> Result<StoredRows, HandlerError> {
        let sql = self.stored_rows_sql();
        let queries: Vec<_> = page
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
            .map(|query| async move {
                query.fetch_arrow().await.map_err(|err| {
                    HandlerError::Processing(format!("edge diff read failed: {err}"))
                })
            })
            .buffer_unordered(client.max_concurrent_reads)
            .try_collect()
            .await?;

        let mut stored: StoredRows = HashMap::new();
        for batch in batches.iter().flatten() {
            for row in self.rows(batch)? {
                stored
                    .entry((row.traversal_path.clone(), row.id))
                    .or_default()
                    .push(row);
            }
        }
        Ok(stored)
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
        stored: &StoredRows,
    ) -> Result<Vec<(String, RecordBatch)>, HandlerError> {
        let mut incoming: BTreeMap<(String, i64), NodeRow> = BTreeMap::new();
        for batch in node_rows {
            for row in self.rows(batch)? {
                let key = (row.traversal_path.clone(), row.id);
                if incoming
                    .get(&key)
                    .is_none_or(|kept| kept.version < row.version)
                {
                    incoming.insert(key, row);
                }
            }
        }

        let mut retired: BTreeMap<String, Vec<(EdgeKey, i64)>> = BTreeMap::new();
        for (key, row) in &incoming {
            let previous = previous_row(stored.get(key).map(Vec::as_slice).unwrap_or(&[]), row);
            for edge in self.retired_keys(previous, row) {
                retired
                    .entry(edge.table.clone())
                    .or_default()
                    .push((edge, row.version));
            }
        }
        retired
            .into_iter()
            .map(|(table, edges)| Ok((table, tombstone_batch(&edges)?)))
            .collect()
    }

    fn rows(&self, batch: &RecordBatch) -> Result<Vec<NodeRow>, HandlerError> {
        let paths = column(batch, "traversal_path", &DataType::Utf8)?;
        let paths = downcast::<StringArray>(&paths)?;
        let ids = column(batch, "id", &DataType::Int64)?;
        let ids = downcast::<Int64Array>(&ids)?;
        let versions = column(
            batch,
            "_version",
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )?;
        let versions = downcast::<TimestampMicrosecondArray>(&versions)?;
        let deleted = column(batch, "_deleted", &DataType::Boolean)?;
        let deleted = downcast::<BooleanArray>(&deleted)?;
        let values: Vec<ArrayRef> = self
            .columns
            .iter()
            .map(|c| match c.shape {
                Shape::Id => column(batch, &c.name, &DataType::Int64),
                Shape::Kind => column(batch, &c.name, &DataType::Utf8),
                Shape::Elements(_) => batch.column_by_name(&c.name).cloned().ok_or_else(|| {
                    HandlerError::Processing(format!("edge diff: page has no column {}", c.name))
                }),
            })
            .collect::<Result<_, _>>()?;

        (0..batch.num_rows())
            .map(|row| {
                Ok(NodeRow {
                    traversal_path: paths.value(row).to_string(),
                    id: ids.value(row),
                    version: versions.value(row),
                    deleted: deleted.value(row),
                    values: self
                        .columns
                        .iter()
                        .zip(&values)
                        .map(|(c, array)| value_at(&c.shape, array.as_ref(), row))
                        .collect::<Result<_, _>>()?,
                })
            })
            .collect()
    }

    fn retired_keys(&self, previous: Option<&NodeRow>, incoming: &NodeRow) -> Vec<EdgeKey> {
        let Some(previous) = previous.filter(|row| !row.deleted) else {
            return Vec::new();
        };
        let emitted: BTreeSet<EdgeKey> = if incoming.deleted {
            BTreeSet::new()
        } else {
            self.edge_keys(incoming).collect()
        };
        self.edge_keys(previous)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|key| !emitted.contains(key))
            .collect()
    }

    fn edge_keys<'a>(&'a self, row: &'a NodeRow) -> impl Iterator<Item = EdgeKey> + 'a {
        self.keyers.iter().flat_map(move |keyer| keyer.keys(row))
    }
}

impl EdgeKeyer {
    fn keys(&self, row: &NodeRow) -> Vec<EdgeKey> {
        let parents = match &row.values[self.parent_column] {
            Some(Value::Int(id)) => vec![*id],
            Some(Value::Ids(ids)) => ids.clone(),
            _ => return Vec::new(),
        };
        let parent_kind = match &self.parent_kind {
            ParentKind::Literal(kind) => kind.clone(),
            ParentKind::Column { column, mapping } => {
                let Some(Value::Text(raw)) = &row.values[*column] else {
                    return Vec::new();
                };
                mapping.get(raw).cloned().unwrap_or_else(|| raw.clone())
            }
        };
        parents
            .into_iter()
            .filter_map(|parent_id| {
                let (source_id, source_kind, target_id, target_kind) = if self.emitter_is_source {
                    (
                        row.id,
                        self.emitter_kind.clone(),
                        parent_id,
                        parent_kind.clone(),
                    )
                } else {
                    (
                        parent_id,
                        parent_kind.clone(),
                        row.id,
                        self.emitter_kind.clone(),
                    )
                };
                let route = self.routes.iter().find(|route| {
                    route.source_kind == source_kind && route.target_kind == target_kind
                })?;
                Some(EdgeKey {
                    table: route.table.clone(),
                    traversal_path: row.traversal_path.clone(),
                    relationship_kind: self.relationship_kind.clone(),
                    source_id,
                    source_kind,
                    target_id,
                    target_kind,
                })
            })
            .collect()
    }
}

fn previous_row<'a>(stored: &'a [NodeRow], incoming: &NodeRow) -> Option<&'a NodeRow> {
    stored
        .iter()
        .filter(|row| row.version < incoming.version)
        .max_by_key(|row| row.version)
}

fn tombstone_batch(edges: &[(EdgeKey, i64)]) -> Result<RecordBatch, HandlerError> {
    let text = |pick: fn(&EdgeKey) -> &str| -> ArrayRef {
        Arc::new(StringArray::from_iter_values(
            edges.iter().map(|(key, _)| pick(key)),
        ))
    };
    let int = |pick: fn(&EdgeKey) -> i64| -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(
            edges.iter().map(|(key, _)| pick(key)),
        ))
    };
    let schema = Schema::new(vec![
        Field::new("traversal_path", DataType::Utf8, false),
        Field::new("relationship_kind", DataType::Utf8, false),
        Field::new("source_id", DataType::Int64, false),
        Field::new("source_kind", DataType::Utf8, false),
        Field::new("target_id", DataType::Int64, false),
        Field::new("target_kind", DataType::Utf8, false),
        Field::new(
            "_version",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("_deleted", DataType::Boolean, false),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            text(|key| &key.traversal_path),
            text(|key| &key.relationship_kind),
            int(|key| key.source_id),
            text(|key| &key.source_kind),
            int(|key| key.target_id),
            text(|key| &key.target_kind),
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(
                    edges.iter().map(|(_, version)| *version),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(BooleanArray::from(vec![true; edges.len()])),
        ],
    )
    .map_err(|err| HandlerError::Processing(format!("edge diff tombstones: {err}")))
}

fn column(batch: &RecordBatch, name: &str, data_type: &DataType) -> Result<ArrayRef, HandlerError> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| HandlerError::Processing(format!("edge diff: page has no column {name}")))?;
    cast(array, data_type)
        .map_err(|err| HandlerError::Processing(format!("edge diff: column {name}: {err}")))
}

fn downcast<A: 'static>(array: &ArrayRef) -> Result<&A, HandlerError> {
    array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| HandlerError::Processing("edge diff: unexpected column type".to_string()))
}

fn value_at(shape: &Shape, array: &dyn Array, row: usize) -> Result<Option<Value>, HandlerError> {
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(match shape {
        Shape::Id => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|ints| Value::Int(ints.value(row))),
        Shape::Kind => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|text| Value::Text(text.value(row).to_string())),
        Shape::Elements(element) => {
            let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                HandlerError::Processing("edge diff: array column is not a list".to_string())
            })?;
            let items = list.value(row);
            let items = items
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    HandlerError::Processing(
                        "edge diff: array column is not a tuple list".to_string(),
                    )
                })?;
            let ids = items.column_by_name(element).ok_or_else(|| {
                HandlerError::Processing(format!("edge diff: tuple has no field {element}"))
            })?;
            let ids = cast(ids, &DataType::Int64)
                .map_err(|err| HandlerError::Processing(format!("edge diff: {element}: {err}")))?;
            let ids = downcast::<Int64Array>(&ids)?;
            Some(Value::Ids(ids.iter().flatten().collect()))
        }
    })
}

#[cfg(test)]
mod tests {
    use arrow::array::UInt64Array;
    use arrow::datatypes::Fields;

    use super::*;

    fn diff_for(node: &str) -> EdgeDiff {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let node = ontology.get_node(node).expect("node exists");
        EdgeDiff::for_node(node, node.pipelines[0].transform.edges(), &ontology)
            .expect("node has FK edges")
    }

    fn row(diff: &EdgeDiff, version: i64, deleted: bool, values: &[(&str, Value)]) -> NodeRow {
        NodeRow {
            traversal_path: "1/100/1000/".to_string(),
            id: 7001,
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

    fn kinds(keys: &[EdgeKey]) -> Vec<(String, i64, i64)> {
        keys.iter()
            .map(|key| (key.relationship_kind.clone(), key.source_id, key.target_id))
            .collect()
    }

    #[test]
    fn a_deleted_row_retires_every_edge_it_emitted() {
        let diff = diff_for("Job");
        let retired = kinds(&diff.retired_keys(Some(&job(&diff, 1, 1)), &row(&diff, 2, true, &[])));
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
        assert!(
            diff.retired_keys(Some(&job(&diff, 1, 1)), &job(&diff, 2, 1))
                .is_empty()
        );
    }

    #[test]
    fn a_changed_key_retires_only_the_old_edge() {
        let diff = diff_for("Job");
        let retired = kinds(&diff.retired_keys(Some(&job(&diff, 1, 1)), &job(&diff, 2, 2)));
        assert_eq!(retired, vec![("TRIGGERED".to_string(), 1, 7001)]);
    }

    #[test]
    fn nothing_is_retired_without_a_live_previous_row() {
        let diff = diff_for("Job");
        let incoming = row(&diff, 2, true, &[]);
        assert!(diff.retired_keys(None, &incoming).is_empty());
        assert!(
            diff.retired_keys(Some(&row(&diff, 1, true, &[])), &incoming)
                .is_empty()
        );
    }

    #[test]
    fn a_replayed_page_diffs_against_the_row_before_its_own() {
        let diff = diff_for("Job");
        let older = job(&diff, 1, 1);
        let page_row = job(&diff, 2, 2);
        let stored = vec![page_row.clone(), older.clone()];
        assert_eq!(previous_row(&stored, &page_row), Some(&older));
        assert_eq!(
            kinds(&diff.retired_keys(previous_row(&stored, &page_row), &page_row)),
            vec![("TRIGGERED".to_string(), 1, 7001)]
        );
    }

    #[test]
    fn a_polymorphic_parent_takes_its_kind_from_the_stored_type() {
        let diff = diff_for("Note");
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
            diff.retired_keys(Some(&note(kind)), &deleted)
                .into_iter()
                .filter(|key| key.relationship_kind == "HAS_NOTE")
                .map(|key| key.source_kind)
                .collect()
        };
        assert_eq!(has_note("Issue"), vec!["WorkItem".to_string()]);
        assert_eq!(has_note("MergeRequest"), vec!["MergeRequest".to_string()]);
        assert!(has_note("Commit").is_empty());
    }

    #[test]
    fn a_removed_array_element_retires_only_its_edge() {
        let diff = diff_for("MergeRequest");
        let reviewers =
            |ids: &[i64]| row(&diff, 0, false, &[("reviewers", Value::Ids(ids.to_vec()))]);
        let mut previous = reviewers(&[4, 5]);
        previous.version = 1;
        let mut incoming = reviewers(&[4]);
        incoming.version = 2;
        assert_eq!(
            kinds(&diff.retired_keys(Some(&previous), &incoming)),
            vec![("REVIEWER".to_string(), 5, 7001)]
        );
    }

    #[test]
    fn array_columns_read_the_mapped_tuple_field() {
        let fields = Fields::from(vec![Field::new("user_id", DataType::UInt64, false)]);
        let items = StructArray::new(
            fields.clone(),
            vec![Arc::new(UInt64Array::from(vec![4u64, 5]))],
            None,
        );
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Struct(fields), false)),
            arrow::buffer::OffsetBuffer::from_lengths([2]),
            Arc::new(items),
            None,
        );
        assert_eq!(
            value_at(&Shape::Elements("user_id".to_string()), &list, 0).expect("reads"),
            Some(Value::Ids(vec![4, 5]))
        );
    }

    #[test]
    fn group_and_project_parents_are_stored() {
        let labels = |node: &str| -> Vec<String> {
            diff_for(node)
                .keyers
                .iter()
                .map(|k| k.relationship_kind.clone())
                .collect()
        };
        assert!(labels("Group").contains(&"OWNER".to_string()));
        assert!(labels("Group").contains(&"CONTAINS".to_string()));
        assert!(labels("Project").contains(&"CREATOR".to_string()));
        assert!(labels("Project").contains(&"CONTAINS".to_string()));
    }

    #[test]
    fn the_stored_read_keeps_two_distinct_versions_per_key() {
        let sql = diff_for("Job").stored_rows_sql();
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
        let keys = diff.retired_keys(Some(&job(&diff, 1, 1)), &job(&diff, 42, 2));
        let batch = tombstone_batch(&keys.into_iter().map(|key| (key, 42)).collect::<Vec<_>>())
            .expect("batch builds");
        let versions = batch
            .column_by_name("_version")
            .and_then(|column| column.as_any().downcast_ref::<TimestampMicrosecondArray>())
            .expect("version column");
        assert_eq!(versions.value(0), 42);
    }
}

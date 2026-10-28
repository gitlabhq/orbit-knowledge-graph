use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::clickhouse::ArrowClickHouseClient;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use ontology::{EdgeMapping, NodeEntity, NodeRef, NodeRefKind, Ontology};
use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};

use crate::handler::HandlerError;

const STORED_ROW_READ_CHUNK: usize = 10_000;

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct EdgeDiff {
    node_table: String,
    columns: Vec<String>,
    keyers: Vec<EdgeKeyer>,
}

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
}

#[derive(Debug, Clone, PartialEq)]
struct NodeRow {
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
        let mut columns: Vec<String> = Vec::new();
        let mut column_index = |name: &str| {
            columns.iter().position(|c| c == name).unwrap_or_else(|| {
                columns.push(name.to_string());
                columns.len() - 1
            })
        };
        let graph_column = |source_field: &str| {
            node.fields
                .iter()
                .find(|field| field.column_name() == Some(source_field))
                .map(|field| field.name.clone())
        };

        let mut keyers = Vec::new();
        for mapping in mappings.iter().filter(|m| m.array_field.is_none()) {
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
            let Some(parent_column) = graph_column(&parent.field) else {
                continue;
            };
            let parent_kind = match &parent.kind {
                NodeRefKind::Literal(kind) => ParentKind::Literal(kind.clone()),
                NodeRefKind::Derived { column, mapping } => {
                    let Some(kind_column) = graph_column(column) else {
                        continue;
                    };
                    ParentKind::Column {
                        column: column_index(&kind_column),
                        mapping: mapping.clone(),
                    }
                }
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
            keyers.push(EdgeKeyer {
                relationship_kind: mapping.label.clone(),
                emitter_kind: node.name.clone(),
                emitter_is_source,
                parent_column: column_index(&parent_column),
                parent_kind,
                routes,
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

    pub(in crate::modules::sdlc) async fn tombstones(
        &self,
        graph: &ArrowClickHouseClient,
        node_rows: &[RecordBatch],
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
        if incoming.is_empty() {
            return Ok(Vec::new());
        }

        let stored = self.read_stored_rows(graph, &incoming).await?;
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

    async fn read_stored_rows(
        &self,
        graph: &ArrowClickHouseClient,
        incoming: &BTreeMap<(String, i64), NodeRow>,
    ) -> Result<HashMap<(String, i64), Vec<NodeRow>>, HandlerError> {
        let keys: Vec<&(String, i64)> = incoming.keys().collect();
        let extra_columns: String = self
            .columns
            .iter()
            .map(|column| format!(", {column}"))
            .collect();
        let mut stored: HashMap<(String, i64), Vec<NodeRow>> = HashMap::new();
        for chunk in keys.chunks(STORED_ROW_READ_CHUNK) {
            let paths: BTreeSet<String> = chunk.iter().map(|(path, _)| quote(path)).collect();
            let ids: BTreeSet<i64> = chunk.iter().map(|(_, id)| *id).collect();
            let sql = format!(
                "SELECT traversal_path, id, _version, _deleted{extra_columns} \
                 FROM {table} \
                 WHERE traversal_path IN ({paths}) AND id IN ({ids})",
                table = self.node_table,
                paths = paths.into_iter().collect::<Vec<_>>().join(", "),
                ids = ids
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            let batches = graph
                .query_arrow(&sql)
                .await
                .map_err(|err| HandlerError::Processing(format!("edge diff read failed: {err}")))?;
            for batch in &batches {
                for row in self.rows(batch)? {
                    let key = (row.traversal_path.clone(), row.id);
                    if incoming.contains_key(&key) {
                        stored.entry(key).or_default().push(row);
                    }
                }
            }
        }
        Ok(stored)
    }

    fn rows(&self, batch: &RecordBatch) -> Result<Vec<NodeRow>, HandlerError> {
        let column = |name: &str, data_type: &DataType| -> Result<ArrayRef, HandlerError> {
            let array = batch.column_by_name(name).ok_or_else(|| {
                HandlerError::Processing(format!(
                    "edge diff: {} has no column {name}",
                    self.node_table
                ))
            })?;
            cast(array, data_type)
                .map_err(|err| HandlerError::Processing(format!("edge diff: column {name}: {err}")))
        };
        let paths = column("traversal_path", &DataType::Utf8)?;
        let paths = downcast::<StringArray>(&paths)?;
        let ids = column("id", &DataType::Int64)?;
        let ids = downcast::<Int64Array>(&ids)?;
        let versions = column(
            "_version",
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )?;
        let versions = downcast::<TimestampMicrosecondArray>(&versions)?;
        let deleted = column("_deleted", &DataType::Boolean)?;
        let deleted = downcast::<BooleanArray>(&deleted)?;

        let mut kinds: Vec<bool> = vec![false; self.columns.len()];
        for keyer in &self.keyers {
            if let ParentKind::Column { column, .. } = keyer.parent_kind {
                kinds[column] = true;
            }
        }
        let values: Vec<ArrayRef> = self
            .columns
            .iter()
            .zip(&kinds)
            .map(|(name, is_kind)| {
                column(
                    name,
                    if *is_kind {
                        &DataType::Utf8
                    } else {
                        &DataType::Int64
                    },
                )
            })
            .collect::<Result<_, _>>()?;

        Ok((0..batch.num_rows())
            .map(|row| NodeRow {
                traversal_path: paths.value(row).to_string(),
                id: ids.value(row),
                version: versions.value(row),
                deleted: deleted.value(row),
                values: values
                    .iter()
                    .map(|array| value_at(array.as_ref(), row))
                    .collect(),
            })
            .collect())
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
        self.keyers.iter().filter_map(move |keyer| keyer.key(row))
    }
}

impl EdgeKeyer {
    fn key(&self, row: &NodeRow) -> Option<EdgeKey> {
        let Some(Value::Int(parent_id)) = row.values[self.parent_column] else {
            return None;
        };
        let parent_kind = match &self.parent_kind {
            ParentKind::Literal(kind) => kind.clone(),
            ParentKind::Column { column, mapping } => {
                let Some(Value::Text(raw)) = &row.values[*column] else {
                    return None;
                };
                mapping.get(raw).cloned().unwrap_or_else(|| raw.clone())
            }
        };
        let (source_id, source_kind, target_id, target_kind) = if self.emitter_is_source {
            (row.id, self.emitter_kind.clone(), parent_id, parent_kind)
        } else {
            (parent_id, parent_kind, row.id, self.emitter_kind.clone())
        };
        let route = self
            .routes
            .iter()
            .find(|route| route.source_kind == source_kind && route.target_kind == target_kind)?;
        Some(EdgeKey {
            table: route.table.clone(),
            traversal_path: row.traversal_path.clone(),
            relationship_kind: self.relationship_kind.clone(),
            source_id,
            source_kind,
            target_id,
            target_kind,
        })
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

fn downcast<A: 'static>(array: &ArrayRef) -> Result<&A, HandlerError> {
    array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| HandlerError::Processing("edge diff: unexpected column type".to_string()))
}

fn value_at(array: &dyn Array, row: usize) -> Option<Value> {
    if array.is_null(row) {
        return None;
    }
    if let Some(ints) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(Value::Int(ints.value(row)));
    }
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|text| Value::Text(text.value(row).to_string()))
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ontology() -> Ontology {
        Ontology::load_embedded().expect("ontology must load")
    }

    fn diff_for(node: &str) -> EdgeDiff {
        let ontology = ontology();
        let node = ontology.get_node(node).expect("node exists");
        let mappings: Vec<EdgeMapping> = node
            .pipelines
            .iter()
            .flat_map(|pipeline| pipeline.transform.edges().to_vec())
            .collect();
        EdgeDiff::for_node(node, &mappings, &ontology).expect("node has FK edges")
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
                        .find(|(name, _)| name == column)
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
        let previous = job(&diff, 1, 1);
        let incoming = row(&diff, 2, true, &[]);
        let retired = kinds(&diff.retired_keys(Some(&previous), &incoming));
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
    fn a_key_set_from_null_retires_nothing_and_a_key_cleared_retires_it() {
        let diff = diff_for("Job");
        let without_user = row(
            &diff,
            1,
            false,
            &[
                ("project_id", Value::Int(1000)),
                ("stage_id", Value::Int(6001)),
                ("pipeline_id", Value::Int(5001)),
            ],
        );
        assert!(
            diff.retired_keys(Some(&without_user), &job(&diff, 2, 1))
                .is_empty()
        );
        let cleared = row(
            &diff,
            3,
            false,
            &[
                ("project_id", Value::Int(1000)),
                ("stage_id", Value::Int(6001)),
                ("pipeline_id", Value::Int(5001)),
            ],
        );
        assert_eq!(
            kinds(&diff.retired_keys(Some(&job(&diff, 2, 1)), &cleared)),
            vec![("TRIGGERED".to_string(), 1, 7001)]
        );
    }

    #[test]
    fn nothing_is_retired_without_a_live_previous_row() {
        let diff = diff_for("Job");
        let incoming = row(&diff, 2, true, &[]);
        assert!(diff.retired_keys(None, &incoming).is_empty());
        let deleted_before = row(&diff, 1, true, &[]);
        assert!(
            diff.retired_keys(Some(&deleted_before), &incoming)
                .is_empty()
        );
    }

    #[test]
    fn the_previous_row_is_the_latest_one_older_than_the_page_row() {
        let diff = diff_for("Job");
        let stored = vec![job(&diff, 1, 1), job(&diff, 3, 2), job(&diff, 5, 3)];
        let incoming = job(&diff, 5, 3);
        assert_eq!(previous_row(&stored, &incoming), Some(&stored[1]));
        assert_eq!(previous_row(&stored[..1], &job(&diff, 1, 1)), None);
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
    fn array_valued_parents_are_not_diffed() {
        let diff = diff_for("MergeRequest");
        for keyer in &diff.keyers {
            assert!(
                !["REVIEWER", "APPROVED"].contains(&keyer.relationship_kind.as_str()),
                "{keyer:?}"
            );
        }
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
        let deleted = batch
            .column_by_name("_deleted")
            .and_then(|column| column.as_any().downcast_ref::<BooleanArray>())
            .expect("deleted column");
        assert!(deleted.value(0));
    }
}

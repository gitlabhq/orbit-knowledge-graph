use std::collections::BTreeMap;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::DateTime;
use futures::{StreamExt, TryStreamExt, stream};
use ontology::{EdgeMapping, NodeEntity, NodeRef, NodeRefKind, Ontology};
use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};

use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::handler::HandlerError;

const PAGE_KEY_CHUNK: usize = 10_000;
const NODE_KEY: [&str; 2] = ["traversal_path", "id"];

#[derive(Clone)]
pub(in crate::modules::sdlc) struct EdgeDiffClient {
    graph: ArrowClickHouseClient,
    max_concurrent_statements: usize,
}

impl EdgeDiffClient {
    pub(in crate::modules::sdlc) fn new(
        graph: ArrowClickHouseClient,
        max_concurrent_statements: usize,
    ) -> Self {
        Self {
            graph,
            max_concurrent_statements: max_concurrent_statements.max(1),
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct EdgeDiff {
    node_table: String,
    key_range_pages: bool,
    statements: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(in crate::modules::sdlc) struct DiffPage {
    lo: (String, i64),
    hi: (String, i64),
    min_version: i64,
    ids: Vec<i64>,
}

#[derive(Debug, Clone)]
struct EdgeKeyer {
    relationship_kind: String,
    emitter_kind: String,
    emitter_is_source: bool,
    parent: Parent,
    parent_kind: ParentKind,
    routes: Vec<(String, String, String)>,
}

#[derive(Debug, Clone)]
enum Parent {
    Scalar(usize),
    Array { column: usize, element: String },
}

#[derive(Debug, Clone)]
enum ParentKind {
    Literal(String),
    Column {
        column: usize,
        mapping: BTreeMap<String, String>,
    },
}

impl EdgeDiff {
    pub(in crate::modules::sdlc) fn for_node(
        node: &NodeEntity,
        mappings: &[EdgeMapping],
        sort_key: &[String],
        ontology: &Ontology,
    ) -> Option<Self> {
        let mut columns: Vec<String> = Vec::new();
        let mut column_index = |name: String| {
            columns.iter().position(|c| *c == name).unwrap_or_else(|| {
                columns.push(name);
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
                        column: column_index(kind_column),
                        mapping: mapping.clone(),
                    }
                }
            };
            let parent = match &mapping.array_field {
                Some(element) => Parent::Array {
                    column: column_index(parent_column),
                    element: element.clone(),
                },
                None => Parent::Scalar(column_index(parent_column)),
            };
            keyers.push(EdgeKeyer {
                relationship_kind: mapping.label.clone(),
                emitter_kind: node.name.clone(),
                emitter_is_source,
                parent,
                parent_kind,
                routes: ontology
                    .edge_routes_for_mapping(mapping)
                    .into_iter()
                    .map(|edge| {
                        (
                            edge.source_kind.clone(),
                            edge.target_kind.clone(),
                            prefixed_table_name(&edge.destination_table, *SCHEMA_VERSION),
                        )
                    })
                    .collect(),
            });
        }
        if keyers.is_empty() {
            return None;
        }

        let node_table = prefixed_table_name(&node.destination_table, *SCHEMA_VERSION);
        let key_range_pages = sort_key.len() >= 2 && sort_key[..2] == NODE_KEY;
        let mut tables: Vec<&str> = keyers
            .iter()
            .flat_map(|keyer| keyer.routes.iter().map(|(_, _, table)| table.as_str()))
            .collect();
        tables.sort_unstable();
        tables.dedup();
        let statements = tables
            .into_iter()
            .map(|table| retire_statement(table, &node_table, &columns, &keyers, key_range_pages))
            .collect();

        Some(Self {
            node_table,
            key_range_pages,
            statements,
        })
    }

    pub(in crate::modules::sdlc) fn node_table(&self) -> &str {
        &self.node_table
    }

    pub(in crate::modules::sdlc) fn page(
        &self,
        node_rows: &[RecordBatch],
    ) -> Result<Option<DiffPage>, HandlerError> {
        let mut page: Option<DiffPage> = None;
        for batch in node_rows {
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
            for row in 0..batch.num_rows() {
                let key = (paths.value(row).to_string(), ids.value(row));
                let version = versions.value(row);
                match &mut page {
                    None => {
                        page = Some(DiffPage {
                            lo: key.clone(),
                            hi: key,
                            min_version: version,
                            ids: vec![ids.value(row)],
                        })
                    }
                    Some(page) => {
                        if key < page.lo {
                            page.lo = key.clone();
                        }
                        if key > page.hi {
                            page.hi = key;
                        }
                        page.min_version = page.min_version.min(version);
                        page.ids.push(ids.value(row));
                    }
                }
            }
        }
        if let Some(page) = &mut page {
            page.ids.sort_unstable();
            page.ids.dedup();
        }
        Ok(page)
    }

    pub(in crate::modules::sdlc) async fn retire(
        &self,
        client: &EdgeDiffClient,
        page: &DiffPage,
    ) -> Result<(), HandlerError> {
        let min_version = DateTime::from_timestamp_micros(page.min_version)
            .ok_or_else(|| HandlerError::Processing("edge diff: page version out of range".into()))?
            .format(TIMESTAMP_FORMAT)
            .to_string();
        let queries: Vec<_> = self
            .jobs(page)
            .into_iter()
            .map(|(statement, ids)| {
                let query = client
                    .graph
                    .query(statement)
                    .param("lo_path", page.lo.0.as_str())
                    .param("lo_id", page.lo.1)
                    .param("hi_path", page.hi.0.as_str())
                    .param("hi_id", page.hi.1)
                    .param("min_version", min_version.as_str());
                match ids {
                    Some(ids) => query.param("ids", ids),
                    None => query,
                }
            })
            .collect();
        stream::iter(queries)
            .map(|query| async move {
                query
                    .execute()
                    .await
                    .map_err(|err| HandlerError::Processing(format!("edge diff failed: {err}")))
            })
            .buffer_unordered(client.max_concurrent_statements)
            .try_collect::<Vec<()>>()
            .await
            .map(|_| ())
    }

    fn jobs<'a>(&'a self, page: &'a DiffPage) -> Vec<(&'a str, Option<&'a [i64]>)> {
        self.statements
            .iter()
            .flat_map(|statement| {
                let chunks: Vec<Option<&[i64]>> = if self.key_range_pages {
                    vec![None]
                } else {
                    page.ids.chunks(PAGE_KEY_CHUNK).map(Some).collect()
                };
                chunks.into_iter().map(move |ids| (statement.as_str(), ids))
            })
            .collect()
    }
}

fn retire_statement(
    table: &str,
    node_table: &str,
    columns: &[String],
    keyers: &[EdgeKeyer],
    key_range_pages: bool,
) -> String {
    let row_columns: String = columns.iter().map(|c| format!(", {c}")).collect();
    let id_filter = if key_range_pages {
        ""
    } else {
        " AND id IN {ids:Array(Int64)}"
    };
    let path_range = "traversal_path >= {lo_path:String} AND traversal_path <= {hi_path:String}";
    let old_keys = edge_keys(table, keyers, "previous");
    let new_keys = edge_keys(table, keyers, "newest");
    format!(
        "INSERT INTO {table} \
           (traversal_path, relationship_kind, source_id, source_kind, target_id, target_kind, _version, _deleted) \
         SELECT traversal_path, key.2, key.3, key.4, key.5, key.6, newest.1, true \
         FROM ( \
           SELECT traversal_path, id, rows[1] AS newest, \
                  arrayFirst(row -> row.1 < newest.1, rows) AS previous, \
                  arrayExists(row -> row.1 < newest.1, rows) AS has_previous \
           FROM ( \
             SELECT traversal_path, id, \
                    arrayReverseSort(row -> row.1, groupArray(tuple(_version, _deleted{row_columns}))) AS rows \
             FROM {node_table} \
             WHERE {path_range} \
               AND (traversal_path, id) IN ( \
                 SELECT traversal_path, id FROM {node_table} \
                 WHERE {path_range} \
                   AND (traversal_path > {{lo_path:String}} OR id >= {{lo_id:Int64}}) \
                   AND (traversal_path < {{hi_path:String}} OR id <= {{hi_id:Int64}}) \
                   AND _version >= {{min_version:String}}{id_filter}) \
             GROUP BY traversal_path, id) \
           WHERE has_previous AND NOT previous.2) \
         ARRAY JOIN arrayFilter(key -> key.1 = 1 AND (newest.2 OR NOT has({new_keys}, key)), {old_keys}) AS key"
    )
}

fn edge_keys(table: &str, keyers: &[EdgeKeyer], row: &str) -> String {
    let mut scalars = Vec::new();
    let mut arrays = Vec::new();
    for keyer in keyers {
        let kinds: Vec<&str> = keyer
            .routes
            .iter()
            .filter(|(_, _, route_table)| route_table == table)
            .map(|(source, target, _)| {
                if keyer.emitter_is_source {
                    target
                } else {
                    source
                }
            })
            .map(String::as_str)
            .collect();
        if kinds.is_empty() {
            continue;
        }
        let (parent_kind, kind_valid) = match &keyer.parent_kind {
            ParentKind::Literal(kind) => (format!("'{kind}'"), "1".to_string()),
            ParentKind::Column { column, mapping } => {
                let raw = format!("ifNull({row}.{}, '')", column + 3);
                let kind = if mapping.is_empty() {
                    raw
                } else {
                    let cases: Vec<String> = mapping
                        .iter()
                        .map(|(from, to)| format!("{raw} = '{from}', '{to}'"))
                        .collect();
                    format!("multiIf({}, {raw})", cases.join(", "))
                };
                let allowed: Vec<String> = kinds.iter().map(|kind| format!("'{kind}'")).collect();
                let valid = format!("{kind} IN ({})", allowed.join(", "));
                (kind, valid)
            }
        };
        let key = |parent_id: &str, valid: &str| {
            let (source, target) = if keyer.emitter_is_source {
                (
                    format!("id, '{}'", keyer.emitter_kind),
                    format!("{parent_id}, {parent_kind}"),
                )
            } else {
                (
                    format!("{parent_id}, {parent_kind}"),
                    format!("id, '{}'", keyer.emitter_kind),
                )
            };
            format!(
                "tuple(toUInt8({valid}), '{}', {source}, {target})",
                keyer.relationship_kind
            )
        };
        match &keyer.parent {
            Parent::Scalar(column) => {
                let value = format!("{row}.{}", column + 3);
                scalars.push(key(
                    &format!("toInt64(ifNull({value}, 0))"),
                    &format!("isNotNull({value}) AND {kind_valid}"),
                ));
            }
            Parent::Array { column, element } => {
                arrays.push(format!(
                    "arrayMap(element -> {}, {row}.{})",
                    key(
                        &format!("toInt64(tupleElement(element, '{element}'))"),
                        &kind_valid
                    ),
                    column + 3
                ));
            }
        }
    }
    let mut parts = Vec::new();
    if !scalars.is_empty() {
        parts.push(format!("[{}]", scalars.join(", ")));
    }
    parts.extend(arrays);
    match parts.len() {
        1 => parts.remove(0),
        _ => format!("arrayConcat({})", parts.join(", ")),
    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};

    use super::*;

    fn diff_for(node: &str) -> EdgeDiff {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let node = ontology.get_node(node).expect("node exists");
        let pipeline = &node.pipelines[0];
        let ontology::Extract::ClickHouse(extract) = &pipeline.extract;
        EdgeDiff::for_node(
            node,
            pipeline.transform.edges(),
            &extract.order_by,
            &ontology,
        )
        .expect("node has FK edges")
    }

    fn statement(diff: &EdgeDiff, table: &str) -> String {
        diff.statements
            .iter()
            .find(|sql| sql.starts_with(&format!("INSERT INTO v{}_{table} ", *SCHEMA_VERSION)))
            .unwrap_or_else(|| panic!("no statement for {table}: {:?}", diff.statements))
            .clone()
    }

    fn page_batch(rows: &[(&str, i64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("traversal_path", DataType::Utf8, false),
                Field::new("id", DataType::Int64, false),
                Field::new(
                    "_version",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
            ])),
            vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(
                    TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| r.2))
                        .with_timezone("UTC"),
                ),
            ],
        )
        .expect("batch builds")
    }

    #[test]
    fn a_page_is_bounded_by_its_key_range_and_oldest_version() {
        let diff = diff_for("Job");
        let page = diff
            .page(&[
                page_batch(&[("1/100/", 7, 50), ("1/100/", 3, 40)]),
                page_batch(&[("1/200/", 1, 60)]),
            ])
            .expect("page reads")
            .expect("page has rows");
        assert_eq!(page.lo, ("1/100/".to_string(), 3));
        assert_eq!(page.hi, ("1/200/".to_string(), 1));
        assert_eq!(page.min_version, 40);
        assert_eq!(page.ids, vec![1, 3, 7]);
        assert_eq!(diff.page(&[]).expect("empty page reads"), None);
    }

    #[test]
    fn key_range_pages_run_one_statement_per_edge_table() {
        let diff = diff_for("Job");
        assert!(diff.key_range_pages);
        let page = DiffPage {
            lo: ("1/".to_string(), 1),
            hi: ("1/".to_string(), 30_000),
            min_version: 0,
            ids: (1..=30_000).collect(),
        };
        let jobs = diff.jobs(&page);
        assert_eq!(jobs.len(), diff.statements.len());
        assert!(jobs.iter().all(|(_, ids)| ids.is_none()));
        assert!(!diff.statements[0].contains("{ids:Array(Int64)}"));
    }

    #[test]
    fn other_pages_bind_their_ids_in_bounded_chunks() {
        let diff = diff_for("Note");
        assert!(!diff.key_range_pages);
        let page = DiffPage {
            lo: ("1/".to_string(), 1),
            hi: ("1/".to_string(), 25_001),
            min_version: 0,
            ids: (1..=25_001).collect(),
        };
        let jobs = diff.jobs(&page);
        assert_eq!(jobs.len(), diff.statements.len() * 3);
        let sizes: Vec<usize> = jobs
            .iter()
            .take(3)
            .map(|(_, ids)| ids.expect("ids bound").len())
            .collect();
        assert_eq!(sizes, vec![PAGE_KEY_CHUNK, PAGE_KEY_CHUNK, 5_001]);
        assert!(diff.statements[0].contains("AND id IN {ids:Array(Int64)}"));
    }

    #[test]
    fn the_previous_row_is_the_latest_one_older_than_the_newest() {
        let sql = statement(&diff_for("Job"), "gl_ci_edge");
        assert!(
            sql.contains("arrayFirst(row -> row.1 < newest.1, rows) AS previous"),
            "{sql}"
        );
        assert!(
            sql.contains("WHERE has_previous AND NOT previous.2"),
            "{sql}"
        );
        assert!(sql.contains("_version >= {min_version:String}"), "{sql}");
        assert!(sql.contains(", newest.1, true"), "{sql}");
    }

    #[test]
    fn a_deleted_newest_row_retires_every_old_key() {
        let sql = statement(&diff_for("Job"), "gl_ci_edge");
        assert!(
            sql.contains("arrayFilter(key -> key.1 = 1 AND (newest.2 OR NOT has("),
            "{sql}"
        );
    }

    #[test]
    fn job_keys_cover_parents_on_both_sides() {
        let sql = statement(&diff_for("Job"), "gl_ci_edge");
        for fragment in [
            "'HAS_JOB', toInt64(ifNull(previous.",
            "'TRIGGERED', toInt64(ifNull(previous.",
            "'IN_PROJECT', id, 'Job', toInt64(ifNull(previous.",
            "'IN_PIPELINE', id, 'Job'",
        ] {
            assert!(sql.contains(fragment), "{fragment} missing: {sql}");
        }
    }

    #[test]
    fn array_parents_key_every_element() {
        let sql = statement(&diff_for("MergeRequest"), "gl_edge");
        assert!(
            sql.contains("arrayMap(element -> tuple(toUInt8(1), 'REVIEWER', toInt64(tupleElement(element, 'user_id')), 'User', id, 'MergeRequest')"),
            "{sql}"
        );
        assert!(sql.contains("'APPROVED'"), "{sql}");
    }

    #[test]
    fn a_polymorphic_parent_maps_and_gates_its_kind() {
        let sql = statement(&diff_for("Note"), "gl_edge");
        assert!(sql.contains("= 'Issue', 'WorkItem'"), "{sql}");
        assert!(sql.contains("IN ('"), "{sql}");
    }

    #[test]
    fn group_and_project_parents_are_stored() {
        assert!(statement(&diff_for("Group"), "gl_edge").contains("'OWNER'"));
        assert!(statement(&diff_for("Group"), "gl_edge").contains("'CONTAINS'"));
        assert!(statement(&diff_for("Project"), "gl_edge").contains("'CREATOR'"));
        assert!(statement(&diff_for("Project"), "gl_edge").contains("'CONTAINS'"));
    }
}

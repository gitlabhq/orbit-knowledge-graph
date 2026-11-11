//! A row's identity is its entity's identity, and a relationship lives only while both
//! endpoints do: every writer's node tombstones reach the rows and edges these rules govern.

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{AsArray, BooleanArray, StringArray};
use arrow::datatypes::{DataType as ArrowType, Int64Type, UInt64Type};
use arrow::record_batch::RecordBatch;
use orbit_arrow::ArrowUtils;
use orbit_utils::clickhouse::strings_per_array_param;
use query_data_model::{
    Catalog, ClickHouse, DEFAULT_PRIMARY_KEY, DELETED_COLUMN, DataModelError, EntityId,
    RelationshipVariantId, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};

use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};

use crate::clickhouse::{ClickHouseWriter, WriteError};

/// One tombstoning `INSERT ... SELECT` against a versioned table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tombstone {
    /// Unprefixed table name.
    pub table: String,
    pub sql: String,
}

/// A namespaced entity's table, and the columns its tombstones carry: the sort key, then the
/// columns that never change.
#[derive(Debug, Clone)]
pub struct NodeTable {
    pub entity: EntityId,
    pub table: String,
    pub sort_key: Vec<String>,
    pub carried: Vec<String>,
}

impl NodeTable {
    fn columns(&self) -> String {
        self.sort_key
            .iter()
            .chain(&self.carried)
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn prefixed(&self) -> String {
        prefixed_table_name(&self.table, *SCHEMA_VERSION)
    }
}

/// The edges of one endpoint side of an entity in one edge table.
#[derive(Debug, Clone)]
struct EdgeSide {
    entity: EntityId,
    kind: String,
    table: String,
    columns: Vec<String>,
    id: &'static str,
    kind_column: &'static str,
    id_type: String,
    relationships: BTreeSet<String>,
    /// The endpoint's node table and sort key, when ClickHouse stores the endpoint.
    node: Option<(String, Vec<String>)>,
}

impl EdgeSide {
    /// Tombstones this side's live edges whose endpoint id passes `endpoint`.
    fn tombstone(&self, endpoint: &str) -> Tombstone {
        let columns = self.columns.join(", ");
        let relationships = self
            .relationships
            .iter()
            .map(|relationship| format!("'{relationship}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let prefixed = prefixed_table_name(&self.table, *SCHEMA_VERSION);
        let (id, kind_column, kind) = (self.id, self.kind_column, &self.kind);
        Tombstone {
            table: self.table.clone(),
            sql: format!(
                "INSERT INTO {prefixed} ({columns}, {DELETED_COLUMN}, {VERSION_COLUMN}) \
                 SELECT {columns}, true, now64(6) FROM {prefixed} AS e \
                 WHERE e.relationship_kind IN ({relationships}) AND e.{kind_column} = '{kind}' \
                 AND e.{DELETED_COLUMN} = false AND e.{id} {endpoint}"
            ),
        }
    }
}

/// Every node table and edge side the two rules govern.
#[derive(Debug, Default)]
pub struct Lifetime {
    nodes: Vec<NodeTable>,
    edges: Vec<EdgeSide>,
}

impl Lifetime {
    pub fn from_catalog(catalog: &Catalog<ClickHouse>) -> Result<Self, DataModelError> {
        Self::build(catalog, |_| true)
    }

    /// The rules for the rows `writer` tombstones itself, which reach only the edges of other
    /// writers: its own edges it sweeps with its rows.
    pub fn foreign_to(catalog: &Catalog<ClickHouse>, writer: &str) -> Result<Self, DataModelError> {
        let sources = catalog.model().sources();
        Self::build(catalog, |variant| {
            sources
                .variant_writer(variant)
                .is_none_or(|own| sources.row_writer_name(own) != writer)
        })
    }

    fn build(
        catalog: &Catalog<ClickHouse>,
        edge_of: impl Fn(RelationshipVariantId) -> bool,
    ) -> Result<Self, DataModelError> {
        let graph = catalog.graph();
        let schema = catalog.schema()?;
        let table_schema = |table: &str| {
            schema
                .table(table)
                .ok_or_else(|| DataModelError::Invalid(format!("{table} has no schema")))
        };
        let mut entities: Vec<_> = graph
            .entities()
            .filter(|entity| catalog.has_path_column(entity.id))
            .collect();
        entities.sort_by_key(|entity| entity.name.as_str());
        let mut nodes = Vec::new();
        for entity in entities {
            let Some(table) = catalog.table_of(entity.id) else {
                continue;
            };
            let node_table = table_schema(table)?;
            let sort_key = node_table.sort_key.clone();
            if sort_key.is_empty() {
                return Err(DataModelError::Invalid(format!("{table} has no sort key")));
            }
            let id_type = node_table
                .columns
                .iter()
                .find(|column| column.name == DEFAULT_PRIMARY_KEY)
                .map(|column| column.storage_type.as_str());
            if id_type != Some("Int64") {
                return Err(DataModelError::Invalid(format!(
                    "{table}.{DEFAULT_PRIMARY_KEY} is {id_type:?}; the indexer writes Int64 identities"
                )));
            }
            let carried = entity
                .properties
                .iter()
                .map(|property| graph.property(*property))
                .filter(|property| {
                    catalog.model().immutable(property.id) && catalog.column(property.id).is_some()
                })
                .map(|property| property.name.clone())
                .filter(|name| !sort_key.contains(name))
                .collect();
            nodes.push(NodeTable {
                entity: entity.id,
                table: table.to_string(),
                sort_key,
                carried,
            });
        }

        let mut sides = BTreeMap::<(EntityId, &str, bool), BTreeSet<String>>::new();
        for variant in graph.variants().filter(|variant| edge_of(variant.id)) {
            let Some(table) = catalog.edge_table(variant.id) else {
                continue;
            };
            let relationship = &graph.relationship(variant.relationship).name;
            for (entity, source) in [(variant.source, true), (variant.target, false)] {
                sides
                    .entry((entity, table, source))
                    .or_default()
                    .insert(relationship.clone());
            }
        }
        let mut edges = Vec::new();
        for ((entity, table, source), relationships) in sides {
            let (id, kind_column) = if source {
                ("source_id", "source_kind")
            } else {
                ("target_id", "target_kind")
            };
            let edge_table = table_schema(table)?;
            let id_type = edge_table
                .columns
                .iter()
                .find(|column| column.name == id)
                .map(|column| column.storage_type.clone())
                .ok_or_else(|| DataModelError::Invalid(format!("{table} has no {id} column")))?;
            edges.push(EdgeSide {
                entity,
                kind: graph.entity(entity).name.clone(),
                table: table.to_string(),
                columns: edge_table
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .filter(|name| name != VERSION_COLUMN && name != DELETED_COLUMN)
                    .collect(),
                id,
                kind_column,
                id_type,
                relationships,
                node: match catalog.table_of(entity) {
                    Some(node) => Some((node.to_string(), table_schema(node)?.sort_key.clone())),
                    None => None,
                },
            });
        }
        Ok(Self { nodes, edges })
    }

    pub fn nodes(&self) -> &[NodeTable] {
        &self.nodes
    }

    pub fn node(&self, entity: EntityId) -> Option<&NodeTable> {
        self.nodes.iter().find(|node| node.entity == entity)
    }

    /// Tombstones the entity's live rows in `ids:Array(Int64)` whose `(traversal_path, id)` is
    /// not among the `keys:Array(Tuple(String, Int64))` just written.
    pub fn identity(&self, entity: EntityId) -> Option<Tombstone> {
        let node = self.node(entity)?;
        let (columns, prefixed) = (node.columns(), node.prefixed());
        Some(Tombstone {
            table: node.table.clone(),
            sql: format!(
                "INSERT INTO {prefixed} ({columns}, {DELETED_COLUMN}, {VERSION_COLUMN}) \
                 SELECT {columns}, true, now64(6) FROM {prefixed} \
                 WHERE {DEFAULT_PRIMARY_KEY} IN {{ids:Array(Int64)}} AND {DELETED_COLUMN} = false \
                 AND ({TRAVERSAL_PATH_COLUMN}, {DEFAULT_PRIMARY_KEY}) NOT IN \
                 {{keys:Array(Tuple(String, Int64))}}"
            ),
        })
    }

    /// Tombstones the live edges, in every edge table, of the entity's nodes in `ids`.
    pub fn edges_of(&self, entity: EntityId) -> Vec<Tombstone> {
        self.sides(entity)
            .map(|side| side.tombstone(&format!("IN {{ids:Array({})}}", side.id_type)))
            .collect()
    }

    /// Tombstones the live edges, in every edge table, of the live nodes under
    /// `traversal_path:String`; it runs before those nodes are tombstoned.
    pub fn edges_under_path(&self) -> Vec<Tombstone> {
        self.edges
            .iter()
            .filter_map(|side| {
                let node = self.node(side.entity)?;
                Some(side.tombstone(&format!(
                    "IN (SELECT {DEFAULT_PRIMARY_KEY} FROM {} FINAL \
                     WHERE startsWith({TRAVERSAL_PATH_COLUMN}, {{traversal_path:String}}))",
                    node.prefixed()
                )))
            })
            .collect()
    }

    /// Reconciles the rows under `traversal_path:String` that predate the two rules: the live
    /// edges whose endpoint has only tombstoned rows, then the live rows of an identity that
    /// has a newer live row under another path.
    pub fn reconcile_under_path(&self) -> Vec<Tombstone> {
        let path = format!("startsWith({TRAVERSAL_PATH_COLUMN}, {{traversal_path:String}})");
        let mut tombstones: Vec<_> = self
            .edges
            .iter()
            .filter_map(|side| {
                let (node, key) = side.node.as_ref()?;
                let node = prefixed_table_name(node, *SCHEMA_VERSION);
                let edges = prefixed_table_name(&side.table, *SCHEMA_VERSION);
                let (id, kind_column, kind) = (side.id, side.kind_column, &side.kind);
                let key = key.join(", ");
                Some(side.tombstone(&format!(
                    "IN (SELECT {DEFAULT_PRIMARY_KEY} FROM (SELECT {DEFAULT_PRIMARY_KEY}, \
                     argMax({DELETED_COLUMN}, {VERSION_COLUMN}) AS dead FROM {node} \
                     WHERE {DEFAULT_PRIMARY_KEY} IN (SELECT {id} FROM {edges} \
                     WHERE {path} AND {kind_column} = '{kind}') GROUP BY {key}) \
                     GROUP BY {DEFAULT_PRIMARY_KEY} HAVING min(dead) = true) \
                     AND startsWith(e.{TRAVERSAL_PATH_COLUMN}, {{traversal_path:String}})"
                )))
            })
            .collect();
        tombstones.extend(self.nodes.iter().map(|node| {
            let (columns, prefixed) = (node.columns(), node.prefixed());
            let key = node.sort_key.join(", ");
            Tombstone {
                table: node.table.clone(),
                sql: format!(
                    "INSERT INTO {prefixed} ({columns}, {DELETED_COLUMN}, {VERSION_COLUMN}) \
                     SELECT {columns}, true, now64(6) FROM {prefixed} \
                     WHERE {path} AND {DELETED_COLUMN} = false AND ({key}) IN (\
                     SELECT {key} FROM (SELECT {key}, max({VERSION_COLUMN}) AS version, \
                     max(version) OVER (PARTITION BY {DEFAULT_PRIMARY_KEY}) AS newest \
                     FROM {prefixed} WHERE {DEFAULT_PRIMARY_KEY} IN \
                     (SELECT {DEFAULT_PRIMARY_KEY} FROM {prefixed} WHERE {path}) \
                     GROUP BY {key} HAVING argMax({DELETED_COLUMN}, {VERSION_COLUMN}) = false) \
                     WHERE version < newest)"
                ),
            }
        }));
        tombstones
    }

    fn sides(&self, entity: EntityId) -> impl Iterator<Item = &EdgeSide> {
        self.edges.iter().filter(move |side| side.entity == entity)
    }

    /// The tombstones that one block of the entity's written node rows triggers.
    pub fn of_node(&self, entity: EntityId) -> NodeLifetime {
        NodeLifetime {
            identity: self.identity(entity),
            edges: self.edges_of(entity),
        }
    }
}

/// The tombstones a block of one entity's written node rows triggers.
#[derive(Debug, Clone)]
pub struct NodeLifetime {
    identity: Option<Tombstone>,
    edges: Vec<Tombstone>,
}

impl NodeLifetime {
    /// Applies both rules to one written block: other rows of the written identities are
    /// tombstoned, and so are the edges of the deleted nodes.
    pub async fn written(
        &self,
        writer: &ClickHouseWriter,
        batches: &[RecordBatch],
    ) -> Result<(), WriteError> {
        let rows = WrittenRows::read(batches, self.identity.is_some())?;
        if let Some(identity) = &self.identity {
            let paths: Vec<&str> = rows.keys.iter().map(|(path, _)| path.as_str()).collect();
            let per_param = strings_per_array_param(&paths).min(IDS_PER_PARAM);
            for keys in rows.keys.chunks(per_param) {
                let ids: Vec<i64> = keys.iter().map(|(_, id)| *id).collect();
                writer
                    .tombstone(&identity.sql, |query| {
                        query.param("ids", &ids).param("keys", keys)
                    })
                    .await?;
            }
        }
        if rows.deleted.is_empty() {
            return Ok(());
        }
        for edges in &self.edges {
            for ids in rows.deleted.chunks(IDS_PER_PARAM) {
                writer
                    .tombstone(&edges.sql, |query| query.param("ids", ids))
                    .await?;
            }
        }
        Ok(())
    }
}

/// Ids per bound array parameter, under the URL budget of one parameter.
pub(crate) const IDS_PER_PARAM: usize = 2_000;

/// The keys of one block of an entity's written node rows.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct WrittenRows {
    /// `(traversal_path, id)` of every row; empty for an entity without a path column.
    pub keys: Vec<(String, i64)>,
    pub deleted: Vec<i64>,
}

impl WrittenRows {
    pub fn read(batches: &[RecordBatch], namespaced: bool) -> Result<Self, WriteError> {
        let missing = |name: &str| {
            WriteError::Write(format!(
                "written node rows have no {name} column of its type"
            ))
        };
        let mut rows = Self::default();
        for batch in batches {
            let ids = batch
                .column_by_name(DEFAULT_PRIMARY_KEY)
                .ok_or_else(|| missing(DEFAULT_PRIMARY_KEY))?;
            // A hashed id arrives as UInt64; the Int64 column stores it wrapped.
            let id = |row: usize| match ids.data_type() {
                ArrowType::Int64 => ids.as_primitive::<Int64Type>().value(row),
                _ => ids.as_primitive::<UInt64Type>().value(row) as i64,
            };
            if !matches!(ids.data_type(), ArrowType::Int64 | ArrowType::UInt64) {
                return Err(missing(DEFAULT_PRIMARY_KEY));
            }
            let deleted = ArrowUtils::get_column_by_name::<BooleanArray>(batch, DELETED_COLUMN)
                .ok_or_else(|| missing(DELETED_COLUMN))?;
            let paths = if namespaced {
                let paths =
                    ArrowUtils::get_column_by_name::<StringArray>(batch, TRAVERSAL_PATH_COLUMN)
                        .ok_or_else(|| missing(TRAVERSAL_PATH_COLUMN))?;
                Some(paths)
            } else {
                None
            };
            for row in 0..batch.num_rows() {
                let id = id(row);
                if deleted.value(row) {
                    rows.deleted.push(id);
                }
                if let Some(paths) = paths {
                    rows.keys.push((paths.value(row).to_string(), id));
                }
            }
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn lifetime() -> (Catalog<ClickHouse>, Lifetime) {
        let catalog = Catalog::<ClickHouse>::embedded().expect("embedded catalog");
        let lifetime = Lifetime::from_catalog(&catalog).expect("lifetime");
        (catalog, lifetime)
    }

    fn entity(catalog: &Catalog<ClickHouse>, name: &str) -> EntityId {
        catalog.graph().entity_id(name).expect("entity")
    }

    #[test]
    fn a_deleted_merge_request_takes_its_approvals_whoever_wrote_them() {
        let (catalog, lifetime) = lifetime();
        let edges = lifetime.edges_of(entity(&catalog, "MergeRequest"));
        let approvals = edges
            .iter()
            .find(|edge| {
                edge.table == "gl_edge" && edge.sql.contains("target_kind = 'MergeRequest'")
            })
            .expect("the merge request's incoming gl_edge edges");
        let sql = &approvals.sql;
        assert!(sql.contains("'APPROVED'"), "{sql}");
        assert!(sql.contains("e.target_id IN {ids:Array(Int64)}"), "{sql}");
        assert!(sql.contains("e._deleted = false"), "{sql}");
        assert!(
            sql.contains("source_tags, target_tags, _deleted, _version"),
            "the tombstone carries the full row: {sql}"
        );
    }

    #[test]
    fn a_node_reaches_every_edge_table_that_binds_a_variant_of_it() {
        let (catalog, lifetime) = lifetime();
        let tables: BTreeSet<String> = lifetime
            .edges_of(entity(&catalog, "Project"))
            .into_iter()
            .map(|edge| edge.table)
            .collect();
        for table in [
            "gl_edge",
            "gl_ci_edge",
            "gl_sec_edge",
            "gl_diff_edge",
            "gl_code_edge",
        ] {
            assert!(tables.contains(table), "{table}: {tables:?}");
        }
    }

    #[test]
    fn identity_tombstones_the_other_paths_of_the_written_ids() {
        let (catalog, lifetime) = lifetime();
        let sql = lifetime
            .identity(entity(&catalog, "Group"))
            .expect("a namespaced entity")
            .sql;
        let group = prefixed_table_name("gl_group", *SCHEMA_VERSION);
        assert!(
            sql.starts_with(&format!("INSERT INTO {group} (traversal_path, id")),
            "{sql}"
        );
        assert!(sql.contains("id IN {ids:Array(Int64)}"), "{sql}");
        assert!(
            sql.contains("(traversal_path, id) NOT IN {keys:Array(Tuple(String, Int64))}"),
            "{sql}"
        );
        assert!(lifetime.identity(entity(&catalog, "User")).is_none());
    }

    #[test]
    fn written_rows_hold_every_key_and_the_deleted_ids() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(DEFAULT_PRIMARY_KEY, DataType::Int64, false),
            Field::new(TRAVERSAL_PATH_COLUMN, DataType::Utf8, false),
            Field::new(DELETED_COLUMN, DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["1/2/", "1/3/"])),
                Arc::new(BooleanArray::from(vec![false, true])),
            ],
        )
        .unwrap();
        let rows = WrittenRows::read(&[batch], true).unwrap();
        assert_eq!(
            rows,
            WrittenRows {
                keys: vec![("1/2/".into(), 1), ("1/3/".into(), 2)],
                deleted: vec![2],
            }
        );
    }
}

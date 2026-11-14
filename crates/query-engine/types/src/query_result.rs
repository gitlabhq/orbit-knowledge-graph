use std::collections::{HashMap, HashSet};

use arrow::array::{Array, Int64Array, ListArray, StringArray, StructArray};
use arrow::record_batch::RecordBatch;
use compiler::constants::{
    edge_kinds_column, identity_part_column, neighbor_id_column, neighbor_type_column, path_column,
};
use compiler::internal_column_prefix;
use compiler::orbit_authorization::Key;
use compiler::{EntityAuthConfig, KeyKind, KeySource, QueryType, RedactionNode, ResultContext};

use super::{ResourceAuthorization, ResourceCheck};
use orbit_arrow::{ArrowUtils, ColumnValue};

#[derive(Debug, Clone)]
pub struct NodeRef {
    pub id: Key,
    pub entity_type: String,
    /// Hydrated entity properties, populated after redaction.
    /// Empty until the hydration stage fetches full properties for this node.
    pub properties: HashMap<String, ColumnValue>,
    /// The key properties its row checks read, looked up by its id after the base query.
    pub keys: HashMap<String, ColumnValue>,
}

impl PartialEq for NodeRef {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.entity_type == other.entity_type
    }
}

impl Eq for NodeRef {}

impl NodeRef {
    pub fn new(id: impl Into<Key>, entity_type: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            entity_type: entity_type.into(),
            properties: HashMap::new(),
            keys: HashMap::new(),
        }
    }

    /// The key the check sends its provider for this node: its own id, or the key property
    /// looked up by it.
    pub fn check_key(&self, check: &EntityAuthConfig) -> Option<Key> {
        match check.key_source {
            KeySource::Identity => key(check.key_kind, &identity_value(&self.id)?),
            KeySource::Redaction | KeySource::Lookup => {
                key(check.key_kind, self.keys.get(&check.auth_id_column)?)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryResultRow {
    columns: HashMap<String, ColumnValue>,
    /// Nodes discovered dynamically at query time that need redaction checks (e.g nodes where their entity type is not known ahead of time).
    dynamic_nodes: Vec<NodeRef>,
    /// Relationship kinds for each hop in a path finding query.
    /// `edge_kinds[i]` is the relationship that connects `dynamic_nodes[i]` to `dynamic_nodes[i+1]`.
    /// Empty for non-path-finding queries.
    edge_kinds: Vec<String>,
    /// The key properties of each returned node, by alias, looked up after the base query.
    keys: HashMap<String, HashMap<String, ColumnValue>>,
    authorized: bool,
}

impl QueryResultRow {
    fn new(
        columns: HashMap<String, ColumnValue>,
        dynamic_nodes: Vec<NodeRef>,
        edge_kinds: Vec<String>,
    ) -> Self {
        Self {
            columns,
            dynamic_nodes,
            edge_kinds,
            keys: HashMap::new(),
            authorized: true,
        }
    }

    pub fn get(&self, column: &str) -> Option<&ColumnValue> {
        self.columns.get(column)
    }

    pub fn columns(&self) -> impl Iterator<Item = (&String, &ColumnValue)> {
        self.columns.iter()
    }

    pub fn get_id(&self, node: &RedactionNode) -> Option<Key> {
        identity(self.columns.get(&node.id_column)?)
    }

    /// The node's identity key: its primary key column, or its redaction id when the base query
    /// returned no separate primary key; a composite identity reads one column per part.
    pub fn get_public_id(&self, node: &RedactionNode) -> Option<Key> {
        match node.parts {
            1 => self
                .columns
                .get(&node.pk_column)
                .and_then(identity)
                .or_else(|| self.get_id(node)),
            parts => self.key(&node.pk_column, parts),
        }
    }

    /// The key held under `column`: that column's value, or a composite of `parts` part columns.
    pub fn key(&self, column: &str, parts: usize) -> Option<Key> {
        match parts {
            1 => identity(self.columns.get(column)?),
            parts => (0..parts)
                .map(|part| identity(self.columns.get(&identity_part_column(column, part))?))
                .collect::<Option<Vec<_>>>()
                .map(Key::Composite),
        }
    }

    pub fn get_type(&self, node: &RedactionNode) -> Option<&str> {
        self.columns
            .get(&node.type_column)?
            .as_string()
            .map(|s| s.as_str())
    }

    /// The key the check sends its provider for this row's node.
    pub fn check_key(&self, node: &RedactionNode, check: &EntityAuthConfig) -> Option<Key> {
        let value = match check.key_source {
            KeySource::Identity => self
                .columns
                .get(&node.pk_column)
                .or_else(|| self.columns.get(&node.id_column)),
            KeySource::Redaction => self.columns.get(&node.id_column),
            KeySource::Lookup => self.keys.get(&node.alias)?.get(&check.auth_id_column),
        };
        key(check.key_kind, value?)
    }

    pub fn path_nodes(&self) -> &[NodeRef] {
        &self.dynamic_nodes
    }

    /// For neighbors queries, returns the neighbor node if present.
    /// Returns None for non-neighbors queries or if neighbor is null.
    pub fn neighbor_node(&self) -> Option<&NodeRef> {
        self.dynamic_nodes.first()
    }

    pub fn dynamic_nodes(&self) -> &[NodeRef] {
        &self.dynamic_nodes
    }

    pub fn dynamic_nodes_mut(&mut self) -> &mut [NodeRef] {
        &mut self.dynamic_nodes
    }

    pub fn edge_kinds(&self) -> &[String] {
        &self.edge_kinds
    }

    pub fn is_authorized(&self) -> bool {
        self.authorized
    }

    pub fn set_unauthorized(&mut self) {
        self.authorized = false;
    }

    pub fn entity_properties(&self, alias: &str) -> HashMap<String, ColumnValue> {
        let prefix = format!("{alias}_");
        let mut props = HashMap::new();
        for (name, value) in &self.columns {
            if name.starts_with(internal_column_prefix()) {
                continue;
            }
            if let Some(prop_name) = name.strip_prefix(&prefix) {
                props.insert(prop_name.to_string(), value.clone());
            }
        }
        props
    }

    pub fn column(&self, name: &str) -> Option<&ColumnValue> {
        self.columns.get(name)
    }

    pub fn get_column_i64(&self, column: &str) -> Option<i64> {
        self.columns.get(column)?.as_int64().copied()
    }

    pub fn get_column_string(&self, column: &str) -> Option<String> {
        self.columns.get(column)?.as_string().cloned()
    }

    pub fn set_column(&mut self, column: String, value: ColumnValue) {
        self.columns.insert(column, value);
    }

    pub fn remove_column(&mut self, column: &str) {
        self.columns.remove(column);
    }
}

/// Key properties of one entity to read by node id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyLookup {
    pub entity: String,
    pub ids: Vec<Key>,
    pub properties: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    rows: Vec<QueryResultRow>,
    ctx: ResultContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryResultError;

impl std::fmt::Display for QueryResultError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("query result has invalid dynamic authorization metadata")
    }
}

impl std::error::Error for QueryResultError {}

enum DynamicNodeColumns<'a> {
    Paths(Vec<&'a ListArray>),
    Neighbor {
        ids: &'a dyn Array,
        types: &'a StringArray,
    },
}

impl<'a> DynamicNodeColumns<'a> {
    fn new(batch: &'a RecordBatch, ctx: &ResultContext) -> Result<Self, QueryResultError> {
        match ctx.query_type {
            Some(QueryType::PathFinding) => Ok(Self::Paths(vec![column(batch, path_column())?])),
            Some(QueryType::Neighbors) => Ok(Self::Neighbor {
                ids: batch
                    .column_by_name(neighbor_id_column())
                    .ok_or(QueryResultError)?
                    .as_ref(),
                types: column(batch, neighbor_type_column())?,
            }),
            Some(QueryType::Traversal) => Ok(Self::Paths(
                ctx.edges()
                    .iter()
                    .filter_map(|edge| edge.path_column.as_deref())
                    .map(|name| column(batch, name))
                    .collect::<Result<_, _>>()?,
            )),
            _ => Ok(Self::Paths(Vec::new())),
        }
    }

    fn nodes(&self, row: usize) -> Result<Vec<NodeRef>, QueryResultError> {
        match self {
            Self::Paths(paths) => {
                let mut result = Vec::new();
                for path in paths {
                    result.extend(path_nodes(path, row)?);
                }
                Ok(result)
            }
            Self::Neighbor { ids, types } => {
                if types.is_null(row) {
                    return Err(QueryResultError);
                }
                Ok(vec![NodeRef::new(array_key(*ids, row)?, types.value(row))])
            }
        }
    }
}

fn column<'a, A: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a A, QueryResultError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| QueryResultError)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref()
        .ok_or(QueryResultError)
}

fn path_nodes(path: &ListArray, row: usize) -> Result<Vec<NodeRef>, QueryResultError> {
    if path.is_null(row) {
        return Err(QueryResultError);
    }
    let values = path.value(row);
    let nodes = values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or(QueryResultError)?;
    if nodes.null_count() > 0 || nodes.num_columns() < 2 {
        return Err(QueryResultError);
    }
    let ids = nodes.column(0).as_ref();
    let types = nodes
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(QueryResultError)?;
    if types.null_count() > 0 {
        return Err(QueryResultError);
    }
    (0..nodes.len())
        .map(|index| Ok(NodeRef::new(array_key(ids, index)?, types.value(index))))
        .collect()
}

/// The identity key at `index` of an id column, in the column's own type; a null fails closed.
fn array_key(ids: &dyn Array, index: usize) -> Result<Key, QueryResultError> {
    if ids.is_null(index) {
        return Err(QueryResultError);
    }
    if let Some(ids) = ids.as_any().downcast_ref::<Int64Array>() {
        return Ok(Key::Int(ids.value(index)));
    }
    ids.as_any()
        .downcast_ref::<StringArray>()
        .map(|ids| Key::Text(ids.value(index).to_string()))
        .ok_or(QueryResultError)
}

impl QueryResult {
    pub fn from_batches(
        batches: &[RecordBatch],
        ctx: &ResultContext,
    ) -> Result<Self, QueryResultError> {
        let is_path_finding = ctx.query_type == Some(QueryType::PathFinding);

        let mut rows = Vec::new();
        for batch in batches {
            let dynamic_nodes = DynamicNodeColumns::new(batch, ctx)?;
            for row_idx in 0..batch.num_rows() {
                let columns = ArrowUtils::extract_row(batch, row_idx);
                let dynamic_nodes = dynamic_nodes.nodes(row_idx)?;

                let edge_kinds = if is_path_finding {
                    ArrowUtils::get_string_list(batch, edge_kinds_column(), row_idx)
                } else {
                    Vec::new()
                };

                rows.push(QueryResultRow::new(columns, dynamic_nodes, edge_kinds));
            }
        }

        Ok(Self {
            rows,
            ctx: ctx.clone(),
        })
    }

    pub fn rows(&self) -> &[QueryResultRow] {
        &self.rows
    }

    /// Remove authorized rows that do not satisfy `predicate`.
    /// Used for post-hydration filtering (e.g. virtual column filters).
    pub fn retain_rows(&mut self, mut predicate: impl FnMut(&QueryResultRow) -> bool) {
        self.rows.retain(|row| !row.authorized || predicate(row));
    }

    pub fn ctx(&self) -> &ResultContext {
        &self.ctx
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &QueryResultRow> {
        self.rows.iter()
    }

    pub fn resource_checks(&self) -> Vec<ResourceCheck> {
        let mut keys = HashMap::new();
        let ctx = &self.ctx;
        for row in &self.rows {
            for node in ctx.nodes() {
                let Some(entity) = row.get_type(node) else {
                    continue;
                };
                for check in ctx.entity_checks(entity) {
                    request(&mut keys, check, row.check_key(node, check));
                }
            }
            for node_ref in &row.dynamic_nodes {
                for check in ctx.entity_checks(&node_ref.entity_type) {
                    request(&mut keys, check, node_ref.check_key(check));
                }
            }
        }

        keys.into_iter()
            .map(|((provider, resource_type, ability), keys)| ResourceCheck {
                provider: provider.to_string(),
                resource_type: resource_type.to_string(),
                ids: keys.into_iter().collect(),
                ability: ability.to_string(),
            })
            .collect()
    }

    /// The key properties to read for each entity, by node id: the static nodes whose checks
    /// key rows by a property the row does not carry, and the dynamic nodes whose checks key
    /// rows by anything but their id.
    pub fn key_lookups(&self) -> Vec<KeyLookup> {
        let mut lookups: HashMap<String, (HashSet<Key>, HashSet<String>)> = HashMap::new();
        let mut need = |entity: &str,
                        id: Option<Key>,
                        carried: &[KeySource],
                        found: Option<&HashMap<String, ColumnValue>>| {
            let properties: Vec<String> = self
                .ctx
                .entity_checks(entity)
                .iter()
                .filter(|check| !carried.contains(&check.key_source))
                .filter(|check| {
                    found.is_none_or(|found| !found.contains_key(&check.auth_id_column))
                })
                .map(|check| check.auth_id_column.clone())
                .collect();
            if let (Some(id), false) = (id, properties.is_empty()) {
                let (ids, columns) = lookups.entry(entity.to_string()).or_default();
                ids.insert(id);
                columns.extend(properties);
            }
        };
        for row in &self.rows {
            for node in self.ctx.nodes() {
                if let Some(entity) = row.get_type(node) {
                    let carried = [KeySource::Identity, KeySource::Redaction];
                    let found = row.keys.get(&node.alias);
                    need(entity, row.get_public_id(node), &carried, found);
                }
            }
            for node_ref in &row.dynamic_nodes {
                let found = Some(&node_ref.keys);
                need(
                    &node_ref.entity_type,
                    Some(node_ref.id.clone()),
                    &[KeySource::Identity],
                    found,
                );
            }
        }
        lookups
            .into_iter()
            .map(|(entity, (ids, properties))| {
                let mut ids: Vec<Key> = ids.into_iter().collect();
                ids.sort_unstable();
                let mut properties: Vec<String> = properties.into_iter().collect();
                properties.sort_unstable();
                KeyLookup {
                    entity,
                    ids,
                    properties,
                }
            })
            .collect()
    }

    /// Fills the looked-up key properties into every node they were read for.
    pub fn set_lookup_keys(
        &mut self,
        found: &HashMap<(String, Key), HashMap<String, ColumnValue>>,
    ) {
        let nodes: Vec<RedactionNode> = self.ctx.nodes().cloned().collect();
        for row in &mut self.rows {
            for node in &nodes {
                let key = row
                    .get_type(node)
                    .zip(row.get_public_id(node))
                    .map(|(entity, id)| (entity.to_string(), id));
                if let Some(keys) = key.and_then(|key| found.get(&key)) {
                    row.keys.insert(node.alias.clone(), keys.clone());
                }
            }
            for node_ref in &mut row.dynamic_nodes {
                if let Some(keys) = found.get(&(node_ref.entity_type.clone(), node_ref.id.clone()))
                {
                    node_ref.keys = keys.clone();
                }
            }
        }
    }

    /// Apply authorization results to rows. Fails closed: any row containing a node
    /// that cannot be verified as authorized is marked unauthorized.
    pub fn apply_authorizations(&mut self, authorizations: &[ResourceAuthorization]) -> usize {
        let mut redacted_count = 0;
        let ctx = &self.ctx;
        for row in self.rows.iter_mut().filter(|row| row.authorized) {
            let statics = ctx.nodes().all(|node| {
                ctx.stage_checked(&node.alias)
                    || row.get_type(node).is_some_and(|entity| {
                        allows(ctx.entity_checks(entity), authorizations, |check| {
                            row.check_key(node, check)
                        })
                    })
            });
            let dynamics = row.dynamic_nodes.iter().all(|node_ref| {
                allows(
                    ctx.entity_checks(&node_ref.entity_type),
                    authorizations,
                    |check| node_ref.check_key(check),
                )
            });
            if !(statics && dynamics) {
                row.set_unauthorized();
                redacted_count += 1;
            }
        }
        redacted_count
    }

    pub fn authorized_rows(&self) -> impl Iterator<Item = &QueryResultRow> {
        self.rows.iter().filter(|r| r.authorized)
    }

    pub fn authorized_rows_mut(&mut self) -> impl Iterator<Item = &mut QueryResultRow> {
        self.rows.iter_mut().filter(|r| r.authorized)
    }

    pub fn authorized_count(&self) -> usize {
        self.rows.iter().filter(|r| r.authorized).count()
    }

    /// Drops rows past the page window (the overfetched `has_more` probe).
    pub fn truncate(&mut self, len: usize) {
        self.rows.truncate(len);
    }
}

type Requests<'c> = HashMap<(&'c str, &'c str, &'c str), HashSet<Key>>;

fn request<'c>(requests: &mut Requests<'c>, check: &'c EntityAuthConfig, key: Option<Key>) {
    if let Some(key) = key {
        requests
            .entry((
                check.provider.as_str(),
                check.resource_type.as_str(),
                check.ability.as_str(),
            ))
            .or_default()
            .insert(key);
    }
}

/// Every check of the node allows the key it derives from the row; a node without checks or
/// without a key fails closed.
fn allows(
    checks: &[EntityAuthConfig],
    authorizations: &[ResourceAuthorization],
    key: impl Fn(&EntityAuthConfig) -> Option<Key>,
) -> bool {
    !checks.is_empty()
        && checks.iter().all(|check| {
            key(check).is_some_and(|key| {
                authorizations
                    .iter()
                    .find(|a| {
                        a.provider == check.provider && a.resource_type == check.resource_type
                    })
                    .and_then(|answer| answer.authorized.get(&key).copied())
                    == Some(true)
            })
        })
}

/// An identity column value as the key it holds; identities are integers or strings.
pub fn identity(value: &ColumnValue) -> Option<Key> {
    match value {
        ColumnValue::Int64(id) => Some(Key::Int(*id)),
        ColumnValue::String(id) => Some(Key::Text(id.clone())),
        _ => None,
    }
}

/// The column value an identity key was read from.
pub fn identity_value(key: &Key) -> Option<ColumnValue> {
    match key {
        Key::Int(id) => Some(ColumnValue::Int64(*id)),
        Key::Text(id) | Key::Uuid(id) => Some(ColumnValue::String(id.clone())),
        Key::Composite(_) => None,
    }
}

/// A row value as the key of a check, in the type the check's provider keys by.
fn key(kind: KeyKind, value: &ColumnValue) -> Option<Key> {
    match (kind, value) {
        (KeyKind::Int, ColumnValue::Int64(id)) => Some(Key::Int(*id)),
        (KeyKind::Text, ColumnValue::String(key)) => Some(Key::Text(key.clone())),
        (KeyKind::Uuid, ColumnValue::String(key)) => Some(Key::Uuid(key.clone())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray, TimestampMillisecondArray, UInt64Array};
    use arrow::datatypes::{Field, Schema};
    use compiler::EntityAuthConfig;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn answers<const N: usize>(answers: [(i64, bool); N]) -> HashMap<Key, bool> {
        answers
            .into_iter()
            .map(|(id, ok)| (Key::Int(id), ok))
            .collect()
    }

    fn make_batch(columns: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<Arc<dyn Array>> = columns.into_iter().map(|(_, arr)| arr).collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    fn make_test_batch() -> RecordBatch {
        make_batch(vec![
            ("_orbit_u_id", Arc::new(Int64Array::from(vec![1, 2, 3]))),
            (
                "_orbit_u_type",
                Arc::new(StringArray::from(vec!["User", "User", "User"])),
            ),
            (
                "_orbit_p_id",
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ),
            (
                "_orbit_p_type",
                Arc::new(StringArray::from(vec!["Project", "Project", "Project"])),
            ),
        ])
    }

    fn test_ctx() -> ResultContext {
        let mut ctx = ResultContext::new();
        ctx.add_node("u", "User");
        ctx.add_node("p", "Project");
        ctx.add_entity_auth(
            "User",
            EntityAuthConfig {
                resource_type: "user".to_string(),
                ability: "read_user".to_string(),
                auth_id_column: "id".to_string(),
                key_source: KeySource::Identity,
                key_kind: KeyKind::Int,
                provider: "rails".to_string(),
            },
        );
        ctx.add_entity_auth(
            "Project",
            EntityAuthConfig {
                resource_type: "project".to_string(),
                ability: "read".to_string(),
                auth_id_column: "id".to_string(),
                key_source: KeySource::Identity,
                key_kind: KeyKind::Int,
                provider: "rails".to_string(),
            },
        );
        ctx
    }

    fn full_auth() -> Vec<ResourceAuthorization> {
        vec![
            ResourceAuthorization {
                provider: "rails".to_string(),
                resource_type: "user".to_string(),
                authorized: answers([(1, true), (2, true), (3, true)]),
            },
            ResourceAuthorization {
                provider: "rails".to_string(),
                resource_type: "project".to_string(),
                authorized: answers([(100, true), (200, true), (300, true)]),
            },
        ]
    }

    mod string_identity_tests {
        use super::*;
        use arrow::array::{ListArray, StructArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{DataType, Fields};

        #[test]
        fn neighbor_ids_decode_in_their_column_type() {
            let batch = make_batch(vec![
                (
                    neighbor_id_column(),
                    Arc::new(StringArray::from(vec!["T1", "T2"])),
                ),
                (
                    neighbor_type_column(),
                    Arc::new(StringArray::from(vec!["SlackWorkspace"; 2])),
                ),
            ]);
            let ctx = ResultContext::new().with_query_type(QueryType::Neighbors);
            let result = QueryResult::from_batches(&[batch], &ctx).unwrap();
            let ids: Vec<&Key> = result
                .rows()
                .iter()
                .filter_map(|row| row.neighbor_node().map(|node| &node.id))
                .collect();
            assert_eq!(ids, [&Key::Text("T1".into()), &Key::Text("T2".into())]);
        }

        #[test]
        fn path_nodes_keep_text_ids_and_a_null_id_fails_closed() {
            let fields = Fields::from(vec![
                Field::new("id", DataType::Utf8, true),
                Field::new("kind", DataType::Utf8, false),
            ]);
            let path = |ids: Vec<Option<&str>>| {
                let len = ids.len() as i32;
                let nodes = StructArray::new(
                    fields.clone(),
                    vec![
                        Arc::new(StringArray::from(ids)) as Arc<dyn Array>,
                        Arc::new(StringArray::from(vec!["Thread"; len as usize])),
                    ],
                    None,
                );
                let item = Arc::new(Field::new("item", DataType::Struct(fields.clone()), false));
                let offsets = OffsetBuffer::new(vec![0, len].into());
                make_batch(vec![(
                    path_column(),
                    Arc::new(ListArray::new(item, offsets, Arc::new(nodes), None)),
                )])
            };
            let ctx = ResultContext::new().with_query_type(QueryType::PathFinding);
            let result =
                QueryResult::from_batches(&[path(vec![Some("a"), Some("b")])], &ctx).unwrap();
            let ids: Vec<&Key> = result.rows()[0]
                .path_nodes()
                .iter()
                .map(|node| &node.id)
                .collect();
            assert_eq!(ids, [&Key::Text("a".into()), &Key::Text("b".into())]);
            assert!(QueryResult::from_batches(&[path(vec![Some("a"), None])], &ctx).is_err());
        }
    }

    mod node_ref_tests {
        use super::*;

        #[test]
        fn new_creates_node_ref() {
            let node = NodeRef::new(Key::Int(42), "User");
            assert_eq!(node.id, Key::Int(42));
            assert_eq!(node.entity_type, "User");
        }

        #[test]
        fn new_accepts_string() {
            let node = NodeRef::new(Key::Int(1), String::from("Project"));
            assert_eq!(node.entity_type, "Project");
        }

        #[test]
        fn equality() {
            let a = NodeRef::new(Key::Int(1), "User");
            let b = NodeRef::new(Key::Int(1), "User");
            let c = NodeRef::new(Key::Int(2), "User");
            let d = NodeRef::new(Key::Int(1), "Project");

            assert_eq!(a, b);
            assert_ne!(a, c);
            assert_ne!(a, d);
        }

        #[test]
        fn clone() {
            let original = NodeRef::new(Key::Int(99), "Group");
            let cloned = original.clone();
            assert_eq!(original, cloned);
        }
    }

    mod query_result_row_tests {
        use super::*;

        #[test]
        fn get_returns_column_value() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            let row = &result.rows()[0];

            assert_eq!(row.get("_orbit_u_id"), Some(&ColumnValue::Int64(1)));
            assert_eq!(
                row.get("_orbit_u_type"),
                Some(&ColumnValue::String("User".to_string()))
            );
            assert_eq!(row.get("nonexistent"), None);
        }

        #[test]
        fn get_id_extracts_redaction_auth_id() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            let row = &result.rows()[0];
            let u = result.ctx().get("u").unwrap();
            let p = result.ctx().get("p").unwrap();

            assert_eq!(row.get_id(u), Some(Key::Int(1)));
            assert_eq!(row.get_id(p), Some(Key::Int(100)));
        }

        #[test]
        fn get_public_id_falls_back_to_redaction_id_when_pk_is_absent() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            let row = &result.rows()[0];
            let u = result.ctx().get("u").unwrap();

            assert_eq!(row.get_public_id(u), Some(Key::Int(1)));
        }

        #[test]
        fn get_public_id_prefers_pk_for_indirect_auth_entities() {
            let batch = make_batch(vec![
                ("_orbit_d_id", Arc::new(Int64Array::from(vec![1000]))),
                ("_orbit_d_pk", Arc::new(Int64Array::from(vec![5000]))),
                (
                    "_orbit_d_type",
                    Arc::new(StringArray::from(vec!["Definition"])),
                ),
            ]);

            let mut ctx = ResultContext::new();
            ctx.add_node("d", "Definition");

            let result = QueryResult::from_batches(&[batch], &ctx).unwrap();
            let row = &result.rows()[0];
            let d = result.ctx().get("d").unwrap();

            assert_eq!(row.get_id(d), Some(Key::Int(1000)));
            assert_eq!(row.get_public_id(d), Some(Key::Int(5000)));
        }

        #[test]
        fn get_type_extracts_node_type() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            let row = &result.rows()[0];
            let u = result.ctx().get("u").unwrap();
            let p = result.ctx().get("p").unwrap();

            assert_eq!(row.get_type(u), Some("User"));
            assert_eq!(row.get_type(p), Some("Project"));
        }

        #[test]
        fn check_key_reads_the_redaction_id_in_the_check_key_type() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            let row = &result.rows()[0];
            let u = result.ctx().get("u").unwrap();
            let p = result.ctx().get("p").unwrap();
            let user = &result.ctx().entity_checks("User")[0];
            let project = &result.ctx().entity_checks("Project")[0];

            assert_eq!(row.check_key(u, user), Some(Key::Int(1)));
            assert_eq!(row.check_key(p, project), Some(Key::Int(100)));
            let text = EntityAuthConfig {
                key_kind: KeyKind::Text,
                ..user.clone()
            };
            assert_eq!(row.check_key(u, &text), None);
        }

        #[test]
        fn unknown_alias_returns_none() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            let row = &result.rows()[0];

            assert!(result.ctx().get("nonexistent").is_none());

            let ghost = RedactionNode {
                alias: "ghost".to_string(),
                entity_type: "Ghost".to_string(),
                pk_column: "_orbit_ghost_id".to_string(),
                id_column: "_orbit_ghost_id".to_string(),
                type_column: "_orbit_ghost_type".to_string(),
                parts: 1,
            };
            assert_eq!(row.get_id(&ghost), None);
            assert_eq!(row.get_type(&ghost), None);
            let user = &result.ctx().entity_checks("User")[0];
            assert!(row.check_key(&ghost, user).is_none());
        }

        #[test]
        fn authorized_by_default() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            for row in result.rows() {
                assert!(row.is_authorized());
            }
        }

        #[test]
        fn set_unauthorized() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            assert!(result.rows()[0].is_authorized());

            result.rows[0].set_unauthorized();
            assert!(!result.rows()[0].is_authorized());
        }

        #[test]
        fn columns_iterator_returns_all_columns() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            let row = &result.rows()[0];

            let column_names: HashSet<&str> =
                row.columns().map(|(name, _)| name.as_str()).collect();

            assert!(column_names.contains("_orbit_u_id"));
            assert!(column_names.contains("_orbit_u_type"));
            assert!(column_names.contains("_orbit_p_id"));
            assert!(column_names.contains("_orbit_p_type"));
            assert_eq!(column_names.len(), 4);
        }

        #[test]
        fn columns_iterator_returns_correct_values() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            let row = &result.rows()[0];

            let columns: HashMap<&str, &ColumnValue> =
                row.columns().map(|(k, v)| (k.as_str(), v)).collect();

            assert_eq!(columns.get("_orbit_u_id"), Some(&&ColumnValue::Int64(1)));
            assert_eq!(
                columns.get("_orbit_u_type"),
                Some(&&ColumnValue::String("User".to_string()))
            );
            assert_eq!(columns.get("_orbit_p_id"), Some(&&ColumnValue::Int64(100)));
        }
    }

    mod query_result_tests {
        use super::*;

        #[test]
        fn from_batches_empty() {
            let result = QueryResult::from_batches(&[], &ResultContext::new()).unwrap();
            assert!(result.is_empty());
            assert_eq!(result.len(), 0);
        }

        #[test]
        fn from_batches_multiple_batches() {
            let batch1 = make_batch(vec![
                ("_orbit_u_id", Arc::new(Int64Array::from(vec![1, 2]))),
                (
                    "_orbit_u_type",
                    Arc::new(StringArray::from(vec!["User", "User"])),
                ),
            ]);
            let batch2 = make_batch(vec![
                ("_orbit_u_id", Arc::new(Int64Array::from(vec![3, 4]))),
                (
                    "_orbit_u_type",
                    Arc::new(StringArray::from(vec!["User", "User"])),
                ),
            ]);

            let mut ctx = ResultContext::new();
            ctx.add_node("u", "User");

            let result = QueryResult::from_batches(&[batch1, batch2], &ctx).unwrap();
            assert_eq!(result.len(), 4);

            let u = result.ctx().get("u").unwrap();
            let ids: Vec<Key> = result.iter().filter_map(|r| r.get_id(u)).collect();
            assert_eq!(ids, [1, 2, 3, 4].map(Key::Int));
        }

        #[test]
        fn len_and_is_empty() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            assert_eq!(result.len(), 3);
            assert!(!result.is_empty());
        }

        #[test]
        fn iter_returns_all_rows() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            let u = result.ctx().get("u").unwrap();
            let ids: Vec<Key> = result.iter().filter_map(|r| r.get_id(u)).collect();
            assert_eq!(ids, [1, 2, 3].map(Key::Int));
        }

        #[test]
        fn authorized_rows_filters_correctly() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            result.rows[1].set_unauthorized();

            let u = result.ctx().get("u").unwrap();
            let authorized: Vec<_> = result.authorized_rows().collect();
            assert_eq!(authorized.len(), 2);

            let ids: Vec<Key> = authorized.iter().filter_map(|r| r.get_id(u)).collect();
            assert_eq!(ids, [1, 3].map(Key::Int));
        }

        #[test]
        fn authorized_count() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            assert_eq!(result.authorized_count(), 3);

            result.rows[0].set_unauthorized();
            assert_eq!(result.authorized_count(), 2);

            result.rows[1].set_unauthorized();
            result.rows[2].set_unauthorized();
            assert_eq!(result.authorized_count(), 0);
        }
    }

    mod apply_authorizations_tests {
        use super::*;

        #[test]
        fn all_authorized_returns_zero_redacted() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            let redacted = result.apply_authorizations(&full_auth());
            assert_eq!(redacted, 0);
            assert_eq!(result.authorized_count(), 3);
        }

        #[test]
        fn single_deny_redacts_one_row() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();

            let authorizations = vec![
                ResourceAuthorization {
                    provider: "rails".to_string(),
                    resource_type: "user".to_string(),
                    authorized: answers([(1, true), (2, true), (3, true)]),
                },
                ResourceAuthorization {
                    provider: "rails".to_string(),
                    resource_type: "project".to_string(),
                    authorized: answers([(100, true), (200, false), (300, true)]),
                },
            ];

            let redacted = result.apply_authorizations(&authorizations);

            assert_eq!(redacted, 1);
            assert_eq!(result.authorized_count(), 2);
            assert!(result.rows()[0].is_authorized());
            assert!(!result.rows()[1].is_authorized());
            assert!(result.rows()[2].is_authorized());
        }

        #[test]
        fn multiple_denies_redact_multiple_rows() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();

            let authorizations = vec![
                ResourceAuthorization {
                    provider: "rails".to_string(),
                    resource_type: "user".to_string(),
                    authorized: answers([(1, false), (2, true), (3, false)]),
                },
                ResourceAuthorization {
                    provider: "rails".to_string(),
                    resource_type: "project".to_string(),
                    authorized: answers([(100, true), (200, true), (300, true)]),
                },
            ];

            let redacted = result.apply_authorizations(&authorizations);

            assert_eq!(redacted, 2);
            assert_eq!(result.authorized_count(), 1);
            assert!(!result.rows()[0].is_authorized());
            assert!(result.rows()[1].is_authorized());
            assert!(!result.rows()[2].is_authorized());
        }

        #[test]
        fn fail_closed_unknown_entity_type() {
            let mut ctx = ResultContext::new();
            ctx.add_node("u", "User");
            ctx.add_node("p", "Project");
            let mut result = QueryResult::from_batches(&[make_test_batch()], &ctx).unwrap();
            let redacted = result.apply_authorizations(&[]);
            assert_eq!(redacted, 3);
            assert_eq!(result.authorized_count(), 0);
        }

        #[test]
        fn a_check_less_node_passes_only_once_its_read_was_proved() {
            let ctx = || {
                let mut ctx = test_ctx();
                ctx.add_node("i", "JiraIssue");
                ctx
            };
            let batch = || {
                make_batch(vec![
                    ("_orbit_i_id", Arc::new(Int64Array::from(vec![7, 8, 9]))),
                    (
                        "_orbit_i_type",
                        Arc::new(StringArray::from(vec!["JiraIssue"; 3])),
                    ),
                    ("_orbit_u_id", Arc::new(Int64Array::from(vec![1, 2, 3]))),
                    (
                        "_orbit_u_type",
                        Arc::new(StringArray::from(vec!["User"; 3])),
                    ),
                    (
                        "_orbit_p_id",
                        Arc::new(Int64Array::from(vec![100, 200, 300])),
                    ),
                    (
                        "_orbit_p_type",
                        Arc::new(StringArray::from(vec!["Project"; 3])),
                    ),
                ])
            };
            let unproved = ctx();
            let mut result = QueryResult::from_batches(&[batch()], &unproved).unwrap();
            assert_eq!(result.apply_authorizations(&full_auth()), 3);

            let mut proved = ctx();
            proved.mark_proved(&std::collections::BTreeSet::from([
                "i".to_string(),
                "u".to_string(),
            ]));
            assert!(proved.stage_checked("i"));
            assert!(!proved.stage_checked("u"), "a node with checks keeps them");
            let mut result = QueryResult::from_batches(&[batch()], &proved).unwrap();
            assert_eq!(result.apply_authorizations(&full_auth()), 0);
            assert_eq!(result.authorized_count(), 3);
        }

        #[test]
        fn every_check_of_an_entity_must_allow_its_row() {
            let mut ctx = test_ctx();
            ctx.add_entity_auth(
                "Project",
                EntityAuthConfig {
                    resource_type: "project".to_string(),
                    ability: "view".to_string(),
                    auth_id_column: "id".to_string(),
                    key_source: KeySource::Identity,
                    key_kind: KeyKind::Int,
                    provider: "cedar".to_string(),
                },
            );
            let mut result = QueryResult::from_batches(&[make_test_batch()], &ctx).unwrap();
            let checks = result.resource_checks();
            let providers: HashSet<&str> = checks.iter().map(|c| c.provider.as_str()).collect();
            assert_eq!(providers, HashSet::from(["rails", "cedar"]));
            let mut authorizations = full_auth();
            authorizations.push(ResourceAuthorization {
                provider: "cedar".to_string(),
                resource_type: "project".to_string(),
                authorized: answers([(100, true), (200, false)]),
            });

            let redacted = result.apply_authorizations(&authorizations);

            assert_eq!(redacted, 2);
            assert_eq!(result.authorized_count(), 1);
        }

        #[test]
        fn fail_closed_missing_resource_authorization() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            let redacted = result.apply_authorizations(&[]);
            assert_eq!(redacted, 3);
            assert_eq!(result.authorized_count(), 0);
        }

        #[test]
        fn fail_closed_partial_resource_authorization() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();

            let authorizations = vec![ResourceAuthorization {
                provider: "rails".to_string(),
                resource_type: "user".to_string(),
                authorized: answers([(1, true), (2, true), (3, true)]),
            }];

            let redacted = result.apply_authorizations(&authorizations);

            assert_eq!(redacted, 3);
            assert_eq!(result.authorized_count(), 0);
        }

        #[test]
        fn fail_closed_missing_id_in_authorization() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();

            let authorizations = vec![
                ResourceAuthorization {
                    provider: "rails".to_string(),
                    resource_type: "user".to_string(),
                    authorized: answers([(1, true)]),
                },
                ResourceAuthorization {
                    provider: "rails".to_string(),
                    resource_type: "project".to_string(),
                    authorized: answers([(100, true), (200, true), (300, true)]),
                },
            ];

            let redacted = result.apply_authorizations(&authorizations);

            assert_eq!(redacted, 2);
            assert_eq!(result.authorized_count(), 1);
            assert!(result.rows()[0].is_authorized());
        }

        #[test]
        fn already_unauthorized_rows_not_double_counted() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx()).unwrap();
            result.rows[0].set_unauthorized();
            let redacted = result.apply_authorizations(&full_auth());
            assert_eq!(redacted, 0);
            assert_eq!(result.authorized_count(), 2);
        }

        #[test]
        fn fail_closed_dynamic_node_unresolvable_owner() {
            let batch = make_batch(vec![
                ("_orbit_u_id", Arc::new(Int64Array::from(vec![1]))),
                ("_orbit_u_type", Arc::new(StringArray::from(vec!["User"]))),
            ]);

            let mut ctx = ResultContext::new();
            ctx.add_node("u", "User");
            ctx.add_entity_auth(
                "User",
                EntityAuthConfig {
                    resource_type: "user".to_string(),
                    ability: "read_user".to_string(),
                    auth_id_column: "id".to_string(),
                    key_source: KeySource::Identity,
                    key_kind: KeyKind::Int,
                    provider: "rails".to_string(),
                },
            );
            ctx.add_entity_auth(
                "Definition",
                EntityAuthConfig {
                    resource_type: "project".to_string(),
                    ability: "read_code".to_string(),
                    auth_id_column: "project_id".to_string(),
                    key_source: KeySource::Redaction,
                    key_kind: KeyKind::Int,
                    provider: "rails".to_string(),
                },
            );

            let mut result = QueryResult::from_batches(&[batch], &ctx).unwrap();
            result.rows[0].dynamic_nodes = vec![NodeRef::new(Key::Int(1000), "Definition")];

            let authorizations = vec![
                ResourceAuthorization {
                    provider: "rails".to_string(),
                    resource_type: "user".to_string(),
                    authorized: answers([(1, true)]),
                },
                ResourceAuthorization {
                    provider: "rails".to_string(),
                    resource_type: "project".to_string(),
                    authorized: answers([(1000, true)]),
                },
            ];

            let redacted = result.apply_authorizations(&authorizations);

            assert_eq!(
                redacted, 1,
                "row must be denied: Definition's owner (Project) is not in the row"
            );
            assert_eq!(result.authorized_count(), 0);
        }

        fn project_row_with_definition() -> QueryResult {
            let batch = make_batch(vec![
                ("_orbit_p_id", Arc::new(Int64Array::from(vec![1000]))),
                (
                    "_orbit_p_type",
                    Arc::new(StringArray::from(vec!["Project"])),
                ),
            ]);

            let mut ctx = ResultContext::new();
            ctx.add_node("p", "Project");
            ctx.add_entity_auth(
                "Project",
                EntityAuthConfig {
                    resource_type: "project".to_string(),
                    ability: "read_code".to_string(),
                    auth_id_column: "id".to_string(),
                    key_source: KeySource::Identity,
                    key_kind: KeyKind::Int,
                    provider: "rails".to_string(),
                },
            );
            ctx.add_entity_auth(
                "Definition",
                EntityAuthConfig {
                    resource_type: "project".to_string(),
                    ability: "read_code".to_string(),
                    auth_id_column: "project_id".to_string(),
                    key_source: KeySource::Redaction,
                    key_kind: KeyKind::Int,
                    provider: "rails".to_string(),
                },
            );

            let mut result = QueryResult::from_batches(&[batch], &ctx).unwrap();
            result.rows[0].dynamic_nodes = vec![NodeRef::new(Key::Int(5000), "Definition")];
            result
        }

        fn project_auth() -> Vec<ResourceAuthorization> {
            vec![ResourceAuthorization {
                provider: "rails".to_string(),
                resource_type: "project".to_string(),
                authorized: answers([(1000, true), (1001, false)]),
            }]
        }

        fn project_of(
            id: i64,
            project: i64,
        ) -> HashMap<(String, Key), HashMap<String, ColumnValue>> {
            HashMap::from([(
                ("Definition".to_string(), Key::Int(id)),
                HashMap::from([("project_id".to_string(), ColumnValue::Int64(project))]),
            )])
        }

        #[test]
        fn dynamic_node_uses_resolved_owner_id() {
            let mut result = project_row_with_definition();
            assert_eq!(
                result.key_lookups(),
                [KeyLookup {
                    entity: "Definition".to_string(),
                    ids: vec![Key::Int(5000)],
                    properties: vec!["project_id".to_string()],
                }]
            );
            result.set_lookup_keys(&project_of(5000, 1000));

            assert!(result.key_lookups().is_empty());
            assert_eq!(result.apply_authorizations(&project_auth()), 0);
            assert_eq!(result.authorized_count(), 1);
        }

        #[test]
        fn dynamic_node_denied_by_own_owner_despite_authorized_sibling() {
            let mut result = project_row_with_definition();
            result.set_lookup_keys(&project_of(5000, 1001));

            assert_eq!(result.apply_authorizations(&project_auth()), 1);
            assert_eq!(result.authorized_count(), 0);
        }

        #[test]
        fn dynamic_node_does_not_borrow_sibling_owner() {
            let mut result = project_row_with_definition();

            assert_eq!(result.apply_authorizations(&project_auth()), 1);
            assert_eq!(result.authorized_count(), 0);
        }

        #[test]
        fn a_returned_node_passes_only_if_each_check_allows_the_key_it_derives() {
            let batch = make_batch(vec![
                ("_orbit_f_pk", Arc::new(Int64Array::from(vec![7, 8]))),
                ("_orbit_f_id", Arc::new(Int64Array::from(vec![100, 100]))),
                (
                    "_orbit_f_type",
                    Arc::new(StringArray::from(vec!["File", "File"])),
                ),
            ]);
            let mut ctx = ResultContext::new();
            ctx.add_node("f", "File");
            let check =
                |provider: &str, resource: &str, column: &str, key_source| EntityAuthConfig {
                    provider: provider.to_string(),
                    resource_type: resource.to_string(),
                    ability: "read".to_string(),
                    auth_id_column: column.to_string(),
                    key_source,
                    key_kind: KeyKind::Int,
                };
            ctx.add_entity_auth(
                "File",
                check("rails", "project", "project_id", KeySource::Redaction),
            );
            ctx.add_entity_auth("File", check("cedar", "file", "id", KeySource::Identity));
            ctx.add_entity_auth(
                "File",
                check("zanzibar", "group", "group_id", KeySource::Lookup),
            );
            let mut result = QueryResult::from_batches(&[batch], &ctx).unwrap();
            assert_eq!(
                result.key_lookups(),
                [KeyLookup {
                    entity: "File".to_string(),
                    ids: vec![Key::Int(7), Key::Int(8)],
                    properties: vec!["group_id".to_string()],
                }]
            );
            let group = |id: i64, group: i64| {
                (
                    ("File".to_string(), Key::Int(id)),
                    HashMap::from([("group_id".to_string(), ColumnValue::Int64(group))]),
                )
            };
            result.set_lookup_keys(&HashMap::from([group(7, 30), group(8, 31)]));
            let answer = |provider: &str, resource: &str, keys| ResourceAuthorization {
                provider: provider.to_string(),
                resource_type: resource.to_string(),
                authorized: keys,
            };
            let authorizations = [
                answer("rails", "project", answers([(100, true)])),
                answer("cedar", "file", answers([(7, true), (8, true)])),
                answer("zanzibar", "group", answers([(30, true), (31, false)])),
            ];
            let checks = result.resource_checks();
            assert_eq!(checks.len(), 3);

            assert_eq!(result.apply_authorizations(&authorizations), 1);
            let f = result.ctx().get("f").unwrap().clone();
            let kept: Vec<Key> = result
                .authorized_rows()
                .filter_map(|row| row.get_public_id(&f))
                .collect();
            assert_eq!(kept, [Key::Int(7)]);
        }

        #[test]
        fn empty_result_returns_zero() {
            let mut result = QueryResult::from_batches(&[], &ResultContext::new()).unwrap();
            let redacted = result.apply_authorizations(&[]);
            assert_eq!(redacted, 0);
            assert_eq!(result.authorized_count(), 0);
        }
    }

    mod null_handling_tests {
        use super::*;

        fn user_ctx() -> ResultContext {
            let mut ctx = ResultContext::new();
            ctx.add_node("u", "User");
            ctx
        }

        #[test]
        fn null_values_extracted_as_null() {
            let batch = make_batch(vec![
                (
                    "_orbit_u_id",
                    Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                ),
                (
                    "_orbit_u_type",
                    Arc::new(StringArray::from(vec![Some("User"), Some("User"), None])),
                ),
            ]);

            let result = QueryResult::from_batches(&[batch], &user_ctx()).unwrap();

            assert_eq!(
                result.rows()[0].get("_orbit_u_id"),
                Some(&ColumnValue::Int64(1))
            );
            assert_eq!(
                result.rows()[1].get("_orbit_u_id"),
                Some(&ColumnValue::Null)
            );
            assert_eq!(
                result.rows()[2].get("_orbit_u_type"),
                Some(&ColumnValue::Null)
            );
        }

        #[test]
        fn null_id_returns_none_from_get_id() {
            let batch = make_batch(vec![
                (
                    "_orbit_u_id",
                    Arc::new(Int64Array::from(vec![Some(1), None])),
                ),
                (
                    "_orbit_u_type",
                    Arc::new(StringArray::from(vec!["User", "User"])),
                ),
            ]);

            let result = QueryResult::from_batches(&[batch], &user_ctx()).unwrap();
            let u = result.ctx().get("u").unwrap();

            assert_eq!(result.rows()[0].get_id(u), Some(Key::Int(1)));
            assert_eq!(result.rows()[1].get_id(u), None);
        }

        #[test]
        fn null_type_returns_none_from_get_type() {
            let batch = make_batch(vec![
                ("_orbit_u_id", Arc::new(Int64Array::from(vec![1, 2]))),
                (
                    "_orbit_u_type",
                    Arc::new(StringArray::from(vec![Some("User"), None])),
                ),
            ]);

            let result = QueryResult::from_batches(&[batch], &user_ctx()).unwrap();
            let u = result.ctx().get("u").unwrap();

            assert_eq!(result.rows()[0].get_type(u), Some("User"));
            assert_eq!(result.rows()[1].get_type(u), None);
        }

        #[test]
        fn check_key_is_none_if_id_null() {
            let batch = make_batch(vec![
                ("_orbit_u_id", Arc::new(Int64Array::from(vec![None]))),
                ("_orbit_u_type", Arc::new(StringArray::from(vec!["User"]))),
            ]);

            let result = QueryResult::from_batches(&[batch], &user_ctx()).unwrap();
            let u = result.ctx().get("u").unwrap();
            let user = test_ctx().entity_checks("User")[0].clone();
            assert!(result.rows()[0].check_key(u, &user).is_none());
        }

        #[test]
        fn a_row_without_a_type_is_redacted() {
            let batch = make_batch(vec![
                ("_orbit_u_id", Arc::new(Int64Array::from(vec![1]))),
                (
                    "_orbit_u_type",
                    Arc::new(StringArray::from(vec![Option::<&str>::None])),
                ),
            ]);

            let mut ctx = user_ctx();
            ctx.add_entity_auth("User", test_ctx().entity_checks("User")[0].clone());
            let mut result = QueryResult::from_batches(&[batch], &ctx).unwrap();
            assert_eq!(result.apply_authorizations(&full_auth()), 1);
        }
    }

    mod from_batches_data_type_tests {
        use super::*;

        #[test]
        fn mixed_data_types_in_batch() {
            let ts_arr = TimestampMillisecondArray::new(vec![1704067200000i64].into(), None);
            let batch = make_batch(vec![
                ("_orbit_u_id", Arc::new(Int64Array::from(vec![1]))),
                ("_orbit_u_type", Arc::new(StringArray::from(vec!["User"]))),
                ("view_count", Arc::new(UInt64Array::from(vec![1000u64]))),
                ("created_at", Arc::new(ts_arr)),
            ]);

            let mut ctx = ResultContext::new();
            ctx.add_node("u", "User");

            let result = QueryResult::from_batches(&[batch], &ctx).unwrap();
            let row = &result.rows()[0];
            let u = result.ctx().get("u").unwrap();

            assert_eq!(row.get_id(u), Some(Key::Int(1)));
            assert_eq!(row.get_type(u), Some("User"));
            assert_eq!(row.get("view_count"), Some(&ColumnValue::Int64(1000)));
            assert_eq!(
                row.get("created_at"),
                Some(&ColumnValue::String("2024-01-01T00:00:00Z".to_string()))
            );
        }
    }
}

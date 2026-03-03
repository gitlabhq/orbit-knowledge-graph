//! Type-safe result schema for redaction processing.

use std::collections::{HashMap, HashSet};

use arrow::array::{
    Array, Int64Array, ListArray, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
};
use arrow::record_batch::RecordBatch;
use query_engine::constants::{
    EDGE_KINDS_COLUMN, NEIGHBOR_ID_COLUMN, NEIGHBOR_TYPE_COLUMN, PATH_COLUMN,
};
use query_engine::{QueryType, RedactionNode, ResultContext};

use super::{ResourceAuthorization, ResourceCheck};

#[derive(Debug, Clone)]
pub struct NodeRef {
    pub id: i64,
    pub entity_type: String,
    /// Hydrated entity properties, populated after redaction.
    /// Empty until the hydration stage fetches full properties for this node.
    pub properties: HashMap<String, ColumnValue>,
}

impl PartialEq for NodeRef {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.entity_type == other.entity_type
    }
}

impl Eq for NodeRef {}

impl NodeRef {
    pub fn new(id: i64, entity_type: impl Into<String>) -> Self {
        Self {
            id,
            entity_type: entity_type.into(),
            properties: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RedactableNodes {
    nodes: Vec<NodeRef>,
}

impl RedactableNodes {
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    pub fn add(&mut self, id: i64, entity_type: impl Into<String>) {
        self.nodes.push(NodeRef::new(id, entity_type));
    }

    pub fn nodes(&self) -> &[NodeRef] {
        &self.nodes
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn group_by_type(&self) -> HashMap<&str, Vec<i64>> {
        let mut groups: HashMap<&str, Vec<i64>> = HashMap::new();
        for node in &self.nodes {
            groups
                .entry(node.entity_type.as_str())
                .or_default()
                .push(node.id);
        }
        groups
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Int64(i64),
    String(String),
    Null,
}

impl ColumnValue {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ColumnValue::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            ColumnValue::String(v) => Some(v.as_str()),
            _ => None,
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
            authorized: true,
        }
    }

    pub fn get(&self, column: &str) -> Option<&ColumnValue> {
        self.columns.get(column)
    }

    pub fn columns(&self) -> impl Iterator<Item = (&String, &ColumnValue)> {
        self.columns.iter()
    }

    pub fn get_id(&self, node: &RedactionNode) -> Option<i64> {
        self.columns.get(&node.id_column)?.as_i64()
    }

    pub fn get_type(&self, node: &RedactionNode) -> Option<&str> {
        self.columns.get(&node.type_column)?.as_str()
    }

    pub fn node_ref(&self, node: &RedactionNode) -> Option<NodeRef> {
        Some(NodeRef::new(self.get_id(node)?, self.get_type(node)?))
    }

    /// For path finding queries, returns all nodes in the path.
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

    pub fn get_column_i64(&self, column: &str) -> Option<i64> {
        self.columns.get(column)?.as_i64()
    }

    pub fn get_column_string(&self, column: &str) -> Option<String> {
        self.columns.get(column)?.as_str().map(|s| s.to_string())
    }

    pub fn set_column(&mut self, column: String, value: ColumnValue) {
        self.columns.insert(column, value);
    }
}

/// Type-safe wrapper around Arrow RecordBatch results for redaction processing.
#[derive(Debug)]
pub struct QueryResult {
    rows: Vec<QueryResultRow>,
    ctx: ResultContext,
}

impl QueryResult {
    pub fn from_batches(batches: &[RecordBatch], ctx: &ResultContext) -> Self {
        let is_path_finding = ctx.query_type == Some(QueryType::PathFinding);
        let is_neighbors = ctx.query_type == Some(QueryType::Neighbors);

        let mut rows = Vec::new();
        for batch in batches {
            let schema = batch.schema();
            for row_idx in 0..batch.num_rows() {
                let mut columns = HashMap::new();
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    columns.insert(
                        field.name().clone(),
                        extract_value(batch.column(col_idx).as_ref(), row_idx),
                    );
                }

                // Extract dynamic nodes (path finding nodes, neighbor nodes)
                // Path finding nodes are extracted from _gkg_path column
                // Neighbor nodes are extracted from _gkg_neighbor_id/_gkg_neighbor_type cols
                let dynamic_nodes = if is_path_finding {
                    extract_path_nodes(batch, row_idx)
                } else if is_neighbors {
                    extract_neighbor_node(batch, row_idx).into_iter().collect()
                } else {
                    Vec::new()
                };

                let edge_kinds = if is_path_finding {
                    extract_edge_kinds(batch, row_idx)
                } else {
                    Vec::new()
                };

                rows.push(QueryResultRow::new(columns, dynamic_nodes, edge_kinds));
            }
        }

        Self {
            rows,
            ctx: ctx.clone(),
        }
    }

    pub fn rows(&self) -> &[QueryResultRow] {
        &self.rows
    }

    pub fn rows_mut(&mut self) -> &mut [QueryResultRow] {
        &mut self.rows
    }

    pub fn ctx(&self) -> &ResultContext {
        &self.ctx
    }

    pub fn node_aliases(&self) -> Vec<String> {
        self.ctx.nodes().map(|n| n.alias.clone()).collect()
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

    pub fn extract_redactable_nodes(&self) -> RedactableNodes {
        let mut nodes = RedactableNodes::new();
        for row in &self.rows {
            // Extract nodes from _gkg_* columns
            for redaction_node in self.ctx.nodes() {
                if let Some(node_ref) = row.node_ref(redaction_node) {
                    nodes.add(node_ref.id, node_ref.entity_type);
                }
            }
            // Extract nodes from dynamic nodes (path finding nodes, neighbor nodes)
            for node_ref in &row.dynamic_nodes {
                nodes.add(node_ref.id, &node_ref.entity_type);
            }
        }
        nodes
    }

    /// Collect all resource IDs that need authorization, grouped by (resource_type, ability).
    /// Static node IDs come from `_gkg_{alias}_id` (enforce.rs selects the configured
    /// auth column there). Dynamic node IDs may need owner resolution from edge columns.
    pub fn resource_checks(&self) -> Vec<ResourceCheck> {
        let mut ids: HashMap<(&str, &str), HashSet<i64>> = HashMap::new();

        for row in &self.rows {
            for redaction_node in self.ctx.nodes() {
                let Some(node_ref) = row.node_ref(redaction_node) else {
                    continue;
                };
                let Some(auth) = self.ctx.get_entity_auth(&node_ref.entity_type) else {
                    continue;
                };
                ids.entry((auth.resource_type.as_str(), auth.ability.as_str()))
                    .or_default()
                    .insert(node_ref.id);
            }

            for node_ref in &row.dynamic_nodes {
                let Some(auth) = self.ctx.get_entity_auth(&node_ref.entity_type) else {
                    continue;
                };
                let auth_id = if let Some(ref owner) = auth.owner_entity {
                    // If owner can't be found, skip — apply_authorizations will
                    // fail-closed since no auth result will exist for this entity.
                    let Some(id) = find_owner_id(row, owner, &self.ctx) else {
                        continue;
                    };
                    id
                } else {
                    node_ref.id
                };
                ids.entry((auth.resource_type.as_str(), auth.ability.as_str()))
                    .or_default()
                    .insert(auth_id);
            }
        }

        ids.into_iter()
            .map(|((resource_type, ability), ids)| ResourceCheck {
                resource_type: resource_type.to_string(),
                ids: ids.into_iter().collect(),
                ability: ability.to_string(),
            })
            .collect()
    }

    /// Apply authorization results to rows. Fails closed: any row containing a node
    /// that cannot be verified as authorized is marked unauthorized.
    ///
    /// A row is marked unauthorized if ANY node in the row:
    /// - Has an entity type not configured for redaction (not in entity_to_resource)
    /// - Has no authorization result from the redaction service
    /// - Is explicitly marked as unauthorized
    ///
    /// For search queries, NULL redaction columns are skipped because multi-node
    /// search uses UNION ALL where each arm only has real values for its own entity
    /// and NULLs for others. A search row is denied only if zero nodes are checkable
    /// or any present node fails authorization.
    ///
    /// For path finding queries, all nodes in the path are checked.
    /// For neighbors queries, the neighbor node is checked.
    pub fn apply_authorizations(&mut self, authorizations: &[ResourceAuthorization]) -> usize {
        let mut redacted_count = 0;
        let redaction_nodes: Vec<_> = self.ctx.nodes().cloned().collect();
        let is_search = self.ctx.query_type == Some(QueryType::Search);

        for row in &mut self.rows {
            if !row.authorized {
                continue;
            }

            // Check redaction nodes (from _gkg_* columns).
            //
            // For multi-node search (UNION ALL), each row only has real values
            // for one entity — the others are NULL from UNION padding. We skip
            // NULL nodes for search queries and only deny if zero nodes were
            // checkable (truly empty row) or any present node is unauthorized.
            let mut checked_any = false;
            for redaction_node in &redaction_nodes {
                let Some(node_ref) = row.node_ref(redaction_node) else {
                    if is_search {
                        continue;
                    }
                    // Fail closed: NULL IDs cannot be verified, so deny the row
                    row.set_unauthorized();
                    redacted_count += 1;
                    break;
                };
                checked_any = true;
                if !is_authorized(&node_ref, authorizations, &self.ctx) {
                    row.set_unauthorized();
                    redacted_count += 1;
                    break;
                }
            }

            // For search queries: if no nodes were checkable at all, deny (fail-closed).
            if row.authorized && is_search && !checked_any {
                row.set_unauthorized();
                redacted_count += 1;
            }

            // Dynamic nodes (path finding, neighbors): entity type discovered at runtime,
            // so auth ID may need resolution from edge columns
            if row.authorized {
                for node_ref in &row.dynamic_nodes {
                    let mut node_ref = node_ref.clone();
                    if !resolve_dynamic_auth_id(row, &mut node_ref, &self.ctx)
                        || !is_authorized(&node_ref, authorizations, &self.ctx)
                    {
                        row.set_unauthorized();
                        redacted_count += 1;
                        break;
                    }
                }
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
}

fn is_authorized(
    node_ref: &NodeRef,
    authorizations: &[ResourceAuthorization],
    ctx: &ResultContext,
) -> bool {
    let Some(auth_config) = ctx.get_entity_auth(&node_ref.entity_type) else {
        return false;
    };
    let Some(auth) = authorizations
        .iter()
        .find(|a| a.resource_type == auth_config.resource_type)
    else {
        return false;
    };
    auth.authorized.get(&node_ref.id).copied().unwrap_or(false)
}

/// For indirect-auth entities (owner_entity is set in EntityAuthConfig), rewrite
/// node_ref.id to the owner entity's ID so is_authorized can use it directly.
/// Returns false if the entity requires owner resolution but the owner cannot be
/// found — the caller must deny the row (fail-closed).
fn resolve_dynamic_auth_id(
    row: &QueryResultRow,
    node_ref: &mut NodeRef,
    ctx: &ResultContext,
) -> bool {
    let Some(auth_config) = ctx.get_entity_auth(&node_ref.entity_type) else {
        return true;
    };
    let Some(ref owner) = auth_config.owner_entity else {
        return true;
    };
    match find_owner_id(row, owner, ctx) {
        Some(owner_id) => {
            node_ref.id = owner_id;
            true
        }
        None => false,
    }
}

/// Find the ID of an owner entity in a row by searching static and dynamic nodes.
fn find_owner_id(row: &QueryResultRow, owner_type: &str, ctx: &ResultContext) -> Option<i64> {
    // Check static nodes for a direct match (entity IS the owner type).
    // Also check for an indirect match: a static node that is itself an
    // indirect-auth entity with the same owner_entity already holds the
    // owner's ID in its _gkg_*_id column (enforce.rs emits it from the
    // auth id column, e.g. `project_id` for File/Definition).
    for redaction_node in ctx.nodes() {
        if let Some(nr) = row.node_ref(redaction_node) {
            if nr.entity_type == owner_type {
                return Some(nr.id);
            }
            if let Some(auth) = ctx.get_entity_auth(&nr.entity_type)
                && auth.owner_entity.as_deref() == Some(owner_type)
            {
                return Some(nr.id);
            }
        }
    }

    // Check dynamic nodes in the row (e.g. adjacent path nodes).
    // Only direct entity_type match — dynamic nodes hold their own ID,
    // not the owner's.
    for other in row.dynamic_nodes() {
        if other.entity_type == owner_type {
            return Some(other.id);
        }
    }

    None
}

fn extract_value(array: &dyn Array, idx: usize) -> ColumnValue {
    if array.is_null(idx) {
        return ColumnValue::Null;
    }

    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return ColumnValue::Int64(arr.value(idx));
    }

    if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
        let val = arr.value(idx);
        return ColumnValue::Int64(i64::try_from(val).unwrap_or(i64::MAX));
    }

    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return ColumnValue::String(arr.value(idx).to_string());
    }

    if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
        return timestamp_to_string(arr.value_as_datetime(idx));
    }

    if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return timestamp_to_string(arr.value_as_datetime(idx));
    }

    if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return timestamp_to_string(arr.value_as_datetime(idx));
    }

    if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return timestamp_to_string(arr.value_as_datetime(idx));
    }

    ColumnValue::Null
}

fn timestamp_to_string(dt: Option<chrono::NaiveDateTime>) -> ColumnValue {
    dt.map(|d| ColumnValue::String(d.format("%Y-%m-%dT%H:%M:%SZ").to_string()))
        .unwrap_or(ColumnValue::Null)
}

/// Extract nodes from the _gkg_path column in path finding queries.
/// The column is Array(Tuple(Int64, String)) where each tuple is (node_id, entity_type).
fn extract_path_nodes(batch: &RecordBatch, row_idx: usize) -> Vec<NodeRef> {
    let col_idx = match batch.schema().index_of(PATH_COLUMN) {
        Ok(i) => i,
        Err(_) => return Vec::new(),
    };

    let list = match batch.column(col_idx).as_any().downcast_ref::<ListArray>() {
        Some(l) if !l.is_null(row_idx) => l,
        _ => return Vec::new(),
    };

    let values = list.value(row_idx);
    let structs = match values.as_any().downcast_ref::<StructArray>() {
        Some(s) if s.num_columns() >= 2 => s,
        _ => return Vec::new(),
    };

    let ids = structs.column(0).as_any().downcast_ref::<Int64Array>();
    let types = structs.column(1).as_any().downcast_ref::<StringArray>();

    match (ids, types) {
        (Some(ids), Some(types)) => (0..ids.len())
            .filter(|&i| !ids.is_null(i) && !types.is_null(i))
            .map(|i| NodeRef::new(ids.value(i), types.value(i)))
            .collect(),
        _ => Vec::new(),
    }
}

/// Extract the relationship kinds array from the `_gkg_edge_kinds` column.
/// Returns one String per hop — `edge_kinds[i]` connects `path[i]` to `path[i+1]`.
fn extract_edge_kinds(batch: &RecordBatch, row_idx: usize) -> Vec<String> {
    let col_idx = match batch.schema().index_of(EDGE_KINDS_COLUMN) {
        Ok(i) => i,
        Err(_) => return Vec::new(),
    };

    let list = match batch.column(col_idx).as_any().downcast_ref::<ListArray>() {
        Some(l) if !l.is_null(row_idx) => l,
        _ => return Vec::new(),
    };

    let values = list.value(row_idx);
    match values.as_any().downcast_ref::<StringArray>() {
        Some(arr) => (0..arr.len())
            .filter(|&i| !arr.is_null(i))
            .map(|i| arr.value(i).to_string())
            .collect(),
        None => Vec::new(),
    }
}

/// Extract the neighbor node from neighbor_id/neighbor_type columns in neighbors queries.
/// Returns None if either column is missing or null.
fn extract_neighbor_node(batch: &RecordBatch, row_idx: usize) -> Option<NodeRef> {
    let id_col_idx = batch.schema().index_of(NEIGHBOR_ID_COLUMN).ok()?;
    let type_col_idx = batch.schema().index_of(NEIGHBOR_TYPE_COLUMN).ok()?;

    let id_array = batch
        .column(id_col_idx)
        .as_any()
        .downcast_ref::<Int64Array>()?;
    let type_array = batch
        .column(type_col_idx)
        .as_any()
        .downcast_ref::<StringArray>()?;

    if id_array.is_null(row_idx) || type_array.is_null(row_idx) {
        return None;
    }

    Some(NodeRef::new(
        id_array.value(row_idx),
        type_array.value(row_idx),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use query_engine::EntityAuthConfig;
    use std::collections::HashSet;
    use std::sync::Arc;

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
            ("_gkg_u_id", Arc::new(Int64Array::from(vec![1, 2, 3]))),
            (
                "_gkg_u_type",
                Arc::new(StringArray::from(vec!["User", "User", "User"])),
            ),
            ("_gkg_p_id", Arc::new(Int64Array::from(vec![100, 200, 300]))),
            (
                "_gkg_p_type",
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
                owner_entity: None,
            },
        );
        ctx.add_entity_auth(
            "Project",
            EntityAuthConfig {
                resource_type: "project".to_string(),
                ability: "read".to_string(),
                auth_id_column: "id".to_string(),
                owner_entity: None,
            },
        );
        ctx
    }

    fn full_auth() -> Vec<ResourceAuthorization> {
        vec![
            ResourceAuthorization {
                resource_type: "user".to_string(),
                authorized: [(1, true), (2, true), (3, true)].into_iter().collect(),
            },
            ResourceAuthorization {
                resource_type: "project".to_string(),
                authorized: [(100, true), (200, true), (300, true)]
                    .into_iter()
                    .collect(),
            },
        ]
    }

    mod node_ref_tests {
        use super::*;

        #[test]
        fn new_creates_node_ref() {
            let node = NodeRef::new(42, "User");
            assert_eq!(node.id, 42);
            assert_eq!(node.entity_type, "User");
        }

        #[test]
        fn new_accepts_string() {
            let node = NodeRef::new(1, String::from("Project"));
            assert_eq!(node.entity_type, "Project");
        }

        #[test]
        fn equality() {
            let a = NodeRef::new(1, "User");
            let b = NodeRef::new(1, "User");
            let c = NodeRef::new(2, "User");
            let d = NodeRef::new(1, "Project");

            assert_eq!(a, b);
            assert_ne!(a, c);
            assert_ne!(a, d);
        }

        #[test]
        fn clone() {
            let original = NodeRef::new(99, "Group");
            let cloned = original.clone();
            assert_eq!(original, cloned);
        }
    }

    mod redactable_nodes_tests {
        use super::*;

        #[test]
        fn new_is_empty() {
            let nodes = RedactableNodes::new();
            assert!(nodes.is_empty());
            assert_eq!(nodes.len(), 0);
        }

        #[test]
        fn add_and_retrieve() {
            let mut nodes = RedactableNodes::new();
            nodes.add(1, "User");
            nodes.add(2, "Project");
            nodes.add(3, "User");

            assert_eq!(nodes.len(), 3);
            assert!(!nodes.is_empty());

            let refs = nodes.nodes();
            assert_eq!(refs[0], NodeRef::new(1, "User"));
            assert_eq!(refs[1], NodeRef::new(2, "Project"));
            assert_eq!(refs[2], NodeRef::new(3, "User"));
        }

        #[test]
        fn group_by_type_empty() {
            let nodes = RedactableNodes::new();
            let groups = nodes.group_by_type();
            assert!(groups.is_empty());
        }

        #[test]
        fn group_by_type_single_type() {
            let mut nodes = RedactableNodes::new();
            nodes.add(1, "User");
            nodes.add(2, "User");
            nodes.add(3, "User");

            let groups = nodes.group_by_type();
            assert_eq!(groups.len(), 1);
            assert_eq!(groups.get("User"), Some(&vec![1, 2, 3]));
        }

        #[test]
        fn group_by_type_multiple_types() {
            let mut nodes = RedactableNodes::new();
            nodes.add(1, "User");
            nodes.add(100, "Project");
            nodes.add(2, "User");
            nodes.add(200, "Group");
            nodes.add(101, "Project");

            let groups = nodes.group_by_type();
            assert_eq!(groups.len(), 3);
            assert_eq!(groups.get("User"), Some(&vec![1, 2]));
            assert_eq!(groups.get("Project"), Some(&vec![100, 101]));
            assert_eq!(groups.get("Group"), Some(&vec![200]));
        }

        #[test]
        fn group_by_type_preserves_order() {
            let mut nodes = RedactableNodes::new();
            nodes.add(3, "User");
            nodes.add(1, "User");
            nodes.add(2, "User");

            let groups = nodes.group_by_type();
            assert_eq!(groups.get("User"), Some(&vec![3, 1, 2]));
        }
    }

    mod column_value_tests {
        use super::*;

        #[test]
        fn int64_as_i64() {
            let val = ColumnValue::Int64(42);
            assert_eq!(val.as_i64(), Some(42));
            assert_eq!(val.as_str(), None);
        }

        #[test]
        fn string_as_str() {
            let val = ColumnValue::String("hello".to_string());
            assert_eq!(val.as_str(), Some("hello"));
            assert_eq!(val.as_i64(), None);
        }

        #[test]
        fn null_returns_none() {
            let val = ColumnValue::Null;
            assert_eq!(val.as_i64(), None);
            assert_eq!(val.as_str(), None);
        }

        #[test]
        fn equality() {
            assert_eq!(ColumnValue::Int64(1), ColumnValue::Int64(1));
            assert_ne!(ColumnValue::Int64(1), ColumnValue::Int64(2));
            assert_eq!(
                ColumnValue::String("a".to_string()),
                ColumnValue::String("a".to_string())
            );
            assert_ne!(ColumnValue::Null, ColumnValue::Int64(0));
        }
    }

    mod query_result_row_tests {
        use super::*;

        #[test]
        fn get_returns_column_value() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let row = &result.rows()[0];

            assert_eq!(row.get("_gkg_u_id"), Some(&ColumnValue::Int64(1)));
            assert_eq!(
                row.get("_gkg_u_type"),
                Some(&ColumnValue::String("User".to_string()))
            );
            assert_eq!(row.get("nonexistent"), None);
        }

        #[test]
        fn get_id_extracts_node_id() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let row = &result.rows()[0];
            let u = result.ctx().get("u").unwrap();
            let p = result.ctx().get("p").unwrap();

            assert_eq!(row.get_id(u), Some(1));
            assert_eq!(row.get_id(p), Some(100));
        }

        #[test]
        fn get_type_extracts_node_type() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let row = &result.rows()[0];
            let u = result.ctx().get("u").unwrap();
            let p = result.ctx().get("p").unwrap();

            assert_eq!(row.get_type(u), Some("User"));
            assert_eq!(row.get_type(p), Some("Project"));
        }

        #[test]
        fn node_ref_combines_id_and_type() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let row = &result.rows()[0];
            let u = result.ctx().get("u").unwrap();
            let p = result.ctx().get("p").unwrap();

            let user_ref = row.node_ref(u).unwrap();
            assert_eq!(user_ref.id, 1);
            assert_eq!(user_ref.entity_type, "User");

            let project_ref = row.node_ref(p).unwrap();
            assert_eq!(project_ref.id, 100);
            assert_eq!(project_ref.entity_type, "Project");
        }

        #[test]
        fn unknown_alias_returns_none() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let row = &result.rows()[0];

            assert!(result.ctx().get("nonexistent").is_none());

            let ghost = RedactionNode {
                alias: "ghost".to_string(),
                entity_type: "Ghost".to_string(),
                pk_column: "_gkg_ghost_id".to_string(),
                id_column: "_gkg_ghost_id".to_string(),
                type_column: "_gkg_ghost_type".to_string(),
            };
            assert_eq!(row.get_id(&ghost), None);
            assert_eq!(row.get_type(&ghost), None);
            assert!(row.node_ref(&ghost).is_none());
        }

        #[test]
        fn authorized_by_default() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            for row in result.rows() {
                assert!(row.is_authorized());
            }
        }

        #[test]
        fn set_unauthorized() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            assert!(result.rows()[0].is_authorized());

            result.rows_mut()[0].set_unauthorized();
            assert!(!result.rows()[0].is_authorized());
        }

        #[test]
        fn columns_iterator_returns_all_columns() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let row = &result.rows()[0];

            let column_names: HashSet<&str> =
                row.columns().map(|(name, _)| name.as_str()).collect();

            assert!(column_names.contains("_gkg_u_id"));
            assert!(column_names.contains("_gkg_u_type"));
            assert!(column_names.contains("_gkg_p_id"));
            assert!(column_names.contains("_gkg_p_type"));
            assert_eq!(column_names.len(), 4);
        }

        #[test]
        fn columns_iterator_returns_correct_values() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let row = &result.rows()[0];

            let columns: HashMap<&str, &ColumnValue> =
                row.columns().map(|(k, v)| (k.as_str(), v)).collect();

            assert_eq!(columns.get("_gkg_u_id"), Some(&&ColumnValue::Int64(1)));
            assert_eq!(
                columns.get("_gkg_u_type"),
                Some(&&ColumnValue::String("User".to_string()))
            );
            assert_eq!(columns.get("_gkg_p_id"), Some(&&ColumnValue::Int64(100)));
        }
    }

    mod query_result_tests {
        use super::*;

        #[test]
        fn from_batches_empty() {
            let result = QueryResult::from_batches(&[], &ResultContext::new());
            assert!(result.is_empty());
            assert_eq!(result.len(), 0);
            assert!(result.node_aliases().is_empty());
        }

        #[test]
        fn from_batches_uses_context_aliases() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let aliases: HashSet<String> = result.node_aliases().into_iter().collect();

            assert_eq!(aliases.len(), 2);
            assert!(aliases.contains("u"));
            assert!(aliases.contains("p"));
        }

        #[test]
        fn from_batches_multiple_batches() {
            let batch1 = make_batch(vec![
                ("_gkg_u_id", Arc::new(Int64Array::from(vec![1, 2]))),
                (
                    "_gkg_u_type",
                    Arc::new(StringArray::from(vec!["User", "User"])),
                ),
            ]);
            let batch2 = make_batch(vec![
                ("_gkg_u_id", Arc::new(Int64Array::from(vec![3, 4]))),
                (
                    "_gkg_u_type",
                    Arc::new(StringArray::from(vec!["User", "User"])),
                ),
            ]);

            let mut ctx = ResultContext::new();
            ctx.add_node("u", "User");

            let result = QueryResult::from_batches(&[batch1, batch2], &ctx);
            assert_eq!(result.len(), 4);

            let u = result.ctx().get("u").unwrap();
            let ids: Vec<i64> = result.iter().filter_map(|r| r.get_id(u)).collect();
            assert_eq!(ids, vec![1, 2, 3, 4]);
        }

        #[test]
        fn len_and_is_empty() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            assert_eq!(result.len(), 3);
            assert!(!result.is_empty());
        }

        #[test]
        fn iter_returns_all_rows() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let u = result.ctx().get("u").unwrap();
            let ids: Vec<i64> = result.iter().filter_map(|r| r.get_id(u)).collect();
            assert_eq!(ids, vec![1, 2, 3]);
        }

        #[test]
        fn extract_redactable_nodes_all_nodes() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let nodes = result.extract_redactable_nodes();

            assert_eq!(nodes.len(), 6);

            let groups = nodes.group_by_type();
            assert_eq!(groups.get("User"), Some(&vec![1, 2, 3]));
            assert_eq!(groups.get("Project"), Some(&vec![100, 200, 300]));
        }

        #[test]
        fn authorized_rows_filters_correctly() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            result.rows_mut()[1].set_unauthorized();

            let u = result.ctx().get("u").unwrap();
            let authorized: Vec<_> = result.authorized_rows().collect();
            assert_eq!(authorized.len(), 2);

            let ids: Vec<i64> = authorized.iter().filter_map(|r| r.get_id(u)).collect();
            assert_eq!(ids, vec![1, 3]);
        }

        #[test]
        fn authorized_count() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            assert_eq!(result.authorized_count(), 3);

            result.rows_mut()[0].set_unauthorized();
            assert_eq!(result.authorized_count(), 2);

            result.rows_mut()[1].set_unauthorized();
            result.rows_mut()[2].set_unauthorized();
            assert_eq!(result.authorized_count(), 0);
        }
    }

    mod apply_authorizations_tests {
        use super::*;

        #[test]
        fn all_authorized_returns_zero_redacted() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let redacted = result.apply_authorizations(&full_auth());
            assert_eq!(redacted, 0);
            assert_eq!(result.authorized_count(), 3);
        }

        #[test]
        fn single_deny_redacts_one_row() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());

            let authorizations = vec![
                ResourceAuthorization {
                    resource_type: "user".to_string(),
                    authorized: [(1, true), (2, true), (3, true)].into_iter().collect(),
                },
                ResourceAuthorization {
                    resource_type: "project".to_string(),
                    authorized: [(100, true), (200, false), (300, true)]
                        .into_iter()
                        .collect(),
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
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());

            let authorizations = vec![
                ResourceAuthorization {
                    resource_type: "user".to_string(),
                    authorized: [(1, false), (2, true), (3, false)].into_iter().collect(),
                },
                ResourceAuthorization {
                    resource_type: "project".to_string(),
                    authorized: [(100, true), (200, true), (300, true)]
                        .into_iter()
                        .collect(),
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
            // ctx with no entity_auth → every node is unknown → all rows denied
            let mut ctx = ResultContext::new();
            ctx.add_node("u", "User");
            ctx.add_node("p", "Project");
            let mut result = QueryResult::from_batches(&[make_test_batch()], &ctx);
            let redacted = result.apply_authorizations(&[]);
            assert_eq!(redacted, 3);
            assert_eq!(result.authorized_count(), 0);
        }

        #[test]
        fn fail_closed_missing_resource_authorization() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let redacted = result.apply_authorizations(&[]);
            assert_eq!(redacted, 3);
            assert_eq!(result.authorized_count(), 0);
        }

        #[test]
        fn fail_closed_partial_resource_authorization() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());

            let authorizations = vec![ResourceAuthorization {
                resource_type: "user".to_string(),
                authorized: [(1, true), (2, true), (3, true)].into_iter().collect(),
            }];

            let redacted = result.apply_authorizations(&authorizations);

            assert_eq!(redacted, 3);
            assert_eq!(result.authorized_count(), 0);
        }

        #[test]
        fn fail_closed_missing_id_in_authorization() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());

            let authorizations = vec![
                ResourceAuthorization {
                    resource_type: "user".to_string(),
                    authorized: [(1, true)].into_iter().collect(),
                },
                ResourceAuthorization {
                    resource_type: "project".to_string(),
                    authorized: [(100, true), (200, true), (300, true)]
                        .into_iter()
                        .collect(),
                },
            ];

            let redacted = result.apply_authorizations(&authorizations);

            assert_eq!(redacted, 2);
            assert_eq!(result.authorized_count(), 1);
            assert!(result.rows()[0].is_authorized());
        }

        #[test]
        fn already_unauthorized_rows_not_double_counted() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            result.rows_mut()[0].set_unauthorized();
            let redacted = result.apply_authorizations(&full_auth());
            assert_eq!(redacted, 0);
            assert_eq!(result.authorized_count(), 2);
        }

        #[test]
        fn fail_closed_dynamic_node_unresolvable_owner() {
            // Simulates a dynamic node (e.g. Definition) that requires indirect auth
            // via an owner entity (Project), but the owner isn't in the row.
            // Must deny even if the entity's own ID collides with an authorized
            // project ID.
            let batch = make_batch(vec![
                ("_gkg_u_id", Arc::new(Int64Array::from(vec![1]))),
                ("_gkg_u_type", Arc::new(StringArray::from(vec!["User"]))),
            ]);

            let mut ctx = ResultContext::new();
            ctx.add_node("u", "User");
            ctx.add_entity_auth(
                "User",
                EntityAuthConfig {
                    resource_type: "user".to_string(),
                    ability: "read_user".to_string(),
                    auth_id_column: "id".to_string(),
                    owner_entity: None,
                },
            );
            ctx.add_entity_auth(
                "Definition",
                EntityAuthConfig {
                    resource_type: "project".to_string(),
                    ability: "read_code".to_string(),
                    auth_id_column: "project_id".to_string(),
                    owner_entity: Some("Project".to_string()),
                },
            );

            let mut result = QueryResult::from_batches(&[batch], &ctx);

            // Manually inject a dynamic node: Definition with ID 1000.
            // No Project node exists in the row, so find_owner_id will return None.
            result.rows_mut()[0].dynamic_nodes = vec![NodeRef::new(1000, "Definition")];

            // Authorize user 1 AND project 1000 — the ID collision scenario.
            let authorizations = vec![
                ResourceAuthorization {
                    resource_type: "user".to_string(),
                    authorized: [(1, true)].into_iter().collect(),
                },
                ResourceAuthorization {
                    resource_type: "project".to_string(),
                    authorized: [(1000, true)].into_iter().collect(),
                },
            ];

            let redacted = result.apply_authorizations(&authorizations);

            assert_eq!(
                redacted, 1,
                "row must be denied: Definition's owner (Project) is not in the row"
            );
            assert_eq!(result.authorized_count(), 0);
        }

        #[test]
        fn dynamic_node_with_resolvable_owner_passes() {
            // Same setup as above, but now the owner Project IS in the row
            // as a static node. The Definition should resolve its auth via
            // the Project and pass.
            let batch = make_batch(vec![
                ("_gkg_p_id", Arc::new(Int64Array::from(vec![1000]))),
                ("_gkg_p_type", Arc::new(StringArray::from(vec!["Project"]))),
            ]);

            let mut ctx = ResultContext::new();
            ctx.add_node("p", "Project");
            ctx.add_entity_auth(
                "Project",
                EntityAuthConfig {
                    resource_type: "project".to_string(),
                    ability: "read_code".to_string(),
                    auth_id_column: "id".to_string(),
                    owner_entity: None,
                },
            );
            ctx.add_entity_auth(
                "Definition",
                EntityAuthConfig {
                    resource_type: "project".to_string(),
                    ability: "read_code".to_string(),
                    auth_id_column: "project_id".to_string(),
                    owner_entity: Some("Project".to_string()),
                },
            );

            let mut result = QueryResult::from_batches(&[batch], &ctx);

            // Inject a Definition neighbor — owner Project 1000 IS in the row.
            result.rows_mut()[0].dynamic_nodes = vec![NodeRef::new(5000, "Definition")];

            let authorizations = vec![ResourceAuthorization {
                resource_type: "project".to_string(),
                authorized: [(1000, true)].into_iter().collect(),
            }];

            let redacted = result.apply_authorizations(&authorizations);

            assert_eq!(
                redacted, 0,
                "Definition should pass: owner Project 1000 is authorized"
            );
            assert_eq!(result.authorized_count(), 1);
        }

        #[test]
        fn dynamic_node_resolves_owner_via_sibling_indirect_static_node() {
            // Center node is File (indirect auth, owner: Project).
            // enforce.rs emits _gkg_f_id = project_id (1000), _gkg_f_type = "File".
            // Dynamic neighbor is Definition (also indirect auth, owner: Project).
            // find_owner_id should recognize that File's _gkg_f_id IS the
            // Project owner's ID because File's auth config has
            // owner_entity == Some("Project").
            let batch = make_batch(vec![
                ("_gkg_f_id", Arc::new(Int64Array::from(vec![1000]))),
                ("_gkg_f_type", Arc::new(StringArray::from(vec!["File"]))),
            ]);

            let mut ctx = ResultContext::new();
            ctx.add_node("f", "File");
            ctx.add_entity_auth(
                "File",
                EntityAuthConfig {
                    resource_type: "project".to_string(),
                    ability: "read_code".to_string(),
                    auth_id_column: "project_id".to_string(),
                    owner_entity: Some("Project".to_string()),
                },
            );
            ctx.add_entity_auth(
                "Definition",
                EntityAuthConfig {
                    resource_type: "project".to_string(),
                    ability: "read_code".to_string(),
                    auth_id_column: "project_id".to_string(),
                    owner_entity: Some("Project".to_string()),
                },
            );

            let mut result = QueryResult::from_batches(&[batch], &ctx);
            result.rows_mut()[0].dynamic_nodes = vec![NodeRef::new(5000, "Definition")];

            let authorizations = vec![ResourceAuthorization {
                resource_type: "project".to_string(),
                authorized: [(1000, true)].into_iter().collect(),
            }];

            let redacted = result.apply_authorizations(&authorizations);

            assert_eq!(
                redacted, 0,
                "Definition should pass: File's _gkg_f_id (project_id=1000) \
                 resolves as the shared Project owner"
            );
            assert_eq!(result.authorized_count(), 1);
        }

        #[test]
        fn empty_result_returns_zero() {
            let mut result = QueryResult::from_batches(&[], &ResultContext::new());
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
                    "_gkg_u_id",
                    Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                ),
                (
                    "_gkg_u_type",
                    Arc::new(StringArray::from(vec![Some("User"), Some("User"), None])),
                ),
            ]);

            let result = QueryResult::from_batches(&[batch], &user_ctx());

            assert_eq!(
                result.rows()[0].get("_gkg_u_id"),
                Some(&ColumnValue::Int64(1))
            );
            assert_eq!(result.rows()[1].get("_gkg_u_id"), Some(&ColumnValue::Null));
            assert_eq!(
                result.rows()[2].get("_gkg_u_type"),
                Some(&ColumnValue::Null)
            );
        }

        #[test]
        fn null_id_returns_none_from_get_id() {
            let batch = make_batch(vec![
                ("_gkg_u_id", Arc::new(Int64Array::from(vec![Some(1), None]))),
                (
                    "_gkg_u_type",
                    Arc::new(StringArray::from(vec!["User", "User"])),
                ),
            ]);

            let result = QueryResult::from_batches(&[batch], &user_ctx());
            let u = result.ctx().get("u").unwrap();

            assert_eq!(result.rows()[0].get_id(u), Some(1));
            assert_eq!(result.rows()[1].get_id(u), None);
        }

        #[test]
        fn null_type_returns_none_from_get_type() {
            let batch = make_batch(vec![
                ("_gkg_u_id", Arc::new(Int64Array::from(vec![1, 2]))),
                (
                    "_gkg_u_type",
                    Arc::new(StringArray::from(vec![Some("User"), None])),
                ),
            ]);

            let result = QueryResult::from_batches(&[batch], &user_ctx());
            let u = result.ctx().get("u").unwrap();

            assert_eq!(result.rows()[0].get_type(u), Some("User"));
            assert_eq!(result.rows()[1].get_type(u), None);
        }

        #[test]
        fn node_ref_returns_none_if_id_null() {
            let batch = make_batch(vec![
                ("_gkg_u_id", Arc::new(Int64Array::from(vec![None]))),
                ("_gkg_u_type", Arc::new(StringArray::from(vec!["User"]))),
            ]);

            let result = QueryResult::from_batches(&[batch], &user_ctx());
            let u = result.ctx().get("u").unwrap();
            assert!(result.rows()[0].node_ref(u).is_none());
        }

        #[test]
        fn node_ref_returns_none_if_type_null() {
            let batch = make_batch(vec![
                ("_gkg_u_id", Arc::new(Int64Array::from(vec![1]))),
                (
                    "_gkg_u_type",
                    Arc::new(StringArray::from(vec![Option::<&str>::None])),
                ),
            ]);

            let result = QueryResult::from_batches(&[batch], &user_ctx());
            let u = result.ctx().get("u").unwrap();
            assert!(result.rows()[0].node_ref(u).is_none());
        }
    }

    mod extract_value_tests {
        use super::*;

        #[test]
        fn extract_uint64_as_int64() {
            let batch = make_batch(vec![(
                "count",
                Arc::new(UInt64Array::from(vec![100u64, 200, 300])),
            )]);

            let result = QueryResult::from_batches(&[batch], &ResultContext::new());

            assert_eq!(
                result.rows()[0].get("count"),
                Some(&ColumnValue::Int64(100))
            );
            assert_eq!(
                result.rows()[1].get("count"),
                Some(&ColumnValue::Int64(200))
            );
            assert_eq!(
                result.rows()[2].get("count"),
                Some(&ColumnValue::Int64(300))
            );
        }

        #[test]
        fn extract_timestamp_second() {
            let arr = TimestampSecondArray::new(
                vec![1704067200i64].into(), // 2024-01-01T00:00:00Z
                None,
            );
            let batch = make_batch(vec![("ts", Arc::new(arr))]);

            let result = QueryResult::from_batches(&[batch], &ResultContext::new());

            assert_eq!(
                result.rows()[0].get("ts"),
                Some(&ColumnValue::String("2024-01-01T00:00:00Z".to_string()))
            );
        }

        #[test]
        fn extract_timestamp_millisecond() {
            let arr = TimestampMillisecondArray::new(
                vec![1704067200000i64].into(), // 2024-01-01T00:00:00Z in ms
                None,
            );
            let batch = make_batch(vec![("ts", Arc::new(arr))]);

            let result = QueryResult::from_batches(&[batch], &ResultContext::new());

            assert_eq!(
                result.rows()[0].get("ts"),
                Some(&ColumnValue::String("2024-01-01T00:00:00Z".to_string()))
            );
        }

        #[test]
        fn extract_timestamp_microsecond() {
            let arr = TimestampMicrosecondArray::new(
                vec![1704067200000000i64].into(), // 2024-01-01T00:00:00Z in μs
                None,
            );
            let batch = make_batch(vec![("ts", Arc::new(arr))]);

            let result = QueryResult::from_batches(&[batch], &ResultContext::new());

            assert_eq!(
                result.rows()[0].get("ts"),
                Some(&ColumnValue::String("2024-01-01T00:00:00Z".to_string()))
            );
        }

        #[test]
        fn extract_timestamp_nanosecond() {
            let arr = TimestampNanosecondArray::new(
                vec![1704067200000000000i64].into(), // 2024-01-01T00:00:00Z in ns
                None,
            );
            let batch = make_batch(vec![("ts", Arc::new(arr))]);

            let result = QueryResult::from_batches(&[batch], &ResultContext::new());

            assert_eq!(
                result.rows()[0].get("ts"),
                Some(&ColumnValue::String("2024-01-01T00:00:00Z".to_string()))
            );
        }

        #[test]
        fn extract_null_timestamp_returns_null() {
            let arr: TimestampSecondArray = vec![Some(1704067200i64), None].into_iter().collect();
            let batch = make_batch(vec![("ts", Arc::new(arr))]);

            let result = QueryResult::from_batches(&[batch], &ResultContext::new());

            assert_eq!(
                result.rows()[0].get("ts"),
                Some(&ColumnValue::String("2024-01-01T00:00:00Z".to_string()))
            );
            assert_eq!(result.rows()[1].get("ts"), Some(&ColumnValue::Null));
        }

        #[test]
        fn extract_uint64_overflow_clamps_to_max() {
            let batch = make_batch(vec![("big", Arc::new(UInt64Array::from(vec![u64::MAX])))]);

            let result = QueryResult::from_batches(&[batch], &ResultContext::new());

            assert_eq!(
                result.rows()[0].get("big"),
                Some(&ColumnValue::Int64(i64::MAX))
            );
        }

        #[test]
        fn mixed_data_types_in_batch() {
            let ts_arr = TimestampMillisecondArray::new(vec![1704067200000i64].into(), None);
            let batch = make_batch(vec![
                ("_gkg_u_id", Arc::new(Int64Array::from(vec![1]))),
                ("_gkg_u_type", Arc::new(StringArray::from(vec!["User"]))),
                ("view_count", Arc::new(UInt64Array::from(vec![1000u64]))),
                ("created_at", Arc::new(ts_arr)),
            ]);

            let mut ctx = ResultContext::new();
            ctx.add_node("u", "User");

            let result = QueryResult::from_batches(&[batch], &ctx);
            let row = &result.rows()[0];
            let u = result.ctx().get("u").unwrap();

            assert_eq!(row.get_id(u), Some(1));
            assert_eq!(row.get_type(u), Some("User"));
            assert_eq!(row.get("view_count"), Some(&ColumnValue::Int64(1000)));
            assert_eq!(
                row.get("created_at"),
                Some(&ColumnValue::String("2024-01-01T00:00:00Z".to_string()))
            );
        }
    }
}

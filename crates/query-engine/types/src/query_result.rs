//! Type-safe result schema for redaction processing.

use std::collections::{HashMap, HashSet};

use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use compiler::constants::{
    edge_kinds_column, neighbor_id_column, neighbor_type_column, path_column,
};
use compiler::internal_column_prefix;
use compiler::{QueryType, RedactionNode, ResultContext};

use super::{ResourceAuthorization, ResourceCheck};
use gkg_utils::arrow::{ArrowUtils, ColumnValue};

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
        self.columns.get(&node.id_column)?.as_int64().copied()
    }

    pub fn get_pk(&self, node: &RedactionNode) -> Option<i64> {
        self.columns.get(&node.pk_column)?.as_int64().copied()
    }

    pub fn get_public_id(&self, node: &RedactionNode) -> Option<i64> {
        self.columns
            .get(&node.pk_column)
            .and_then(|v| v.as_int64().copied())
            .or_else(|| self.get_id(node))
    }

    pub fn get_type(&self, node: &RedactionNode) -> Option<&str> {
        self.columns
            .get(&node.type_column)?
            .as_string()
            .map(|s| s.as_str())
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

    pub fn entity_properties(
        &self,
        alias: &str,
        skip_prefixes: &[&str],
    ) -> HashMap<String, ColumnValue> {
        let prefix = format!("{alias}_");
        let mut props = HashMap::new();
        for (name, value) in &self.columns {
            if name.starts_with(internal_column_prefix()) {
                continue;
            }
            if skip_prefixes.iter().any(|sp| name.starts_with(sp)) {
                continue;
            }
            if let Some(prop_name) = name.strip_prefix(&prefix) {
                props.insert(prop_name.to_string(), value.clone());
            }
        }
        props
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
}

/// Type-safe wrapper around Arrow RecordBatch results for redaction processing.
#[derive(Debug, Clone)]
pub struct QueryResult {
    rows: Vec<QueryResultRow>,
    ctx: ResultContext,
}

impl QueryResult {
    pub fn from_batches(batches: &[RecordBatch], ctx: &ResultContext) -> Self {
        let is_path_finding = ctx.query_type == Some(QueryType::PathFinding);
        let is_neighbors = ctx.query_type == Some(QueryType::Neighbors);

        let traversal_path_columns: Vec<&str> = ctx
            .edges()
            .iter()
            .filter_map(|edge| edge.path_column.as_deref())
            .collect();

        let mut rows = Vec::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let columns = ArrowUtils::extract_row(batch, row_idx);

                let dynamic_nodes = if is_path_finding {
                    ArrowUtils::get_i64_string_pairs(batch, path_column(), row_idx)
                        .into_iter()
                        .map(|(id, t)| NodeRef::new(id, t))
                        .collect()
                } else if is_neighbors {
                    let neighbor =
                        ArrowUtils::get_column::<Int64Type>(batch, neighbor_id_column(), row_idx)
                            .and_then(|id| {
                                ArrowUtils::get_column_string(
                                    batch,
                                    neighbor_type_column(),
                                    row_idx,
                                )
                                .map(|t| NodeRef::new(id, t))
                            });
                    neighbor.into_iter().collect()
                } else if !traversal_path_columns.is_empty() {
                    traversal_path_columns
                        .iter()
                        .flat_map(|column| ArrowUtils::get_i64_string_pairs(batch, column, row_idx))
                        .map(|(id, t)| NodeRef::new(id, t))
                        .collect()
                } else {
                    Vec::new()
                };

                let edge_kinds = if is_path_finding {
                    ArrowUtils::get_string_list(batch, edge_kinds_column(), row_idx)
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

    /// Collect all resource IDs that need authorization, grouped by (resource_type, ability).
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
    pub fn apply_authorizations(&mut self, authorizations: &[ResourceAuthorization]) -> usize {
        let mut redacted_count = 0;
        let redaction_nodes: Vec<_> = self.ctx.nodes().cloned().collect();

        for row in &mut self.rows {
            if !row.authorized {
                continue;
            }

            for redaction_node in &redaction_nodes {
                let Some(node_ref) = row.node_ref(redaction_node) else {
                    row.set_unauthorized();
                    redacted_count += 1;
                    break;
                };
                if !is_authorized(&node_ref, authorizations, &self.ctx) {
                    row.set_unauthorized();
                    redacted_count += 1;
                    break;
                }
            }

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

    /// Slice the authorized result set to `[offset..offset+page_size]`.
    ///
    /// Must be called **after** hydration — marks out-of-window authorized
    /// rows as unauthorized so `authorized_rows()` returns only the page.
    /// Returns whether more authorized rows exist beyond this page.
    pub fn apply_cursor(&mut self, offset: u32, page_size: u32) -> bool {
        let end = (offset as usize).saturating_add(page_size as usize);
        let mut i = 0usize;
        for row in &mut self.rows {
            if !row.authorized {
                continue;
            }
            if i < offset as usize || i >= end {
                row.authorized = false;
            }
            i += 1;
        }
        i > end
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

fn find_owner_id(row: &QueryResultRow, owner_type: &str, ctx: &ResultContext) -> Option<i64> {
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

    for other in row.dynamic_nodes() {
        if other.entity_type == owner_type {
            return Some(other.id);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray, TimestampMillisecondArray, UInt64Array};
    use arrow::datatypes::{Field, Schema};
    use compiler::EntityAuthConfig;
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
                required_access_level: 20,
            },
        );
        ctx.add_entity_auth(
            "Project",
            EntityAuthConfig {
                resource_type: "project".to_string(),
                ability: "read".to_string(),
                auth_id_column: "id".to_string(),
                owner_entity: None,
                required_access_level: 20,
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
        fn get_id_extracts_redaction_auth_id() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let row = &result.rows()[0];
            let u = result.ctx().get("u").unwrap();
            let p = result.ctx().get("p").unwrap();

            assert_eq!(row.get_id(u), Some(1));
            assert_eq!(row.get_id(p), Some(100));
        }

        #[test]
        fn get_public_id_falls_back_to_redaction_id_when_pk_is_absent() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let row = &result.rows()[0];
            let u = result.ctx().get("u").unwrap();

            assert_eq!(row.get_pk(u), None);
            assert_eq!(row.get_public_id(u), Some(1));
        }

        #[test]
        fn get_public_id_prefers_pk_for_indirect_auth_entities() {
            let batch = make_batch(vec![
                ("_gkg_d_id", Arc::new(Int64Array::from(vec![1000]))),
                ("_gkg_d_pk", Arc::new(Int64Array::from(vec![5000]))),
                (
                    "_gkg_d_type",
                    Arc::new(StringArray::from(vec!["Definition"])),
                ),
            ]);

            let mut ctx = ResultContext::new();
            ctx.add_node("d", "Definition");

            let result = QueryResult::from_batches(&[batch], &ctx);
            let row = &result.rows()[0];
            let d = result.ctx().get("d").unwrap();

            assert_eq!(row.get_id(d), Some(1000));
            assert_eq!(row.get_pk(d), Some(5000));
            assert_eq!(row.get_public_id(d), Some(5000));

            let node_ref = row.node_ref(d).unwrap();
            assert_eq!(node_ref.id, 1000);
            assert_eq!(node_ref.entity_type, "Definition");
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
                    required_access_level: 20,
                },
            );
            ctx.add_entity_auth(
                "Definition",
                EntityAuthConfig {
                    resource_type: "project".to_string(),
                    ability: "read_code".to_string(),
                    auth_id_column: "project_id".to_string(),
                    owner_entity: Some("Project".to_string()),
                    required_access_level: 20,
                },
            );

            let mut result = QueryResult::from_batches(&[batch], &ctx);
            result.rows_mut()[0].dynamic_nodes = vec![NodeRef::new(1000, "Definition")];

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
                    required_access_level: 20,
                },
            );
            ctx.add_entity_auth(
                "Definition",
                EntityAuthConfig {
                    resource_type: "project".to_string(),
                    ability: "read_code".to_string(),
                    auth_id_column: "project_id".to_string(),
                    owner_entity: Some("Project".to_string()),
                    required_access_level: 20,
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
                "Definition should pass: owner Project 1000 is authorized"
            );
            assert_eq!(result.authorized_count(), 1);
        }

        #[test]
        fn dynamic_node_resolves_owner_via_sibling_indirect_static_node() {
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
                    required_access_level: 20,
                },
            );
            ctx.add_entity_auth(
                "Definition",
                EntityAuthConfig {
                    resource_type: "project".to_string(),
                    ability: "read_code".to_string(),
                    auth_id_column: "project_id".to_string(),
                    owner_entity: Some("Project".to_string()),
                    required_access_level: 20,
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

    mod from_batches_data_type_tests {
        use super::*;

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

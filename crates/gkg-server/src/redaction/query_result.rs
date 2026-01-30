//! Type-safe result schema for redaction processing.

use std::collections::HashMap;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use query_engine::{ResultContext, id_column, type_column};

use super::ResourceAuthorization;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRef {
    pub id: i64,
    pub entity_type: String,
}

impl NodeRef {
    pub fn new(id: i64, entity_type: impl Into<String>) -> Self {
        Self {
            id,
            entity_type: entity_type.into(),
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
    authorized: bool,
}

impl QueryResultRow {
    fn new(columns: HashMap<String, ColumnValue>) -> Self {
        Self {
            columns,
            authorized: true,
        }
    }

    pub fn get(&self, column: &str) -> Option<&ColumnValue> {
        self.columns.get(column)
    }

    pub fn get_id(&self, node_alias: &str) -> Option<i64> {
        self.columns.get(&id_column(node_alias))?.as_i64()
    }

    pub fn get_type(&self, node_alias: &str) -> Option<&str> {
        self.columns.get(&type_column(node_alias))?.as_str()
    }

    pub fn node_ref(&self, node_alias: &str) -> Option<NodeRef> {
        Some(NodeRef::new(
            self.get_id(node_alias)?,
            self.get_type(node_alias)?,
        ))
    }

    pub fn is_authorized(&self) -> bool {
        self.authorized
    }

    pub fn set_unauthorized(&mut self) {
        self.authorized = false;
    }
}

/// Type-safe wrapper around Arrow RecordBatch results for redaction processing.
#[derive(Debug)]
pub struct QueryResult {
    rows: Vec<QueryResultRow>,
    node_aliases: Vec<String>,
}

impl QueryResult {
    pub fn from_batches(batches: &[RecordBatch], ctx: &ResultContext) -> Self {
        let node_aliases: Vec<String> = ctx.nodes().map(|n| n.alias.clone()).collect();

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
                rows.push(QueryResultRow::new(columns));
            }
        }

        Self { rows, node_aliases }
    }

    pub fn rows(&self) -> &[QueryResultRow] {
        &self.rows
    }

    pub fn rows_mut(&mut self) -> &mut [QueryResultRow] {
        &mut self.rows
    }

    pub fn node_aliases(&self) -> &[String] {
        &self.node_aliases
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
            for alias in &self.node_aliases {
                if let Some(node_ref) = row.node_ref(alias) {
                    nodes.add(node_ref.id, node_ref.entity_type);
                }
            }
        }
        nodes
    }

    /// Apply authorization results to rows. Fails closed: any row containing a node
    /// that cannot be verified as authorized is marked unauthorized.
    ///
    /// A row is marked unauthorized if ANY node in the row:
    /// - Has an entity type not configured for redaction (not in entity_to_resource)
    /// - Has no authorization result from the redaction service
    /// - Is explicitly marked as unauthorized
    pub fn apply_authorizations(
        &mut self,
        authorizations: &[ResourceAuthorization],
        entity_to_resource: &HashMap<&str, &str>,
    ) -> usize {
        let mut redacted_count = 0;
        let aliases = self.node_aliases.clone();

        for row in &mut self.rows {
            if !row.authorized {
                continue;
            }

            for alias in &aliases {
                let Some(node_ref) = row.node_ref(alias) else {
                    continue;
                };

                if !is_node_authorized(&node_ref, authorizations, entity_to_resource) {
                    row.set_unauthorized();
                    redacted_count += 1;
                    break;
                }
            }
        }

        redacted_count
    }

    pub fn authorized_rows(&self) -> impl Iterator<Item = &QueryResultRow> {
        self.rows.iter().filter(|r| r.authorized)
    }

    pub fn authorized_count(&self) -> usize {
        self.rows.iter().filter(|r| r.authorized).count()
    }
}

fn is_node_authorized(
    node_ref: &NodeRef,
    authorizations: &[ResourceAuthorization],
    entity_to_resource: &HashMap<&str, &str>,
) -> bool {
    let Some(&resource_type) = entity_to_resource.get(node_ref.entity_type.as_str()) else {
        return false;
    };

    let Some(auth) = authorizations
        .iter()
        .find(|a| a.resource_type == resource_type)
    else {
        return false;
    };

    auth.authorized.get(&node_ref.id).copied().unwrap_or(false)
}

fn extract_value(array: &dyn Array, idx: usize) -> ColumnValue {
    if array.is_null(idx) {
        return ColumnValue::Null;
    }

    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return ColumnValue::Int64(arr.value(idx));
    }

    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return ColumnValue::String(arr.value(idx).to_string());
    }

    ColumnValue::Null
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
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
        ctx
    }

    fn full_auth() -> Vec<ResourceAuthorization> {
        vec![
            ResourceAuthorization {
                resource_type: "users".to_string(),
                authorized: [(1, true), (2, true), (3, true)].into_iter().collect(),
            },
            ResourceAuthorization {
                resource_type: "projects".to_string(),
                authorized: [(100, true), (200, true), (300, true)]
                    .into_iter()
                    .collect(),
            },
        ]
    }

    fn entity_map() -> HashMap<&'static str, &'static str> {
        [("User", "users"), ("Project", "projects")]
            .into_iter()
            .collect()
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

            assert_eq!(row.get_id("u"), Some(1));
            assert_eq!(row.get_id("p"), Some(100));
            assert_eq!(row.get_id("nonexistent"), None);
        }

        #[test]
        fn get_type_extracts_node_type() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let row = &result.rows()[0];

            assert_eq!(row.get_type("u"), Some("User"));
            assert_eq!(row.get_type("p"), Some("Project"));
            assert_eq!(row.get_type("nonexistent"), None);
        }

        #[test]
        fn node_ref_combines_id_and_type() {
            let result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let row = &result.rows()[0];

            let user_ref = row.node_ref("u").unwrap();
            assert_eq!(user_ref.id, 1);
            assert_eq!(user_ref.entity_type, "User");

            let project_ref = row.node_ref("p").unwrap();
            assert_eq!(project_ref.id, 100);
            assert_eq!(project_ref.entity_type, "Project");

            assert!(row.node_ref("nonexistent").is_none());
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
            let aliases: HashSet<_> = result.node_aliases().iter().collect();

            assert_eq!(aliases.len(), 2);
            assert!(aliases.contains(&"u".to_string()));
            assert!(aliases.contains(&"p".to_string()));
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

            let ids: Vec<i64> = result.iter().filter_map(|r| r.get_id("u")).collect();
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
            let ids: Vec<i64> = result.iter().filter_map(|r| r.get_id("u")).collect();
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

            let authorized: Vec<_> = result.authorized_rows().collect();
            assert_eq!(authorized.len(), 2);

            let ids: Vec<i64> = authorized.iter().filter_map(|r| r.get_id("u")).collect();
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

            let redacted = result.apply_authorizations(&full_auth(), &entity_map());

            assert_eq!(redacted, 0);
            assert_eq!(result.authorized_count(), 3);
        }

        #[test]
        fn single_deny_redacts_one_row() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let entity_map: HashMap<&str, &str> = [("User", "users"), ("Project", "projects")]
                .into_iter()
                .collect();

            let authorizations = vec![
                ResourceAuthorization {
                    resource_type: "users".to_string(),
                    authorized: [(1, true), (2, true), (3, true)].into_iter().collect(),
                },
                ResourceAuthorization {
                    resource_type: "projects".to_string(),
                    authorized: [(100, true), (200, false), (300, true)]
                        .into_iter()
                        .collect(),
                },
            ];

            let redacted = result.apply_authorizations(&authorizations, &entity_map);

            assert_eq!(redacted, 1);
            assert_eq!(result.authorized_count(), 2);
            assert!(result.rows()[0].is_authorized());
            assert!(!result.rows()[1].is_authorized());
            assert!(result.rows()[2].is_authorized());
        }

        #[test]
        fn multiple_denies_redact_multiple_rows() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let entity_map: HashMap<&str, &str> = [("User", "users"), ("Project", "projects")]
                .into_iter()
                .collect();

            let authorizations = vec![
                ResourceAuthorization {
                    resource_type: "users".to_string(),
                    authorized: [(1, false), (2, true), (3, false)].into_iter().collect(),
                },
                ResourceAuthorization {
                    resource_type: "projects".to_string(),
                    authorized: [(100, true), (200, true), (300, true)]
                        .into_iter()
                        .collect(),
                },
            ];

            let redacted = result.apply_authorizations(&authorizations, &entity_map);

            assert_eq!(redacted, 2);
            assert_eq!(result.authorized_count(), 1);
            assert!(!result.rows()[0].is_authorized());
            assert!(result.rows()[1].is_authorized());
            assert!(!result.rows()[2].is_authorized());
        }

        #[test]
        fn fail_closed_unknown_entity_type() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let entity_map: HashMap<&str, &str> = HashMap::new();

            let redacted = result.apply_authorizations(&[], &entity_map);

            assert_eq!(redacted, 3);
            assert_eq!(result.authorized_count(), 0);
        }

        #[test]
        fn fail_closed_missing_resource_authorization() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let entity_map: HashMap<&str, &str> = [("User", "users"), ("Project", "projects")]
                .into_iter()
                .collect();

            let redacted = result.apply_authorizations(&[], &entity_map);

            assert_eq!(redacted, 3);
            assert_eq!(result.authorized_count(), 0);
        }

        #[test]
        fn fail_closed_partial_resource_authorization() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let entity_map: HashMap<&str, &str> = [("User", "users"), ("Project", "projects")]
                .into_iter()
                .collect();

            let authorizations = vec![ResourceAuthorization {
                resource_type: "users".to_string(),
                authorized: [(1, true), (2, true), (3, true)].into_iter().collect(),
            }];

            let redacted = result.apply_authorizations(&authorizations, &entity_map);

            assert_eq!(redacted, 3);
            assert_eq!(result.authorized_count(), 0);
        }

        #[test]
        fn fail_closed_missing_id_in_authorization() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            let entity_map: HashMap<&str, &str> = [("User", "users"), ("Project", "projects")]
                .into_iter()
                .collect();

            let authorizations = vec![
                ResourceAuthorization {
                    resource_type: "users".to_string(),
                    authorized: [(1, true)].into_iter().collect(),
                },
                ResourceAuthorization {
                    resource_type: "projects".to_string(),
                    authorized: [(100, true), (200, true), (300, true)]
                        .into_iter()
                        .collect(),
                },
            ];

            let redacted = result.apply_authorizations(&authorizations, &entity_map);

            assert_eq!(redacted, 2);
            assert_eq!(result.authorized_count(), 1);
            assert!(result.rows()[0].is_authorized());
        }

        #[test]
        fn already_unauthorized_rows_not_double_counted() {
            let mut result = QueryResult::from_batches(&[make_test_batch()], &test_ctx());
            result.rows_mut()[0].set_unauthorized();

            let redacted = result.apply_authorizations(&full_auth(), &entity_map());

            assert_eq!(redacted, 0);
            assert_eq!(result.authorized_count(), 2);
        }

        #[test]
        fn empty_result_returns_zero() {
            let mut result = QueryResult::from_batches(&[], &ResultContext::new());
            let entity_map: HashMap<&str, &str> = [("User", "users")].into_iter().collect();

            let redacted = result.apply_authorizations(&[], &entity_map);

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

            assert_eq!(result.rows()[0].get_id("u"), Some(1));
            assert_eq!(result.rows()[1].get_id("u"), None);
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

            assert_eq!(result.rows()[0].get_type("u"), Some("User"));
            assert_eq!(result.rows()[1].get_type("u"), None);
        }

        #[test]
        fn node_ref_returns_none_if_id_null() {
            let batch = make_batch(vec![
                ("_gkg_u_id", Arc::new(Int64Array::from(vec![None]))),
                ("_gkg_u_type", Arc::new(StringArray::from(vec!["User"]))),
            ]);

            let result = QueryResult::from_batches(&[batch], &user_ctx());
            assert!(result.rows()[0].node_ref("u").is_none());
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
            assert!(result.rows()[0].node_ref("u").is_none());
        }
    }
}

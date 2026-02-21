use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use query_engine::{HydrationPlan, HydrationTemplate, QueryType, ResultContext, SecurityContext};

use crate::redaction::{ColumnValue, QueryResult};

use super::super::error::PipelineError;

type PropertyMap = HashMap<(String, i64), HashMap<String, ColumnValue>>;

pub struct HydrationStage {
    ontology: Arc<Ontology>,
    client: Arc<ArrowClickHouseClient>,
}

impl HydrationStage {
    pub fn new(ontology: Arc<Ontology>, client: Arc<ArrowClickHouseClient>) -> Self {
        Self { ontology, client }
    }

    /// Pre-auth hydration: for dynamic nodes with indirect authorization
    /// (owner_entity is set), fetch the auth_id_column value from the entity table
    /// so authorization can resolve without relying on edge columns.
    pub async fn resolve_auth_ids(
        &self,
        result: &mut QueryResult,
        result_context: &ResultContext,
        security_context: &SecurityContext,
    ) -> Result<(), PipelineError> {
        let mut indirect_refs: HashMap<String, Vec<i64>> = HashMap::new();
        let mut auth_columns: HashMap<String, String> = HashMap::new();

        for row in result.rows() {
            for node_ref in row.dynamic_nodes() {
                let Some(auth) = result_context.get_entity_auth(&node_ref.entity_type) else {
                    continue;
                };
                if auth.owner_entity.is_none() {
                    continue;
                }
                indirect_refs
                    .entry(node_ref.entity_type.clone())
                    .or_default()
                    .push(node_ref.id);
                auth_columns
                    .entry(node_ref.entity_type.clone())
                    .or_insert_with(|| auth.auth_id_column.clone());
            }
        }

        for ids in indirect_refs.values_mut() {
            ids.sort_unstable();
            ids.dedup();
        }

        if indirect_refs.is_empty() {
            return Ok(());
        }

        let mut overrides = HashMap::new();
        for (entity_type, ids) in &indirect_refs {
            let auth_col = &auth_columns[entity_type];
            let resolved = self
                .fetch_auth_ids(entity_type, ids, auth_col, security_context)
                .await?;
            overrides.extend(resolved);
        }

        result.set_auth_id_overrides(overrides);
        Ok(())
    }

    pub async fn execute(
        &self,
        mut result: QueryResult,
        result_context: &ResultContext,
        security_context: &SecurityContext,
        hydration_plan: &HydrationPlan,
    ) -> Result<QueryResult, PipelineError> {
        match hydration_plan {
            HydrationPlan::None => return Ok(result),
            HydrationPlan::Static(templates) => {
                let property_map = self
                    .hydrate_static(&result, templates, result_context, security_context)
                    .await?;
                self.merge_static_properties(&mut result, &property_map, result_context);
            }
            HydrationPlan::Dynamic => {
                let refs = self.extract_entity_refs(&result, result_context);
                if refs.is_empty() {
                    return Ok(result);
                }
                let property_map = self.fetch_all_properties(&refs, security_context).await?;
                self.merge_dynamic_properties(&mut result, &property_map, result_context);
            }
        }
        Ok(result)
    }

    /// Hydrate using pre-compiled templates (Traversal/Search).
    /// Extracts entity IDs from `_gkg_{alias}_pk` or `_gkg_{alias}_id` columns,
    /// then fetches full properties via search queries.
    async fn hydrate_static(
        &self,
        result: &QueryResult,
        templates: &[HydrationTemplate],
        result_context: &ResultContext,
        security_context: &SecurityContext,
    ) -> Result<PropertyMap, PipelineError> {
        let mut all_props = HashMap::new();

        for template in templates {
            let redaction_node = result_context.get(&template.node_alias);
            let Some(rn) = redaction_node else { continue };

            // Prefer _gkg_{alias}_pk (true primary key) for hydration lookup;
            // fall back to _gkg_{alias}_id which may be the auth ID.
            let mut ids: Vec<i64> = result
                .authorized_rows()
                .filter_map(|row| {
                    row.get_column_i64(&rn.pk_column)
                        .or_else(|| row.get_column_i64(&rn.id_column))
                })
                .collect();
            ids.sort_unstable();
            ids.dedup();

            if ids.is_empty() {
                continue;
            }

            let props = self
                .fetch_entity_properties(&template.entity_type, &ids, security_context)
                .await?;
            all_props.extend(props);
        }

        Ok(all_props)
    }

    /// Merge hydrated properties into rows for static (Traversal/Search) queries.
    fn merge_static_properties(
        &self,
        result: &mut QueryResult,
        property_map: &PropertyMap,
        result_context: &ResultContext,
    ) {
        for row in result.authorized_rows_mut() {
            for rn in result_context.nodes() {
                let pk = row
                    .get_column_i64(&rn.pk_column)
                    .or_else(|| row.get_column_i64(&rn.id_column));
                let Some(pk) = pk else { continue };

                if let Some(props) = property_map.get(&(rn.entity_type.clone(), pk)) {
                    for (key, value) in props {
                        row.set_column(key.clone(), value.clone());
                    }
                }
            }
        }
    }

    /// Extract entity references from dynamic query results (Neighbors or PathFinding).
    fn extract_entity_refs(
        &self,
        result: &QueryResult,
        result_context: &ResultContext,
    ) -> HashMap<String, Vec<i64>> {
        let mut refs: HashMap<String, Vec<i64>> = HashMap::new();

        let is_neighbors = matches!(result_context.query_type, Some(QueryType::Neighbors));

        for row in result.authorized_rows() {
            if is_neighbors
                && let (Some(id), Some(entity_type)) = (
                    row.get_column_i64("_gkg_neighbor_id"),
                    row.get_column_string("_gkg_neighbor_type"),
                )
            {
                refs.entry(entity_type).or_default().push(id);
            }

            // PathFinding and Neighbors both carry dynamic_nodes
            for node_ref in row.dynamic_nodes() {
                refs.entry(node_ref.entity_type.clone())
                    .or_default()
                    .push(node_ref.id);
            }
        }

        for ids in refs.values_mut() {
            ids.sort_unstable();
            ids.dedup();
        }

        refs
    }

    async fn fetch_all_properties(
        &self,
        refs: &HashMap<String, Vec<i64>>,
        security_context: &SecurityContext,
    ) -> Result<PropertyMap, PipelineError> {
        let mut result = HashMap::new();

        for (entity_type, ids) in refs {
            if ids.is_empty() {
                continue;
            }

            let props = self
                .fetch_entity_properties(entity_type, ids, security_context)
                .await?;
            result.extend(props);
        }

        Ok(result)
    }

    async fn fetch_entity_properties(
        &self,
        entity_type: &str,
        ids: &[i64],
        security_context: &SecurityContext,
    ) -> Result<PropertyMap, PipelineError> {
        let query_json = self.build_search_query(entity_type, ids);
        let compiled =
            query_engine::compile_with_columns(&query_json, &self.ontology, security_context)
                .map_err(|e| PipelineError::Compile(e.to_string()))?;

        let mut query = self.client.query(&compiled.sql);
        for (key, value) in &compiled.params {
            query = ArrowClickHouseClient::bind_param(query, key, value);
        }
        let batches = query
            .fetch_arrow()
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))?;

        self.parse_property_batches(entity_type, &batches)
    }

    /// Fetch just the auth_id_column for a set of entity IDs. Returns a map
    /// of (entity_type, entity_pk) -> auth_id for use in pre-auth resolution.
    async fn fetch_auth_ids(
        &self,
        entity_type: &str,
        ids: &[i64],
        auth_id_column: &str,
        security_context: &SecurityContext,
    ) -> Result<HashMap<(String, i64), i64>, PipelineError> {
        let query_json = self.build_auth_id_query(entity_type, ids, auth_id_column);
        let compiled =
            query_engine::compile_with_columns(&query_json, &self.ontology, security_context)
                .map_err(|e| PipelineError::Compile(e.to_string()))?;

        let mut query = self.client.query(&compiled.sql);
        for (key, value) in &compiled.params {
            query = ArrowClickHouseClient::bind_param(query, key, value);
        }
        let batches = query
            .fetch_arrow()
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))?;

        let mut result = HashMap::new();
        for batch in &batches {
            let schema = batch.schema();
            let pk_idx = schema.index_of("_gkg_n_id").ok();
            let auth_idx = schema.index_of(auth_id_column).ok();

            let (Some(pk_idx), Some(auth_idx)) = (pk_idx, auth_idx) else {
                continue;
            };

            let pk_col = batch.column(pk_idx).as_any().downcast_ref::<Int64Array>();
            let auth_col = batch.column(auth_idx).as_any().downcast_ref::<Int64Array>();

            let (Some(pks), Some(auths)) = (pk_col, auth_col) else {
                continue;
            };

            for row in 0..batch.num_rows() {
                if pks.is_null(row) || auths.is_null(row) {
                    continue;
                }
                result.insert((entity_type.to_string(), pks.value(row)), auths.value(row));
            }
        }

        Ok(result)
    }

    fn build_auth_id_query(&self, entity_type: &str, ids: &[i64], auth_id_column: &str) -> String {
        serde_json::json!({
            "query_type": "search",
            "node": {
                "id": "n",
                "entity": entity_type,
                "columns": [auth_id_column],
                "node_ids": ids
            },
            "limit": 1000
        })
        .to_string()
    }

    fn build_search_query(&self, entity_type: &str, ids: &[i64]) -> String {
        serde_json::json!({
            "query_type": "search",
            "node": {
                "id": "n",
                "entity": entity_type,
                "columns": "*",
                "node_ids": ids
            },
            "limit": 1000
        })
        .to_string()
    }

    fn parse_property_batches(
        &self,
        entity_type: &str,
        batches: &[RecordBatch],
    ) -> Result<PropertyMap, PipelineError> {
        let mut result = HashMap::new();

        for batch in batches {
            let schema = batch.schema();
            let id_idx = schema.index_of("_gkg_n_id").ok();

            let Some(id_idx) = id_idx else {
                continue;
            };

            let id_col = batch.column(id_idx).as_any().downcast_ref::<Int64Array>();
            let Some(ids) = id_col else {
                continue;
            };

            for row in 0..batch.num_rows() {
                if ids.is_null(row) {
                    continue;
                }
                let id = ids.value(row);

                let mut props = HashMap::new();
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let name = field.name();
                    if name.starts_with("_gkg_") {
                        continue;
                    }
                    if let Some(value) = self.column_value_to_column(batch.column(col_idx), row) {
                        props.insert(name.clone(), value);
                    }
                }

                result.insert((entity_type.to_string(), id), props);
            }
        }

        Ok(result)
    }

    fn column_value_to_column(&self, col: &dyn Array, row: usize) -> Option<ColumnValue> {
        if col.is_null(row) {
            return Some(ColumnValue::Null);
        }
        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            return Some(ColumnValue::String(arr.value(row).to_string()));
        }
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            return Some(ColumnValue::Int64(arr.value(row)));
        }
        if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
            return Some(ColumnValue::String(arr.value(row).to_string()));
        }
        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            return Some(ColumnValue::String(arr.value(row).to_string()));
        }
        None
    }

    /// Merge hydrated properties into rows for dynamic (Neighbors/PathFinding) queries.
    ///
    /// Neighbors: properties are merged as flat columns on the row (one neighbor per row).
    /// PathFinding: properties are stored as a JSON array in a `path_nodes` column.
    /// Each element has `id`, `type`, and the entity's property columns (with the
    /// hydration query alias prefix stripped so names match the ontology schema).
    fn merge_dynamic_properties(
        &self,
        result: &mut QueryResult,
        property_map: &PropertyMap,
        result_context: &ResultContext,
    ) {
        let is_neighbors = matches!(result_context.query_type, Some(QueryType::Neighbors));

        for row in result.authorized_rows_mut() {
            if is_neighbors {
                if let Some(id) = row.get_column_i64("_gkg_neighbor_id")
                    && let Some(entity_type) = row.get_column_string("_gkg_neighbor_type")
                    && let Some(props) = property_map.get(&(entity_type, id))
                {
                    for (key, value) in props {
                        row.set_column(key.clone(), value.clone());
                    }
                }
                continue;
            }

            let nodes: Vec<serde_json::Value> = row
                .dynamic_nodes()
                .iter()
                .map(|node_ref| {
                    let mut obj = serde_json::Map::new();
                    obj.insert("id".to_string(), serde_json::json!(node_ref.id));
                    obj.insert("type".to_string(), serde_json::json!(&node_ref.entity_type));

                    if let Some(props) =
                        property_map.get(&(node_ref.entity_type.clone(), node_ref.id))
                    {
                        for (key, value) in props {
                            let name = key.strip_prefix("n_").unwrap_or(key);
                            let json_val = match value {
                                ColumnValue::Int64(v) => serde_json::json!(v),
                                ColumnValue::String(v) => serde_json::json!(v),
                                ColumnValue::Json(v) => v.clone(),
                                ColumnValue::Null => serde_json::Value::Null,
                            };
                            obj.insert(name.to_string(), json_val);
                        }
                    }

                    serde_json::Value::Object(obj)
                })
                .collect();

            row.set_column(
                "path_nodes".to_string(),
                ColumnValue::Json(serde_json::Value::Array(nodes)),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redaction::{NodeRef, QueryResultRow};
    use query_engine::QueryType;
    use std::collections::HashMap;

    fn make_path_result(paths: Vec<Vec<NodeRef>>) -> (QueryResult, ResultContext) {
        let mut ctx = ResultContext::new();
        ctx = ctx.with_query_type(QueryType::PathFinding);

        let rows: Vec<QueryResultRow> = paths
            .into_iter()
            .map(|nodes| {
                let mut columns = HashMap::new();
                columns.insert("depth".to_string(), ColumnValue::Int64(nodes.len() as i64));
                QueryResultRow::new(columns, nodes)
            })
            .collect();

        (QueryResult::from_rows(rows, ctx.clone()), ctx)
    }

    fn make_neighbor_result(neighbors: Vec<NodeRef>) -> (QueryResult, ResultContext) {
        let mut ctx = ResultContext::new();
        ctx = ctx.with_query_type(QueryType::Neighbors);

        let rows: Vec<QueryResultRow> = neighbors
            .into_iter()
            .map(|node| {
                let mut columns = HashMap::new();
                columns.insert("_gkg_neighbor_id".to_string(), ColumnValue::Int64(node.id));
                columns.insert(
                    "_gkg_neighbor_type".to_string(),
                    ColumnValue::String(node.entity_type.clone()),
                );
                QueryResultRow::new(columns, vec![node])
            })
            .collect();

        (QueryResult::from_rows(rows, ctx.clone()), ctx)
    }

    fn make_property_map(entries: Vec<(&str, i64, Vec<(&str, ColumnValue)>)>) -> PropertyMap {
        let mut map = HashMap::new();
        for (entity_type, id, props) in entries {
            let prop_map: HashMap<String, ColumnValue> =
                props.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
            map.insert((entity_type.to_string(), id), prop_map);
        }
        map
    }

    fn make_stage() -> HydrationStage {
        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        let client = Arc::new(ArrowClickHouseClient::new(
            "http://localhost:8123",
            "test",
            "default",
            None,
        ));
        HydrationStage::new(ontology, client)
    }

    #[test]
    fn path_merge_builds_hydrated_path_nodes_array() {
        let stage = make_stage();
        let path = vec![
            NodeRef::new(1, "User"),
            NodeRef::new(100, "Group"),
            NodeRef::new(1000, "Project"),
        ];
        let (mut result, ctx) = make_path_result(vec![path]);

        let props = make_property_map(vec![
            (
                "User",
                1,
                vec![
                    ("n_username", ColumnValue::String("alice".to_string())),
                    ("n_state", ColumnValue::String("active".to_string())),
                ],
            ),
            (
                "Group",
                100,
                vec![("n_name", ColumnValue::String("Engineering".to_string()))],
            ),
            (
                "Project",
                1000,
                vec![("n_name", ColumnValue::String("gkg".to_string()))],
            ),
        ]);

        stage.merge_dynamic_properties(&mut result, &props, &ctx);

        let row = &result.rows()[0];
        let path_nodes = row.get("path_nodes").expect("path_nodes should be set");
        let ColumnValue::Json(json_val) = path_nodes else {
            panic!("path_nodes should be ColumnValue::Json");
        };

        let arr = json_val.as_array().unwrap();
        assert_eq!(arr.len(), 3);

        assert_eq!(arr[0]["id"], 1);
        assert_eq!(arr[0]["type"], "User");
        assert_eq!(arr[0]["username"], "alice");
        assert_eq!(arr[0]["state"], "active");

        assert_eq!(arr[1]["id"], 100);
        assert_eq!(arr[1]["type"], "Group");
        assert_eq!(arr[1]["name"], "Engineering");

        assert_eq!(arr[2]["id"], 1000);
        assert_eq!(arr[2]["type"], "Project");
        assert_eq!(arr[2]["name"], "gkg");
    }

    #[test]
    fn path_merge_handles_missing_properties() {
        let stage = make_stage();
        let path = vec![NodeRef::new(1, "User"), NodeRef::new(999, "Group")];
        let (mut result, ctx) = make_path_result(vec![path]);

        // Only User has properties; Group 999 is missing from the map.
        let props = make_property_map(vec![(
            "User",
            1,
            vec![("n_username", ColumnValue::String("alice".to_string()))],
        )]);

        stage.merge_dynamic_properties(&mut result, &props, &ctx);

        let row = &result.rows()[0];
        let ColumnValue::Json(json_val) = row.get("path_nodes").unwrap() else {
            panic!("expected Json");
        };
        let arr = json_val.as_array().unwrap();
        assert_eq!(arr.len(), 2);

        // User has properties
        assert_eq!(arr[0]["username"], "alice");

        // Group 999 has only id and type (no properties found)
        assert_eq!(arr[1]["id"], 999);
        assert_eq!(arr[1]["type"], "Group");
        assert_eq!(arr[1].as_object().unwrap().len(), 2);
    }

    #[test]
    fn path_merge_handles_multiple_rows() {
        let stage = make_stage();
        let path1 = vec![NodeRef::new(1, "User"), NodeRef::new(100, "Group")];
        let path2 = vec![NodeRef::new(2, "User"), NodeRef::new(100, "Group")];
        let (mut result, ctx) = make_path_result(vec![path1, path2]);

        let props = make_property_map(vec![
            (
                "User",
                1,
                vec![("n_username", ColumnValue::String("alice".to_string()))],
            ),
            (
                "User",
                2,
                vec![("n_username", ColumnValue::String("bob".to_string()))],
            ),
            (
                "Group",
                100,
                vec![("n_name", ColumnValue::String("Engineering".to_string()))],
            ),
        ]);

        stage.merge_dynamic_properties(&mut result, &props, &ctx);

        let rows = result.rows();
        let ColumnValue::Json(v1) = rows[0].get("path_nodes").unwrap() else {
            panic!("expected Json");
        };
        let ColumnValue::Json(v2) = rows[1].get("path_nodes").unwrap() else {
            panic!("expected Json");
        };

        assert_eq!(v1[0]["username"], "alice");
        assert_eq!(v2[0]["username"], "bob");
        // Both paths share group 100
        assert_eq!(v1[1]["name"], "Engineering");
        assert_eq!(v2[1]["name"], "Engineering");
    }

    #[test]
    fn neighbor_merge_sets_flat_columns() {
        let stage = make_stage();
        let (mut result, ctx) =
            make_neighbor_result(vec![NodeRef::new(1, "User"), NodeRef::new(100, "Group")]);

        let props = make_property_map(vec![
            (
                "User",
                1,
                vec![("n_username", ColumnValue::String("alice".to_string()))],
            ),
            (
                "Group",
                100,
                vec![("n_name", ColumnValue::String("Engineering".to_string()))],
            ),
        ]);

        stage.merge_dynamic_properties(&mut result, &props, &ctx);

        let rows = result.rows();
        // Neighbor rows get flat columns (with n_ prefix from hydration query)
        assert_eq!(
            rows[0].get("n_username"),
            Some(&ColumnValue::String("alice".to_string()))
        );
        assert_eq!(
            rows[1].get("n_name"),
            Some(&ColumnValue::String("Engineering".to_string()))
        );
        // Neighbors should NOT have path_nodes column
        assert!(rows[0].get("path_nodes").is_none());
        assert!(rows[1].get("path_nodes").is_none());
    }

    #[test]
    fn path_merge_strips_n_prefix_from_property_names() {
        let stage = make_stage();
        let path = vec![NodeRef::new(1, "User")];
        let (mut result, ctx) = make_path_result(vec![path]);

        let props = make_property_map(vec![(
            "User",
            1,
            vec![
                ("n_username", ColumnValue::String("alice".to_string())),
                ("n_id", ColumnValue::Int64(1)),
            ],
        )]);

        stage.merge_dynamic_properties(&mut result, &props, &ctx);

        let row = &result.rows()[0];
        let ColumnValue::Json(json_val) = row.get("path_nodes").unwrap() else {
            panic!("expected Json");
        };
        let node = &json_val[0];
        // "n_username" becomes "username", "n_id" becomes "id" (but id is also set from NodeRef)
        assert!(node.get("username").is_some());
        assert!(node.get("n_username").is_none());
    }
}

//! Enforce return columns for query results.
//!
//! Ensures all query results include ID and type columns for entities, enabling
//! the gkg-server to extract entity IDs and types for redaction validation.
//!
//! For aggregation queries, only nodes that appear in GROUP BY clauses can have
//! their ID columns selected (aggregated nodes don't have individual IDs).
//!
//! For path finding queries, the start node's ID is added to the base query and
//! the end node's ID is added to the final query.

use llqm::expr;

use crate::constants::{primary_key_column, redaction_id_column, redaction_type_column};
use crate::error::Result;
use crate::input::{EntityAuthConfig, Input, QueryType};
use crate::lower::LoweredQuery;
use ontology::constants::DEFAULT_PRIMARY_KEY;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionNode {
    pub alias: String,
    pub entity_type: String,
    /// Column holding the entity's own row ID (always "id"). Used for hydration lookups.
    pub pk_column: String,
    /// Column holding the global ID used for authorization lookup. For most entities
    /// this is "id", but for entities like Definition it is "project_id" — the ID
    /// of the resource whose access controls govern this entity.
    pub id_column: String,
    pub type_column: String,
}

#[derive(Debug, Clone, Default)]
pub struct ResultContext {
    pub query_type: Option<QueryType>,
    nodes: HashMap<String, RedactionNode>,
    /// Auth config for every entity type that requires redaction.
    /// Covers all entities in the ontology, not just those in the current query,
    /// so dynamic nodes (path/neighbors) can be resolved without re-consulting the ontology.
    entity_auth: HashMap<String, EntityAuthConfig>,
}

impl ResultContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_query_type(mut self, query_type: QueryType) -> Self {
        self.query_type = Some(query_type);
        self
    }

    pub fn add_node(&mut self, alias: &str, entity_type: &str) {
        self.nodes.insert(
            alias.to_string(),
            RedactionNode {
                alias: alias.to_string(),
                entity_type: entity_type.to_string(),
                pk_column: primary_key_column(alias),
                id_column: redaction_id_column(alias),
                type_column: redaction_type_column(alias),
            },
        );
    }

    pub fn add_entity_auth(&mut self, entity_type: impl Into<String>, config: EntityAuthConfig) {
        self.entity_auth.insert(entity_type.into(), config);
    }

    pub fn get_entity_auth(&self, entity_type: &str) -> Option<&EntityAuthConfig> {
        self.entity_auth.get(entity_type)
    }

    pub fn entity_auth(&self) -> impl Iterator<Item = (&str, &EntityAuthConfig)> {
        self.entity_auth.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn nodes(&self) -> impl Iterator<Item = &RedactionNode> {
        self.nodes.values()
    }

    pub fn get(&self, alias: &str) -> Option<&RedactionNode> {
        self.nodes.get(alias)
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }
}

/// Enforce redaction return columns on a `LoweredQuery`.
///
/// Adds `_gkg_{alias}_id` and `_gkg_{alias}_type` columns to `lq.projections`.
/// For aggregation queries, also adds the redaction ID expr to `lq.group_by`.
/// For entities with non-default redaction IDs, adds a separate `_gkg_{alias}_pk`.
pub fn enforce_return(lq: &mut LoweredQuery, input: &Input) -> Result<ResultContext> {
    let mut ctx = ResultContext::new().with_query_type(input.query_type);
    ctx.entity_auth = input.entity_auth.clone();

    let selectable_nodes: HashSet<&str> = match input.query_type {
        QueryType::Aggregation => input
            .aggregations
            .iter()
            .filter_map(|agg| agg.group_by.as_deref())
            .collect(),
        QueryType::Traversal | QueryType::Search | QueryType::Neighbors => {
            input.nodes.iter().map(|n| n.id.as_str()).collect()
        }
        QueryType::PathFinding => HashSet::new(),
    };

    for node in &input.nodes {
        let Some(entity) = &node.entity else { continue };

        if !selectable_nodes.contains(node.id.as_str()) {
            continue;
        }

        ctx.add_node(&node.id, entity);
        let redaction_node = ctx.get(&node.id).expect("just inserted by add_node");

        let pk_col = redaction_node.pk_column.clone();
        let id_col = redaction_node.id_column.clone();
        let type_col = redaction_node.type_column.clone();

        let needs_separate_pk = node.redaction_id_column != DEFAULT_PRIMARY_KEY;

        // When the auth ID column differs from "id", emit a separate pk column
        // so hydration can still look up the entity by its own row ID.
        if needs_separate_pk {
            let has_pk = lq.projections.iter().any(|(_, a)| *a == pk_col);
            if !has_pk {
                lq.projections
                    .push((expr::col(&node.id, DEFAULT_PRIMARY_KEY), pk_col));
            }
        }

        let has_id = lq.projections.iter().any(|(_, a)| *a == id_col);
        let has_type = lq.projections.iter().any(|(_, a)| *a == type_col);

        if !has_id {
            let id_expr = expr::col(&node.id, &node.redaction_id_column);
            lq.projections.push((id_expr.clone(), id_col.clone()));

            // Push down id column to aggregation group by if not already present.
            if input.query_type == QueryType::Aggregation
                && !lq.group_by.is_empty()
                && !lq.group_by.iter().any(|(e, _)| *e == id_expr)
            {
                lq.group_by.push((id_expr, id_col.clone()));
            }
        }

        if !has_type {
            // Insert type column right after the id column.
            let insert_pos = lq
                .projections
                .iter()
                .position(|(_, a)| *a == id_col)
                .map(|i| i + 1)
                .unwrap_or(lq.projections.len());

            lq.projections
                .insert(insert_pos, (expr::string(entity.as_str()), type_col));
        }
    }

    Ok(ctx)
}

#[cfg(test)]
#[allow(irrefutable_let_patterns)]
mod tests {
    use super::*;
    use crate::input::{InputNode, QueryType};
    use crate::lower::SelectItem;
    use llqm::expr::DataType;
    use llqm::plan::PlanBuilder;

    fn test_input() -> Input {
        Input {
            nodes: vec![
                InputNode {
                    id: "u".to_string(),
                    entity: Some("User".to_string()),
                    table: Some("gl_user".to_string()),
                    ..Default::default()
                },
                InputNode {
                    id: "p".to_string(),
                    entity: Some("Project".to_string()),
                    table: Some("gl_project".to_string()),
                    ..Default::default()
                },
            ],
            ..Input::default()
        }
    }

    fn make_base_lq(projections: Vec<SelectItem>) -> LoweredQuery {
        let mut b = PlanBuilder::new();
        let u = b.read(
            "gl_user",
            "u",
            &[
                ("id", DataType::Int64),
                ("username", DataType::String),
                ("traversal_path", DataType::String),
            ],
        );
        let p = b.read(
            "gl_project",
            "p",
            &[
                ("id", DataType::Int64),
                ("name", DataType::String),
                ("traversal_path", DataType::String),
            ],
        );
        let joined = b.join(
            llqm::expr::JoinType::Inner,
            u,
            p,
            expr::eq(expr::col("u", "id"), expr::col("p", "id")),
        );
        LoweredQuery {
            builder: b,
            base_rel: joined,
            projections,
            group_by: vec![],
            agg_measures: vec![],
            sort_keys: vec![],
            limit: Some(30),
            offset: None,
            ctes: vec![],
        }
    }

    #[test]
    fn adds_type_columns_after_id_columns() {
        let projections = vec![
            (expr::col("u", "id"), "_gkg_u_id".to_string()),
            (expr::col("p", "id"), "_gkg_p_id".to_string()),
        ];
        let mut lq = make_base_lq(projections);
        let input = test_input();

        enforce_return(&mut lq, &input).unwrap();

        assert_eq!(lq.projections.len(), 4);
        assert_eq!(lq.projections[0].1, "_gkg_u_id");
        assert_eq!(lq.projections[1].1, "_gkg_u_type");
        assert_eq!(lq.projections[2].1, "_gkg_p_id");
        assert_eq!(lq.projections[3].1, "_gkg_p_type");
    }

    #[test]
    fn skips_existing_type_columns() {
        let projections = vec![
            (expr::col("u", "id"), "_gkg_u_id".to_string()),
            (expr::string("User"), "_gkg_u_type".to_string()),
            (expr::col("p", "id"), "_gkg_p_id".to_string()),
        ];
        let mut lq = make_base_lq(projections);
        let input = test_input();

        enforce_return(&mut lq, &input).unwrap();

        assert_eq!(lq.projections.len(), 4);
        assert_eq!(lq.projections[0].1, "_gkg_u_id");
        assert_eq!(lq.projections[1].1, "_gkg_u_type");
        assert_eq!(lq.projections[2].1, "_gkg_p_id");
        assert_eq!(lq.projections[3].1, "_gkg_p_type");
    }

    #[test]
    fn adds_id_and_type_columns_when_missing() {
        let projections = vec![(expr::col("u", "username"), "name".to_string())];
        let mut lq = make_base_lq(projections);
        let input = test_input();

        enforce_return(&mut lq, &input).unwrap();

        assert_eq!(lq.projections.len(), 5);
        assert_eq!(lq.projections[0].1, "name");
        assert_eq!(lq.projections[1].1, "_gkg_u_id");
        assert_eq!(lq.projections[2].1, "_gkg_u_type");
        assert_eq!(lq.projections[3].1, "_gkg_p_id");
        assert_eq!(lq.projections[4].1, "_gkg_p_type");
    }

    #[test]
    fn skips_nodes_without_entity() {
        let input = Input {
            nodes: vec![InputNode {
                id: "n".to_string(),
                ..Default::default()
            }],
            ..Input::default()
        };

        let mut b = PlanBuilder::new();
        let rel = b.read(
            "kg_node",
            "n",
            &[("id", DataType::Int64), ("label", DataType::String)],
        );
        let mut lq = LoweredQuery {
            builder: b,
            base_rel: rel,
            projections: vec![(expr::col("n", "id"), "n_id".to_string())],
            group_by: vec![],
            agg_measures: vec![],
            sort_keys: vec![],
            limit: Some(30),
            offset: None,
            ctes: vec![],
        };

        let ctx = enforce_return(&mut lq, &input).unwrap();

        assert_eq!(lq.projections.len(), 1);
        assert!(ctx.is_empty());
    }

    #[test]
    fn builds_result_context() {
        let input = test_input();
        let mut lq = make_base_lq(vec![]);

        let ctx = enforce_return(&mut lq, &input).unwrap();

        assert_eq!(ctx.len(), 2);

        let user = ctx.get("u").unwrap();
        assert_eq!(user.entity_type, "User");
        assert_eq!(user.id_column, "_gkg_u_id");
        assert_eq!(user.type_column, "_gkg_u_type");

        let project = ctx.get("p").unwrap();
        assert_eq!(project.entity_type, "Project");
    }

    #[test]
    fn aggregation_only_adds_columns_for_group_by_nodes() {
        use crate::input::{AggFunction, InputAggregation};

        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "u".to_string(),
                    entity: Some("User".to_string()),
                    table: Some("gl_user".to_string()),
                    ..Default::default()
                },
                InputNode {
                    id: "n".to_string(),
                    entity: Some("Note".to_string()),
                    table: Some("gl_note".to_string()),
                    ..Default::default()
                },
            ],
            aggregations: vec![InputAggregation {
                function: AggFunction::Count,
                target: Some("n".to_string()),
                group_by: Some("u".to_string()),
                property: None,
                alias: Some("note_count".to_string()),
            }],
            limit: 10,
            ..Input::default()
        };

        let mut b = PlanBuilder::new();
        let rel = b.read(
            "gl_user",
            "u",
            &[("id", DataType::Int64), ("username", DataType::String)],
        );
        let mut lq = LoweredQuery {
            builder: b,
            base_rel: rel,
            projections: vec![(expr::col("u", "id"), "u_id".to_string())],
            group_by: vec![(expr::col("u", "id"), "u_id".to_string())],
            agg_measures: vec![],
            sort_keys: vec![],
            limit: Some(10),
            offset: None,
            ctes: vec![],
        };

        let ctx = enforce_return(&mut lq, &input).unwrap();

        // Should only have columns for 'u' (group_by node), not 'n' (target node)
        assert_eq!(lq.projections.len(), 3); // u_id, _gkg_u_id, _gkg_u_type
        assert!(lq.projections.iter().any(|(_, a)| a == "_gkg_u_id"));
        assert!(lq.projections.iter().any(|(_, a)| a == "_gkg_u_type"));
        assert!(!lq.projections.iter().any(|(_, a)| a == "_gkg_n_id"));
        assert!(!lq.projections.iter().any(|(_, a)| a == "_gkg_n_type"));
        assert_eq!(lq.group_by.len(), 1); // u.id already present, no duplicate added

        // Context should only have the group_by node
        assert_eq!(ctx.len(), 1);
        assert!(ctx.get("u").is_some());
        assert!(ctx.get("n").is_none());
    }

    #[test]
    fn aggregation_adds_redaction_id_to_group_by() {
        use crate::input::{AggFunction, InputAggregation};

        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "u".to_string(),
                    entity: Some("User".to_string()),
                    table: Some("gl_user".to_string()),
                    ..Default::default()
                },
                InputNode {
                    id: "mr".to_string(),
                    entity: Some("MergeRequest".to_string()),
                    table: Some("gl_merge_request".to_string()),
                    ..Default::default()
                },
            ],
            aggregations: vec![InputAggregation {
                function: AggFunction::Count,
                target: Some("mr".to_string()),
                group_by: Some("u".to_string()),
                property: None,
                alias: Some("mr_count".to_string()),
            }],
            ..Input::default()
        };

        let mut b = PlanBuilder::new();
        let rel = b.read(
            "gl_user",
            "u",
            &[("id", DataType::Int64), ("username", DataType::String)],
        );
        let mut lq = LoweredQuery {
            builder: b,
            base_rel: rel,
            projections: vec![(expr::col("u", "username"), "u_username".to_string())],
            group_by: vec![(expr::col("u", "username"), "u_username".to_string())],
            agg_measures: vec![],
            sort_keys: vec![],
            limit: Some(10),
            offset: None,
            ctes: vec![],
        };

        enforce_return(&mut lq, &input).unwrap();

        assert!(
            lq.group_by.iter().any(|(e, _)| *e == expr::col("u", "id")),
            "redaction id column must be in GROUP BY: {:?}",
            lq.group_by
        );
        assert_eq!(lq.group_by.len(), 2); // username + id
    }

    #[test]
    fn uses_correct_redaction_id_column_per_node() {
        let mut b = PlanBuilder::new();
        let d = b.read(
            "gl_definition",
            "d",
            &[
                ("id", DataType::Int64),
                ("project_id", DataType::Int64),
                ("name", DataType::String),
                ("traversal_path", DataType::String),
            ],
        );
        let p = b.read(
            "gl_project",
            "p",
            &[
                ("id", DataType::Int64),
                ("name", DataType::String),
                ("traversal_path", DataType::String),
            ],
        );
        let joined = b.join(
            llqm::expr::JoinType::Inner,
            d,
            p,
            expr::eq(expr::col("d", "id"), expr::col("p", "id")),
        );
        let mut lq = LoweredQuery {
            builder: b,
            base_rel: joined,
            projections: vec![],
            group_by: vec![],
            agg_measures: vec![],
            sort_keys: vec![],
            limit: Some(10),
            offset: None,
            ctes: vec![],
        };

        let input = Input {
            nodes: vec![
                InputNode {
                    id: "d".to_string(),
                    entity: Some("Definition".to_string()),
                    table: Some("gl_definition".to_string()),
                    redaction_id_column: "project_id".to_string(),
                    ..Default::default()
                },
                InputNode {
                    id: "p".to_string(),
                    entity: Some("Project".to_string()),
                    table: Some("gl_project".to_string()),
                    ..Default::default()
                },
            ],
            limit: 10,
            ..Input::default()
        };

        let ctx = enforce_return(&mut lq, &input).unwrap();

        assert_eq!(lq.projections.len(), 5);

        // Definition: pk column (d.id) + auth id column (d.project_id) + type literal
        assert_eq!(lq.projections[0].1, "_gkg_d_pk");
        assert_eq!(lq.projections[0].0, expr::col("d", "id"));
        assert_eq!(lq.projections[1].1, "_gkg_d_id");
        assert_eq!(lq.projections[1].0, expr::col("d", "project_id"));
        assert_eq!(lq.projections[2].1, "_gkg_d_type");
        assert_eq!(lq.projections[2].0, expr::string("Definition"));

        // Project: default id column + type literal (no separate pk needed)
        assert_eq!(lq.projections[3].1, "_gkg_p_id");
        assert_eq!(lq.projections[3].0, expr::col("p", "id"));
        assert_eq!(lq.projections[4].1, "_gkg_p_type");
        assert_eq!(lq.projections[4].0, expr::string("Project"));

        assert_eq!(ctx.len(), 2);
        let d_node = ctx.get("d").unwrap();
        assert_eq!(d_node.entity_type, "Definition");
        assert_eq!(d_node.pk_column, "_gkg_d_pk");
        assert_eq!(d_node.id_column, "_gkg_d_id");
        let p_node = ctx.get("p").unwrap();
        assert_eq!(p_node.entity_type, "Project");
        assert_eq!(p_node.pk_column, "_gkg_p_pk");
        assert_eq!(p_node.id_column, "_gkg_p_id");
    }

    #[test]
    fn path_finding_uses_gkg_path_column() {
        use crate::input::InputPath;

        let input = Input {
            query_type: QueryType::PathFinding,
            nodes: vec![
                InputNode {
                    id: "start".to_string(),
                    entity: Some("Project".to_string()),
                    table: Some("gl_project".to_string()),
                    node_ids: vec![100],
                    ..Default::default()
                },
                InputNode {
                    id: "end".to_string(),
                    entity: Some("Project".to_string()),
                    table: Some("gl_project".to_string()),
                    node_ids: vec![200],
                    ..Default::default()
                },
            ],
            path: Some(InputPath {
                path_type: crate::input::PathType::Shortest,
                from: "start".to_string(),
                to: "end".to_string(),
                max_depth: 3,
                rel_types: vec![],
            }),
            ..Input::default()
        };

        let mut b = PlanBuilder::new();
        let rel = b.read(
            "gl_project",
            "start",
            &[("id", DataType::Int64), ("name", DataType::String)],
        );
        let mut lq = LoweredQuery {
            builder: b,
            base_rel: rel,
            projections: vec![(expr::col("paths", "path"), "_gkg_path".to_string())],
            group_by: vec![],
            agg_measures: vec![],
            sort_keys: vec![],
            limit: Some(30),
            offset: None,
            ctes: vec![],
        };

        let ctx = enforce_return(&mut lq, &input).unwrap();

        // Path finding queries use _gkg_path column for redaction data.
        // No additional _gkg_* columns are added by enforce_return.
        assert!(ctx.is_empty());
        assert_eq!(ctx.query_type, Some(QueryType::PathFinding));
    }

    #[test]
    fn default_entity_does_not_emit_pk_column() {
        let input = Input {
            nodes: vec![InputNode {
                id: "p".to_string(),
                entity: Some("Project".to_string()),
                table: Some("gl_project".to_string()),
                ..Default::default()
            }],
            ..Input::default()
        };

        let mut b = PlanBuilder::new();
        let rel = b.read(
            "gl_project",
            "p",
            &[
                ("id", DataType::Int64),
                ("name", DataType::String),
                ("traversal_path", DataType::String),
            ],
        );
        let mut lq = LoweredQuery {
            builder: b,
            base_rel: rel,
            projections: vec![(expr::col("p", "name"), "p_name".to_string())],
            group_by: vec![],
            agg_measures: vec![],
            sort_keys: vec![],
            limit: None,
            offset: None,
            ctes: vec![],
        };

        enforce_return(&mut lq, &input).unwrap();

        let aliases: Vec<_> = lq.projections.iter().map(|(_, a)| a.as_str()).collect();
        assert!(aliases.contains(&"_gkg_p_id"));
        assert!(aliases.contains(&"_gkg_p_type"));
        assert!(
            !aliases.contains(&"_gkg_p_pk"),
            "default entity (redaction_id_column == id) should not emit _gkg_p_pk"
        );
        assert_eq!(lq.projections.len(), 3); // p_name + _gkg_p_id + _gkg_p_type
    }
}

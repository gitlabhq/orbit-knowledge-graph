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

use crate::ast::{Expr, JoinType, Node, Query, SelectExpr, TableRef};
use crate::constants::{primary_key_column, redaction_id_column, redaction_type_column};
use crate::error::{QueryError, Result};
use crate::input::{EntityAuthConfig, Input, QueryType};
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

/// Metadata for an edge relationship in the query, used by formatters to extract
/// edge columns without scanning column names.
#[derive(Debug, Clone)]
pub struct EdgeMeta {
    /// Column prefix for this edge (e.g. "e0_", "hop_e1_").
    pub column_prefix: String,
    /// Optional internal path column for multi-hop relationships.
    pub path_column: Option<String>,
    /// Relationship types from the input (e.g. ["AUTHORED"]).
    pub rel_types: Vec<String>,
    /// Source node alias (e.g. "u").
    pub from_alias: String,
    /// Target node alias (e.g. "p").
    pub to_alias: String,
    /// Pre-computed column name for edge type (e.g. "e0_type").
    pub type_column: String,
    /// Pre-computed column name for source ID (e.g. "e0_src").
    pub src_column: String,
    /// Pre-computed column name for source type (e.g. "e0_src_type").
    pub src_type_column: String,
    /// Pre-computed column name for destination ID (e.g. "e0_dst").
    pub dst_column: String,
    /// Pre-computed column name for destination type (e.g. "e0_dst_type").
    pub dst_type_column: String,
}

#[derive(Debug, Clone, Default)]
pub struct ResultContext {
    pub query_type: Option<QueryType>,
    nodes: HashMap<String, RedactionNode>,
    /// Auth config for every entity type that requires redaction.
    /// Covers all entities in the ontology, not just those in the current query,
    /// so dynamic nodes (path/neighbors) can be resolved without re-consulting the ontology.
    entity_auth: HashMap<String, EntityAuthConfig>,
    edges: Vec<EdgeMeta>,
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

    pub fn add_edge(&mut self, edge: EdgeMeta) {
        self.edges.push(edge);
    }

    pub fn edges(&self) -> &[EdgeMeta] {
        &self.edges
    }
}

pub fn enforce_return(node: &mut Node, input: &Input) -> Result<ResultContext> {
    let mut ctx = ResultContext::new().with_query_type(input.query_type);
    ctx.entity_auth = input.entity_auth.clone();

    let selectable_nodes: HashSet<&str> = match input.query_type {
        QueryType::Aggregation => input
            .aggregations
            .iter()
            .filter_map(|agg| agg.group_by.as_deref())
            .collect(),
        QueryType::Traversal | QueryType::Neighbors => {
            input.nodes.iter().map(|n| n.id.as_str()).collect()
        }
        QueryType::PathFinding | QueryType::Hydration => HashSet::new(),
    };

    match node {
        Node::Query(q) => enforce_return_columns(q, input, &selectable_nodes, &mut ctx)?,
        Node::Insert(_) => return Ok(ctx),
    }

    if matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) {
        use crate::constants::{
            EDGE_DST_SUFFIX, EDGE_DST_TYPE_SUFFIX, EDGE_SRC_SUFFIX, EDGE_SRC_TYPE_SUFFIX,
            EDGE_TYPE_SUFFIX,
        };

        for (i, rel) in input.relationships.iter().enumerate() {
            let prefix = if rel.max_hops > 1 {
                format!("hop_e{i}_")
            } else {
                format!("e{i}_")
            };
            let path_column = (rel.max_hops > 1).then(|| format!("{prefix}path_nodes"));
            ctx.edges.push(EdgeMeta {
                type_column: format!("{prefix}{EDGE_TYPE_SUFFIX}"),
                src_column: format!("{prefix}{EDGE_SRC_SUFFIX}"),
                src_type_column: format!("{prefix}{EDGE_SRC_TYPE_SUFFIX}"),
                dst_column: format!("{prefix}{EDGE_DST_SUFFIX}"),
                dst_type_column: format!("{prefix}{EDGE_DST_TYPE_SUFFIX}"),
                column_prefix: prefix,
                path_column,
                rel_types: rel.types.clone(),
                from_alias: rel.from.clone(),
                to_alias: rel.to.clone(),
            });
        }
    }

    Ok(ctx)
}

/// Ensure `expr` sits in `GROUP BY` for aggregation queries. The identity
/// columns pushed into SELECT (`_gkg_*_pk`, `_gkg_*_id`) are functionally
/// dependent on the group key, but DuckDB's strict GROUP BY requires them
/// in the clause. ClickHouse accepts the redundancy.
fn ensure_in_group_by(q: &mut Query, query_type: QueryType, expr: Expr) {
    if query_type != QueryType::Aggregation {
        return;
    }
    if q.group_by.is_empty() || q.group_by.contains(&expr) {
        return;
    }
    q.group_by.push(expr);
}

fn enforce_return_columns(
    q: &mut Query,
    input: &Input,
    selectable_nodes: &HashSet<&str>,
    ctx: &mut ResultContext,
) -> Result<()> {
    let select_len_before = q.select.len();
    // Neighbors emit _gkg_* columns directly in the lowerer (per UNION arm)
    // because the center edge column differs per direction.
    // Search-shaped traversal (1 node, 0 rels) uses table-centric columns
    // like search, not edge-centric. The lowerer produces a flat table scan
    // without populating node_edge_col.
    let globally_edge_centric = matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Neighbors
    ) && !input.is_search();
    let node_edge_col = &input.compiler.node_edge_col;

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

        // Neighbors emit _gkg_* columns directly in the lowerer per UNION arm
        // because the center edge column differs per direction.
        if input.query_type == QueryType::Neighbors
            && q.select.iter().any(|s| s.alias.as_ref() == Some(&id_col))
        {
            continue;
        }

        let needs_separate_pk = node.redaction_id_column != DEFAULT_PRIMARY_KEY;

        // Use edge-centric path if the query type is globally edge-centric,
        // or if this specific node has an edge column mapping (e.g. edge-only
        // aggregation targets).
        let node_is_edge_centric = globally_edge_centric || node_edge_col.contains_key(&node.id);

        if node_is_edge_centric {
            let (edge_alias, edge_col) = node_edge_col.get(&node.id).ok_or_else(|| {
                QueryError::Enforcement(format!(
                    "node '{}' has no edge mapping in node_edge_col",
                    node.id
                ))
            })?;
            let edge_id_expr = Expr::col(edge_alias, edge_col.as_str());

            if needs_separate_pk {
                // JOIN node table for the auth column (e.g. merge_request_id).
                let table = node.table.as_ref().ok_or_else(|| {
                    QueryError::Enforcement(format!(
                        "traversal node '{}' has non-default redaction_id_column '{}' but no resolved table",
                        node.id, node.redaction_id_column
                    ))
                })?;
                let join_cond = Expr::eq(
                    Expr::col(edge_alias, edge_col.as_str()),
                    Expr::col(&node.id, DEFAULT_PRIMARY_KEY),
                );
                q.from = TableRef::join(
                    JoinType::Inner,
                    std::mem::replace(&mut q.from, TableRef::scan("_placeholder", "_")),
                    TableRef::scan(table, &node.id),
                    join_cond,
                );

                let has_pk = q.select.iter().any(|s| s.alias.as_ref() == Some(&pk_col));
                if !has_pk {
                    q.select.push(SelectExpr {
                        expr: edge_id_expr.clone(),
                        alias: Some(pk_col),
                    });
                }

                let has_id = q.select.iter().any(|s| s.alias.as_ref() == Some(&id_col));
                if !has_id {
                    q.select.push(SelectExpr {
                        expr: Expr::col(&node.id, &node.redaction_id_column),
                        alias: Some(id_col.clone()),
                    });
                }
            } else {
                let has_id = q.select.iter().any(|s| s.alias.as_ref() == Some(&id_col));
                if !has_id {
                    q.select.push(SelectExpr {
                        expr: edge_id_expr,
                        alias: Some(id_col.clone()),
                    });
                }
            }

            let has_type = q.select.iter().any(|s| s.alias.as_ref() == Some(&type_col));
            if !has_type {
                let insert_pos = q
                    .select
                    .iter()
                    .position(|s| s.alias.as_ref() == Some(&id_col))
                    .map(|i| i + 1)
                    .unwrap_or(q.select.len());

                q.select.insert(
                    insert_pos,
                    SelectExpr {
                        expr: Expr::string(entity.as_str()),
                        alias: Some(type_col),
                    },
                );
            }
        } else {
            // Table-centric: search, aggregation — node tables are in FROM.
            if needs_separate_pk {
                let pk_expr = Expr::col(&node.id, DEFAULT_PRIMARY_KEY);
                let has_pk = q.select.iter().any(|s| s.alias.as_ref() == Some(&pk_col));
                if !has_pk {
                    q.select.push(SelectExpr {
                        expr: pk_expr.clone(),
                        alias: Some(pk_col),
                    });
                }
                // The pk lands in SELECT regardless of who put it there, so
                // guard GROUP BY membership separately: idempotent on re-entry
                // and robust if a lowerer pre-populates the pk column.
                ensure_in_group_by(q, input.query_type, pk_expr);
            }

            let has_id = q.select.iter().any(|s| s.alias.as_ref() == Some(&id_col));
            let has_type = q.select.iter().any(|s| s.alias.as_ref() == Some(&type_col));

            if !has_id {
                let id_expr = Expr::col(&node.id, &node.redaction_id_column);
                q.select.push(SelectExpr {
                    expr: id_expr.clone(),
                    alias: Some(id_col.clone()),
                });
                ensure_in_group_by(q, input.query_type, id_expr);
            }

            if !has_type {
                let insert_pos = q
                    .select
                    .iter()
                    .position(|s| s.alias.as_ref() == Some(&id_col))
                    .map(|i| i + 1)
                    .unwrap_or(q.select.len());

                q.select.insert(
                    insert_pos,
                    SelectExpr {
                        expr: Expr::string(entity.as_str()),
                        alias: Some(type_col),
                    },
                );
            }
        }
    }

    // Propagate added columns to UNION ALL arms so column counts match.
    if !q.union_all.is_empty() {
        let added: Vec<SelectExpr> = q.select[select_len_before..].to_vec();
        for arm in &mut q.union_all {
            for sel in &added {
                if !arm.select.iter().any(|s| s.alias == sel.alias) {
                    arm.select.push(sel.clone());
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(irrefutable_let_patterns)]
mod tests {
    use super::*;
    use crate::ast::{JoinType, TableRef};
    use crate::input::{CompilerMetadata, InputNode, QueryType};

    fn has_scan(t: &TableRef, tbl: &str) -> bool {
        match t {
            TableRef::Scan { table, .. } => table == tbl,
            TableRef::Join { left, right, .. } => has_scan(left, tbl) || has_scan(right, tbl),
            TableRef::Union { queries, .. } => queries.iter().any(|q| has_scan(&q.from, tbl)),
            TableRef::Subquery { query, .. } => has_scan(&query.from, tbl),
        }
    }

    /// Single-node traversal (search shape) for table-centric enforce tests.
    fn test_input() -> Input {
        Input {
            query_type: QueryType::Traversal,
            nodes: vec![InputNode {
                id: "u".to_string(),
                entity: Some("User".to_string()),
                table: Some("gl_user".to_string()),
                ..Default::default()
            }],
            ..Input::default()
        }
    }

    /// Two-node input for tests that need multiple selectable nodes.
    fn test_input_two_nodes() -> Input {
        Input {
            query_type: QueryType::Traversal,
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
            compiler: CompilerMetadata {
                node_edge_col: [
                    ("u".into(), ("e0".into(), "source_id".into())),
                    ("p".into(), ("e0".into(), "target_id".into())),
                ]
                .into(),
                ..Default::default()
            },
            ..Input::default()
        }
    }

    fn single_table_from() -> TableRef {
        TableRef::scan("gl_user", "u")
    }

    fn two_table_from() -> TableRef {
        TableRef::join(
            JoinType::Inner,
            TableRef::scan("gl_user", "u"),
            TableRef::scan("gl_project", "p"),
            Expr::lit(true),
        )
    }

    #[test]
    fn adds_type_columns_after_id_columns() {
        let query = Query {
            select: vec![
                SelectExpr {
                    expr: Expr::col("u", "id"),
                    alias: Some("_gkg_u_id".into()),
                },
                SelectExpr {
                    expr: Expr::col("p", "id"),
                    alias: Some("_gkg_p_id".into()),
                },
            ],
            from: two_table_from(),
            limit: Some(30),
            ..Default::default()
        };

        let input = test_input_two_nodes();
        let mut node = Node::Query(Box::new(query));

        enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        assert_eq!(q.select.len(), 4);
        assert_eq!(q.select[0].alias, Some("_gkg_u_id".into()));
        assert_eq!(q.select[1].alias, Some("_gkg_u_type".into()));
        assert_eq!(q.select[2].alias, Some("_gkg_p_id".into()));
        assert_eq!(q.select[3].alias, Some("_gkg_p_type".into()));

        if let Expr::Param { value, .. } = &q.select[1].expr {
            assert_eq!(value.as_str(), Some("User"));
        } else {
            panic!("expected param");
        }
        if let Expr::Param { value, .. } = &q.select[3].expr {
            assert_eq!(value.as_str(), Some("Project"));
        } else {
            panic!("expected param");
        }
    }

    #[test]
    fn skips_existing_type_columns() {
        let query = Query {
            select: vec![
                SelectExpr {
                    expr: Expr::col("u", "id"),
                    alias: Some("_gkg_u_id".into()),
                },
                SelectExpr {
                    expr: Expr::lit("User"),
                    alias: Some("_gkg_u_type".into()),
                },
                SelectExpr {
                    expr: Expr::col("p", "id"),
                    alias: Some("_gkg_p_id".into()),
                },
            ],
            from: two_table_from(),
            limit: Some(30),
            ..Default::default()
        };

        let input = test_input_two_nodes();
        let mut node = Node::Query(Box::new(query));

        enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        assert_eq!(q.select.len(), 4);
        assert_eq!(q.select[0].alias, Some("_gkg_u_id".into()));
        assert_eq!(q.select[1].alias, Some("_gkg_u_type".into()));
        assert_eq!(q.select[2].alias, Some("_gkg_p_id".into()));
        assert_eq!(q.select[3].alias, Some("_gkg_p_type".into()));
    }

    #[test]
    fn adds_id_and_type_columns_when_missing() {
        let query = Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "username"),
                alias: Some("name".into()),
            }],
            from: single_table_from(),
            limit: Some(30),
            ..Default::default()
        };

        let input = test_input();
        let mut node = Node::Query(Box::new(query));

        enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        assert_eq!(q.select.len(), 3);
        assert_eq!(q.select[0].alias, Some("name".into()));
        assert_eq!(q.select[1].alias, Some("_gkg_u_id".into()));
        assert_eq!(q.select[2].alias, Some("_gkg_u_type".into()));

        if let Expr::Column { table, column } = &q.select[1].expr {
            assert_eq!(table, "u");
            assert_eq!(column, "id");
        } else {
            panic!("expected column expression for _gkg_u_id");
        }
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

        let query = Query {
            select: vec![SelectExpr {
                expr: Expr::col("n", "id"),
                alias: Some("n_id".into()),
            }],
            from: TableRef::scan("kg_node", "n"),
            limit: Some(30),
            ..Default::default()
        };

        let mut node = Node::Query(Box::new(query));
        let ctx = enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        assert_eq!(q.select.len(), 1);
        assert!(ctx.is_empty());
    }

    #[test]
    fn builds_result_context() {
        let input = test_input_two_nodes();
        let query = Query {
            select: vec![],
            from: two_table_from(),
            limit: Some(30),
            ..Default::default()
        };

        let mut node = Node::Query(Box::new(query));
        let ctx = enforce_return(&mut node, &input).unwrap();

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

        let query = Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "id"),
                alias: Some("u_id".into()),
            }],
            from: TableRef::scan("kg_user", "u"),
            group_by: vec![Expr::col("u", "id")],
            limit: Some(10),
            ..Default::default()
        };

        let mut node = Node::Query(Box::new(query));
        let ctx = enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        // Should only have columns for 'u' (group_by node), not 'n' (target node)
        assert_eq!(q.select.len(), 3); // u_id, _gkg_u_id, _gkg_u_type
        assert!(
            q.select
                .iter()
                .any(|s| s.alias.as_ref() == Some(&"_gkg_u_id".to_string()))
        );
        assert!(
            q.select
                .iter()
                .any(|s| s.alias.as_ref() == Some(&"_gkg_u_type".to_string()))
        );
        assert!(
            !q.select
                .iter()
                .any(|s| s.alias.as_ref() == Some(&"_gkg_n_id".to_string()))
        );
        assert!(
            !q.select
                .iter()
                .any(|s| s.alias.as_ref() == Some(&"_gkg_n_type".to_string()))
        );
        assert_eq!(q.group_by.len(), 1); // u.id already present, no duplicate added

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

        let query = Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "username"),
                alias: Some("u_username".into()),
            }],
            from: TableRef::scan("gl_user", "u"),
            group_by: vec![Expr::col("u", "username")],
            limit: Some(10),
            ..Default::default()
        };

        let mut node = Node::Query(Box::new(query));
        enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        assert!(
            q.group_by.contains(&Expr::col("u", "id")),
            "redaction id column must be in GROUP BY: {:?}",
            q.group_by
        );
        assert_eq!(q.group_by.len(), 2); // username + id
    }

    #[test]
    fn aggregation_adds_separate_pk_to_group_by() {
        use crate::input::{AggFunction, InputAggregation};

        // When the group-by node has redaction_id_column != "id", enforce
        // emits a separate _gkg_*_pk column. DuckDB rejects SELECT columns
        // that aren't in GROUP BY, so the pk must be appended.
        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "f".to_string(),
                    entity: Some("File".to_string()),
                    table: Some("gl_file".to_string()),
                    redaction_id_column: "project_id".to_string(),
                    ..Default::default()
                },
                InputNode {
                    id: "d".to_string(),
                    entity: Some("Definition".to_string()),
                    table: Some("gl_definition".to_string()),
                    ..Default::default()
                },
            ],
            aggregations: vec![InputAggregation {
                function: AggFunction::Count,
                target: Some("d".to_string()),
                group_by: Some("f".to_string()),
                property: None,
                alias: Some("defs".to_string()),
            }],
            limit: 10,
            ..Input::default()
        };

        let query = Query {
            select: vec![SelectExpr {
                expr: Expr::col("f", "path"),
                alias: Some("f_path".into()),
            }],
            from: TableRef::scan("gl_file", "f"),
            group_by: vec![Expr::col("f", "path")],
            limit: Some(10),
            ..Default::default()
        };

        let mut node = Node::Query(Box::new(query));
        enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        let pk_expr = Expr::col("f", "id");
        let id_expr = Expr::col("f", "project_id");
        assert!(
            q.group_by.contains(&pk_expr),
            "separate pk column must be in GROUP BY: {:?}",
            q.group_by
        );
        assert!(
            q.group_by.contains(&id_expr),
            "redaction id column must be in GROUP BY: {:?}",
            q.group_by
        );
        assert_eq!(q.group_by.len(), 3); // path + id + project_id
    }

    #[test]
    fn uses_correct_redaction_id_column_per_node() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_definition", "d"),
                TableRef::scan("gl_project", "p"),
                Expr::lit(true),
            ),
            limit: Some(10),
            ..Default::default()
        }));

        let input = Input {
            query_type: QueryType::Traversal,
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
            compiler: CompilerMetadata {
                node_edge_col: [
                    ("d".into(), ("e0".into(), "source_id".into())),
                    ("p".into(), ("e0".into(), "target_id".into())),
                ]
                .into(),
                ..Default::default()
            },
            limit: 10,
            ..Input::default()
        };

        let ctx = enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        assert_eq!(q.select.len(), 5);

        // Definition (edge-centric, non-default redaction): pk = edge col,
        // auth id = joined d.project_id, type = literal
        assert_eq!(q.select[0].alias, Some("_gkg_d_pk".into()));
        assert!(
            matches!(&q.select[0].expr, Expr::Column { table, column } if table == "e0" && column == "source_id")
        );
        assert_eq!(q.select[1].alias, Some("_gkg_d_id".into()));
        assert!(matches!(&q.select[1].expr, Expr::Column { column, .. } if column == "project_id"));
        assert_eq!(q.select[2].alias, Some("_gkg_d_type".into()));
        assert!(matches!(&q.select[2].expr, Expr::Param { value, .. } if value == "Definition"));

        // Project (edge-centric, default redaction): id = edge col, type = literal
        assert_eq!(q.select[3].alias, Some("_gkg_p_id".into()));
        assert!(
            matches!(&q.select[3].expr, Expr::Column { table, column } if table == "e0" && column == "target_id")
        );
        assert_eq!(q.select[4].alias, Some("_gkg_p_type".into()));
        assert!(matches!(&q.select[4].expr, Expr::Param { value, .. } if value == "Project"));

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
        use crate::ast::Cte;
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
                forward_first_hop_rel_types: vec![],
                backward_first_hop_rel_types: vec![],
            }),
            ..Input::default()
        };

        // Path finding generates a Query with unrolled CTEs
        let mut query = Node::Query(Box::new(Query {
            ctes: vec![
                Cte::new(
                    "d0",
                    Query {
                        select: vec![SelectExpr {
                            expr: Expr::col("start", "id"),
                            alias: Some("node_id".into()),
                        }],
                        from: TableRef::scan("gl_project", "start"),
                        ..Default::default()
                    },
                ),
                Cte::new(
                    "d1",
                    Query {
                        from: TableRef::scan("d0", "p"),
                        ..Default::default()
                    },
                ),
            ],
            select: vec![SelectExpr {
                expr: Expr::col("all_paths", "path"),
                alias: Some("_gkg_path".into()),
            }],
            from: TableRef::scan("gl_project", "end"),
            limit: Some(30),
            ..Default::default()
        }));

        let ctx = enforce_return(&mut query, &input).unwrap();

        // Path finding queries use _gkg_path column for redaction data.
        // No additional _gkg_* columns are added by enforce_return.
        // The ResultContext is empty but has query_type set for path extraction.
        assert!(ctx.is_empty());
        assert_eq!(ctx.query_type, Some(QueryType::PathFinding));
    }

    #[test]
    fn default_entity_does_not_emit_pk_column() {
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![InputNode {
                id: "p".to_string(),
                entity: Some("Project".to_string()),
                table: Some("gl_project".to_string()),
                // redaction_id_column defaults to "id" — same as DEFAULT_PRIMARY_KEY
                ..Default::default()
            }],
            ..Input::default()
        };

        let query = Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "name"),
                alias: Some("p_name".into()),
            }],
            from: TableRef::scan("gl_project", "p"),
            ..Default::default()
        };

        let mut node = Node::Query(Box::new(query));
        enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"_gkg_p_id".to_string()));
        assert!(aliases.contains(&&"_gkg_p_type".to_string()));
        assert!(
            !aliases.contains(&&"_gkg_p_pk".to_string()),
            "default entity (redaction_id_column == id) should not emit _gkg_p_pk"
        );
        assert_eq!(q.select.len(), 3); // p_name + _gkg_p_id + _gkg_p_type
    }

    // ─── Traversal (edge-centric) tests ──────────────────────────────

    fn traversal_input_with_edge_col(
        nodes: Vec<InputNode>,
        node_edge_col: HashMap<String, (String, String)>,
    ) -> Input {
        use crate::input::CompilerMetadata;
        Input {
            query_type: QueryType::Traversal,
            nodes,
            compiler: CompilerMetadata {
                node_edge_col,
                ..Default::default()
            },
            ..Input::default()
        }
    }

    fn edge_from() -> TableRef {
        TableRef::scan("kg_edge", "e0")
    }

    #[test]
    fn traversal_emits_gkg_id_from_edge_column() {
        let node_edge_col: HashMap<String, (String, String)> = [
            ("u".into(), ("e0".into(), "source_id".into())),
            ("mr".into(), ("e0".into(), "target_id".into())),
        ]
        .into();

        let input = traversal_input_with_edge_col(
            vec![
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
            node_edge_col,
        );

        let query = Query {
            select: vec![],
            from: edge_from(),
            limit: Some(10),
            ..Default::default()
        };

        let mut node = Node::Query(Box::new(query));
        enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        // u: _gkg_u_id (from e0.source_id), _gkg_u_type
        let u_id = q
            .select
            .iter()
            .find(|s| s.alias.as_deref() == Some("_gkg_u_id"))
            .expect("missing _gkg_u_id");
        assert!(
            matches!(&u_id.expr, Expr::Column { table, column } if table == "e0" && column == "source_id")
        );

        // mr: _gkg_mr_id (from e0.target_id), _gkg_mr_type
        let mr_id = q
            .select
            .iter()
            .find(|s| s.alias.as_deref() == Some("_gkg_mr_id"))
            .expect("missing _gkg_mr_id");
        assert!(
            matches!(&mr_id.expr, Expr::Column { table, column } if table == "e0" && column == "target_id")
        );

        // No _pk columns for default entities
        assert!(
            !q.select
                .iter()
                .any(|s| s.alias.as_deref() == Some("_gkg_u_pk"))
        );
        assert!(
            !q.select
                .iter()
                .any(|s| s.alias.as_deref() == Some("_gkg_mr_pk"))
        );

        // Type columns present
        assert!(
            q.select
                .iter()
                .any(|s| s.alias.as_deref() == Some("_gkg_u_type"))
        );
        assert!(
            q.select
                .iter()
                .any(|s| s.alias.as_deref() == Some("_gkg_mr_type"))
        );
    }

    #[test]
    fn traversal_non_default_redaction_emits_pk_and_joins_node_table() {
        let node_edge_col: HashMap<String, (String, String)> = [
            ("mr".into(), ("e0".into(), "source_id".into())),
            ("d".into(), ("e0".into(), "target_id".into())),
        ]
        .into();

        let input = traversal_input_with_edge_col(
            vec![
                InputNode {
                    id: "mr".to_string(),
                    entity: Some("MergeRequest".to_string()),
                    table: Some("gl_merge_request".to_string()),
                    ..Default::default()
                },
                InputNode {
                    id: "d".to_string(),
                    entity: Some("MergeRequestDiff".to_string()),
                    table: Some("gl_mergerequestdiff".to_string()),
                    redaction_id_column: "merge_request_id".to_string(),
                    ..Default::default()
                },
            ],
            node_edge_col,
        );

        let query = Query {
            select: vec![],
            from: edge_from(),
            limit: Some(10),
            ..Default::default()
        };

        let mut node = Node::Query(Box::new(query));
        enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        // _gkg_d_pk from edge column (e0.target_id)
        let d_pk = q
            .select
            .iter()
            .find(|s| s.alias.as_deref() == Some("_gkg_d_pk"))
            .expect("missing _gkg_d_pk");
        assert!(
            matches!(&d_pk.expr, Expr::Column { table, column } if table == "e0" && column == "target_id")
        );

        // _gkg_d_id from joined node table (d.merge_request_id)
        let d_id = q
            .select
            .iter()
            .find(|s| s.alias.as_deref() == Some("_gkg_d_id"))
            .expect("missing _gkg_d_id");
        assert!(
            matches!(&d_id.expr, Expr::Column { table, column } if table == "d" && column == "merge_request_id")
        );

        assert!(
            has_scan(&q.from, "gl_mergerequestdiff"),
            "non-default redaction_id_column should JOIN the node table"
        );

        // mr (default) has no pk column
        assert!(
            !q.select
                .iter()
                .any(|s| s.alias.as_deref() == Some("_gkg_mr_pk"))
        );
    }

    #[test]
    fn traversal_non_default_redaction_on_source_side() {
        let node_edge_col: HashMap<String, (String, String)> = [
            ("d".into(), ("e0".into(), "source_id".into())),
            ("mr".into(), ("e0".into(), "target_id".into())),
        ]
        .into();

        let input = traversal_input_with_edge_col(
            vec![
                InputNode {
                    id: "d".to_string(),
                    entity: Some("MergeRequestDiff".to_string()),
                    table: Some("gl_mergerequestdiff".to_string()),
                    redaction_id_column: "merge_request_id".to_string(),
                    ..Default::default()
                },
                InputNode {
                    id: "mr".to_string(),
                    entity: Some("MergeRequest".to_string()),
                    table: Some("gl_merge_request".to_string()),
                    ..Default::default()
                },
            ],
            node_edge_col,
        );

        let query = Query {
            select: vec![],
            from: edge_from(),
            limit: Some(10),
            ..Default::default()
        };

        let mut node = Node::Query(Box::new(query));
        enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        // _gkg_d_pk should use source_id (d is on the from side)
        let d_pk = q
            .select
            .iter()
            .find(|s| s.alias.as_deref() == Some("_gkg_d_pk"))
            .expect("missing _gkg_d_pk");
        assert!(
            matches!(&d_pk.expr, Expr::Column { table, column } if table == "e0" && column == "source_id"),
            "_gkg_d_pk should be e0.source_id, got {:?}",
            d_pk.expr
        );
    }

    #[test]
    fn traversal_node_without_edge_mapping_returns_error() {
        // Multi-node traversal: node "x" is selectable but has no edge mapping.
        let input = traversal_input_with_edge_col(
            vec![
                InputNode {
                    id: "x".to_string(),
                    entity: Some("User".to_string()),
                    table: Some("gl_user".to_string()),
                    ..Default::default()
                },
                InputNode {
                    id: "y".to_string(),
                    entity: Some("Project".to_string()),
                    table: Some("gl_project".to_string()),
                    ..Default::default()
                },
            ],
            HashMap::new(),
        );

        let query = Query {
            select: vec![],
            from: edge_from(),
            limit: Some(10),
            ..Default::default()
        };

        let mut node = Node::Query(Box::new(query));
        let err = enforce_return(&mut node, &input).unwrap_err();
        assert!(
            err.to_string().contains("no edge mapping"),
            "expected edge mapping error, got: {err}"
        );
    }

    #[test]
    fn traversal_non_default_redaction_without_table_returns_error() {
        let node_edge_col: HashMap<String, (String, String)> =
            [("d".into(), ("e0".into(), "target_id".into()))].into();

        let input = traversal_input_with_edge_col(
            vec![InputNode {
                id: "d".to_string(),
                entity: Some("MergeRequestDiff".to_string()),
                table: None, // no resolved table
                redaction_id_column: "merge_request_id".to_string(),
                ..Default::default()
            }],
            node_edge_col,
        );

        let query = Query {
            select: vec![],
            from: edge_from(),
            limit: Some(10),
            ..Default::default()
        };

        let mut node = Node::Query(Box::new(query));
        let err = enforce_return(&mut node, &input).unwrap_err();
        assert!(
            err.to_string().contains("no resolved table"),
            "expected missing table error, got: {err}"
        );
    }
}

//! Enforce return columns for query results.
//!
//! Ensures all query results include ID and type columns for entities, enabling
//! the orbit-server to extract entity IDs and types for redaction validation.
//!
//! For aggregation queries, only nodes that appear in node group keys can have
//! their ID columns selected (aggregated nodes don't have individual IDs).

use crate::ast::{Expr, JoinType, Node, Query, SelectExpr, TableRef};
use crate::constants::{
    primary_key_column, redaction_id_column, redaction_type_column, traversal_path_column,
};
use crate::error::{QueryError, Result};
use crate::input::{EntityAuthConfig, Input, QueryType};
use crate::passes::lower::LoweredMetadata;
use crate::passes::shared::{deleted_false, filter_to_expr, id_list_predicate, id_range_predicate};
use ontology::constants::{DEFAULT_PRIMARY_KEY, TRAVERSAL_PATH_COLUMN};
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
    /// Internal path column, present only for multi-hop relationships.
    pub path_column: Option<String>,
    pub rel_types: Vec<String>,
    pub from_alias: String,
    pub to_alias: String,
    pub type_column: String,
    pub src_column: String,
    pub src_type_column: String,
    pub dst_column: String,
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

pub fn enforce_lowered_return(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &(impl crate::data_model::AuthorizationModel + ?Sized),
) -> Result<ResultContext> {
    let mut ctx = ResultContext::new().with_query_type(input.query_type);
    ctx.entity_auth = model.entity_auth();
    enforce_lowered_return_with(node, input, metadata, model, &mut ctx, |entity| {
        crate::data_model::AuthorizationModel::redaction_id_column(model, entity).to_string()
    })?;
    Ok(ctx)
}

pub fn enforce_local_return(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &(impl crate::data_model::QueryModel + ?Sized),
) -> Result<ResultContext> {
    let mut ctx = ResultContext::new().with_query_type(input.query_type);
    enforce_lowered_return_with(node, input, metadata, model, &mut ctx, |_| {
        DEFAULT_PRIMARY_KEY.to_string()
    })?;
    Ok(ctx)
}

fn enforce_lowered_return_with(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &(impl crate::data_model::QueryModel + ?Sized),
    ctx: &mut ResultContext,
    redaction_column: impl Fn(query_data_model::EntityId) -> String,
) -> Result<()> {
    let selectable_nodes: HashSet<&str> = match input.query_type {
        QueryType::Aggregation => {
            crate::input::node_group_ids(&input.aggregation.group_by).collect()
        }
        QueryType::Traversal | QueryType::Neighbors => {
            input.nodes.iter().map(|n| n.id.as_str()).collect()
        }
        QueryType::PathFinding | QueryType::Hydration => HashSet::new(),
    };

    match node {
        Node::Query(q) => enforce_return_columns(
            q,
            input,
            &selectable_nodes,
            ctx,
            &metadata.node_sources,
            model,
            redaction_column,
        )?,
        Node::Insert(_) => return Ok(()),
    }

    if matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) {
        use crate::constants::{
            EDGE_DST_SUFFIX, EDGE_DST_TYPE_SUFFIX, EDGE_SRC_SUFFIX, EDGE_SRC_TYPE_SUFFIX,
            EDGE_TYPE_SUFFIX,
        };

        for edge in &metadata.edges {
            let prefix = edge.column_prefix.clone();
            ctx.edges.push(EdgeMeta {
                type_column: format!("{prefix}{EDGE_TYPE_SUFFIX}"),
                src_column: format!("{prefix}{EDGE_SRC_SUFFIX}"),
                src_type_column: format!("{prefix}{EDGE_SRC_TYPE_SUFFIX}"),
                dst_column: format!("{prefix}{EDGE_DST_SUFFIX}"),
                dst_type_column: format!("{prefix}{EDGE_DST_TYPE_SUFFIX}"),
                column_prefix: prefix,
                path_column: edge.path_column.clone(),
                rel_types: edge.rel_types.clone(),
                from_alias: String::new(),
                to_alias: String::new(),
            });
        }
    }

    Ok(())
}

pub fn enforce_role_scans(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &(impl crate::data_model::AuthorizationModel + ?Sized),
) -> Result<()> {
    let Node::Query(query) = node else {
        return Ok(());
    };
    for input_node in &input.nodes {
        let Some(entity) = input_node.entity.as_deref() else {
            continue;
        };
        if alias_exists_in_from(&query.from, &input_node.id) {
            continue;
        }
        let elevated = model.graph().entity_id(entity).is_some_and(|entity| {
            crate::data_model::AuthorizationModel::redaction_id_column(model, entity)
                != DEFAULT_PRIMARY_KEY
        });
        if !elevated {
            continue;
        }
        let Some((source_alias, source_column)) = metadata.node_sources.get(&input_node.id) else {
            continue;
        };
        let table = model
            .entity_table(
                model
                    .graph()
                    .entity_id(entity)
                    .ok_or_else(|| QueryError::Enforcement(format!("unknown entity '{entity}'")))?,
            )
            .ok_or_else(|| {
                QueryError::Enforcement(format!("protected node '{}' has no table", input_node.id))
            })?;
        let role_alias = format!("_role_{}", input_node.id);
        let scan = TableRef::scan_final(table, &role_alias);
        let on = Expr::eq(
            Expr::col(source_alias, source_column),
            Expr::col(&role_alias, DEFAULT_PRIMARY_KEY),
        );
        query.from = TableRef::join(
            JoinType::Inner,
            std::mem::replace(&mut query.from, TableRef::scan("_placeholder", "_")),
            scan,
            on,
        );
        query.where_clause = Some(match query.where_clause.take() {
            Some(existing) => Expr::and(existing, deleted_false(&role_alias)),
            None => deleted_false(&role_alias),
        });
    }
    Ok(())
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
    node_edge_col: &HashMap<String, (String, String)>,
    model: &(impl crate::data_model::QueryModel + ?Sized),
    redaction_column: impl Fn(query_data_model::EntityId) -> String,
) -> Result<()> {
    let select_len_before = q.select.len();
    let globally_edge_centric = matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Neighbors
    ) && !input.is_search();

    for node in &input.nodes {
        let Some(entity) = &node.entity else { continue };
        let entity_id = model
            .graph()
            .entity_id(entity)
            .ok_or_else(|| QueryError::Enforcement(format!("unknown entity '{entity}'")))?;
        let redaction_column = redaction_column(entity_id);

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
        if input.query_type == QueryType::Neighbors && q.selects_alias(&id_col) {
            continue;
        }

        let needs_separate_pk = redaction_column != DEFAULT_PRIMARY_KEY;

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
            // For FK-elided nodes, the edge_alias (e.g. "mr") may not exist
            // in FROM because the node was absorbed into a filter. If the
            // node is pinned with a single ID, emit the literal value.
            let edge_id_expr =
                if !alias_exists_in_from(&q.from, edge_alias) && node.node_ids.len() == 1 {
                    Expr::lit(node.node_ids[0])
                } else {
                    Expr::col(edge_alias, edge_col.as_str())
                };

            if needs_separate_pk {
                // JOIN node table for the auth column (e.g. merge_request_id).
                // Skip if the alias already exists in FROM (the lowerer
                // hydrates nodes inline with dedup subqueries).
                let table = model.entity_table(entity_id).ok_or_else(|| {
                    QueryError::Enforcement(format!(
                        "traversal node '{}' has non-default redaction_id_column '{}' but no resolved table",
                        node.id, redaction_column
                    ))
                })?;
                if !alias_exists_in_from(&q.from, &node.id) {
                    let join_cond = Expr::eq(
                        Expr::col(edge_alias, edge_col.as_str()),
                        Expr::col(&node.id, DEFAULT_PRIMARY_KEY),
                    );
                    let node_scan = if node.filters.is_empty() {
                        TableRef::scan_final(table, &node.id)
                    } else {
                        let mut node_predicates = Vec::new();
                        for (prop, filters) in &node.filters {
                            let data_type = model
                                .graph()
                                .property_id(entity_id, prop)
                                .map(|property| model.graph().property(property).data_type);
                            for filter in filters {
                                node_predicates.push(filter_to_expr(
                                    &node.id,
                                    prop,
                                    &crate::passes::plan::BoundFilter {
                                        filter: filter.clone(),
                                        data_type,
                                        selectivity: ontology::FieldSelectivity::High,
                                    },
                                ));
                            }
                        }
                        if !node.node_ids.is_empty() {
                            node_predicates.push(id_list_predicate(
                                &node.id,
                                DEFAULT_PRIMARY_KEY,
                                &node.node_ids,
                            ));
                        }
                        if let Some(ref range) = node.id_range {
                            node_predicates.push(id_range_predicate(&node.id, range));
                        }
                        node_predicates.push(deleted_false(&node.id));
                        TableRef::subquery(
                            Query {
                                select: vec![SelectExpr::star()],
                                from: TableRef::scan_final(table, &node.id),
                                where_clause: Expr::conjoin(node_predicates),
                                ..Default::default()
                            },
                            &node.id,
                        )
                    };
                    q.from = TableRef::join(
                        JoinType::Inner,
                        std::mem::replace(&mut q.from, TableRef::scan("_placeholder", "_")),
                        node_scan,
                        join_cond,
                    );
                    if node.filters.is_empty() {
                        q.where_clause = Some(match q.where_clause.take() {
                            Some(existing) => Expr::and(existing, deleted_false(&node.id)),
                            None => deleted_false(&node.id),
                        });
                    }
                }

                let has_pk = q.selects_alias(&pk_col);
                if !has_pk {
                    q.select.push(SelectExpr {
                        expr: edge_id_expr.clone(),
                        alias: Some(pk_col),
                    });
                }
                ensure_in_group_by(q, input.query_type, edge_id_expr.clone());

                let has_id = q.selects_alias(&id_col);
                let id_expr = Expr::col(&node.id, &redaction_column);
                if !has_id {
                    q.select.push(SelectExpr {
                        expr: id_expr.clone(),
                        alias: Some(id_col.clone()),
                    });
                }
                ensure_in_group_by(q, input.query_type, id_expr);
            } else {
                let has_id = q.selects_alias(&id_col);
                if !has_id {
                    q.select.push(SelectExpr {
                        expr: edge_id_expr.clone(),
                        alias: Some(id_col.clone()),
                    });
                }
                ensure_in_group_by(q, input.query_type, edge_id_expr);
            }

            let has_type = q.selects_alias(&type_col);
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
                let has_pk = q.selects_alias(&pk_col);
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

            let has_id = q.selects_alias(&id_col);
            let has_type = q.selects_alias(&type_col);

            if !has_id {
                let id_expr = Expr::col(&node.id, &redaction_column);
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

        // Emit traversal_path column for hydration narrowing.
        // Only for nodes whose table carries traversal_path.
        // Skip aggregation queries: TP can't go in GROUP BY (splits counts)
        // and ClickHouse rejects non-GROUP-BY columns in SELECT.
        // Skip when the source alias doesn't exist in FROM (FK-elided nodes
        // where the node table was absorbed into an edge filter).
        if model.entity_has_traversal_path(entity_id) && input.query_type != QueryType::Aggregation
        {
            let tp_col = traversal_path_column(&node.id);
            let has_tp = q.selects_alias(&tp_col);
            if !has_tp {
                let tp_expr = if node_is_edge_centric {
                    if let Some((edge_alias, _)) = node_edge_col.get(&node.id) {
                        if alias_exists_in_from(&q.from, edge_alias) {
                            Some(Expr::col(edge_alias, TRAVERSAL_PATH_COLUMN))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else if alias_exists_in_from(&q.from, &node.id) {
                    Some(Expr::col(&node.id, TRAVERSAL_PATH_COLUMN))
                } else {
                    None
                };
                if let Some(tp_expr) = tp_expr {
                    q.select.push(SelectExpr {
                        expr: tp_expr,
                        alias: Some(tp_col),
                    });
                }
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

fn alias_exists_in_from(from: &TableRef, target: &str) -> bool {
    match from {
        TableRef::Scan { alias, .. }
        | TableRef::Subquery { alias, .. }
        | TableRef::Union { alias, .. } => alias == target,
        TableRef::Join { left, right, .. } => {
            alias_exists_in_from(left, target) || alias_exists_in_from(right, target)
        }
    }
}

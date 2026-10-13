use crate::ast::{Ast, Expr, JoinType, Select, SelectExpr, TableRef};
use crate::constants::{
    primary_key_column, redaction_id_column, redaction_type_column, traversal_path_column,
};
use crate::error::{QueryError, Result};
use crate::passes::lower::sql::{
    deleted_false, filter_to_expr, id_list_predicate, id_range_predicate,
};
use crate::passes::lower::{LoweredMetadata, NodeBinding};
use crate::query::{Query, QueryType};
use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::EntityAuthConfig;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionNode {
    pub alias: String,
    pub entity_type: String,
    pub pk_column: String,
    pub id_column: String,
    pub type_column: String,
}

#[derive(Debug, Clone)]
pub struct EdgeMeta {
    pub column_prefix: String,
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

pub fn enforce_lowered_return<S>(
    ast: &mut Ast<S>,
    query: &Query,
    metadata: &LoweredMetadata,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> Result<ResultContext> {
    let mut ctx = ResultContext::new().with_query_type(query.query_type());
    ctx.entity_auth.clone_from(model.entity_auth());
    enforce_lowered_return_with(ast, query, metadata, &mut ctx, |entity| {
        query
            .nodes
            .iter()
            .find(|node| node.entity == entity)
            .map_or_else(
                || DEFAULT_PRIMARY_KEY.to_string(),
                |node| node.redaction_id_column.clone(),
            )
    })?;
    Ok(ctx)
}

pub fn enforce_local_return<S>(
    ast: &mut Ast<S>,
    query: &Query,
    metadata: &LoweredMetadata,
    _model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> Result<ResultContext> {
    let mut ctx = ResultContext::new().with_query_type(query.query_type());
    enforce_lowered_return_with(ast, query, metadata, &mut ctx, |_| {
        DEFAULT_PRIMARY_KEY.to_string()
    })?;
    Ok(ctx)
}

fn enforce_lowered_return_with<S>(
    ast: &mut Ast<S>,
    query: &Query,
    metadata: &LoweredMetadata,
    ctx: &mut ResultContext,
    redaction_column: impl Fn(query_data_model::EntityId) -> String,
) -> Result<()> {
    let selectable_nodes: HashSet<&str> = match query.query_type() {
        QueryType::Aggregation => query
            .aggregation()
            .expect("aggregation query")
            .groups
            .iter()
            .filter(|&group| matches!(group, crate::query::Group::Node { .. }))
            .map(|group| query.node(group.node()).alias.as_str())
            .collect(),
        QueryType::Traversal | QueryType::Neighbors => {
            query.nodes.iter().map(|node| node.alias.as_str()).collect()
        }
        QueryType::PathFinding | QueryType::Hydration => HashSet::new(),
    };
    let Some(select) = ast.select_mut() else {
        return Ok(());
    };
    enforce_return_columns(
        select,
        query,
        &selectable_nodes,
        ctx,
        &metadata.nodes,
        redaction_column,
    )?;

    if matches!(
        query.query_type(),
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

pub fn enforce_role_scans<S>(
    ast: &mut Ast<S>,
    query: &Query,
    metadata: &LoweredMetadata,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> Result<()> {
    let Some(select) = ast.select_mut() else {
        return Ok(());
    };
    for input_node in &query.nodes {
        let entity = input_node.entity_name.as_str();
        let Some(binding) = metadata.nodes.get(&input_node.alias) else {
            continue;
        };
        if !model
            .entity_minimum_access_level(entity)
            .is_some_and(|level| level > crate::types::DEFAULT_PATH_ACCESS_LEVEL)
        {
            continue;
        }
        let Some(identity) = binding.role_identity()? else {
            continue;
        };
        let table = input_node.table.as_str();
        let role_alias = format!("_role_{}", input_node.alias);
        select.from = TableRef::join(
            JoinType::Inner,
            std::mem::replace(&mut select.from, TableRef::scan("_placeholder", "_")),
            TableRef::scan_final(table, &role_alias),
            Expr::eq(
                identity.clone(),
                Expr::col(&role_alias, DEFAULT_PRIMARY_KEY),
            ),
        );
        select.where_clause = Some(match select.where_clause.take() {
            Some(existing) => Expr::and(existing, deleted_false(&role_alias)),
            None => deleted_false(&role_alias),
        });
    }
    Ok(())
}

fn ensure_in_group_by(q: &mut Select, query_type: QueryType, expr: Expr) {
    if query_type == QueryType::Aggregation && !q.group_by.is_empty() && !q.group_by.contains(&expr)
    {
        q.group_by.push(expr);
    }
}

fn enforce_return_columns(
    q: &mut Select,
    query: &Query,
    selectable_nodes: &HashSet<&str>,
    ctx: &mut ResultContext,
    bindings: &HashMap<String, NodeBinding>,
    redaction_column: impl Fn(query_data_model::EntityId) -> String,
) -> Result<()> {
    let select_len_before = q.select.len();
    for node in &query.nodes {
        let entity = &node.entity_name;
        if !selectable_nodes.contains(node.alias.as_str()) {
            continue;
        }
        let entity_id = node.entity;
        ctx.add_node(&node.alias, entity);
        let id_col = redaction_id_column(&node.alias);
        let binding = bindings.get(&node.alias).ok_or_else(|| {
            QueryError::Enforcement(format!("node '{}' has no lowered binding", node.alias))
        })?;
        if matches!(binding, NodeBinding::Filtered) {
            return Err(QueryError::Enforcement(format!(
                "node '{}' has no result identity",
                node.alias
            )));
        }
        let NodeBinding::Values {
            identity,
            table_alias,
            traversal_path,
        } = binding
        else {
            continue;
        };
        let redaction_column = redaction_column(entity_id);
        let needs_separate_pk = redaction_column != DEFAULT_PRIMARY_KEY;
        let authorization_id = if needs_separate_pk {
            if table_alias.is_none() {
                let table = node.table.as_str();
                let scan = if node.filters.is_empty() {
                    TableRef::scan_final(table, &node.alias)
                } else {
                    let mut predicates: Vec<Expr> = node
                        .filters
                        .iter()
                        .filter(|filter| filter.stored)
                        .map(|filter| {
                            filter_to_expr(
                                &node.alias,
                                &filter.name,
                                &crate::passes::plan::PlannedFilter::from(filter),
                            )
                        })
                        .collect();
                    if !node.node_ids.is_empty() {
                        predicates.push(id_list_predicate(
                            &node.alias,
                            &node.id_property.name,
                            &node.node_ids,
                        ));
                    }
                    if let Some(range) = &node.id_range {
                        predicates.push(id_range_predicate(&node.alias, range));
                    }
                    predicates.push(deleted_false(&node.alias));
                    TableRef::subquery(
                        Select {
                            select: vec![SelectExpr::star()],
                            from: TableRef::scan_final(table, &node.alias),
                            where_clause: Expr::conjoin(predicates),
                            ..Default::default()
                        },
                        &node.alias,
                    )
                };
                q.from = TableRef::join(
                    JoinType::Inner,
                    std::mem::replace(&mut q.from, TableRef::scan("_placeholder", "_")),
                    scan,
                    Expr::eq(
                        identity.clone(),
                        Expr::col(&node.alias, DEFAULT_PRIMARY_KEY),
                    ),
                );
                if node.filters.is_empty() {
                    q.where_clause = Some(match q.where_clause.take() {
                        Some(existing) => Expr::and(existing, deleted_false(&node.alias)),
                        None => deleted_false(&node.alias),
                    });
                }
            }
            Expr::col(
                table_alias.as_deref().unwrap_or(&node.alias),
                redaction_column,
            )
        } else {
            identity.clone()
        };

        if needs_separate_pk {
            let name = primary_key_column(&node.alias);
            if !q.selects_alias(&name) {
                q.select.push(SelectExpr::new(identity.clone(), name));
            }
            ensure_in_group_by(q, query.query_type(), identity.clone());
        }
        if !q.selects_alias(&id_col) {
            q.select
                .push(SelectExpr::new(authorization_id.clone(), &id_col));
        }
        ensure_in_group_by(q, query.query_type(), authorization_id);
        let type_col = redaction_type_column(&node.alias);
        if !q.selects_alias(&type_col) {
            let position = q
                .select
                .iter()
                .position(|select| select.alias.as_ref() == Some(&id_col))
                .map_or(q.select.len(), |index| index + 1);
            q.select
                .insert(position, SelectExpr::new(Expr::string(entity), type_col));
        }
        if query.query_type() != QueryType::Aggregation
            && node.has_traversal_path
            && let Some(path) = traversal_path
        {
            let name = traversal_path_column(&node.alias);
            if !q.selects_alias(&name) {
                q.select.push(SelectExpr::new(path.clone(), name));
            }
        }
    }
    if !q.union_all.is_empty() {
        let added = q.select[select_len_before..].to_vec();
        for arm in &mut q.union_all {
            for select in &added {
                if !arm
                    .select
                    .iter()
                    .any(|existing| existing.alias == select.alias)
                {
                    arm.select.push(select.clone());
                }
            }
        }
    }
    Ok(())
}

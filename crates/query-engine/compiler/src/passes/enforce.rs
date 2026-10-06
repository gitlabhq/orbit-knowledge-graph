use crate::ast::{Expr, JoinType, Node, Query, TableRef};
use crate::config::BindingNames;
use crate::constants::{
    primary_key_column, redaction_id_column, redaction_type_column, traversal_path_column,
};
use crate::error::{QueryError, Result};
use crate::input::{Input, QueryType};
use crate::passes::lower::context::LoweringContext;
use crate::passes::lower::context::{select, stored_column};
use crate::passes::lower::sql::{
    deleted_false, filter_to_expr, id_list_predicate, id_range_predicate,
};
use crate::passes::lower::{LoweredMetadata, NodeBinding};
use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::EntityAuthConfig;
use query_data_model::{QueryBackendCatalog, bindings::QueryBindings};
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

pub fn enforce_lowered_return(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    arena: &mut QueryBindings,
    names: &mut BindingNames,
) -> Result<ResultContext> {
    let mut ctx = ResultContext::new().with_query_type(input.query_type);
    ctx.entity_auth.clone_from(model.entity_auth());
    enforce_lowered_return_with(
        node,
        input,
        metadata,
        &mut LoweringContext {
            model,
            bindings: arena,
            names,
        },
        &mut ctx,
        |entity| {
            model
                .redaction_id_column(entity)
                .unwrap_or(DEFAULT_PRIMARY_KEY)
                .to_string()
        },
    )?;
    Ok(ctx)
}

pub fn enforce_local_return(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    arena: &mut QueryBindings,
    names: &mut BindingNames,
) -> Result<ResultContext> {
    let mut ctx = ResultContext::new().with_query_type(input.query_type);
    enforce_lowered_return_with(
        node,
        input,
        metadata,
        &mut LoweringContext {
            model,
            bindings: arena,
            names,
        },
        &mut ctx,
        |_| DEFAULT_PRIMARY_KEY.to_string(),
    )?;
    Ok(ctx)
}

fn enforce_lowered_return_with(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    context: &mut LoweringContext<'_, impl query_data_model::QueryDataModel + ?Sized>,
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
            &metadata.nodes,
            context,
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
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    arena: &mut QueryBindings,
    names: &mut BindingNames,
) -> Result<()> {
    let Node::Query(query) = node else {
        return Ok(());
    };
    for input_node in &input.nodes {
        let Some(entity) = input_node.entity.as_deref() else {
            continue;
        };
        let Some(binding) = metadata.nodes.get(&input_node.id) else {
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
        let table = model.entity_table(entity).ok_or_else(|| {
            QueryError::Enforcement(format!("protected node '{}' has no table", input_node.id))
        })?;
        let role_alias = format!("_role_{}", input_node.id);
        let (scan, relation) = LoweringContext {
            model,
            bindings: arena,
            names,
        }
        .scan(query.scope, table, &role_alias, true)?;
        let left = std::mem::replace(&mut query.from, scan.clone());
        query.from = TableRef::join(
            JoinType::Inner,
            left,
            scan,
            Expr::eq(
                identity.clone(),
                Expr::Column(stored_column(
                    model,
                    arena,
                    query.scope,
                    relation,
                    DEFAULT_PRIMARY_KEY,
                )?),
            ),
        );
        if let Some(deleted) = deletion_filter(query.scope, relation, model, arena, names)? {
            query.where_clause = Expr::and_all([query.where_clause.take(), Some(deleted)]);
        }
    }
    Ok(())
}

fn ensure_in_group_by(q: &mut Query, query_type: QueryType, expr: Expr) {
    if query_type == QueryType::Aggregation && !q.group_by.is_empty() && !q.group_by.contains(&expr)
    {
        q.group_by.push(expr);
    }
}

fn enforce_return_columns(
    q: &mut Query,
    input: &Input,
    selectable_nodes: &HashSet<&str>,
    ctx: &mut ResultContext,
    bindings: &HashMap<String, NodeBinding>,
    context: &mut LoweringContext<'_, impl query_data_model::QueryDataModel + ?Sized>,
    redaction_column: impl Fn(query_data_model::EntityId) -> String,
) -> Result<()> {
    let model = context.model;
    let arena = &mut *context.bindings;
    let names = &mut *context.names;
    let select_len_before = q.select.len();
    for node in &input.nodes {
        let Some(entity) = &node.entity else {
            continue;
        };
        if !selectable_nodes.contains(node.id.as_str()) {
            continue;
        }
        let entity_id = model
            .entity(entity)
            .map(|entity| entity.id)
            .ok_or_else(|| QueryError::Enforcement(format!("unknown entity '{entity}'")))?;
        ctx.add_node(&node.id, entity);
        let id_col = redaction_id_column(&node.id);
        let binding = bindings.get(&node.id).ok_or_else(|| {
            QueryError::Enforcement(format!("node '{}' has no lowered binding", node.id))
        })?;
        if matches!(binding, NodeBinding::Filtered) {
            return Err(QueryError::Enforcement(format!(
                "node '{}' has no result identity",
                node.id
            )));
        }
        let NodeBinding::Values {
            identity,
            relation,
            traversal_path,
        } = binding
        else {
            continue;
        };
        let redaction_column = redaction_column(entity_id);
        let needs_separate_pk = redaction_column != DEFAULT_PRIMARY_KEY;
        let authorization_id = if needs_separate_pk {
            let relation = if let Some(relation) = relation {
                *relation
            } else {
                let table = model.entity_table(entity).ok_or_else(|| {
                    QueryError::Enforcement(format!(
                        "node '{}' has no authorization table",
                        node.id
                    ))
                })?;
                let (scan, relation) = if node.filters.is_empty() {
                    LoweringContext {
                        model,
                        bindings: arena,
                        names,
                    }
                    .scan(q.scope, table, &node.id, true)?
                } else {
                    let body = arena
                        .scope(q.scope)
                        .map_err(|error| QueryError::Enforcement(error.to_string()))?;
                    let (from, relation) = LoweringContext {
                        model,
                        bindings: arena,
                        names,
                    }
                    .scan(body, table, &node.id, true)?;
                    let mut predicates = Vec::new();
                    for (property, filters) in &node.filters {
                        let data_type = model
                            .property_for_entity_id(entity_id, property)
                            .map(|property| property.data_type);
                        for filter in filters {
                            predicates.push(filter_to_expr(
                                stored_column(model, arena, body, relation, property)?,
                                filter
                                    .rhs_column
                                    .as_ref()
                                    .map(|(_, name)| {
                                        stored_column(model, arena, body, relation, name)
                                    })
                                    .transpose()?,
                                &crate::passes::plan::BoundFilter {
                                    filter: filter.clone(),
                                    property: None,
                                    data_type,
                                    selectivity: ontology::FieldSelectivity::High,
                                },
                            ));
                        }
                    }
                    if !node.node_ids.is_empty() {
                        predicates.push(id_list_predicate(
                            stored_column(model, arena, body, relation, DEFAULT_PRIMARY_KEY)?,
                            &node.node_ids,
                        ));
                    }
                    if let Some(range) = &node.id_range {
                        predicates.push(id_range_predicate(
                            stored_column(model, arena, body, relation, DEFAULT_PRIMARY_KEY)?,
                            range,
                        ));
                    }
                    predicates.extend(deletion_filter(body, relation, model, arena, names)?);
                    let mut query = Query::new(body, from);
                    for export in arena
                        .exports(relation)
                        .map_err(|error| QueryError::Enforcement(error.to_string()))?
                        .to_vec()
                    {
                        let column = arena
                            .column(body, relation, export)
                            .map_err(|error| QueryError::Enforcement(error.to_string()))?;
                        query.select.push(select(
                            arena,
                            names,
                            body,
                            Expr::Column(column),
                            names.exports[&export].clone(),
                        )?);
                    }
                    query.where_clause = Expr::conjoin(predicates);
                    LoweringContext {
                        model,
                        bindings: arena,
                        names,
                    }
                    .derived(q.scope, query, &node.id)?
                };
                let left = std::mem::replace(&mut q.from, scan.clone());
                q.from = TableRef::join(
                    JoinType::Inner,
                    left,
                    scan,
                    Expr::eq(
                        identity.clone(),
                        Expr::Column(stored_column(
                            model,
                            arena,
                            q.scope,
                            relation,
                            DEFAULT_PRIMARY_KEY,
                        )?),
                    ),
                );
                if node.filters.is_empty() {
                    q.where_clause = Expr::and_all([
                        q.where_clause.take(),
                        deletion_filter(q.scope, relation, model, arena, names)?,
                    ]);
                }
                relation
            };
            Expr::Column(stored_column(
                model,
                arena,
                q.scope,
                relation,
                &redaction_column,
            )?)
        } else {
            identity.clone()
        };

        if needs_separate_pk {
            let name = primary_key_column(&node.id);
            if !names.selects(q, &name) {
                q.select
                    .push(select(arena, names, q.scope, identity.clone(), name)?);
            }
            ensure_in_group_by(q, input.query_type, identity.clone());
        }
        if !names.selects(q, &id_col) {
            q.select.push(select(
                arena,
                names,
                q.scope,
                authorization_id.clone(),
                &id_col,
            )?);
        }
        ensure_in_group_by(q, input.query_type, authorization_id);
        let type_col = redaction_type_column(&node.id);
        if !names.selects(q, &type_col) {
            let position = q
                .select
                .iter()
                .position(|select| {
                    select
                        .alias
                        .as_ref()
                        .is_some_and(|export| names.exports[export] == id_col)
                })
                .map_or(q.select.len(), |index| index + 1);
            q.select.insert(
                position,
                select(arena, names, q.scope, Expr::string(entity), type_col)?,
            );
        }
        if input.query_type != QueryType::Aggregation
            && model.entity_has_traversal_path(entity)
            && let Some(path) = traversal_path
        {
            let name = traversal_path_column(&node.id);
            if !names.selects(q, &name) {
                q.select
                    .push(select(arena, names, q.scope, path.clone(), name)?);
            }
        }
    }
    if !q.union_all.is_empty() {
        let added = q.select[select_len_before..].to_vec();
        for arm in &mut q.union_all {
            for select in &added {
                if !arm.select.iter().any(|existing| {
                    existing.alias.as_ref().map(|export| &names.exports[export])
                        == select.alias.as_ref().map(|export| &names.exports[export])
                }) {
                    arm.select.push(select.clone());
                }
            }
        }
    }
    Ok(())
}

fn deletion_filter(
    scope: query_data_model::bindings::ScopeId,
    relation: query_data_model::bindings::RelationId,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    bindings: &QueryBindings,
    names: &BindingNames,
) -> Result<Option<Expr>> {
    let table = names.source(bindings, relation)?;
    let storage = model.query_backend().storage();
    let table = storage
        .resolve_table(table)
        .map_err(|error| QueryError::Enforcement(error.to_string()))?;
    let query_data_model::storage::RowSemantics::Versioned {
        deletion: Some(deletion),
        ..
    } = storage.table(table).row_semantics()
    else {
        return Ok(None);
    };
    bindings
        .stored_column(
            scope,
            relation,
            query_data_model::storage::StoredColumnRef {
                table,
                column: deletion.column,
            },
        )
        .map(|column| Some(deleted_false(column)))
        .map_err(|error| QueryError::Enforcement(error.to_string()))
}

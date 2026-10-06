use crate::ast::{Expr, JoinType, Node, Query, SelectExpr, TableRef};
use crate::constants::{
    primary_key_column, redaction_id_column, redaction_type_column, traversal_path_column,
};
use crate::error::{QueryError, Result};
use crate::input::{Input, QueryType};
use crate::passes::lower::sql::{
    deleted_false, filter_to_expr, id_list_predicate, id_range_predicate,
};
use crate::passes::lower::{LoweredMetadata, NodeBinding};
use crate::passes::plan::helpers::{FilterOwner, ordered_filters};
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

pub fn enforce_lowered_return(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
) -> Result<ResultContext> {
    let mut ctx = ResultContext::new().with_query_type(input.query_type);
    ctx.entity_auth.clone_from(model.entity_auth());
    enforce_lowered_return_with(node, input, metadata, model, &mut ctx, |entity| {
        model
            .redaction_id_column(entity)
            .unwrap_or(DEFAULT_PRIMARY_KEY)
            .to_string()
    })?;
    Ok(ctx)
}

pub fn enforce_local_return(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
) -> Result<ResultContext> {
    let mut ctx = ResultContext::new().with_query_type(input.query_type);
    enforce_lowered_return_with(node, input, metadata, model, &mut ctx, |_| {
        DEFAULT_PRIMARY_KEY.to_string()
    })?;
    Ok(ctx)
}

pub fn enforce_graph_return<'a, M: query_data_model::QueryDataModel + ?Sized>(
    graph: &mut crate::query_graph::QueryGraph<
        'a,
        M,
        crate::query_graph::Expression<'a>,
        crate::query_graph::LoweredOperation<'a>,
    >,
    root: crate::query_graph::BlockId,
    input: &Input,
) -> Result<ResultContext> {
    use crate::query_graph::Expression;
    let model = graph.catalog();
    let mut context = ResultContext::new().with_query_type(input.query_type);
    context.entity_auth.clone_from(model.entity_auth());
    for (index, node) in input.nodes.iter().enumerate() {
        let relation = graph.input_node(root, index)?;
        let entity_name = node
            .entity
            .as_deref()
            .ok_or_else(|| QueryError::Enforcement("node has no entity".into()))?;
        let entity = model
            .entity(entity_name)
            .ok_or_else(|| QueryError::Enforcement(format!("unknown entity '{entity_name}'")))?;
        let redaction_column = model
            .redaction_id_column(entity.id)
            .unwrap_or(DEFAULT_PRIMARY_KEY);
        let mut columns = vec![(redaction_id_column(&node.id), redaction_column)];
        if redaction_column != DEFAULT_PRIMARY_KEY {
            columns.push((primary_key_column(&node.id), DEFAULT_PRIMARY_KEY));
        }
        if model.entity_has_traversal_path(entity_name) {
            columns.push((
                traversal_path_column(&node.id),
                ontology::TRAVERSAL_PATH_COLUMN,
            ));
        }
        for (label, column) in columns {
            graph.project(
                root,
                label,
                Expression::Column(graph.stored_column(relation, column)?),
            )?;
        }
        graph.project(
            root,
            redaction_type_column(&node.id),
            Expression::Text(entity_name.into()),
        )?;
        context.add_node(&node.id, entity_name);
    }
    for (index, relationship) in input.relationships.iter().enumerate() {
        let [kind] = relationship.types.as_slice() else {
            return Err(QueryError::Enforcement(
                "graph edge identity requires one relationship kind".into(),
            ));
        };
        let (source, target) = if relationship.direction == crate::input::Direction::Incoming {
            (&relationship.to, &relationship.from)
        } else {
            (&relationship.from, &relationship.to)
        };
        let entity = |alias: &str| {
            input
                .nodes
                .iter()
                .find(|node| node.id == alias)
                .and_then(|node| node.entity.as_deref())
                .ok_or_else(|| QueryError::Enforcement("edge endpoint has no entity".into()))
        };
        let prefix = format!("e{index}_");
        for (suffix, value) in [
            ("type", kind.as_str()),
            ("src_type", entity(source)?),
            ("dst_type", entity(target)?),
        ] {
            graph.project(
                root,
                format!("{prefix}{suffix}"),
                Expression::Text(value.into()),
            )?;
        }
        context.add_edge(EdgeMeta {
            type_column: format!("{prefix}type"),
            src_column: format!("{prefix}src"),
            src_type_column: format!("{prefix}src_type"),
            dst_column: format!("{prefix}dst"),
            dst_type_column: format!("{prefix}dst_type"),
            column_prefix: prefix,
            path_column: None,
            rel_types: relationship.types.clone(),
            from_alias: relationship.from.clone(),
            to_alias: relationship.to.clone(),
        });
    }
    Ok(context)
}

fn enforce_lowered_return_with(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
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
    model: &(impl query_data_model::QueryDataModel + ?Sized),
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
        query.from = TableRef::join(
            JoinType::Inner,
            std::mem::replace(&mut query.from, TableRef::scan("_placeholder", "_")),
            TableRef::scan_final(table, &role_alias),
            Expr::eq(
                identity.clone(),
                Expr::col(&role_alias, DEFAULT_PRIMARY_KEY),
            ),
        );
        query.where_clause = Some(match query.where_clause.take() {
            Some(existing) => Expr::and(existing, deleted_false(&role_alias)),
            None => deleted_false(&role_alias),
        });
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
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    redaction_column: impl Fn(query_data_model::EntityId) -> String,
) -> Result<()> {
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
                let table = model.entity_table(entity).ok_or_else(|| {
                    QueryError::Enforcement(format!(
                        "node '{}' has no authorization table",
                        node.id
                    ))
                })?;
                let scan = if node.filters.is_empty() {
                    TableRef::scan_final(table, &node.id)
                } else {
                    let mut predicates: Vec<Expr> =
                        ordered_filters(&node.filters, FilterOwner::Entity(entity_id), model)
                            .iter()
                            .map(|(property, bound)| filter_to_expr(&node.id, property, bound))
                            .collect();
                    if !node.node_ids.is_empty() {
                        predicates.push(id_list_predicate(
                            &node.id,
                            DEFAULT_PRIMARY_KEY,
                            &node.node_ids,
                        ));
                    }
                    if let Some(range) = &node.id_range {
                        predicates.push(id_range_predicate(&node.id, range));
                    }
                    predicates.push(deleted_false(&node.id));
                    TableRef::subquery(
                        Query {
                            select: vec![SelectExpr::star()],
                            from: TableRef::scan_final(table, &node.id),
                            where_clause: Expr::conjoin(predicates),
                            ..Default::default()
                        },
                        &node.id,
                    )
                };
                q.from = TableRef::join(
                    JoinType::Inner,
                    std::mem::replace(&mut q.from, TableRef::scan("_placeholder", "_")),
                    scan,
                    Expr::eq(identity.clone(), Expr::col(&node.id, DEFAULT_PRIMARY_KEY)),
                );
                if node.filters.is_empty() {
                    q.where_clause = Some(match q.where_clause.take() {
                        Some(existing) => Expr::and(existing, deleted_false(&node.id)),
                        None => deleted_false(&node.id),
                    });
                }
            }
            Expr::col(table_alias.as_deref().unwrap_or(&node.id), redaction_column)
        } else {
            identity.clone()
        };

        if needs_separate_pk {
            let name = primary_key_column(&node.id);
            if !q.selects_alias(&name) {
                q.select.push(SelectExpr::new(identity.clone(), name));
            }
            ensure_in_group_by(q, input.query_type, identity.clone());
        }
        if !q.selects_alias(&id_col) {
            q.select
                .push(SelectExpr::new(authorization_id.clone(), &id_col));
        }
        ensure_in_group_by(q, input.query_type, authorization_id);
        let type_col = redaction_type_column(&node.id);
        if !q.selects_alias(&type_col) {
            let position = q
                .select
                .iter()
                .position(|select| select.alias.as_ref() == Some(&id_col))
                .map_or(q.select.len(), |index| index + 1);
            q.select
                .insert(position, SelectExpr::new(Expr::string(entity), type_col));
        }
        if input.query_type != QueryType::Aggregation
            && model.entity_has_traversal_path(entity)
            && let Some(path) = traversal_path
        {
            let name = traversal_path_column(&node.id);
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

use crate::ast::{Expr, Node, Query, SelectExpr};
use crate::constants::{
    primary_key_column, redaction_id_column, redaction_type_column, traversal_path_column,
};
use crate::error::{QueryError, Result};
use crate::input::{Input, QueryType};
use crate::passes::lower::{LoweredMetadata, NodeBinding};
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

pub fn enforce_local_return(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
) -> Result<ResultContext> {
    let mut ctx = ResultContext::new().with_query_type(input.query_type);
    enforce_local_return_with(node, input, metadata, model, &mut ctx)?;
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
    if matches!(
        input.query_type,
        QueryType::Hydration | QueryType::PathFinding
    ) {
        return Ok(context);
    }
    if input.query_type == QueryType::Neighbors {
        for node in &input.nodes {
            if let Some(entity) = &node.entity {
                context.add_node(&node.id, entity);
            }
        }
        return Ok(context);
    }
    let grouped_nodes: HashSet<_> =
        crate::input::node_group_ids(&input.aggregation.group_by).collect();
    for (index, node) in input.nodes.iter().enumerate() {
        if input.query_type == QueryType::Aggregation && !grouped_nodes.contains(node.id.as_str()) {
            continue;
        }
        let mut relation = graph.input_node(root, index).ok();
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
        let identity = graph.input_identity(root, input, index)?;
        if relation.is_none() && redaction_column != DEFAULT_PRIMARY_KEY {
            use crate::query_graph::{LoweredOperation, ScanInput};
            let table = model
                .entity_table(entity_name)
                .ok_or_else(|| QueryError::Enforcement("missing authorization table".into()))?;
            let scan = graph.scan(root, table, &node.id)?;
            graph.bind_scan(scan, ScanInput::Node(index))?;
            let key = graph.stored_column(scan, DEFAULT_PRIMARY_KEY)?;
            let deleted = graph.stored_column(scan, ontology::DELETED_COLUMN)?;
            let right = LoweredOperation::current(scan).filter(Expression::equal(
                Expression::Column(deleted),
                Expression::Boolean(false),
            ));
            let operation = graph.operation_mut(root)?;
            let source = match operation {
                LoweredOperation::Limit { input, .. } => input.as_mut(),
                source => source,
            };
            *source = std::mem::replace(source, LoweredOperation::One).join(
                right,
                Expression::equal(Expression::Column(identity), Expression::Column(key)),
            );
            relation = Some(scan);
        }
        let authorization_id = if redaction_column == DEFAULT_PRIMARY_KEY {
            identity
        } else {
            graph.stored_column(
                relation
                    .ok_or_else(|| QueryError::Enforcement("missing authorization scan".into()))?,
                redaction_column,
            )?
        };
        let mut columns = vec![(redaction_id_column(&node.id), authorization_id)];
        if redaction_column != DEFAULT_PRIMARY_KEY {
            columns.push((primary_key_column(&node.id), identity));
        }
        if model.entity_has_traversal_path(entity_name)
            && let Some(relation) = relation
        {
            columns.push((
                traversal_path_column(&node.id),
                graph.stored_column(relation, ontology::TRAVERSAL_PATH_COLUMN)?,
            ));
        }
        for (label, column) in columns {
            if input.query_type == QueryType::Aggregation {
                let mut operation = graph.operation_mut(root)?;
                loop {
                    match operation {
                        crate::query_graph::Relational::Aggregate { groups, .. } => {
                            let value = Expression::Column(column);
                            if !groups.contains(&value) {
                                groups.push(value);
                            }
                            break;
                        }
                        crate::query_graph::Relational::Limit { input, .. } => operation = input,
                        _ => {
                            return Err(QueryError::Enforcement(
                                "grouped result has no aggregate".into(),
                            ));
                        }
                    }
                }
            }
            graph.project(root, label, Expression::Column(column))?;
        }
        graph.project(
            root,
            redaction_type_column(&node.id),
            Expression::Text(entity_name.into()),
        )?;
        context.add_node(&node.id, entity_name);
    }
    for (index, relationship) in input.relationships.iter().enumerate() {
        if input.query_type == QueryType::Aggregation {
            continue;
        }
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
        let prefix = if relationship.hops.max > 1 {
            format!("hop_e{index}_")
        } else {
            format!("e{index}_")
        };
        for (suffix, value) in [
            (
                "type",
                relationship
                    .types
                    .as_slice()
                    .first()
                    .map(String::as_str)
                    .unwrap_or("*"),
            ),
            ("src_type", entity(source)?),
            ("dst_type", entity(target)?),
        ] {
            let label = format!("{prefix}{suffix}");
            if graph.outputs(root)?.any(|output| {
                graph
                    .output_label(output)
                    .is_ok_and(|existing| existing == label)
            }) {
                continue;
            }
            graph.project(root, label, Expression::Text(value.into()))?;
        }
        context.add_edge(EdgeMeta {
            type_column: format!("{prefix}type"),
            src_column: format!("{prefix}src"),
            src_type_column: format!("{prefix}src_type"),
            dst_column: format!("{prefix}dst"),
            dst_type_column: format!("{prefix}dst_type"),
            path_column: (relationship.hops.max > 1).then(|| format!("{prefix}path_nodes")),
            column_prefix: prefix,
            rel_types: relationship.types.as_slice().to_vec(),
            from_alias: relationship.from.clone(),
            to_alias: relationship.to.clone(),
        });
    }
    Ok(context)
}

fn enforce_local_return_with(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    ctx: &mut ResultContext,
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
        Node::Query(q) => {
            enforce_return_columns(q, input, &selectable_nodes, ctx, &metadata.nodes, model)?
        }
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
) -> Result<()> {
    let select_len_before = q.select.len();
    for node in &input.nodes {
        let Some(entity) = &node.entity else {
            continue;
        };
        if !selectable_nodes.contains(node.id.as_str()) {
            continue;
        }
        model
            .entity(entity)
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
            traversal_path,
            ..
        } = binding
        else {
            continue;
        };
        if !q.selects_alias(&id_col) {
            q.select.push(SelectExpr::new(identity.clone(), &id_col));
        }
        ensure_in_group_by(q, input.query_type, identity.clone());
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

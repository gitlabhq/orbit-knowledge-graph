use crate::ast::{Expr, Node, Query, SelectExpr};
use crate::constants::{
    primary_key_column, redaction_id_column, redaction_type_column, traversal_path_column,
};
use crate::error::{QueryError, Result};
use crate::input::{Input, QueryType};
use crate::passes::lower::{LoweredMetadata, NodeBinding};
use crate::query_graph::{LoweredGraph, QueryId, lit};
use query_data_model::{EntityAuthConfig, QueryDataModel};
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
            alias.into(),
            RedactionNode {
                alias: alias.into(),
                entity_type: entity_type.into(),
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
        self.entity_auth
            .iter()
            .map(|(name, config)| (name.as_str(), config))
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

pub fn enforce_graph_return<'a, M: QueryDataModel + ?Sized>(
    graph: LoweredGraph<'a, M>,
    root: QueryId,
    input: &Input,
) -> Result<(LoweredGraph<'a, M>, ResultContext)> {
    let model = graph.graph().catalog();
    let mut context = ResultContext::new().with_query_type(input.query_type);
    context.entity_auth.clone_from(model.entity_auth());
    if matches!(
        input.query_type,
        QueryType::Hydration | QueryType::PathFinding
    ) {
        return Ok((graph, context));
    }
    if input.query_type == QueryType::Neighbors {
        for node in &input.nodes {
            if let Some(entity) = &node.entity {
                context.add_node(&node.id, entity);
            }
        }
        return Ok((graph, context));
    }
    let grouped = crate::input::node_group_ids(&input.aggregation.group_by).collect::<HashSet<_>>();
    let graph = graph.map_result(root, |q, rows| {
        let (rows, mut outputs, measures) = if input.query_type == QueryType::Aggregation {
            let (rows, groups, measures) = rows.remove_limit()?.into_aggregate()?;
            (rows, groups, Some(measures))
        } else {
            let (rows, outputs) = rows.into_select()?;
            (rows, outputs, None)
        };
        for node in &input.nodes {
            if input.query_type == QueryType::Aggregation && !grouped.contains(node.id.as_str()) {
                continue;
            }
            let name = node
                .entity
                .as_deref()
                .ok_or_else(|| QueryError::Enforcement("node has no entity".into()))?;
            let entity = model
                .entity(name)
                .ok_or_else(|| QueryError::Enforcement(format!("unknown entity '{name}'")))?;
            let redaction = model.redaction_id_column(entity.id).unwrap_or("id");
            let identity = rows.column_from(&node.id, "id")?;
            let authorization = rows.column_from(&node.id, redaction)?;
            let mut columns = vec![(redaction_id_column(&node.id), authorization)];
            if redaction != "id" {
                columns.push((primary_key_column(&node.id), identity));
            }
            if model.entity_has_traversal_path(name) {
                columns.push((
                    traversal_path_column(&node.id),
                    rows.column_from(&node.id, ontology::TRAVERSAL_PATH_COLUMN)?,
                ));
            }
            for (label, column) in columns {
                if !outputs.iter().any(|output| output.name == label) {
                    outputs.push(column.named(label));
                }
            }
            outputs.push(lit(name).named(redaction_type_column(&node.id)));
            context.add_node(&node.id, name);
        }
        if input.query_type != QueryType::Aggregation {
            for (index, relationship) in input.relationships.iter().enumerate() {
                let (source, target) =
                    if relationship.direction == crate::input::Direction::Incoming {
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
                        .ok_or(crate::query_graph::Error::Column)
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
                    if !outputs.iter().any(|output| output.name == label) {
                        outputs.push(lit(value).named(label));
                    }
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
        }
        if let Some(measures) = measures {
            let rows = q.aggregate(rows, outputs, measures)?;
            Ok::<_, QueryError>(q.limit(rows, input.limit)?)
        } else {
            Ok(q.select(rows, outputs)?)
        }
    })?;
    Ok((graph, context))
}

pub fn enforce_local_return(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &(impl QueryDataModel + ?Sized),
) -> Result<ResultContext> {
    let mut context = ResultContext::new().with_query_type(input.query_type);
    let selectable = match input.query_type {
        QueryType::Aggregation => {
            crate::input::node_group_ids(&input.aggregation.group_by).collect::<HashSet<_>>()
        }
        QueryType::Traversal | QueryType::Neighbors => {
            input.nodes.iter().map(|node| node.id.as_str()).collect()
        }
        QueryType::PathFinding | QueryType::Hydration => HashSet::new(),
    };
    let Node::Query(query) = node else {
        return Ok(context);
    };
    enforce_local_columns(
        query,
        input,
        &selectable,
        &mut context,
        &metadata.nodes,
        model,
    )?;
    if matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) {
        for edge in &metadata.edges {
            let prefix = edge.column_prefix.clone();
            context.add_edge(EdgeMeta {
                type_column: format!("{prefix}type"),
                src_column: format!("{prefix}src"),
                src_type_column: format!("{prefix}src_type"),
                dst_column: format!("{prefix}dst"),
                dst_type_column: format!("{prefix}dst_type"),
                column_prefix: prefix,
                path_column: edge.path_column.clone(),
                rel_types: edge.rel_types.clone(),
                from_alias: String::new(),
                to_alias: String::new(),
            });
        }
    }
    Ok(context)
}

fn enforce_local_columns(
    query: &mut Query,
    input: &Input,
    selectable: &HashSet<&str>,
    context: &mut ResultContext,
    bindings: &HashMap<String, NodeBinding>,
    model: &(impl QueryDataModel + ?Sized),
) -> Result<()> {
    let original_width = query.select.len();
    for node in &input.nodes {
        let Some(entity) = &node.entity else { continue };
        if !selectable.contains(node.id.as_str()) {
            continue;
        }
        model
            .entity(entity)
            .ok_or_else(|| QueryError::Enforcement(format!("unknown entity '{entity}'")))?;
        context.add_node(&node.id, entity);
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
        let id = redaction_id_column(&node.id);
        if !query.selects_alias(&id) {
            query.select.push(SelectExpr::new(identity.clone(), &id));
        }
        if input.query_type == QueryType::Aggregation
            && !query.group_by.is_empty()
            && !query.group_by.contains(identity)
        {
            query.group_by.push(identity.clone());
        }
        let entity_type = redaction_type_column(&node.id);
        if !query.selects_alias(&entity_type) {
            let position = query
                .select
                .iter()
                .position(|select| select.alias.as_ref() == Some(&id))
                .map_or(query.select.len(), |index| index + 1);
            query
                .select
                .insert(position, SelectExpr::new(Expr::string(entity), entity_type));
        }
        if input.query_type != QueryType::Aggregation
            && model.entity_has_traversal_path(entity)
            && let Some(path) = traversal_path
        {
            let label = traversal_path_column(&node.id);
            if !query.selects_alias(&label) {
                query.select.push(SelectExpr::new(path.clone(), label));
            }
        }
    }
    let added = &query.select[original_width..];
    for arm in &mut query.union_all {
        for select in added {
            if !arm
                .select
                .iter()
                .any(|existing| existing.alias == select.alias)
            {
                arm.select.push(select.clone());
            }
        }
    }
    Ok(())
}

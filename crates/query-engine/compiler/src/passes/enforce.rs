use std::collections::HashMap;

use query_data_model::{EntityAuthConfig, QueryDataModel};

use crate::ast::{Expr, Identifier, Node, OrderExpr, SelectExpr};
use crate::constants::{
    EDGE_DST_SUFFIX, EDGE_DST_TYPE_SUFFIX, EDGE_SRC_SUFFIX, EDGE_SRC_TYPE_SUFFIX, EDGE_TYPE_SUFFIX,
    internal_column_prefix, primary_key_column, redaction_id_column, redaction_type_column,
    traversal_path_column,
};
use crate::error::{QueryError, Result};
use crate::input::{Input, QueryType, node_group_ids};

#[derive(Clone, Default)]
pub struct ResultBindings {
    pub source_bindings: HashMap<Identifier, String>,
    pub stable_order: Vec<OrderExpr>,
}

#[derive(Clone, Default)]
pub struct ReturnRequirements {
    pub required: Vec<(String, String, Identifier)>,
    pub edge_outputs: Vec<[Identifier; 5]>,
    pub redactions: Vec<(String, usize)>,
}

impl ReturnRequirements {
    pub fn prepare(input: &Input, model: &impl QueryDataModel) -> Result<Self> {
        if input.query_type == QueryType::Hydration {
            return Ok(Self::default());
        }

        let mut requirements = Self {
            required: input
                .nodes
                .iter()
                .map(|node| {
                    (
                        node.id.clone(),
                        node.id_property.clone(),
                        redaction_id_column(&node.id).into(),
                    )
                })
                .collect(),
            edge_outputs: input
                .relationships
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    [
                        EDGE_SRC_SUFFIX,
                        EDGE_DST_SUFFIX,
                        EDGE_SRC_TYPE_SUFFIX,
                        EDGE_DST_TYPE_SUFFIX,
                        EDGE_TYPE_SUFFIX,
                    ]
                    .map(|field| format!("{}edge_{index}_{field}", internal_column_prefix()).into())
                })
                .collect(),
            ..Default::default()
        };
        if let Some(order) = &input.order_by {
            requirements.required.push((
                order.node.clone(),
                order.property.clone(),
                Identifier::generated(),
            ));
        }

        for node in &input.nodes {
            let entity = node
                .entity
                .as_deref()
                .and_then(|name| model.entity(name))
                .ok_or_else(|| QueryError::ReferenceError("node entity is unavailable".into()))?;
            if let Some(property) = model.redaction_id_column(entity.id)
                && property != node.id_property
                && selectable(input, &node.id)
            {
                requirements
                    .redactions
                    .push((node.id.clone(), requirements.required.len()));
                requirements.required.push((
                    node.id.clone(),
                    property.into(),
                    Identifier::generated(),
                ));
            }
        }

        if input.query_type != QueryType::Aggregation {
            for node in &input.nodes {
                if node.entity.as_deref().is_some_and(|entity| {
                    model.entity_auth().contains_key(entity)
                        && model.entity_has_traversal_path(entity)
                }) {
                    requirements.required.push((
                        node.id.clone(),
                        ontology::constants::TRAVERSAL_PATH_COLUMN.into(),
                        traversal_path_column(&node.id).into(),
                    ));
                }
            }
        }
        Ok(requirements)
    }
}

pub fn enforce_lowered_return(
    node: &mut Node,
    input: &Input,
    requirements: &ReturnRequirements,
    model: &impl QueryDataModel,
) -> Result<ResultContext> {
    let mut result = ResultContext::new().with_query_type(input.query_type);
    result.entity_auth.clone_from(model.entity_auth());
    let Node::Query(query) = node else {
        return Ok(result);
    };
    if input.query_type == QueryType::Hydration {
        return Ok(result);
    }

    if let Some((_, _, hidden)) = requirements
        .required
        .get(input.nodes.len())
        .filter(|_| input.order_by.is_some())
    {
        query
            .select
            .retain(|select| select.alias.as_ref() != Some(hidden));
    }
    for (alias, position) in &requirements.redactions {
        let hidden = &requirements.required[*position].2;
        let position = query
            .select
            .iter()
            .position(|select| select.alias.as_ref() == Some(hidden))
            .ok_or_else(|| QueryError::Enforcement("redaction identity was not retained".into()))?;
        let mut identity = query.select.remove(position);
        let name = redaction_id_column(alias);
        let primary = query
            .select
            .iter_mut()
            .find(|select| select.alias.as_ref().and_then(Identifier::name) == Some(name.as_str()))
            .ok_or_else(|| QueryError::Enforcement("primary identity was not retained".into()))?;
        primary.alias = Some(primary_key_column(alias).into());
        identity.alias = Some(name.into());
        query.select.push(identity);
    }
    for node in input
        .nodes
        .iter()
        .filter(|node| selectable(input, &node.id))
    {
        if !query.selects_alias(&redaction_id_column(&node.id)) {
            return Err(QueryError::Enforcement(format!(
                "identity for {} was not retained",
                node.id
            )));
        }
        let entity = node
            .entity
            .as_deref()
            .ok_or_else(|| QueryError::Enforcement("node requires an entity".into()))?;
        let type_column = redaction_type_column(&node.id);
        if !query.selects_alias(&type_column) {
            query
                .select
                .push(SelectExpr::new(Expr::string(entity), type_column));
        }
        if input.query_type != QueryType::Aggregation
            && model.entity_auth().contains_key(entity)
            && model.entity_has_traversal_path(entity)
            && !query.selects_alias(&traversal_path_column(&node.id))
        {
            return Err(QueryError::Enforcement(format!(
                "hydration path for {} was not retained",
                node.id
            )));
        }
        result.add_node(&node.id, entity);
    }
    if input.query_type != QueryType::Aggregation {
        for (index, (relationship, outputs)) in input
            .relationships
            .iter()
            .zip(&requirements.edge_outputs)
            .enumerate()
        {
            let names = outputs
                .iter()
                .map(|output| {
                    output
                        .name()
                        .filter(|name| query.selects_alias(name))
                        .map(String::from)
                        .ok_or_else(|| {
                            QueryError::Enforcement("edge output was not retained".into())
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            let [source, target, source_kind, target_kind, kind]: [String; 5] =
                names.try_into().expect("five edge fields");
            result.add_edge(EdgeMeta {
                column_prefix: crate::constants::edge_column_prefix(index),
                path_column: (relationship.hops != crate::input::HopRange::default())
                    .then(|| crate::constants::edge_path_column(index)),
                rel_types: relationship.types.clone(),
                from_alias: relationship.from.clone(),
                to_alias: relationship.to.clone(),
                type_column: kind,
                src_column: source,
                dst_column: target,
                src_type_column: source_kind,
                dst_type_column: target_kind,
            });
        }
    }
    Ok(result)
}

fn selectable(input: &Input, alias: &str) -> bool {
    match input.query_type {
        QueryType::Aggregation => {
            node_group_ids(&input.aggregation.group_by).any(|group| group == alias)
        }
        QueryType::Traversal | QueryType::Neighbors => true,
        QueryType::PathFinding | QueryType::Hydration => false,
    }
}

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

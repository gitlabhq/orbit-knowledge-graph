mod construct;

use std::collections::HashMap;

use orbit_utils::traversal_path::TraversalPath;
use query_data_model::OrbitQueryModel;

use crate::error::{QueryError, Result};
use crate::input::{
    Input, InputAggregation, InputFilter, InputNode, InputRelationship, JoinPredicate,
};
use crate::query::*;
use crate::query_limits::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Public,
    Local,
}

struct Bind<'a, M> {
    model: &'a M,
    local: bool,
    nodes: Vec<Node>,
    aliases: HashMap<String, NodeId>,
    comparisons: Vec<Comparison>,
}

impl<'a, M: OrbitQueryModel> Bind<'a, M> {
    fn nodes(inputs: Vec<InputNode>, model: &'a M, mode: Mode) -> Result<Self> {
        let mut this = Self {
            model,
            local: mode == Mode::Local,
            nodes: Vec::new(),
            aliases: HashMap::new(),
            comparisons: Vec::new(),
        };
        let mut filters = Vec::new();
        for mut input in inputs {
            filters.push(std::mem::take(&mut input.filters));
            let node = construct::node(input, model, this.local)?;
            let id = NodeId(this.nodes.len());
            if this.aliases.insert(node.alias.clone(), id).is_some() {
                return Err(QueryError::ReferenceError(format!(
                    "duplicate node id \"{}\"",
                    node.alias
                )));
            }
            this.nodes.push(node);
        }
        for (index, filters) in filters.into_iter().enumerate() {
            this.filters(NodeId(index), filters)?;
        }
        Ok(this)
    }

    fn id(&self, alias: &str, usage: &str) -> Result<NodeId> {
        self.aliases.get(alias).copied().ok_or_else(|| {
            QueryError::ReferenceError(format!("{usage} references undefined node \"{alias}\""))
        })
    }

    fn stored(&self, node: NodeId, name: &str, usage: &str) -> Result<StoredProperty> {
        construct::stored_on(
            &self.nodes[node.index()],
            self.model,
            name,
            usage,
            self.local,
        )
    }

    fn filters(&mut self, node: NodeId, filters: HashMap<String, Vec<InputFilter>>) -> Result<()> {
        let comparisons = construct::filters(
            &mut self.nodes[node.index()],
            filters,
            self.model,
            self.local,
        )?;
        for (left_name, right_alias, right_name, op) in comparisons {
            let right_node = self.id(&right_alias, "property comparison")?;
            let left = self.comparable(node, &left_name, op)?;
            let right = self.comparable(right_node, &right_name, op)?;
            if left.data_type != right.data_type {
                return Err(QueryError::Validation(format!(
                    "property comparison type mismatch: {} and {}",
                    left.data_type, right.data_type
                )));
            }
            self.comparisons.push(Comparison {
                left: PropertyRef {
                    node,
                    property: left,
                },
                op,
                right: PropertyRef {
                    node: right_node,
                    property: right,
                },
            });
        }
        Ok(())
    }

    fn comparable(&self, node: NodeId, name: &str, op: FilterOp) -> Result<StoredProperty> {
        construct::comparable(&self.nodes[node.index()], self.model, name, op, self.local)
    }

    fn relationships(&self, inputs: Vec<InputRelationship>) -> Result<Vec<Relationship>> {
        inputs
            .into_iter()
            .enumerate()
            .map(|(index, input)| {
                let from = self.id(&input.from, &format!("relationship[{index}].from"))?;
                let to = self.id(&input.to, &format!("relationship[{index}].to"))?;
                let source = &self.nodes[from.index()];
                let target = &self.nodes[to.index()];
                construct::relationship(self.model, index, input, source, target, from, to)
            })
            .collect()
    }

    fn path_routes(&self, names: Vec<String>) -> Result<Vec<RelationshipRoute>> {
        construct::path_routes(self.model, names)
    }

    fn joined_comparisons(&self, inputs: Vec<JoinPredicate>) -> Result<Vec<Comparison>> {
        inputs
            .into_iter()
            .map(|input| {
                construct::comparison(input, &self.nodes, &self.aliases, self.model, self.local)
            })
            .collect()
    }
}

pub fn bind<M: OrbitQueryModel>(input: Input, model: &M, mode: Mode) -> Result<Query> {
    check_shape(&input)?;
    let Input {
        query_type,
        nodes,
        relationships,
        aggregation,
        path,
        neighbors,
        limit,
        cursor,
        order_by,
        options,
        join_predicates,
    } = input;
    let mut bind = Bind::nodes(nodes, model, mode)?;
    let body = match query_type {
        QueryType::Traversal => {
            none(&aggregation, &path, &neighbors, "traversal")?;
            let relationships = bind.relationships(relationships)?;
            if !relationships.is_empty()
                && relationships.len() != bind.nodes.len().saturating_sub(1)
            {
                return Err(QueryError::Validation(
                    "traversal requires n-1 relationships for n nodes".into(),
                ));
            }
            let order = order_by
                .map(|order| {
                    let node = bind.id(&order.node, "order_by")?;
                    Ok::<_, QueryError>(Order {
                        node,
                        property: bind.stored(node, &order.property, "order_by")?,
                        direction: order.direction,
                    })
                })
                .transpose()?;
            let mut comparisons = std::mem::take(&mut bind.comparisons);
            comparisons.extend(bind.joined_comparisons(join_predicates)?);
            QueryBody::Traversal {
                relationships,
                order,
                comparisons,
            }
        }
        QueryType::Aggregation => {
            absent("order_by", &order_by)?;
            if path.is_some() {
                return Err(QueryError::Validation(
                    "aggregation over shortest paths is not supported".into(),
                ));
            }
            absent("neighbors", &neighbors)?;
            empty("property comparisons", &join_predicates)?;
            empty("same-node property comparisons", &bind.comparisons)?;
            QueryBody::Aggregation {
                relationships: bind.relationships(relationships)?,
                aggregation: construct::aggregation(
                    aggregation,
                    &bind.nodes,
                    &bind.aliases,
                    bind.model,
                    bind.local,
                )?,
            }
        }
        QueryType::PathFinding => {
            empty("relationships", &relationships)?;
            no_aggregation(&aggregation)?;
            absent("order_by", &order_by)?;
            absent("neighbors", &neighbors)?;
            empty("property comparisons", &join_predicates)?;
            let path = path.ok_or_else(|| QueryError::Validation("path is required".into()))?;
            let from = bind.id(&path.from, "path.from")?;
            let to = bind.id(&path.to, "path.to")?;
            if path.max_depth == 0
                || path.max_depth > MAX_DEPTH_CAP
                || [from, to]
                    .iter()
                    .any(|node| !bind.nodes[node.index()].bounds_scan())
            {
                return Err(QueryError::Validation(
                    "path endpoints must be bounded and max_depth supported".into(),
                ));
            }
            QueryBody::PathFinding(Path {
                from,
                to,
                max_depth: path.max_depth,
                wildcard: path.rel_types.as_slice() == ["*"],
                routes: bind.path_routes(path.rel_types)?,
            })
        }
        QueryType::Neighbors => {
            empty("relationships", &relationships)?;
            no_aggregation(&aggregation)?;
            absent("order_by", &order_by)?;
            absent("path", &path)?;
            empty("property comparisons", &join_predicates)?;
            let neighbors = neighbors.ok_or_else(|| {
                QueryError::Validation("neighbors configuration is required".into())
            })?;
            if bind.nodes.len() != 1
                || bind.nodes[0].node_ids.is_empty()
                    && bind.nodes[0].id_range.is_none()
                    && bind.nodes[0].filters.is_empty()
                || neighbors.rel_types.len() > MAX_REL_TYPES
            {
                return Err(QueryError::Validation(
                    "neighbors requires one bounded center node".into(),
                ));
            }
            QueryBody::Neighbors(Neighbors {
                direction: neighbors.direction,
                routes: bind.path_routes(if crate::query::is_wildcard(&neighbors.rel_types) {
                    vec!["*".into()]
                } else {
                    neighbors.rel_types
                })?,
            })
        }
        QueryType::Hydration => {
            return Err(QueryError::Validation(
                "public queries cannot request internal hydration".into(),
            ));
        }
    };
    let comparison_count = match &body {
        QueryBody::Traversal { comparisons, .. } => comparisons.len(),
        _ => 0,
    };
    let predicate_count = comparison_count
        + bind
            .nodes
            .iter()
            .map(|node| {
                node.filters.len()
                    + usize::from(!node.node_ids.is_empty())
                    + 2 * usize::from(node.id_range.is_some())
            })
            .sum::<usize>()
        + match &body {
            QueryBody::Traversal { relationships, .. }
            | QueryBody::Aggregation { relationships, .. } => relationships
                .iter()
                .map(|relationship| relationship.filters.len())
                .sum(),
            _ => 0,
        };
    if predicate_count > MAX_PREDICATES {
        return Err(QueryError::Validation(format!(
            "Query has {predicate_count} predicates; the maximum is {MAX_PREDICATES} across the entire query. Remove redundant conditions or simplify the query."
        )));
    }
    if mode == Mode::Public
        && !matches!(body, QueryBody::PathFinding(_) | QueryBody::Neighbors(_))
        && !bind.nodes.iter().any(|node| {
            !node.node_ids.is_empty() || node.id_range.is_some() || !node.filters.is_empty()
        })
    {
        return Err(QueryError::Validation(
            "add a bounded filter, node_ids, or id_range".into(),
        ));
    }
    Ok(Query::new(
        bind.nodes,
        body,
        limit,
        cursor.map(|cursor| Cursor {
            page_size: cursor.page_size,
            after: cursor.after,
        }),
        options.dynamic_columns,
        options.include_debug_sql,
    ))
}

pub fn bind_hydration<M, I>(nodes: I, limit: u32, model: &M) -> Result<Query>
where
    M: OrbitQueryModel,
    I: IntoIterator<Item = (String, Vec<String>, Vec<i64>, Vec<TraversalPath>)>,
{
    let mut output = Vec::new();
    for (entity_name, columns, node_ids, traversal_paths) in nodes {
        output.push(construct::hydration_node(
            entity_name,
            columns,
            node_ids,
            traversal_paths,
            model,
        )?);
    }
    if output.is_empty() || limit == 0 {
        return Err(QueryError::Validation(
            "hydration requires at least one node and a positive limit".into(),
        ));
    }
    Ok(Query::new(
        output,
        QueryBody::Hydration,
        limit,
        None,
        Default::default(),
        false,
    ))
}

fn check_shape(input: &Input) -> Result<()> {
    if input.nodes.is_empty()
        || input.nodes.len() > MAX_NODES_CAP
        || input.relationships.len() > MAX_RELS_CAP
        || input.limit == 0
        || input.limit > MAX_LIMIT
    {
        return Err(QueryError::Validation(
            "query exceeds the supported shape".into(),
        ));
    }
    if input
        .cursor
        .as_ref()
        .is_some_and(|cursor| cursor.page_size == 0 || cursor.page_size > MAX_LIMIT)
    {
        return Err(QueryError::Validation(format!(
            "cursor.page_size must be between 1 and {MAX_LIMIT}"
        )));
    }
    if input
        .nodes
        .iter()
        .any(|node| node.node_ids.len() > MAX_NODE_IDS || node.filters.len() > MAX_FILTERS_PER_NODE)
    {
        return Err(QueryError::Validation(
            "node selector exceeds the supported shape".into(),
        ));
    }
    Ok(())
}

fn none<T, U>(
    aggregation: &InputAggregation,
    left: &Option<T>,
    right: &Option<U>,
    kind: &str,
) -> Result<()> {
    no_aggregation(aggregation)?;
    absent("path", left)?;
    absent("neighbors", right)
        .map_err(|_| QueryError::Validation(format!("unsupported fields on {kind} query")))
}

fn no_aggregation(aggregation: &InputAggregation) -> Result<()> {
    empty("aggregation fields", &aggregation.metrics)?;
    empty("group_by", &aggregation.group_by)?;
    absent("aggregation_sort", &aggregation.sort)
}

fn absent<T>(name: &str, value: &Option<T>) -> Result<()> {
    value.as_ref().map_or(Ok(()), |_| {
        Err(QueryError::Validation(format!(
            "{name} is not supported for this query type"
        )))
    })
}

fn empty<T>(name: &str, values: &[T]) -> Result<()> {
    if values.is_empty() {
        Ok(())
    } else {
        Err(QueryError::Validation(format!(
            "{name} are not supported for this query type"
        )))
    }
}

pub(crate) fn validate_identifier(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    if identifier.len() <= MAX_IDENTIFIER_LEN
        && chars
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        && chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        Ok(())
    } else {
        Err(QueryError::Validation(format!(
            "invalid identifier {identifier:?}"
        )))
    }
}

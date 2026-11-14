use std::collections::{BTreeSet, HashMap};

use orbit_server_config::QueryLimits;
use query_data_model::catalog::{Needs, Operation, SearchMode};
use query_data_model::plan::logical::{
    Columns, Comparison, Conjunct, Expr, LogicalOperation, Measure, Operand, Predicate, Projection,
};
use query_data_model::plan::tracking::Ref;
use query_data_model::surface::Statement;
use query_data_model::{
    Catalog, ConnectionId, Direction, Endpoint, EntityId, PropertyId, RelationshipId, StoreId,
};

use super::cut::{Cut, Group};
use super::{Connections, Dag, Place, Stage, StageQuery};
use crate::error::{QueryError, Result};
use crate::logical::{Kind, Query, Step};
use crate::passes::codegen::SqlBackend;

/// A relationship step whose endpoints live on different connections, with the key pairs the
/// Combine stage joins its two sides on.
pub(super) struct Crossing {
    pub groups: (usize, usize),
    pub pairs: Vec<((Ref, PropertyId), (Ref, PropertyId))>,
    /// The step's index among the pattern's steps.
    pub step: usize,
    pub relationship: RelationshipId,
    /// The bindings of the relationship variant's source and target.
    pub ends: (Ref, Ref),
}

/// Splits a statement into stages, one per connection it reads. A query one relational
/// connection serves alone is one stage on it; otherwise every stage hands its rows to the
/// Combine stage, which shapes the response.
/// `frontend` is the catalog the deployment's frontend rules read, which the query's statistics
/// read too.
pub fn split<B: SqlBackend>(
    statement: Statement,
    frontend: &Catalog<B>,
    connections: &Connections,
    limits: &QueryLimits,
) -> Result<Dag> {
    let query = Query::new(&statement.plan, frontend.model());
    let places = places(query, connections)?;
    let distinct: BTreeSet<ConnectionId> = places.values().map(|place| place.connection).collect();
    let single = match distinct.iter().collect::<Vec<_>>()[..] {
        [connection] => connections.get(*connection).filter(|peer| peer.joins()),
        _ => None,
    };
    if let Some(peer) = single {
        return Ok(Dag {
            stages: vec![Stage {
                id: 0,
                backend: peer.backend(),
                runs: super::Runs::On {
                    connection: peer.connection(),
                    stores: stores(query, &places, |_| true),
                },
                query: StageQuery::Gql(statement),
                inputs: Vec::new(),
                response: true,
                checks: Vec::new(),
            }],
            logical: Vec::new(),
        });
    }
    if query.kind() != Kind::Traversal {
        let names: Vec<&str> = distinct
            .iter()
            .filter_map(|connection| connections.get(*connection))
            .map(super::Peer::name)
            .collect();
        return Err(QueryError::Validation(format!(
            "a {} query reads one connection; this one reads {}",
            <&str>::from(crate::types::QueryType::from(query.kind())),
            names.join(" and ")
        )));
    }
    let groups = groups(query, &places);
    let crossings = query
        .steps()
        .into_iter()
        .enumerate()
        .filter_map(|(index, step)| {
            let (from, to) = (group_of(&groups, step.from)?, group_of(&groups, step.to)?);
            (from != to).then(|| crossing(query, index, step, (from, to)))
        })
        .collect::<Result<Vec<_>>>()?;
    let cut = Cut {
        query,
        groups,
        crossings,
        limits,
        connections,
    };
    let response = super::Response {
        shape: crate::passes::result_contract::ResultShape::new(query)?,
        stats: crate::analytics::QueryStats::new(query, frontend),
        debug: statement.debug,
    };
    cut.dag(response)
}

/// Each binding's store and the served connection it sits on: the store the model places a read
/// with the binding's needs on.
pub(crate) fn places(query: Query<'_>, connections: &Connections) -> Result<HashMap<Ref, Place>> {
    let model = query.model;
    let mut needs = needs(query);
    query
        .bindings()
        .into_iter()
        .map(|binding| {
            let wanted = needs.remove(&binding.output).unwrap_or_default();
            let placement = model
                .get_store(binding.entity, &wanted)
                .map_err(|unplaced| QueryError::AllowlistRejected(unplaced.to_string()))?;
            let [(store, _)] = placement.parts() else {
                return Err(QueryError::PipelineInvariant(
                    "a read runs on one store".into(),
                ));
            };
            let store = model.store(*store);
            if connections.get(store.connection()).is_none() {
                return Err(QueryError::AllowlistRejected(format!(
                    "{} reads from store {} on connection {}, which this deployment does not serve",
                    model.graph().entity(binding.entity).name,
                    store.name(),
                    model.connection(store.connection()).name()
                )));
            }
            Ok((
                binding.output,
                Place {
                    connection: store.connection(),
                    store: store.id(),
                },
            ))
        })
        .collect()
}

/// What each binding reads and applies: the properties its projections, groups and order read,
/// the conditions on them, and its structural matches.
fn needs(query: Query<'_>) -> HashMap<Ref, Needs> {
    let mut needs: HashMap<Ref, Needs> = HashMap::new();
    for (_, operation) in query.operations() {
        match operation {
            LogicalOperation::Filter(predicate) => {
                for conjunct in predicate.conjuncts() {
                    match conjunct {
                        Conjunct::Leaf { predicate, negated } => {
                            condition(predicate, negated, &mut needs)
                        }
                        Conjunct::Negated(group) => {
                            for leaf in group.leaf_predicates() {
                                condition(leaf, true, &mut needs);
                            }
                        }
                    }
                }
            }
            LogicalOperation::Structural(structural) => {
                if let Expr::Property { binding, property } = structural.target {
                    needs
                        .entry(binding)
                        .or_default()
                        .apply(property, Operation::Search(SearchMode::Structural));
                }
            }
            LogicalOperation::Select(items) => {
                for item in items {
                    match item {
                        Projection::Graph {
                            binding,
                            columns: Columns::Listed(properties),
                        } => {
                            let binding = needs.entry(*binding).or_default();
                            properties
                                .iter()
                                .for_each(|property| binding.read(*property));
                        }
                        Projection::Graph { .. } => {}
                        Projection::Value(expr) => read(*expr, &mut needs),
                    }
                }
            }
            LogicalOperation::Order(keys) => {
                keys.iter().for_each(|(expr, _)| read(*expr, &mut needs))
            }
            LogicalOperation::Aggregate { groups, measures } => {
                groups.iter().for_each(|expr| read(*expr, &mut needs));
                for measure in measures {
                    match measure {
                        Measure::Count(Some(expr))
                        | Measure::CountDistinct(expr)
                        | Measure::Sum(expr)
                        | Measure::Avg(expr)
                        | Measure::Min(expr)
                        | Measure::Max(expr) => read(*expr, &mut needs),
                        Measure::Count(None) => {}
                    }
                }
            }
            _ => {}
        }
    }
    needs
}

fn read(expr: Expr, needs: &mut HashMap<Ref, Needs>) {
    if let Expr::Property { binding, property }
    | Expr::Truncate {
        binding, property, ..
    } = expr
    {
        needs.entry(binding).or_default().read(property);
    }
}

/// One condition's need on the property it tests; a negated leaf is its complement when it has
/// one. A comparison with another expression reads both sides, which the stage that holds them
/// both evaluates.
fn condition(predicate: &Predicate, negated: bool, needs: &mut HashMap<Ref, Needs>) {
    if let Predicate::Compare {
        left,
        right: Operand::Expr(right),
        ..
    } = predicate
    {
        read(*left, needs);
        read(*right, needs);
        return;
    }
    let complement = negated.then(|| predicate.complement());
    let operation = match (complement.as_ref(), predicate) {
        (Some(Some(complement)), _) => operation(complement),
        (Some(None), _) => Operation::Other,
        (None, predicate) => operation(predicate),
    };
    for expr in predicate.exprs() {
        match expr {
            Expr::Property { binding, property } => {
                needs.entry(binding).or_default().apply(property, operation)
            }
            other => read(other, needs),
        }
    }
}

fn operation(predicate: &Predicate) -> Operation {
    match predicate {
        Predicate::Compare {
            comparison: Comparison::Eq,
            ..
        } => Operation::Equal,
        Predicate::Compare {
            comparison: Comparison::Lt | Comparison::Le | Comparison::Gt | Comparison::Ge,
            ..
        } => Operation::Bound,
        Predicate::In { .. } | Predicate::InTuple { .. } => Operation::OneOf,
        Predicate::Text { matching, .. } => Operation::Text(*matching),
        _ => Operation::Other,
    }
}

/// Bindings joined by steps that stay on one connection, in the order the plan binds them.
fn groups(query: Query<'_>, places: &HashMap<Ref, Place>) -> Vec<Group> {
    let mut groups: Vec<Group> = Vec::new();
    let mut joined: HashMap<Ref, Ref> = HashMap::new();
    let connection = |binding: &Ref| places.get(binding).map(|place| place.connection);
    for step in query.steps() {
        if connection(&step.from) == connection(&step.to) {
            joined.insert(step.to, step.from);
        }
    }
    for binding in query.bindings() {
        let mut root = binding.output;
        while let Some(from) = joined.get(&root) {
            root = *from;
        }
        match groups.iter_mut().find(|group| group.bindings[0] == root) {
            Some(group) => group.bindings.push(binding.output),
            None => groups.push(Group {
                place: places[&binding.output].connection,
                bindings: vec![binding.output],
                stores: Vec::new(),
            }),
        }
    }
    for group in &mut groups {
        let bindings = group.bindings.clone();
        group.stores = stores(query, places, |binding| bindings.contains(&binding));
    }
    groups
}

/// The placed store of each entity the bindings `mine` keeps read, once per entity.
fn stores(
    query: Query<'_>,
    places: &HashMap<Ref, Place>,
    mine: impl Fn(Ref) -> bool,
) -> Vec<(EntityId, StoreId)> {
    let mut stores: Vec<(EntityId, StoreId)> = Vec::new();
    for binding in query
        .bindings()
        .into_iter()
        .filter(|binding| mine(binding.output))
    {
        let placed = (binding.entity, places[&binding.output].store);
        if !stores.contains(&placed) {
            stores.push(placed);
        }
    }
    stores
}

fn group_of(groups: &[Group], binding: Ref) -> Option<usize> {
    groups
        .iter()
        .position(|group| group.bindings.contains(&binding))
}

/// The foreign key of the step's one relationship variant, as the key pair the Combine stage joins on.
fn crossing(
    query: Query<'_>,
    index: usize,
    step: Step<'_>,
    groups: (usize, usize),
) -> Result<Crossing> {
    let graph = query.model.graph();
    let (source, target) = match step.direction {
        Direction::Incoming => (step.to, step.from),
        _ => (step.from, step.to),
    };
    let (source_entity, target_entity) = (query.entity(source), query.entity(target));
    let unjoinable = || {
        QueryError::Validation(format!(
            "{} -> {} crosses connections, so it needs one relationship with a foreign key the Combine stage joins on",
            query.name(source),
            query.name(target)
        ))
    };
    let [relationship] = step.relationships else {
        return Err(unjoinable());
    };
    let (endpoint, keys) = graph
        .variants()
        .filter(|variant| variant.relationship == *relationship)
        .filter(|variant| Some(variant.source) == source_entity)
        .filter(|variant| Some(variant.target) == target_entity)
        .find_map(|variant| match query.model.foreign_key(variant.id) {
            Some(key) => Some((key.holder, vec![(key.property, key.referenced_key)])),
            None => query
                .model
                .composite_key(variant.id)
                .map(|(holder, pairs)| (holder, pairs.to_vec())),
        })
        .filter(|_| step.range.max == 1)
        .ok_or_else(unjoinable)?;
    let (holder, referenced) = match endpoint {
        Endpoint::Source => (source, target),
        Endpoint::Target => (target, source),
    };
    Ok(Crossing {
        groups,
        step: index,
        relationship: *relationship,
        ends: (source, target),
        pairs: keys
            .into_iter()
            .map(|(held, key)| ((holder, held), (referenced, key)))
            .collect(),
    })
}

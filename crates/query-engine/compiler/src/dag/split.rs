use std::collections::{BTreeSet, HashMap};

use orbit_server_config::QueryLimits;
use query_data_model::plan::logical::{Expr, LogicalOperation};
use query_data_model::plan::tracking::Ref;
use query_data_model::surface::Statement;
use query_data_model::{Catalog, Direction, Endpoint, PropertyId};

use super::cut::{Cut, Group};
use super::{Connections, Dag, Stage, StageQuery};
use crate::error::{QueryError, Result};
use crate::logical::{Kind, Query, Step};
use crate::passes::codegen::SqlBackend;

/// The served connection a binding reads, by its index among the deployment's connections.
pub(super) type Place = usize;

/// A relationship step whose endpoints live on different connections, with the key pairs the
/// engine joins its two sides on.
pub(super) struct Crossing {
    pub groups: (usize, usize),
    pub pairs: Vec<((Ref, PropertyId), (Ref, PropertyId))>,
}

/// Splits a statement into stages, one per connection it reads. A query one relational
/// connection serves alone is one stage on it; otherwise every stage hands its rows to the
/// engine stage, which shapes the response.
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
    let distinct: BTreeSet<Place> = places.values().copied().collect();
    let single = match distinct.iter().collect::<Vec<_>>()[..] {
        [place] => Some(&connections.peers()[*place]).filter(|peer| peer.remote().is_none()),
        _ => None,
    };
    if let Some(peer) = single {
        return Ok(Dag {
            stages: vec![Stage {
                id: 0,
                backend: peer.backend(),
                connection: Some(peer.connection().to_string()),
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
            .map(|place| connections.peers()[*place].connection())
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
        .filter_map(|step| {
            let (from, to) = (group_of(&groups, step.from)?, group_of(&groups, step.to)?);
            (from != to).then(|| crossing(query, step, (from, to)))
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

/// Each binding's connection: the code connection that searches the property a structural
/// match runs over, else the connection that holds its entity.
fn places(query: Query<'_>, connections: &Connections) -> Result<HashMap<Ref, Place>> {
    let searched: HashMap<Ref, PropertyId> = query
        .operations()
        .filter_map(|(_, operation)| match operation {
            LogicalOperation::Structural(structural) => match structural.target {
                Expr::Property { binding, property } => Some((binding, property)),
                _ => None,
            },
            _ => None,
        })
        .collect();
    query
        .bindings()
        .into_iter()
        .map(|binding| {
            let entity = binding.entity;
            let name = &query.model.graph().entity(entity).name;
            let place = match searched.get(&binding.output) {
                Some(property) => connections.searching(entity, *property).ok_or_else(|| {
                    QueryError::AllowlistRejected(format!(
                        "no served connection searches {name}.{}",
                        query.model.graph().property(*property).name
                    ))
                })?,
                None => connections
                    .holding(entity)
                    .map_err(|unplaced| super::unplaced(name, unplaced))?,
            };
            Ok((binding.output, place))
        })
        .collect()
}

/// Bindings joined by steps that stay on one connection, in the order the plan binds them.
fn groups(query: Query<'_>, places: &HashMap<Ref, Place>) -> Vec<Group> {
    let mut groups: Vec<Group> = Vec::new();
    let mut joined: HashMap<Ref, Ref> = HashMap::new();
    for step in query.steps() {
        if places.get(&step.from) == places.get(&step.to) {
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
                place: places[&binding.output],
                bindings: vec![binding.output],
            }),
        }
    }
    groups
}

fn group_of(groups: &[Group], binding: Ref) -> Option<usize> {
    groups
        .iter()
        .position(|group| group.bindings.contains(&binding))
}

/// The foreign key of the step's one relationship variant, as the key pair the engine joins on.
fn crossing(query: Query<'_>, step: Step<'_>, groups: (usize, usize)) -> Result<Crossing> {
    let graph = query.model.graph();
    let (source, target) = match step.direction {
        Direction::Incoming => (step.to, step.from),
        _ => (step.from, step.to),
    };
    let (source_entity, target_entity) = (query.entity(source), query.entity(target));
    let unjoinable = || {
        QueryError::Validation(format!(
            "{} -> {} crosses connections, so it needs one relationship with a foreign key the engine joins on",
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
        pairs: keys
            .into_iter()
            .map(|(held, key)| ((holder, held), (referenced, key)))
            .collect(),
    })
}

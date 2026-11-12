use std::collections::{BTreeSet, HashMap};

use orbit_server_config::QueryLimits;
use query_data_model::plan::logical::{Expr, LogicalOperation};
use query_data_model::plan::tracking::Ref;
use query_data_model::surface::Statement;
use query_data_model::{Catalog, Direction, Endpoint, PropertyId};

use super::cut::{Cut, Group};
use super::{Backend, Dag, Remotes, Stage, StageQuery};
use crate::error::{QueryError, Result};
use crate::logical::{Kind, Query, Step};
use crate::passes::codegen::SqlBackend;

/// The connection a binding reads: the relational one, or a served remote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) enum Place {
    Primary,
    Remote(usize),
}

/// A relationship step whose endpoints live on different connections, with the key pairs the
/// engine joins its two sides on.
pub(super) struct Crossing {
    pub groups: (usize, usize),
    pub pairs: Vec<((Ref, PropertyId), (Ref, PropertyId))>,
}

/// Splits a statement into stages, one per connection it reads.
pub fn split<B: SqlBackend>(
    statement: Statement,
    primary: &Catalog<B>,
    remotes: &Remotes,
    limits: &QueryLimits,
) -> Result<Dag> {
    let query = Query::new(&statement.plan, primary.model());
    let places = places(query, primary, remotes);
    let distinct: BTreeSet<Place> = places.values().copied().collect();
    if distinct.len() <= 1 {
        let (backend, connection) = match distinct.first() {
            Some(Place::Remote(index)) => {
                let target = &remotes.targets()[*index];
                (target.backend(), target.connection())
            }
            _ => (B::BACKEND, primary.connection()),
        };
        return Ok(Dag {
            stages: vec![Stage {
                id: 0,
                backend,
                connection: Some(connection.to_string()),
                query: StageQuery::Gql(statement),
                inputs: Vec::new(),
            }],
            logical: Vec::new(),
        });
    }
    if query.kind() != Kind::Traversal {
        return Err(QueryError::Validation(format!(
            "a {} query reads one connection; this one reads {}",
            <&str>::from(crate::types::QueryType::from(query.kind())),
            names(&distinct, primary, remotes)
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
        backends: places_backends(primary.connection(), B::BACKEND, remotes),
        limits,
    };
    cut.dag(statement.debug)
}

fn places<B: SqlBackend>(
    query: Query<'_>,
    primary: &Catalog<B>,
    remotes: &Remotes,
) -> HashMap<Ref, Place> {
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
    let index = |target: &crate::passes::plan::remote::Target| {
        remotes
            .targets()
            .iter()
            .position(|candidate| std::ptr::eq(candidate, target))
            .map(Place::Remote)
    };
    query
        .bindings()
        .into_iter()
        .map(|binding| {
            let entity = binding.entity;
            let place = match searched.get(&binding.output) {
                Some(property) => remotes.searching(entity, *property).and_then(index),
                None if primary.table_of(entity).is_some() => Some(Place::Primary),
                None => remotes.holding(entity).and_then(index),
            };
            (binding.output, place.unwrap_or(Place::Primary))
        })
        .collect()
}

fn names<B: SqlBackend>(
    places: &BTreeSet<Place>,
    primary: &Catalog<B>,
    remotes: &Remotes,
) -> String {
    let name = |place: &Place| match place {
        Place::Primary => primary.connection(),
        Place::Remote(index) => remotes.targets()[*index].connection(),
    };
    places.iter().map(name).collect::<Vec<_>>().join(" and ")
}

fn places_backends(primary: &str, backend: Backend, remotes: &Remotes) -> Vec<(String, Backend)> {
    std::iter::once((primary.to_string(), backend))
        .chain(
            remotes
                .targets()
                .iter()
                .map(|target| (target.connection().to_string(), target.backend())),
        )
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
    let key = graph
        .variants()
        .filter(|variant| variant.relationship == *relationship)
        .filter(|variant| Some(variant.source) == source_entity)
        .filter(|variant| Some(variant.target) == target_entity)
        .find_map(|variant| query.model.foreign_key(variant.id))
        .filter(|_| step.range.max == 1)
        .ok_or_else(unjoinable)?;
    let (holder, referenced) = match key.holder {
        Endpoint::Source => (source, target),
        Endpoint::Target => (target, source),
    };
    Ok(Crossing {
        groups,
        pairs: vec![((holder, key.property), (referenced, key.referenced_key))],
    })
}

use query_data_model::OrbitQueryModel;

use crate::error::{QueryError, Result};
use crate::input::InputRelationship;
use crate::query::{Direction, Filter, HopRange, NodeId, Query, Relationship};
use crate::query_limits::{MAX_FILTERS_PER_REL, MAX_HOPS_CAP, MAX_REL_TYPES};

use super::properties::bind_predicate;
use super::validation::check_value;

pub(super) fn bind_relationships<M: OrbitQueryModel>(
    query: &mut Query,
    relationships: Vec<InputRelationship>,
    model: &M,
) -> Result<()> {
    for (index, input) in relationships.into_iter().enumerate() {
        let from = query.node_id(&input.from).ok_or_else(|| {
            QueryError::ReferenceError(format!(
                "relationship[{index}] references undefined node \"{}\" in 'from'",
                input.from
            ))
        })?;
        let to = query.node_id(&input.to).ok_or_else(|| {
            QueryError::ReferenceError(format!(
                "relationship[{index}] references undefined node \"{}\" in 'to'",
                input.to
            ))
        })?;
        if input.types.is_empty() {
            return Err(QueryError::Validation(
                "relationships require at least one type".into(),
            ));
        }
        if input.hops.min == 0 || input.hops.min > input.hops.max || input.hops.max > MAX_HOPS_CAP {
            return Err(QueryError::Validation(format!(
                "relationship hop bounds must be ordered between 1 and {MAX_HOPS_CAP}"
            )));
        }
        if input.types.len() > MAX_REL_TYPES {
            return Err(QueryError::Validation(format!(
                "relationship type count ({}) must not exceed {MAX_REL_TYPES}",
                input.types.len()
            )));
        }
        if input.filters.len() > MAX_FILTERS_PER_REL {
            return Err(QueryError::Validation(format!(
                "relationship filter property count ({}) must not exceed {MAX_FILTERS_PER_REL}",
                input.filters.len()
            )));
        }
        if input.hops.max > 1 && !input.filters.is_empty() {
            return Err(QueryError::Validation(
                "property filters on variable-length relationships are unsupported".into(),
            ));
        }
        let type_count = input.types.len();
        let names = specialize_relationships(
            query,
            from,
            to,
            input.direction,
            input.hops,
            input.types,
            model,
        )?;
        let ids: Vec<_> = names
            .iter()
            .filter(|name| name.as_str() != "*")
            .map(|name| {
                model.graph().relationship_id(name).ok_or_else(|| {
                    QueryError::AllowlistRejected(format!("unknown relationship type {name:?}"))
                })
            })
            .collect::<Result<_>>()?;
        let tables = model.relationship_tables(&names);
        if tables.len() > 1 && type_count <= 2 && !names.iter().any(|name| name == "*") {
            return Err(QueryError::Validation(format!(
                "relationship alternatives span unsupported edge tables: {}",
                tables.join(", ")
            )));
        }
        if type_count <= 2 {
            check_relationship_variants(
                query,
                from,
                to,
                input.direction,
                input.hops,
                &names,
                model,
            )?;
        }
        let table = tables
            .first()
            .map(String::as_str)
            .unwrap_or_else(|| model.default_edge_table());
        let mut filters = Vec::new();
        for (name, predicates) in input.filters {
            let data_type = model.table_column_type(table, &name).ok_or_else(|| {
                QueryError::Validation(format!(
                    "relationship[{index}] filter on unknown edge column \"{name}\""
                ))
            })?;
            for predicate in predicates {
                let predicate = bind_predicate(predicate);
                check_value(&predicate, data_type, &name)?;
                filters.push(Filter {
                    name: name.clone(),
                    property: None,
                    data_type,
                    stored: true,
                    selectivity: ontology::FieldSelectivity::default(),
                    in_sort_key: model.in_sort_key(table, &name),
                    predicate,
                });
            }
        }
        query.relationships.push(Relationship {
            ids,
            names,
            tables,
            from,
            to,
            hops: input.hops,
            direction: input.direction,
            filters,
        });
    }
    Ok(())
}

fn specialize_relationships<M: OrbitQueryModel>(
    query: &Query,
    from: NodeId,
    to: NodeId,
    direction: Direction,
    hops: HopRange,
    types: Vec<String>,
    model: &M,
) -> Result<Vec<String>> {
    let wildcard = types.len() == 1 && types[0] == "*";
    if types.iter().any(|kind| kind == "*") && !wildcard {
        return Err(QueryError::Validation(
            "relationship wildcard cannot be combined with concrete types".into(),
        ));
    }
    if !wildcard || hops.max > 1 {
        return Ok(types);
    }
    let from = &query.node(from).entity_name;
    let to = &query.node(to).entity_name;
    let names = match direction {
        Direction::Outgoing => model.graph().relationship_names(Some(from), Some(to)),
        Direction::Incoming => model.graph().relationship_names(Some(to), Some(from)),
        Direction::Both => model
            .graph()
            .relationship_names(Some(from), Some(to))
            .into_iter()
            .chain(model.graph().relationship_names(Some(to), Some(from)))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect(),
    };
    if names.is_empty() {
        return Ok(types);
    }
    Ok(names)
}

fn check_relationship_variants<M: OrbitQueryModel>(
    query: &Query,
    from: NodeId,
    to: NodeId,
    direction: Direction,
    hops: HopRange,
    names: &[String],
    model: &M,
) -> Result<()> {
    if hops.max > 1 || direction == Direction::Both {
        return Ok(());
    }
    let (source, target) = match direction {
        Direction::Outgoing => (query.node(from), query.node(to)),
        Direction::Incoming => (query.node(to), query.node(from)),
        Direction::Both => unreachable!(),
    };
    let from_alias = &query.node(from).alias;
    let to_alias = &query.node(to).alias;
    let invalid: Vec<_> = names
        .iter()
        .filter(|name| name.as_str() != "*")
        .filter(|name| {
            let Some(relationship) = model.graph().relationship_id(name) else {
                return true;
            };
            let variants = &model.graph().relationship(relationship).variants;
            !variants.is_empty()
                && model
                    .graph()
                    .variant_id(relationship, source.entity, target.entity)
                    .is_none()
        })
        .collect();
    if invalid.len() < names.len() {
        return Ok(());
    }
    let reversed: Vec<_> = invalid
        .iter()
        .copied()
        .filter(|name| {
            model
                .graph()
                .relationship_id(name)
                .is_some_and(|relationship| {
                    model
                        .graph()
                        .variant_id(relationship, target.entity, source.entity)
                        .is_some()
                })
        })
        .collect();
    let unconnected: Vec<_> = invalid
        .iter()
        .copied()
        .filter(|name| !reversed.contains(name))
        .collect();
    if !reversed.is_empty() {
        let reversed = reversed
            .into_iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join("|");
        let fix = format!(
            "{reversed} goes from {} to {}; reverse the arrow: ({to_alias})-[:{reversed}]->({from_alias})",
            target.entity_name, source.entity_name
        );
        if unconnected.is_empty() {
            return Err(QueryError::Validation(fix));
        }
        return Err(QueryError::Validation(format!(
            "{fix}; {} does not connect {} and {}",
            unconnected
                .into_iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join("|"),
            source.entity_name,
            target.entity_name
        )));
    }
    Err(QueryError::Validation(format!(
        "{} does not connect {} and {} in either direction",
        unconnected
            .into_iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join("|"),
        source.entity_name,
        target.entity_name
    )))
}

use super::super::api::*;
use super::predicates;
use crate::constants::*;
use crate::input::{Direction, Input, InputFilter, InputNode, RelationshipSelection};
use query_data_model::{DenormalizedDirection, DenormalizedKey, QueryDataModel};

struct Route<'a> {
    direction: Direction,
    tables: Vec<&'a str>,
}

pub(super) fn build<'a, M: QueryDataModel + ?Sized>(
    q: &mut QueryScope<'_, 'a, M>,
    input: &Input,
) -> Result<Rows<'a>> {
    let [node] = input.nodes.as_slice() else {
        return Err(Error::Outputs);
    };
    if input.order_by.is_some() || !input.join_predicates.is_empty() {
        return Err(Error::Outputs);
    }
    let config = input.neighbors.as_ref().ok_or(Error::Outputs)?;
    let model = q.catalog();
    let entity = node
        .entity
        .as_deref()
        .and_then(|name| model.entity(name))
        .ok_or(Error::Outputs)?;
    let routes = routes(model, entity.id, config.direction, &config.rel_types);
    let covered = node.filters.iter().all(|(property, filters)| {
        routes.iter().all(|route| {
            tag(
                model,
                node,
                property,
                filters,
                route.direction,
                &config.rel_types,
            )
            .is_some()
        })
    });
    let redaction = model.redaction_id_column(entity.id).unwrap_or("id");
    let needs_scan = !covered
        || node.id_range.is_some()
        || redaction != "id"
        || model
            .entity_minimum_access_level(&entity.name)
            .is_some_and(|level| level > crate::types::DEFAULT_PATH_ACCESS_LEVEL);
    let rows = if config.direction == Direction::Both
        && !needs_scan
        && routes
            .iter()
            .all(|route| route.tables.len() == 1 && route.tables[0] == routes[0].tables[0])
    {
        fused(q, node, &config.rel_types, routes[0].tables[0])?
    } else {
        let mut arms = Vec::new();
        for route in routes {
            for table in route.tables {
                arms.push(q.subquery(|q| {
                    directional(
                        q,
                        node,
                        &config.rel_types,
                        route.direction,
                        table,
                        needs_scan,
                    )
                })?);
            }
        }
        if arms.len() == 1 {
            q.from(arms[0])?
        } else {
            q.union_all(arms)?
        }
    };
    q.limit(rows, input.limit)
}

fn routes<'a>(
    model: &'a (impl QueryDataModel + ?Sized),
    entity: query_data_model::EntityId,
    direction: Direction,
    kinds: &RelationshipSelection,
) -> Vec<Route<'a>> {
    [Direction::Outgoing, Direction::Incoming]
        .into_iter()
        .filter(|candidate| direction == Direction::Both || direction == *candidate)
        .map(|direction| {
            let mut tables = model
                .graph()
                .relationships()
                .filter(|relationship| kinds.matches(&relationship.name))
                .filter_map(|relationship| model.relationship_route(&relationship.name))
                .filter(|route| {
                    if direction == Direction::Outgoing {
                        route.has_source(entity)
                    } else {
                        route.has_target(entity)
                    }
                })
                .map(|route| route.table)
                .collect::<Vec<_>>();
            if tables.is_empty() {
                tables = model
                    .graph()
                    .relationships()
                    .filter(|relationship| kinds.matches(&relationship.name))
                    .filter_map(|relationship| model.relationship_table(&relationship.name))
                    .collect();
            }
            if tables.is_empty() {
                tables.push(model.default_edge_table());
            }
            tables.sort_unstable();
            tables.dedup();
            Route { direction, tables }
        })
        .collect()
}

fn directional<'a, M: QueryDataModel + ?Sized>(
    q: &mut QueryScope<'_, 'a, M>,
    center: &InputNode,
    kinds: &RelationshipSelection,
    direction: Direction,
    table: &str,
    needs_scan: bool,
) -> Result<Rows<'a>> {
    let entity = center.entity.as_deref().ok_or(Error::Outputs)?;
    let entity_id = q.catalog().entity(entity).ok_or(Error::Outputs)?.id;
    let redaction = q.catalog().redaction_id_column(entity_id).unwrap_or("id");
    let edge = q.scan(table, Read::Raw)?;
    let (identity_name, _, neighbor, kind) = columns(direction);
    let identity = edge.column(identity_name)?;
    let mut values = vec![
        edge.column(neighbor)?.named(neighbor_id_column()),
        edge.column(kind)?.named(neighbor_type_column()),
        edge.column("relationship_kind")?
            .named(relationship_type_column()),
        lit(i64::from(direction == Direction::Outgoing)).named(neighbor_is_outgoing_column()),
    ];
    let path = if q.catalog().entity_has_traversal_path(entity) {
        Some(
            edge.column(ontology::TRAVERSAL_PATH_COLUMN)?
                .named(traversal_path_column(&center.id)),
        )
    } else {
        None
    };
    let condition = condition(q.catalog(), &edge, center, direction, kinds)?;
    let mut rows = source(q, edge, kinds, condition)?;
    if direction == Direction::Incoming
        && !center.node_ids.is_empty()
        && let Some((table, key)) = q
            .catalog()
            .traversal_path_lookup(entity, ontology::TraversalPathKind::Id)
    {
        let lookup = q.scan(table, Read::Raw)?;
        let path = lookup.column(ontology::TRAVERSAL_PATH_COLUMN)?;
        let predicate = lookup
            .column(key)?
            .in_values(center.node_ids.iter().copied())
            .and(lookup.column(ontology::DELETED_COLUMN)?.eq(false));
        let lookup = q.filter(lookup, predicate)?;
        let edge_path = rows.column(ontology::TRAVERSAL_PATH_COLUMN)?;
        rows = q.filter_in(rows, edge_path, lookup, path)?;
    }
    let authorization = if needs_scan {
        let table = q.catalog().entity_table(entity).ok_or(Error::Outputs)?;
        let mut nodes = q.scan(table, Read::Current)?.labeled(&center.id)?;
        let id = nodes.column("id")?;
        let authorization = nodes.column(redaction)?;
        for predicate in predicates::node(q.catalog(), &nodes, center)? {
            nodes = q.filter(nodes, predicate)?;
        }
        rows = q.join(rows, nodes, identity.eq(id))?;
        authorization
    } else {
        identity.clone()
    };
    values.push(authorization.named(redaction_id_column(&center.id)));
    values.push(lit(entity).named(redaction_type_column(&center.id)));
    if redaction != "id" {
        values.push(identity.named(primary_key_column(&center.id)));
    }
    values.extend(path);
    q.select(rows, values)
}

fn fused<'a, M: QueryDataModel + ?Sized>(
    q: &mut QueryScope<'_, 'a, M>,
    center: &InputNode,
    kinds: &RelationshipSelection,
    table: &str,
) -> Result<Rows<'a>> {
    let edges = q.scan(table, Read::Raw)?;
    let outgoing = condition(q.catalog(), &edges, center, Direction::Outgoing, kinds)?;
    let incoming = condition(q.catalog(), &edges, center, Direction::Incoming, kinds)?;
    let relationship = edges.column("relationship_kind")?.expr();
    let entity = center.entity.as_deref().ok_or(Error::Outputs)?;
    let path = if q.catalog().entity_has_traversal_path(entity) {
        Some(
            edges
                .column(ontology::TRAVERSAL_PATH_COLUMN)?
                .named(traversal_path_column(&center.id)),
        )
    } else {
        None
    };
    let mut matches = Vec::new();
    for (direction, condition) in [
        (Direction::Outgoing, outgoing.clone()),
        (Direction::Incoming, incoming.clone()),
    ] {
        let (identity, _, neighbor, kind) = columns(direction);
        matches.push(singleton_if(
            condition,
            tuple([
                lit(i64::from(direction == Direction::Outgoing)),
                edges.column(neighbor)?.expr(),
                edges.column(kind)?.expr(),
                edges.column(identity)?.expr(),
            ]),
        ));
    }
    let rows = source(q, edges, kinds, outgoing.or(incoming))?;
    let rows = q.expand(rows, array_concat(matches).named("neighbor"))?;
    let neighbor = rows.column("neighbor")?;
    let mut values = vec![
        neighbor.field(0).named(neighbor_is_outgoing_column()),
        neighbor.field(1).named(neighbor_id_column()),
        neighbor.field(2).named(neighbor_type_column()),
        neighbor.field(3).named(redaction_id_column(&center.id)),
        relationship.named(relationship_type_column()),
        lit(entity).named(redaction_type_column(&center.id)),
    ];
    values.extend(path);
    q.select(rows, values)
}

fn tag<'a>(
    model: &'a (impl QueryDataModel + ?Sized),
    center: &InputNode,
    property: &str,
    filters: &[InputFilter],
    direction: Direction,
    kinds: &RelationshipSelection,
) -> Option<(&'a str, Vec<Vec<String>>)> {
    let property = model.property(center.entity.as_deref()?, property)?;
    let direction = if direction == Direction::Outgoing {
        DenormalizedDirection::Source
    } else {
        DenormalizedDirection::Target
    };
    let facts = model.denormalized().property(DenormalizedKey {
        property: property.id,
        direction,
    })?;
    if kinds.is_any()
        || kinds.is_empty()
        || !kinds.iter().any(|kind| {
            model
                .graph()
                .relationship_id(kind)
                .is_some_and(|id| facts.relationships.contains(&id))
        })
    {
        return None;
    }
    let values = filters
        .iter()
        .map(|filter| crate::passes::plan::helpers::denorm_tag_values(&facts.tag_key, filter))
        .collect::<Option<_>>()?;
    Some((&facts.edge_column, values))
}

fn condition(
    model: &(impl QueryDataModel + ?Sized),
    rows: &Rows<'_>,
    center: &InputNode,
    direction: Direction,
    kinds: &RelationshipSelection,
) -> Result<Expr> {
    let (identity, kind, _, _) = columns(direction);
    let mut condition = rows
        .column(kind)?
        .eq(center.entity.as_deref().ok_or(Error::Outputs)?);
    if !center.node_ids.is_empty() {
        condition = condition.and(
            rows.column(identity)?
                .in_values(center.node_ids.iter().copied()),
        );
    }
    let mut properties = center.filters.iter().collect::<Vec<_>>();
    properties.sort_by_key(|(name, _)| *name);
    for (property, filters) in properties {
        if let Some((column, groups)) = tag(model, center, property, filters, direction, kinds) {
            let column = rows.column(column)?;
            for values in groups {
                condition = condition.and(column.has_any(values));
            }
        }
    }
    Ok(condition)
}

fn source<'a, M: QueryDataModel + ?Sized>(
    q: &QueryScope<'_, 'a, M>,
    rows: Rows<'a>,
    kinds: &RelationshipSelection,
    mut condition: Expr,
) -> Result<Rows<'a>> {
    if let RelationshipSelection::Kinds(kinds) = kinds {
        let column = rows.column("relationship_kind")?;
        let predicate = if let [kind] = kinds.as_slice() {
            column.eq(kind.as_str())
        } else {
            column.in_values(kinds.iter().cloned())
        };
        condition = condition.and(predicate);
    }
    condition = condition.and(rows.column(ontology::DELETED_COLUMN)?.eq(false));
    q.filter(rows, condition)
}

fn columns(direction: Direction) -> (&'static str, &'static str, &'static str, &'static str) {
    match direction {
        Direction::Outgoing => ("source_id", "source_kind", "target_id", "target_kind"),
        Direction::Incoming => ("target_id", "target_kind", "source_id", "source_kind"),
        Direction::Both => unreachable!(),
    }
}

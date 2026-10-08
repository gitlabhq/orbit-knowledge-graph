use super::super::api::*;
use super::predicates;
use crate::input::{ColumnSelection, Direction, Input, InputNode, QueryType};
use query_data_model::{Endpoint, ForeignKey, QueryDataModel};

pub(super) fn build<'a, M: QueryDataModel + ?Sized>(
    q: &mut QueryScope<'_, 'a, M>,
    input: &Input,
) -> Result<Rows<'a>> {
    let mut rows = matches(q, input)?;
    let mut outputs = Vec::new();
    for node in &input.nodes {
        outputs.extend(node_outputs(q.catalog(), &rows, node)?);
    }
    for (index, relationship) in input.relationships.iter().enumerate() {
        let label = format!("e{index}");
        if rows.column_from(&label, "relationship_kind").is_ok() {
            for (column, suffix) in [
                ("relationship_kind", "type"),
                ("source_id", "src"),
                ("source_kind", "src_type"),
                ("target_id", "dst"),
                ("target_kind", "dst_type"),
            ] {
                outputs.push(
                    rows.column_from(&label, column)?
                        .named(format!("{label}_{suffix}")),
                );
            }
        } else {
            let (source, target) = if relationship.direction == Direction::Incoming {
                (&relationship.to, &relationship.from)
            } else {
                (&relationship.from, &relationship.to)
            };
            outputs.extend([
                rows.column_from(source, "id")?
                    .named(format!("{label}_src")),
                rows.column_from(target, "id")?
                    .named(format!("{label}_dst")),
            ]);
        }
    }
    if let Some(order) = &input.order_by {
        let column = property(q.catalog(), input, &rows, &order.node, &order.property)?;
        rows = q.sort(
            rows,
            [Order {
                value: column.expr(),
                descending: order.direction == crate::input::OrderDirection::Desc,
            }],
        )?;
    }
    let rows = q.limit(rows, input.limit)?;
    q.select(rows, outputs)
}

pub(super) fn matches<'a, M: QueryDataModel + ?Sized>(
    q: &mut QueryScope<'_, 'a, M>,
    input: &Input,
) -> Result<Rows<'a>> {
    let mut rows = if input.relationships.is_empty() {
        let [node] = input.nodes.as_slice() else {
            return Err(Error::Outputs);
        };
        scan_node(q, node)?
    } else if let Some(keys) = foreign_keys(q.catalog(), input)? {
        let first = node(input, &input.relationships[0].from)?;
        let mut rows = scan_node(q, first)?;
        for (relationship, key) in input.relationships.iter().zip(keys) {
            let holder_from = matches!(
                (relationship.direction, key.holder),
                (Direction::Outgoing, Endpoint::Source) | (Direction::Incoming, Endpoint::Target)
            );
            let (holder, target) = if holder_from {
                (&relationship.from, &relationship.to)
            } else {
                (&relationship.to, &relationship.from)
            };
            let holder_column = q
                .catalog()
                .property_column(key.property)
                .ok_or(Error::Column)?;
            let target_column = q
                .catalog()
                .property_column(key.referenced_key)
                .ok_or(Error::Column)?;
            let left = rows.column_from(holder, holder_column);
            let right = rows.column_from(target, target_column);
            rows = match (left, right) {
                (Ok(left), Ok(right)) => q.filter(rows, left.eq(right))?,
                (Ok(left), Err(_)) => {
                    let target = scan_node(q, node(input, target)?)?;
                    let condition = left.eq(target.column(target_column)?);
                    q.join(rows, target, condition)?
                }
                (Err(_), Ok(right)) => {
                    let holder = scan_node(q, node(input, holder)?)?;
                    let condition = holder.column(holder_column)?.eq(right);
                    q.join(rows, holder, condition)?
                }
                _ => return Err(Error::Column),
            };
        }
        rows
    } else {
        let mut combined: Option<Rows<'a>> = None;
        for (index, relationship) in input.relationships.iter().enumerate() {
            let edge = scan_edge(q, input, index)?;
            combined = Some(if let Some(left) = combined {
                let mut conditions = Vec::new();
                let (start, end) = relationship.direction.edge_columns();
                for (previous_index, previous) in input.relationships[..index].iter().enumerate() {
                    let (previous_start, previous_end) = previous.direction.edge_columns();
                    for (alias, column) in [(&relationship.from, start), (&relationship.to, end)] {
                        for (previous_alias, previous_column) in [
                            (&previous.from, previous_start),
                            (&previous.to, previous_end),
                        ] {
                            if alias == previous_alias {
                                conditions.push(
                                    left.column_from(
                                        &format!("e{previous_index}"),
                                        previous_column,
                                    )?
                                    .eq(edge.column(column)?),
                                );
                            }
                        }
                    }
                }
                q.join(
                    left,
                    edge,
                    conditions
                        .into_iter()
                        .reduce(Expr::and)
                        .ok_or(Error::Column)?,
                )?
            } else {
                edge
            });
        }
        let mut rows = combined.ok_or(Error::Outputs)?;
        for node in &input.nodes {
            let (index, column) = input
                .relationships
                .iter()
                .enumerate()
                .find_map(|(index, relationship)| {
                    let (start, end) = relationship.direction.edge_columns();
                    if relationship.from == node.id {
                        Some((index, start))
                    } else if relationship.to == node.id {
                        Some((index, end))
                    } else {
                        None
                    }
                })
                .ok_or(Error::Column)?;
            let endpoint = rows.column_from(&format!("e{index}"), column)?;
            let nodes = scan_node(q, node)?;
            let condition = nodes.column("id")?.eq(endpoint);
            rows = q.join(rows, nodes, condition)?;
        }
        rows
    };
    for node in &input.nodes {
        rows.column_from(&node.id, "id")?;
    }
    for predicate in &input.join_predicates {
        let left = property(
            q.catalog(),
            input,
            &rows,
            &predicate.lhs_node,
            &predicate.lhs_prop,
        )?;
        let right = property(
            q.catalog(),
            input,
            &rows,
            &predicate.rhs_node,
            &predicate.rhs_prop,
        )?;
        rows = q.filter(
            rows,
            predicates::compare(left.expr(), predicate.op, right.expr())?,
        )?;
    }
    Ok(rows)
}

fn scan_node<'a, M: QueryDataModel + ?Sized>(
    q: &mut QueryScope<'_, 'a, M>,
    node: &InputNode,
) -> Result<Rows<'a>> {
    let entity = node.entity.as_deref().ok_or(Error::Outputs)?;
    let table = q
        .catalog()
        .entity_table(entity)
        .ok_or_else(|| Error::Unknown(entity.into()))?;
    let mut rows = q.scan(table, Read::Current)?.labeled(&node.id)?;
    for predicate in predicates::node(q.catalog(), &rows, node)? {
        rows = q.filter(rows, predicate)?;
    }
    Ok(rows)
}

fn scan_edge<'a, M: QueryDataModel + ?Sized>(
    q: &mut QueryScope<'_, 'a, M>,
    input: &Input,
    index: usize,
) -> Result<Rows<'a>> {
    let relationship = &input.relationships[index];
    if relationship.direction == Direction::Both || relationship.hops.max != 1 {
        return Err(Error::Outputs);
    }
    let (source, target) = if relationship.direction == Direction::Incoming {
        (&relationship.to, &relationship.from)
    } else {
        (&relationship.from, &relationship.to)
    };
    let table = q
        .catalog()
        .relationship_table_for_query(relationship.types.as_slice());
    let latest = input.query_type == QueryType::Aggregation && input.relationships.len() == 1;
    let read = if input.relationships.len() == 1 {
        Read::Raw
    } else {
        Read::Current
    };
    let mut rows = q.scan(table, read)?.labeled(format!("e{index}"))?;
    let deleted = rows.column(ontology::DELETED_COLUMN)?;
    let mut predicates = Vec::new();
    if !relationship.types.is_any() {
        let column = rows.column("relationship_kind")?;
        predicates.push(if let [kind] = relationship.types.as_slice() {
            column.eq(kind.as_str())
        } else {
            column.expr().binary(
                Operator::In,
                Expr::literal(
                    orbit_utils::query_types::SqlType::String.to_array(),
                    relationship.types.as_slice().to_vec().into(),
                ),
            )
        });
    }
    for (column, alias) in [("source_kind", source), ("target_kind", target)] {
        predicates.push(
            rows.column(column)?.eq(node(input, alias)?
                .entity
                .as_deref()
                .ok_or(Error::Outputs)?),
        );
    }
    for (alias, column) in [(source, "source_id"), (target, "target_id")] {
        let node = node(input, alias)?;
        if node.id_property == "id" {
            let column = rows.column(column)?;
            if let [id] = node.node_ids.as_slice() {
                predicates.push(column.eq(*id));
            } else if !node.node_ids.is_empty() {
                predicates.push(column.in_values(node.node_ids.iter().copied()));
            }
            if let Some(range) = &node.id_range {
                predicates.push(column.ge(range.start).and(column.le(range.end)));
            }
        }
        let mut properties = node.filters.iter().collect::<Vec<_>>();
        properties.sort_by_key(|(name, _)| *name);
        for (property, filters) in properties {
            if let Some((column, groups)) =
                predicates::edge_tag(q.catalog(), node, property, filters, relationship)
            {
                for values in groups {
                    predicates.push(rows.column(column)?.has_any(values));
                }
            }
        }
    }
    let mut filters = relationship.filters.iter().collect::<Vec<_>>();
    filters.sort_by_key(|(name, _)| *name);
    for (name, filters) in filters {
        for filter in filters {
            predicates.push(predicates::property(
                &rows.column(name)?,
                filter,
                q.catalog().in_sort_key(table, name),
            )?);
        }
    }
    let mut outside = Vec::new();
    for predicate in predicates {
        let mut immutable = true;
        predicate.walk(&mut |value| {
            if let ExprKind::Column(column) = value.kind() {
                immutable &= q.catalog().in_sort_key(table, column.name());
            }
            Ok::<_, Error>(())
        })?;
        if latest && !immutable {
            outside.push(predicate);
        } else {
            rows = q.filter(rows, predicate)?;
        }
    }
    if latest {
        let keys = q
            .catalog()
            .table_sort_key(table)
            .ok_or(Error::Latest)?
            .iter()
            .map(|name| rows.column(name))
            .collect::<Result<Vec<_>>>()?;
        let version = rows.column(ontology::VERSION_COLUMN)?;
        rows = q.latest(rows, keys, version)?;
    }
    rows = q.filter(rows, deleted.eq(false))?;
    for predicate in outside {
        rows = q.filter(rows, predicate)?;
    }
    Ok(rows)
}

fn foreign_keys(
    model: &(impl QueryDataModel + ?Sized),
    input: &Input,
) -> Result<Option<Vec<ForeignKey>>> {
    let mut keys = Vec::new();
    let mut holder = None;
    let mut star = true;
    let mut chain = input.relationships.len() >= 2;
    for relationship in &input.relationships {
        if relationship.direction == Direction::Both
            || relationship.hops.min != 1
            || relationship.hops.max != 1
            || !relationship.filters.is_empty()
        {
            return Ok(None);
        }
        let from = node(input, &relationship.from)?;
        let to = node(input, &relationship.to)?;
        let from_entity = from.entity.as_deref().ok_or(Error::Outputs)?;
        let to_entity = to.entity.as_deref().ok_or(Error::Outputs)?;
        let (source, target) = if relationship.direction == Direction::Incoming {
            (to_entity, from_entity)
        } else {
            (from_entity, to_entity)
        };
        let Some(key) = model.foreign_key(relationship.types.as_slice(), source, target) else {
            return Ok(None);
        };
        let current = if matches!(
            (relationship.direction, key.holder),
            (Direction::Outgoing, Endpoint::Source) | (Direction::Incoming, Endpoint::Target)
        ) {
            &from.id
        } else {
            &to.id
        };
        star &= *holder.get_or_insert(current) == current;
        chain &= [from, to]
            .iter()
            .all(|node| node.node_ids.is_empty() && node.id_range.is_none())
            && (model.entity_is_global(source)
                || model.entity_is_global(target)
                || relationship.types.iter().all(|kind| {
                    model
                        .variant_scope(kind, source, target)
                        .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
                }));
        keys.push(key);
    }
    Ok((star || chain).then_some(keys))
}

pub(super) fn node<'a>(input: &'a Input, alias: &str) -> Result<&'a InputNode> {
    input
        .nodes
        .iter()
        .find(|node| node.id == alias)
        .ok_or(Error::Column)
}

pub(super) fn property(
    model: &(impl QueryDataModel + ?Sized),
    input: &Input,
    rows: &Rows<'_>,
    alias: &str,
    property: &str,
) -> Result<Column> {
    let entity = node(input, alias)?
        .entity
        .as_deref()
        .ok_or(Error::Outputs)?;
    let column = model
        .property_column_named(entity, property)
        .ok_or_else(|| Error::Unknown(property.into()))?;
    rows.column_from(alias, column)
}

pub(super) fn node_outputs(
    model: &(impl QueryDataModel + ?Sized),
    rows: &Rows<'_>,
    node: &InputNode,
) -> Result<Vec<Named>> {
    let entity = node.entity.as_deref().ok_or(Error::Outputs)?;
    let Some(ColumnSelection::List(properties)) = &node.columns else {
        return Err(Error::Outputs);
    };
    properties
        .iter()
        .filter_map(|property| {
            model.property_column_named(entity, property).map(|column| {
                rows.column_from(&node.id, column)
                    .map(|column| column.named(format!("{}_{property}", node.id)))
            })
        })
        .collect()
}

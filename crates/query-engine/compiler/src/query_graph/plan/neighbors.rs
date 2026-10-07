use super::*;
use crate::constants::*;
use crate::input::{Direction, Input, InputFilter, InputNode, RelationshipSelection};
use query_data_model::{DenormalizedDirection, DenormalizedKey};

struct NeighborRoute<'a> {
    direction: Direction,
    tables: Vec<&'a str>,
}

struct NeighborCenter<'a> {
    node: &'a InputNode,
    entity: &'a str,
    redaction: &'a str,
    needs_scan: bool,
    has_path: bool,
}

struct NeighborValues<'a> {
    identity: Expression<'a>,
    authorization: Expression<'a>,
    neighbor: Expression<'a>,
    kind: Expression<'a>,
    relationship: Expression<'a>,
    outgoing: Expression<'a>,
    path: Option<Expression<'a>>,
}

impl<'a> NeighborValues<'a> {
    fn outputs(self, center: &NeighborCenter<'_>, fused: bool) -> Vec<(String, Expression<'a>)> {
        let neighbor = (neighbor_id_column().into(), self.neighbor);
        let kind = (neighbor_type_column().into(), self.kind);
        let relationship = (relationship_type_column().into(), self.relationship);
        let outgoing = (neighbor_is_outgoing_column().into(), self.outgoing);
        let authorization = (redaction_id_column(&center.node.id), self.authorization);
        let mut outputs = if fused {
            vec![outgoing, neighbor, kind, authorization, relationship]
        } else {
            vec![neighbor, kind, relationship, outgoing, authorization]
        };
        outputs.push((
            redaction_type_column(&center.node.id),
            Expression::Text(center.entity.into()),
        ));
        if center.redaction != "id" {
            outputs.push((primary_key_column(&center.node.id), self.identity));
        }
        if let Some(path) = self.path {
            outputs.push((traversal_path_column(&center.node.id), path));
        }
        outputs
    }
}

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, LatestRows<'a>> {
    pub(super) fn neighbors(&mut self, input: &Input) -> Result<BlockId> {
        let [node] = input.nodes.as_slice() else {
            return Err(GraphError::UnsupportedInput(
                "neighbors requires one center".into(),
            ));
        };
        if input.order_by.is_some() || !input.join_predicates.is_empty() {
            return Err(GraphError::UnsupportedInput(
                "neighbor ordering or comparisons".into(),
            ));
        }
        let config = input.neighbors.as_ref().ok_or(GraphError::MissingOutput)?;
        let entity = node
            .entity
            .as_deref()
            .and_then(|name| self.catalog.entity(name))
            .ok_or(GraphError::MissingOutput)?;
        let routes = self.neighbor_routes(entity.id, config.direction, &config.rel_types);
        let covered = node.filters.iter().all(|(property, filters)| {
            routes.iter().all(|route| {
                self.neighbor_tag(node, property, filters, route.direction, &config.rel_types)
                    .is_some()
            })
        });
        let redaction = self.catalog.redaction_id_column(entity.id).unwrap_or("id");
        let center = NeighborCenter {
            node,
            entity: &entity.name,
            redaction,
            needs_scan: !covered
                || node.id_range.is_some()
                || redaction != "id"
                || self.requires_authorization_scan(&entity.name),
            has_path: self.catalog.entity_has_traversal_path(&entity.name),
        };
        if config.direction == Direction::Both
            && !center.needs_scan
            && routes
                .iter()
                .all(|route| route.tables.len() == 1 && route.tables[0] == routes[0].tables[0])
        {
            return self.fused_neighbors(
                &center,
                &config.rel_types,
                routes[0].tables[0],
                input.limit,
            );
        }
        let mut directions = Vec::new();
        for route in routes {
            let mut arms = Vec::new();
            for table in &route.tables {
                arms.push(self.directional_neighbors(
                    &center,
                    &config.rel_types,
                    route.direction,
                    table,
                    if route.tables.len() == 1 { "e" } else { "_e" },
                )?);
            }
            directions.push(self.combine_neighbors(arms)?);
        }
        let body = self.combine_neighbors(directions)?;
        self.page_neighbors(body, input.limit)
    }

    fn neighbor_routes(
        &self,
        entity: query_data_model::EntityId,
        direction: Direction,
        kinds: &RelationshipSelection,
    ) -> Vec<NeighborRoute<'a>> {
        [Direction::Outgoing, Direction::Incoming]
            .into_iter()
            .filter(|candidate| direction == Direction::Both || direction == *candidate)
            .map(|direction| {
                let mut tables = self
                    .catalog
                    .graph()
                    .relationships()
                    .filter(|relationship| kinds.matches(&relationship.name))
                    .filter_map(|relationship| self.catalog.relationship_route(&relationship.name))
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
                    tables = self
                        .catalog
                        .graph()
                        .relationships()
                        .filter(|relationship| kinds.matches(&relationship.name))
                        .filter_map(|relationship| {
                            self.catalog.relationship_table(&relationship.name)
                        })
                        .collect();
                }
                if tables.is_empty() {
                    tables.push(self.catalog.default_edge_table());
                }
                tables.sort_unstable();
                tables.dedup();
                NeighborRoute { direction, tables }
            })
            .collect()
    }

    fn directional_neighbors(
        &mut self,
        center: &NeighborCenter<'_>,
        kinds: &RelationshipSelection,
        direction: Direction,
        table: &'a str,
        hint: &str,
    ) -> Result<BlockId> {
        let block = self.query();
        let edge = self.scan(block, table, hint)?;
        let condition = self.neighbor_condition(edge, center.node, direction, kinds)?;
        let mut operation = self.neighbor_source(edge, kinds, condition)?;
        if direction == Direction::Incoming
            && !center.node.node_ids.is_empty()
            && let Some((table, key)) = self
                .catalog
                .traversal_path_lookup(center.entity, ontology::TraversalPathKind::Id)
        {
            let lookup = self.query_in(block)?;
            let scan = self.scan(lookup, table, "_tpd")?;
            let source = self.filter_relation(
                self.read_relation(scan, ReadMode::Raw)?,
                Expression::membership(self.stored_column(scan, key)?, &center.node.node_ids),
            )?;
            let source = self.filter_relation(
                source,
                Expression::equal(
                    Expression::Column(self.stored_column(scan, ontology::DELETED_COLUMN)?),
                    Expression::Boolean(false),
                ),
            )?;
            let projection = self.project_values(
                source,
                [(
                    ontology::TRAVERSAL_PATH_COLUMN.into(),
                    Expression::Column(self.stored_column(scan, ontology::TRAVERSAL_PATH_COLUMN)?),
                )],
            )?;
            let output = projection
                .outputs()
                .next()
                .ok_or(GraphError::EmptyProjection)?
                .0;
            self.finish_query(projection)?;
            let relation = self.derive(block, lookup, "_tpd")?;
            operation = self.membership_relation(
                operation,
                self.stored_column(edge, ontology::TRAVERSAL_PATH_COLUMN)?,
                self.output_column(relation, output)?,
            )?;
        }
        let (identity, _, neighbor, kind) = neighbor_columns(direction);
        let identity = self.stored_column(edge, identity)?;
        let authorization = if center.needs_scan {
            let table = self
                .catalog
                .entity_table(center.entity)
                .ok_or(GraphError::MissingOutput)?;
            let scan = self.scan(block, table, &center.node.id)?;
            self.bind_scan(scan, ScanInput::Node(0))?;
            let source = self.materialize_relation(self.node_source(scan, center.node)?, scan)?;
            operation = self.join_relations(
                operation,
                source,
                JoinKind::Inner,
                Expression::equal(
                    Expression::Column(identity),
                    Expression::Column(self.stored_column(scan, "id")?),
                ),
            )?;
            self.stored_column(scan, center.redaction)?
        } else {
            identity
        };
        let values = NeighborValues {
            identity: Expression::Column(identity),
            authorization: Expression::Column(authorization),
            neighbor: Expression::Column(self.stored_column(edge, neighbor)?),
            kind: Expression::Column(self.stored_column(edge, kind)?),
            relationship: Expression::Column(self.stored_column(edge, "relationship_kind")?),
            outgoing: Expression::Integer(i64::from(direction == Direction::Outgoing)),
            path: if center.has_path {
                Some(Expression::Column(
                    self.stored_column(edge, ontology::TRAVERSAL_PATH_COLUMN)?,
                ))
            } else {
                None
            },
        };
        let projection = self.project_values(operation, values.outputs(center, false))?;
        self.finish_query(projection)
    }

    fn fused_neighbors(
        &mut self,
        center: &NeighborCenter<'_>,
        kinds: &RelationshipSelection,
        table: &'a str,
        limit: u32,
    ) -> Result<BlockId> {
        let inner = self.query();
        let edge = self.scan(inner, table, "e")?;
        let outgoing = self.neighbor_condition(edge, center.node, Direction::Outgoing, kinds)?;
        let incoming = self.neighbor_condition(edge, center.node, Direction::Incoming, kinds)?;
        let operation = self.neighbor_source(
            edge,
            kinds,
            Expression::Or(Box::new(outgoing.clone()), Box::new(incoming.clone())),
        )?;
        let mut arms = Vec::new();
        for (direction, condition) in [
            (Direction::Outgoing, outgoing),
            (Direction::Incoming, incoming),
        ] {
            let (identity, _, neighbor, kind) = neighbor_columns(direction);
            arms.push(Expression::Keep {
                condition: Box::new(condition),
                value: Box::new(Expression::Tuple(vec![
                    Expression::Integer(i64::from(direction == Direction::Outgoing)),
                    Expression::Column(self.stored_column(edge, neighbor)?),
                    Expression::Column(self.stored_column(edge, kind)?),
                    Expression::Column(self.stored_column(edge, identity)?),
                ])),
            });
        }
        let mut projection = self.project_values(
            operation,
            [("_gkg_arm_row".into(), Expression::Concat(arms))],
        )?;
        let rows = projection
            .outputs()
            .next()
            .ok_or(GraphError::EmptyProjection)?
            .0;
        let relationship = self.append_projection(
            &mut projection,
            relationship_type_column(),
            Expression::Column(self.stored_column(edge, "relationship_kind")?),
        )?;
        let path = if center.has_path {
            Some(self.append_projection(
                &mut projection,
                traversal_path_column(&center.node.id),
                Expression::Column(self.stored_column(edge, ontology::TRAVERSAL_PATH_COLUMN)?),
            )?)
        } else {
            None
        };
        self.finish_query(projection)?;
        let root = self.query();
        let source = self.derive(root, inner, "_gkg_fused")?;
        let row = self.output_column(source, rows)?;
        let operation = self.expand_relation(self.read_relation(source, ReadMode::Raw)?, row)?;
        let field = |index| Expression::Field {
            tuple: Box::new(Expression::Column(row)),
            index,
        };
        let values = NeighborValues {
            identity: field(3),
            authorization: field(3),
            neighbor: field(1),
            kind: field(2),
            outgoing: field(0),
            relationship: Expression::Column(self.output_column(source, relationship)?),
            path: path
                .map(|output| self.output_column(source, output).map(Expression::Column))
                .transpose()?,
        };
        let projection = self.project_values(
            self.limit_relation(operation, limit)?,
            values.outputs(center, true),
        )?;
        self.finish_query(projection)
    }

    fn neighbor_tag(
        &self,
        center: &InputNode,
        property: &str,
        filters: &[InputFilter],
        direction: Direction,
        kinds: &RelationshipSelection,
    ) -> Option<(&'a str, Vec<Vec<String>>)> {
        let property = self.catalog.property(center.entity.as_deref()?, property)?;
        let direction = if direction == Direction::Outgoing {
            DenormalizedDirection::Source
        } else {
            DenormalizedDirection::Target
        };
        let facts = self.catalog.denormalized().property(DenormalizedKey {
            property: property.id,
            direction,
        })?;
        if kinds.is_any()
            || kinds.is_empty()
            || !kinds.iter().any(|kind| {
                self.catalog
                    .graph()
                    .relationship_id(kind)
                    .is_some_and(|id| facts.relationships.contains(&id))
            })
        {
            return None;
        }
        Some((
            &facts.edge_column,
            filters
                .iter()
                .map(|filter| {
                    crate::passes::plan::helpers::denorm_tag_values(&facts.tag_key, filter)
                })
                .collect::<Option<_>>()?,
        ))
    }

    fn neighbor_condition(
        &self,
        edge: RelationId,
        center: &InputNode,
        direction: Direction,
        kinds: &RelationshipSelection,
    ) -> Result<Expression<'a>> {
        let (identity, kind, _, _) = neighbor_columns(direction);
        let mut condition = Expression::equal(
            Expression::Column(self.stored_column(edge, kind)?),
            Expression::Text(center.entity.clone().ok_or(GraphError::MissingOutput)?),
        );
        if !center.node_ids.is_empty() {
            condition = Expression::And(
                Box::new(condition),
                Box::new(Expression::membership(
                    self.stored_column(edge, identity)?,
                    &center.node_ids,
                )),
            );
        }
        let mut filters = center.filters.iter().collect::<Vec<_>>();
        filters.sort_by_key(|(name, _)| *name);
        for (property, filters) in filters {
            if let Some((column, groups)) =
                self.neighbor_tag(center, property, filters, direction, kinds)
            {
                for values in groups {
                    let predicate = if values.is_empty() {
                        Expression::Boolean(false)
                    } else {
                        Expression::HasAny(
                            Box::new(Expression::Column(self.stored_column(edge, column)?)),
                            Box::new(Expression::Array(
                                values.into_iter().map(Expression::Text).collect(),
                            )),
                        )
                    };
                    condition = Expression::And(Box::new(condition), Box::new(predicate));
                }
            }
        }
        Ok(condition)
    }

    fn neighbor_source(
        &self,
        edge: RelationId,
        kinds: &RelationshipSelection,
        condition: Expression<'a>,
    ) -> Result<PhysicalOperation<'a>> {
        let mut operation =
            self.filter_relation(self.read_relation(edge, ReadMode::Raw)?, condition)?;
        if let RelationshipSelection::Kinds(kinds) = kinds {
            let column = Expression::Column(self.stored_column(edge, "relationship_kind")?);
            let predicate = if let [kind] = kinds.as_slice() {
                Expression::equal(column, Expression::Text(kind.clone()))
            } else {
                Expression::In(
                    Box::new(column),
                    Box::new(Expression::Strings(kinds.clone())),
                )
            };
            operation = self.filter_relation(operation, predicate)?;
        }
        self.filter_relation(
            operation,
            Expression::equal(
                Expression::Column(self.stored_column(edge, ontology::DELETED_COLUMN)?),
                Expression::Boolean(false),
            ),
        )
    }

    fn combine_neighbors(&mut self, arms: Vec<BlockId>) -> Result<BlockId> {
        let first = *arms.first().ok_or(GraphError::EmptyProjection)?;
        if arms.len() == 1 {
            return Ok(first);
        }
        let labels = self
            .outputs(first)?
            .map(|output| self.output_label(output).map(str::to_owned))
            .collect::<Result<_>>()?;
        self.union_all(arms, labels)
    }

    fn page_neighbors(&mut self, body: BlockId, limit: u32) -> Result<BlockId> {
        let root = self.query();
        let relation = self.derive(root, body, "neighbors")?;
        let outputs = self
            .outputs(body)?
            .map(|output| {
                Ok((
                    self.output_label(output)?.to_owned(),
                    Expression::Column(self.output_column(relation, output)?),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let operation = self.limit_relation(self.read_relation(relation, ReadMode::Raw)?, limit)?;
        let projection = self.project_values(operation, outputs)?;
        self.finish_query(projection)
    }
}

fn neighbor_columns(
    direction: Direction,
) -> (&'static str, &'static str, &'static str, &'static str) {
    match direction {
        Direction::Outgoing => ("source_id", "source_kind", "target_id", "target_kind"),
        Direction::Incoming => ("target_id", "target_kind", "source_id", "source_kind"),
        Direction::Both => unreachable!(),
    }
}

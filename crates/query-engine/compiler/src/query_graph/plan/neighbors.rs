use super::*;
use crate::constants::*;
use crate::input::{Direction, Input, InputNode};
use query_data_model::{DenormalizedDirection, DenormalizedKey};

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, PhysicalOperation<'catalog>>
{
    pub(super) fn neighbors(&mut self, input: &Input) -> Result<BlockId> {
        let [center] = input.nodes.as_slice() else {
            return Err(GraphError::UnsupportedInput(
                "neighbors requires one center".into(),
            ));
        };
        let config = input.neighbors.as_ref().ok_or(GraphError::MissingOutput)?;
        if input.order_by.is_some() || !input.join_predicates.is_empty() {
            return Err(GraphError::UnsupportedInput(
                "neighbor ordering or comparisons".into(),
            ));
        }
        let entity = center
            .entity
            .as_deref()
            .and_then(|name| self.catalog.entity(name))
            .ok_or(GraphError::MissingOutput)?;
        let redaction = self.catalog.redaction_id_column(entity.id).unwrap_or("id");
        let mut routes = Vec::new();
        for direction in [Direction::Outgoing, Direction::Incoming] {
            if config.direction != Direction::Both && config.direction != direction {
                continue;
            }
            let mut tables = self
                .catalog
                .graph()
                .relationships()
                .filter(|relationship| config.rel_types.matches(&relationship.name))
                .filter_map(|relationship| self.catalog.relationship_route(&relationship.name))
                .filter(|route| {
                    if direction == Direction::Outgoing {
                        route.has_source(entity.id)
                    } else {
                        route.has_target(entity.id)
                    }
                })
                .map(|route| route.table)
                .collect::<Vec<_>>();
            tables.sort_unstable();
            tables.dedup();
            if tables.is_empty() {
                tables = self
                    .catalog
                    .graph()
                    .relationships()
                    .filter(|relationship| config.rel_types.matches(&relationship.name))
                    .filter_map(|relationship| self.catalog.relationship_table(&relationship.name))
                    .collect();
                tables.sort_unstable();
                tables.dedup();
            }
            if tables.is_empty() {
                tables.push(self.catalog.default_edge_table());
            }
            routes.push((direction, tables));
        }
        let covered = center.filters.iter().all(|(property, filters)| {
            routes.iter().all(|(direction, _)| {
                self.neighbor_tag(center, property, filters, *direction, &config.rel_types)
                    .is_some()
            })
        });
        let center_filter = !covered || center.id_range.is_some();
        let fused = config.direction == Direction::Both
            && !center_filter
            && redaction == "id"
            && !self.requires_authorization_scan(&entity.name)
            && routes
                .iter()
                .all(|(_, tables)| tables.len() == 1 && tables[0] == routes[0].1[0]);
        if fused {
            return self.fused_neighbors(input, center, routes[0].1[0]);
        }
        let mut directions = Vec::new();
        for (direction, tables) in routes {
            let mut arms = Vec::new();
            for table in &tables {
                let block = self.select(PhysicalOperation::One);
                let edge = self.scan(block, table, if tables.len() > 1 { "_e" } else { "e" })?;
                let mut operation = PhysicalOperation::source(edge)
                    .filter(self.neighbor_condition(edge, center, direction, &config.rel_types)?);
                operation = self.neighbor_edge_filters(edge, &config.rel_types, operation)?;
                if direction == Direction::Incoming
                    && !center.node_ids.is_empty()
                    && let Some((table, key_column)) = self
                        .catalog
                        .traversal_path_lookup(&entity.name, ontology::TraversalPathKind::Id)
                {
                    let keys = self.select(PhysicalOperation::One);
                    let scan = self.scan(keys, table, "_tpd")?;
                    *self.operation_mut(keys)? = PhysicalOperation::source(scan)
                        .filter(Expression::membership(
                            self.stored_column(scan, key_column)?,
                            &center.node_ids,
                        ))
                        .filter(Expression::equal(
                            Expression::Column(self.stored_column(scan, ontology::DELETED_COLUMN)?),
                            Expression::Boolean(false),
                        ));
                    let output = self.project(
                        keys,
                        ontology::TRAVERSAL_PATH_COLUMN,
                        Expression::Column(
                            self.stored_column(scan, ontology::TRAVERSAL_PATH_COLUMN)?,
                        ),
                    )?;
                    let reference = self.derive(block, keys, "_tpd")?;
                    operation = operation.membership(
                        self.stored_column(edge, ontology::TRAVERSAL_PATH_COLUMN)?,
                        self.output_column(reference, output)?,
                    );
                }
                let (center_id, _, neighbor_id, neighbor_kind) = neighbor_columns(direction);
                let identity = self.stored_column(edge, center_id)?;
                let mut authorization = identity;
                if center_filter
                    || redaction != "id"
                    || self.requires_authorization_scan(&entity.name)
                {
                    let scan = self.scan(
                        block,
                        self.catalog
                            .entity_table(&entity.name)
                            .ok_or(GraphError::MissingOutput)?,
                        &center.id,
                    )?;
                    self.bind_scan(scan, ScanInput::Node(0))?;
                    operation = operation.join(
                        self.node_source(scan, center)?.materialize(scan),
                        Expression::equal(
                            Expression::Column(identity),
                            Expression::Column(self.stored_column(scan, "id")?),
                        ),
                    );
                    authorization = self.stored_column(scan, redaction)?;
                }
                for (label, value) in [
                    (
                        neighbor_id_column().to_owned(),
                        Expression::Column(self.stored_column(edge, neighbor_id)?),
                    ),
                    (
                        neighbor_type_column().to_owned(),
                        Expression::Column(self.stored_column(edge, neighbor_kind)?),
                    ),
                    (
                        relationship_type_column().to_owned(),
                        Expression::Column(self.stored_column(edge, "relationship_kind")?),
                    ),
                    (
                        neighbor_is_outgoing_column().to_owned(),
                        Expression::Integer(i64::from(direction == Direction::Outgoing)),
                    ),
                    (
                        redaction_id_column(&center.id),
                        Expression::Column(authorization),
                    ),
                    (
                        redaction_type_column(&center.id),
                        Expression::Text(entity.name.clone()),
                    ),
                ] {
                    self.project(block, label, value)?;
                }
                if redaction != "id" {
                    self.project(
                        block,
                        primary_key_column(&center.id),
                        Expression::Column(identity),
                    )?;
                }
                if self.catalog.entity_has_traversal_path(&entity.name) {
                    self.project(
                        block,
                        traversal_path_column(&center.id),
                        Expression::Column(
                            self.stored_column(edge, ontology::TRAVERSAL_PATH_COLUMN)?,
                        ),
                    )?;
                }
                *self.operation_mut(block)? = operation;
                arms.push(block);
            }
            directions.push(self.combine_neighbors(arms)?);
        }
        let body = self.combine_neighbors(directions)?;
        self.page_neighbors(body, input.limit)
    }

    fn neighbor_tag(
        &self,
        center: &InputNode,
        property: &str,
        filters: &[crate::input::InputFilter],
        direction: Direction,
        kinds: &crate::input::RelationshipSelection,
    ) -> Option<(&'catalog str, Vec<Vec<String>>)> {
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
        kinds: &crate::input::RelationshipSelection,
    ) -> Result<Expression<'catalog>> {
        let (id, kind, _, _) = neighbor_columns(direction);
        let mut parts = vec![Expression::equal(
            Expression::Column(self.stored_column(edge, kind)?),
            Expression::Text(center.entity.clone().ok_or(GraphError::MissingOutput)?),
        )];
        if !center.node_ids.is_empty() {
            parts.push(Expression::membership(
                self.stored_column(edge, id)?,
                &center.node_ids,
            ));
        }
        let mut filters = center.filters.iter().collect::<Vec<_>>();
        filters.sort_by_key(|(name, _)| *name);
        for (property, filters) in filters {
            if let Some((column, values)) =
                self.neighbor_tag(center, property, filters, direction, kinds)
            {
                for values in values {
                    parts.push(if values.is_empty() {
                        Expression::Boolean(false)
                    } else {
                        Expression::HasAny(
                            Box::new(Expression::Column(self.stored_column(edge, column)?)),
                            Box::new(Expression::Array(
                                values.into_iter().map(Expression::Text).collect(),
                            )),
                        )
                    });
                }
            }
        }
        Ok(parts
            .into_iter()
            .reduce(|left, right| Expression::And(Box::new(left), Box::new(right)))
            .expect("center kind"))
    }

    fn neighbor_edge_filters(
        &self,
        edge: RelationId,
        kinds: &crate::input::RelationshipSelection,
        mut operation: PhysicalOperation<'catalog>,
    ) -> Result<PhysicalOperation<'catalog>> {
        if let crate::input::RelationshipSelection::Kinds(kinds) = kinds {
            let column = Expression::Column(self.stored_column(edge, "relationship_kind")?);
            operation = operation.filter(if let [kind] = kinds.as_slice() {
                Expression::equal(column, Expression::Text(kind.clone()))
            } else {
                Expression::In(
                    Box::new(column),
                    Box::new(Expression::Strings(kinds.to_vec())),
                )
            });
        }
        Ok(operation.filter(Expression::equal(
            Expression::Column(self.stored_column(edge, ontology::DELETED_COLUMN)?),
            Expression::Boolean(false),
        )))
    }

    fn fused_neighbors(
        &mut self,
        input: &Input,
        center: &InputNode,
        table: &'catalog str,
    ) -> Result<BlockId> {
        let kinds = &input
            .neighbors
            .as_ref()
            .ok_or(GraphError::MissingOutput)?
            .rel_types;
        let inner = self.select(PhysicalOperation::One);
        let edge = self.scan(inner, table, "e")?;
        let outgoing = self.neighbor_condition(edge, center, Direction::Outgoing, kinds)?;
        let incoming = self.neighbor_condition(edge, center, Direction::Incoming, kinds)?;
        *self.operation_mut(inner)? = self.neighbor_edge_filters(
            edge,
            kinds,
            PhysicalOperation::source(edge).filter(Expression::Or(
                Box::new(outgoing.clone()),
                Box::new(incoming.clone()),
            )),
        )?;
        let mut arrays = Vec::new();
        for (direction, condition) in [
            (Direction::Outgoing, outgoing),
            (Direction::Incoming, incoming),
        ] {
            let (center_id, _, neighbor_id, neighbor_kind) = neighbor_columns(direction);
            arrays.push(Expression::Keep {
                condition: Box::new(condition),
                value: Box::new(Expression::Tuple(vec![
                    Expression::Integer(i64::from(direction == Direction::Outgoing)),
                    Expression::Column(self.stored_column(edge, neighbor_id)?),
                    Expression::Column(self.stored_column(edge, neighbor_kind)?),
                    Expression::Column(self.stored_column(edge, center_id)?),
                ])),
            });
        }
        let rows = self.project(inner, "_gkg_arm_row", Expression::Concat(arrays))?;
        let kind = self.project(
            inner,
            relationship_type_column(),
            Expression::Column(self.stored_column(edge, "relationship_kind")?),
        )?;
        let has_path = self
            .catalog
            .entity_has_traversal_path(center.entity.as_deref().ok_or(GraphError::MissingOutput)?);
        let path = if has_path {
            Some(self.project(
                inner,
                traversal_path_column(&center.id),
                Expression::Column(self.stored_column(edge, ontology::TRAVERSAL_PATH_COLUMN)?),
            )?)
        } else {
            None
        };
        let root = self.select(PhysicalOperation::One);
        let source = self.derive(root, inner, "_gkg_fused")?;
        let row = self.output_column(source, rows)?;
        *self.operation_mut(root)? = PhysicalOperation::source(source)
            .expand(row)
            .limit(input.limit);
        for (label, index) in [
            (neighbor_is_outgoing_column().to_owned(), 0),
            (neighbor_id_column().to_owned(), 1),
            (neighbor_type_column().to_owned(), 2),
            (redaction_id_column(&center.id), 3),
        ] {
            self.project(
                root,
                label,
                Expression::Field {
                    tuple: Box::new(Expression::Column(row)),
                    index,
                },
            )?;
        }
        self.project(
            root,
            relationship_type_column(),
            Expression::Column(self.output_column(source, kind)?),
        )?;
        self.project(
            root,
            redaction_type_column(&center.id),
            Expression::Text(center.entity.clone().ok_or(GraphError::MissingOutput)?),
        )?;
        if let Some(path) = path {
            self.project(
                root,
                traversal_path_column(&center.id),
                Expression::Column(self.output_column(source, path)?),
            )?;
        }
        Ok(root)
    }

    fn combine_neighbors(&mut self, arms: Vec<BlockId>) -> Result<BlockId> {
        let first = *arms.first().ok_or(GraphError::EmptyProjection)?;
        if arms.len() == 1 {
            return Ok(first);
        }
        let labels = self
            .outputs(first)?
            .map(|output| self.output_label(output).map(str::to_owned))
            .collect::<Result<Vec<_>>>()?;
        self.union_all(arms, labels)
    }

    fn page_neighbors(&mut self, body: BlockId, limit: u32) -> Result<BlockId> {
        let root = self.select(PhysicalOperation::One);
        let relation = self.derive(root, body, "neighbors")?;
        let outputs = self.outputs(body)?.collect::<Vec<_>>();
        for output in outputs {
            self.project(
                root,
                self.output_label(output)?.to_owned(),
                Expression::Column(self.output_column(relation, output)?),
            )?;
        }
        *self.operation_mut(root)? = PhysicalOperation::source(relation).limit(limit);
        Ok(root)
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

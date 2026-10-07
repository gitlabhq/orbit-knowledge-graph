use super::*;
use crate::input::{Direction, Input, InputRelationship, OrderDirection};

struct HopStep<'a> {
    relation: RelationId,
    start: ColumnRef<'a>,
    end: ColumnRef<'a>,
    end_kind: ColumnRef<'a>,
    live: Expression<'a>,
    kind: Expression<'a>,
}

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, LatestRows<'a>> {
    pub(super) fn variable_hops(&mut self, input: &Input) -> Result<BlockId> {
        let [relationship] = input.relationships.as_slice() else {
            return self.access(input);
        };
        if relationship.direction == Direction::Both || !relationship.filters.is_empty() {
            return Err(GraphError::UnsupportedInput(
                "variable-hop direction or comparisons".into(),
            ));
        }
        let root = self.query();
        let edge = self.hop_relation(root, relationship, 0)?;
        let mut operation = self.filter_relation(
            self.read_relation(edge, ReadMode::Raw)?,
            Expression::equal(
                Expression::Column(self.column(edge, ontology::DELETED_COLUMN)?),
                Expression::Boolean(false),
            ),
        )?;
        let (start, end) = relationship.direction.edge_columns();
        let mut outputs = Vec::new();
        for (alias, endpoint) in [(&relationship.from, start), (&relationship.to, end)] {
            let (index, node) = input
                .nodes
                .iter()
                .enumerate()
                .find(|(_, node)| node.id == *alias)
                .ok_or(GraphError::MissingOutput)?;
            let entity = node.entity.as_deref().ok_or(GraphError::MissingOutput)?;
            if node.id_property != "id" {
                return Err(GraphError::UnsupportedInput(
                    "variable-hop alternate identity".into(),
                ));
            }
            let identity = self.column(edge, endpoint)?;
            let kind = if endpoint == "source_id" {
                "source_kind"
            } else {
                "target_kind"
            };
            operation = self.filter_relation(
                operation,
                Expression::equal(
                    Expression::Column(self.column(edge, kind)?),
                    Expression::Text(entity.into()),
                ),
            )?;
            for predicate in Expression::identity_predicates(identity, node) {
                operation = self.filter_relation(operation, predicate)?;
            }
            let needs_scan = !node.filters.is_empty()
                || input
                    .order_by
                    .as_ref()
                    .is_some_and(|order| order.node == node.id)
                || input.join_predicates.iter().any(|predicate| {
                    predicate.lhs_node == node.id || predicate.rhs_node == node.id
                })
                || self.requires_authorization_scan(entity);
            if needs_scan {
                let table = self
                    .catalog
                    .entity_table(entity)
                    .ok_or(GraphError::MissingOutput)?;
                let scan = self.scan(root, table, alias)?;
                self.bind_scan(scan, ScanInput::Node(index))?;
                let source = self.materialize_relation(self.node_source(scan, node)?, scan)?;
                operation = self.join_relations(
                    operation,
                    source,
                    JoinKind::Inner,
                    Expression::equal(
                        Expression::Column(self.stored_column(scan, "id")?),
                        Expression::Column(identity),
                    ),
                )?;
            }
            outputs.push((format!("{alias}_id"), Expression::Column(identity)));
        }
        for predicate in &input.join_predicates {
            let left =
                self.hop_node_column(root, input, &predicate.lhs_node, &predicate.lhs_prop)?;
            let right =
                self.hop_node_column(root, input, &predicate.rhs_node, &predicate.rhs_prop)?;
            operation = self.filter_relation(
                operation,
                Expression::Predicate {
                    operator: predicate.op,
                    value: Box::new(Expression::Column(left)),
                    argument: Some(Box::new(Expression::Column(right))),
                    fold_case: false,
                },
            )?;
        }
        for (column, suffix) in [
            ("relationship_kind", "type"),
            ("source_id", "src"),
            ("source_kind", "src_type"),
            ("target_id", "dst"),
            ("target_kind", "dst_type"),
            ("path_nodes", "path_nodes"),
        ] {
            outputs.push((
                format!("hop_e0_{suffix}"),
                Expression::Column(self.column(edge, column)?),
            ));
        }
        if let Some(order) = &input.order_by {
            let column = self.hop_node_column(root, input, &order.node, &order.property)?;
            operation = self.sort_relation(
                operation,
                vec![(column, order.direction == OrderDirection::Desc)],
            )?;
        }
        let projection =
            self.project_values(self.limit_relation(operation, input.limit)?, outputs)?;
        self.finish_query(projection)
    }

    fn hop_node_column(
        &self,
        root: BlockId,
        input: &Input,
        alias: &str,
        property: &str,
    ) -> Result<ColumnRef<'a>> {
        let index = input
            .nodes
            .iter()
            .position(|node| node.id == alias)
            .ok_or(GraphError::MissingOutput)?;
        self.column(self.input_node(root, index)?, property)
    }

    pub(super) fn hop_relation(
        &mut self,
        root: BlockId,
        relationship: &InputRelationship,
        input_index: usize,
    ) -> Result<RelationId> {
        let [kind] = relationship.types.as_slice() else {
            return Err(GraphError::UnsupportedInput(
                "variable-hop relationship kinds".into(),
            ));
        };
        let table = self
            .catalog
            .relationship_table_for_query(relationship.types.as_slice());
        let mut arms = Vec::new();
        for depth in relationship.hops.min.max(1)..=relationship.hops.max {
            let block = self.query_in(root)?;
            let first =
                self.hop_step(block, table, relationship.direction, input_index, kind, 1)?;
            let source = self.filter_relation(
                self.read_relation(first.relation, ReadMode::Raw)?,
                first.kind,
            )?;
            let mut operation = self.filter_relation(source, first.live)?;
            let mut path = vec![hop_node(first.end, first.end_kind)];
            let mut last_relation = first.relation;
            let mut last_end = first.end;
            for step in 2..=depth {
                let next = self.hop_step(
                    block,
                    table,
                    relationship.direction,
                    input_index,
                    kind,
                    step,
                )?;
                let endpoint =
                    Expression::equal(Expression::Column(last_end), Expression::Column(next.start));
                let condition = Expression::And(
                    Box::new(Expression::And(Box::new(endpoint), Box::new(next.live))),
                    Box::new(next.kind),
                );
                operation = self.join_relations(
                    operation,
                    self.read_relation(next.relation, ReadMode::Raw)?,
                    JoinKind::Inner,
                    condition,
                )?;
                path.push(hop_node(next.end, next.end_kind));
                last_relation = next.relation;
                last_end = next.end;
            }
            let (source, target, kind_scan) = if relationship.direction == Direction::Incoming {
                (last_relation, first.relation, last_relation)
            } else {
                (first.relation, last_relation, first.relation)
            };
            let mut outputs = Vec::new();
            for (scan, column) in [
                (kind_scan, "relationship_kind"),
                (source, "source_id"),
                (source, "source_kind"),
                (source, "source_tags"),
                (target, "target_id"),
                (target, "target_kind"),
                (target, "target_tags"),
                (first.relation, ontology::DELETED_COLUMN),
                (first.relation, ontology::TRAVERSAL_PATH_COLUMN),
            ] {
                outputs.push((
                    column.into(),
                    Expression::Column(self.stored_column(scan, column)?),
                ));
            }
            outputs.push(("path_nodes".into(), Expression::Array(path)));
            outputs.push(("depth".into(), Expression::Integer(i64::from(depth))));
            let projection = self.project_values(operation, outputs)?;
            arms.push(self.finish_query(projection)?);
        }
        let first = *arms.first().ok_or(GraphError::EmptyProjection)?;
        let labels = self
            .outputs(first)?
            .map(|output| self.output_label(output).map(str::to_owned))
            .collect::<Result<_>>()?;
        let body = self.union_all(arms, labels)?;
        let relation = self.derive(root, body, "e0")?;
        self.bind_scan(relation, ScanInput::Relationship(input_index))?;
        Ok(relation)
    }

    fn hop_step(
        &mut self,
        block: BlockId,
        table: &'a str,
        direction: Direction,
        input_index: usize,
        kind: &str,
        step: u32,
    ) -> Result<HopStep<'a>> {
        let relation = self.scan(block, table, format!("e{step}"))?;
        self.bind_scan(relation, ScanInput::Relationship(input_index))?;
        let (start, end) = direction.edge_columns();
        let end_kind = if direction == Direction::Incoming {
            "source_kind"
        } else {
            "target_kind"
        };
        Ok(HopStep {
            relation,
            start: self.stored_column(relation, start)?,
            end: self.stored_column(relation, end)?,
            end_kind: self.stored_column(relation, end_kind)?,
            live: Expression::equal(
                Expression::Column(self.stored_column(relation, ontology::DELETED_COLUMN)?),
                Expression::Boolean(false),
            ),
            kind: Expression::equal(
                Expression::Column(self.stored_column(relation, "relationship_kind")?),
                Expression::Text(kind.into()),
            ),
        })
    }
}

fn hop_node<'a>(identity: ColumnRef<'a>, kind: ColumnRef<'a>) -> Expression<'a> {
    Expression::Tuple(vec![Expression::Column(identity), Expression::Column(kind)])
}

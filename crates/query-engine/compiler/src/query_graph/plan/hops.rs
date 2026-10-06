use super::*;
use crate::input::{Direction, Input};

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, PhysicalOperation<'catalog>>
{
    pub(super) fn variable_hops(&mut self, input: &Input) -> Result<BlockId> {
        let [relationship] = input.relationships.as_slice() else {
            return Err(GraphError::UnsupportedInput(
                "variable hops in multi-relationship queries".into(),
            ));
        };
        if relationship.direction == Direction::Both
            || !relationship.filters.is_empty()
            || !input.join_predicates.is_empty()
        {
            return Err(GraphError::UnsupportedInput(
                "variable-hop direction or comparisons".into(),
            ));
        }
        let [kind] = relationship.types.as_slice() else {
            return Err(GraphError::UnsupportedInput(
                "variable-hop relationship kinds".into(),
            ));
        };
        let (start, end) = relationship.direction.edge_columns();
        let end_kind = if relationship.direction == Direction::Incoming {
            "source_kind"
        } else {
            "target_kind"
        };
        let table = self
            .catalog
            .relationship_table_for_query(&relationship.types);
        let mut arms = Vec::new();
        for depth in relationship.hops.min.max(1)..=relationship.hops.max {
            let arm = self.select(PhysicalOperation::One);
            let mut scans = Vec::new();
            let mut operation = PhysicalOperation::One;
            let mut path = Vec::new();
            for step in 1..=depth {
                let scan = self.scan(arm, table, format!("e{step}"))?;
                self.bind_scan(scan, ScanInput::Relationship(0))?;
                let live = Expression::equal(
                    Expression::Column(self.stored_column(scan, ontology::DELETED_COLUMN)?),
                    Expression::Boolean(false),
                );
                let kind = Expression::equal(
                    Expression::Column(self.stored_column(scan, "relationship_kind")?),
                    Expression::Text(kind.clone()),
                );
                operation = if let Some(previous) = scans.last() {
                    let endpoint = Expression::equal(
                        Expression::Column(self.stored_column(*previous, end)?),
                        Expression::Column(self.stored_column(scan, start)?),
                    );
                    operation.join(
                        PhysicalOperation::source(scan),
                        Expression::And(
                            Box::new(Expression::And(Box::new(endpoint), Box::new(live))),
                            Box::new(kind),
                        ),
                    )
                } else {
                    PhysicalOperation::source(scan).filter(kind).filter(live)
                };
                path.push(Expression::Tuple(vec![
                    Expression::Column(self.stored_column(scan, end)?),
                    Expression::Column(self.stored_column(scan, end_kind)?),
                ]));
                scans.push(scan);
            }
            let first = scans[0];
            let last = *scans.last().expect("nonzero depth");
            let (source, target, kind_scan) = if relationship.direction == Direction::Incoming {
                (last, first, last)
            } else {
                (first, last, first)
            };
            for (scan, column) in [
                (kind_scan, "relationship_kind"),
                (source, "source_id"),
                (source, "source_kind"),
                (source, "source_tags"),
                (target, "target_id"),
                (target, "target_kind"),
                (target, "target_tags"),
                (first, "_deleted"),
                (first, "traversal_path"),
            ] {
                self.project(
                    arm,
                    column,
                    Expression::Column(self.stored_column(scan, column)?),
                )?;
            }
            self.project(arm, "path_nodes", Expression::Array(path))?;
            self.project(arm, "depth", Expression::Integer(i64::from(depth)))?;
            *self.operation_mut(arm)? = operation;
            arms.push(arm);
        }
        let first = *arms.first().ok_or(GraphError::EmptyProjection)?;
        let labels = self
            .outputs(first)?
            .map(|output| self.output_label(output).map(str::to_owned))
            .collect::<Result<Vec<_>>>()?;
        let union = self.union_all(arms, labels)?;
        let root = self.select(PhysicalOperation::One);
        let edge = self.derive(root, union, "e0")?;
        self.bind_scan(edge, ScanInput::Relationship(0))?;
        let mut operation = PhysicalOperation::source(edge).filter(Expression::equal(
            Expression::Column(self.column(edge, "_deleted")?),
            Expression::Boolean(false),
        ));
        for (alias, endpoint, kind_column) in [
            (
                &relationship.from,
                start,
                if start == "source_id" {
                    "source_kind"
                } else {
                    "target_kind"
                },
            ),
            (&relationship.to, end, end_kind),
        ] {
            let (index, node) = input
                .nodes
                .iter()
                .enumerate()
                .find(|(_, node)| node.id == *alias)
                .ok_or(GraphError::MissingOutput)?;
            let entity = node.entity.as_deref().ok_or(GraphError::MissingOutput)?;
            let identity = self.column(edge, endpoint)?;
            operation = operation.filter(Expression::equal(
                Expression::Column(self.column(edge, kind_column)?),
                Expression::Text(entity.into()),
            ));
            if node.id_property != "id" {
                return Err(GraphError::UnsupportedInput(
                    "variable-hop alternate identity".into(),
                ));
            }
            for predicate in Expression::identity_predicates(identity, node) {
                operation = operation.filter(predicate);
            }
            let needs_scan = !node.filters.is_empty()
                || input
                    .order_by
                    .as_ref()
                    .is_some_and(|order| order.node == node.id)
                || self
                    .catalog
                    .entity_minimum_access_level(entity)
                    .is_some_and(|level| level > crate::types::DEFAULT_PATH_ACCESS_LEVEL);
            if needs_scan {
                let relation = self.scan(
                    root,
                    self.catalog
                        .entity_table(entity)
                        .ok_or(GraphError::MissingOutput)?,
                    alias,
                )?;
                self.bind_scan(relation, ScanInput::Node(index))?;
                operation = operation.join(
                    self.node_source(relation, node)?.materialize(relation),
                    Expression::equal(
                        Expression::Column(self.stored_column(relation, "id")?),
                        Expression::Column(identity),
                    ),
                );
            }
            self.project(root, format!("{alias}_id"), Expression::Column(identity))?;
        }
        for (column, suffix) in [
            ("relationship_kind", "type"),
            ("source_id", "src"),
            ("source_kind", "src_type"),
            ("target_id", "dst"),
            ("target_kind", "dst_type"),
            ("path_nodes", "path_nodes"),
        ] {
            self.project(
                root,
                format!("hop_e0_{suffix}"),
                Expression::Column(self.column(edge, column)?),
            )?;
        }
        if let Some(order) = &input.order_by {
            let index = input
                .nodes
                .iter()
                .position(|node| node.id == order.node)
                .ok_or(GraphError::MissingOutput)?;
            operation = operation.sort(vec![(
                self.stored_column(self.input_node(root, index)?, &order.property)?,
                order.direction == crate::input::OrderDirection::Desc,
            )]);
        }
        *self.operation_mut(root)? = operation.limit(input.limit);
        Ok(root)
    }
}

use super::*;
use crate::input::{ColumnSelection, Direction, FilterOp, Input, InputNode, QueryType};
use std::collections::HashMap;

pub(super) struct AccessPlan<'input, 'catalog> {
    pub(super) root: BlockId,
    pub(super) relations: HashMap<&'input str, RelationId>,
    pub(super) node_operations: HashMap<&'input str, PhysicalOperation<'catalog>>,
    pub(super) node_predicates: HashMap<&'input str, Vec<Expression<'catalog>>>,
    pub(super) elided: HashSet<&'input str>,
    pub(super) keys: Vec<Option<query_data_model::ForeignKey>>,
    pub(super) star_center: Option<&'input str>,
    pub(super) star: bool,
    pub(super) eligible: bool,
    pub(super) aggregate_condition: Option<Expression<'catalog>>,
}

pub(super) fn entity<'a>(input: &'a Input, alias: &str) -> Result<&'a str> {
    input
        .nodes
        .iter()
        .find(|node| node.id == alias)
        .and_then(|node| node.entity.as_deref())
        .ok_or_else(|| GraphError::UnsupportedInput(format!("unknown entity for {alias}")))
}

pub(super) fn needs_values(input: &Input, node: &InputNode) -> bool {
    input
        .aggregation
        .group_by
        .iter()
        .any(|group| group.node() == node.id)
        || input
            .aggregation
            .metrics
            .iter()
            .any(|metric| metric.expr.node() == node.id && metric.expr.property().is_some())
        || input
            .order_by
            .as_ref()
            .is_some_and(|order| order.node == node.id)
        || input
            .join_predicates
            .iter()
            .any(|predicate| predicate.lhs_node == node.id || predicate.rhs_node == node.id)
}

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, LatestRows<'a>> {
    pub(super) fn access(&mut self, input: &Input) -> Result<BlockId> {
        let (plan, mut operation) = self.plan_access(input)?;
        if !plan.node_operations.is_empty() {
            return Err(GraphError::UnsupportedInput("disconnected nodes".into()));
        }
        for predicate in &input.join_predicates {
            if !matches!(
                predicate.op,
                FilterOp::Eq
                    | FilterOp::Ne
                    | FilterOp::Gt
                    | FilterOp::Lt
                    | FilterOp::Gte
                    | FilterOp::Lte
            ) {
                return Err(GraphError::UnsupportedInput(
                    "cross-node comparison operator".into(),
                ));
            }
            let column = |alias: &str, property: &str| {
                let name = self
                    .catalog
                    .property_column_named(entity(input, alias)?, property)
                    .ok_or(GraphError::MissingOutput)?;
                self.stored_column(
                    *plan.relations.get(alias).ok_or(GraphError::MissingOutput)?,
                    name,
                )
            };
            operation = self.filter_relation(
                operation,
                Expression::Predicate {
                    operator: predicate.op,
                    value: Box::new(Expression::Column(column(
                        &predicate.lhs_node,
                        &predicate.lhs_prop,
                    )?)),
                    argument: Some(Box::new(Expression::Column(column(
                        &predicate.rhs_node,
                        &predicate.rhs_prop,
                    )?))),
                    fold_case: false,
                },
            )?;
        }
        let mut outputs = self.access_node_outputs(input, &plan)?;
        outputs.extend(self.access_edge_outputs(input, &plan)?);
        if let Some(order) = &input.order_by {
            let column =
                self.stored_column(plan.relations[order.node.as_str()], &order.property)?;
            operation = self.sort_relation(
                operation,
                vec![(
                    column,
                    order.direction == crate::input::OrderDirection::Desc,
                )],
            )?;
        }
        let projection =
            self.project_values(self.limit_relation(operation, input.limit)?, outputs)?;
        self.finish_query(projection)
    }

    pub(super) fn plan_access<'input>(
        &mut self,
        input: &'input Input,
    ) -> Result<(AccessPlan<'input, 'a>, PhysicalOperation<'a>)> {
        let mut plan = self.prepare_access(input)?;
        if plan.star {
            self.narrow_star(input, &mut plan)?;
        }
        let operation = if input.relationships.is_empty() && input.nodes.len() == 1 {
            plan.node_operations
                .remove(input.nodes[0].id.as_str())
                .ok_or(GraphError::MissingOutput)?
        } else if plan.eligible || plan.star {
            self.join_foreign_keys(input, &mut plan)?
        } else {
            self.join_edges(input, &mut plan)?
        };
        Ok((plan, operation))
    }

    fn prepare_access<'input>(&mut self, input: &'input Input) -> Result<AccessPlan<'input, 'a>> {
        if !matches!(
            input.query_type,
            QueryType::Traversal | QueryType::Aggregation
        ) || input.nodes.is_empty()
        {
            return Err(GraphError::UnsupportedInput(
                "expected nonempty traversal".into(),
            ));
        }
        let mut plan = AccessPlan {
            root: self.query(),
            relations: HashMap::new(),
            node_operations: HashMap::new(),
            node_predicates: HashMap::new(),
            elided: HashSet::new(),
            keys: Vec::new(),
            star_center: None,
            star: !input.relationships.is_empty(),
            eligible: input.relationships.len() >= 2
                && input.nodes.iter().any(|node| {
                    node.entity
                        .as_deref()
                        .is_some_and(|entity| self.catalog.entity_has_traversal_path(entity))
                }),
            aggregate_condition: None,
        };
        let mut reached = HashSet::new();
        for (index, relationship) in input.relationships.iter().enumerate() {
            let from = entity(input, &relationship.from)?;
            let to = entity(input, &relationship.to)?;
            let (source, target) = if relationship.direction == Direction::Incoming {
                (to, from)
            } else {
                (from, to)
            };
            let key = self
                .catalog
                .foreign_key(relationship.types.as_slice(), source, target);
            let holder = key.as_ref().map(|key| {
                if matches!(
                    (relationship.direction, key.holder),
                    (Direction::Outgoing, query_data_model::Endpoint::Source)
                        | (Direction::Incoming, query_data_model::Endpoint::Target)
                ) {
                    relationship.from.as_str()
                } else {
                    relationship.to.as_str()
                }
            });
            if index == 0 {
                plan.star_center = holder;
            }
            let single_hop = relationship.direction != Direction::Both
                && relationship.hops.min == 1
                && relationship.hops.max == 1
                && relationship.filters.is_empty();
            plan.star &= holder.is_some() && holder == plan.star_center && single_hop;
            let preserving = !relationship.types.is_any()
                && !relationship.types.is_empty()
                && relationship.types.iter().all(|kind| {
                    self.catalog
                        .variant_scope(kind, from, to)
                        .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
                        || self
                            .catalog
                            .variant_scope(kind, to, from)
                            .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
                });
            let point_selective = input
                .nodes
                .iter()
                .filter(|node| node.id == relationship.from || node.id == relationship.to)
                .any(|node| !node.node_ids.is_empty() || node.id_range.is_some());
            plan.eligible &= key.is_some()
                && single_hop
                && !point_selective
                && (preserving
                    || self.catalog.entity_is_global(from)
                    || self.catalog.entity_is_global(to))
                && (index == 0
                    || reached.contains(&relationship.from) != reached.contains(&relationship.to));
            reached.insert(relationship.from.clone());
            reached.insert(relationship.to.clone());
            plan.keys.push(key);
        }
        for (index, node) in input.nodes.iter().enumerate() {
            let entity = node.entity.as_deref().ok_or(GraphError::MissingOutput)?;
            let table = self
                .catalog
                .entity_table(entity)
                .ok_or_else(|| GraphError::UnknownStored(entity.into()))?;
            let needed = node.existence == crate::input::NodeExistence::CurrentRow
                || needs_values(input, node);
            let protected = self.requires_authorization_scan(entity);
            let primary_key_target = input
                .relationships
                .iter()
                .zip(&plan.keys)
                .filter(|(relationship, _)| {
                    relationship.from == node.id || relationship.to == node.id
                })
                .all(|(_, key)| {
                    key.as_ref().is_some_and(|key| {
                        self.catalog.property_column(key.referenced_key) == Some("id")
                    })
                });
            let edge_covered = !plan.star
                && !plan.eligible
                && !input.relationships.is_empty()
                && node.filters.iter().all(|(property, filters)| {
                    input.relationships.iter().any(|relationship| {
                        super::predicates::edge_tag(
                            self.catalog,
                            node,
                            property,
                            filters,
                            relationship,
                        )
                        .is_some()
                    })
                });
            let star_target = plan.star
                && input.query_type == QueryType::Aggregation
                && Some(node.id.as_str()) != plan.star_center
                && primary_key_target;
            if !needed && !protected && node.id_property == "id" && (edge_covered || star_target) {
                plan.elided.insert(node.id.as_str());
                plan.node_predicates.insert(node.id.as_str(), Vec::new());
                continue;
            }
            let filter_only = !plan.star
                && !plan.eligible
                && input.relationships.len() >= 2
                && !needed
                && !node.filters.is_empty()
                && !protected
                && node.id_property == "id";
            let block = if filter_only {
                self.query_in(plan.root)?
            } else {
                plan.root
            };
            let relation = self.scan(block, table, &node.id)?;
            self.bind_scan(relation, ScanInput::Node(index))?;
            let predicates = self.node_predicates(relation, node)?;
            let mut operation = self.read_relation(relation, ReadMode::Current)?;
            for predicate in &predicates {
                operation = self.filter_relation(operation, predicate.clone())?;
            }
            plan.relations.insert(node.id.as_str(), relation);
            plan.node_predicates.insert(node.id.as_str(), predicates);
            if filter_only {
                let projection = self.project_values(
                    operation,
                    [(
                        "id".into(),
                        Expression::Column(self.stored_column(relation, "id")?),
                    )],
                )?;
                self.finish_query(projection)?;
                plan.elided.insert(node.id.as_str());
            } else {
                if !input.relationships.is_empty() {
                    operation = self.materialize_relation(operation, relation)?;
                }
                plan.node_operations.insert(node.id.as_str(), operation);
            }
        }
        Ok(plan)
    }

    fn access_node_outputs(
        &self,
        input: &Input,
        plan: &AccessPlan<'_, 'a>,
    ) -> Result<Vec<(String, Expression<'a>)>> {
        let mut outputs = Vec::new();
        for (index, node) in input.nodes.iter().enumerate() {
            let Some(ColumnSelection::List(properties)) = &node.columns else {
                return Err(GraphError::UnsupportedInput(
                    "expected normalized columns".into(),
                ));
            };
            if plan.elided.contains(node.id.as_str()) {
                if properties.iter().any(|property| property == "id") {
                    outputs.push((
                        format!("{}_id", node.id),
                        Expression::Column(self.input_identity(plan.root, input, index)?),
                    ));
                }
                continue;
            }
            let entity = node.entity.as_deref().ok_or(GraphError::MissingOutput)?;
            for property in properties {
                if let Some(column) = self.catalog.property_column_named(entity, property) {
                    outputs.push((
                        format!("{}_{property}", node.id),
                        Expression::Column(
                            self.stored_column(plan.relations[node.id.as_str()], column)?,
                        ),
                    ));
                }
            }
        }
        Ok(outputs)
    }

    fn access_edge_outputs(
        &self,
        input: &Input,
        plan: &AccessPlan<'_, 'a>,
    ) -> Result<Vec<(String, Expression<'a>)>> {
        let mut outputs = Vec::new();
        for (index, relationship) in input.relationships.iter().enumerate() {
            let edge = self.relations(plan.root)?.find(|relation| {
                self.relation(*relation)
                    .is_ok_and(|relation| relation.input == Some(ScanInput::Relationship(index)))
            });
            if let Some(edge) = edge {
                let prefix = if relationship.hops.max > 1 {
                    format!("hop_e{index}_")
                } else {
                    format!("e{index}_")
                };
                for (column, suffix) in [
                    ("relationship_kind", "type"),
                    ("source_id", "src"),
                    ("source_kind", "src_type"),
                    ("target_id", "dst"),
                    ("target_kind", "dst_type"),
                ] {
                    outputs.push((
                        format!("{prefix}{suffix}"),
                        Expression::Column(self.column(edge, column)?),
                    ));
                }
                if relationship.hops.max > 1 {
                    outputs.push((
                        format!("{prefix}path_nodes"),
                        Expression::Column(self.column(edge, "path_nodes")?),
                    ));
                }
                continue;
            }
            let (source, target) = if relationship.direction == Direction::Incoming {
                (&relationship.to, &relationship.from)
            } else {
                (&relationship.from, &relationship.to)
            };
            let key = plan.keys[index].as_ref().ok_or(GraphError::MissingOutput)?;
            let (holder, referenced) = match key.holder {
                query_data_model::Endpoint::Source => (source, target),
                query_data_model::Endpoint::Target => (target, source),
            };
            for (alias, suffix) in [(source, "src"), (target, "dst")] {
                let identity = if plan.star
                    && alias == referenced
                    && self.catalog.property_column(key.referenced_key) == Some("id")
                {
                    let column = self
                        .catalog
                        .property_column(key.property)
                        .ok_or(GraphError::MissingOutput)?;
                    self.stored_column(plan.relations[holder.as_str()], column)?
                } else {
                    self.stored_column(plan.relations[alias.as_str()], "id")?
                };
                outputs.push((format!("e{index}_{suffix}"), Expression::Column(identity)));
            }
        }
        Ok(outputs)
    }
}

use super::*;
use crate::input::{Direction, Input, QueryType};
use std::collections::HashMap;

use crate::input::{ColumnSelection, FilterOp};

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

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, PhysicalOperation<'catalog>>
{
    pub(super) fn access(&mut self, input: &Input) -> Result<BlockId> {
        self.plan_access(input).map(|(root, _)| root)
    }

    pub(super) fn plan_access(
        &mut self,
        input: &Input,
    ) -> Result<(BlockId, Option<Expression<'catalog>>)> {
        let mut plan = self.prepare_access(input)?;
        if plan.star {
            self.narrow_star(input, &mut plan)?;
        }
        let operation = if input.relationships.is_empty() && input.nodes.len() == 1 {
            plan.node_operations
                .remove(input.nodes[0].id.as_str())
                .expect("declared node")
        } else if plan.eligible || plan.star {
            self.join_foreign_keys(input, &mut plan)?
        } else {
            self.join_edges(input, &mut plan)?
        };
        let condition = plan.aggregate_condition.take();
        Ok((self.finish_access(input, plan, operation)?, condition))
    }

    fn prepare_access<'input>(
        &mut self,
        input: &'input Input,
    ) -> Result<AccessPlan<'input, 'catalog>> {
        if !matches!(
            input.query_type,
            QueryType::Traversal | QueryType::Aggregation
        ) || input.nodes.is_empty()
        {
            return Err(GraphError::UnsupportedInput(
                "expected nonempty traversal".into(),
            ));
        }
        let entity = |alias: &str| entity(input, alias);
        let mut keys = Vec::new();
        let mut star_center = None;
        let mut star = !input.relationships.is_empty();
        let mut reached = HashSet::new();
        let mut eligible = input.relationships.len() >= 2
            && input.nodes.iter().any(|node| {
                node.entity
                    .as_deref()
                    .is_some_and(|entity| self.catalog.entity_has_traversal_path(entity))
            });
        for (index, relationship) in input.relationships.iter().enumerate() {
            let from = entity(&relationship.from)?;
            let to = entity(&relationship.to)?;
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
                star_center = holder;
            }
            star &= holder.is_some()
                && holder == star_center
                && relationship.direction != Direction::Both
                && relationship.hops.min == 1
                && relationship.hops.max == 1
                && relationship.filters.is_empty();
            let scope_preserving = !relationship.types.is_any()
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
            eligible &= key.is_some()
                && relationship.direction != Direction::Both
                && relationship.hops.min == 1
                && relationship.hops.max == 1
                && relationship.filters.is_empty()
                && !point_selective
                && (scope_preserving
                    || self.catalog.entity_is_global(from)
                    || self.catalog.entity_is_global(to))
                && (index == 0
                    || reached.contains(&relationship.from) != reached.contains(&relationship.to));
            reached.insert(relationship.from.clone());
            reached.insert(relationship.to.clone());
            keys.push(key);
        }
        let root = self.select(PhysicalOperation::One);
        let mut relations = HashMap::new();
        let mut node_operations = HashMap::new();
        let mut node_predicates = HashMap::new();
        let mut elided = HashSet::new();
        for (index, node) in input.nodes.iter().enumerate() {
            let entity = node
                .entity
                .as_deref()
                .ok_or_else(|| GraphError::UnsupportedInput("missing node entity".into()))?;
            let table = self
                .catalog
                .entity_table(entity)
                .ok_or_else(|| GraphError::UnknownStored(entity.into()))?;
            let needed = node.existence == crate::input::NodeExistence::CurrentRow
                || input
                    .aggregation
                    .group_by
                    .iter()
                    .any(|group| group.node() == node.id)
                || input.aggregation.metrics.iter().any(|metric| {
                    metric.expr.node() == node.id && metric.expr.property().is_some()
                })
                || input
                    .order_by
                    .as_ref()
                    .is_some_and(|order| order.node == node.id)
                || input.join_predicates.iter().any(|predicate| {
                    predicate.lhs_node == node.id || predicate.rhs_node == node.id
                });
            let primary_key_target = input
                .relationships
                .iter()
                .zip(&keys)
                .filter(|(relationship, _)| {
                    relationship.from == node.id || relationship.to == node.id
                })
                .all(|(_, key)| {
                    key.as_ref().is_some_and(|key| {
                        self.catalog.property_column(key.referenced_key) == Some("id")
                    })
                });
            let requires_authorization_scan = self.requires_authorization_scan(entity);
            if !star
                && !eligible
                && !input.relationships.is_empty()
                && !needed
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
                })
                && node.id_property == "id"
                && !requires_authorization_scan
            {
                elided.insert(node.id.as_str());
                node_predicates.insert(node.id.as_str(), Vec::new());
                continue;
            }
            if star
                && input.query_type == QueryType::Aggregation
                && Some(node.id.as_str()) != star_center
                && !needed
                && primary_key_target
                && node.id_property == "id"
                && !requires_authorization_scan
            {
                elided.insert(node.id.as_str());
                continue;
            }
            let filter_only = !star
                && !eligible
                && input.relationships.len() >= 2
                && !needed
                && !node.filters.is_empty()
                && !requires_authorization_scan
                && node.id_property == "id";
            let block = if filter_only {
                self.select(PhysicalOperation::One)
            } else {
                root
            };
            let relation = self.scan(block, table, &node.id)?;
            self.bind_scan(relation, ScanInput::Node(index))?;
            let operation = self.node_source(relation, node)?;
            relations.insert(node.id.as_str(), relation);
            let mut predicates = Vec::new();
            let mut source = &operation;
            while let Relational::Filter { input, predicate } = source {
                predicates.push(predicate.clone());
                source = input;
            }
            predicates.reverse();
            node_predicates.insert(node.id.as_str(), predicates);
            if filter_only {
                *self.operation_mut(block)? = operation;
                elided.insert(node.id.as_str());
                continue;
            }
            node_operations.insert(
                node.id.as_str(),
                if input.relationships.is_empty() {
                    operation
                } else {
                    operation.materialize(relation)
                },
            );
        }
        Ok(AccessPlan {
            root,
            relations,
            node_operations,
            node_predicates,
            elided,
            keys,
            star_center,
            star,
            eligible,
            aggregate_condition: None,
        })
    }

    fn finish_access<'input>(
        &mut self,
        input: &'input Input,
        plan: AccessPlan<'input, 'catalog>,
        mut operation: PhysicalOperation<'catalog>,
    ) -> Result<BlockId> {
        let AccessPlan {
            root,
            relations,
            node_operations,
            elided,
            keys,
            star,
            ..
        } = plan;
        let entity = |alias: &str| entity(input, alias);
        if !node_operations.is_empty() {
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
                let stored = self
                    .catalog
                    .property_column_named(entity(alias)?, property)
                    .ok_or(GraphError::MissingOutput)?;
                self.stored_column(
                    *relations.get(alias).ok_or(GraphError::MissingOutput)?,
                    stored,
                )
            };
            operation = operation.filter(Expression::Predicate {
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
            });
        }
        if input.query_type == QueryType::Aggregation {
            *self.operation_mut(root)? = operation;
            return Ok(root);
        }
        for (index, node) in input.nodes.iter().enumerate() {
            let Some(ColumnSelection::List(columns)) = &node.columns else {
                return Err(GraphError::UnsupportedInput(
                    "expected normalized columns".into(),
                ));
            };
            if elided.contains(node.id.as_str()) {
                if columns.iter().any(|column| column == "id") {
                    self.project(
                        root,
                        format!("{}_id", node.id),
                        Expression::Column(self.input_identity(root, input, index)?),
                    )?;
                }
                continue;
            }
            for property in columns {
                let entity = node.entity.as_deref().unwrap();
                let Some(column) = self.catalog.property_column_named(entity, property) else {
                    continue;
                };
                self.project(
                    root,
                    format!("{}_{property}", node.id),
                    Expression::Column(self.stored_column(relations[node.id.as_str()], column)?),
                )?;
            }
        }
        for (index, relationship) in input.relationships.iter().enumerate() {
            let edge = self.relations(root)?.find(|relation| {
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
                    self.project(
                        root,
                        format!("{prefix}{suffix}"),
                        Expression::Column(self.column(edge, column)?),
                    )?;
                }
                if relationship.hops.max > 1 {
                    self.project(
                        root,
                        format!("{prefix}path_nodes"),
                        Expression::Column(self.column(edge, "path_nodes")?),
                    )?;
                }
                continue;
            }
            let (source, target) = if relationship.direction == Direction::Incoming {
                (&relationship.to, &relationship.from)
            } else {
                (&relationship.from, &relationship.to)
            };
            let identity = |alias: &str| {
                let key = keys[index].as_ref().ok_or(GraphError::MissingOutput)?;
                let (holder, referenced) = match key.holder {
                    query_data_model::Endpoint::Source => (source, target),
                    query_data_model::Endpoint::Target => (target, source),
                };
                if star
                    && alias == referenced
                    && self.catalog.property_column(key.referenced_key) == Some("id")
                {
                    self.stored_column(
                        relations[holder.as_str()],
                        self.catalog
                            .property_column(key.property)
                            .ok_or(GraphError::MissingOutput)?,
                    )
                } else {
                    self.stored_column(relations[alias], "id")
                }
            };
            let source_id = identity(source)?;
            let target_id = identity(target)?;
            self.project(root, format!("e{index}_src"), Expression::Column(source_id))?;
            self.project(root, format!("e{index}_dst"), Expression::Column(target_id))?;
        }
        if let Some(order) = &input.order_by {
            operation = operation.sort(vec![(
                self.stored_column(relations[order.node.as_str()], &order.property)?,
                order.direction == crate::input::OrderDirection::Desc,
            )]);
        }
        *self.operation_mut(root)? = operation.limit(input.limit);
        Ok(root)
    }
}

#[cfg(test)]
mod parity;
mod predicates;
mod results;

use std::collections::HashMap;

use query_data_model::plan::logical::{LogicalOperation, LogicalPlan};
use query_data_model::plan::tracking::{NodeId, Ref};
use query_data_model::surface::Statement;
use query_data_model::{Model, RelationshipId};

use crate::input::{
    BooleanExpression, Condition, Direction, HopRange, InputNeighbors, InputNode, InputPath,
    InputRelationship, PathType, QueryType,
};
use crate::{Input, QueryError, Result};

pub fn input(statement: &Statement, model: &Model) -> Result<Input> {
    let mut bridge = Bridge {
        plan: &statement.plan,
        model,
        input: Input::default(),
        nodes: HashMap::new(),
        edges: HashMap::new(),
        outputs: HashMap::new(),
        conditions: Vec::new(),
    };
    for node in statement.plan.nodes() {
        bridge.operation(node)?;
    }
    for condition in std::mem::take(&mut bridge.conditions) {
        bridge.input.absorb(condition)?;
    }
    bridge.input.promote_id_filters()?;
    bridge.input.options.include_debug_sql = statement.debug;
    Ok(bridge.input)
}

struct Bridge<'p> {
    plan: &'p LogicalPlan,
    model: &'p Model,
    input: Input,
    nodes: HashMap<Ref, String>,
    edges: HashMap<Ref, usize>,
    outputs: HashMap<Ref, String>,
    conditions: Vec<BooleanExpression<Condition>>,
}

fn direction(direction: query_data_model::Direction) -> Direction {
    match direction {
        query_data_model::Direction::Outgoing => Direction::Outgoing,
        query_data_model::Direction::Incoming => Direction::Incoming,
        query_data_model::Direction::Both => Direction::Both,
    }
}

impl Bridge<'_> {
    fn declared(&self, node: NodeId, back: usize) -> Ref {
        let outputs: Vec<Ref> = self.plan.outputs(node).collect();
        outputs[outputs.len() - back]
    }

    fn alias(&self, binding: Ref) -> Result<String> {
        self.nodes
            .get(&binding)
            .or_else(|| self.outputs.get(&binding))
            .cloned()
            .ok_or_else(|| QueryError::PipelineInvariant(format!("unbound reference {binding:?}")))
    }

    fn names(&self, relationships: &[RelationshipId]) -> Vec<String> {
        match relationships {
            [] => vec!["*".to_string()],
            named => named
                .iter()
                .map(|id| self.model.graph().relationship(*id).name.clone())
                .collect(),
        }
    }

    fn bind(&mut self, binding: Ref, entity: Option<String>) {
        let alias = self.plan.name(binding).unwrap_or_default().to_string();
        self.nodes.insert(binding, alias.clone());
        self.input.nodes.push(InputNode {
            id: alias,
            entity,
            ..Default::default()
        });
    }

    fn entity_name(&self, binding: Ref) -> Option<String> {
        match self.plan.value(binding) {
            query_data_model::plan::logical::LogicalType::Entity(entity) => {
                Some(self.model.graph().entity(*entity).name.clone())
            }
            _ => None,
        }
    }

    fn operation(&mut self, node: NodeId) -> Result<()> {
        match self.plan.operation(node) {
            LogicalOperation::Entities(_) => {
                let binding = self.declared(node, 1);
                self.bind(binding, self.entity_name(binding));
            }
            LogicalOperation::Follow {
                from,
                relationships,
                direction: way,
                range,
                ..
            } => {
                let (edge, target) = (self.declared(node, 2), self.declared(node, 1));
                self.bind(target, self.entity_name(target));
                self.edges.insert(edge, self.input.relationships.len());
                self.input.relationships.push(InputRelationship {
                    types: self.names(relationships),
                    from: self.alias(*from)?,
                    to: self.alias(target)?,
                    hops: HopRange {
                        min: range.min,
                        max: range.max,
                    },
                    direction: direction(*way),
                    filters: HashMap::new(),
                });
            }
            LogicalOperation::Neighbors {
                relationships,
                direction: way,
                ..
            } => {
                let (edge, far) = (self.declared(node, 2), self.declared(node, 1));
                self.edges.insert(edge, 0);
                self.outputs
                    .insert(far, self.plan.name(far).unwrap_or_default().to_string());
                self.input.query_type = QueryType::Neighbors;
                self.input.neighbors = Some(InputNeighbors {
                    direction: direction(*way),
                    rel_types: self.names(relationships),
                });
            }
            LogicalOperation::Shortest {
                from,
                to,
                relationships,
                max_depth,
            } => {
                self.input.query_type = QueryType::PathFinding;
                self.input.path = Some(InputPath {
                    path_type: PathType::Shortest,
                    from: self.alias(*from)?,
                    to: self.alias(*to)?,
                    max_depth: *max_depth,
                    rel_types: self.names(relationships),
                });
            }
            LogicalOperation::Filter(predicate) => {
                let condition = self.condition(predicate)?;
                self.conditions.push(condition);
            }
            LogicalOperation::Aggregate { groups, measures } => {
                self.aggregate(node, groups, measures)?
            }
            LogicalOperation::Order(keys) => self.order(keys)?,
            LogicalOperation::Select(items) => self.select(node, items)?,
            LogicalOperation::Limit { rows, skip } => {
                self.input.limit = u32::try_from(*rows).unwrap_or(u32::MAX);
                self.input.skip = (*skip > 0).then(|| u32::try_from(*skip).unwrap_or(u32::MAX));
            }
            LogicalOperation::Union | LogicalOperation::Read(_) => {
                return Err(QueryError::PipelineInvariant(
                    "the surfaces never build unions or shared bodies".into(),
                ));
            }
        }
        Ok(())
    }
}

use std::collections::BTreeSet;

use super::limits;
use super::{
    Binding, Hops, LogicalBuilder, LogicalCatalog, LogicalOperation, LogicalType, PlanError, Rows,
    Step,
};
use crate::catalog::Unresolved;
use crate::plan::tracking::{Construction, Declare, Declared, Outputs};
use crate::{Direction, EntityId, Hop, Model, RelationshipId};

const LABELED: &str = "every node needs one label, except the far endpoint of a neighbors query";

impl<'m> LogicalBuilder<'m> {
    pub fn traverse(
        &mut self,
        rows: Rows<'m>,
        from: Binding<'m>,
        step: Step,
    ) -> Result<Rows<'m>, PlanError> {
        let (operation, to) = self.attempt(|catalog, construction| {
            let model = catalog.model();
            let start = Self::bound(from)?;
            labeled(&rows)?;
            if step.direction == Direction::Both {
                return Err(PlanError::shape(
                    "undirected relationships are only supported for neighbors; use -> or <- between labeled nodes",
                ));
            }
            for name in step.node.iter().chain(&step.edge) {
                rows.fresh(name)?;
            }
            let nodes = rows.bound().filter(|binding| binding.entity().is_some());
            limits::at_most("nodes", nodes.count() + 1, catalog.limits().nodes)?;
            let relationships = relationships(catalog, construction, &step.relationships)?;
            let target = step
                .target
                .map(|target| construction.declared(target))
                .transpose()?;
            range("hops", step.hops, catalog.limits().hops)?;
            let direction = step.direction;
            let (hops, to) = if step.hops.max == 1 {
                one_hop(model, start, &relationships, target, direction)?
            } else {
                let target = target.ok_or_else(|| {
                    PlanError::shape("a variable-length step needs a target entity")
                })?;
                reach(
                    model,
                    start,
                    &relationships,
                    target,
                    direction,
                    step.hops.max,
                )?;
                (first_hops(model, start, &relationships, direction), target)
            };
            let operation = LogicalOperation::Follow {
                from: from.output(),
                relationships,
                direction,
                range: step.hops,
                hops,
            };
            Ok((operation, to))
        })?;
        let node = step
            .node
            .unwrap_or_else(|| self.catalog().model().graph().entity(to).name.clone());
        let edge = Declare {
            name: step.edge,
            value: LogicalType::Edge,
        };
        self.connect(operation, vec![rows], |_, _, _| {
            Ok(Outputs::extend([
                edge,
                Declare::named(node, LogicalType::Entity(to)),
            ]))
        })
    }

    /// Expands one node to every entity one relationship away; `step` names the far node.
    pub fn neighbors(
        &mut self,
        rows: Rows<'m>,
        from: Binding<'m>,
        step: Step,
    ) -> Result<Rows<'m>, PlanError> {
        let name = step
            .node
            .clone()
            .ok_or_else(|| PlanError::shape("a neighbors step names its far node"))?;
        let operation = self.attempt(|catalog, construction| {
            let start = Self::bound(from)?;
            if rows.bound().count() != 1 {
                return Err(PlanError::shape(LABELED));
            }
            if step.hops != Hops::ONE || step.target.is_some() {
                return Err(PlanError::shape(
                    "neighbors expands one hop to an unlabeled node",
                ));
            }
            for name in std::iter::once(&name).chain(&step.edge) {
                rows.fresh(name)?;
            }
            let relationships = relationships(catalog, construction, &step.relationships)?;
            catalog
                .model()
                .connect(start, &relationships, None, step.direction)?;
            Ok(LogicalOperation::Neighbors {
                from: from.output(),
                relationships,
                direction: step.direction,
            })
        })?;
        let edge = Declare {
            name: step.edge,
            value: LogicalType::Edge,
        };
        self.connect(operation, vec![rows], |_, _, _| {
            Ok(Outputs::extend([
                edge,
                Declare::named(name, LogicalType::Node),
            ]))
        })
    }

    /// The shortest path from the endpoint of `start` to the endpoint of `end`, bound as `name`.
    pub fn shortest(
        &mut self,
        start: Rows<'m>,
        end: Rows<'m>,
        step: Step,
        name: &str,
    ) -> Result<Rows<'m>, PlanError> {
        let operation = self.attempt(|catalog, construction| {
            let (from, to) = (start.endpoint()?, end.endpoint()?);
            if step.direction != Direction::Outgoing || step.hops.min != 1 {
                return Err(PlanError::shape(
                    "path finding supports outgoing paths starting at one hop",
                ));
            }
            for name in std::iter::once(name).chain(step.edge.as_deref()) {
                start.fresh(name)?;
                end.fresh(name)?;
            }
            if let Some(name) = end.names().find(|name| start.fresh(name).is_err()) {
                return Err(PlanError::Duplicate(name.to_string()));
            }
            let relationships = relationships(catalog, construction, &step.relationships)?;
            range("path depth", step.hops, catalog.limits().path_depth)?;
            let (source, target) = (Self::bound(from)?, Self::bound(to)?);
            reach(
                catalog.model(),
                source,
                &relationships,
                target,
                Direction::Outgoing,
                step.hops.max,
            )?;
            Ok(LogicalOperation::Shortest {
                from: from.output(),
                to: to.output(),
                relationships,
                max_depth: step.hops.max,
            })
        })?;
        let edge = Declare {
            name: step.edge,
            value: LogicalType::Edge,
        };
        self.connect(operation, vec![start, end], |_, _, _| {
            Ok(Outputs::extend([
                edge,
                Declare::named(name, LogicalType::Path),
            ]))
        })
    }
}

fn labeled(rows: &Rows<'_>) -> Result<(), PlanError> {
    match rows
        .bound()
        .any(|binding| binding.kind() == LogicalType::Node)
    {
        true => Err(PlanError::shape(LABELED)),
        false => Ok(()),
    }
}

fn relationships(
    catalog: LogicalCatalog<'_>,
    construction: &Construction<super::Logical>,
    declared: &[Declared<RelationshipId>],
) -> Result<Vec<RelationshipId>, PlanError> {
    let max = catalog.limits().relationship_types;
    limits::at_most("relationship types", declared.len(), max)?;
    Ok(declared
        .iter()
        .map(|relationship| construction.declared(*relationship))
        .collect::<Result<_, _>>()?)
}

fn range(what: &str, hops: Hops, max: u32) -> Result<(), PlanError> {
    if hops.min == 0 || hops.min > hops.max {
        return Err(PlanError::shape(format!(
            "{what} must be ordered and at least 1"
        )));
    }
    limits::at_most(what, hops.max as usize, max as usize)
}

fn one_hop(
    model: &Model,
    start: EntityId,
    relationships: &[RelationshipId],
    target: Option<EntityId>,
    direction: Direction,
) -> Result<(Vec<Hop>, EntityId), PlanError> {
    let hops = model.connect(start, relationships, target, direction)?;
    let reached: BTreeSet<EntityId> = hops.iter().map(|hop| hop.to).collect();
    match (target, Vec::from_iter(reached).as_slice()) {
        (Some(to), _) | (None, &[to]) => Ok((hops, to)),
        (None, reached) => Err(PlanError::Ambiguous {
            relationship: names(model, relationships),
            endpoints: reached
                .iter()
                .map(|entity| model.graph().entity(*entity).name.clone())
                .collect(),
        }),
    }
}

fn first_hops(
    model: &Model,
    start: EntityId,
    relationships: &[RelationshipId],
    direction: Direction,
) -> Vec<Hop> {
    model
        .graph()
        .hops(start, direction)
        .filter(|hop| relationships.is_empty() || relationships.contains(&hop.relationship))
        .collect()
}

fn reach(
    model: &Model,
    start: EntityId,
    relationships: &[RelationshipId],
    target: EntityId,
    direction: Direction,
    depth: u32,
) -> Result<(), PlanError> {
    let reachable = model
        .graph()
        .walk(start)
        .direction(direction)
        .max_hops(depth as usize)
        .filter(|hop| relationships.is_empty() || relationships.contains(&hop.relationship))
        .reachable()
        .path_to(target)
        .is_some();
    if reachable {
        return Ok(());
    }
    let graph = model.graph();
    Err(Unresolved::NotConnected {
        from: graph.entity(start).name.clone(),
        to: graph.entity(target).name.clone(),
        relationships: names(model, relationships),
        connecting: Vec::new(),
        reversed: None,
    }
    .into())
}

fn names(model: &Model, relationships: &[RelationshipId]) -> String {
    match relationships {
        [] => "*".to_string(),
        named => named
            .iter()
            .map(|id| model.graph().relationship(*id).name.as_str())
            .collect::<Vec<_>>()
            .join("|"),
    }
}

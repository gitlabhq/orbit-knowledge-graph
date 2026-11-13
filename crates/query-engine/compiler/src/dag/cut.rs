use orbit_server_config::QueryLimits;
use query_data_model::PropertyId;
use query_data_model::plan::logical::{Expr, LogicalOperation};
use query_data_model::plan::tracking::Ref;

use super::split::Crossing;
use super::{
    Backend, CombineColumn, CombineEdge, CombineJoin, CombineNode, CombineQuery, Connections, Dag,
    Input, Peer, RowCheck, Stage, StageId, StageQuery,
};
use crate::error::{QueryError, Result};
use crate::logical::Query;

pub(super) struct Group {
    /// The connection the group's stage runs on.
    pub place: query_data_model::ConnectionId,
    pub bindings: Vec<Ref>,
    /// The placed store of each entity the group reads.
    pub stores: Vec<(query_data_model::EntityId, query_data_model::StoreId)>,
}

pub(super) struct Cut<'q> {
    pub query: Query<'q>,
    pub groups: Vec<Group>,
    pub crossings: Vec<Crossing>,
    pub limits: &'q QueryLimits,
    pub connections: &'q Connections,
}

impl Cut<'_> {
    pub fn dag(self, response: super::Response) -> Result<Dag> {
        let debug = response.debug;
        let order = self.order()?;
        let stage = |group: usize| order.iter().position(|g| *g == group).unwrap_or_default();
        let group_of = |binding: Ref| {
            self.groups
                .iter()
                .position(|group| group.bindings.contains(&binding))
                .unwrap_or_default()
        };
        let mut needed = self.selected()?;
        for crossing in &self.crossings {
            for (binding, property) in crossing.pairs.iter().flat_map(|(a, b)| [*a, *b]) {
                push(needed.entry(binding).or_default(), property);
            }
        }
        let producers: Vec<usize> = self
            .crossings
            .iter()
            .map(|crossing| {
                let consumer = crossing.consumer(&order);
                match crossing.groups {
                    (producer, other) if other == consumer => producer,
                    (_, producer) => producer,
                }
            })
            .collect();
        let mut checks: Vec<Vec<RowCheck>> = vec![Vec::new(); self.groups.len()];
        let mut nodes = Vec::new();
        let graph = self.query.model.graph();
        let access = self.query.model.access();
        for (index, group) in self.groups.iter().enumerate() {
            let joins = self.peer(group.place).joins();
            if joins && !producers.contains(&index) {
                continue;
            }
            for binding in &group.bindings {
                let Some(entity) = self.query.entity(*binding) else {
                    continue;
                };
                for check in &access.entity(entity).checks {
                    let query_data_model::catalog::Check::Rows {
                        provider,
                        permission,
                        resource,
                        id,
                        ..
                    } = check
                    else {
                        continue;
                    };
                    push(needed.entry(*binding).or_default(), *id);
                    let check = RowCheck {
                        provider: provider.clone(),
                        permission: access.permission(*permission).name.clone(),
                        resource: resource.clone(),
                        column: self.column(stage(index), *binding, *id).column,
                        kind: crate::KeyKind::of(graph.property(*id).data_type),
                    };
                    if !checks[index].contains(&check) {
                        checks[index].push(check);
                    }
                }
                if joins {
                    continue;
                }
                let [identity] = graph.entity(entity).identity[..] else {
                    continue;
                };
                if !self.store(group, entity).returns_identity(entity) {
                    continue;
                }
                push(needed.entry(*binding).or_default(), identity);
                nodes.push(CombineNode {
                    alias: self.query.name(*binding).to_string(),
                    entity: graph.entity(entity).name.clone(),
                    id: self.column(stage(index), *binding, identity),
                });
            }
        }
        let mut combine_order = Vec::new();
        for (key, direction) in self.query.sort_keys() {
            let Expr::Property { binding, property } = key else {
                return Err(QueryError::Validation(
                    "a query across connections orders by node properties".into(),
                ));
            };
            push(needed.entry(binding).or_default(), property);
            combine_order.push((
                self.column(stage(group_of(binding)), binding, property),
                direction,
            ));
        }
        let mut stages = Vec::new();
        for (id, group) in order.iter().enumerate() {
            let peer = self.peer(self.groups[*group].place);
            let (connection, backend) = (peer.connection(), peer.backend());
            let checks = std::mem::take(&mut checks[*group]);
            let inputs = self
                .crossings
                .iter()
                .filter(|crossing| crossing.consumer(&order) == *group)
                .map(|crossing| self.keys(crossing, &order).map(Input::Keys))
                .collect::<Result<_>>()?;
            let group = &self.groups[*group];
            let plan = super::sub::plan(
                self.query,
                group,
                &needed,
                self.limits,
                self.connections.ids(),
            )?;
            stages.push(Stage {
                id,
                backend,
                runs: super::Runs::On {
                    connection,
                    stores: group.stores.clone(),
                },
                query: StageQuery::Gql(query_data_model::surface::Statement { plan, debug }),
                inputs,
                response: false,
                checks,
            });
        }
        let joins = self
            .crossings
            .iter()
            .map(|crossing| {
                let (producer, consumer) = crossing.sides(&order, group_of);
                let side = |pairs: Vec<(Ref, PropertyId)>| {
                    pairs
                        .into_iter()
                        .map(|(binding, property)| {
                            self.column(stage(group_of(binding)), binding, property)
                        })
                        .collect()
                };
                let graph = self.query.model.graph();
                CombineJoin {
                    types: consumer
                        .iter()
                        .map(|(_, property)| graph.property(*property).data_type)
                        .collect(),
                    left: side(producer),
                    right: side(consumer),
                }
            })
            .collect();
        let (limit, skip) = self
            .query
            .limit()
            .unwrap_or((self.limits.default_rows, None));
        let edges = self
            .crossings
            .iter()
            .map(|crossing| CombineEdge {
                step: crossing.step,
                relationship: graph.relationship(crossing.relationship).name.clone(),
                source: self.query.name(crossing.ends.0).to_string(),
                target: self.query.name(crossing.ends.1).to_string(),
            })
            .collect();
        let pattern = self.query.steps();
        let steps = order
            .iter()
            .map(|group| {
                let own = |binding: Ref| group_of(binding) == *group;
                pattern
                    .iter()
                    .enumerate()
                    .filter(|(_, step)| own(step.from) && own(step.to))
                    .map(|(index, _)| index)
                    .collect()
            })
            .collect();
        let combine = CombineQuery {
            edges,
            steps,
            joins,
            excerpted: self.excerpted()?,
            columns: self.result_columns(&stage, &group_of)?,
            order: combine_order,
            limit,
            skip,
            nodes,
        };
        let inputs = (0..stages.len()).map(|from| Input::Rows { from }).collect();
        stages.push(Stage {
            id: stages.len(),
            backend: Backend::Combine,
            runs: super::Runs::Combine,
            query: StageQuery::Combine(combine, Box::new(response)),
            inputs,
            response: true,
            checks: Vec::new(),
        });
        Ok(Dag {
            stages,
            logical: Vec::new(),
        })
    }

    fn peer(&self, connection: query_data_model::ConnectionId) -> &Peer {
        self.connections
            .get(connection)
            .expect("a group's connection is served")
    }

    /// The store placement put `entity` on in `group`.
    pub(super) fn store(
        &self,
        group: &Group,
        entity: query_data_model::EntityId,
    ) -> &query_data_model::catalog::Store {
        let (_, store) = group
            .stores
            .iter()
            .find(|(placed, _)| *placed == entity)
            .expect("every entity a group reads is placed");
        self.query.model.store(*store)
    }

    /// The name of the column a stage returns a binding's property in.
    pub(super) fn column(
        &self,
        stage: StageId,
        binding: Ref,
        property: PropertyId,
    ) -> CombineColumn {
        let name = &self.query.model.graph().property(property).name;
        let group = self
            .groups
            .iter()
            .find(|group| group.bindings.contains(&binding));
        let relational = group.is_some_and(|group| self.peer(group.place).joins());
        CombineColumn {
            stage,
            column: match relational {
                true => format!("{}_{name}", self.query.name(binding)),
                false => name.clone(),
            },
        }
    }

    /// Stages run from the bounded side of each crossing: a stage whose own bindings carry a
    /// condition reads first, and a relational connection reads first between equals.
    fn order(&self) -> Result<Vec<usize>> {
        let bounded = |group: &Group| {
            self.query
                .operations()
                .any(|(_, operation)| match operation {
                    LogicalOperation::Filter(predicate) => predicate.exprs().iter().all(|expr| {
                        referenced(*expr).is_some_and(|binding| group.bindings.contains(&binding))
                    }),
                    _ => false,
                })
        };
        let rank = |index: usize| {
            let group = &self.groups[index];
            let joins = self.peer(group.place).joins();
            (!bounded(group), !joins, index)
        };
        let root = (0..self.groups.len())
            .min_by_key(|index| rank(*index))
            .ok_or_else(|| QueryError::PipelineInvariant("a query binds no node".into()))?;
        let mut order = vec![root];
        while order.len() < self.groups.len() {
            let next = self
                .crossings
                .iter()
                .filter_map(|crossing| {
                    let (a, b) = crossing.groups;
                    match (order.contains(&a), order.contains(&b)) {
                        (true, false) => Some(b),
                        (false, true) => Some(a),
                        _ => None,
                    }
                })
                .min_by_key(|index| rank(*index))
                .ok_or_else(|| {
                    QueryError::Validation("the pattern's connections are not joined".into())
                })?;
            order.push(next);
        }
        Ok(order)
    }
}

fn push(properties: &mut Vec<PropertyId>, property: PropertyId) {
    if !properties.contains(&property) {
        properties.push(property);
    }
}

pub(super) fn referenced(expr: Expr) -> Option<Ref> {
    match expr {
        Expr::Property { binding, .. }
        | Expr::Truncate { binding, .. }
        | Expr::Edge { binding, .. }
        | Expr::Attribute { binding, .. }
        | Expr::Node(binding)
        | Expr::Output(binding) => Some(binding),
    }
}

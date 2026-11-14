use std::collections::HashMap;

use orbit_server_config::QueryLimits;
use query_data_model::plan::combine::Position;
use query_data_model::plan::logical::{Expr, LogicalOperation, MATCH_LINE};
use query_data_model::plan::tracking::Ref;
use query_data_model::{DataType, Direction, EdgeField, EntityId, PropertyId};

use super::split::Crossing;
use super::{
    Backend, ColumnType, CombineEdge, CombineJoin, CombineNode, CombineQuery, Connections,
    Contract, Dag, Handle, Input, Output, Peer, RowCheck, Stage, StageQuery,
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
        let lookups = order
            .iter()
            .map(|group| self.lookups(*group))
            .collect::<Result<Vec<_>>>()?;
        let mut ids = vec![0; self.groups.len()];
        let mut next = 0;
        for (group, lookups) in order.iter().zip(&lookups) {
            next += lookups.len();
            ids[*group] = next;
            next += 1;
        }
        let stage = |group: usize| ids[group];
        let checked = self.checked(&order);
        let needed = self.needed(&checked)?;
        let contracts = (0..self.groups.len())
            .map(|group| self.contract(group, &needed))
            .collect::<Result<Vec<_>>>()?;
        let position = |binding: Ref, property: PropertyId| {
            let group = self.group(binding);
            let output = self.node(group, binding, property)?;
            let column = contracts[group].position(&output).ok_or_else(|| {
                QueryError::PipelineInvariant("a stage's contract lacks a needed column".into())
            })?;
            Ok::<_, QueryError>(Position {
                stage: stage(group),
                column,
            })
        };
        let graph = self.query.model.graph();
        let access = self.query.model.access();
        let mut checks: Vec<Vec<RowCheck>> = vec![Vec::new(); self.groups.len()];
        let mut nodes = Vec::new();
        for (index, group) in self.groups.iter().enumerate() {
            let joins = self.peer(group.place).joins();
            for binding in &group.bindings {
                let Some(entity) = self.query.entity(*binding) else {
                    continue;
                };
                let declared = graph.entity(entity);
                let key = match joins {
                    true => self
                        .redaction_key(entity)
                        .map(|key| position(*binding, key))
                        .transpose()?,
                    false => None,
                };
                let own = self.query.model.checks(entity, group.place);
                for check in own.filter(|_| checked[index]) {
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
                    let check = RowCheck {
                        provider: provider.clone(),
                        permission: access.permission(*permission).name.clone(),
                        resource: resource.clone(),
                        position: position(*binding, *id)?.column,
                        kind: crate::KeyKind::of(graph.property(*id).data_type),
                    };
                    if !checks[index].contains(&check) {
                        checks[index].push(check);
                    }
                }
                if !self.returns_identity(index, entity) {
                    continue;
                }
                let identity = declared
                    .identity
                    .iter()
                    .map(|part| Ok((position(*binding, *part)?, graph.property(*part).data_type)))
                    .collect::<Result<_>>()?;
                nodes.push(CombineNode {
                    alias: self.query.name(*binding).to_string(),
                    entity: declared.name.clone(),
                    stage: stage(index),
                    identity,
                    key,
                    checked: !joins,
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
            combine_order.push((position(binding, property)?, direction));
        }
        let mut stages = Vec::new();
        for (group, lookups) in order.iter().zip(&lookups) {
            let peer = self.peer(self.groups[*group].place);
            let (connection, backend) = (peer.connection(), peer.backend());
            let mut inputs: Vec<Input> = self
                .crossings
                .iter()
                .filter(|crossing| crossing.consumer(&order) == *group)
                .map(|crossing| {
                    self.keys(crossing, &order, &ids, &contracts)
                        .map(Input::Keys)
                })
                .collect::<Result<_>>()?;
            for lookup in lookups {
                let scope = self.lookup_stage(lookup, stages.len(), &inputs, debug)?;
                inputs.extend(self.scopes(lookup, &scope)?);
                stages.push(scope);
            }
            let plan = super::sub::plan(
                self.query,
                &self.groups[*group],
                &contracts[*group],
                self.limits,
                self.connections.ids(),
            )?;
            stages.push(Stage {
                id: stages.len(),
                backend,
                runs: super::Runs::On {
                    connection,
                    stores: self.groups[*group].stores.clone(),
                },
                query: StageQuery::Gql(query_data_model::surface::Statement { plan, debug }),
                inputs,
                contract: Some(contracts[*group].clone()),
                checks: std::mem::take(&mut checks[*group]),
            });
        }
        let joins = self
            .crossings
            .iter()
            .map(|crossing| {
                let (producer, consumer) = crossing.sides(&order, |binding| self.group(binding));
                let side = |pairs: &[(Ref, PropertyId)]| {
                    pairs
                        .iter()
                        .map(|(binding, property)| position(*binding, *property))
                        .collect::<Result<Vec<_>>>()
                };
                Ok(CombineJoin {
                    types: consumer
                        .iter()
                        .map(|(_, property)| graph.property(*property).data_type)
                        .collect(),
                    left: side(&producer)?,
                    right: side(&consumer)?,
                })
            })
            .collect::<Result<_>>()?;
        let (limit, skip) = self
            .query
            .limit()
            .unwrap_or((self.limits.default_rows, None));
        let mut edges: Vec<CombineEdge> = self
            .crossings
            .iter()
            .map(|crossing| CombineEdge::Crossing {
                step: crossing.step,
                relationship: graph.relationship(crossing.relationship).name.clone(),
                source: self.query.name(crossing.ends.0).to_string(),
                target: self.query.name(crossing.ends.1).to_string(),
            })
            .collect();
        for (group, contract) in contracts.iter().enumerate() {
            edges.extend(self.stage_edges(group, stage(group), contract)?);
        }
        let combine = CombineQuery {
            edges,
            joins,
            excerpted: self.excerpted()?,
            columns: self.result_columns(&position, &stage, &contracts)?,
            order: combine_order,
            limit,
            skip,
            nodes,
        };
        let inputs = order
            .iter()
            .map(|group| Input::Rows {
                from: stage(*group),
            })
            .collect();
        stages.push(Stage {
            id: stages.len(),
            backend: Backend::Combine,
            runs: super::Runs::Combine,
            query: StageQuery::Combine(combine, Box::new(response)),
            inputs,
            contract: None,
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

    /// Whether each group's stage puts its rows through their entities' row checks: a remote
    /// stage does, and a relational stage whose keys cross into another.
    fn checked(&self, order: &[usize]) -> Vec<bool> {
        let mut checked: Vec<bool> = self
            .groups
            .iter()
            .map(|group| !self.peer(group.place).joins())
            .collect();
        for crossing in &self.crossings {
            let consumer = crossing.consumer(order);
            let producer = match crossing.groups {
                (producer, other) if other == consumer => producer,
                (_, producer) => producer,
            };
            checked[producer] = true;
        }
        checked
    }

    /// The key redaction checks a relational node's rows by when it is not the identity.
    pub(super) fn redaction_key(&self, entity: EntityId) -> Option<PropertyId> {
        let identity = &self.query.model.graph().entity(entity).identity;
        self.query
            .model
            .access()
            .entity(entity)
            .checks
            .iter()
            .find_map(|check| match check {
                query_data_model::catalog::Check::Rows { id, .. } if *identity != [*id] => {
                    Some(*id)
                }
                _ => None,
            })
    }

    /// Whether a group's stage returns the identity of `entity`'s nodes: a relational stage
    /// always does, a live stage when its store returns it, and a code stage never yet.
    fn returns_identity(&self, group: usize, entity: EntityId) -> bool {
        let group = &self.groups[group];
        match self.peer(group.place).backend() {
            Backend::ClickHouse | Backend::DuckDb => true,
            Backend::Live => self.store(group, entity).returns_identity(entity),
            Backend::OrbitCode | Backend::Combine => false,
        }
    }

    /// The contract output that holds a binding's property in its group's stage.
    pub(super) fn node(&self, group: usize, binding: Ref, property: PropertyId) -> Result<Output> {
        let entity = self.query.entity(binding).ok_or_else(|| {
            QueryError::PipelineInvariant("a binding a stage returns has no entity".into())
        })?;
        let identity = &self.query.model.graph().entity(entity).identity;
        let handle = match identity.iter().position(|part| *part == property) {
            Some(part) if self.returns_identity(group, entity) => Handle::Identity(part),
            _ => Handle::Property(property),
        };
        Ok(Output::Node {
            binding: self.query.name(binding).to_string(),
            handle,
        })
    }

    /// The columns a group's stage returns: for each node, the properties later stages read and
    /// its identity when the stage returns it; then each own step's edge, or the match columns.
    fn contract(&self, group: usize, needed: &HashMap<Ref, Vec<PropertyId>>) -> Result<Contract> {
        let graph = self.query.model.graph();
        let mut contract = Contract {
            columns: Vec::new(),
        };
        for binding in &self.groups[group].bindings {
            let Some(entity) = self.query.entity(*binding) else {
                continue;
            };
            let identity = &graph.entity(entity).identity;
            let parts = match self.returns_identity(group, entity) {
                true => identity.as_slice(),
                false => &[],
            };
            let listed = needed.get(binding).map_or(&[][..], Vec::as_slice);
            for property in listed.iter().chain(parts) {
                let data_type = ColumnType::Scalar(graph.property(*property).data_type);
                contract.push(self.node(group, *binding, *property)?, data_type);
            }
        }
        for (step, global) in self.own_steps(group).into_iter().enumerate() {
            let pattern = &self.query.steps()[global];
            let (source, target) = match pattern.direction {
                Direction::Outgoing | Direction::Both => (pattern.from, pattern.to),
                Direction::Incoming => (pattern.to, pattern.from),
            };
            let text = ColumnType::Scalar(DataType::String);
            for (field, data_type) in [
                (EdgeField::Kind, text),
                (EdgeField::SourceId, self.id_type(source)?),
                (EdgeField::SourceKind, text),
                (EdgeField::TargetId, self.id_type(target)?),
                (EdgeField::TargetKind, text),
            ] {
                contract.push(Output::Edge { step, field }, data_type);
            }
            if pattern.range.max > 1 {
                contract.push(Output::Path { step }, ColumnType::Path);
            }
        }
        for item in self.query.selected() {
            let query_data_model::plan::logical::Projection::Value(Expr::Output(output)) = item
            else {
                continue;
            };
            if self.matched_group(*output)? != group {
                continue;
            }
            let (column, data_type) = match self.query.name(*output) {
                MATCH_LINE => (Output::Line, DataType::Int),
                other => (
                    Output::Capture(other.strip_prefix("match.").unwrap_or(other).to_string()),
                    DataType::String,
                ),
            };
            contract.push(column, ColumnType::Scalar(data_type));
        }
        Ok(contract)
    }

    /// The pattern steps whose ends a group holds, in the order its stage follows them.
    fn own_steps(&self, group: usize) -> Vec<usize> {
        let bindings = &self.groups[group].bindings;
        let joins = self.peer(self.groups[group].place).joins();
        self.query
            .steps()
            .iter()
            .enumerate()
            .filter(|(_, step)| {
                joins && bindings.contains(&step.from) && bindings.contains(&step.to)
            })
            .map(|(index, _)| index)
            .collect()
    }

    /// The type of the identity an edge's endpoint carries.
    fn id_type(&self, binding: Ref) -> Result<ColumnType> {
        let graph = self.query.model.graph();
        let identity = self
            .query
            .entity(binding)
            .map(|entity| graph.entity(entity).identity.as_slice());
        match identity {
            Some([part]) => Ok(ColumnType::Scalar(graph.property(*part).data_type)),
            _ => Err(QueryError::Validation(format!(
                "{} has no single identity for an edge to carry",
                self.query.name(binding)
            ))),
        }
    }

    /// The edges a relational group's stage returns, under the pattern step each stands for.
    fn stage_edges(
        &self,
        group: usize,
        stage: usize,
        contract: &Contract,
    ) -> Result<Vec<CombineEdge>> {
        let graph = self.query.model.graph();
        let steps = self.query.steps();
        let at = |output: Output| {
            contract
                .position(&output)
                .map(|column| Position { stage, column })
                .ok_or_else(|| {
                    QueryError::PipelineInvariant("a stage's contract lacks an edge column".into())
                })
        };
        self.own_steps(group)
            .into_iter()
            .enumerate()
            .map(|(local, global)| {
                let field = |field| at(Output::Edge { step: local, field });
                let rel_types = match steps[global].relationships {
                    [] => vec!["*".to_string()],
                    named => named
                        .iter()
                        .map(|relationship| graph.relationship(*relationship).name.clone())
                        .collect(),
                };
                Ok(CombineEdge::Stage {
                    step: global,
                    rel_types,
                    kind: field(EdgeField::Kind)?,
                    source: field(EdgeField::SourceId)?,
                    source_kind: field(EdgeField::SourceKind)?,
                    target: field(EdgeField::TargetId)?,
                    target_kind: field(EdgeField::TargetKind)?,
                    path: match steps[global].range.max > 1 {
                        true => Some(at(Output::Path { step: local })?),
                        false => None,
                    },
                })
            })
            .collect()
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

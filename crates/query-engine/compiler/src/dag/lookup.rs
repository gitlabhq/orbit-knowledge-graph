//! The scope lookups of a remote stage. Each `via` chain its reads follow is a relational stage
//! that reads the chain from its head, on the store placement puts the head on, under every chain
//! entity's guard and checks, and hands the head's keys to the remote stage as its scope keys.

use query_data_model::catalog::{Check, Guard, Needs, Operation, Rule};
use query_data_model::plan::logical::{
    Columns, Comparison, Conjunct, Expr, Hops, Literal, LogicalOperation, Operand, Predicate,
    Projection, Step,
};
use query_data_model::plan::tracking::Ref;
use query_data_model::surface::Statement;
use query_data_model::{ConnectionId, Direction, Endpoint, EntityId, Hop, PropertyId, StoreId};

use super::cut::Cut;
use super::{
    ColumnType, Contract, Handle, Input, KeyInput, Output, RowCheck, Runs, Stage, StageId,
    StageQuery,
};
use crate::error::{QueryError, Result};

/// One chain a remote group's reads follow, and the bindings whose reads it admits.
pub(super) struct Lookup {
    group: usize,
    head: EntityId,
    /// The hops past the head, which the lookup follows on the head's connection.
    chain: Vec<Hop>,
    /// Each admitted binding, with each of its properties and the head key it holds.
    admits: Vec<(Ref, Vec<(PropertyId, PropertyId)>)>,
}

impl Lookup {
    /// The head keys the admitted bindings hold, once each.
    fn keys(&self) -> Vec<PropertyId> {
        let mut keys = Vec::new();
        for (_, pairs) in &self.admits {
            for (_, key) in pairs {
                if !keys.contains(key) {
                    keys.push(*key);
                }
            }
        }
        keys
    }
}

fn name(index: usize) -> String {
    match index {
        0 => "scope".to_string(),
        index => format!("scope{index}"),
    }
}

impl Cut<'_> {
    /// The lookups a group's reads need, one per distinct chain.
    pub(super) fn lookups(&self, index: usize) -> Result<Vec<Lookup>> {
        let group = &self.groups[index];
        let mut lookups: Vec<Lookup> = Vec::new();
        for binding in &group.bindings {
            let Some(entity) = self.query.entity(*binding) else {
                continue;
            };
            let Some(Guard {
                rule: Rule::Via(hops),
                ..
            }) = self.store(group, entity).guard(entity)
            else {
                continue;
            };
            let [first, chain @ ..] = &hops[..] else {
                return Err(QueryError::Security(format!(
                    "{}'s via chain is empty",
                    self.query.model.graph().entity(entity).name
                )));
            };
            let pairs = self.held_keys(first)?;
            match lookups
                .iter_mut()
                .find(|lookup| lookup.head == first.to && lookup.chain == chain)
            {
                Some(lookup) => lookup.admits.push((*binding, pairs)),
                None => lookups.push(Lookup {
                    group: index,
                    head: first.to,
                    chain: chain.to_vec(),
                    admits: vec![(*binding, pairs)],
                }),
            }
        }
        Ok(lookups)
    }

    /// The properties the reading side of a chain's first hop holds the head's keys in.
    fn held_keys(&self, hop: &Hop) -> Result<Vec<(PropertyId, PropertyId)>> {
        let model = self.query.model;
        let holder = match hop.forward {
            true => Endpoint::Source,
            false => Endpoint::Target,
        };
        model
            .foreign_key(hop.variant)
            .filter(|key| key.holder == holder)
            .map(|key| vec![(key.property, key.referenced_key)])
            .or_else(|| {
                model
                    .composite_key(hop.variant)
                    .filter(|(held, _)| *held == holder)
                    .map(|(_, pairs)| pairs.to_vec())
            })
            .ok_or_else(|| {
                QueryError::Security(format!(
                    "{}'s via chain starts with a step whose keys it does not hold",
                    model.graph().entity(hop.from).name
                ))
            })
    }

    /// The lookup's stage `id`, bound by the key sets in `inputs` its consumer reads and by the
    /// consumer's own conditions on the keys it holds.
    pub(super) fn lookup_stage(
        &self,
        lookup: &Lookup,
        id: StageId,
        inputs: &[Input],
        debug: bool,
    ) -> Result<Stage> {
        let model = self.query.model;
        let graph = model.graph();
        let keyed = self.keyed(lookup, inputs);
        let named = self.named(lookup);
        let keys = lookup.keys();
        let mut needs = Needs::default();
        for key in keys.iter().copied() {
            let bound = keyed.iter().any(|input| input.properties.contains(&key))
                || named.iter().any(|(property, _)| *property == key);
            match bound {
                true => needs.apply(key, Operation::OneOf),
                false => needs.read(key),
            }
        }
        let (connection, head) = self.place(lookup.head, &needs)?;
        let peer = self
            .connections
            .get(connection)
            .filter(|peer| peer.joins())
            .ok_or_else(|| {
                QueryError::Security(format!(
                    "no served relational connection stores {}, the head of a via chain",
                    graph.entity(lookup.head).name
                ))
            })?;
        let store = model.store(head);
        for input in &keyed {
            if let Some(key) = input
                .properties
                .iter()
                .find(|key| !store.accepts(lookup.head, **key))
            {
                return Err(QueryError::Validation(format!(
                    "{} on connection {} takes no key set on {}.{}, so no scope lookup can read it",
                    graph.entity(lookup.head).name,
                    peer.name(),
                    graph.entity(lookup.head).name,
                    graph.property(*key).name
                )));
            }
        }
        let entities: Vec<EntityId> = std::iter::once(lookup.head)
            .chain(lookup.chain.iter().map(|hop| hop.to))
            .collect();
        let mut stores = vec![(lookup.head, head)];
        for entity in &entities[1..] {
            let (at, store) = self.place(*entity, &Needs::default())?;
            if at != connection {
                return Err(QueryError::Security(format!(
                    "{}'s via chain crosses from connection {} to {}",
                    graph.entity(lookup.head).name,
                    peer.name(),
                    model.connection(at).name()
                )));
            }
            stores.push((*entity, store));
        }
        let mut contract = Contract {
            columns: Vec::new(),
        };
        for key in keys {
            contract.push(self.scope_node(0, lookup.head, key), self.typed(key));
        }
        let access = model.access();
        let mut checks = Vec::new();
        for (index, entity) in entities.iter().enumerate() {
            for check in model.checks(*entity, connection) {
                let Check::Rows {
                    provider,
                    permission,
                    resource,
                    id: key,
                    ..
                } = check
                else {
                    continue;
                };
                let position =
                    contract.push(self.scope_node(index, *entity, *key), self.typed(*key));
                let check = RowCheck {
                    provider: provider.clone(),
                    permission: access.permission(*permission).name.clone(),
                    resource: resource.clone(),
                    position,
                    kind: crate::KeyKind::of(graph.property(*key).data_type),
                };
                if !checks.contains(&check) {
                    checks.push(check);
                }
            }
        }
        let plan = self.lookup_plan(lookup, &entities, &named, &contract)?;
        Ok(Stage {
            id,
            backend: peer.backend(),
            runs: Runs::On { connection, stores },
            query: StageQuery::Gql(Statement { plan, debug }),
            inputs: keyed.into_iter().map(Input::Keys).collect(),
            contract: Some(contract),
            checks,
        })
    }

    /// The scope keys a lookup's stage hands each binding it admits.
    pub(super) fn scopes(&self, lookup: &Lookup, stage: &Stage) -> Result<Vec<Input>> {
        let contract = stage.contract.as_ref().ok_or_else(|| {
            QueryError::PipelineInvariant("a scope lookup returns its keys".into())
        })?;
        lookup
            .admits
            .iter()
            .map(|(binding, pairs)| {
                let positions = pairs
                    .iter()
                    .map(|(_, key)| {
                        contract
                            .position(&self.scope_node(0, lookup.head, *key))
                            .ok_or_else(|| {
                                QueryError::PipelineInvariant(
                                    "a scope lookup returns no key column".into(),
                                )
                            })
                    })
                    .collect::<Result<_>>()?;
                Ok(Input::Scope(KeyInput {
                    from: stage.id,
                    positions,
                    binding: self.query.name(*binding).to_string(),
                    properties: pairs.iter().map(|(held, _)| *held).collect(),
                    types: pairs
                        .iter()
                        .map(|(_, key)| self.query.model.graph().property(*key).data_type)
                        .collect(),
                }))
            })
            .collect()
    }

    /// The store and connection placement puts a read of `entity` with `needs` on.
    fn place(&self, entity: EntityId, needs: &Needs) -> Result<(ConnectionId, StoreId)> {
        let model = self.query.model;
        let placement = model
            .get_store(entity, needs)
            .map_err(|unplaced| QueryError::AllowlistRejected(unplaced.to_string()))?;
        let [(store, _)] = placement.parts() else {
            return Err(QueryError::PipelineInvariant(
                "a read runs on one store".into(),
            ));
        };
        Ok((model.store(*store).connection(), *store))
    }

    fn scope_node(&self, index: usize, entity: EntityId, property: PropertyId) -> Output {
        let identity = &self.query.model.graph().entity(entity).identity;
        let handle = match identity.iter().position(|part| *part == property) {
            Some(part) => Handle::Identity(part),
            None => Handle::Property(property),
        };
        Output::Node {
            binding: name(index),
            handle,
        }
    }

    fn typed(&self, property: PropertyId) -> ColumnType {
        ColumnType::Scalar(self.query.model.graph().property(property).data_type)
    }

    /// Each binding property that holds a head key, or that the group's own steps hold equal to
    /// one, with the head key it bounds.
    fn holders(&self, lookup: &Lookup) -> Vec<(Ref, PropertyId, PropertyId)> {
        let steps = self.query.steps();
        let pairs: Vec<_> = steps
            .iter()
            .enumerate()
            .filter(|(_, step)| {
                self.group(step.from) == lookup.group && self.group(step.to) == lookup.group
            })
            .filter_map(|(index, step)| {
                super::split::crossing(self.query, index, *step, (lookup.group, lookup.group)).ok()
            })
            .flat_map(|crossing| crossing.pairs)
            .collect();
        let mut holders = Vec::new();
        for (binding, held_keys) in &lookup.admits {
            for (held, key) in held_keys {
                let mut equal = vec![(*binding, *held)];
                let mut grown = true;
                while grown {
                    grown = false;
                    for (a, b) in &pairs {
                        for (from, to) in [(a, b), (b, a)] {
                            if equal.contains(from) && !equal.contains(to) {
                                equal.push(*to);
                                grown = true;
                            }
                        }
                    }
                }
                for (binding, property) in equal {
                    if !holders.contains(&(binding, property, *key)) {
                        holders.push((binding, property, *key));
                    }
                }
            }
        }
        holders
    }

    /// The key sets the consumer reads on properties that hold head keys, as key sets on those
    /// head keys.
    fn keyed(&self, lookup: &Lookup, inputs: &[Input]) -> Vec<KeyInput> {
        let holders = self.holders(lookup);
        let graph = self.query.model.graph();
        let mut keyed = Vec::new();
        for input in inputs {
            let Input::Keys(input) = input else {
                continue;
            };
            let mut matched: Vec<(usize, PropertyId)> = Vec::new();
            for (part, property) in input.properties.iter().enumerate() {
                let key = holders.iter().find_map(|(binding, held, key)| {
                    (self.query.name(*binding) == input.binding && held == property).then_some(*key)
                });
                if let Some(key) = key.filter(|key| !matched.iter().any(|(_, at)| at == key)) {
                    matched.push((input.positions[part], key));
                }
            }
            if matched.is_empty() {
                continue;
            }
            keyed.push(KeyInput {
                from: input.from,
                positions: matched.iter().map(|(position, _)| *position).collect(),
                binding: name(0),
                properties: matched.iter().map(|(_, key)| *key).collect(),
                types: matched
                    .iter()
                    .map(|(_, key)| graph.property(*key).data_type)
                    .collect(),
            });
        }
        keyed
    }

    /// The values the consumer's own conditions name for properties that hold head keys, as the
    /// head key each set bounds.
    fn named(&self, lookup: &Lookup) -> Vec<(PropertyId, Vec<Literal>)> {
        let holders = self.holders(lookup);
        let held = |expr: &Expr| {
            holders.iter().find_map(|(binding, held, key)| {
                matches!(expr, Expr::Property { binding: b, property } if b == binding && property == held)
                    .then_some(*key)
            })
        };
        let mut named = Vec::new();
        for (_, operation) in self.query.operations() {
            let LogicalOperation::Filter(predicate) = operation else {
                continue;
            };
            for conjunct in predicate.conjuncts() {
                let Conjunct::Leaf {
                    predicate,
                    negated: false,
                } = conjunct
                else {
                    continue;
                };
                match predicate {
                    Predicate::Compare {
                        left,
                        comparison: Comparison::Eq,
                        right: Operand::Literal(value),
                    } => named.extend(held(left).map(|key| (key, vec![value.clone()]))),
                    Predicate::In { left, values } => {
                        named.extend(held(left).map(|key| (key, values.clone())))
                    }
                    Predicate::InTuple { left, values } => {
                        for (part, expr) in left.iter().enumerate() {
                            if let Some(key) = held(expr) {
                                let column = values.iter().map(|tuple| tuple[part].clone());
                                named.push((key, column.collect()));
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        named
    }

    /// The chain from its head, each bound value set as a condition on the head, returning the
    /// contract's columns.
    fn lookup_plan(
        &self,
        lookup: &Lookup,
        entities: &[EntityId],
        named: &[(PropertyId, Vec<Literal>)],
        contract: &Contract,
    ) -> Result<query_data_model::plan::logical::LogicalPlan> {
        let limits = orbit_server_config::QueryLimits {
            max_rows: self.limits.max_rows.max(self.limits.stage_keys as u64 + 1),
            ..*self.limits
        };
        let model = self.query.model;
        let catalog = model
            .logical(&limits)
            .as_admin()
            .across(self.connections.ids());
        Ok(catalog.build(|q| {
            let mut rows = q.entities_as(catalog.entity(lookup.head), &name(0))?;
            for (key, values) in named {
                let head = rows.binding(&name(0))?;
                rows = q.filter(rows, head.property(*key)?.one_of(values.clone()))?;
            }
            for (index, hop) in lookup.chain.iter().enumerate() {
                let from = rows.binding(&name(index))?;
                let direction = match hop.forward {
                    true => Direction::Outgoing,
                    false => Direction::Incoming,
                };
                let step = Step::new(direction)
                    .hops(Hops::ONE)
                    .named(name(index + 1))
                    .to(catalog.entity(hop.to))
                    .via([catalog.relationship(hop.relationship)]);
                rows = q.traverse(rows, from, step)?;
            }
            let graph = model.graph();
            let mut items = Vec::new();
            for (index, entity) in entities.iter().enumerate() {
                let binding = name(index);
                let identity = &graph.entity(*entity).identity;
                let properties: Vec<PropertyId> = contract
                    .columns
                    .iter()
                    .filter_map(|column| match &column.output {
                        Output::Node {
                            binding: at,
                            handle,
                        } if *at == binding => Some(match handle {
                            Handle::Identity(part) => identity[*part],
                            Handle::Property(property) => *property,
                        }),
                        _ => None,
                    })
                    .collect();
                if properties.is_empty() {
                    continue;
                }
                let bound = rows.binding(&binding)?.output();
                let projection = Projection::Graph {
                    binding: bound,
                    columns: Columns::Listed(properties),
                };
                items.push(projection.named(binding));
            }
            let rows = q.select(rows, items)?;
            q.limit(rows, self.limits.stage_keys as u64 + 1)
        })?)
    }
}

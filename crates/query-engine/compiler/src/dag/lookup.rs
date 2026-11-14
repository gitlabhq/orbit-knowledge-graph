//! The scope lookups of a remote stage. Each `via` chain its reads follow is a relational stage
//! that reads the chain from its head, on the store placement puts the head on, under every chain
//! entity's guard and checks, and hands the head's keys to the remote stage as its scope keys.
//! When an earlier relational stage already reads that chain through the variables the remote
//! bindings' steps reach, its rows are the scope keys and no lookup stage runs.

use query_data_model::catalog::{Check, Guard, Needs, Operation, Rule};
use query_data_model::plan::logical::{
    Columns, Comparison, Conjunct, Expr, Hops, Literal, LogicalOperation, LogicalPlan, Operand,
    Predicate, Projection, Step,
};
use query_data_model::plan::tracking::Ref;
use query_data_model::surface::Statement;
use query_data_model::{Direction, Endpoint, EntityId, Hop, PropertyId, StoreId};

use super::cut::Cut;
use super::split::place;
use super::{
    ColumnType, Contract, Handle, Input, KeyInput, Output, Place, RowCheck, Runs, Stage, StageId,
    StageQuery,
};
use crate::error::{QueryError, Result};

/// Each property a binding holds a head key in, with that head key.
type Held = Vec<(PropertyId, PropertyId)>;

/// One chain a remote group's reads follow, and the bindings whose reads it admits.
pub(super) struct Lookup {
    group: usize,
    head: EntityId,
    /// The hops past the head, which the lookup follows on the head's connection.
    chain: Vec<Hop>,
    /// Each admitted binding, the hop it reaches the head by, and each of its properties with the
    /// head key it holds.
    admits: Vec<(Ref, Hop, Held)>,
    /// The earlier relational group whose stage already reads the chain, and the variable it
    /// reads each admitted binding's head as.
    pub reader: Option<(usize, Vec<Ref>)>,
}

fn name(index: usize) -> String {
    match index {
        0 => "scope".to_string(),
        index => format!("scope{index}"),
    }
}

impl Cut<'_> {
    /// The lookups a group's reads need, one per distinct chain, each with the earlier stage in
    /// `order` that already reads it.
    pub(super) fn lookups(&self, index: usize, order: &[usize]) -> Result<Vec<Lookup>> {
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
                return Err(QueryError::Security("a via chain is empty".into()));
            };
            let admitted = (*binding, *first, self.held_keys(first)?);
            match lookups
                .iter_mut()
                .find(|lookup| lookup.head == first.to && lookup.chain == chain)
            {
                Some(lookup) => lookup.admits.push(admitted),
                None => lookups.push(Lookup {
                    group: index,
                    head: first.to,
                    chain: chain.to_vec(),
                    admits: vec![admitted],
                    reader: None,
                }),
            }
        }
        for lookup in &mut lookups {
            lookup.reader = self.reader(lookup, order)?;
        }
        Ok(lookups)
    }

    /// The earlier relational group that reads the lookup's chain exactly: each admitted
    /// binding's own steps follow the chain into variables that group reads, on the stores the
    /// lookup would read them from, so its checked rows are the keys the lookup would return; and
    /// the lookup has no bound that group's read lacks.
    fn reader(&self, lookup: &Lookup, order: &[usize]) -> Result<Option<(usize, Vec<Ref>)>> {
        let steps = self.query.steps();
        let reach = |binding: Ref, first: &Hop| {
            let head = follow(&steps, binding, first)?;
            let group = self.group(head);
            let mut at = head;
            for hop in &lookup.chain {
                at = follow(&steps, at, hop).filter(|next| self.group(*next) == group)?;
            }
            Some((group, head))
        };
        let Some(reached) = lookup
            .admits
            .iter()
            .map(|(binding, first, _)| reach(*binding, first))
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let group = reached[0].0;
        let position = |group: usize| order.iter().position(|at| *at == group);
        if reached.iter().any(|(at, _)| *at != group) || position(group) >= position(lookup.group) {
            return Ok(None);
        }
        let holders = self.holders(lookup);
        let holds = |(binding, property): &(Ref, PropertyId)| {
            holders
                .iter()
                .any(|(held, at, _)| held == binding && at == property)
        };
        let bounded_elsewhere = self.crossings.iter().any(|crossing| {
            let (producer, consumer) = crossing.sides(order, |binding| self.group(binding));
            crossing.consumer(order) == lookup.group
                && self.group(producer[0].0) != group
                && consumer.iter().any(holds)
        });
        if bounded_elsewhere || !self.named(&holders).is_empty() {
            return Ok(None);
        }
        let mut needs = Needs::default();
        for (_, key) in lookup.admits.iter().flat_map(|(_, _, pairs)| pairs) {
            needs.apply(*key, Operation::OneOf);
        }
        let (_, stores) = self.placed(lookup, &needs)?;
        let read = &self.groups[group].stores;
        Ok(stores
            .iter()
            .all(|placed| read.contains(placed))
            .then(|| (group, reached.into_iter().map(|(_, head)| head).collect())))
    }

    /// The scope keys of each admitted binding, read from stage `from` at the positions
    /// `position` gives each binding's head keys.
    pub(super) fn scopes(
        &self,
        lookup: &Lookup,
        from: StageId,
        mut position: impl FnMut(usize, PropertyId) -> Result<usize>,
    ) -> Result<Vec<Input>> {
        let graph = self.query.model.graph();
        let mut scopes = Vec::new();
        for (index, (binding, _, pairs)) in lookup.admits.iter().enumerate() {
            scopes.push(Input::Scope(KeyInput {
                from,
                positions: pairs
                    .iter()
                    .map(|(_, key)| position(index, *key))
                    .collect::<Result<_>>()?,
                binding: self.query.name(*binding).to_string(),
                properties: pairs.iter().map(|(held, _)| *held).collect(),
                types: pairs
                    .iter()
                    .map(|(_, key)| graph.property(*key).data_type)
                    .collect(),
            }));
        }
        Ok(scopes)
    }

    /// The head's place for a read with `needs`, and the store of each chain entity, all on
    /// the head's connection, which runs relational stages.
    fn placed(&self, lookup: &Lookup, needs: &Needs) -> Result<(Place, Vec<(EntityId, StoreId)>)> {
        let model = self.query.model;
        let graph = model.graph();
        let head = place(model, self.connections, lookup.head, needs)?;
        let peer = self.peer(head.connection);
        if !peer.joins() {
            return Err(QueryError::Security(format!(
                "{}, the head of a via chain, is read on {}, which runs no relational stage",
                graph.entity(lookup.head).name,
                peer.name()
            )));
        }
        let mut stores = vec![(lookup.head, head.store)];
        for hop in &lookup.chain {
            let follower = place(model, self.connections, hop.to, &Needs::default())?;
            if follower.connection != head.connection {
                return Err(QueryError::Security(format!(
                    "{}'s via chain crosses from connection {} to {}",
                    graph.entity(lookup.head).name,
                    peer.name(),
                    self.peer(follower.connection).name()
                )));
            }
            stores.push((hop.to, follower.store));
        }
        Ok((head, stores))
    }

    /// The properties the reading side of a chain's first hop holds the head's keys in.
    fn held_keys(&self, hop: &Hop) -> Result<Held> {
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
    /// consumer's own conditions on the keys it holds, and the scope keys it hands the consumer.
    pub(super) fn lookup_stage(
        &self,
        lookup: &Lookup,
        id: StageId,
        inputs: &[Input],
        debug: bool,
    ) -> Result<(Stage, Vec<Input>)> {
        let model = self.query.model;
        let graph = model.graph();
        let holders = self.holders(lookup);
        let keyed = self.keyed(&holders, inputs);
        let named = self.named(&holders);
        let mut needs = Needs::default();
        for (_, key) in lookup.admits.iter().flat_map(|(_, _, pairs)| pairs) {
            let bound = keyed.iter().any(|input| input.properties.contains(key))
                || named.iter().any(|(property, _)| property == key);
            match bound {
                true => needs.apply(*key, Operation::OneOf),
                false => needs.read(*key),
            }
        }
        let (head, stores) = self.placed(lookup, &needs)?;
        let peer = self.peer(head.connection);
        if let Some(key) = keyed
            .iter()
            .flat_map(|input| &input.properties)
            .find(|key| !model.store(head.store).accepts(lookup.head, **key))
        {
            return Err(QueryError::Validation(format!(
                "{} on connection {} takes no key set on {}, so no scope lookup can read it",
                graph.entity(lookup.head).name,
                peer.name(),
                graph.property(*key).name
            )));
        }
        let entities: Vec<EntityId> = stores.iter().map(|(entity, _)| *entity).collect();
        let mut contract = Contract {
            columns: Vec::new(),
        };
        let mut returned: Vec<Vec<PropertyId>> = vec![Vec::new(); entities.len()];
        let mut push = |index: usize, property: PropertyId| {
            let identity = &graph.entity(entities[index]).identity;
            let handle = match identity.iter().position(|part| *part == property) {
                Some(part) => Handle::Identity(part),
                None => Handle::Property(property),
            };
            if !returned[index].contains(&property) {
                returned[index].push(property);
            }
            let output = Output::Node {
                binding: name(index),
                handle,
            };
            contract.push(
                output,
                ColumnType::Scalar(graph.property(property).data_type),
            )
        };
        let scopes = self.scopes(lookup, id, |_, key| Ok(push(0, key)))?;
        let access = model.access();
        let mut checks = Vec::new();
        for (index, entity) in entities.iter().enumerate() {
            for check in model.checks(*entity, head.connection) {
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
                let check = RowCheck {
                    provider: provider.clone(),
                    permission: access.permission(*permission).name.clone(),
                    resource: resource.clone(),
                    position: push(index, *key),
                    kind: crate::KeyKind::of(graph.property(*key).data_type),
                };
                if !checks.contains(&check) {
                    checks.push(check);
                }
            }
        }
        let stage = Stage {
            id,
            backend: peer.backend(),
            runs: Runs::On {
                connection: head.connection,
                stores,
            },
            query: StageQuery::Gql(Statement {
                plan: self.lookup_plan(lookup, &named, &returned)?,
                debug,
            }),
            inputs: keyed.into_iter().map(Input::Keys).collect(),
            contract: Some(contract),
            checks,
        };
        Ok((stage, scopes))
    }

    /// Each binding property that holds a head key, or that the group's own steps hold equal to
    /// one, with the head key it bounds.
    fn holders(&self, lookup: &Lookup) -> Vec<(Ref, PropertyId, PropertyId)> {
        let pairs: Vec<_> = self
            .query
            .steps()
            .into_iter()
            .enumerate()
            .filter(|(_, step)| {
                self.group(step.from) == lookup.group && self.group(step.to) == lookup.group
            })
            .filter_map(|(index, step)| {
                super::split::crossing(self.query, index, step, (lookup.group, lookup.group)).ok()
            })
            .flat_map(|crossing| crossing.pairs)
            .collect();
        let mut holders = Vec::new();
        for (binding, _, held_keys) in &lookup.admits {
            for (held, key) in held_keys {
                let mut equal = vec![(*binding, *held)];
                while let Some(next) = pairs
                    .iter()
                    .flat_map(|(a, b)| [(a, b), (b, a)])
                    .find(|(from, to)| equal.contains(from) && !equal.contains(to))
                {
                    equal.push(*next.1);
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
    fn keyed(&self, holders: &[(Ref, PropertyId, PropertyId)], inputs: &[Input]) -> Vec<KeyInput> {
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
    fn named(&self, holders: &[(Ref, PropertyId, PropertyId)]) -> Vec<(PropertyId, Vec<Literal>)> {
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

    /// The chain from its head, each bound value set as a condition on the head, returning
    /// `returned` of each chain binding.
    fn lookup_plan(
        &self,
        lookup: &Lookup,
        named: &[(PropertyId, Vec<Literal>)],
        returned: &[Vec<PropertyId>],
    ) -> Result<LogicalPlan> {
        let limits = orbit_server_config::QueryLimits {
            max_rows: self.limits.max_rows.max(self.limits.stage_keys as u64 + 1),
            ..*self.limits
        };
        let catalog = self
            .query
            .model
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
            let mut items = Vec::new();
            for (index, properties) in returned.iter().enumerate() {
                if properties.is_empty() {
                    continue;
                }
                let projection = Projection::Graph {
                    binding: rows.binding(&name(index))?.output(),
                    columns: Columns::Listed(properties.clone()),
                };
                items.push(projection.named(name(index)));
            }
            let rows = q.select(rows, items)?;
            q.limit(rows, self.limits.stage_keys as u64 + 1)
        })?)
    }
}

/// The variable a single-hop pattern step that walks `hop` from `from` reaches.
fn follow(steps: &[crate::logical::Step<'_>], from: Ref, hop: &Hop) -> Option<Ref> {
    steps.iter().find_map(|step| {
        let [walked] = step.hops else {
            return None;
        };
        if step.range.max != 1 || walked.variant != hop.variant {
            return None;
        }
        match (step.from == from, step.to == from) {
            (true, _) if walked.forward == hop.forward => Some(step.to),
            (_, true) if walked.forward != hop.forward => Some(step.from),
            _ => None,
        }
    })
}

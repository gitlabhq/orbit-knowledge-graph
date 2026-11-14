use std::collections::HashMap;

use query_data_model::PropertyId;
use query_data_model::plan::combine::Position;
use query_data_model::plan::logical::{Columns, Expr, LogicalOperation, MATCH_LINE, Projection};
use query_data_model::plan::tracking::Ref;

use super::cut::Cut;
use super::split::Crossing;
use super::{Contract, KeyInput, Output};
use crate::error::{QueryError, Result};
use crate::logical::columns::defaults;

/// One side of a crossing's key pairs: each binding and the property it joins on.
type KeySide = Vec<(Ref, PropertyId)>;

impl Crossing {
    /// The group that reads this crossing's keys: the one that runs later.
    pub(super) fn consumer(&self, order: &[usize]) -> usize {
        let position = |group| order.iter().position(|g| *g == group);
        let (a, b) = self.groups;
        match position(a) < position(b) {
            true => b,
            false => a,
        }
    }

    /// The key pairs split into the producer's side and the consumer's side; `group` names the
    /// group each binding belongs to.
    pub(super) fn sides(
        &self,
        order: &[usize],
        group: impl Fn(Ref) -> usize,
    ) -> (KeySide, KeySide) {
        let consumer = self.consumer(order);
        self.pairs
            .iter()
            .map(|(first, second)| match group(first.0) == consumer {
                true => (*second, *first),
                false => (*first, *second),
            })
            .unzip()
    }
}

impl Cut<'_> {
    /// The properties each binding's stage returns: the query's result columns, the keys of
    /// each crossing, the keys of the row checks a `checked` stage runs or else the key
    /// redaction reads, and the order keys.
    pub(super) fn needed(&self, checked: &[bool]) -> Result<HashMap<Ref, Vec<PropertyId>>> {
        let mut needed = self.selected()?;
        let mut push = |binding: Ref, property: PropertyId| {
            let properties = needed.entry(binding).or_default();
            if !properties.contains(&property) {
                properties.push(property);
            }
        };
        for crossing in &self.crossings {
            for (binding, property) in crossing.pairs.iter().flat_map(|(a, b)| [*a, *b]) {
                push(binding, property);
            }
        }
        let access = self.query.model.access();
        for (group, bindings) in self.groups.iter().map(|group| &group.bindings).enumerate() {
            for binding in bindings {
                let Some(entity) = self.query.entity(*binding) else {
                    continue;
                };
                if !checked[group] {
                    self.redaction_key(entity)
                        .into_iter()
                        .for_each(|key| push(*binding, key));
                    continue;
                }
                for check in &access.entity(entity).checks {
                    if let query_data_model::catalog::Check::Rows { id, .. } = check {
                        push(*binding, *id);
                    }
                }
            }
        }
        for (key, _) in self.query.sort_keys() {
            if let Expr::Property { binding, property } = key {
                push(binding, property);
            }
        }
        Ok(needed)
    }

    /// The properties each binding's stage returns for the query's result columns.
    fn selected(&self) -> Result<HashMap<Ref, Vec<PropertyId>>> {
        let graph = self.query.model.graph();
        let access = self.query.model.access();
        let mut needed: HashMap<Ref, Vec<PropertyId>> = HashMap::new();
        for item in self.query.selected() {
            match item {
                Projection::Graph { binding, columns } => {
                    let entity = self.query.entity(*binding).ok_or_else(|| {
                        QueryError::Validation(
                            "a query across connections returns nodes, not paths".into(),
                        )
                    })?;
                    let entity = graph.entity(entity);
                    let implicit = |property: &PropertyId| !access.property(*property).admin_only;
                    let listed: Vec<PropertyId> = match columns {
                        Columns::Listed(listed) => listed.clone(),
                        Columns::Default => defaults(entity).filter(implicit).collect(),
                        Columns::All => {
                            entity.properties.iter().copied().filter(implicit).collect()
                        }
                    };
                    let readable = listed
                        .into_iter()
                        .filter(|property| !access.property(*property).hidden);
                    needed.entry(*binding).or_default().extend(readable);
                }
                Projection::Value(Expr::Property { binding, property }) => {
                    needed.entry(*binding).or_default().push(*property);
                }
                Projection::Value(Expr::Output(_)) => {}
                Projection::Value(_) => {
                    return Err(QueryError::Validation(
                        "a query across connections returns properties and match columns".into(),
                    ));
                }
            }
        }
        Ok(needed)
    }

    /// The key set a crossing hands its consumer, on properties the consumer's store accepts;
    /// `stages` holds each group's stage.
    pub(super) fn keys(
        &self,
        crossing: &Crossing,
        order: &[usize],
        stages: &[usize],
        contracts: &[Contract],
    ) -> Result<KeyInput> {
        let (producer, consumer) = crossing.sides(order, |binding| self.group(binding));
        let producer_group = self.group(producer[0].0);
        let binding = self.query.name(consumer[0].0).to_string();
        let graph = self.query.model.graph();
        let group = &self.groups[self.group(consumer[0].0)];
        for (bound, property) in &consumer {
            let entity = self.query.entity(*bound).ok_or_else(|| {
                QueryError::PipelineInvariant("a crossing's endpoint has no entity".into())
            })?;
            let store = self.store(group, entity);
            if !store.accepts(entity, *property) {
                return Err(QueryError::Validation(format!(
                    "{} on connection {} takes no key set on {}.{}, so the query cannot cross into it there",
                    self.query.name(*bound),
                    self.query.model.connection(store.connection()).name(),
                    graph.entity(entity).name,
                    graph.property(*property).name
                )));
            }
        }
        let positions = producer
            .iter()
            .map(|(binding, property)| {
                let output = self.node(producer_group, *binding, *property)?;
                contracts[producer_group].position(&output).ok_or_else(|| {
                    QueryError::PipelineInvariant("a producer returns no key column".into())
                })
            })
            .collect::<Result<_>>()?;
        Ok(KeyInput {
            from: stages[producer_group],
            positions,
            binding,
            properties: consumer.iter().map(|(_, property)| *property).collect(),
            types: consumer
                .iter()
                .map(|(_, property)| graph.property(*property).data_type)
                .collect(),
        })
    }

    pub(super) fn group(&self, binding: Ref) -> usize {
        self.groups
            .iter()
            .position(|group| group.bindings.contains(&binding))
            .unwrap_or_default()
    }

    /// The result columns of returned text properties, named as `result_columns` names them;
    /// the keys a lookup resolves other properties by stay whole, as in a relational response.
    pub(super) fn excerpted(&self) -> Result<Vec<String>> {
        let model = self.query.model;
        let graph = model.graph();
        let mut excerpted = Vec::new();
        for (binding, properties) in self.selected()? {
            let Some(entity) = self.query.entity(binding) else {
                continue;
            };
            let keys: Vec<PropertyId> = graph
                .entity(entity)
                .properties
                .iter()
                .filter_map(|property| model.lookup(*property))
                .flat_map(|lookup| lookup.keys.iter().copied())
                .collect();
            let name = self.query.name(binding);
            for property in properties {
                let declared = graph.property(property);
                if declared.data_type == query_data_model::DataType::String
                    && !keys.contains(&property)
                {
                    excerpted.push(format!("{name}_{}", declared.name));
                }
            }
        }
        excerpted.sort();
        Ok(excerpted)
    }

    /// The query's result columns in its RETURN order, each read from the stage that holds it.
    pub(super) fn result_columns(
        &self,
        position: &impl Fn(Ref, PropertyId) -> Result<Position>,
        stage: &impl Fn(usize) -> usize,
        contracts: &[Contract],
    ) -> Result<Vec<(String, Position)>> {
        let selected = self.selected()?;
        let graph = self.query.model.graph();
        let mut columns = Vec::new();
        for (node, operation) in self.query.operations() {
            let LogicalOperation::Select(items) = operation else {
                continue;
            };
            let outputs: Vec<Ref> = self.query.plan.outputs(node).collect();
            let names = &outputs[outputs.len() - items.len()..];
            for (item, output) in items.iter().zip(names) {
                let name = self.query.name(*output);
                match item {
                    Projection::Graph { binding, .. } => {
                        for property in &selected[binding] {
                            columns.push((
                                format!("{name}_{}", graph.property(*property).name),
                                position(*binding, *property)?,
                            ));
                        }
                    }
                    Projection::Value(Expr::Property { binding, property }) => {
                        columns.push((name.to_string(), position(*binding, *property)?));
                    }
                    Projection::Value(Expr::Output(output)) => {
                        let matched = match self.query.name(*output) {
                            MATCH_LINE => Output::Line,
                            other => Output::Capture(
                                other.strip_prefix("match.").unwrap_or(other).to_string(),
                            ),
                        };
                        let holder = self.matched_group(*output)?;
                        let column = contracts[holder].position(&matched).ok_or_else(|| {
                            QueryError::PipelineInvariant("a match column has no position".into())
                        })?;
                        columns.push((
                            name.to_string(),
                            Position {
                                stage: stage(holder),
                                column,
                            },
                        ));
                    }
                    Projection::Value(_) => {
                        return Err(QueryError::Validation(
                            "a query across connections returns properties and match columns"
                                .into(),
                        ));
                    }
                }
            }
        }
        Ok(columns)
    }

    /// The group of the binding a structural match runs over.
    pub(super) fn matched_group(&self, output: Ref) -> Result<usize> {
        self.query
            .operations()
            .find_map(|(node, operation)| match operation {
                LogicalOperation::Structural(structural)
                    if self
                        .query
                        .plan
                        .outputs(node)
                        .any(|declared| declared == output) =>
                {
                    super::cut::referenced(structural.target).map(|binding| self.group(binding))
                }
                _ => None,
            })
            .ok_or_else(|| QueryError::PipelineInvariant("a match column has no match".into()))
    }
}

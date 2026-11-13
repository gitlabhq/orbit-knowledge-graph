use std::collections::HashMap;

use query_data_model::PropertyId;
use query_data_model::plan::logical::{Columns, Expr, LogicalOperation, MATCH_LINE, Projection};
use query_data_model::plan::tracking::Ref;

use super::cut::Cut;
use super::split::Crossing;
use super::{CombineColumn, KeyInput, StageId};
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
    /// The properties each binding's stage returns for the query's result columns.
    pub(super) fn selected(&self) -> Result<HashMap<Ref, Vec<PropertyId>>> {
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
                    let listed: Vec<PropertyId> = match columns {
                        Columns::Listed(listed) => listed.clone(),
                        Columns::Default => defaults(entity).collect(),
                        Columns::All => entity.properties.clone(),
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

    /// The key set a crossing hands its consumer, on properties the consumer's store accepts.
    pub(super) fn keys(&self, crossing: &Crossing, order: &[usize]) -> Result<KeyInput> {
        let stage = |group: usize| order.iter().position(|g| *g == group).unwrap_or_default();
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
        Ok(KeyInput {
            from: stage(producer_group),
            columns: producer
                .iter()
                .map(|(binding, property)| {
                    self.column(stage(producer_group), *binding, *property)
                        .column
                })
                .collect(),
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

    /// The query's result columns in its RETURN order, each read from the stage that holds it.
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

    pub(super) fn result_columns(
        &self,
        stage: &impl Fn(usize) -> StageId,
        group_of: &impl Fn(Ref) -> usize,
    ) -> Result<Vec<(String, CombineColumn)>> {
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
                            let column =
                                self.column(stage(group_of(*binding)), *binding, *property);
                            columns.push((
                                format!("{name}_{}", graph.property(*property).name),
                                column,
                            ));
                        }
                    }
                    Projection::Value(Expr::Property { binding, property }) => {
                        let column = self.column(stage(group_of(*binding)), *binding, *property);
                        columns.push((name.to_string(), column));
                    }
                    Projection::Value(Expr::Output(output)) => {
                        let matched = self.query.name(*output);
                        let column = match matched {
                            MATCH_LINE => "line",
                            other => other.strip_prefix("match.").unwrap_or(other),
                        };
                        let holder = self.matched_group(*output)?;
                        columns.push((
                            name.to_string(),
                            CombineColumn {
                                stage: stage(holder),
                                column: column.to_string(),
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

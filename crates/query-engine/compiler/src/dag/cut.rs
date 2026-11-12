use orbit_server_config::QueryLimits;
use query_data_model::PropertyId;
use query_data_model::plan::logical::{Expr, LogicalOperation};
use query_data_model::plan::tracking::Ref;

use super::split::{Crossing, Place};
use super::{
    Backend, Dag, EngineColumn, EngineJoin, EngineQuery, Input, Stage, StageId, StageQuery,
};
use crate::error::{QueryError, Result};
use crate::logical::Query;

pub(super) struct Group {
    pub place: Place,
    pub bindings: Vec<Ref>,
}

pub(super) struct Cut<'q> {
    pub query: Query<'q>,
    pub groups: Vec<Group>,
    pub crossings: Vec<Crossing>,
    /// The primary connection first, then each served remote.
    pub backends: Vec<(String, Backend)>,
    pub limits: &'q QueryLimits,
    pub remotes: &'q [String],
}

impl Cut<'_> {
    pub fn dag(self, debug: bool) -> Result<Dag> {
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
        let mut engine_order = Vec::new();
        for (key, direction) in self.query.sort_keys() {
            let Expr::Property { binding, property } = key else {
                return Err(QueryError::Validation(
                    "a query across connections orders by node properties".into(),
                ));
            };
            push(needed.entry(binding).or_default(), property);
            engine_order.push((
                self.column(stage(group_of(binding)), binding, property),
                direction,
            ));
        }
        let mut stages = Vec::new();
        for (id, group) in order.iter().enumerate() {
            let (connection, backend) = self.backend(self.groups[*group].place);
            let inputs = self
                .crossings
                .iter()
                .filter(|crossing| crossing.consumer(&order) == *group)
                .map(|crossing| Input::Keys(self.keys(crossing, &order)))
                .collect();
            let group = &self.groups[*group];
            let plan = super::sub::plan(self.query, group, &needed, self.limits, self.remotes)?;
            stages.push(Stage {
                id,
                backend,
                connection: Some(connection),
                query: StageQuery::Gql(query_data_model::surface::Statement { plan, debug }),
                inputs,
                response: false,
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
                EngineJoin {
                    left: side(producer),
                    right: side(consumer),
                }
            })
            .collect();
        let (limit, skip) = self
            .query
            .limit()
            .unwrap_or((self.limits.default_rows, None));
        let engine = EngineQuery {
            joins,
            columns: self.result_columns(&stage, &group_of)?,
            order: engine_order,
            limit,
            skip,
        };
        let inputs = (0..stages.len()).map(|from| Input::Rows { from }).collect();
        stages.push(Stage {
            id: stages.len(),
            backend: Backend::Engine,
            connection: None,
            query: StageQuery::Engine(engine),
            inputs,
            response: true,
        });
        Ok(Dag {
            stages,
            logical: Vec::new(),
        })
    }

    fn backend(&self, place: Place) -> (String, Backend) {
        match place {
            Place::Primary => self.backends[0].clone(),
            Place::Remote(index) => self.backends[index + 1].clone(),
        }
    }

    /// The name of the column a stage returns a binding's property in.
    pub(super) fn column(
        &self,
        stage: StageId,
        binding: Ref,
        property: PropertyId,
    ) -> EngineColumn {
        let name = &self.query.model.graph().property(property).name;
        let group = self
            .groups
            .iter()
            .find(|group| group.bindings.contains(&binding));
        let relational = group.is_some_and(|group| self.backend(group.place).1.relational());
        EngineColumn {
            stage,
            column: match relational {
                true => format!("{}_{name}", self.query.name(binding)),
                false => name.clone(),
            },
        }
    }

    /// Stages run from the bounded side of each crossing: a stage whose own bindings carry a
    /// condition reads first, and the relational connection reads first between equals.
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
            (!bounded(group), group.place != Place::Primary, index)
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
